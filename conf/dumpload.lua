-- THEORY OF OPERATION
-- Main loop:
-- - read key batch from key lister (ie: 1000 keys)
-- - write key request batch to source host. cap with "mn\r\n"
-- - select free dest host connection
-- - move responses from source host to dest host write buffer
-- - stop when "MN\r\n" received from source host
-- - send dest connection to coroutine
--   - coroutine adds "mn\r\n" cap to write buffer
--   - flush requests
--   - read responses.
--     - counts NS.
--     - errors if CLIENT_ERROR|ERROR
--     - if SERVER_ERROR or dest disconnected, mark batch for retry
--   - if dst is disconnected, wait for reconnect in loop
--   - else mark batch completed
-- - in main loop, if dest connection is found waiting for retry, re-submit
--   its key batch to the source host as above and re-dispatch.
-- - when key dumping is complete and no dest conns are busy, exit.
--
-- TECHNICAL DETAILS
-- * The main coroutine reads keys from the key list and writes/reads data
-- from the source connection
-- * A coroutine exists for each destination connection. These coroutines
-- handle flushing their own write buffers to the network, and reading
-- responses back from the destination host to confirm a batch was sent.
-- * Internally this allows some syscall parallelism due to io_uring.
-- * Multiple coroutines can make progress concurrently, allowing us to make
-- good use of the CPU.
--
-- RETRY THEORY
-- * If key lister or source host disconnect, halt.
-- * If anything goes wrong with the destination, the entire key batch is retried.
-- * If the destination socket disconnected, we cannot confirm if any keys
-- were successfully written.
-- * If any individual key fails with SERVER_ERROR, it is likely a lot more
-- will fail.
-- * There is no benefit to only retrying keys which met with
-- SERVER_ERROR, as we _must_ be tolerant to retrying the entire batch in cases
-- where the destination host reconnects.
-- * The code is much shorter and simpler if we retry the whole batch.
-- * Confirming individual keys halves the performance
-- * The batches are not very large; and even smaller if key rate limits are
-- employed, minimizing the amount of rewriting.

-- TODO: top level issues:
-- * destination buffer size limiter.
-- * CLIENT_ERROR / etc aren't killing the process properly under retry mode, as
--   those errors are handled by mcshredder. needs mcshredder fix.
-- * key prefix filtering
-- * jump hash node filtering
-- * some more response filtering from the source host is probably helpful for
-- paranoia.
-- * I lost 20% performance converting the main loop for retries. See if this
-- can be regained.

local DEFAULT_HOST = "127.0.0.1"
local DEFAULT_PORT = "11211"
local o = {
    key_host = { host = DEFAULT_HOST, port = DEFAULT_PORT },
    src_host = { host = DEFAULT_HOST, port = DEFAULT_PORT },
    dst_host = { host = DEFAULT_HOST, port = DEFAULT_PORT },
    dst_conns = 1,
    startwait = 10,
    key_batch_size = 1000,
    key_rate_limit = 0,
    bw_rate_limit = 0, -- kilobits
    retry = false,
}

local verbose = false
key_complete = false -- global
dump_complete = false -- global
dump_started = false -- global
s_keys_listed = 0
s_keys_sent = 0
s_bytes_sent = 0
s_notstored = 0
s_server_error = 0
s_retries = 0

-- INTERNAL GLOBALS
-- to parallelize writing data to and verifying responses with destination
-- connections, we use more coroutines. These globals are used for signalling
-- and passing the destination sockets to the other coroutines.
-- a short sleep is currently used for polling, but wait-and-wake semantics
-- should be pretty easy to pull off to remove this.
--
-- Even with just the sleep the performance seems to nearly match mcdumpload's
-- multi-dest sockets.
g_dst_conns = {}
g_dst_flushing = {}
g_dst_batch = {}
g_dst_retry = {}

function say(...)
    if verbose then
        print(...)
    end
end

function help()
    local msg =[[

verbose (false) (print extra state information)
stats (false) (print some stats output every second)
retry (false) (add extra validation and retry steps for unreliable situations)
key_host (127.0.0.1) (host to fetch key list from)
key_port (11211) (port for above)
src_host (127.0.0.1) (host to fetch key data from)
src_port (11211) (port for above)
dst_host (127.0.0.1) (host to send key data to)
dst_port (11211) (port for above)
startwait (10) (seconds to wait for key item stream to start)
key_batch_size (1000) (number of keys to fetch from source at once)
key_rate_limit (0) (max number of keys to process per second)
bw_rate_limit (0) (max number of kilobits to transfer per second)
dst_conns (1) (number of destination sockets to use)
    ]]
    print(msg)
end

function config(a)
    if a.verbose then
        verbose = true
        o.verbose = true
    end
    if a.retry then
        o.retry = true
    end
    if a.key_host then
        say("overriding key host name:", a.key_host)
        o.key_host.host = a.key_host
    end
    if a.key_port then
        say("overriding key host port:", a.key_port)
        o.key_host.port = a.key_port
    end
    if a.src_host then
        say("overriding src host name:", a.src_host)
        o.src_host.host = a.src_host
    end
    if a.src_port then
        say("overriding src host port:", a.src_port)
        o.src_host.port = a.src_port
    end
    if a.dst_host then
        say("overriding dst host name:", a.dst_host)
        o.dst_host.host = a.dst_host
    end
    if a.dst_port then
        say("overriding dst host port:", a.dst_port)
        o.dst_host.port = a.dst_port
    end
    if a.startwait then
        say("overriding start wait time:", a.startwait)
        o.startwait = tonumber(a.startwait)
    end
    if a.batch_size then
        say("overriding key batch size:", a.batch_size)
        o.key_batch_size = tonumber(a.batch_size)
    end
    if a.key_rate_limit then
        say("overriding key rate limit:", a.key_rate_limit)
        o.key_rate_limit = tonumber(a.key_rate_limit)
    end
    if a.bw_rate_limit then
        --say("overriding bandwidth rate limit:", a.bw_rate_limit)
        --o.bw_rate_limit = tonumber(a.bw_rate_limit)
        print("WARNING: bandwidth limiter disabled pending code fix")
    end
    if a.dst_conns then
        say("overriding destination connection limit:", a.dst_conns)
        o.dst_conns = tonumber(a.dst_conns)
    end
    say("completed argument parsing")

    -- setup the rate limiters if specified.
    if o.key_rate_limit ~= 0 then
        -- limit every 10th of a second for smoothing.
        o.key_batch_size = o.key_rate_limit / 10
    end

    if o.bw_rate_limit ~= 0 then
        local bits_sec = o.bw_rate_limit * 1000
        -- since we write bytes.
        local bytes_sec = bits_sec / 8
        -- reduce to the timeslice limit.
        o.bw_rate_limit = bytes_sec / 10
    end

    -- initialize thread and coroutines.
    local t = mcs.thread()
    mcs.add_custom(t, { func = "dumpload" }, o)
    for x=1,o.dst_conns do
        mcs.add_custom(t, { func = "destwriter" }, { idx = x, retry = o.retry })
    end
    if a.stats then
        mcs.add_custom(t, { func = "stats" })
    end
    mcs.shredder({t})
end

-- since this stats function and the dumpload are using the same OS thread,
-- they use the same VM and we can share data via globals.
-- not using a table for the stats for a little extra speed in the dumpload
-- part.
function stats()
    local last_keys_listed = 0
    local last_keys_sent = 0
    local last_bytes_sent = 0
    local last_notstored = 0
    local last_server_error = 0
    local last_retries = 0
    while dump_started == false do
        mcs.sleep_millis(100)
        -- so we don't print random junk if the dump doesn't even start.
        if dump_complete then
            return
        end
    end
    while dump_complete == false do
        mcs.sleep_millis(1000)
        print("===STATS===")
        print("keys_listed:", s_keys_listed - last_keys_listed)
        print("keys_sent:", s_keys_sent - last_keys_sent)
        print("bytes_sent:", s_bytes_sent - last_bytes_sent)
        print("notstored:", s_notstored - last_notstored)
        print("server_error:", s_server_error - last_server_error)
        print("batch retries:", s_retries - last_retries)
        print("===END STATS===")
        last_keys_listed = s_keys_listed
        last_keys_sent = s_keys_sent
        last_bytes_sent = s_bytes_sent
        last_notstored = s_notstored
        last_server_error = s_server_error
        last_retries = s_retries
    end

    print("===FINAL STATS===")
    print("keys_listed:", s_keys_listed)
    print("keys_sent:", s_keys_sent)
    print("bytes_sent:", s_bytes_sent)
    print("notstored:", s_notstored)
    print("sever_error:", s_server_error)
    print("batch retries:", s_retries)
    print("===END STATS===")
end

function dumpload(a)
    verbose = a.verbose
    dumpload_run(a)

    print("===DUMP=== complete")
    dump_complete = true
end

-- creates and initializes the client objects.
-- side effect: modifies g_dst_conns
function dumpload_makeconns(a)
    local key_c = mcs.client_new(a.key_host)
    if not mcs.client_connect(key_c) then
        print("ERROR: Failed to connect to key dump source")
        return
    end

    say("keylist socket connected")
    local src_c = mcs.client_new(a.src_host)
    if not mcs.client_connect(src_c) then
        print("ERROR: Failed to connet to data source")
        return
    end

    say("source socket connected")
    local dst_conns = {}
    for x=1,a.dst_conns do
        local dst_c = mcs.client_new(a.dst_host)
        if not mcs.client_connect(dst_c) then
            print("ERROR: Failed to connect to destination")
            return
        end
        dst_conns[x] = dst_c
        g_dst_conns[x] = dst_c
    end
    say("destination sockets connected:", #dst_conns)

    return key_c, src_c, dst_conns
end

function dumpload_start(key_c, trials)
    local keys_in = {}

    for x=1,trials do
        mcs.client_write(key_c, "lru_crawler mgdump hash\r\n")
        mcs.client_flush(key_c)
        -- read a raw line to avoid protocol parsing
        local rline = mcs.client_readline(key_c)
        if rline == "BUSY" then
            print("waiting for lru_crawler")
            mcs.sleep_millis(1000)
        else
            -- need to remember the first key returned.
            table.insert(keys_in, rline)
            s_keys_listed = s_keys_listed + 1
            break
        end
    end

    if #keys_in == 0 then
        error("ERROR: timed out waiting for lru_crawler to become available")
    end

    return keys_in
end

function dumpload_run(a)
    local key_c, src_c, dst_conns = dumpload_makeconns(a)
    local keys_in = dumpload_start(key_c, a.startwait)

    say("dump started")
    dump_started = true

    local key_batch_size = a.key_batch_size
    local bw_rate_limit = a.bw_rate_limit
    local key_rate_limit = a.key_rate_limit
    local window_start = mcs.time_millis()

    while true do

        local busy_dst = 0
        for x=1,#dst_conns do
            local dst_c = dst_conns[x]
            -- check if destination is ready for a new batch.
            if g_dst_flushing[x] then
                busy_dst = busy_dst + 1
            else
                -- reuse the prior key batch if we need to issue a retry.
                if g_dst_retry[x] then
                    g_dst_retry[x] = false
                else
                    read_keys(key_c, keys_in, key_batch_size)
                    g_dst_batch[x] = keys_in
                    keys_in = {}
                end

                -- if the key dump is complete, we might not fetch keys. lua
                -- has no 'continue' so we test the batch length.
                if #g_dst_batch[x] ~= 0 then
                    request_src_keys(src_c, g_dst_batch[x], a.retry)
                    send_keys_to_dst(src_c, dst_c, window_start, bw_rate_limit)

                    -- mark destination connection as ready to flush.
                    -- coroutine will pick it up
                    g_dst_flushing[x] = true
                    busy_dst = busy_dst + 1

                    -- rate limiter is checked for each key batch.
                    if bw_rate_limit ~= 0 or key_rate_limit ~= 0 then
                        relax(window_start)
                        window_start = mcs.time_millis()
                    end
                else
                    print("BATCH EMPTY")
                end
            end
        end

        -- let other coroutines run.
        if busy_dst ~= 0 then
            mcs.sleep_millis(1)
        end

        if key_complete and busy_dst == 0 then
            break
        end
    end
end

function relax(window_start)
    local window_end = mcs.time_millis()
    if window_start + 100 > window_end then
        local tosleep = window_start + 100 - window_end
        --print("sleeping:", tosleep)
        mcs.sleep_millis(tosleep)
    end
end

-- TODO: limit bytes in buffer by periodically flushing to dest.
--
function send_keys_to_dst(src_c, dst_c, window_start, bw_limit)
    -- "res" objects are only valid until the next time mcs.client_read(c) is
    -- called, so we cannot stack them. This is done to avoid extra large
    -- allocations and data copies.
    local sent_bytes = 0
    local res = mcs.res_new()

    while true do
        local err = mcs.client_read(src_c, res)
        if err ~= nil then
            error("ERROR: reading response from source failed: " .. err)
        end
        -- TODO: tick counter and skip write if EN
        local rline = mcs.resline(res)
        -- TODO: c func for asking if this res is an MN instead of copying the
        -- string line.
        if rline == "MN" then
            break
        else
            local done = mcs.client_write_mgres_to_ms(res, dst_c)

            if done then
                s_keys_sent = s_keys_sent + 1
                local bytes = mcs.res_len(res)
                sent_bytes = sent_bytes + bytes
                s_bytes_sent = s_bytes_sent + bytes
            end
        end

        -- FIXME: this doesn't align with retry mode.
        -- might have to just let bw be spikey and relax at a higher level?
        -- or remove the bw limiter mode and just use key rate?
        --if bw_limit ~= 0 and sent_bytes > bw_limit then
        --    mcs.client_flush(dst_c)
        --    relax(window_start)
        --    window_start = mcs.time_millis()
        --    sent_bytes = 0
        --end
    end
end

-- separate coroutine function for flushing to destination sockets.
-- NOTE: can't use pcall() and handle errors for the retry loop here, as
-- protected calls across yielded C functions requires special handling.
function destwriter(o)
    local idx = o.idx
    while not dump_started do
        mcs.sleep_millis(10)
    end
    local dst_c = g_dst_conns[idx]

    while true do
        if not g_dst_flushing[idx] then
            mcs.sleep_millis(1)
        else
            local done = dest_flush(dst_c)
            if not done then
                if o.retry then
                    g_dst_retry[idx] = true
                    s_retries = s_retries + 1
                    while not mcs.client_connected(dst_c) do
                        mcs.sleep_millis(500)
                        mcs.client_connect(dst_c)
                    end
                elseif not mcs.client_connected(dst_c) then
                    -- only error if we didn't want to retry but the whole
                    -- socket is lost.
                    error("connection lost while reading from dest")
                end
            end
            g_dst_flushing[idx] = false

        end

        if dump_complete then
            break
        end
    end
end

-- actually handles flushing
function dest_flush(dst_c)
    local done = true
    -- cap the write and flush
    mcs.client_write(dst_c, "mn\r\n")
    local err = mcs.client_flush(dst_c)
    if err ~= nil then
        print("ERROR: flushing requests to dest failed: " .. err)
        return false
    end

    local res = mcs.res_new()

    while true do
        local err = mcs.client_read(dst_c, res)
        if err ~= nil then
            print("ERROR: reading response from dest failed: " .. err)
            return false
        end
        local rline = mcs.resline(res)
        if rline == "MN" then
            break
        elseif rline == "NS" then
            s_notstored = s_notstored + 1
        else
            -- Some sort of error.
            local t = mcs.res_split(res)
            -- FIXME: need internal fixes before the non-SE sections below can
            -- work. The destination read will fail earlier than this code.
            if t[1] == "SERVER_ERROR" then
                s_server_error = s_server_error + 1
                done = false
                print(rline)
            elseif t[1] == "CLIENT_ERROR" or t[1] == "CLIENT_ERROR" or t[1] == "ERROR" then
                error("ERROR: must stop, protocol error: " .. rline)
            else
                error("ERROR: garbage received from dest: " .. rline)
            end
        end
    end

    return done
end

function request_src_keys(src_c, keys_in)
    for _, req in ipairs(keys_in) do
        local full_req = req .. " k f t v u\r\n"
        mcs.client_write(src_c, full_req)
    end
    -- send end cap for this batch.
    mcs.client_write(src_c, "mn\r\n")
    local err = mcs.client_flush(src_c)
    if err ~= nil then
        error("ERROR: flushing requests to data source: " .. err)
    end
end

function read_keys(key_c, keys_in, key_batch_size)
    if key_complete then
        return
    end

    while #keys_in < key_batch_size do
        -- read raw line to avoid protocol parsing
        local rline, err = mcs.client_readline(key_c)
        if rline == nil then
            error("ERROR: key dump read failed: " .. err)
        elseif rline == "EN" then
            print("===DUMP=== key listing complete")
            key_complete = true
            return
        else
            s_keys_listed = s_keys_listed + 1
            table.insert(keys_in, rline)
        end
    end
end
