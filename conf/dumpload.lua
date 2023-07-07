-- TODO: top level issues:
-- implement multiple destination sockets for through-proxy performance.
-- destination buffer size limiter.

local DEFAULT_HOST = "127.0.0.1"
local DEFAULT_PORT = "11211"
local o = {
    key_host = { host = DEFAULT_HOST, port = DEFAULT_PORT },
    src_host = { host = DEFAULT_HOST, port = DEFAULT_PORT },
    dst_host = { host = DEFAULT_HOST, port = DEFAULT_PORT },
    startwait = 10,
    key_batch_size = 1000,
    key_rate_limit = 0,
    bw_rate_limit = 0 -- kilobits
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

function say(...)
    if verbose then
        print(...)
    end
end

function help()
    local msg =[[

verbose (false) (print extra state information)
stats (false) (print some stats output every second)
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
    ]]
    print(msg)
end

function config(a)
    if a.verbose then
        verbose = true
        o.verbose = true
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
        o.startwait = a.startwait
    end
    if a.batch_size then
        say("overriding key batch size:", a.batch_size)
        o.key_batch_size = a.batch_size
    end
    if a.key_rate_limit then
        say("overriding key rate limit:", a.key_rate_limit)
        o.key_rate_limit = a.key_rate_limit
    end
    if a.bw_rate_limit then
        say("overriding bandwidth rate limit:", a.bw_rate_limit)
        o.bw_rate_limit = a.bw_rate_limit
    end
    say("completed argument parsing")

    local t = mcs.thread()
    mcs.add_custom(t, { func = "dumpload" }, o)
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
        last_keys_listed = s_keys_listed
        last_keys_sent = s_keys_sent
        last_bytes_sent = s_bytes_sent
        last_notstored = s_notstored
        last_server_error = s_server_error
    end

    print("===FINAL STATS===")
    print("keys_listed:", s_keys_listed)
    print("keys_sent:", s_keys_sent)
    print("bytes_sent:", s_bytes_sent)
    print("notstored:", s_notstored)
    print("sever_error:", s_server_error)
end

function dumpload(a)
    dumpload_run(a)
    dump_complete = true
end

function dumpload_run(a)
    local keys_in = {}
    verbose = a.verbose

    say("key host:", a.key_host["host"])
    say("key port:", a.key_host["port"])
    --mcs.sleep_millis(5000)
    local key_c = mcs.client_new(a.key_host)
    -- TODO: error handling.
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
    local dst_c = mcs.client_new(a.dst_host)
    if not mcs.client_connect(dst_c) then
        print("ERROR: Failed to connect to destination")
        return
    end
    say("destination socket connected")

    for x=1,a.startwait do
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

    say("dump started")
    dump_started = true

    local key_batch_size = a.key_batch_size
    local bw_rate_limit = 0
    local key_rate_limit = 0
    if a.key_rate_limit ~= 0 then
        -- limit every 10th of a second for smoothing.
        key_batch_size = a.key_rate_limit / 10
        key_rate_limit = a.key_rate_limit
    end

    if a.bw_rate_limit ~= 0 then
        local bits_sec = a.bw_rate_limit * 1000
        -- since we write bytes.
        local bytes_sec = bits_sec / 8
        -- reduce to the timeslice limit.
        bw_rate_limit = bytes_sec / 10
    end

    while true do
        local window_start = mcs.time_millis()

        --print("reading key batch")
        read_keys(key_c, keys_in, key_batch_size)
        --print("key batch read")
        request_src_keys(src_c, keys_in)
        --print("received keys from source")
        send_keys_to_dst(src_c, dst_c, window_start, bw_rate_limit)
        --print("sent keys to dest")

        if bw_rate_limit ~= 0 or key_rate_limit ~= 0 then
            relax(window_start)
        end

        -- clear the keys queue so we'll fetch another batch.
        keys_in = {}
        if key_complete then
            print("===DUMP=== complete")
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
function send_keys_to_dst(src_c, dst_c, window_start, bw_limit)
    -- "res" objects are only valid until the next time mcs.client_read(c) is
    -- called, so we cannot stack them. This is done to avoid extra large
    -- allocations and data copies.
    local sent_bytes = 0
    while true do
        local res, err = mcs.client_read(src_c)
        if res == nil then
            error("ERROR: reading from data source failed: " .. err)
        end
        local rline = mcs.resline(res)
        --print("result line from source:", rline)
        -- TODO: c func for asking if this res is an MN instead of copying the
        -- string line.
        if rline == "MN" then
            --print("completed dest batch write")
            mcs.client_write(dst_c, "mn\r\n")
            break
        else
            s_keys_sent = s_keys_sent + 1
            mcs.client_write_mgres_to_ms(res, dst_c)
            local bytes = mcs.res_len(res)
            sent_bytes = sent_bytes + bytes
            s_bytes_sent = s_bytes_sent + bytes
        end
        if bw_limit ~= 0 and sent_bytes > bw_limit then
            mcs.client_flush(dst_c)
            relax(window_start)
            window_start = mcs.time_millis()
            sent_bytes = 0
        end
    end

    -- write outstanding bytes to the network socket.
    local success, err = mcs.client_flush(dst_c)
    if not success then
        error("ERROR: flushing requests to dest failed: " .. err)
    end
    -- confirm batch was sent.
    while true do
        local res, err = mcs.client_read(dst_c)
        if res == nil then
            error("ERROR: reading response from dest failed: " .. err)
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
                print(rline)
            elseif t[1] == "CLIENT_ERROR" or t[1] == "CLIENT_ERROR" or t[1] == "ERROR" then
                error("ERROR: must stop, protocol error: " .. rline)
            else
                error("ERROR: garbage received from dest: " .. rline)
            end
        end
    end
    --print("flushed destination client")
end

function request_src_keys(src_c, keys_in)
    for _, req in ipairs(keys_in) do
        local full_req = req .. " k f t v u\r\n"
        mcs.client_write(src_c, full_req)
    end
    -- send end cap for this batch.
    mcs.client_write(src_c, "mn\r\n")
    local success, err = mcs.client_flush(src_c)
    if not success then
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
