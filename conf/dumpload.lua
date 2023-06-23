-- TODO: top level issues:
-- arg passing for add_custom() to turn below into commandline arguments
-- implement multiple destination sockets for through-proxy performance.
-- verbose option
-- per-second stats/progress output like mcdumpload.
-- lua level help output.
-- destination buffer size limiter.
-- various error handling for connection retries.
local key_host = { host = "127.0.0.1", port = "11212" }
local src_host = { host = "127.0.0.1", port = "11212" }
local dst_host = { host = "127.0.0.1", port = "11213" }
local keydump_retries = 10
local key_batch_size = 1000
local key_complete = false

local rate_limit = 0
-- kilobits.
local bw_limit = 0

function config(a)
    local t = mcs.thread()
    mcs.add_custom(t, { func = "dumpload" })
    mcs.shredder({t})
end

function dumpload(a)
    local keys_in = {}

    local key_c = mcs.client_new(key_host)
    -- TODO: error handling.
    mcs.client_connect(key_c)
    print("keylist socket connected")
    local src_c = mcs.client_new(src_host)
    mcs.client_connect(src_c)
    print("source socket connected")
    local dst_c = mcs.client_new(dst_host)
    mcs.client_connect(dst_c)
    print("destination socket connected")

    for x=1,keydump_retries do
        mcs.client_write(key_c, "lru_crawler mgdump hash\r\n")
        mcs.client_flush(key_c)
        -- read a raw line to avoid protocol parsing
        local rline = mcs.client_readline(key_c)
        if rline == "BUSY" then
            print("waiting for lru_crawler")
            mcs.sleep_millis(1000)
        else
            table.insert(keys_in, rline)
            break
        end
    end

    print("dump started")

    if rate_limit ~= 0 then
        -- limit every 10th of a second for smoothing.
        key_batch_size = rate_limit / 10
    end

    if bw_limit ~= 0 then
        local bits_sec = bw_limit * 1000
        -- since we write bytes.
        local bytes_sec = bits_sec / 8
        -- reduce to the timeslice limit.
        bw_limit = bytes_sec / 10
    end

    while true do
        local window_start = mcs.time_millis()

        --print("reading key batch")
        read_keys(key_c, keys_in)
        --print("key batch read")
        request_src_keys(src_c, keys_in)
        --print("received keys from source")
        send_keys_to_dst(src_c, dst_c, window_start, bw_limit)
        --print("sent keys to dest")

        if bw_limit ~= 0 or rate_limit ~= 0 then
            relax(window_start)
        end

        keys_in = {}
        if key_complete then
            print("dump complete")
            return
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
        local res = mcs.client_read(src_c)
        local rline = mcs.resline(res)
        --print("result line from source:", rline)
        -- TODO: c func for asking if this res is an MN instead of copying the
        -- string line.
        if rline == "MN" then
            --print("completed dest batch write")
            mcs.client_write(dst_c, "mn\r\n")
            break
        else
            mcs.client_write_mgres_to_ms(res, dst_c)
            sent_bytes = sent_bytes + mcs.res_len(res)
        end
        if bw_limit ~= 0 and sent_bytes > bw_limit then
            mcs.client_flush(dst_c)
            relax(window_start)
            window_start = mcs.time_millis()
            sent_bytes = 0
        end
    end

    -- write outstanding bytes to the network socket.
    mcs.client_flush(dst_c)
    -- confirm batch was sent.
    while true do
        local res = mcs.client_read(dst_c)
        local rline = mcs.resline(res)
        if rline == "MN" then
            break
        end
        -- TODO: else count the NS.
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
    mcs.client_flush(src_c)

    -- clear the keys queue so we'll fetch another batch.
    keys_in = {}
end

function read_keys(key_c, keys_in)
    if key_complete then
        return
    end

    while #keys_in < key_batch_size do
        -- read raw line to avoid protocol parsing
        local rline = mcs.client_readline(key_c)
        if rline == "EN" then
            print("key read complete")
            key_complete = true
            return
        else
            table.insert(keys_in, rline)
        end
    end
end
