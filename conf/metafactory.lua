local total_keys = 100000
local prefix = "foo"
local vsize = 100

function config()
    print("starting pre-warm")
    local warm = mcs.thread()
    mcs.run(warm, { func = "warm", clients = 1, limit = total_keys, rate_limit = 0 })
    mcs.shredder({warm}, 30) -- wait up to 30 seconds for warmer to run
    print("warming completed")

    print("starting test")
    local threads = {}
    for x=1,2 do
        table.insert(threads, mcs.thread())
    end
    local ts = mcs.thread()

    mcs.add(threads, { func = "basic", clients = 50, rate_limit = 0, init = true }, { total = total_keys, prefix = prefix, vsize = vsize })
    --mcs.add(threads, { func = "basic_noinit", clients = 50, rate_limit = 0 }, { total = total_keys, prefix = prefix, vsize = vsize })

    mcs.add(ts, { func = "timer", clients = 1, rate_limit = 5 })
    mcs.add(ts, { func = "statsample", clients = 1, rate_limit = 1, rate_period = 1000 })

    table.insert(threads, ts)
    mcs.shredder(threads, 30)
    print("done")
end

local counter = 0
function warm()
    local req = mcs.set(prefix, counter, 0, 9999, vsize)
    mcs.write(req)
    mcs.flush()
    local res = mcs.read()
    counter = counter + 1
end

function basic(a)
    local total = a.total
    local pfx = a.prefix
    local size = a.vsize
    local req = mcs.mg_factory(pfx, "v")

    return function()
        local num = math.random(total)
        mcs.write_factory(req, num)
        mcs.flush()

        local res = mcs.read()
--[[       local rline = mcs.resline(res)
        if rline == "EN" then
            local set = mcs.set(prefix, num, 0, 30, size)
            mcs.write(set)
            mcs.flush()
            local res = mcs.read()
        end
--]]
    end
end

function basic_noinit(a)
    local total = a.total
    local pfx = a.prefix
    local size = a.vsize

    local num = math.random(total)
    local req = mcs.mg(a.prefix, num)
    mcs.write(req)
    mcs.flush()

    local res = mcs.read()
--[[
    local rline = mcs.resline(res)
    if rline == "EN" then
        local set = mcs.set(a.prefix, num, 0, 30, a.vsize)
        mcs.write(set)
        mcs.flush()
        local res = mcs.read()
    end
--]]
end

function timer()
    local num = math.random(total_keys)
    local req = mcs.get(prefix, num)
    mcs.write(req)
    mcs.flush()

    local res = mcs.read()
    local status, elapsed = mcs.match(req, res)
    print("elapsed response: " .. elapsed)

    if mcs.resline(res) == "END" then
        local set = mcs.set(prefix, num, 0, 30, vsize)
        mcs.write(set)
        mcs.flush()
        local res = mcs.read()
    else
        -- pull the END
        local res = mcs.read()
    end
end

local previous_stats = {}
local stats_ready = false
local track_stats = { "cmd_get", "cmd_set" }
function statsample()
    mcs.write("stats\r\n")
    mcs.flush()
    local stats = {}
    while true do
        local res = mcs.read()
        if mcs.resline(res) == "END" then
            break
        end
        stats[mcs.res_statname(res)] = mcs.res_stat(res)
    end

    if stats_ready then
        for _, s in pairs(track_stats) do
            local count = stats[s] - previous_stats[s]
            print(s, ": ", count)
        end
    end

    previous_stats = stats
    stats_ready = true
end


