-- First the "config()" function is called,
-- which configures thread objects, attaches workloads to thread objects, then
-- runs the test.
function config()
    -- example of calling mcs.shredder() multiple times to create a
    -- pre-warming function
    print("starting pre-warm")
    local warm = mcs.thread()
    mcs.run(warm, { func = "warm", clients = 1, limit = 1000 })
    mcs.shredder({warm}, 30) -- wait up to 30 seconds for warmer to run
    print("warming completed")

    print("starting test")
    -- Create a dedicated POSIX thread
    local t1 = mcs.thread()
    -- Attach a workload to this thread.
    -- NOTE:
    -- func must be a "string", as each thread gets a unique lua VM, requiring
    -- an indirect calling convention.
    -- clients: number of concurrent client connections to run
    -- rate_limit: the total requests per second to target across all clients
    --             in this case 5 connections will run a total of 100 rps.
    -- rate_period: in milliseconds, the time period for the rate limit,
    -- default 1000 (one second)
    -- reconn_every: force the client to reconnect every N requests.
    -- limit: number of times to run each function for each connection
    mcs.run(t1, { func = "metaget", clients = 5, rate_limit = 100 })
    -- Multiple workloads can run on the same thread.
    -- mcs.run(t1, { func = "toast", clients = 5 })

    -- Optionally, we can create more threads in order to scale workloads.
    -- local t2 = mcs.thread()
    -- mcs.run(t2, { func = "basic", clients = 25, rate_limit = 10000 })

    -- Run the test for 10 seconds.
    -- If no argument passed, wait for a kill or stop signal.
    mcs.shredder({t1}, 10)
    print("done")
end

local counter = 0
-- another way to do this: set "limit" in mcs.run() to 1 and loop inside the
-- warming function.
function warm()
    local req = mcs.set("doot", counter, 0, 300, 50)
    mcs.write(req)
    mcs.flush()
    local res = mcs.read()
    counter = counter + 1
end

function basic()
    local num = math.random(20)
    local req = mcs.get("toast/", num)
    mcs.write(req)
    mcs.flush()
    local res = mcs.read()
    if mcs.resline(res) == "END\r\n" then
        local set = mcs.set("toast/", num, 0, 90, 100)
        mcs.write(set)
        mcs.flush()
        local res = mcs.read()
    else
        -- If we got a hit, we need to still read the END marker.
        local res = mcs.read()
    end
end

function metaget()
    local num = math.random(20)
    local req = mcs.mg("toast/", num, "v")
    mcs.write(req)
    mcs.flush()
    local res = mcs.read()
    if mcs.resline(res) == "EN\r\n" then
        local set = mcs.ms("toast/", num, "T90", 50)
        mcs.write(set)
        mcs.flush()
        local res = mcs.read()
    end
end


