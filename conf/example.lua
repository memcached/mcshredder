-- First the "config()" function is called,
-- which configures thread objects, attaches workloads to thread objects, then
-- runs the test.
function config()
    print("starting test")
    -- Create a dedicated POSIX thread
    local t1 = mcs.thread()
    -- Attach a workload to this thread.
    -- NOTE:
    -- func must be a "string", as each thread gets a unique lua VM, requiring
    -- an indirect calling convention.
    -- conns: number of independent client connections to run
    -- rate_limit: the total requests per second to target across all conns
    --             in this case 5 connections will run a total of 100 rps.
    -- rate_period: in milliseconds, the time period for the rate limit,
    -- default 1000 (one second)
    mcs.run(t1, { func = "metaget", conns = 5, rate_limit = 100 })
    -- Multiple workloads can run on the same thread.
    -- mcs.run(t1, { func = "toast", conns = 5 })

    -- Optionally, we can create more threads in order to scale workloads.
    -- local t2 = mcs.thread()
    -- mcs.run(t2, { func = "basic", conns = 25, rate_limit = 10000 })

    -- Run the test for 10 seconds.
    -- If no argument passed, wait for a kill or stop signal.
    mcs.shredder(10)
    print("done")
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


