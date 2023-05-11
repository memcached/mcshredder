-- First the "config()" function is called,
-- which configures thread objects, attaches workloads to thread objects, then
-- runs the test.
-- arguments can be passed from the command line via:
-- --args warm,rate=5000
function config(args)
    -- example of calling mcs.shredder() multiple times to create a
    -- pre-warming function
    if args["warm"] ~= nil then
        print("starting pre-warm")
        local warm = mcs.thread()
        mcs.run(warm, { func = "warm", clients = 1, limit = 1000 })
        mcs.shredder({warm}, 30) -- wait up to 30 seconds for warmer to run
        print("warming completed")
    end

    local rate = 100
    if args["rate"] ~= nil then
        rate = args["rate"]
        print("overriding rate from command line: " .. rate)
    end

    print("starting test")
    -- Create a dedicated OS thread
    local t1 = mcs.thread()

    -- Attach a workload to this thread.
    -- NOTE:
    -- func must be a string, as each thread gets a unique lua VM, requiring
    -- an indirect calling convention.
    -- clients: number of concurrent client connections to run
    -- rate_limit: the total requests per second to target across all clients
    --             in this case 5 connections will run a total of 100 rps.
    -- rate_period: in milliseconds, the time period for the rate limit,
    -- default 1000 (one second)
    -- reconn_every: force the client to reconnect every N requests.
    -- limit: number of times to run each function for each client
    mcs.add(t1, { func = "metaget", clients = 5, rate_limit = rate })
    -- Multiple workloads can run on the same OS thread.
    -- mcs.add(t1, { func = "toast", clients = 5 })
    -- Multiple threads may be passed in at once. The client count and rate
    -- will be divided across the threads
    -- mcs.add({t1, t2, t3}, { etc })
    -- arguments may be passed to functions as well:
    -- mcs.add(t1, { options }, "argument")
    -- mcs.add(t1, { options }, 5)
    -- mcs.add(t1, { options }, { key = "value", more = 5 })
    -- If used, will be passed in as the sole argument to the function
    -- Do not use complex data structures for arguments! Will hurt speed

    -- Optionally, we can create more threads in order to scale workloads.
    local t2 = mcs.thread()
    -- mcs.run(t2, { func = "basic", clients = 25, rate_limit = 10000 })
    -- watch stats output from another thread
    mcs.add(t2, { func = "statsample", clients = 1, rate_limit = 1, period = 2000 })

    -- Run the test for 10 seconds.
    -- If no argument passed, wait for a kill or stop signal.
    mcs.shredder({t1}, 10)
    print("done")
end

function help()
    print("extra argument explanations here")
end

-- we use a global counter here, note that any global variable will be local
-- to each OS thread created, as each OS thread uses an independent lua VM
-- loading this same code.
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

function argumented(arg)
    -- arg can be string, number, or table
    -- note that the argument is shared with all functions created at the same
    -- time, so do not modify the argument if it is a table!
end

function basic()
    local num = math.random(20)
    -- create a request object with the request string inside.
    local req = mcs.get("toast/", num)
    -- write the request to the client write buffer
    mcs.write(req)
    -- flush the request out to the network
    -- this suspends and later resumes this coroutine, allowing other
    -- clients to run on the same OS thread concurrently.
    mcs.flush()

    -- wait for a response and parse it into a response object.
    local res = mcs.read()
    -- NOTE: a response object is only valid until the next time mcs.read() is
    -- called: res points directly into the client read buffer, which moves
    -- every time read() is called.
    --
    -- check if we had a miss.
    if mcs.resline(res) == "END" then
        -- similar to above, but create a set backfill
        local set = mcs.set("toast/", num, 0, 90, 100)
        mcs.write(set)
        mcs.flush()
        local res = mcs.read()
        -- note validating the response here is optional.
    else
        -- If we got a hit, we need to still read the END marker.
        local res = mcs.read()
        -- note we're not validating the END marker here.
    end
end

function metaget()
    local num = math.random(20)
    local req = mcs.mg("toast/", num, "v")
    mcs.write(req)
    mcs.flush()
    local res = mcs.read()
    if mcs.resline(res) == "EN" then
        local set = mcs.ms("toast/", num, 50, "T90")
        mcs.write(set)
        mcs.flush()
        local res = mcs.read()
    else
        local status, elapsed = mcs.match(req, res)
        if not status then
            print("Result did not match request!")
        end
    end
end

-- example latency sampler.
-- could get from main corpus of data, individually test timing of set/get
-- routine, etc. run with a low rate limit to get trend data.
function latency()
    local req = mcs.set("canary", 0, 0, 10, 100)
    mcs.write(req)
    mcs.flush()

    local res = mcs.read()
    -- NOTE: as of writing "status" is meaningless.
    -- mcs.match(req, res) will do basic validation that the response makes
    -- sense for the request.
    local status, elapsed = mcs.match(req, res)
    print("sample time: " .. elapsed)
end

-- print some stats output periodically.
-- controlled by the rate/period settings from mcs.run()
--
-- I'm building towards having internal feedback loops: if the request rate is
-- less than expected, stop the test or print warnings, etc.
--
-- If you are building sets of tests, you should create libraries with code
-- like below and import them via require or dofile rather than copy/paste.
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


