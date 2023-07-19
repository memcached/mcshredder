-- This is a simple file for loading some stress testing data for dumpload to
-- copy to another host.
-- We only care about varying what dumpload actually sees:
-- * the key length
-- * TTL's of 0 or non-zero with a few different lengths
-- * client flags either absent or several different lengths
-- * value length
-- * value content doesn't matter since it's not examined.

function config(a)
    if a["prefix"] then
        print("overriding prefix")
    else
        a.prefix = "foo"
    end
    if a["count"] then
        print("overriding total_keys")
        total_keys = a["count"]
    else
        a.count = 100000
    end

    print("starting pre-warm")
    local warm = mcs.thread()
    mcs.add(warm, { func = "warm", clients = 1, limit = 1}, a)
    mcs.shredder({warm}, 0)
    print("warming completed")
end

function warm(a)
    local count = a.count
    local prefix = a.prefix
    local flusher = 0
    local res = mcs.res_new()

    for i=0,count do
        local rand_cflags = math.random(50000)
        local cflags = ""
        if rand_cflags < 25000 then
            cflags = "F" .. tostring(rand_cflags)
        end

        local rand_ttl = math.random(50000)
        local ttl = "T0"
        if rand_ttl < 25000 then
            ttl = "T" .. tostring(rand_ttl)
        end

        -- not valuable to test with huge values; it can copy bytes fine, but
        -- we want to vary other things.
        local rand_size = math.random(10,1000)
        local key_extra = string.rep("extra", math.random(1,10))
        local req = mcs.ms(prefix .. key_extra, i, rand_size, "q", cflags, ttl)
        mcs.write(req)
        flusher = flusher + 1
        if flusher > 50 then
            mcs.flush()
            flusher = 0
        end
    end
    -- ensure final commands were written
    mcs.write("mn\r\n")
    mcs.flush()
    mcs.read(res)
end
