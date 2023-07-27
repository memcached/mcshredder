-- TODO:
-- convert validator to use init function. already testing the mg factory with
-- it.

local o = {
    count = 5000000,
    prefix = "foo",
    vsize = 100,
}

function config(a)
    if a["prefix"] then
        print("overriding prefix")
        o.prefix = a.prefix
    end
    if a["vsize"] then
        print("overriding vsize: " .. a["vsize"])
        o.vsize = a.vsize
    end
    if a["count"] then
        print("overriding total_keys")
        o.count = a.count
    end
    print("starting pre-warm")
    local warm = mcs.thread()
    if a["validate"] then
        print("validating previous warm")
        mcs.add(warm, { func = "validate", clients = 1, limit = o.count, rate_limit = 0 }, o)
    else
        mcs.add_custom(warm, { func = "warm" }, o)
    end
    mcs.shredder({warm}, 0)
    print("warming completed")

    print("done")
end

local FLUSH_AFTER <const> = 50000000
function warm(a)
    local count = a.count
    local size = a.vsize
    local prefix = a.prefix
    local written = 0
    local c = mcs.client_new({})
    if c == nil then
        print("ERROR: warmer failed to connect to host")
        return
    end
    if mcs.client_connect(c) == false then
        print("ERROR: warmer failed to connect")
        return
    end

    local req = mcs.ms_factory(prefix, "q")

    for i=1,count do
        mcs.client_write_factory(c, req, i, size)
        written = written + size
        if written > FLUSH_AFTER then
            mcs.client_flush(c)
            written = 0
        end
    end

    mcs.client_write(c, "mn\r\n")
    mcs.client_flush(c)
    local res = mcs.res_new()
    -- TODO: bother validating MN? this doesn't fail here.
    mcs.client_read(c, res)
end

local vcounter = 0
function validate(a)
    if a["prefix"] then
        prefix = a["prefix"]
    end

    local num = vcounter
    local req = mcs.mg_factory(prefix, "v")
    mcs.write_factory(req, num)
    mcs.flush()

    vcounter = vcounter + 1
    local res = mcs.res_new()
    mcs.read(res)
    if mcs.res_startswith(res, "EN") then
        print("miss: " .. num)
    end
    local status, elapsed = mcs.match(req, res)
    if not status then
        print("result did not match: " .. num)
    end
end
