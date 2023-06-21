function config(a)
    local t = mcs.thread()
    mcs.add_custom(t, { func = "test" })
    mcs.shredder({t}, 30)
end

-- TODO: arg passing.
function test(a)
    print("creating client")
    local c = mcs.client_new({ host = "127.0.0.1", port = "11212"})
    mcs.client_connect(c)
    while true do
        print("writing get")
        mcs.client_write(c, "get foo\r\n")
        print("calling flush")
        mcs.client_flush(c)
        print("calling read")
        local res = mcs.client_read(c)
        print("completed read")
        local rline = mcs.resline(res)
        print("read line:", rline)
    end
end
