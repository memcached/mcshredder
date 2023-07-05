function config(a)
    local t = mcs.thread()
    --mcs.add_custom(t, { func = "test" }, { opt = "foo" })
    mcs.add_custom(t, { func = "watch" })
    mcs.shredder({t}, 30)
end

function test(a)
    print("received option:", a.opt)
    mcs.sleep_millis(1000)
    print("creating client")
    -- uses default host/port from commandline if omitted.
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

function watch(a)
    print("creating client")
    local c = mcs.client_new({ host = "127.0.0.1", port = "11212"})
    mcs.client_connect(c)
    mcs.client_write(c, "watch fetchers\r\n")
    mcs.client_flush(c)
    while true do
        local rline = mcs.client_readline(c)
        print("read line:", rline)
    end
end
