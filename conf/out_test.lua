function config()
    local t = mcs.thread()
    local tout = mcs.thread()

    -- test sending the messages across threads.
    mcs.add_custom(t, { func = "sender" })
    mcs.add_custom(tout, { func = "receiver" })

    mcs.shredder({t, tout}, 30)
end

function sender()
    while true do
        local now = mcs.time_millis()
        mcs.out("tick:", now)
        mcs.sleep_millis(1000)
    end
end

function receiver()
    while true do
        -- sleeps until something is written to the out stream.
        mcs.out_wait()

        while true do
            local line = mcs.out_readline()
            if line == nil then
                break
            end
            print("received:", line)
        end
    end
end
