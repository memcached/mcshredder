function config()
    local t = mcs.thread()

    mcs.add_custom(t, { func = "stopper" })

    mcs.shredder({t}, 30)
end

function stopper()
    while true do
        mcs.stop()
        mcs.sleep_millis(5000)
    end
end
