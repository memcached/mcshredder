function config(a)
    local t = mcs.thread()
    mcs.add_custom(t, { func = "stats" }, { stats = { "cmd_get", "cmd_set" }, track = { "uptime" } })
    mcs.shredder({t}, 30)
end

function stats(a)
    local stats = {}
    local res = mcs.res_new()
    local previous_stats = {}

    local c = mcs.client_new({})
    mcs.client_connect(c)

    while true do
        mcs.client_write(c, "stats\r\n")
        mcs.client_flush(c)

        while true do
            mcs.client_read(c, res)
            -- testing resline here conditionally.
            --local rline = mcs.resline(res)
            --print("RESLINE:", rline)
            if mcs.res_startswith(res, "END") then
                break
            end
            stats[mcs.res_statname(res)] = mcs.res_stat(res)
        end

        for _, s in pairs(a["stats"]) do
            if previous_stats[s] ~= nil then
                local count = stats[s] - previous_stats[s]
                print("stat:", s, ": ", count)
            end
        end
        for _, s in pairs(a["track"]) do
            if stats[s] ~= nil then
                print("stat:", s, ": ", stats[s])
            end
        end
        
        previous_stats = stats
        stats = {}

        mcs.sleep_millis(1250)
    end
end
