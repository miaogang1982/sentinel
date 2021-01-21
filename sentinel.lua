-- redis sentinel
-- author: miaogang
-- QQ: 233300787
-- deps: redis-lua@2.0.4, split@3.2.1

local redis = require("redis")
local split = require("split")

local _M = {
    _VERSION = '0.1'
}

-- @ host_string string eg: "127.0.0.1:26379;127.0.0.1:26380"
-- @ host_array table eg: { {"ip" = "127.0.0.1", "port" = "26379"}, {"ip" = "127.0.0.1", "port" = "26380"} }
function _M:convert_hosts(host_string)
    host_table = {}
    for param in split.each(host_string, '%s*;%s*') do
        local item = {}
        local k, v = split.first_and_rest(param, '%s*:%s*')
        item["ip"] = k
        item["port"] = v
        table.insert(host_table, item)
    end
    return host_table
end

-- @hosts table eg: { {"ip" = "127.0.0.1", "port" = "26379"}, {"ip" = "127.0.0.1", "port" = "26380"} }
-- @name string eg: mymaster
-- @return table eg: { "host": "127.0.0.1", "port" = "6279" }
function _M.master_for(self, hosts, name)
    -- loop connect to sentinel
    local master
    for _, v in pairs(hosts) do
        local client = redis.connect(v["ip"], v["port"])
        master = client:raw_cmd("SENTINEL GET-MASTER-ADDR-BY-NAME " .. name .. "\r\n")
    end
    if master ~= nil then
        return { ip = master[1], port = master[2] }
    end
    return nil
end

-- @hosts table eg: { {"ip" = "127.0.0.1", "port" = "26379"}, {"ip" = "127.0.0.1", "port" = "26380"} }
-- @name string eg: mymaster
-- @return table eg: { "host": "127.0.0.1", "port" = "6280" }
function _M.slave_for(self, hosts, name)
    -- loop connect to sentinel
    for _, v in pairs(hosts) do
        local client = redis.connect(v["ip"], v["port"])
        local slaves = client:raw_cmd("SENTINEL SLAVES " .. name .. "\r\n")
        if slaves and type(slaves) == "table" then
            for _, sv in ipairs(slaves) do
                local slave = {}
                for i = 1, #sv, 2 do
                    slave[sv[i]] = sv[i + 1]
                end
                local master_link_status_ok = slave["master-link-status"] == "ok"
                local is_down = slave["flags"] and (string.find(slave["flags"], "s_down") or string.find(slave["flags"], "disconnected"))
                if not is_down then
                    return { ip = slave["ip"], port = slave["port"] }
                end
            end
        end
    end
    return nil
end

return _M

