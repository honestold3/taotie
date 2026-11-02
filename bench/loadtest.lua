-- wrk 压测脚本：每条请求轮换不同的埋点数据（避免重复同一 payload）
--
-- 用法:
--   wrk -t4 -c200 -d30s --latency -s bench/loadtest.lua \
--       "http://127.0.0.1:8080/bigData/receiveGet" -- bench/payloads.txt

local payloads = {}
local idx = 0
local suffix = "&ext=crc%3D1538298110"

function init(args)
    local path = (args and args[1]) or "bench/payloads.txt"
    local file = assert(io.open(path, "r"), "无法打开 payload 文件: " .. path)
    for line in file:lines() do
        line = line:gsub("%s+$", "")
        if #line > 0 then
            payloads[#payloads + 1] = line
        end
    end
    file:close()
    assert(#payloads > 0, "payload 文件为空: " .. path)
    if args and args[2] then
        suffix = args[2]   -- 第二个参数为 query 后缀，空字符串表示不带 ext
    end
end

function request()
    idx = (idx % #payloads) + 1
    return wrk.format(
        "GET",
        "/bigData/receiveGet?data=" .. payloads[idx] .. suffix
    )
end
