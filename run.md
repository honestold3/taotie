cd /Users/wq/zed_prj/taotie

# 1) 启动（新终端不用 cc-system；本会话/旧终端先执行 cc-system）
cargo run
# taotie 启动，监听 http://0.0.0.0:8080

# 2) 健康检查
curl http://127.0.0.1:8080/healthz          # ok

# 3) 用真实示例上报（换成你自己的 base64）
curl "http://127.0.0.1:8080/bigData/receiveGet?data=<base64>&ext=crc%3D1538298110"   # ok

# 4) 看 Kafka 落库
kcat -b localhost:9092 -t bury-track -C -o -1 -c 1 -f 'key=%k\n%s\n'

# 看最近一条完整内容
kcat -b localhost:9092 -t bury-track -C -o -1 -c 1 -f 'key=%k\n%s\n'

# 从头看全部（看完会退出）
kcat -b localhost:9092 -t bury-track -C -o beginning -e

至此整条链路（HTTP → 解码 → 校验 → Kafka）已经全部打通。

# 5) 看日志
tail -f logs/taotie.$(date +%F).log

几个注意点：

- config.toml 按当前目录的相对路径读取，务必在项目根目录启动，或设置 TAOTIE_CONFIG=/绝对路径/config.toml
- 测试进程我已全部停掉，8080 端口是空闲的
- 日志时间戳是 UTC，比本地慢 8 小时
- 想看更细的日志：RUST_LOG=debug cargo run


# 完全不带参数
curl -i "http://127.0.0.1:8080/bigData/receiveGet"

# 只带 ext，不带 data
curl -w '\n[HTTP %{http_code}]\n' "http://127.0.0.1:8080/bigData/receiveGet?ext=crc%3D1"

# 压力测试
cd /Users/wq/zed_prj/taotie
./bench/run.sh

默认参数：4 线程、200 并发、跑 30 秒。脚本会自动完成：启动独立压测实例（8090 → bury-track-bench）→ 生成/复用压测数据 → 压测 → 打印服务端指标 → 停止压测实例。

常用变体

# 自定义并发/时长
THREADS=8 CONNECTIONS=500 DURATION=60s ./bench/run.sh

# 小规模快速验证（几秒钟出结果）
THREADS=2 CONNECTIONS=20 DURATION=5s ./bench/run.sh

# 压测已有实例（不自动启停，默认目标是 8080）
AUTO_START=0 ./bench/run.sh
AUTO_START=0 BASE_URL=http://10.0.0.10:8080 ./bench/run.sh

# 换压测 topic / 端口
BENCH_TOPIC=bench2 BENCH_PORT=8091 ./bench/run.sh

执行前确认

1. Kafka 在跑：kcat -b localhost:9092 -L | head
2. 二进制已构建：cargo build（脚本发现没有二进制会自动构建；本机构建前先 cc-system）
3. wrk 已安装：✅ 你的机器上已有
4. 高并发前调大句柄数：ulimit -n 65535（否则 500+ 并发容易报 too many open files）

输出里看什么

Requests/sec:   2352.36          ← 客户端吞吐
Latency Distribution
   50%    7.79ms                 ← 延迟分位
   99%   56.45ms
== 服务端指标增量 ==
200 成功: 11865
400 拒绝: 0
503 失败: 0                       ← 出现就说明 Kafka 或队列顶不住了
压测 topic bury-track-bench 消息总数: 11865

重点关注：P99 是否陡增、503 是否出现、服务端吞吐与 wrk 是否接近（差太多说明有请求没结算）。

压测实例的日志在 logs/taotie-bench.*.log，业务实例（8080）完全不受影响，可以同时跑着。
