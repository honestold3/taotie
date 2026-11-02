# Taotie 压测指南

本文档说明如何用 `wrk` 对埋点网关做压测，包括编译、执行、参数配置、结果解读与清理。

## 1. 依赖

| 工具 | 用途 | 安装 |
|---|---|---|
| `wrk` | HTTP 压测 | `brew install wrk` |
| `python3` | 生成压测数据、脚本辅助 | 系统自带 |
| `kafka-topics.sh` / `kcat` | 创建/清空 topic、核对消息数 | 可选（脚本会降级处理） |

## 2. 编译 release

**压测必须使用 release 构建**，debug 版本吞吐约为 release 的一半、延迟翻倍。

```sh
cd /Users/wq/zed_prj/taotie
cc-system              # macOS 本机需要；新终端可不执行
cargo build --release
ls -l target/release/taotie
```

`bench/run.sh` 会优先使用 `target/release/taotie`，不存在时才回退 debug。
**注意脚本不会自动重新编译**，改过代码后需要手动重新 `cargo build --release`。

## 3. 快速开始

```sh
# 默认：4 线程 / 200 并发 / 30 秒
./bench/run.sh

# 高并发（先调大文件句柄数）
ulimit -n 65535
THREADS=8 CONNECTIONS=1000 DURATION=30s ./bench/run.sh
```

脚本会自动：

1. 在 **8090** 端口启动一个独立压测实例，写入 **`bury-track-bench`**
2. **清空该 topic**（删除→等待彻底消失→重建），并打印清空后的 offset（应为 0）
3. 用 wrk 发起压测（每条请求轮换不同 `distinct_id`）
4. 等待 `taotie_requests_inflight` 归零、请求数稳定后，对比 `/metrics` 增量与 topic 消息总数
5. SIGTERM 停止压测实例（优雅退出、队列刷盘）

业务实例（8080 → `bury-track`）全程不受影响。

## 4. 参数

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `THREADS` | 4 | wrk 线程数 |
| `CONNECTIONS` | 200 | 并发连接数 |
| `DURATION` | 30s | 压测时长 |
| `PAYLOADS` | `bench/payloads.txt` | 压测数据文件（首次自动生成 1000 条） |
| `AUTO_START` | 1 | 是否自动启停独立压测实例 |
| `BENCH_PORT` | 8090 | 压测实例端口 |
| `BENCH_TOPIC` | bury-track-bench | 压测写入的 topic |
| `BROKERS` | localhost:9092 | 压测实例连接的 Kafka |
| `BASE_URL` | `http://127.0.0.1:8090` | 压测目标地址 |
| `CLEAR_TOPIC` | 1 | 每次压测前清空 topic |
| `CLEAR_TOPIC_FORCE` | 0 | topic 名不含 `bench` 时默认拒绝清空，设为 1 强制 |
| `BENCH_EXT` | `&ext=crc%3D1538298110` | 请求 query 后缀；设为空字符串可压无 ext 路径 |

示例：

```sh
# 500 并发 / 60 秒
THREADS=8 CONNECTIONS=500 DURATION=60s ./bench/run.sh

# 压一个更大的唯一数据集（避免 payload 循环）
python3 bench/gen_payloads.py 100000 /tmp/payloads-100k.txt
PAYLOADS=/tmp/payloads-100k.txt THREADS=4 CONNECTIONS=200 DURATION=30s ./bench/run.sh

# 压已有实例（此时数据写入该实例配置的 topic，脚本不会清空）
AUTO_START=0 BASE_URL=http://10.0.0.10:8080 ./bench/run.sh
```

> `AUTO_START=0` 时目标实例写的是它自己 `config.toml` 里的 topic（通常是 `bury-track`），
> 压测数据会混进业务 topic，请谨慎使用。

## 5. 输出解读

```text
Requests/sec:  84,981.00          ← wrk 客户端统计（收到响应的请求数）
Latency
  50%   10.78ms
  99%   26.69ms
== 服务端指标增量 ==
200 成功: 2,558,884               ← /metrics 统计（服务端处理完的请求）
503 失败: 0
服务端吞吐: 85,296 req/s
压测 topic bury-track-bench 消息总数: 2,558,915
```

| 数字 | 含义 |
|---|---|
| 客户端 QPS | wrk 收到完整响应的请求数；压测结束时连接上的在途请求不计入 |
| 服务端 QPS | `/metrics` 中 `taotie_requests_total` 的增量 |
| topic 消息数 | Kafka 实际落盘的消息数 |

正常情况下三者应基本一致：

- 客户端比服务端少几百条是正常的（停止瞬间的在途请求）
- 服务端与 topic 应**精确相等**（脚本会等 `taotie_requests_inflight = 0` 后再对账）
- 若 `topic > metrics`：可能是生产者重试产生的重复消息（`acks=1` 且未开启幂等，属 at-least-once）
- 若 `topic < metrics`：**需要排查**，可能意味着消息未真正落盘

## 6. 参考数据

测试环境：Mac 10 核，Kafka 与网关同机，payload 约 450B，每次请求包含一次 `acks=1` 往返。

| 构建 | 并发 | 服务端 QPS | P50 | P99 | 503 |
|---|---|---|---|---|---|
| debug | 200 | 28,495 | 6.82ms | 9.77ms | 0 |
| debug | 500 | 43,532 | 10.35ms | 36.79ms | 0 |
| debug | 1000 | 46,098 | 19.92ms | 56.64ms | 0 |
| **release** | **1000** | **84,500 ~ 85,300** | **~10.8ms** | **~27ms** | **0** |

结论：

- 200 并发时压测端（wrk）先饱和，看不出服务端上限
- 500 → 1000 并发吞吐只涨 6%，延迟翻倍，已越过拐点
- 单机（压测端 + 网关 + Kafka 同机）稳定吞吐约 8.5 万/秒；生产环境 Kafka 独立部署时上限更高
- 短时突刺（3~5 秒）会因 Kafka 页缓存而显著高于持续吞吐，不代表真实容量

## 7. 压测后清理

```sh
KB=/Users/wq/opt/kafka_2.12-3.5.1/bin
$KB/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic bury-track-bench
```

下次压测脚本会自动重建该 topic 并先清空。

## 8. 常见问题

| 现象 | 原因 / 处理 |
|---|---|
| QPS 只有预期一半 | 用的是 debug 构建；执行 `cargo build --release` |
| 报 `Too many open files` | `ulimit -n 65535` |
| 出现 503 | Kafka 不可达、topic 不存在或本地队列满；看 `logs/taotie-bench.*.log` |
| 对账差几条 | 统计窗口；脚本已等待在途归零，仍差少量多为生产者重试 |
| 清空后 offset 不是 0 | 删除尚未完成就重建；脚本已加入等待，必要时换个 `BENCH_TOPIC` |
| `Socket errors: read ...` | 压测端连接数过高，客户端吃紧；降低并发或换独立压测机 |
| 客户端 QPS 低于服务端很多 | 正常，停止瞬间在途请求未计入；差距过大时检查 wrk 是否成为瓶颈 |





手动执行
topic是 bury-track-bench，不是 bury-track。默认压测会启动一个独立实例（8080 的业务服务完全不受影响）。

1. 编译 release

cd /Users/wq/zed_prj/taotie
cc-system              # 新终端已不需要；保险起见可执行
cargo build --release
ls -l target/release/taotie    # 确认产物存在

bench/run.sh 会优先使用 target/release/taotie，只有不存在时才回退 debug。注意它不会自动重新编译，改过代码后要自己 cargo build --release。

2. 跑压测

# 默认：4 线程 / 200 并发 / 30 秒
./bench/run.sh

# 高并发（先调大句柄数）
ulimit -n 65535
THREADS=8 CONNECTIONS=1000 DURATION=30s ./bench/run.sh

脚本自动完成：

1. 在 8090 端口启动独立实例，写入 bury-track-bench（不存在则创建）
2. 每次压测前清空该 topic（并打印清空后 offset，应为 0）
3. wrk 压测 → 等待在途请求归零 → 对比 /metrics 与 topic 消息数
4. SIGTERM 停掉压测实例（优雅退出）

3. 如果要压真实业务实例

AUTO_START=0 BASE_URL=http://127.0.0.1:8080 ./bench/run.sh

这时数据会写进 bury-track，脚本不会清空它（清空只针对自动启动的压测实例）。压完要清理只能手动删 topic 重建。

4. 压完清理（可选）

/Users/wq/opt/kafka_2.12-3.5.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic bury-track-bench

下次再跑脚本会自动重建，也会先清空。

预期结果（release / 1000 并发 / 30 秒）

Requests/sec:  约 84,000~85,000
P50 约 11ms   P99 约 27ms
200 成功 ≈ topic 消息总数
503 失败: 0
