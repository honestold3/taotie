# Taotie（饕餮）

埋点接收网关。兼容原有 SpringBoot 服务 `https://bigdata-up.bjx.com.cn/bigData/receiveGet`，
接收前端 js-sdk 通过 HTTP GET 上报的 base64 埋点数据，解码、校验后投递到 Kafka。

前端埋点**无需修改任何上报地址**，直接切流量即可。

## 接口

```
GET /bigData/receiveGet?data=<base64>&ext=<可选>
```

| 参数 | 必填 | 说明 |
|---|---|---|
| `data` | 是 | 埋点原始 JSON 的标准 base64 编码 |
| `ext` | 否 | 附加参数，原样透传，如 `ext=crc%3D1538298110` |

**处理流程**

1. 解析 query 参数 `data`、`ext`（按 Servlet 语义解码：只解 `%XX`，`+` 不转空格）
2. 标准 base64 解码 → UTF-8 字符串
3. `serde_json` 反序列化为 `BuryEvent`
4. `garde` 校验字段
5. 把 `ext` 作为字段附加进埋点 JSON（原有字段不动）
6. 投递到 Kafka（等待 broker ack）
7. 返回结果

**响应**

| 场景 | 状态码 | 响应体 |
|---|---|---|
| 成功 | 200 | `ok` |
| 缺少 `data` | 400 | `缺少参数 data` |
| base64 非法 | 400 | `data 不是合法的 base64` |
| 非 UTF-8 | 400 | `data 解码后不是合法的 UTF-8 字符串` |
| 非 JSON | 400 | `data 解码后不是合法的 JSON` |
| 字段缺失/类型错误 | 400 | `埋点数据缺少必要字段或字段类型错误` |
| 字段校验失败 | 400 | `埋点字段校验失败: distinct_id: length is lower than 1` |
| Kafka 投递失败 | 503 | `kafka 投递失败: ...` |

返回 503 时前端 SDK 会按原有重试策略重试，不会静默丢数据。

## 健康检查

| 路径 | 含义 | 典型用途 |
|---|---|---|
| `GET /healthz` | 进程存活即返回 `200 ok`，**不探测 Kafka** | livenessProbe |
| `GET /readyz` | 探测 Kafka 与目标 topic 是否可用（500ms 超时），不可用时返回 503 | readinessProbe |

`/healthz` 不探测 Kafka，避免 Kafka 抖动导致实例被反复重启；流量摘除交给 `/readyz`。

## 监控指标

```
GET /metrics
```

Prometheus 文本格式（`/healthz`、`/metrics` 自身不计入）：

| 指标 | 类型 | 说明 |
|---|---|---|
| `taotie_requests_total{status}` | counter | 按状态码 200 / 400 / 503 统计的请求数 |
| `taotie_request_duration_seconds{status}` | histogram | 处理耗时（秒），含等待 Kafka ack |
| `taotie_requests_inflight` | gauge | 当前处理中的请求数（用于压测对账、观察并发压力） |

## 数据校验规则

```json
{
  "identities": { "...": "对象" },
  "distinct_id": "非空字符串",
  "lib": { "...": "对象" },
  "properties": { "...": "对象" },
  "anonymous_id": "非空字符串",
  "type": "非空字符串",
  "event": "非空字符串",
  "time": 1787725281606,
  "_track_id": 737861607,
  "_flush_time": 1787725281607
}
```

- `distinct_id`、`anonymous_id`、`type`、`event` 不能为空
- `identities` / `lib` / `properties` 只要求是 JSON 对象，不校验内部字段
- 未知字段会被**完整保留**并原样透传，不会因 SDK 升级丢数据

## Kafka 消息

| 项 | 值 |
|---|---|
| topic | `bury-track`（可配置） |
| key | `track`（可配置） |
| value | 原始埋点 JSON + 追加的 `ext` 字段 |

示例：

```json
{"identities":{...},"distinct_id":"...","lib":{...},"properties":{...},"anonymous_id":"...","type":"track","event":"$pageview","time":1787725281606,"_track_id":737861607,"_flush_time":1787725281607,"ext":"crc=1538298110"}
```

`ext` 未传时不会出现该字段。

## 配置

默认读取项目根目录 `config.toml`，可用环境变量 `TAOTIE_CONFIG` 指定其他路径。
文件不存在时使用内置默认值。

```toml
listen_addr = "0.0.0.0:8080"

[kafka]
brokers = "localhost:9092"
topic = "bury-track"
key = "track"
acks = "1"
queue_buffering_ms = 5      # queue.buffering.max.ms
compression = "lz4"         # 消息压缩：lz4 / snappy / zstd / gzip / none
send_timeout_ms = 3000      # 单条消息最长投递时间，超时返回 503
queue_timeout_ms = 100      # 本地队列满等待时间，超时返回 503
shutdown_flush_ms = 5000    # 优雅退出时等待 Kafka 队列刷盘的最长时间

[log]
level = "warn"              # trace / debug / info / warn / error，可用 RUST_LOG 覆盖
dir = "logs"
file_prefix = "taotie"
file = true                 # K8s 等只采集 stdout 的环境设为 false
```

## 运行

```sh
cargo run
# taotie 启动，监听 http://0.0.0.0:8080（brokers=localhost:9092 topic=bury-track）
```

> 这一行由 `println!` 输出，**不受日志级别影响**，`level = "warn"` 时也能看到。

测试：

```sh
curl "http://127.0.0.1:8080/healthz"
# ok

curl "http://127.0.0.1:8080/bigData/receiveGet?data=<base64>&ext=crc%3D1538298110"
# ok

kcat -b localhost:9092 -t bury-track -C -o beginning -e
```

### 构建环境

- Rust 1.99+（edition 2024）
- `cmake`、C 编译器（`librdkafka` 通过 `rdkafka` 的 `cmake-build` 特性内置编译，**无需**单独安装 librdkafka）
- Linux 还需 curl 头文件（Debian/Ubuntu：`libcurl4-openssl-dev`）。librdkafka 2.12.1 的
  `rdkafka_conf.c` 用 `#ifdef` 判断 `WITH_OAUTHBEARER_OIDC`，而 `config.h` 总会把它定义成 0，
  因此会无条件包含 `<curl/curl.h>`；即使 `WITH_CURL=0` 也需要头文件（不会链接 curl）。
  macOS 的 SDK 自带该头文件，无需额外安装。
- 不依赖 OpenSSL：当前为 PLAINTEXT 连接，`rdkafka` 未启用 `ssl` feature

"> macOS 提示：本机 `.zshrc` 若仍有失效的 `CC`/`CXX`，构建前先执行 `cc-system` 切换到系统 clang。
>
> **构建报 OpenSSL 符号错误**（如 `_BIO_eof`、`_OPENSSL_sk_set_cmp_thunks`）时：
> 本机同时存在 `openssl@3`（keg-only）和 `openssl@4`（占用 `/opt/homebrew` 默认 prefix），
> 若历史构建启过 `ssl` feature，CMake 会复用旧的 `WITH_SSL=ON` 缓存，导致 4.x 头文件配 3.x 库。
> 执行 `cargo clean -p rdkafka-sys` 后重新构建即可（当前版本无 SSL 需求，不会编译 OpenSSL）。
> 更彻底的规避方式：在 `~/.cargo/config.toml` 中设置 `[env] OPENSSL_ROOT_DIR = "/opt/homebrew/opt/openssl@3"`（本机已配置），
> 确保即使启用 SSL，头文件和库也统一用 openssl@3。

## 目录结构

```
taotie/
├── Cargo.toml
├── config.toml
├── Dockerfile
├── deploy/
│   └── k8s.yaml
├── bench/           # 压测脚本（wrk + Lua）
│   ├── run.sh
│   ├── loadtest.lua
│   └── gen_payloads.py
├── src/
│   ├── main.rs      # 服务启动、日志、路由组装
│   ├── config.rs    # 配置加载
│   ├── handler.rs   # 接收、解码、校验、组装
│   ├── kafka.rs     # Kafka 生产者封装
│   └── metrics.rs   # Prometheus 指标
├── README.md
├── DEPLOY.md
└── LOADTEST.md
```

## 优雅退出

收到 `SIGTERM`（`kill` / `systemctl stop`）或 `SIGINT`（Ctrl+C）后：

1. 立即停止接受新连接
2. 等待在途请求处理完成
3. 把 Kafka 本地队列中未发送的消息刷给 broker（最长等待 `shutdown_flush_ms`）
4. 正常退出

## 压测

基于 `wrk`。默认**自动拉起一个独立的压测实例**（端口 8090、topic `bury-track-bench`），
压测结束后自动优雅退出，业务实例和 `bury-track` 完全不受影响。

> 完整说明（编译、参数、结果解读、常见问题）见 [LOADTEST.md](LOADTEST.md)。

```sh
# 依赖：brew install wrk
./bench/run.sh                                  # 4 线程 / 200 并发 / 30 秒
THREADS=8 CONNECTIONS=500 DURATION=60s ./bench/run.sh

# 压测已有实例（不自动启停，请自行确认该实例写入的 topic）
AUTO_START=0 BASE_URL=http://10.0.0.10:8080 ./bench/run.sh
```

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `THREADS` / `CONNECTIONS` / `DURATION` | 4 / 200 / 30s | wrk 参数 |
| `AUTO_START` | 1 | 是否自动启停独立压测实例 |
| `BENCH_PORT` | 8090 | 压测实例端口 |
| `BENCH_TOPIC` | bury-track-bench | 压测写入的 topic |
| `BROKERS` | localhost:9092 | 压测实例连接的 Kafka |
| `BASE_URL` | `http://127.0.0.1:8090` | 压测目标地址 |
| `CLEAR_TOPIC` | 1 | 每次压测前删除并重建 topic（数据清零） |
| `CLEAR_TOPIC_FORCE` | 0 | topic 名不含 `bench` 时默认拒绝清空，设为 1 强制 |
| `BENCH_EXT` | `&ext=crc%3D1538298110` | 请求的 query 后缀；设为空字符串可压测无 ext 路径 |

首次运行会自动生成 1000 条压测数据到 `bench/payloads.txt`（已 gitignore）。
每次压测前会**清空压测 topic**（删除→等待彻底消失→重建，并打印清空后的 offset，正常应为 0）。
压测结束后脚本会等待 `taotie_requests_inflight` 归零且请求数稳定，再对比 `/metrics` 增量与压测 topic 的消息总数，两者可精确对账。
压测实例的日志写入 `logs/taotie-bench.*.log`，级别为 `warn`（避免日志 IO 影响结果）。

本机参考数据（1000 并发 / 30 秒 / 约 450B 埋点数据 / Kafka 同机 / 10 核）：

| 构建 | 客户端 QPS | 服务端 QPS | P50 | P99 | 503 |
|---|---|---|---|---|---|
| debug | 46,615 | 46,098 | 21.98ms | 56.64ms | 0 |
| **release** | **84,981** | **85,296** | **10.78ms** | **26.69ms** | **0** |

> 生产必须使用 `cargo build --release`；debug 构建约为 release 的一半吞吐、两倍延迟。

> 每条请求都包含一次 Kafka `acks=1` 往返，吞吐/延迟直接反映端到端能力。
> 高并发前先 `ulimit -n 65535`；Kafka 与网关同机会争抢 CPU/磁盘，生产前建议在独立环境复测。

## 日志

- 控制台 + 文件双输出，文件按天滚动：`logs/taotie.YYYY-MM-DD.log`
- 成功上报 `INFO`（含字节数、耗时），参数/校验失败 `WARN`（含原因），Kafka 失败 `ERROR`（含 `distinct_id`、`event`，便于定位具体用户与事件）
- 高并发场景可将 `level` 调为 `warn`，只保留失败日志
- librdkafka 自身的连接/重试日志默认关闭（Kafka 故障时每 0.5 秒刷屏），只保留业务侧的 `kafka 投递失败`
- 需要排查 librdkafka 内部问题时：`RUST_LOG=librdkafka=debug cargo run`（设置 `RUST_LOG` 后完全按它过滤）
- 时间戳为 UTC

## Docker上的编译，k8s上部署
deploy/k8s.yaml 有，但它里面不包含程序本体——它只是"告诉 K8s 怎么跑"的声明，程序在镜像里。

两者的关系

Dockerfile ──构建──▶ 镜像 (registry.example.com/taotie:0.1.0) ──推送──▶ 镜像仓库
                                                                      │
k8s.yaml ──提交──▶ Deployment ──拉取镜像──▶ Pod 运行 taotie 二进制 ◀──┘

k8s.yaml 里这一行指向镜像：

containers:
  - name: taotie
    image: registry.example.com/taotie:0.1.0   # ← 只是一个地址

K8s 自己不会编译代码，Pod 启动时是从镜像仓库拉取这个镜像来运行的。所以顺序是：

1. 用 Dockerfile 构建出 x86_64 镜像（因为你的集群节点是 x86_64）
2. 推送到镜像仓库
3. 把 k8s.yaml 里的 image 改成实际地址
4. kubectl apply -f deploy/k8s.yaml，Pod 才会去拉镜像启动

能不能不构建镜像？

理论上可以把二进制放进 ConfigMap 或 hostPath 挂进去，但：

- ConfigMap 单文件上限 1MB，二进制 4.6MB，放不下
- hostPath 要求所有节点都手工放好文件，失去 K8s 的调度优势
- 无法做滚动更新和版本回滚

标准做法就是"镜像 + 仓库"，k8s.yaml 只负责声明式部署。

所以现在的完整流程还是：构建 amd64 镜像 → 推送 → 改 k8s.yaml 里的 image 地址 → apply。

完整链路

# 1) 构建 x86_64 镜像（Mac 上通过 buildx + QEMU）
docker buildx build --platform linux/amd64 \
  -t registry.example.com/taotie:0.1.0 --push .

# 2) 改 deploy/k8s.yaml 两处
#    - image: registry.example.com/taotie:0.1.0
#    - ConfigMap 里的 brokers（K8s 集群内 Kafka 地址，不是 localhost）

# 3) 部署
kubectl apply -f deploy/k8s.yaml
kubectl rollout status deploy/taotie

# 4) 验证
kubectl get pods -l app=taotie
kubectl logs -l app=taotie --tail=20
kubectl port-forward svc/taotie 8080:8080
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics

两个常见坑

1. 私有仓库鉴权：如果镜像仓库需要登录，Deployment 要加 imagePullSecrets

spec:
  template:
    spec:
      imagePullSecrets:
        - name: regcred
2. 节点架构：镜像只有 amd64，如果集群里有 arm64 节点，建议在 Deployment 里加约束：

nodeSelector:
  kubernetes.io/arch: amd64

   （你的集群如果全是 x86_64 就不用管）

后续

第一次手工跑通后，把第 1、2 步搬进 CI（GitLab CI / Jenkins），用 docker buildx build --platform linux/amd64 --push 出镜像，
K8s 侧只改 tag 或接入 Argo CD，就形成标准发布流水线了。
