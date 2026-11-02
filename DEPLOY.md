# Taotie 部署文档

本文档描述 Taotie 埋点网关的构建、部署与运维。

## 1. 环境要求

| 项 | 要求 |
|---|---|
| 操作系统 | macOS / Linux（x86_64、arm64 均可） |
| Rust | 1.99+（edition 2024） |
| 构建工具 | `cmake`、`make`、C 编译器（clang/gcc） |
| Kafka | 2.x / 3.x，需可从网关所在机器访问 |
| 运行时依赖 | 无（librdkafka 静态编译进二进制，未链接 OpenSSL） |

> **不需要**单独安装 `librdkafka`：`rdkafka` 启用了 `cmake-build`，构建时会把 librdkafka 源码一起编译并静态链接。
>
> macOS 若 shell 中 `CC`/`CXX` 指向不存在的路径（如 `/opt/homebrew/bin/clang`），构建前先执行 `cc-system` 切换到系统 clang。

## 2. 构建

```sh
cd taotie
cargo build --release
```

产物：`target/release/taotie`（单文件可执行程序）。

如果目标机与构建机架构/系统不同，需在目标机或对应交叉编译环境中构建。

## 3. 部署目录

推荐布局：

```
/opt/taotie/
├── taotie              # 可执行文件
├── config.toml         # 配置（放在工作目录下）
└── logs/               # 日志目录（自动创建）
```

```sh
sudo mkdir -p /opt/taotie
sudo cp target/release/taotie /opt/taotie/
sudo cp config.toml /opt/taotie/
sudo chown -R taotie:taotie /opt/taotie   # 建议使用专用账号
```

> 配置文件按**相对路径** `config.toml` 读取，因此服务的工作目录必须是 `/opt/taotie`。
> 也可用 `TAOTIE_CONFIG=/etc/taotie/config.toml` 指定绝对路径。

## 4. 配置（生产建议）

```toml
listen_addr = "0.0.0.0:8080"

[kafka]
brokers = "kafka1:9092,kafka2:9092,kafka3:9092"   # 多 broker 用逗号分隔
topic = "bury-track"
key = "track"
acks = "1"
queue_buffering_ms = 5
compression = "lz4"
send_timeout_ms = 3000
queue_timeout_ms = 100
shutdown_flush_ms = 5000

[log]
level = "info"        # 高 QPS 建议 warn，只记录失败请求
dir = "logs"
file_prefix = "taotie"
```

说明：

- `acks = "1"`：leader 落盘即返回，兼顾吞吐与可靠性；要求更强一致可改 `"all"`（延迟上升）
- `send_timeout_ms`：Kafka 不可达时，超过该时间返回 503 让客户端重试；不要调得过大，否则请求会长时间挂起
- `queue_timeout_ms`：本地队列满时的等待上限，超过直接 503
- 日志级别可用环境变量 `RUST_LOG` 覆盖，如 `RUST_LOG=taotie=debug`

## 5. 准备 Kafka topic

首次部署前创建 topic（或用自动建 topic 策略）：

```sh
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic bury-track --partitions 1 --replication-factor 1
```

确认：

```sh
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bury-track
```

> 消息 key 固定为 `track`，所有消息会落在同一个分区。需要更高写入吞吐时，
> 应同时调整 key 策略（如改为 `distinct_id`）并同步下游消费逻辑。

## 6. 启动方式

### 方式一：systemd（推荐）

`/etc/systemd/system/taotie.service`：

```ini
[Unit]
Description=Taotie bury-track gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=taotie
Group=taotie
WorkingDirectory=/opt/taotie
ExecStart=/opt/taotie/taotie
Restart=always
RestartSec=3
# 收到 SIGTERM 后留出时间完成在途请求与 Kafka 刷盘
TimeoutStopSec=15
LimitNOFILE=65535
# 可选：覆盖配置路径与日志级别
# Environment=TAOTIE_CONFIG=/opt/taotie/config.toml
# Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now taotie
sudo systemctl status taotie
```

### 方式二：手动后台运行

```sh
cd /opt/taotie
nohup ./taotie >> logs/stdout.log 2>&1 &
```

## 7. Kubernetes 部署

项目内置多阶段 `Dockerfile` 和 `deploy/k8s.yaml`。**不要在 Pod 内编译**：镜像由 CI/构建机生成后推送到镜像仓库。

### 构建并推送镜像（x86_64）

```sh
# Apple Silicon 构建机上需显式指定目标架构
docker buildx build --platform linux/amd64 \
  -t registry.example.com/taotie:0.1.0 --push .
```

> 构建镜像基于 `rust:1.99-slim-bullseye`（glibc 2.31），产物可运行在 glibc ≥ 2.31 的节点上。
> 若节点更老（如 CentOS 7 的 2.17），需改用对应基础镜像或 musl 静态编译，否则会报 `GLIBC_2.xx not found`。

### 部署

```sh
# 先修改 deploy/k8s.yaml 中的镜像地址和 ConfigMap 里的 Kafka 地址
kubectl apply -f deploy/k8s.yaml
kubectl rollout status deploy/taotie
```

清单包含：

| 资源 | 说明 |
|---|---|
| ConfigMap `taotie-config` | `config.toml`，其中 `file = false`（仅 stdout 日志） |
| Deployment | 2 副本、滚动更新、preStop 摘流、探针、资源限制 |
| Service | ClusterIP:8080 |
| PodDisruptionBudget | 至少保留 1 个可用副本 |

### 关键配置

- **日志**：Pod 内 `file = false`，日志全部走 stdout 由集群采集，不写容器文件系统
- **探针**：liveness → `/healthz`（不探测 Kafka）；readiness → `/readyz`（探测 Kafka 与 topic）
- **优雅退出**：`terminationGracePeriodSeconds: 30` 必须大于 `shutdown_flush_ms` + 在途请求处理时间
- **资源**：只设 memory limit，不设 CPU limit，避免 CFS 限流造成 P99 抖动
- **指标**：Pod 带 `prometheus.io/scrape` 注解；使用 Prometheus Operator 时可改为 ServiceMonitor

## 8. 部署验证

```sh
# 1) 进程与端口
ss -lntp | grep 8080        # 或: lsof -nP -iTCP:8080 -sTCP:LISTEN

# 2) 启动日志
tail -f /opt/taotie/logs/taotie.$(date +%F).log
# taotie 启动，监听 http://0.0.0.0:8080（brokers=... topic=bury-track）
# 上面这行由 println 输出，不受日志级别影响

# 3) 健康检查
curl -s http://127.0.0.1:8080/healthz     # ok

# 4) 上报一条测试数据（返回 ok 即成功）
curl "http://127.0.0.1:8080/bigData/receiveGet?data=<base64>&ext=crc%3D1"

# 5) 确认 Kafka 收到
kcat -b localhost:9092 -t bury-track -C -o -1 -c 1 -f 'key=%k value=%s\n'
```

## 9. 监控接入

网关暴露 Prometheus 指标：`GET /metrics`。

```yaml
# prometheus.yml
scrape_configs:
  - job_name: taotie
    metrics_path: /metrics
    static_configs:
      - targets: ['10.0.0.10:8080']
```

推荐告警规则：

```yaml
groups:
  - name: taotie
    rules:
      # Kafka 投递失败比例超过 1%
      - alert: TaotieKafkaDeliveryFailing
        expr: |
          sum(rate(taotie_requests_total{status="503"}[5m]))
            / sum(rate(taotie_requests_total[5m])) > 0.01
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "埋点网关 503 比例超过 1%"

      # 参数/校验失败比例异常（通常说明上游 SDK 上报格式变了）
      - alert: TaotieBadRequestSpike
        expr: |
          sum(rate(taotie_requests_total{status="400"}[5m]))
            / sum(rate(taotie_requests_total[5m])) > 0.2
        for: 5m
        labels: { severity: warning }

      # P99 处理耗时（含 Kafka ack）
      - alert: TaotieLatencyHigh
        expr: |
          histogram_quantile(0.99,
            sum by (le) (rate(taotie_request_duration_seconds_bucket[5m]))) > 1
        for: 5m
        labels: { severity: warning }
```

## 10. 日志与排错

| 现象 | 可能原因 | 处理 |
|---|---|---|
| 启动报 `监听 ... 失败` | 端口被占用 / 无权限 | 改 `listen_addr` 或释放端口 |
| 启动报 `配置文件 ... 解析失败` | TOML 语法错误 | 按报错行号修正 |
| 大量 400 `data 不是合法的 base64` | 上游 SDK 编码方式变化 / 参数被二次转义 | 检查原始 query string |
| 大量 400 `埋点字段校验失败` | 上游字段为空或类型不符 | 按 reason 中字段名排查 |
| 大量 503 `kafka 投递失败` | Kafka 不可达、topic 不存在、队列满 | 检查 broker 连通性、topic、磁盘与网络 |
| 进程重启 | 见 systemd `journalctl -u taotie` | 结合日志定位 |

日志文件：`<工作目录>/logs/taotie.YYYY-MM-DD.log`，按天滚动，时间为 UTC。

## 11. 升级与回滚

```sh
# 备份旧版本
cp /opt/taotie/taotie /opt/taotie/taotie.bak.$(date +%F_%H%M)

# 替换并重启
sudo cp target/release/taotie /opt/taotie/taotie
sudo systemctl restart taotie

# 回滚
sudo cp /opt/taotie/taotie.bak.<时间戳> /opt/taotie/taotie
sudo systemctl restart taotie
```

## 12. 已知限制

- 无鉴权、无限流、无本地重试落盘（当前版本设计如此）
- Kafka 不可用时依赖客户端重试，网关会返回 503 而非本地缓存
- 单条消息 key 固定，写入集中在单一分区
- `/healthz` 只反映进程存活，不代表 Kafka 可用
- 日志分片按天，不自动清理，需配合 logrotate 自行管理
