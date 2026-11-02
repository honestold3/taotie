#!/usr/bin/env bash
# 埋点网关压测脚本（基于 wrk）
#
# 默认启动一个独立的压测实例（端口 8090，topic bury-track-bench），
# 压测结束后自动优雅退出，不污染业务实例和 bury-track。
#
# 用法:
#   ./bench/run.sh                                                 # 4 线程 / 200 并发 / 30 秒
#   THREADS=8 CONNECTIONS=500 DURATION=60s ./bench/run.sh
#   AUTO_START=0 BASE_URL=http://10.0.0.10:8080 ./bench/run.sh     # 压已有实例
#
# 环境变量:
#   THREADS / CONNECTIONS / DURATION / PAYLOADS   压测参数
#   AUTO_START=1|0                                是否自动启停压测实例（默认 1）
#   BENCH_PORT=8090 / BENCH_TOPIC=bury-track-bench / BROKERS=localhost:9092
#   BASE_URL                                      AUTO_START=0 时压测目标
set -euo pipefail

THREADS="${THREADS:-4}"
CONNECTIONS="${CONNECTIONS:-200}"
DURATION="${DURATION:-30s}"
PAYLOADS="${PAYLOADS:-bench/payloads.txt}"
AUTO_START="${AUTO_START:-1}"
BENCH_PORT="${BENCH_PORT:-8090}"
BENCH_TOPIC="${BENCH_TOPIC:-bury-track-bench}"
BROKERS="${BROKERS:-localhost:9092}"
BENCH_EXT="${BENCH_EXT-&ext=crc%3D1538298110}"
BENCH_CONFIG="/tmp/taotie-bench.toml"

if [ "$AUTO_START" = "1" ]; then
    BASE_URL="${BASE_URL:-http://127.0.0.1:${BENCH_PORT}}"
else
    BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
fi

cd "$(dirname "$0")/.."

if ! command -v wrk >/dev/null 2>&1; then
    echo "未找到 wrk，请先安装: brew install wrk" >&2
    exit 1
fi

if [ ! -f "$PAYLOADS" ]; then
    echo "首次运行，生成压测数据…"
    python3 -I bench/gen_payloads.py 1000 "$PAYLOADS"
fi

BENCH_PID=""
cleanup() {
    if [ -n "$BENCH_PID" ] && kill -0 "$BENCH_PID" 2>/dev/null; then
        echo "停止压测实例 (pid=$BENCH_PID)…"
        kill -TERM "$BENCH_PID" 2>/dev/null || true
        wait "$BENCH_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# 查找 kafka-topics.sh（PATH 中或 $HOME/opt/kafka_*/bin）
find_kafka_topics() {
    local kafka_topics candidate
    kafka_topics="$(command -v kafka-topics.sh 2>/dev/null || true)"
    if [ -z "$kafka_topics" ]; then
        for candidate in "$HOME"/opt/kafka_*/bin/kafka-topics.sh; do
            if [ -x "$candidate" ]; then
                kafka_topics="$candidate"
                break
            fi
        done
    fi
    echo "$kafka_topics"
}

# 确保压测 topic 存在（找不到 Kafka CLI 时依赖 broker 自动建 topic）
ensure_topic() {
    local kafka_topics
    kafka_topics="$(find_kafka_topics)"
    if [ -n "$kafka_topics" ]; then
        "$kafka_topics" --bootstrap-server "$BROKERS" --create --if-not-exists \
            --topic "$BENCH_TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
    else
        echo "未找到 kafka-topics.sh，依赖 broker 自动创建 topic: $BENCH_TOPIC"
    fi
}

# 等待 topic 从元数据和 topic 列表中彻底消失
wait_topic_gone() {
    local kafka_topics="$1" i listed metadata
    for i in $(seq 1 80); do
        listed="$("$kafka_topics" --bootstrap-server "$BROKERS" --list 2>/dev/null | grep -cx "$BENCH_TOPIC" || true)"
        metadata=0
        if command -v kcat >/dev/null 2>&1; then
            metadata="$(kcat -b "$BROKERS" -L 2>/dev/null | grep -c "topic \"$BENCH_TOPIC\"" || true)"
        fi
        if [ "${listed:-0}" -eq 0 ] && [ "${metadata:-0}" -eq 0 ]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

# 每次压测前清空 topic（删除后重建，避免历史数据影响对账）
clear_topic() {
    local kafka_topics
    kafka_topics="$(find_kafka_topics)"
    if [ -z "$kafka_topics" ]; then
        echo "未找到 kafka-topics.sh，跳过清空 topic" >&2
        return 0
    fi

    # 安全保护：只清空名字里带 bench 的 topic
    if ! printf '%s' "$BENCH_TOPIC" | grep -qi bench && [ "${CLEAR_TOPIC_FORCE:-0}" != "1" ]; then
        echo "拒绝清空非压测 topic '$BENCH_TOPIC'（如确需清空请设置 CLEAR_TOPIC_FORCE=1）" >&2
        return 1
    fi

    echo "清空压测 topic: ${BENCH_TOPIC}（删除后重建）"
    "$kafka_topics" --bootstrap-server "$BROKERS" --delete --topic "$BENCH_TOPIC" >/dev/null 2>&1 || true

    if ! wait_topic_gone "$kafka_topics"; then
        echo "等待 topic 删除超时，跳过重建（本次数据可能与历史数据混在一起）" >&2
        return 0
    fi

    # ZK 模式删除是异步的，稍等再建，避免新建 topic 的数据被延迟清理掉
    sleep 2
    "$kafka_topics" --bootstrap-server "$BROKERS" --create --topic "$BENCH_TOPIC" \
        --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

    if command -v kcat >/dev/null 2>&1; then
        local offset
        offset="$(kcat -b "$BROKERS" -Q -t "${BENCH_TOPIC}:0:-1" 2>/dev/null | awk '{print $NF}')"
        echo "  清空后 offset: ${offset:-未知}"
    fi
}

if [ "$AUTO_START" = "1" ]; then
    BIN="target/release/taotie"
    [ -x "$BIN" ] || BIN="target/debug/taotie"
    if [ ! -x "$BIN" ]; then
        echo "未找到可执行文件，先执行 cargo build…"
        cargo build
        BIN="target/debug/taotie"
    fi

    mkdir -p logs
    cat > "$BENCH_CONFIG" <<EOF
listen_addr = "0.0.0.0:${BENCH_PORT}"

[kafka]
brokers = "${BROKERS}"
topic = "${BENCH_TOPIC}"
key = "track"
acks = "1"
queue_buffering_ms = 5
send_timeout_ms = 3000
queue_timeout_ms = 100
shutdown_flush_ms = 5000

[log]
level = "warn"
dir = "logs"
file_prefix = "taotie-bench"
EOF

    if [ "${CLEAR_TOPIC:-1}" = "1" ]; then
        clear_topic
    else
        ensure_topic
    fi

    echo "启动独立压测实例: 端口 ${BENCH_PORT} / topic ${BENCH_TOPIC}"
    TAOTIE_CONFIG="$BENCH_CONFIG" "$BIN" > logs/bench-stdout.log 2>&1 &
    BENCH_PID=$!

    if ! curl -sf --max-time 2 --retry 50 --retry-connrefused --retry-delay 0 \
        "http://127.0.0.1:${BENCH_PORT}/healthz" >/dev/null; then
        echo "压测实例启动失败，检查 logs/bench-stdout.log" >&2
        exit 1
    fi
else
    if ! curl -sf --max-time 3 "$BASE_URL/healthz" >/dev/null; then
        echo "服务不可用: $BASE_URL/healthz" >&2
        exit 1
    fi
fi

count_of() {
    # $1: 指标快照  $2: 状态码
    # 该状态码可能尚未出现过（grep 返回 1），这里不能让脚本因 set -e 退出
    echo "$1" | grep -F "taotie_requests_total{status=\"$2\"}" | awk '{print $2}' | head -1 || true
}

inflight_of() {
    echo "$1" | grep -F "taotie_requests_inflight" | awk '{print $2}' | head -1 || true
}

total_of() {
    local sum=0 value
    for status in 200 400 503; do
        value="$(count_of "$1" "$status")"
        value="${value:-0}"
        sum=$((sum + value))
    done
    echo "$sum"
}

# 等待服务端指标停止增长（在途请求全部结算），返回稳定后的指标快照
wait_stable() {
    local prev cur prev_total cur_total i
    prev="$(curl -s --max-time 3 "$BASE_URL/metrics")"
    prev_total="$(total_of "$prev")"
    for i in $(seq 1 25); do
        sleep 0.2
        cur="$(curl -s --max-time 3 "$BASE_URL/metrics")"
        cur_total="$(total_of "$cur")"
        cur_inflight="$(inflight_of "$cur")"
        if [ "$cur_total" = "$prev_total" ] && [ "${cur_inflight:-1}" = "0" ]; then
            echo "$cur"
            return
        fi
        prev="$cur"
        prev_total="$cur_total"
    done
    echo "$prev"
}

before="$(curl -s --max-time 3 "$BASE_URL/metrics")"
before_200="$(count_of "$before" 200)"; before_200="${before_200:-0}"
before_400="$(count_of "$before" 400)"; before_400="${before_400:-0}"
before_503="$(count_of "$before" 503)"; before_503="${before_503:-0}"

echo "== 压测配置 =="
echo "目标:   $BASE_URL/bigData/receiveGet"
echo "线程:   $THREADS"
echo "并发:   $CONNECTIONS"
echo "时长:   $DURATION"
echo "数据:   $PAYLOADS"
if [ "$AUTO_START" = "1" ]; then
    echo "topic:  ${BENCH_TOPIC}（独立实例，不影响业务 topic）"
fi
echo

# macOS 默认文件句柄上限较低，大并发前建议: ulimit -n 65535
wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" --latency \
    -s bench/loadtest.lua "$BASE_URL/bigData/receiveGet" -- "$PAYLOADS" "$BENCH_EXT"

echo "等待在途请求处理完成（指标稳定）…" >&2
after="$(wait_stable)"
after_200="$(count_of "$after" 200)"; after_200="${after_200:-0}"
after_400="$(count_of "$after" 400)"; after_400="${after_400:-0}"
after_503="$(count_of "$after" 503)"; after_503="${after_503:-0}"

ok=$((after_200 - before_200))
bad=$((after_400 - before_400))
err=$((after_503 - before_503))
total=$((ok + bad + err))

secs="$(echo "$DURATION" | sed 's/[^0-9].*//')"
[ -z "$secs" ] && secs=1
[ "$secs" -eq 0 ] && secs=1

echo
echo "== 服务端指标增量（约 ${secs}s）=="
printf '200 成功: %d\n' "$ok"
printf '400 拒绝: %d\n' "$bad"
printf '503 失败: %d\n' "$err"
if [ "$total" -gt 0 ]; then
    awk -v t="$total" -v e="$err" -v b="$bad" \
        'BEGIN { printf "错误率(503): %.3f%%   拒绝率(400): %.3f%%\n", e*100/t, b*100/t }'
    printf '服务端吞吐: %d req/s\n' "$((total / secs))"
else
    echo "未采集到指标增量（检查 /metrics 是否可访问）"
fi

if [ "$AUTO_START" = "1" ] && command -v kcat >/dev/null 2>&1; then
    topic_total="$(kcat -b "$BROKERS" -Q -t "${BENCH_TOPIC}:0:-1" 2>/dev/null | awk '{print $NF}')"
    if [ -n "$topic_total" ]; then
        printf '压测 topic %s 消息总数: %s\n' "$BENCH_TOPIC" "$topic_total"
    fi
fi
