use std::sync::Arc;
use std::time::Instant;

use axum::extract::{RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use garde::Validate;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kafka::Producer;

/// 应用共享状态
pub struct AppState {
    pub producer: Producer,
}

/// 埋点原始上报结构（前端 js-sdk 上报的通用埋点字段）
#[derive(Debug, Deserialize, Serialize, Validate)]
#[allow(dead_code)] // 仅做结构校验，字段值本身不需要读取
struct BuryEvent {
    /// 身份标识集合
    #[garde(skip)]
    identities: serde_json::Map<String, Value>,
    /// 用户唯一标识
    #[garde(length(min = 1))]
    distinct_id: String,
    /// SDK 信息
    #[garde(skip)]
    lib: serde_json::Map<String, Value>,
    /// 事件属性
    #[garde(skip)]
    properties: serde_json::Map<String, Value>,
    /// 匿名标识
    #[garde(length(min = 1))]
    anonymous_id: String,
    /// 事件类型
    #[garde(length(min = 1))]
    r#type: String,
    /// 事件名
    #[garde(length(min = 1))]
    event: String,
    /// 事件发生时间（毫秒时间戳）
    #[garde(skip)]
    time: i64,
    /// 埋点记录 id
    #[garde(skip)]
    #[serde(rename = "_track_id")]
    track_id: i64,
    /// SDK 上报时间（毫秒时间戳）
    #[garde(skip)]
    #[serde(rename = "_flush_time")]
    flush_time: i64,
    /// 其余未知字段原样保留（含 ext），保证透传不丢字段
    #[garde(skip)]
    #[serde(flatten)]
    extra: serde_json::Map<String, Value>,
}

/// 健康检查：进程存活即返回 ok
///
/// 不探测 Kafka：Kafka 抖动时不应让实例被负载均衡摘除，
/// Kafka 可用性由上报接口的 503 与监控体现。
pub async fn healthz() -> &'static str {
    "ok"
}

/// 接收埋点上报：`GET /bigData/receiveGet?data=xxx&ext=crc=1538298110`
pub async fn receive_get(
    State(state): State<Arc<AppState>>,
    RawQuery(raw): RawQuery,
) -> impl IntoResponse {
    let _inflight = crate::metrics::begin();
    let start = Instant::now();
    let (status, body) = process(&state, raw, start).await;
    crate::metrics::record(status.as_u16(), start.elapsed());
    (status, body)
}

/// 就绪检查：探测 Kafka 与目标 topic 是否可用（供 K8s readinessProbe 使用）
pub async fn readyz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let state = state.clone();
    let result = tokio::task::spawn_blocking(move || {
        state
            .producer
            .check_ready(std::time::Duration::from_millis(500))
    })
    .await;

    match result {
        Ok(Ok(())) => (StatusCode::OK, "ok".to_string()),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "就绪检查失败");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("kafka 不可用: {e}"),
            )
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("就绪检查任务失败: {e}"),
        ),
    }
}

/// Prometheus 指标输出
pub async fn metrics() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        crate::metrics::render(),
    )
}

/// 处理上报：解码 → 校验 → 投递 Kafka
async fn process(state: &AppState, raw: Option<String>, start: Instant) -> (StatusCode, String) {
    let raw = raw.unwrap_or_default();

    let Some(data) = query_param(&raw, "data") else {
        return bad_request("缺少参数 data");
    };

    let Ok(mut payload) = STANDARD.decode(data.trim()) else {
        return bad_request("data 不是合法的 base64");
    };
    // 只解析一次：已知字段走类型校验，未知字段进 extra 原样保留
    // simd-json 原地解析会改写缓冲区，因此统一附加 ext 后重新序列化
    let mut bury = match parse_event(&mut payload, &data) {
        Ok(bury) => bury,
        Err(response) => return response,
    };
    if let Err(report) = bury.validate() {
        return bad_request(format!("埋点字段校验失败: {report}"));
    }

    // ext 原样附加到埋点 JSON 中（不改动已有字段）
    if let Some(ext) = query_param(&raw, "ext") {
        bury.extra.insert("ext".to_string(), Value::String(ext));
    }

    let payload = match simd_json::serde::to_vec(&bury) {
        Ok(payload) => payload,
        Err(e) => return bad_request(format!("埋点数据序列化失败: {e}")),
    };

    match state.producer.send(&payload).await {
        Ok(()) => {
            tracing::info!(
                status = 200,
                bytes = payload.len(),
                elapsed_ms = start.elapsed().as_micros() as f64 / 1000.0,
                "埋点上报成功"
            );
            (StatusCode::OK, "ok".to_string())
        }
        Err(e) => {
            tracing::error!(
                status = 503,
                distinct_id = %bury.distinct_id,
                event = %bury.event,
                bytes = payload.len(),
                elapsed_ms = start.elapsed().as_micros() as f64 / 1000.0,
                error = %e,
                "kafka 投递失败"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("kafka 投递失败: {e}"),
            )
        }
    }
}

/// 解析埋点 JSON（simd-json 原地解析，会改写传入的缓冲区）。
///
/// UTF-8 校验由 simd-json 内部完成，非法时走 classify_parse_error 给出准确提示。
fn parse_event(buf: &mut [u8], data: &str) -> Result<BuryEvent, (StatusCode, String)> {
    match simd_json::serde::from_slice::<BuryEvent>(buf) {
        Ok(bury) => Ok(bury),
        Err(e) => {
            tracing::debug!(error = %e, "埋点 JSON 解析失败");
            Err(classify_parse_error(data))
        }
    }
}

/// 解析失败时区分「JSON 语法错误」和「字段缺失/类型错误」。
///
/// 只在错误路径执行，不影响正常请求的性能。
fn classify_parse_error(data: &str) -> (StatusCode, String) {
    let Ok(mut probe) = STANDARD.decode(data.trim()) else {
        return bad_request("data 不是合法的 base64");
    };
    if std::str::from_utf8(&probe).is_err() {
        return bad_request("data 解码后不是合法的 UTF-8 字符串");
    }
    match simd_json::serde::from_slice::<Value>(&mut probe) {
        Ok(_) => bad_request("埋点数据缺少必要字段或字段类型错误"),
        Err(_) => bad_request("data 解码后不是合法的 JSON"),
    }
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, String) {
    let msg = msg.into();
    tracing::warn!(status = 400, reason = %msg, "埋点上报被拒绝");
    (StatusCode::BAD_REQUEST, msg)
}

/// 从原始 query string 中提取参数。
///
/// 只做 `%XX` 解码，**不**把 `+` 当作空格：标准 base64 字母表包含 `+`，
/// 按表单规则解码会破坏数据（前端 SDK 会用 `%2B` 编码，此处行为与老服务一致）。
/// 非法转义（如 `%%%`）保留原值，交给后续 base64 校验给出准确原因。
fn query_param(raw: &str, name: &str) -> Option<String> {
    raw.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then(|| percent_decode(value).unwrap_or_else(|| value.to_string()))
    })
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex(bytes.get(i + 1).copied()?)?;
            let lo = hex(bytes.get(i + 2).copied()?)?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
