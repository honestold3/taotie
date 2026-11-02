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

    // ext 原样附加到埋点 JSON 中（数据里已有 ext 字段时会被覆盖）
    attach_ext(&mut bury, query_param(&raw, "ext").as_deref());

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

/// 把 query 中的 `ext` 附加到埋点 JSON 的顶层字段中（已有 `ext` 会被覆盖）。
fn attach_ext(bury: &mut BuryEvent, ext: Option<&str>) {
    if let Some(ext) = ext {
        bury.extra
            .insert("ext".to_string(), Value::String(ext.to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 一条字段完整的合法埋点，`custom_field` 用于验证未知字段透传
    const VALID: &str = r#"{"identities":{"$device_id":"d1"},"distinct_id":"u1","lib":{"$lib":"js-sdk"},"properties":{"page":"/home"},"anonymous_id":"a1","type":"track","event":"$pageview","time":1787725281606,"_track_id":737861607,"_flush_time":1787725281607,"custom_field":{"nested":[1,2,3]}}"#;

    /// 走与线上一致的路径：base64 编码 → 解码 → simd-json 原地解析
    fn parse_json(json: &str) -> Result<BuryEvent, (StatusCode, String)> {
        let encoded = STANDARD.encode(json);
        let mut buf = STANDARD
            .decode(encoded.as_bytes())
            .expect("测试数据编码失败");
        parse_event(&mut buf, &encoded)
    }

    fn classify(raw: &str) -> String {
        classify_parse_error(raw).1
    }

    // ---------- query 参数解析 ----------

    #[test]
    fn query_param_finds_each_key() {
        let q = "data=abc&ext=crc%3D1";
        assert_eq!(query_param(q, "data").as_deref(), Some("abc"));
        assert_eq!(query_param(q, "ext").as_deref(), Some("crc=1"));
    }

    #[test]
    fn query_param_missing_key_returns_none() {
        assert_eq!(query_param("ext=1", "data"), None);
        assert_eq!(query_param("", "data"), None);
    }

    #[test]
    fn query_param_bare_key_and_empty_value() {
        assert_eq!(query_param("data", "data").as_deref(), Some(""));
        assert_eq!(query_param("data=", "data").as_deref(), Some(""));
    }

    #[test]
    fn query_param_keeps_plus_as_literal() {
        // base64 字母表包含 '+'，不能按表单规则转成空格（与老服务行为一致）
        assert_eq!(query_param("data=a+b", "data").as_deref(), Some("a+b"));
    }

    #[test]
    fn query_param_decodes_percent_encoded_plus() {
        assert_eq!(query_param("data=a%2Bb", "data").as_deref(), Some("a+b"));
    }

    #[test]
    fn query_param_keeps_equals_sign_in_value() {
        assert_eq!(
            query_param("data=YQ==&ext=1", "data").as_deref(),
            Some("YQ==")
        );
    }

    #[test]
    fn query_param_duplicate_key_takes_first() {
        assert_eq!(query_param("data=1&data=2", "data").as_deref(), Some("1"));
    }

    #[test]
    fn query_param_invalid_escape_falls_back_to_raw_value() {
        assert_eq!(query_param("data=%zz", "data").as_deref(), Some("%zz"));
        assert_eq!(query_param("data=%2", "data").as_deref(), Some("%2"));
    }

    #[test]
    fn query_param_requires_exact_key() {
        assert_eq!(query_param("dat=1", "data"), None);
    }

    #[test]
    fn percent_decode_hex_is_case_insensitive() {
        assert_eq!(percent_decode("%41%61").as_deref(), Some("Aa"));
        assert_eq!(percent_decode("%4a").as_deref(), Some("J"));
    }

    #[test]
    fn percent_decode_rejects_bad_input() {
        assert_eq!(percent_decode("%zz"), None);
        assert_eq!(percent_decode("%2"), None);
        // 0xFF 解出来不是合法 UTF-8
        assert_eq!(percent_decode("%ff"), None);
    }

    #[test]
    fn percent_decode_plain_text_is_unchanged() {
        assert_eq!(percent_decode("abc+/=").as_deref(), Some("abc+/="));
    }

    // ---------- 错误分类 ----------

    #[test]
    fn classify_reports_invalid_base64() {
        assert_eq!(classify("!!!"), "data 不是合法的 base64");
    }

    #[test]
    fn classify_reports_invalid_utf8() {
        let data = STANDARD.encode([0xff, 0xfe, 0xfd]);
        assert_eq!(classify(&data), "data 解码后不是合法的 UTF-8 字符串");
    }

    #[test]
    fn classify_reports_invalid_json() {
        assert_eq!(
            classify(&STANDARD.encode("{not json")),
            "data 解码后不是合法的 JSON"
        );
    }

    #[test]
    fn classify_reports_missing_fields() {
        assert_eq!(
            classify(&STANDARD.encode("{}")),
            "埋点数据缺少必要字段或字段类型错误"
        );
    }

    // ---------- 埋点解析 ----------

    #[test]
    fn parses_valid_event() {
        let bury = parse_json(VALID).expect("合法埋点应解析成功");
        assert_eq!(bury.distinct_id, "u1");
        assert_eq!(bury.anonymous_id, "a1");
        assert_eq!(bury.r#type, "track");
        assert_eq!(bury.event, "$pageview");
        assert_eq!(bury.time, 1787725281606);
        assert_eq!(bury.track_id, 737861607);
        assert_eq!(bury.flush_time, 1787725281607);
    }

    #[test]
    fn keeps_unknown_fields() {
        let bury = parse_json(VALID).unwrap();
        assert_eq!(
            bury.extra.get("custom_field"),
            Some(&serde_json::json!({"nested": [1, 2, 3]}))
        );
    }

    #[test]
    fn rejects_missing_required_field() {
        let json = r#"{"identities":{},"distinct_id":"u1","anonymous_id":"a1","type":"track"}"#;
        let (status, msg) = parse_json(json).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(msg, "埋点数据缺少必要字段或字段类型错误");
    }

    #[test]
    fn rejects_wrong_field_type() {
        let json = r#"{"identities":[],"distinct_id":"u1","lib":{},"properties":{},"anonymous_id":"a1","type":"track","event":"e","time":1,"_track_id":2,"_flush_time":3}"#;
        let (status, msg) = parse_json(json).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(msg, "埋点数据缺少必要字段或字段类型错误");
    }

    #[test]
    fn rejects_non_integer_time() {
        // 当前实现要求 time 为整数（老服务 Jackson 可能更宽松）
        let json = r#"{"identities":{},"distinct_id":"u1","lib":{},"properties":{},"anonymous_id":"a1","type":"track","event":"e","time":"1787725281606","_track_id":2,"_flush_time":3}"#;
        assert!(parse_json(json).is_err());
    }

    // ---------- 字段校验 ----------

    #[test]
    fn valid_event_passes_validation() {
        let bury = parse_json(VALID).unwrap();
        assert!(bury.validate().is_ok());
    }

    #[test]
    fn rejects_empty_required_strings() {
        let cases = [
            (
                "distinct_id",
                VALID.replace("\"distinct_id\":\"u1\"", "\"distinct_id\":\"\""),
            ),
            (
                "anonymous_id",
                VALID.replace("\"anonymous_id\":\"a1\"", "\"anonymous_id\":\"\""),
            ),
            ("type", VALID.replace("\"type\":\"track\"", "\"type\":\"\"")),
            (
                "event",
                VALID.replace("\"event\":\"$pageview\"", "\"event\":\"\""),
            ),
        ];

        for (field, json) in cases {
            let bury = parse_json(&json).unwrap();
            let report = format!("{}", bury.validate().unwrap_err());
            assert!(report.contains(field), "{field} 应为空，实际报错: {report}");
        }
    }

    // ---------- ext 合并与序列化 ----------

    #[test]
    fn attach_ext_inserts_field() {
        let mut bury = parse_json(VALID).unwrap();
        attach_ext(&mut bury, Some("crc=1538298110"));
        assert_eq!(
            bury.extra.get("ext"),
            Some(&Value::String("crc=1538298110".to_string()))
        );
    }

    #[test]
    fn attach_ext_none_leaves_payload_untouched() {
        let mut bury = parse_json(VALID).unwrap();
        let before = bury.extra.len();
        attach_ext(&mut bury, None);
        assert_eq!(bury.extra.len(), before);
        assert!(bury.extra.get("ext").is_none());
    }

    #[test]
    fn attach_ext_overwrites_existing_field() {
        // 当前语义：query 中的 ext 优先于数据里自带的 ext
        let mut bury = parse_json(VALID).unwrap();
        attach_ext(&mut bury, Some("old"));
        attach_ext(&mut bury, Some("new"));
        assert_eq!(
            bury.extra.get("ext"),
            Some(&Value::String("new".to_string()))
        );
    }

    #[test]
    fn serialized_payload_keeps_unknown_fields_and_ext() {
        let mut bury = parse_json(VALID).unwrap();
        attach_ext(&mut bury, Some("crc=1"));
        let out = simd_json::serde::to_vec(&bury).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\"custom_field\""));
        assert!(text.contains("\"ext\":\"crc=1\""));
        assert!(text.contains("\"_track_id\":737861607"));
    }
}
