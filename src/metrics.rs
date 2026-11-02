//! Prometheus 指标：请求计数与处理耗时直方图。

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};

static METRICS: OnceLock<Metrics> = OnceLock::new();

struct Metrics {
    registry: Registry,
    requests: IntCounterVec,
    duration: HistogramVec,
    inflight: IntGauge,
}

impl Metrics {
    fn new() -> Result<Self> {
        let registry = Registry::new();

        let requests = IntCounterVec::new(
            Opts::new("taotie_requests_total", "埋点上报请求数"),
            &["status"],
        )
        .context("创建 requests 指标失败")?;

        let duration = HistogramVec::new(
            HistogramOpts::new(
                "taotie_request_duration_seconds",
                "埋点上报处理耗时（秒），含等待 Kafka ack",
            )
            .buckets(vec![
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
            &["status"],
        )
        .context("创建 duration 指标失败")?;

        let inflight = IntGauge::new("taotie_requests_inflight", "正在处理中的埋点请求数")
            .context("创建 inflight 指标失败")?;

        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(duration.clone()))?;
        registry.register(Box::new(inflight.clone()))?;

        Ok(Self {
            registry,
            requests,
            duration,
            inflight,
        })
    }
}

/// 初始化指标，进程启动时调用一次
pub fn init() -> Result<()> {
    let metrics = Metrics::new()?;
    METRICS
        .set(metrics)
        .map_err(|_| anyhow::anyhow!("指标已被初始化"))?;
    Ok(())
}

/// 记录一次上报请求的结果
pub fn record(status: u16, elapsed: Duration) {
    if let Some(metrics) = METRICS.get() {
        let status = status.to_string();
        metrics.requests.with_label_values(&[status.as_str()]).inc();
        metrics
            .duration
            .with_label_values(&[status.as_str()])
            .observe(elapsed.as_secs_f64());
    }
}

/// 输出 Prometheus 文本格式
pub fn render() -> String {
    let Some(metrics) = METRICS.get() else {
        return String::new();
    };
    let mut buffer = Vec::new();
    if TextEncoder::new()
        .encode(&metrics.registry.gather(), &mut buffer)
        .is_err()
    {
        return String::new();
    }
    String::from_utf8(buffer).unwrap_or_default()
}

/// 请求开始时调用，guard 在请求结束时（drop）自动递减在途计数
pub struct InflightGuard(Option<&'static IntGauge>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(inflight) = self.0 {
            inflight.dec();
        }
    }
}

pub fn begin() -> InflightGuard {
    match METRICS.get() {
        Some(metrics) => {
            metrics.inflight.inc();
            InflightGuard(Some(&metrics.inflight))
        }
        None => InflightGuard(None),
    }
}
