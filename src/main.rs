//! Taotie —— 埋点接收网关
//!
//! 兼容原有 SpringBoot 接口 `GET /bigData/receiveGet?data=<base64>&ext=<...>`。
//! 本文件负责服务启动、日志、优雅退出与路由组装；数据解析见 [`handler`]，Kafka 投递见 [`kafka`]。

mod config;
mod handler;
mod kafka;
mod metrics;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::routing::get;
use tower_http::cors::CorsLayer;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

use crate::config::{AppConfig, LogConfig};
use crate::handler::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = AppConfig::load()?;
    let _log_guard = init_logging(&cfg.log);

    metrics::init().context("初始化指标失败")?;
    let producer = kafka::Producer::new(&cfg.kafka).context("初始化 Kafka 生产者失败")?;
    let flush_timeout = Duration::from_millis(cfg.kafka.shutdown_flush_ms);

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .with_context(|| format!("监听 {} 失败", cfg.listen_addr))?;
    let addr = listener.local_addr()?;
    // 监听地址用 println 直出：无论日志级别如何都能看到，便于部署时确认
    println!(
        "taotie 启动，监听 http://{addr}（brokers={} topic={}）",
        cfg.kafka.brokers, cfg.kafka.topic
    );
    tracing::info!(
        listen = %format!("http://{addr}"),
        brokers = %cfg.kafka.brokers,
        topic = %cfg.kafka.topic,
        "taotie 启动"
    );

    let state = Arc::new(AppState { producer });
    let app = Router::new()
        .route("/bigData/receiveGet", get(handler::receive_get))
        .route("/healthz", get(handler::healthz))
        .route("/readyz", get(handler::readyz))
        .route("/metrics", get(handler::metrics))
        .layer(CorsLayer::permissive())
        .with_state(state.clone());

    // 收到 SIGTERM / SIGINT 后停止接收新请求，并等待在途请求处理完成
    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    // 把 Kafka 本地队列中尚未发送的消息刷出去，避免退出时丢数据
    tracing::info!(
        timeout_ms = cfg.kafka.shutdown_flush_ms,
        "HTTP 服务已停止，等待 Kafka 队列刷盘"
    );
    if let Err(e) = state.producer.flush(flush_timeout) {
        tracing::warn!(error = %e, "Kafka 队列刷盘未完成");
    } else {
        tracing::info!("Kafka 队列已刷盘");
    }
    tracing::info!("taotie 已退出");

    serve_result.context("HTTP 服务异常退出")
}

/// 等待退出信号：Ctrl+C（SIGINT）或 SIGTERM（systemctl stop / kill）
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "监听 SIGINT 失败");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "监听 SIGTERM 失败");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("收到退出信号，停止接收新请求");
}

/// 构造日志过滤器。
///
/// 未设置 `RUST_LOG` 时使用配置文件中的级别，并关闭 librdkafka 自身的
/// 连接/重试日志（Kafka 故障时每 0.5 秒刷屏），只保留业务侧的「kafka 投递失败」。
/// 需要排查 librdkafka 内部问题时，可用 `RUST_LOG=librdkafka=debug` 覆盖。
fn build_filter(level: &str) -> EnvFilter {
    if let Ok(filter) = EnvFilter::try_from_default_env() {
        return filter;
    }
    EnvFilter::try_new(format!("{level},librdkafka=off,rdkafka=off"))
        .unwrap_or_else(|_| EnvFilter::new("info,librdkafka=off,rdkafka=off"))
}

/// 初始化日志：控制台 + 按天滚动的文件日志（`<dir>/<前缀>.YYYY-MM-DD.log`）。
///
/// 返回的 guard 必须在 main 中一直持有，程序退出时用于把缓冲日志刷入文件。
fn init_logging(cfg: &LogConfig) -> Option<WorkerGuard> {
    let filter = build_filter(&cfg.level);

    // 不需要文件日志时（如 K8s 只采集 stdout），跳过文件输出
    if !cfg.file {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer())
            .try_init();
        return None;
    }

    let file_layer = match RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(&cfg.file_prefix)
        .filename_suffix("log")
        .build(&cfg.dir)
    {
        Ok(appender) => {
            let (writer, guard) = tracing_appender::non_blocking(appender);
            let layer = fmt::layer().with_writer(writer).with_ansi(false);
            Some((layer, guard))
        }
        Err(e) => {
            eprintln!("初始化文件日志失败（{e}），日志仅输出到控制台");
            None
        }
    };

    let (file_layer, guard) = match file_layer {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .with(file_layer)
        .try_init();

    guard
}
