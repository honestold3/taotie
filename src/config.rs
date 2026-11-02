use std::env;
use std::fs;

use anyhow::{Context, Result};
use serde::Deserialize;

/// 默认配置文件路径，可用环境变量 TAOTIE_CONFIG 覆盖
const CONFIG_PATH: &str = "config.toml";

/// 应用配置
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// HTTP 监听地址
    pub listen_addr: String,
    /// Kafka 配置
    pub kafka: KafkaConfig,
    /// 日志配置
    pub log: LogConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".to_string(),
            kafka: KafkaConfig::default(),
            log: LogConfig::default(),
        }
    }
}

/// Kafka 配置
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// 集群地址
    pub brokers: String,
    /// 埋点数据写入的 topic
    pub topic: String,
    /// 消息 key
    pub key: String,
    /// ack 级别
    pub acks: String,
    /// 批量发送的最大等待时间（毫秒），对应 queue.buffering.max.ms
    pub queue_buffering_ms: u64,
    /// 单条消息最长投递时间（毫秒），超时按投递失败处理（返回 503）
    pub send_timeout_ms: u64,
    /// 本地队列满时的等待时间（毫秒），超过则直接返回 503
    pub queue_timeout_ms: u64,
    /// 消息压缩算法：lz4 / snappy / zstd / gzip / none
    pub compression: String,
    /// 优雅退出时等待队列刷盘的最长时间（毫秒）
    pub shutdown_flush_ms: u64,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "bury-track".to_string(),
            key: "track".to_string(),
            acks: "1".to_string(),
            queue_buffering_ms: 5,
            send_timeout_ms: 3_000,
            queue_timeout_ms: 100,
            compression: "lz4".to_string(),
            shutdown_flush_ms: 5_000,
        }
    }
}

/// 日志配置
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    /// 日志级别：trace / debug / info / warn / error
    pub level: String,
    /// 日志目录
    pub dir: String,
    /// 日志文件名前缀，按天滚动为 `<前缀>.YYYY-MM-DD.log`
    pub file_prefix: String,
    /// 是否写文件日志；K8s 等只采集 stdout 的环境建议设为 false
    pub file: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            dir: "logs".to_string(),
            file_prefix: "taotie".to_string(),
            file: true,
        }
    }
}

impl AppConfig {
    /// 读取配置文件；文件不存在时使用内置默认配置
    pub fn load() -> Result<Self> {
        let path = env::var("TAOTIE_CONFIG").unwrap_or_else(|_| CONFIG_PATH.to_string());
        match fs::read_to_string(&path) {
            Ok(content) => {
                toml::from_str(&content).with_context(|| format!("配置文件 {path} 解析失败"))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("未找到配置文件 {path}，使用内置默认配置");
                Ok(Self::default())
            }
            Err(e) => Err(e).with_context(|| format!("读取配置文件 {path} 失败")),
        }
    }
}
