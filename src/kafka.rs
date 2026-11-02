use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::Producer as _;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::config::KafkaConfig;

/// Kafka 生产者封装
pub struct Producer {
    inner: FutureProducer,
    /// 目标 topic
    topic: String,
    /// 消息 key
    key: String,
    /// 本地队列满时的等待时间
    queue_timeout: Duration,
}

impl Producer {
    pub fn new(cfg: &KafkaConfig) -> Result<Self> {
        let inner: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)
            .set("queue.buffering.max.ms", cfg.queue_buffering_ms.to_string())
            .set("acks", &cfg.acks)
            .set("compression.type", &cfg.compression)
            // 限制单条消息的投递时间，保证 Kafka 不可达时能及时返回 503
            .set("message.timeout.ms", cfg.send_timeout_ms.to_string())
            .create()
            .context("创建 Kafka producer 失败")?;

        Ok(Self {
            inner,
            topic: cfg.topic.clone(),
            key: cfg.key.clone(),
            queue_timeout: Duration::from_millis(cfg.queue_timeout_ms),
        })
    }

    /// 投递一条消息，等待 broker ack；失败（队列满、超时、broker 不可达等）返回错误
    pub async fn send(&self, payload: &[u8]) -> Result<()> {
        let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(&self.topic)
            .key(self.key.as_str())
            .payload(payload);

        self.inner
            .send(record, self.queue_timeout)
            .await
            .map(|_| ())
            .map_err(|(e, _)| anyhow::Error::new(e))
    }

    /// 探测 Kafka 与目标 topic 是否可用（供 /readyz 使用）
    pub fn check_ready(&self, timeout: Duration) -> Result<()> {
        let metadata = self
            .inner
            .client()
            .fetch_metadata(Some(&self.topic), timeout)
            .context("获取 Kafka 元数据失败")?;

        let topic_ok = metadata
            .topics()
            .iter()
            .any(|topic| topic.name() == self.topic && topic.error().is_none());

        if topic_ok {
            Ok(())
        } else {
            anyhow::bail!("Kafka topic {} 不可用", self.topic)
        }
    }

    /// 优雅退出时把本地队列中尚未发送的消息刷给 broker，最多等待 `timeout`
    pub fn flush(&self, timeout: Duration) -> Result<()> {
        self.inner
            .flush(timeout)
            .context("Kafka 队列刷盘超时，部分消息可能未发送")
    }
}
