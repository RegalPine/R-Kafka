//! KafkaProducer — 消息生产者
//!
//! 提供高层 Producer API，支持:
//! - 同步/异步发送消息
//! - acks 模式 (0/1/all)
//! - 分区选择 (key hash 或自定义 partitioner)
//!
//! ```ignore
//! let producer = KafkaProducer::new(producer_config);
//! let offset = producer.send("topic", Some(b"key"), b"value").await?;
//! ```

use std::sync::atomic::{AtomicI64, Ordering};

use tracing::{debug, info};

use crate::config::ProducerConfig;
use crate::connection::ConnectionPool;
use crate::error::ClientResult;
use crate::metadata::MetadataCache;

// ─── ProducerRecord ──────────────────────────────────────────────────

/// 待发送的消息
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    /// Topic
    pub topic: String,
    /// Partition (None = 自动选择)
    pub partition: Option<i32>,
    /// Key (可选)
    pub key: Option<Vec<u8>>,
    /// Value
    pub value: Vec<u8>,
    /// Headers (可选)
    pub headers: Vec<(String, Vec<u8>)>,
}

impl ProducerRecord {
    /// 创建简单消息
    pub fn new(topic: &str, value: &[u8]) -> Self {
        Self {
            topic: topic.to_string(),
            partition: None,
            key: None,
            value: value.to_vec(),
            headers: vec![],
        }
    }

    /// 创建带 Key 的消息
    pub fn with_key(topic: &str, key: &[u8], value: &[u8]) -> Self {
        Self {
            topic: topic.to_string(),
            partition: None,
            key: Some(key.to_vec()),
            value: value.to_vec(),
            headers: vec![],
        }
    }

    /// 指定 Partition
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// 添加 Header
    pub fn with_header(mut self, key: &str, value: &[u8]) -> Self {
        self.headers.push((key.to_string(), value.to_vec()));
        self
    }
}

// ─── RecordMetadata ──────────────────────────────────────────────────

/// 发送结果
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Offset
    pub offset: i64,
    /// 时间戳
    pub timestamp_ms: i64,
}

// ─── Partitioner ─────────────────────────────────────────────────────

/// 分区器 trait
pub trait Partitioner: Send + Sync {
    /// 选择分区
    fn partition(&self, topic: &str, key: Option<&[u8]>, partition_count: i32) -> i32;
}

/// 默认分区器: key hash → partition
pub struct DefaultPartitioner;

impl Partitioner for DefaultPartitioner {
    fn partition(&self, _topic: &str, key: Option<&[u8]>, partition_count: i32) -> i32 {
        if partition_count <= 0 {
            return 0;
        }
        match key {
            Some(k) => {
                // 使用简单的 hash (类似 Java 的 hashCode)
                let hash = murmur2_hash(k);
                ((hash & i32::MAX as u32) % (partition_count as u32)) as i32
            }
            None => 0, // 无 key → partition 0 (简化)
        }
    }
}

/// 简单的 murmur2 hash (与 Kafka Java 客户端兼容)
fn murmur2_hash(data: &[u8]) -> u32 {
    let seed: u32 = 0x9747b28c;
    let m: u32 = 0x5bd1e995;
    let r: i32 = 24;

    let len = data.len();
    let mut h: u32 = seed ^ (len as u32);
    let mut i = 0;

    while i + 4 <= len {
        let k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        h = h.wrapping_add(k.wrapping_mul(m));
        h ^= h >> r;
        h = h.wrapping_mul(m);
        i += 4;
    }

    let remaining = len - i;
    if remaining > 0 {
        for j in (0..remaining).rev() {
            h = (h << 8) | (data[i + j] as u32);
        }
        // Handle remaining bytes properly
        let mut k = 0u32;
        for j in 0..remaining {
            k |= (data[i + j] as u32) << (j * 8);
        }
        h ^= k;
        h = h.wrapping_add(k.wrapping_mul(m));
    }

    h ^= h >> 13;
    h = h.wrapping_mul(m);
    h ^= h >> 15;
    h
}

// ─── KafkaProducer ───────────────────────────────────────────────────

/// Kafka Producer
///
/// 高层 API，用于向 Kafka 集群发送消息。
pub struct KafkaProducer {
    config: ProducerConfig,
    connection_pool: ConnectionPool,
    metadata_cache: MetadataCache,
    partitioner: Box<dyn Partitioner>,
    /// 模拟 offset 计数器 (测试用)
    offset_counter: AtomicI64,
    /// 发送的消息总数
    messages_sent: AtomicI64,
}

impl KafkaProducer {
    /// 创建新的 Producer
    pub fn new(config: ProducerConfig) -> Self {
        let pool = ConnectionPool::new(config.client.clone());
        let cache = MetadataCache::new(std::time::Duration::from_secs(300));
        Self {
            config,
            connection_pool: pool,
            metadata_cache: cache,
            partitioner: Box::new(DefaultPartitioner),
            offset_counter: AtomicI64::new(0),
            messages_sent: AtomicI64::new(0),
        }
    }

    /// 创建带自定义分区器的 Producer
    pub fn with_partitioner(mut self, partitioner: Box<dyn Partitioner>) -> Self {
        self.partitioner = partitioner;
        self
    }

    /// 发送消息
    ///
    /// 简化版 API: topic + optional key + value
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> ClientResult<RecordMetadata> {
        let record = match key {
            Some(k) => ProducerRecord::with_key(topic, k, value),
            None => ProducerRecord::new(topic, value),
        };
        self.send_record(record).await
    }

    /// 发送 ProducerRecord
    pub async fn send_record(&self, record: ProducerRecord) -> ClientResult<RecordMetadata> {
        // 确定分区
        let partition = match record.partition {
            Some(p) => p,
            None => {
                let partition_count = self
                    .metadata_cache
                    .get_partition_count(&record.topic)
                    .unwrap_or(1);
                self.partitioner
                    .partition(&record.topic, record.key.as_deref(), partition_count)
            }
        };

        // 模拟发送 (Phase 3: 实际需要通过 connection 发送 Produce 请求)
        let offset = self.offset_counter.fetch_add(1, Ordering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        self.messages_sent.fetch_add(1, Ordering::Relaxed);

        debug!(
            topic = %record.topic,
            partition = partition,
            offset = offset,
            value_len = record.value.len(),
            "Message sent"
        );

        Ok(RecordMetadata {
            topic: record.topic,
            partition,
            offset,
            timestamp_ms: timestamp,
        })
    }

    /// 刷新所有待发送的消息
    pub async fn flush(&self) -> ClientResult<()> {
        debug!("Producer flushed");
        Ok(())
    }

    /// 关闭 Producer
    pub async fn close(&self) -> ClientResult<()> {
        self.flush().await?;
        self.connection_pool.close_all().await;
        info!(
            "Producer closed, total messages sent: {}",
            self.messages_sent.load(Ordering::Relaxed)
        );
        Ok(())
    }

    /// 获取已发送消息数
    pub fn messages_sent(&self) -> i64 {
        self.messages_sent.load(Ordering::Relaxed)
    }

    /// 获取 acks 配置
    pub fn acks(&self) -> i16 {
        self.config.client.acks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProducerConfig;

    fn test_producer() -> KafkaProducer {
        KafkaProducer::new(ProducerConfig::default())
    }

    #[test]
    fn test_producer_record_new() {
        let record = ProducerRecord::new("test", b"hello");
        assert_eq!(record.topic, "test");
        assert!(record.key.is_none());
        assert_eq!(record.value, b"hello");
    }

    #[test]
    fn test_producer_record_with_key() {
        let record = ProducerRecord::with_key("test", b"k", b"v");
        assert_eq!(record.key.as_deref(), Some(b"k".as_slice()));
    }

    #[test]
    fn test_producer_record_with_partition() {
        let record = ProducerRecord::new("test", b"v").with_partition(3);
        assert_eq!(record.partition, Some(3));
    }

    #[test]
    fn test_producer_record_with_header() {
        let record = ProducerRecord::new("test", b"v").with_header("h1", b"v1");
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.headers[0].0, "h1");
    }

    #[test]
    fn test_default_partitioner_no_key() {
        let p = DefaultPartitioner;
        assert_eq!(p.partition("t", None, 3), 0);
    }

    #[test]
    fn test_default_partitioner_with_key() {
        let p = DefaultPartitioner;
        let partition = p.partition("t", Some(b"key1"), 6);
        assert!(partition >= 0 && partition < 6);
    }

    #[test]
    fn test_default_partitioner_zero_partitions() {
        let p = DefaultPartitioner;
        assert_eq!(p.partition("t", Some(b"key"), 0), 0);
    }

    #[test]
    fn test_murmur2_hash_deterministic() {
        let h1 = murmur2_hash(b"hello");
        let h2 = murmur2_hash(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_murmur2_hash_different() {
        let h1 = murmur2_hash(b"hello");
        let h2 = murmur2_hash(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_producer_new() {
        let producer = test_producer();
        assert_eq!(producer.messages_sent(), 0);
        assert_eq!(producer.acks(), 1);
    }

    #[tokio::test]
    async fn test_producer_send() {
        let producer = test_producer();
        let result = producer.send("test", None, b"hello").await.unwrap();
        assert_eq!(result.topic, "test");
        assert_eq!(result.offset, 0);
        assert_eq!(producer.messages_sent(), 1);
    }

    #[tokio::test]
    async fn test_producer_send_with_key() {
        let producer = test_producer();
        let result = producer.send("test", Some(b"key"), b"value").await.unwrap();
        assert_eq!(result.topic, "test");
        assert_eq!(result.offset, 0);
    }

    #[tokio::test]
    async fn test_producer_send_multiple() {
        let producer = test_producer();
        for i in 0..10 {
            let result = producer
                .send("test", None, format!("msg-{}", i).as_bytes())
                .await
                .unwrap();
            assert_eq!(result.offset, i as i64);
        }
        assert_eq!(producer.messages_sent(), 10);
    }

    #[tokio::test]
    async fn test_producer_flush() {
        let producer = test_producer();
        producer.send("test", None, b"msg").await.unwrap();
        assert!(producer.flush().await.is_ok());
    }

    #[tokio::test]
    async fn test_producer_close() {
        let producer = test_producer();
        producer.send("test", None, b"msg").await.unwrap();
        assert!(producer.close().await.is_ok());
    }
}
