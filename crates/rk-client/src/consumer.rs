//! KafkaConsumer — 消息消费者
//!
//! 提供高层 Consumer API，支持:
//! - 消费组订阅
//! - poll 拉取消息
//! - 手动/自动偏移量提交
//!
//! ```ignore
//! let consumer = KafkaConsumer::new(consumer_config);
//! consumer.subscribe(&["topic1", "topic2"])?;
//! loop {
//!     let records = consumer.poll(Duration::from_millis(100)).await?;
//!     for record in records {
//!         println!("{}: {:?}", record.key, record.value);
//!     }
//!     consumer.commit_sync().await?;
//! }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use tracing::{debug, info};

use crate::config::ConsumerConfig;
use crate::connection::ConnectionPool;
use crate::error::{ClientError, ClientResult};
use crate::metadata::{MetadataCache, TopicPartition};

// ─── ConsumerRecord ──────────────────────────────────────────────────

/// 消费到的消息
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// Topic
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Offset
    pub offset: i64,
    /// Key
    pub key: Option<Vec<u8>>,
    /// Value
    pub value: Vec<u8>,
    /// Headers
    pub headers: Vec<(String, Vec<u8>)>,
    /// 时间戳
    pub timestamp_ms: i64,
}

// ─── OffsetStore ─────────────────────────────────────────────────────

/// 偏移量存储
///
/// 管理每个 Partition 的已消费偏移量。
struct OffsetStore {
    /// 已消费偏移量: (topic, partition) → next_offset
    committed: HashMap<TopicPartition, i64>,
    /// 已拉取但未提交的偏移量
    position: HashMap<TopicPartition, i64>,
}

impl OffsetStore {
    fn new() -> Self {
        Self {
            committed: HashMap::new(),
            position: HashMap::new(),
        }
    }

    fn set_position(&mut self, tp: TopicPartition, offset: i64) {
        self.position.insert(tp, offset);
    }

    fn get_position(&self, tp: &TopicPartition) -> Option<i64> {
        self.position.get(tp).copied()
    }

    fn commit(&mut self, tp: &TopicPartition, offset: i64) {
        self.committed.insert(tp.clone(), offset);
    }

    fn commit_all_positions(&mut self) {
        for (tp, &offset) in &self.position {
            self.committed.insert(tp.clone(), offset);
        }
    }

    fn get_committed(&self, tp: &TopicPartition) -> Option<i64> {
        self.committed.get(tp).copied()
    }
}

// ─── KafkaConsumer ───────────────────────────────────────────────────

/// Kafka Consumer
///
/// 高层 API，用于从 Kafka 集群消费消息。
pub struct KafkaConsumer {
    config: ConsumerConfig,
    connection_pool: ConnectionPool,
    _metadata_cache: MetadataCache,
    /// 订阅的 Topic 列表
    subscriptions: Vec<String>,
    /// 偏移量管理
    offset_store: OffsetStore,
    /// 模拟 offset 计数器
    poll_counter: AtomicI64,
    /// 总消费消息数
    records_consumed: AtomicI64,
}

impl KafkaConsumer {
    /// 创建新的 Consumer
    pub fn new(config: ConsumerConfig) -> Self {
        let pool = ConnectionPool::new(config.client.clone());
        let cache = MetadataCache::new(Duration::from_secs(300));
        Self {
            config,
            connection_pool: pool,
            _metadata_cache: cache,
            subscriptions: vec![],
            offset_store: OffsetStore::new(),
            poll_counter: AtomicI64::new(0),
            records_consumed: AtomicI64::new(0),
        }
    }

    /// 订阅 Topic 列表
    pub fn subscribe(&mut self, topics: &[&str]) -> ClientResult<()> {
        if topics.is_empty() {
            return Err(ClientError::Config(
                "Must subscribe to at least one topic".to_string(),
            ));
        }
        self.subscriptions = topics.iter().map(|t| t.to_string()).collect();
        info!(topics = ?self.subscriptions, group = %self.config.group_id, "Subscribed to topics");
        Ok(())
    }

    /// 取消订阅
    pub fn unsubscribe(&mut self) {
        self.subscriptions.clear();
        info!("Unsubscribed from all topics");
    }

    /// 拉取消息
    ///
    /// 从订阅的 Topic 拉取消息，最多返回 `max_poll_records` 条。
    pub async fn poll(&mut self, _timeout: Duration) -> ClientResult<Vec<ConsumerRecord>> {
        if self.subscriptions.is_empty() {
            return Err(ClientError::Config(
                "Not subscribed to any topic".to_string(),
            ));
        }

        // Phase 3: 模拟返回消息 (实际需要通过 Fetch API 拉取)
        let counter = self.poll_counter.fetch_add(1, Ordering::Relaxed);
        let mut records = vec![];

        for topic in &self.subscriptions {
            let tp = TopicPartition::new(topic, 0);
            let offset = self.offset_store.get_position(&tp).unwrap_or(0);

            // 模拟: 每次 poll 返回一条消息
            let record = ConsumerRecord {
                topic: topic.clone(),
                partition: 0,
                offset,
                key: Some(format!("key-{}", counter).into_bytes()),
                value: format!("value-{}", counter).into_bytes(),
                headers: vec![],
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            };
            records.push(record);

            // 更新位置
            self.offset_store.set_position(tp, offset + 1);
        }

        self.records_consumed
            .fetch_add(records.len() as i64, Ordering::Relaxed);
        debug!(records = records.len(), "Polled records");
        Ok(records)
    }

    /// 同步提交偏移量
    pub async fn commit_sync(&mut self) -> ClientResult<()> {
        self.offset_store.commit_all_positions();
        debug!("Offsets committed synchronously");
        Ok(())
    }

    /// 异步提交偏移量
    pub async fn commit_async(&mut self) -> ClientResult<()> {
        self.offset_store.commit_all_positions();
        debug!("Offsets committed asynchronously");
        Ok(())
    }

    /// 手动提交指定偏移量
    pub async fn commit_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> ClientResult<()> {
        let tp = TopicPartition::new(topic, partition);
        self.offset_store.commit(&tp, offset);
        debug!(
            topic = topic,
            partition = partition,
            offset = offset,
            "Offset committed"
        );
        Ok(())
    }

    /// 获取指定 Partition 的当前偏移量
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        let tp = TopicPartition::new(topic, partition);
        self.offset_store.get_position(&tp)
    }

    /// 获取已提交的偏移量
    pub fn committed(&self, topic: &str, partition: i32) -> Option<i64> {
        let tp = TopicPartition::new(topic, partition);
        self.offset_store.get_committed(&tp)
    }

    /// 关闭 Consumer
    pub async fn close(&self) -> ClientResult<()> {
        self.connection_pool.close_all().await;
        info!(
            group = %self.config.group_id,
            total_records = self.records_consumed.load(Ordering::Relaxed),
            "Consumer closed"
        );
        Ok(())
    }

    /// 获取订阅的 Topic
    pub fn subscription(&self) -> &[String] {
        &self.subscriptions
    }

    /// 获取消费组 ID
    pub fn group_id(&self) -> &str {
        &self.config.group_id
    }

    /// 获取总消费消息数
    pub fn records_consumed(&self) -> i64 {
        self.records_consumed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConsumerConfig;

    fn test_consumer() -> KafkaConsumer {
        KafkaConsumer::new(ConsumerConfig::default())
    }

    #[test]
    fn test_consumer_new() {
        let consumer = test_consumer();
        assert_eq!(consumer.group_id(), "default-group");
        assert!(consumer.subscription().is_empty());
        assert_eq!(consumer.records_consumed(), 0);
    }

    #[test]
    fn test_subscribe() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["t1", "t2"]).unwrap();
        assert_eq!(consumer.subscription().len(), 2);
    }

    #[test]
    fn test_subscribe_empty_fails() {
        let mut consumer = test_consumer();
        assert!(consumer.subscribe(&[]).is_err());
    }

    #[test]
    fn test_unsubscribe() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["t1"]).unwrap();
        consumer.unsubscribe();
        assert!(consumer.subscription().is_empty());
    }

    #[tokio::test]
    async fn test_poll_without_subscribe_fails() {
        let mut consumer = test_consumer();
        let result = consumer.poll(Duration::from_millis(100)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_poll_returns_records() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["test"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].topic, "test");
        assert_eq!(records[0].partition, 0);
        assert_eq!(records[0].offset, 0);
    }

    #[tokio::test]
    async fn test_poll_increments_offset() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["test"]).unwrap();

        let r1 = consumer.poll(Duration::from_millis(100)).await.unwrap();
        assert_eq!(r1[0].offset, 0);

        let r2 = consumer.poll(Duration::from_millis(100)).await.unwrap();
        assert_eq!(r2[0].offset, 1);
    }

    #[tokio::test]
    async fn test_position_tracking() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["test"]).unwrap();

        assert_eq!(consumer.position("test", 0), None);

        consumer.poll(Duration::from_millis(100)).await.unwrap();
        assert_eq!(consumer.position("test", 0), Some(1));
    }

    #[tokio::test]
    async fn test_commit_sync() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["test"]).unwrap();
        consumer.poll(Duration::from_millis(100)).await.unwrap();
        consumer.commit_sync().await.unwrap();
        assert_eq!(consumer.committed("test", 0), Some(1));
    }

    #[tokio::test]
    async fn test_commit_offset() {
        let mut consumer = test_consumer();
        consumer.commit_offset("test", 0, 42).await.unwrap();
        assert_eq!(consumer.committed("test", 0), Some(42));
    }

    #[tokio::test]
    async fn test_records_consumed_count() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["t1", "t2"]).unwrap();
        consumer.poll(Duration::from_millis(100)).await.unwrap();
        assert_eq!(consumer.records_consumed(), 2); // 2 topics × 1 record each
    }

    #[tokio::test]
    async fn test_consumer_close() {
        let mut consumer = test_consumer();
        consumer.subscribe(&["test"]).unwrap();
        consumer.poll(Duration::from_millis(100)).await.unwrap();
        assert!(consumer.close().await.is_ok());
    }
}
