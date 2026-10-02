//! Batch Accumulator — 攒批优化器
//!
//! 将多个小的 Produce 请求聚合为大批次，减少磁盘 I/O 次数，提升写入吞吐。
//!
//! 触发刷出条件 (先到先触发):
//! 1. **大小触发**: 累积字节数 >= `max_batch_bytes` (默认 1MB)
//! 2. **时间触发**: 首条记录存活时间 >= `linger_ms` (默认 5ms)
//! 3. **强制刷出**: 调用 `force_flush()` 立即返回所有待发送批次
//!
//! 架构:
//! ```text
//! Produce Request ──► BatchAccumulatorManager
//!                         │
//!                         ├── (topic-A, 0) → PendingBatch { bytes, first_append_time }
//!                         ├── (topic-A, 1) → PendingBatch { ... }
//!                         └── (topic-B, 0) → PendingBatch { ... }
//!                         │
//!                    drain_ready() / force_flush()
//!                         │
//!                         ▼
//!                    PartitionManager → CommitLog
//! ```

use std::collections::HashMap;
use std::time::Instant;

use rk_core::types::{PartitionId, TopicName};

// ─── 配置 ────────────────────────────────────────────────────────────

/// 攒批配置
#[derive(Debug, Clone)]
pub struct AccumulatorConfig {
    /// 单批次最大字节数 (默认 1MB)
    pub max_batch_bytes: usize,
    /// 最大等待时间 (毫秒，默认 5ms)
    pub linger_ms: u64,
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            max_batch_bytes: 1_048_576, // 1MB
            linger_ms: 5,
        }
    }
}

// ─── 待发送批次 ──────────────────────────────────────────────────────

/// 就绪的批次，可以写入 CommitLog
#[derive(Debug, Clone)]
pub struct BatchReady {
    /// Topic 名称
    pub topic: TopicName,
    /// Partition ID
    pub partition: PartitionId,
    /// 聚合后的 record_set 字节
    pub record_set: Vec<u8>,
    /// 包含的子批次数量
    pub sub_batch_count: usize,
}

/// 单个 (topic, partition) 的待发送缓冲
#[derive(Debug)]
struct PendingBatch {
    /// 累积的 record_set 字节
    buffer: Vec<u8>,
    /// 包含的子批次数量
    sub_batch_count: usize,
    /// 首次追加时间 (用于 linger 判定)
    first_append_time: Instant,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(4096),
            sub_batch_count: 0,
            first_append_time: Instant::now(),
        }
    }

    /// 追加一批 record_set 数据
    fn append(&mut self, record_set: &[u8]) {
        if self.sub_batch_count == 0 {
            self.first_append_time = Instant::now();
        }
        self.buffer.extend_from_slice(record_set);
        self.sub_batch_count += 1;
    }

    /// 当前累积字节数
    fn len(&self) -> usize {
        self.buffer.len()
    }

    /// 是否为空
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// 自首次追加以来的存活时间
    fn elapsed_ms(&self) -> u64 {
        self.first_append_time.elapsed().as_millis() as u64
    }

    /// 取出缓冲内容，重置为空的 PendingBatch
    fn take(&mut self) -> (Vec<u8>, usize) {
        let buffer = std::mem::replace(&mut self.buffer, Vec::with_capacity(4096));
        let count = self.sub_batch_count;
        self.sub_batch_count = 0;
        (buffer, count)
    }
}

// ─── Partition Key ───────────────────────────────────────────────────

/// (topic, partition) 组合键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub topic: TopicName,
    pub partition: PartitionId,
}

impl PartitionKey {
    pub fn new(topic: impl Into<TopicName>, partition: impl Into<PartitionId>) -> Self {
        Self {
            topic: topic.into(),
            partition: partition.into(),
        }
    }
}

// ─── Batch Accumulator Manager ───────────────────────────────────────

/// 攒批管理器
///
/// 管理所有 (topic, partition) 的待发送缓冲。
/// 线程安全: 非并发安全，需要外部同步 (通常由单个 Produce 路径独占使用)。
pub struct BatchAccumulatorManager {
    /// 配置
    config: AccumulatorConfig,
    /// 按 (topic, partition) 索引的待发送缓冲
    pending: HashMap<PartitionKey, PendingBatch>,
    /// 统计: 总追加次数
    total_appends: u64,
    /// 统计: 总刷出次数
    total_flushes: u64,
    /// 统计: 总刷出字节数
    total_flushed_bytes: u64,
}

impl BatchAccumulatorManager {
    /// 创建新的攒批管理器
    pub fn new(config: AccumulatorConfig) -> Self {
        Self {
            config,
            pending: HashMap::new(),
            total_appends: 0,
            total_flushes: 0,
            total_flushed_bytes: 0,
        }
    }

    /// 追加一批 record_set 到指定 (topic, partition) 的缓冲区
    ///
    /// 返回追加后该分区缓冲区的当前大小
    pub fn append(
        &mut self,
        topic: impl Into<TopicName>,
        partition: impl Into<PartitionId>,
        record_set: &[u8],
    ) -> usize {
        let key = PartitionKey::new(topic, partition);
        let pending = self.pending.entry(key).or_insert_with(PendingBatch::new);
        pending.append(record_set);
        self.total_appends += 1;
        pending.len()
    }

    /// 检查指定分区是否已就绪 (大小触发 或 时间触发)
    pub fn is_ready(&self, topic: &str, partition: impl Into<PartitionId>) -> bool {
        let key = PartitionKey::new(topic, partition);
        match self.pending.get(&key) {
            Some(pending) if !pending.is_empty() => {
                pending.len() >= self.config.max_batch_bytes
                    || pending.elapsed_ms() >= self.config.linger_ms
            }
            _ => false,
        }
    }

    /// 排空所有就绪的批次
    ///
    /// 就绪条件: 缓冲区非空 且 (大小 >= max_batch_bytes 或 存活时间 >= linger_ms)
    pub fn drain_ready(&mut self) -> Vec<BatchReady> {
        let config = &self.config;
        let mut ready = Vec::new();

        for (key, pending) in self.pending.iter_mut() {
            if pending.is_empty() {
                continue;
            }
            let size_triggered = pending.len() >= config.max_batch_bytes;
            let time_triggered = pending.elapsed_ms() >= config.linger_ms;

            if size_triggered || time_triggered {
                let (buffer, count) = pending.take();
                self.total_flushes += 1;
                self.total_flushed_bytes += buffer.len() as u64;
                ready.push(BatchReady {
                    topic: key.topic.clone(),
                    partition: key.partition,
                    record_set: buffer,
                    sub_batch_count: count,
                });
            }
        }

        ready
    }

    /// 强制刷出所有非空缓冲区 (不论大小和时间)
    pub fn force_flush(&mut self) -> Vec<BatchReady> {
        let mut ready = Vec::new();

        for (key, pending) in self.pending.iter_mut() {
            if pending.is_empty() {
                continue;
            }
            let (buffer, count) = pending.take();
            self.total_flushes += 1;
            self.total_flushed_bytes += buffer.len() as u64;
            ready.push(BatchReady {
                topic: key.topic.clone(),
                partition: key.partition,
                record_set: buffer,
                sub_batch_count: count,
            });
        }

        ready
    }

    /// 获取指定分区的待发送字节数
    pub fn pending_bytes(&self, topic: &str, partition: impl Into<PartitionId>) -> usize {
        let key = PartitionKey::new(topic, partition);
        self.pending.get(&key).map_or(0, |p| p.len())
    }

    /// 获取所有分区的总待发送字节数
    pub fn total_pending_bytes(&self) -> usize {
        self.pending.values().map(|p| p.len()).sum()
    }

    /// 获取有数据待发送的分区数量
    pub fn pending_partition_count(&self) -> usize {
        self.pending.values().filter(|p| !p.is_empty()).count()
    }

    /// 获取配置
    pub fn config(&self) -> &AccumulatorConfig {
        &self.config
    }

    /// 获取统计: 总追加次数
    pub fn total_appends(&self) -> u64 {
        self.total_appends
    }

    /// 获取统计: 总刷出次数
    pub fn total_flushes(&self) -> u64 {
        self.total_flushes
    }

    /// 获取统计: 总刷出字节数
    pub fn total_flushed_bytes(&self) -> u64 {
        self.total_flushed_bytes
    }

    /// 获取攒批比率 (刷出字节数 / 追加次数)，越高说明攒批效果越好
    pub fn batch_ratio(&self) -> f64 {
        if self.total_flushes == 0 {
            return 0.0;
        }
        self.total_flushed_bytes as f64 / self.total_flushes as f64
    }

    /// 清除所有待发送数据 (用于测试或重置)
    pub fn clear(&mut self) {
        self.pending.clear();
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::types::{PartitionId, TopicName};

    fn make_config(max_bytes: usize, linger_ms: u64) -> AccumulatorConfig {
        AccumulatorConfig {
            max_batch_bytes: max_bytes,
            linger_ms,
        }
    }

    fn make_record_set(size: usize) -> Vec<u8> {
        vec![0xABu8; size]
    }

    // --- 基础追加与刷出 ---

    #[test]
    fn test_append_and_force_flush() {
        let mut mgr = BatchAccumulatorManager::new(make_config(1024, 5));

        let batch = make_record_set(100);
        let pending = mgr.append("topic-a", 0, &batch);
        assert_eq!(pending, 100);
        assert_eq!(mgr.total_pending_bytes(), 100);
        assert_eq!(mgr.pending_partition_count(), 1);

        // force_flush 应该返回数据
        let ready = mgr.force_flush();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].topic, TopicName("topic-a".to_string()));
        assert_eq!(ready[0].partition, PartitionId(0));
        assert_eq!(ready[0].record_set.len(), 100);
        assert_eq!(ready[0].sub_batch_count, 1);

        // flush 后应该为空
        assert_eq!(mgr.total_pending_bytes(), 0);
        assert_eq!(mgr.pending_partition_count(), 0);
    }

    #[test]
    fn test_multiple_appends_same_partition() {
        let mut mgr = BatchAccumulatorManager::new(make_config(1024, 1000));

        // 追加 3 批到同一分区
        mgr.append("topic-a", 0, &make_record_set(100));
        mgr.append("topic-a", 0, &make_record_set(200));
        mgr.append("topic-a", 0, &make_record_set(150));

        assert_eq!(mgr.pending_bytes("topic-a", 0), 450);
        assert_eq!(mgr.pending_partition_count(), 1);
        assert_eq!(mgr.total_appends(), 3);

        // force_flush 应该聚合为 1 批
        let ready = mgr.force_flush();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].record_set.len(), 450);
        assert_eq!(ready[0].sub_batch_count, 3);
    }

    #[test]
    fn test_multiple_partitions() {
        let mut mgr = BatchAccumulatorManager::new(make_config(1024, 1000));

        mgr.append("topic-a", 0, &make_record_set(100));
        mgr.append("topic-a", 1, &make_record_set(200));
        mgr.append("topic-b", 0, &make_record_set(300));

        assert_eq!(mgr.total_pending_bytes(), 600);
        assert_eq!(mgr.pending_partition_count(), 3);

        let ready = mgr.force_flush();
        assert_eq!(ready.len(), 3);
    }

    // --- 大小触发 ---

    #[test]
    fn test_size_trigger() {
        let mut mgr = BatchAccumulatorManager::new(make_config(200, 60_000)); // linger 很长

        // 追加 100 字节，不应就绪
        mgr.append("topic-a", 0, &make_record_set(100));
        assert!(!mgr.is_ready("topic-a", 0));

        let ready = mgr.drain_ready();
        assert!(ready.is_empty(), "Should not be ready yet");

        // 再追加 100 字节，达到 200 = max_batch_bytes，应该就绪
        mgr.append("topic-a", 0, &make_record_set(100));
        assert!(mgr.is_ready("topic-a", 0));

        let ready = mgr.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].record_set.len(), 200);
        assert_eq!(ready[0].sub_batch_count, 2);
    }

    // --- 时间触发 ---

    #[test]
    fn test_time_trigger() {
        // linger_ms = 0 意味着立即就绪
        let mut mgr = BatchAccumulatorManager::new(make_config(1_048_576, 0));

        mgr.append("topic-a", 0, &make_record_set(50));

        // linger_ms = 0，任何非空批次都应立即就绪
        assert!(mgr.is_ready("topic-a", 0));
        let ready = mgr.drain_ready();
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn test_linger_timeout() {
        // linger_ms = 1，等待 2ms 后应该就绪
        let mut mgr = BatchAccumulatorManager::new(make_config(1_048_576, 1));

        mgr.append("topic-a", 0, &make_record_set(50));

        // 不应立即就绪 (刚追加)
        // 注意: 在极快的机器上可能已经 >= 1ms，所以先检查 drain
        let _ready = mgr.drain_ready();
        // 可能为空也可能已就绪 (取决于时间)，不做严格断言

        // 等待足够时间
        std::thread::sleep(std::time::Duration::from_millis(5));
        let ready = mgr.drain_ready();
        assert_eq!(ready.len(), 1, "Should be ready after linger timeout");
    }

    // --- drain_ready vs force_flush ---

    #[test]
    fn test_drain_ready_respects_thresholds() {
        let mut mgr = BatchAccumulatorManager::new(make_config(1000, 60_000));

        // 小批次，不应就绪
        mgr.append("topic-a", 0, &make_record_set(50));
        assert!(mgr.drain_ready().is_empty());

        // 达到大小阈值
        mgr.append("topic-a", 0, &make_record_set(950));
        let ready = mgr.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].record_set.len(), 1000);

        // 另一个分区仍然 pending
        mgr.append("topic-b", 0, &make_record_set(30));
        assert!(mgr.drain_ready().is_empty());
        assert_eq!(mgr.pending_bytes("topic-b", 0), 30);
    }

    #[test]
    fn test_force_flush_ignores_thresholds() {
        let mut mgr = BatchAccumulatorManager::new(make_config(1_048_576, 60_000));

        mgr.append("topic-a", 0, &make_record_set(10));
        mgr.append("topic-b", 0, &make_record_set(20));

        // drain_ready 不会返回 (太小太新)
        assert!(mgr.drain_ready().is_empty());

        // force_flush 会返回所有
        let ready = mgr.force_flush();
        assert_eq!(ready.len(), 2);
    }

    // --- 空批次处理 ---

    #[test]
    fn test_empty_batch_not_flushed() {
        let mut mgr = BatchAccumulatorManager::new(make_config(1024, 0));

        // 没有追加任何数据
        let ready = mgr.force_flush();
        assert!(ready.is_empty());

        let ready = mgr.drain_ready();
        assert!(ready.is_empty());
    }

    #[test]
    fn test_clear() {
        let mut mgr = BatchAccumulatorManager::new(make_config(1024, 1000));

        mgr.append("topic-a", 0, &make_record_set(100));
        mgr.append("topic-b", 0, &make_record_set(200));
        assert_eq!(mgr.total_pending_bytes(), 300);

        mgr.clear();
        assert_eq!(mgr.total_pending_bytes(), 0);
        assert_eq!(mgr.pending_partition_count(), 0);
    }

    // --- 统计 ---

    #[test]
    fn test_statistics() {
        let mut mgr = BatchAccumulatorManager::new(make_config(100, 1000));

        mgr.append("topic-a", 0, &make_record_set(50));
        mgr.append("topic-a", 0, &make_record_set(60)); // 达到 110 >= 100

        assert_eq!(mgr.total_appends(), 2);
        assert_eq!(mgr.total_flushes(), 0);

        let ready = mgr.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(mgr.total_flushes(), 1);
        assert_eq!(mgr.total_flushed_bytes(), 110);
        assert!(mgr.batch_ratio() > 0.0);
    }

    // --- PartitionKey ---

    #[test]
    fn test_partition_key_equality() {
        let k1 = PartitionKey::new("topic-a", 0);
        let k2 = PartitionKey::new("topic-a", 0);
        let k3 = PartitionKey::new("topic-a", 1);
        let k4 = PartitionKey::new("topic-b", 0);

        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k1, k4);
    }

    // --- 默认配置 ---

    #[test]
    fn test_default_config() {
        let config = AccumulatorConfig::default();
        assert_eq!(config.max_batch_bytes, 1_048_576);
        assert_eq!(config.linger_ms, 5);
    }

    // --- 混合场景 ---

    #[test]
    fn test_mixed_scenario() {
        let mut mgr = BatchAccumulatorManager::new(make_config(200, 60_000));

        // 分区 0: 追加 200 字节 (大小触发)
        mgr.append("topic-a", 0, &make_record_set(100));
        mgr.append("topic-a", 0, &make_record_set(100));

        // 分区 1: 追加 50 字节 (不触发)
        mgr.append("topic-a", 1, &make_record_set(50));

        // 分区 2: 追加 300 字节 (大小触发)
        mgr.append("topic-b", 0, &make_record_set(300));

        let ready = mgr.drain_ready();
        // 分区 0 和 分区 2 应该就绪
        assert_eq!(ready.len(), 2);

        // 分区 1 仍然 pending
        assert_eq!(mgr.pending_bytes("topic-a", 1), 50);
        assert_eq!(mgr.pending_partition_count(), 1);
    }

    #[test]
    fn test_sequential_flush_cycles() {
        let mut mgr = BatchAccumulatorManager::new(make_config(100, 60_000));

        // 第一轮: 攒批 + 刷出
        mgr.append("topic-a", 0, &make_record_set(100));
        let ready = mgr.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(mgr.total_pending_bytes(), 0);

        // 第二轮: 再次攒批 + 刷出
        mgr.append("topic-a", 0, &make_record_set(150));
        let ready = mgr.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].record_set.len(), 150);

        assert_eq!(mgr.total_flushes(), 2);
    }
}
