//! Replica Fetcher — Follower 网络复制器
//!
//! Follower 定期向 Leader 发送 Fetch 请求，拉取新数据并更新本地 LEO。
//! 模拟 Kafka ReplicaFetcherThread 的行为:
//!
//! ```text
//! ┌──────────────┐     Fetch Request      ┌──────────────┐
//! │  Follower     │ ──────────────────────→ │  Leader       │
//! │  ReplicaFetcher│                        │  (本地/远程)   │
//! │               │ ←────────────────────── │              │
//! └──────────────┘     Fetch Response       └──────────────┘
//!       │
//!       ▼
//!  更新本地 LEO → 通知 ISRTracker → HW 推进
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rk_core::error::{RkError, Result};
use rk_core::types::{BrokerId, Offset, PartitionId, TopicName};
use tracing::{debug, info, warn};

// ─── Fetcher 配置 ────────────────────────────────────────────────────

/// Replica Fetcher 配置
#[derive(Debug, Clone)]
pub struct ReplicaFetcherConfig {
    /// Fetch 间隔 (毫秒)
    pub fetch_interval_ms: u64,
    /// 每次 Fetch 最大字节数
    pub fetch_max_bytes: i32,
    /// Fetch 最小字节数 (低于此值则等待)
    pub fetch_min_bytes: i32,
    /// Fetch 最大等待时间 (毫秒)
    pub fetch_max_wait_ms: u64,
}

impl Default for ReplicaFetcherConfig {
    fn default() -> Self {
        Self {
            fetch_interval_ms: 500,
            fetch_max_bytes: 10 * 1024 * 1024, // 10MB
            fetch_min_bytes: 1,
            fetch_max_wait_ms: 500,
        }
    }
}

// ─── Fetch 请求/响应 ─────────────────────────────────────────────────

/// 复制 Fetch 请求 (Follower → Leader)
#[derive(Debug, Clone)]
pub struct ReplicationFetchRequest {
    /// 目标 Partition
    pub topic: TopicName,
    /// Partition ID
    pub partition: PartitionId,
    /// Follower 当前的 LEO (从该 offset 开始拉取)
    pub fetch_offset: Offset,
    /// Follower 的 broker_id
    pub replica_id: BrokerId,
    /// Leader Epoch (用于验证 Leader 有效性)
    pub current_leader_epoch: i32,
}

/// 复制 Fetch 响应 (Leader → Follower)
#[derive(Debug, Clone)]
pub struct ReplicationFetchResponse {
    /// 错误码 (0 = 成功)
    pub error_code: i16,
    /// Leader 的 HW (Follower 据此更新消费可见性)
    pub high_watermark: Offset,
    /// Leader 的 LEO (Follower 据此计算 lag)
    pub log_end_offset: Offset,
    /// 拉取到的数据批次 (简化为字节向量)
    pub record_batches: Vec<ReplicatedBatch>,
    /// 拉取到的总字节数
    pub fetched_bytes: i64,
}

/// 复制的数据批次 (简化表示)
#[derive(Debug, Clone)]
pub struct ReplicatedBatch {
    /// 起始 offset
    pub base_offset: Offset,
    /// 最后 offset (含)
    pub last_offset: Offset,
    /// 批次数据 (原始字节)
    pub data: Vec<u8>,
    /// 批次大小 (字节)
    pub size_bytes: i64,
}

// ─── Fetcher 状态 ────────────────────────────────────────────────────

/// 单个 Partition 的 Fetcher 状态
#[derive(Debug, Clone)]
pub struct PartitionFetcherState {
    /// 目标 Partition
    pub topic: TopicName,
    /// Partition ID
    pub partition: PartitionId,
    /// Leader Broker ID
    pub leader_id: BrokerId,
    /// 当前 Follower LEO (下次 fetch 的起始 offset)
    pub fetch_offset: Offset,
    /// Leader HW (最近一次响应中获取)
    pub leader_hw: Offset,
    /// Leader LEO (最近一次响应中获取)
    pub leader_leo: Offset,
    /// 上次 fetch 时间
    pub last_fetch_time: Instant,
    /// 连续 fetch 失败次数
    pub consecutive_failures: u32,
    /// 总 fetch 次数
    pub total_fetches: u64,
    /// 总复制字节数
    pub total_bytes_replicated: i64,
    /// Fetcher 是否活跃
    pub active: bool,
}

impl PartitionFetcherState {
    /// 创建新的 Fetcher 状态
    pub fn new(topic: TopicName, partition: PartitionId, leader_id: BrokerId) -> Self {
        Self {
            topic,
            partition,
            leader_id,
            fetch_offset: Offset(0),
            leader_hw: Offset(0),
            leader_leo: Offset(0),
            last_fetch_time: Instant::now(),
            consecutive_failures: 0,
            total_fetches: 0,
            total_bytes_replicated: 0,
            active: true,
        }
    }

    /// 计算与 Leader 的 lag
    pub fn lag(&self) -> i64 {
        (self.leader_leo.0 - self.fetch_offset.0).max(0)
    }

    /// 是否需要 fetch (间隔已到 且 有 lag)
    pub fn needs_fetch(&self, interval: Duration) -> bool {
        self.active && self.last_fetch_time.elapsed() >= interval && self.lag() > 0
    }
}

// ─── ReplicaFetcher ──────────────────────────────────────────────────

/// Replica Fetcher 管理器
///
/// 管理所有 Follower Partition 的复制 Fetcher 线程。
/// 每个 Partition 维护独立的 fetch offset 和状态。
pub struct ReplicaFetcher {
    /// 本 Broker 的 ID (Follower 身份)
    broker_id: BrokerId,
    /// Fetcher 配置
    config: ReplicaFetcherConfig,
    /// 各 Partition 的 Fetcher 状态
    partitions: HashMap<(TopicName, PartitionId), PartitionFetcherState>,
}

impl ReplicaFetcher {
    /// 创建新的 Replica Fetcher
    pub fn new(broker_id: BrokerId, config: ReplicaFetcherConfig) -> Self {
        info!(broker_id = broker_id.0, "ReplicaFetcher created");
        Self {
            broker_id,
            config,
            partitions: HashMap::new(),
        }
    }

    /// 获取 Broker ID
    pub fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    /// 获取配置
    pub fn config(&self) -> &ReplicaFetcherConfig {
        &self.config
    }

    /// 注册需要复制的 Partition
    pub fn add_partition(
        &mut self,
        topic: TopicName,
        partition: PartitionId,
        leader_id: BrokerId,
        start_offset: Offset,
    ) {
        let key = (topic.clone(), partition);
        if self.partitions.contains_key(&key) {
            debug!(topic = %topic.0, partition = partition.0, "Partition already registered");
            return;
        }

        let mut state = PartitionFetcherState::new(topic, partition, leader_id);
        state.fetch_offset = start_offset;

        debug!(
            broker_id = self.broker_id.0,
            topic = %state.topic.0,
            partition = state.partition.0,
            leader_id = leader_id.0,
            start_offset = start_offset.0,
            "Partition registered for replication"
        );

        self.partitions.insert(key, state);
    }

    /// 移除 Partition (不再需要复制)
    pub fn remove_partition(&mut self, topic: &TopicName, partition: PartitionId) {
        let key = (topic.clone(), partition);
        if self.partitions.remove(&key).is_some() {
            debug!(topic = %topic.0, partition = partition.0, "Partition removed from replication");
        }
    }

    /// 暂停 Partition 的复制
    pub fn pause_partition(&mut self, topic: &TopicName, partition: PartitionId) {
        let key = (topic.clone(), partition);
        if let Some(state) = self.partitions.get_mut(&key) {
            state.active = false;
        }
    }

    /// 恢复 Partition 的复制
    pub fn resume_partition(&mut self, topic: &TopicName, partition: PartitionId) {
        let key = (topic.clone(), partition);
        if let Some(state) = self.partitions.get_mut(&key) {
            state.active = true;
        }
    }

    /// 构建 Fetch 请求 (模拟一次 fetch 周期)
    ///
    /// 返回需要发送的 Fetch 请求列表。
    pub fn build_fetch_requests(&mut self) -> Vec<ReplicationFetchRequest> {
        let interval = Duration::from_millis(self.config.fetch_interval_ms);
        let mut requests = Vec::new();

        for ((topic, partition), state) in &self.partitions {
            if state.needs_fetch(interval) {
                requests.push(ReplicationFetchRequest {
                    topic: topic.clone(),
                    partition: *partition,
                    fetch_offset: state.fetch_offset,
                    replica_id: self.broker_id,
                    current_leader_epoch: -1,
                });
            }
        }

        requests
    }

    /// 处理 Fetch 响应，更新本地状态
    ///
    /// 返回成功复制的批次数。
    pub fn handle_fetch_response(
        &mut self,
        topic: &TopicName,
        partition: PartitionId,
        response: &ReplicationFetchResponse,
    ) -> Result<usize> {
        let key = (topic.clone(), partition);
        let state = self.partitions.get_mut(&key).ok_or_else(|| {
            RkError::Protocol(format!(
                "Fetch response for unregistered partition {}-{}",
                topic.0, partition.0
            ))
        })?;

        state.last_fetch_time = Instant::now();
        state.total_fetches += 1;

        if response.error_code != 0 {
            state.consecutive_failures += 1;
            warn!(
                topic = %topic.0,
                partition = partition.0,
                error_code = response.error_code,
                consecutive_failures = state.consecutive_failures,
                "Fetch response error"
            );
            return Ok(0);
        }

        // 成功: 重置失败计数
        state.consecutive_failures = 0;
        state.leader_hw = response.high_watermark;
        state.leader_leo = response.log_end_offset;

        let batch_count = response.record_batches.len();

        // 更新 fetch offset 到最后一批之后
        if let Some(last_batch) = response.record_batches.last() {
            state.fetch_offset = Offset(last_batch.last_offset.0 + 1);
        }

        state.total_bytes_replicated += response.fetched_bytes;

        debug!(
            topic = %topic.0,
            partition = partition.0,
            batches = batch_count,
            bytes = response.fetched_bytes,
            new_offset = state.fetch_offset.0,
            lag = state.lag(),
            "Fetch response processed"
        );

        Ok(batch_count)
    }

    /// 模拟一次完整的 fetch 周期 (构建请求 + 处理响应)
    ///
    /// 用于测试和模拟场景。实际部署中由网络层驱动。
    pub fn simulate_fetch_cycle(
        &mut self,
        leader_data: &HashMap<(TopicName, PartitionId), Vec<ReplicatedBatch>>,
    ) -> FetchCycleResult {
        let requests = self.build_fetch_requests();
        let mut result = FetchCycleResult {
            partitions_fetched: 0,
            total_bytes: 0,
            total_batches: 0,
            errors: 0,
        };

        for req in &requests {
            let key = (req.topic.clone(), req.partition);
            let batches = leader_data.get(&key).cloned().unwrap_or_default();

            // 过滤: 只返回 fetch_offset 之后的数据
            let relevant: Vec<ReplicatedBatch> = batches
                .into_iter()
                .filter(|b| b.base_offset.0 >= req.fetch_offset.0)
                .collect();

            let fetched_bytes: i64 = relevant.iter().map(|b| b.size_bytes).sum();
            let log_end_offset = relevant
                .last()
                .map(|b| Offset(b.last_offset.0 + 1))
                .unwrap_or(req.fetch_offset);

            let response = ReplicationFetchResponse {
                error_code: 0,
                high_watermark: log_end_offset,
                log_end_offset,
                record_batches: relevant,
                fetched_bytes,
            };

            match self.handle_fetch_response(&req.topic, req.partition, &response) {
                Ok(n) => {
                    result.partitions_fetched += 1;
                    result.total_bytes += fetched_bytes;
                    result.total_batches += n;
                }
                Err(_) => {
                    result.errors += 1;
                }
            }
        }

        result
    }

    /// 获取所有 Partition 的 Fetcher 状态快照
    pub fn partition_states(&self) -> Vec<&PartitionFetcherState> {
        self.partitions.values().collect()
    }

    /// 获取指定 Partition 的状态
    pub fn get_partition_state(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<&PartitionFetcherState> {
        self.partitions.get(&(topic.clone(), partition))
    }

    /// 获取指定 Partition 的可变状态
    pub fn get_partition_state_mut(
        &mut self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<&mut PartitionFetcherState> {
        self.partitions.get_mut(&(topic.clone(), partition))
    }

    /// 获取注册的 Partition 数量
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// 获取总复制字节数
    pub fn total_bytes_replicated(&self) -> i64 {
        self.partitions.values().map(|s| s.total_bytes_replicated).sum()
    }

    /// 获取 Fetcher 摘要
    pub fn summary(&self) -> ReplicaFetcherSummary {
        let total_lag: i64 = self.partitions.values().map(|s| s.lag()).sum();
        let active_count = self.partitions.values().filter(|s| s.active).count();
        let total_fetches: u64 = self.partitions.values().map(|s| s.total_fetches).sum();

        ReplicaFetcherSummary {
            broker_id: self.broker_id,
            partition_count: self.partitions.len(),
            active_count,
            total_lag,
            total_bytes_replicated: self.total_bytes_replicated(),
            total_fetches,
        }
    }
}

// ─── 结果/摘要 ───────────────────────────────────────────────────────

/// 一次 Fetch 周期的结果
#[derive(Debug, Clone, Default)]
pub struct FetchCycleResult {
    /// 成功 fetch 的 Partition 数
    pub partitions_fetched: usize,
    /// 总复制字节数
    pub total_bytes: i64,
    /// 总复制批次数
    pub total_batches: usize,
    /// 错误数
    pub errors: usize,
}

/// Fetcher 摘要信息
#[derive(Debug, Clone)]
pub struct ReplicaFetcherSummary {
    pub broker_id: BrokerId,
    pub partition_count: usize,
    pub active_count: usize,
    pub total_lag: i64,
    pub total_bytes_replicated: i64,
    pub total_fetches: u64,
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(id: i32) -> BrokerId {
        BrokerId(id)
    }

    fn topic(name: &str) -> TopicName {
        TopicName(name.to_string())
    }

    fn partition(id: i32) -> PartitionId {
        PartitionId(id)
    }

    #[test]
    fn test_fetcher_config_default() {
        let config = ReplicaFetcherConfig::default();
        assert_eq!(config.fetch_interval_ms, 500);
        assert_eq!(config.fetch_max_bytes, 10 * 1024 * 1024);
    }

    #[test]
    fn test_fetcher_add_remove_partition() {
        let mut fetcher = ReplicaFetcher::new(broker(2), ReplicaFetcherConfig::default());
        assert_eq!(fetcher.partition_count(), 0);

        fetcher.add_partition(topic("test"), partition(0), broker(1), Offset(0));
        assert_eq!(fetcher.partition_count(), 1);

        // 重复添加不报错
        fetcher.add_partition(topic("test"), partition(0), broker(1), Offset(0));
        assert_eq!(fetcher.partition_count(), 1);

        fetcher.remove_partition(&topic("test"), partition(0));
        assert_eq!(fetcher.partition_count(), 0);
    }

    #[test]
    fn test_fetcher_pause_resume() {
        let mut fetcher = ReplicaFetcher::new(broker(2), ReplicaFetcherConfig::default());
        fetcher.add_partition(topic("test"), partition(0), broker(1), Offset(0));

        fetcher.pause_partition(&topic("test"), partition(0));
        let state = fetcher.get_partition_state(&topic("test"), partition(0)).unwrap();
        assert!(!state.active);

        fetcher.resume_partition(&topic("test"), partition(0));
        let state = fetcher.get_partition_state(&topic("test"), partition(0)).unwrap();
        assert!(state.active);
    }

    #[test]
    fn test_partition_fetcher_state_lag() {
        let mut state = PartitionFetcherState::new(topic("test"), partition(0), broker(1));
        state.leader_leo = Offset(100);
        state.fetch_offset = Offset(80);
        assert_eq!(state.lag(), 20);

        state.fetch_offset = Offset(100);
        assert_eq!(state.lag(), 0);

        // lag 不为负
        state.fetch_offset = Offset(120);
        assert_eq!(state.lag(), 0);
    }

    #[test]
    fn test_handle_fetch_response_success() {
        let mut fetcher = ReplicaFetcher::new(broker(2), ReplicaFetcherConfig::default());
        fetcher.add_partition(topic("test"), partition(0), broker(1), Offset(0));

        let response = ReplicationFetchResponse {
            error_code: 0,
            high_watermark: Offset(10),
            log_end_offset: Offset(10),
            record_batches: vec![
                ReplicatedBatch {
                    base_offset: Offset(0),
                    last_offset: Offset(4),
                    data: vec![1, 2, 3],
                    size_bytes: 3,
                },
                ReplicatedBatch {
                    base_offset: Offset(5),
                    last_offset: Offset(9),
                    data: vec![4, 5, 6],
                    size_bytes: 3,
                },
            ],
            fetched_bytes: 6,
        };

        let batches = fetcher.handle_fetch_response(&topic("test"), partition(0), &response).unwrap();
        assert_eq!(batches, 2);

        let state = fetcher.get_partition_state(&topic("test"), partition(0)).unwrap();
        assert_eq!(state.fetch_offset, Offset(10));
        assert_eq!(state.leader_hw, Offset(10));
        assert_eq!(state.total_bytes_replicated, 6);
        assert_eq!(state.total_fetches, 1);
        assert_eq!(state.consecutive_failures, 0);
    }

    #[test]
    fn test_handle_fetch_response_error() {
        let mut fetcher = ReplicaFetcher::new(broker(2), ReplicaFetcherConfig::default());
        fetcher.add_partition(topic("test"), partition(0), broker(1), Offset(0));

        let response = ReplicationFetchResponse {
            error_code: 1,
            high_watermark: Offset(0),
            log_end_offset: Offset(0),
            record_batches: vec![],
            fetched_bytes: 0,
        };

        let batches = fetcher.handle_fetch_response(&topic("test"), partition(0), &response).unwrap();
        assert_eq!(batches, 0);

        let state = fetcher.get_partition_state(&topic("test"), partition(0)).unwrap();
        assert_eq!(state.consecutive_failures, 1);
    }

    #[test]
    fn test_handle_fetch_response_unknown_partition() {
        let mut fetcher = ReplicaFetcher::new(broker(2), ReplicaFetcherConfig::default());
        let response = ReplicationFetchResponse {
            error_code: 0,
            high_watermark: Offset(0),
            log_end_offset: Offset(0),
            record_batches: vec![],
            fetched_bytes: 0,
        };
        assert!(fetcher.handle_fetch_response(&topic("unknown"), partition(0), &response).is_err());
    }

    #[test]
    fn test_simulate_fetch_cycle() {
        let config = ReplicaFetcherConfig {
            fetch_interval_ms: 0, // 立即 fetch
            ..Default::default()
        };
        let mut fetcher = ReplicaFetcher::new(broker(2), config);
        fetcher.add_partition(topic("test"), partition(0), broker(1), Offset(0));

        // 模拟 Follower 已知 Leader 有数据 (产生 lag)
        fetcher.get_partition_state_mut(&topic("test"), partition(0)).unwrap().leader_leo = Offset(10);

        // Leader 数据
        let mut leader_data = HashMap::new();
        leader_data.insert(
            (topic("test"), partition(0)),
            vec![
                ReplicatedBatch {
                    base_offset: Offset(0),
                    last_offset: Offset(4),
                    data: vec![1; 100],
                    size_bytes: 100,
                },
                ReplicatedBatch {
                    base_offset: Offset(5),
                    last_offset: Offset(9),
                    data: vec![2; 200],
                    size_bytes: 200,
                },
            ],
        );

        let result = fetcher.simulate_fetch_cycle(&leader_data);
        assert_eq!(result.partitions_fetched, 1);
        assert_eq!(result.total_batches, 2);
        assert_eq!(result.total_bytes, 300);
        assert_eq!(result.errors, 0);

        let state = fetcher.get_partition_state(&topic("test"), partition(0)).unwrap();
        assert_eq!(state.fetch_offset, Offset(10));
    }

    #[test]
    fn test_fetcher_summary() {
        let mut fetcher = ReplicaFetcher::new(broker(2), ReplicaFetcherConfig::default());
        fetcher.add_partition(topic("test"), partition(0), broker(1), Offset(0));
        fetcher.add_partition(topic("test"), partition(1), broker(1), Offset(50));

        let summary = fetcher.summary();
        assert_eq!(summary.broker_id, broker(2));
        assert_eq!(summary.partition_count, 2);
        assert_eq!(summary.active_count, 2);
    }
}
