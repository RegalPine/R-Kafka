//! ISR (In-Sync Replica) Tracker
//!
//! 管理 ISR 集合的扩缩容:
//! - Follower 追赶: LEO 接近 Leader → 加入 ISR
//! - Follower 落后: lag 超过阈值或超时 → 移出 ISR
//! - ISR 为空时的 unclean leader election 标记
//!
//! 判定条件:
//! 1. `replica_lag_time_max_ms`: Follower 超过此时间未 fetch → 移出 ISR
//! 2. `replica_lag_max_bytes`: Follower LEO 落后 Leader 超过此量 → 移出 ISR

use std::collections::HashSet;
use std::time::Duration;

use rk_core::error::Result;
use rk_core::types::{BrokerId, Offset};
use tracing::{debug, info, warn};

use crate::replica::{PartitionReplicaSet, ReplicaState};

// ─── ISR 配置 ────────────────────────────────────────────────────────

/// ISR 管理配置
#[derive(Debug, Clone)]
pub struct ISRConfig {
    /// Follower 最大 lag 时间 (超过则移出 ISR)
    /// 默认 30 秒
    pub replica_lag_time_max: Duration,
    /// Follower 最大 lag 字节数 (超过则移出 ISR)
    /// 默认 无限制 (仅基于时间)
    pub replica_lag_max_bytes: Option<i64>,
    /// 最小 ISR 大小 (低于此值时 Produce acks=all 将拒绝)
    /// 默认 1 (仅 Leader)
    pub min_isr_size: usize,
}

impl Default for ISRConfig {
    fn default() -> Self {
        Self {
            replica_lag_time_max: Duration::from_secs(30),
            replica_lag_max_bytes: None,
            min_isr_size: 1,
        }
    }
}

// ─── ISR 变更事件 ────────────────────────────────────────────────────

/// ISR 变更事件
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ISREvent {
    /// Follower 加入 ISR
    Expanded {
        broker_id: BrokerId,
        new_isr_size: usize,
    },
    /// Follower 移出 ISR
    Shrunk {
        broker_id: BrokerId,
        reason: ShrinkReason,
        new_isr_size: usize,
    },
    /// ISR 变为空 (需要 unclean election)
    Empty,
}

/// ISR 收缩原因
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShrinkReason {
    /// Fetch 超时 (超过 replica_lag_time_max)
    FetchTimeout,
    /// Lag 过大 (超过 replica_lag_max_bytes)
    LagTooLarge,
    /// 手动移除
    ManualRemoval,
}

impl std::fmt::Display for ShrinkReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShrinkReason::FetchTimeout => write!(f, "FetchTimeout"),
            ShrinkReason::LagTooLarge => write!(f, "LagTooLarge"),
            ShrinkReason::ManualRemoval => write!(f, "ManualRemoval"),
        }
    }
}

// ─── ISRTracker ──────────────────────────────────────────────────────

/// ISR 追踪器
///
/// 负责:
/// 1. 检测哪些 Follower 应该加入/移出 ISR
/// 2. 维护 ISR 变更事件日志
/// 3. 判断 ISR 是否满足最小大小要求
pub struct ISRTracker {
    /// ISR 配置
    config: ISRConfig,
    /// ISR 变更历史 (最近 N 条)
    event_log: Vec<ISREvent>,
    /// 最大事件日志条数
    max_event_log: usize,
}

impl ISRTracker {
    /// 创建 ISR 追踪器
    pub fn new(config: ISRConfig) -> Self {
        Self {
            config,
            event_log: Vec::new(),
            max_event_log: 100,
        }
    }

    /// 创建默认配置的 ISR 追踪器
    pub fn with_defaults() -> Self {
        Self::new(ISRConfig::default())
    }

    /// 获取 ISR 配置
    pub fn config(&self) -> &ISRConfig {
        &self.config
    }

    /// 获取 ISR 变更事件日志
    pub fn events(&self) -> &[ISREvent] {
        &self.event_log
    }

    /// 记录 ISR 事件
    fn record_event(&mut self, event: ISREvent) {
        if self.event_log.len() >= self.max_event_log {
            self.event_log.remove(0);
        }
        self.event_log.push(event);
    }

    /// 检查并更新 ISR 集合
    ///
    /// 返回 ISR 变更事件列表。
    /// 每次 Leader 收到 Follower Fetch 时调用此方法。
    pub fn check_and_update_isr(
        &mut self,
        replica_set: &mut PartitionReplicaSet,
    ) -> Vec<ISREvent> {
        let mut events = Vec::new();

        let leader_leo = replica_set.leader_leo;
        let lag_time_max = self.config.replica_lag_time_max;
        let lag_bytes_max = self.config.replica_lag_max_bytes;

        // 1. 检查 ISR 中的 Follower 是否应该被移除
        let current_isr: Vec<BrokerId> = replica_set.isr.iter().copied().collect();
        for broker_id in current_isr {
            if replica_set.is_leader(broker_id) {
                continue; // Leader 始终在 ISR 中
            }

            if let Some(info) = replica_set.replica_info(broker_id) {
                let should_remove = self.should_shrink_isr(info, leader_leo, lag_time_max, lag_bytes_max);

                if let Some(reason) = should_remove {
                    replica_set.isr.remove(&broker_id);
                    if let Some(replica_info) = replica_set.replicas.get_mut(&broker_id) {
                        replica_info.state = ReplicaState::OutOfSync;
                    }

                    let event = ISREvent::Shrunk {
                        broker_id,
                        reason,
                        new_isr_size: replica_set.isr.len(),
                    };
                    info!(
                        broker_id = broker_id.0,
                        reason = %reason,
                        new_isr_size = replica_set.isr.len(),
                        "ISR shrunk: follower removed"
                    );
                    events.push(event.clone());
                    self.record_event(event);
                }
            }
        }

        // 2. 检查非 ISR 的 Follower 是否应该被加入
        let non_isr_followers: Vec<BrokerId> = replica_set
            .replicas
            .iter()
            .filter(|(&id, info)| {
                !replica_set.isr.contains(&id)
                    && id != replica_set.leader_id
                    && info.role != crate::replica::ReplicaRole::Observer
            })
            .map(|(&id, _)| id)
            .collect();

        for broker_id in non_isr_followers {
            if let Some(info) = replica_set.replica_info(broker_id) {
                if self.should_expand_isr(info, leader_leo, lag_time_max, lag_bytes_max) {
                    replica_set.isr.insert(broker_id);
                    if let Some(replica_info) = replica_set.replicas.get_mut(&broker_id) {
                        replica_info.state = ReplicaState::InSync;
                    }

                    let event = ISREvent::Expanded {
                        broker_id,
                        new_isr_size: replica_set.isr.len(),
                    };
                    info!(
                        broker_id = broker_id.0,
                        new_isr_size = replica_set.isr.len(),
                        "ISR expanded: follower added"
                    );
                    events.push(event.clone());
                    self.record_event(event);
                }
            }
        }

        // 3. 检查 ISR 是否为空
        if replica_set.isr.is_empty() {
            let event = ISREvent::Empty;
            warn!(
                topic = %replica_set.topic.0,
                partition = replica_set.partition.0,
                "ISR is empty! Unclean leader election may be needed"
            );
            events.push(event.clone());
            self.record_event(event);
        }

        events
    }

    /// 判断 Follower 是否应该被移出 ISR
    fn should_shrink_isr(
        &self,
        info: &crate::replica::ReplicaInfo,
        leader_leo: Offset,
        lag_time_max: Duration,
        lag_bytes_max: Option<i64>,
    ) -> Option<ShrinkReason> {
        // 检查 fetch 超时
        if info.time_since_last_fetch() > lag_time_max {
            debug!(
                broker_id = info.broker_id.0,
                elapsed_ms = info.time_since_last_fetch().as_millis() as u64,
                "Follower fetch timeout"
            );
            return Some(ShrinkReason::FetchTimeout);
        }

        // 检查 lag 字节数
        if let Some(max_lag) = lag_bytes_max {
            let lag = info.lag_behind(leader_leo);
            if lag > max_lag {
                debug!(
                    broker_id = info.broker_id.0,
                    lag = lag,
                    max_lag = max_lag,
                    "Follower lag too large"
                );
                return Some(ShrinkReason::LagTooLarge);
            }
        }

        None
    }

    /// 判断 Follower 是否应该被加入 ISR
    fn should_expand_isr(
        &self,
        info: &crate::replica::ReplicaInfo,
        leader_leo: Offset,
        lag_time_max: Duration,
        lag_bytes_max: Option<i64>,
    ) -> bool {
        // 必须最近有 fetch 活动
        if info.time_since_last_fetch() > lag_time_max {
            return false;
        }

        // lag 必须在阈值内
        let lag = info.lag_behind(leader_leo);

        if let Some(max_lag) = lag_bytes_max {
            if lag > max_lag {
                return false;
            }
        }

        // 如果 lag 为 0 或很小，可以加入 ISR
        // 阈值: lag <= leader_leo 的 10% 或 1000 条 (取较大值)
        let threshold = (leader_leo.0 as f64 * 0.1).max(1000.0) as i64;
        lag <= threshold
    }

    /// 手动将 Follower 加入 ISR
    pub fn manually_expand_isr(
        &mut self,
        replica_set: &mut PartitionReplicaSet,
        broker_id: BrokerId,
    ) -> Result<()> {
        if !replica_set.replicas.contains_key(&broker_id) {
            return Err(rk_core::error::RkError::Protocol(format!(
                "Broker {} is not a replica",
                broker_id.0
            )));
        }

        replica_set.isr.insert(broker_id);
        if let Some(info) = replica_set.replicas.get_mut(&broker_id) {
            info.state = ReplicaState::InSync;
        }

        let event = ISREvent::Expanded {
            broker_id,
            new_isr_size: replica_set.isr.len(),
        };
        self.record_event(event);

        Ok(())
    }

    /// 手动将 Follower 移出 ISR
    pub fn manually_shrink_isr(
        &mut self,
        replica_set: &mut PartitionReplicaSet,
        broker_id: BrokerId,
    ) -> Result<()> {
        if broker_id == replica_set.leader_id {
            return Err(rk_core::error::RkError::Protocol(
                "Cannot remove leader from ISR".to_string(),
            ));
        }

        replica_set.isr.remove(&broker_id);
        if let Some(info) = replica_set.replicas.get_mut(&broker_id) {
            info.state = ReplicaState::OutOfSync;
        }

        let event = ISREvent::Shrunk {
            broker_id,
            reason: ShrinkReason::ManualRemoval,
            new_isr_size: replica_set.isr.len(),
        };
        self.record_event(event);

        Ok(())
    }

    /// 检查 ISR 是否满足最小大小要求 (用于 acks=all)
    pub fn is_isr_sufficient(&self, replica_set: &PartitionReplicaSet) -> bool {
        replica_set.isr.len() >= self.config.min_isr_size
    }

    /// 获取 ISR 集合快照
    pub fn isr_snapshot(&self, replica_set: &PartitionReplicaSet) -> HashSet<BrokerId> {
        replica_set.isr.clone()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::PartitionReplicaSet;
    use rk_core::types::{BrokerId, Offset, PartitionId, TopicName};

    fn broker(id: i32) -> BrokerId {
        BrokerId(id)
    }

    fn topic(name: &str) -> TopicName {
        TopicName(name.to_string())
    }

    fn partition(id: i32) -> PartitionId {
        PartitionId(id)
    }

    fn make_test_replica_set() -> PartitionReplicaSet {
        let assignment = vec![broker(1), broker(2), broker(3)];
        PartitionReplicaSet::new(topic("test"), partition(0), broker(1), assignment)
    }

    #[test]
    fn test_isr_tracker_default_config() {
        let tracker = ISRTracker::with_defaults();
        assert_eq!(tracker.config().replica_lag_time_max, Duration::from_secs(30));
        assert_eq!(tracker.config().replica_lag_max_bytes, None);
        assert_eq!(tracker.config().min_isr_size, 1);
    }

    #[test]
    fn test_isr_initial_state() {
        let prs = make_test_replica_set();
        let tracker = ISRTracker::with_defaults();
        assert!(tracker.is_isr_sufficient(&prs));
        assert_eq!(tracker.isr_snapshot(&prs).len(), 3);
    }

    #[test]
    fn test_isr_shrink_on_fetch_timeout() {
        let mut prs = make_test_replica_set();
        prs.update_leader_offsets(Offset(100), Offset(0));

        // 模拟 follower 2 超时 (设置 last_fetch_time 到过去)
        let config = ISRConfig {
            replica_lag_time_max: Duration::from_millis(100),
            replica_lag_max_bytes: None,
            min_isr_size: 1,
        };
        let mut tracker = ISRTracker::new(config);

        // 让 follower 2 的 last_fetch_time 过期
        prs.replicas.get_mut(&broker(2)).unwrap().last_fetch_time =
            std::time::Instant::now() - Duration::from_secs(10);

        let events = tracker.check_and_update_isr(&mut prs);

        // broker(2) 应该被移出 ISR
        assert!(!prs.isr.contains(&broker(2)));
        assert!(prs.isr.contains(&broker(1))); // Leader 始终在
        assert!(prs.isr.contains(&broker(3)));

        // 应该有 Shrunk 事件
        let shrink_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, ISREvent::Shrunk { .. }))
            .collect();
        assert_eq!(shrink_events.len(), 1);
    }

    #[test]
    fn test_isr_shrink_on_lag() {
        let mut prs = make_test_replica_set();
        prs.update_leader_offsets(Offset(1000), Offset(0));

        let config = ISRConfig {
            replica_lag_time_max: Duration::from_secs(30),
            replica_lag_max_bytes: Some(100),
            min_isr_size: 1,
        };
        let mut tracker = ISRTracker::new(config);

        // follower 2 lag = 1000 - 50 = 950 > 100
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(50);

        let events = tracker.check_and_update_isr(&mut prs);

        assert!(!prs.isr.contains(&broker(2)));
        let has_lag_shrink = events.iter().any(|e| {
            matches!(e, ISREvent::Shrunk { reason: ShrinkReason::LagTooLarge, .. })
        });
        assert!(has_lag_shrink);
    }

    #[test]
    fn test_isr_expand_on_catch_up() {
        let mut prs = make_test_replica_set();
        prs.update_leader_offsets(Offset(100), Offset(100));

        // 先移除 broker(2) 出 ISR
        prs.isr.remove(&broker(2));
        prs.replicas.get_mut(&broker(2)).unwrap().state = ReplicaState::OutOfSync;

        let config = ISRConfig {
            replica_lag_time_max: Duration::from_secs(30),
            replica_lag_max_bytes: None,
            min_isr_size: 1,
        };
        let mut tracker = ISRTracker::new(config);

        // follower 2 追赶上来了 (lag = 0)
        prs.replicas.get_mut(&broker(2)).unwrap().log_end_offset = Offset(100);
        prs.replicas.get_mut(&broker(2)).unwrap().last_fetch_time = std::time::Instant::now();

        let events = tracker.check_and_update_isr(&mut prs);

        assert!(prs.isr.contains(&broker(2)));
        let has_expand = events.iter().any(|e| matches!(e, ISREvent::Expanded { .. }));
        assert!(has_expand);
    }

    #[test]
    fn test_isr_manual_expand_shrink() {
        let mut prs = make_test_replica_set();
        prs.isr.remove(&broker(2));

        let mut tracker = ISRTracker::with_defaults();

        // 手动加入
        tracker.manually_expand_isr(&mut prs, broker(2)).unwrap();
        assert!(prs.isr.contains(&broker(2)));

        // 手动移除
        tracker.manually_shrink_isr(&mut prs, broker(2)).unwrap();
        assert!(!prs.isr.contains(&broker(2)));

        // 不能移除 leader
        assert!(tracker.manually_shrink_isr(&mut prs, broker(1)).is_err());
    }

    #[test]
    fn test_isr_min_size_check() {
        let mut prs = make_test_replica_set();
        let config = ISRConfig {
            replica_lag_time_max: Duration::from_secs(30),
            replica_lag_max_bytes: None,
            min_isr_size: 3,
        };
        let tracker = ISRTracker::new(config);

        // ISR = 3, min = 3 → 足够
        assert!(tracker.is_isr_sufficient(&prs));

        // ISR = 2, min = 3 → 不够
        prs.isr.remove(&broker(3));
        assert!(!tracker.is_isr_sufficient(&prs));
    }

    #[test]
    fn test_isr_event_log() {
        let mut prs = make_test_replica_set();
        prs.update_leader_offsets(Offset(100), Offset(100));
        prs.isr.remove(&broker(2));

        let mut tracker = ISRTracker::with_defaults();

        // 手动加入 → 产生事件
        tracker.manually_expand_isr(&mut prs, broker(2)).unwrap();
        assert_eq!(tracker.events().len(), 1);

        // 手动移除 → 产生事件
        tracker.manually_shrink_isr(&mut prs, broker(2)).unwrap();
        assert_eq!(tracker.events().len(), 2);
    }

    #[test]
    fn test_shrink_reason_display() {
        assert_eq!(format!("{}", ShrinkReason::FetchTimeout), "FetchTimeout");
        assert_eq!(format!("{}", ShrinkReason::LagTooLarge), "LagTooLarge");
        assert_eq!(format!("{}", ShrinkReason::ManualRemoval), "ManualRemoval");
    }
}
