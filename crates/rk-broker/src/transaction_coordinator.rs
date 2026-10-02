//! Transaction Coordinator — 事务协调器
//!
//! 管理事务的完整生命周期，实现 Exactly-Once 语义 (EOS):
//!
//! ```text
//! 事务状态机:
//!
//!   ┌────────┐  InitProducerId   ┌───────────┐
//!   │ Empty  │ ───────────────→ │  Empty     │
//!   │(no PID)│                  │(with PID)  │
//!   └────────┘                  └─────┬─────┘
//!                                     │ AddPartitionsToTxn
//!                                     │ 或首条 Produce
//!                                     ▼
//!                               ┌───────────┐
//!                          ┌───→│  Ongoing   │←──┐
//!                          │    └─────┬─────┘   │
//!                          │          │          │
//!                   EndTxn │          │ EndTxn   │
//!                   (abort)│          │ (commit) │
//!                          │          ▼          │
//!                     ┌────┴───┐  ┌────────────┐│
//!                     │Prepare │  │  Prepare    ││
//!                     │ Abort  │  │  Commit     ││
//!                     └────┬───┘  └──────┬─────┘│
//!                          │             │       │
//!                          ▼             ▼       │
//!                     ┌────────────────────┐    │
//!                     │  CompleteCommit    │────┘
//!                     │  (可开始新事务)     │
//!                     └────────────────────┘
//! ```
//!
//! 关键组件:
//! - TransactionCoordinator: 管理所有事务状态
//! - TransactionLog: 事务日志 (__transaction_state 内部 Topic 模拟)
//! - ProducerId 分配: 全局唯一 + Epoch 递增

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use rk_core::error::{RkError, Result};
use tracing::{debug, info, warn};

use crate::producer_state_manager::ProducerStateManager;

// ═══════════════════════════════════════════════════════
// 事务协调器状态 (每个 transactional_id)
// ═══════════════════════════════════════════════════════

/// 事务协调器内部状态
#[derive(Debug, Clone, PartialEq)]
pub enum TxnCoordinatorState {
    /// 空状态 — 无关联 producer
    Empty,
    /// 已初始化 — 有 producer_id/epoch，可以开始事务
    Ready,
    /// 事务进行中
    Ongoing,
    /// 正在提交 (写入 commit marker)
    PrepareCommit,
    /// 正在中止 (写入 abort marker)
    PrepareAbort,
    /// 事务完成 (可重新开始)
    CompleteCommit,
    /// 致命错误状态
    Dead(String),
}

impl std::fmt::Display for TxnCoordinatorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnCoordinatorState::Empty => write!(f, "Empty"),
            TxnCoordinatorState::Ready => write!(f, "Ready"),
            TxnCoordinatorState::Ongoing => write!(f, "Ongoing"),
            TxnCoordinatorState::PrepareCommit => write!(f, "PrepareCommit"),
            TxnCoordinatorState::PrepareAbort => write!(f, "PrepareAbort"),
            TxnCoordinatorState::CompleteCommit => write!(f, "CompleteCommit"),
            TxnCoordinatorState::Dead(reason) => write!(f, "Dead({})", reason),
        }
    }
}

/// 单个事务的元数据
#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    /// transactional_id
    pub transactional_id: String,
    /// 分配的 producer_id
    pub producer_id: i64,
    /// 当前 producer epoch
    pub producer_epoch: i16,
    /// 协调器状态
    pub state: TxnCoordinatorState,
    /// 事务超时 (ms)
    pub txn_timeout_ms: u32,
    /// 事务开始时间戳 (ms)
    pub txn_start_time_ms: i64,
    /// 事务涉及的 topic-partition 集合
    pub partitions: Vec<(String, i32)>,
    /// 上次更新的时间戳
    pub last_update_time_ms: i64,
}

// ═══════════════════════════════════════════════════════
// 事务日志 (模拟 __transaction_state 内部 Topic)
// ═══════════════════════════════════════════════════════

/// 事务日志条目类型
#[derive(Debug, Clone)]
pub enum TxnLogEntryType {
    /// 新 producer 注册
    TxnRegister {
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        txn_timeout_ms: u32,
    },
    /// 事务开始
    TxnStart {
        transactional_id: String,
        producer_id: i64,
    },
    /// 分区添加到事务
    TxnAddPartitions {
        transactional_id: String,
        topic: String,
        partition: i32,
    },
    /// 事务提交
    TxnCommit {
        transactional_id: String,
        producer_id: i64,
    },
    /// 事务中止
    TxnAbort {
        transactional_id: String,
        producer_id: i64,
    },
    /// Epoch 递增 (fencing)
    TxnEpochBump {
        transactional_id: String,
        new_epoch: i16,
    },
}

/// 事务日志条目
#[derive(Debug, Clone)]
pub struct TxnLogEntry {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub entry_type: TxnLogEntryType,
}

/// 事务日志 (内存模拟 __transaction_state)
#[derive(Debug)]
pub struct TransactionLog {
    entries: Vec<TxnLogEntry>,
    next_offset: AtomicI64,
}

impl TransactionLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_offset: AtomicI64::new(0),
        }
    }

    /// 追加日志条目
    pub fn append(&mut self, entry_type: TxnLogEntryType) -> i64 {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let entry = TxnLogEntry {
            offset,
            timestamp_ms: current_time_ms(),
            entry_type,
        };
        self.entries.push(entry);
        offset
    }

    /// 获取所有条目
    pub fn entries(&self) -> &[TxnLogEntry] {
        &self.entries
    }

    /// 获取指定 transactional_id 的最新条目
    pub fn latest_entries_for_txn(&self, transactional_id: &str) -> Vec<&TxnLogEntry> {
        self.entries
            .iter()
            .filter(|e| match &e.entry_type {
                TxnLogEntryType::TxnRegister { transactional_id: id, .. }
                | TxnLogEntryType::TxnStart { transactional_id: id, .. }
                | TxnLogEntryType::TxnAddPartitions { transactional_id: id, .. }
                | TxnLogEntryType::TxnCommit { transactional_id: id, .. }
                | TxnLogEntryType::TxnAbort { transactional_id: id, .. }
                | TxnLogEntryType::TxnEpochBump { transactional_id: id, .. } => {
                    id == transactional_id
                }
            })
            .collect()
    }

    /// 日志条目总数
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ═══════════════════════════════════════════════════════
// TransactionCoordinator
// ═══════════════════════════════════════════════════════

/// 事务协调器 — 管理事务的完整生命周期
pub struct TransactionCoordinator {
    /// transactional_id → TransactionMetadata
    transactions: DashMap<String, TransactionMetadata>,
    /// producer_id → transactional_id 反向索引
    producer_to_txn: DashMap<i64, String>,
    /// 下一个 producer_id (全局唯一)
    next_producer_id: AtomicI64,
    /// 事务日志
    txn_log: std::sync::Mutex<TransactionLog>,
    /// 关联的 ProducerStateManager
    producer_state_manager: Arc<ProducerStateManager>,
    /// 默认事务超时 (ms)
    default_txn_timeout_ms: u32,
    /// 最大事务超时 (ms)
    max_txn_timeout_ms: u32,
}

impl TransactionCoordinator {
    /// 创建新的事务协调器
    pub fn new(producer_state_manager: Arc<ProducerStateManager>) -> Self {
        Self {
            transactions: DashMap::new(),
            producer_to_txn: DashMap::new(),
            next_producer_id: AtomicI64::new(1000),
            txn_log: std::sync::Mutex::new(TransactionLog::new()),
            producer_state_manager,
            default_txn_timeout_ms: 60_000,
            max_txn_timeout_ms: 900_000, // 15 分钟
        }
    }

    /// 创建带自定义超时配置的事务协调器
    pub fn with_timeout(
        producer_state_manager: Arc<ProducerStateManager>,
        default_timeout_ms: u32,
        max_timeout_ms: u32,
    ) -> Self {
        let mut coord = Self::new(producer_state_manager);
        coord.default_txn_timeout_ms = default_timeout_ms;
        coord.max_txn_timeout_ms = max_timeout_ms;
        coord
    }

    // ───────────────────────────────────────────────────
    // InitProducerId — 分配 Producer ID + Epoch
    // ───────────────────────────────────────────────────

    /// 初始化事务性生产者
    ///
    /// 如果 transactional_id 已存在:
    /// - 递增 epoch (fencing 旧 producer)
    /// - 中止任何进行中的事务
    /// - 返回新的 epoch
    ///
    /// 如果 transactional_id 不存在:
    /// - 分配新的 producer_id
    /// - epoch = 0
    pub fn init_producer_id(
        &self,
        transactional_id: &str,
        requested_timeout_ms: Option<u32>,
    ) -> Result<(i64, i16)> {
        let timeout_ms = requested_timeout_ms
            .unwrap_or(self.default_txn_timeout_ms)
            .min(self.max_txn_timeout_ms);

        // 检查是否已有此 transactional_id
        if let Some(mut existing) = self.transactions.get_mut(transactional_id) {
            // 已存在 → 递增 epoch, fencing 旧 producer
            let old_epoch = existing.producer_epoch;
            let new_epoch = old_epoch.wrapping_add(1);

            // 如果有进行中的事务，先中止
            if existing.state == TxnCoordinatorState::Ongoing {
                debug!(
                    transactional_id = transactional_id,
                    "Aborting ongoing transaction during re-init"
                );
                let _ = self.producer_state_manager.abort_transaction(existing.producer_id);
            }

            existing.producer_epoch = new_epoch;
            existing.state = TxnCoordinatorState::Ready;
            existing.txn_timeout_ms = timeout_ms;
            existing.partitions.clear();
            existing.last_update_time_ms = current_time_ms();

            // 写入事务日志
            let mut log = self.txn_log.lock().unwrap();
            log.append(TxnLogEntryType::TxnEpochBump {
                transactional_id: transactional_id.to_string(),
                new_epoch,
            });

            // 更新 ProducerStateManager
            self.producer_state_manager.register_producer(
                existing.producer_id,
                new_epoch,
                Some(transactional_id.to_string()),
            );

            info!(
                transactional_id = transactional_id,
                producer_id = existing.producer_id,
                old_epoch = old_epoch,
                new_epoch = new_epoch,
                "Producer re-initialized (epoch bumped)"
            );

            return Ok((existing.producer_id, new_epoch));
        }

        // 新 transactional_id → 分配 producer_id
        let producer_id = self.next_producer_id.fetch_add(1, Ordering::SeqCst);
        let producer_epoch: i16 = 0;

        let metadata = TransactionMetadata {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
            state: TxnCoordinatorState::Ready,
            txn_timeout_ms: timeout_ms,
            txn_start_time_ms: 0,
            partitions: Vec::new(),
            last_update_time_ms: current_time_ms(),
        };

        self.transactions.insert(transactional_id.to_string(), metadata);
        self.producer_to_txn.insert(producer_id, transactional_id.to_string());

        // 注册到 ProducerStateManager
        self.producer_state_manager.register_producer(
            producer_id,
            producer_epoch,
            Some(transactional_id.to_string()),
        );

        // 写入事务日志
        let mut log = self.txn_log.lock().unwrap();
        log.append(TxnLogEntryType::TxnRegister {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
            txn_timeout_ms: timeout_ms,
        });

        info!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            producer_epoch = producer_epoch,
            timeout_ms = timeout_ms,
            "Transaction producer initialized"
        );

        Ok((producer_id, producer_epoch))
    }

    // ───────────────────────────────────────────────────
    // AddPartitionsToTxn — 添加分区到事务
    // ───────────────────────────────────────────────────

    /// 添加 topic-partition 到当前事务
    pub fn add_partitions_to_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partitions: &[i32],
    ) -> Result<()> {
        // 验证 transactional_id 存在
        let mut entry = self.transactions.get_mut(transactional_id)
            .ok_or_else(|| RkError::Protocol(
                format!("Unknown transactional_id: {}", transactional_id)
            ))?;

        // 验证 producer_id 和 epoch
        self.validate_producer_credentials(&entry, producer_id, producer_epoch)?;

        // 验证状态: 必须是 Ready, Ongoing, 或 CompleteCommit
        match entry.state {
            TxnCoordinatorState::Ready | TxnCoordinatorState::CompleteCommit => {
                // 自动开始事务
                entry.state = TxnCoordinatorState::Ongoing;
                entry.txn_start_time_ms = current_time_ms();
                self.producer_state_manager.begin_transaction(producer_id)?;

                let mut log = self.txn_log.lock().unwrap();
                log.append(TxnLogEntryType::TxnStart {
                    transactional_id: transactional_id.to_string(),
                    producer_id,
                });
            }
            TxnCoordinatorState::Ongoing => {
                // 事务已在进行，继续添加分区
            }
            _ => {
                return Err(RkError::Protocol(format!(
                    "Cannot add partitions in state: {}",
                    entry.state
                )));
            }
        }

        // 添加分区
        for &partition in partitions {
            let tp = (topic.to_string(), partition);
            if !entry.partitions.contains(&tp) {
                entry.partitions.push(tp);

                let mut log = self.txn_log.lock().unwrap();
                log.append(TxnLogEntryType::TxnAddPartitions {
                    transactional_id: transactional_id.to_string(),
                    topic: topic.to_string(),
                    partition,
                });
            }
        }

        entry.last_update_time_ms = current_time_ms();

        debug!(
            transactional_id = transactional_id,
            topic = topic,
            partitions = partitions.len(),
            "Partitions added to transaction"
        );

        Ok(())
    }

    // ───────────────────────────────────────────────────
    // EndTxn — 提交或中止事务
    // ───────────────────────────────────────────────────

    /// 提交事务
    pub fn commit_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        let mut entry = self.transactions.get_mut(transactional_id)
            .ok_or_else(|| RkError::Protocol(
                format!("Unknown transactional_id: {}", transactional_id)
            ))?;

        self.validate_producer_credentials(&entry, producer_id, producer_epoch)?;

        // 必须是 Ongoing 状态
        if entry.state != TxnCoordinatorState::Ongoing {
            return Err(RkError::Protocol(format!(
                "Cannot commit transaction in state: {}",
                entry.state
            )));
        }

        // 状态转换: Ongoing → PrepareCommit → CompleteCommit
        entry.state = TxnCoordinatorState::PrepareCommit;

        // 写入 commit 日志
        {
            let mut log = self.txn_log.lock().unwrap();
            log.append(TxnLogEntryType::TxnCommit {
                transactional_id: transactional_id.to_string(),
                producer_id,
            });
        }

        // 提交 ProducerStateManager 中的事务
        self.producer_state_manager.commit_transaction(producer_id)?;

        // 完成
        entry.state = TxnCoordinatorState::CompleteCommit;
        entry.partitions.clear();
        entry.last_update_time_ms = current_time_ms();

        info!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            "Transaction committed"
        );

        Ok(())
    }

    /// 中止事务
    pub fn abort_transaction(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        let mut entry = self.transactions.get_mut(transactional_id)
            .ok_or_else(|| RkError::Protocol(
                format!("Unknown transactional_id: {}", transactional_id)
            ))?;

        self.validate_producer_credentials(&entry, producer_id, producer_epoch)?;

        // 必须是 Ongoing 状态
        if entry.state != TxnCoordinatorState::Ongoing {
            return Err(RkError::Protocol(format!(
                "Cannot abort transaction in state: {}",
                entry.state
            )));
        }

        // 状态转换: Ongoing → PrepareAbort → CompleteCommit
        entry.state = TxnCoordinatorState::PrepareAbort;

        // 写入 abort 日志
        {
            let mut log = self.txn_log.lock().unwrap();
            log.append(TxnLogEntryType::TxnAbort {
                transactional_id: transactional_id.to_string(),
                producer_id,
            });
        }

        // 中止 ProducerStateManager 中的事务
        self.producer_state_manager.abort_transaction(producer_id)?;

        // 完成
        entry.state = TxnCoordinatorState::CompleteCommit;
        entry.partitions.clear();
        entry.last_update_time_ms = current_time_ms();

        info!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            "Transaction aborted"
        );

        Ok(())
    }

    // ───────────────────────────────────────────────────
    // 查询接口
    // ───────────────────────────────────────────────────

    /// 获取事务元数据
    pub fn get_transaction(&self, transactional_id: &str) -> Option<TransactionMetadata> {
        self.transactions.get(transactional_id).map(|e| e.clone())
    }

    /// 获取事务状态
    pub fn get_state(&self, transactional_id: &str) -> Option<TxnCoordinatorState> {
        self.transactions.get(transactional_id).map(|e| e.state.clone())
    }

    /// 列出所有活跃事务
    pub fn list_transactions(&self) -> Vec<TransactionMetadata> {
        self.transactions.iter().map(|e| e.clone()).collect()
    }

    /// 列出指定状态的事务
    pub fn list_transactions_by_state(
        &self,
        state: &TxnCoordinatorState,
    ) -> Vec<TransactionMetadata> {
        self.transactions
            .iter()
            .filter(|e| &e.state == state)
            .map(|e| e.clone())
            .collect()
    }

    /// 获取事务日志引用 (用于测试/诊断)
    pub fn txn_log_entries(&self) -> Vec<TxnLogEntry> {
        let log = self.txn_log.lock().unwrap();
        log.entries().to_vec()
    }

    /// 获取事务日志条目数
    pub fn txn_log_len(&self) -> usize {
        let log = self.txn_log.lock().unwrap();
        log.len()
    }

    /// 获取事务涉及的分区
    pub fn get_txn_partitions(&self, transactional_id: &str) -> Option<Vec<(String, i32)>> {
        self.transactions.get(transactional_id).map(|e| e.partitions.clone())
    }

    /// 事务总数
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    /// 检查超时事务并清理
    pub fn check_and_expire_timeouts(&self) -> Vec<String> {
        let now = current_time_ms();
        let mut expired = Vec::new();

        for mut entry in self.transactions.iter_mut() {
            if entry.state == TxnCoordinatorState::Ongoing {
                let elapsed = now.saturating_sub(entry.txn_start_time_ms);
                if elapsed > entry.txn_timeout_ms as i64 {
                    warn!(
                        transactional_id = entry.transactional_id,
                        elapsed_ms = elapsed,
                        timeout_ms = entry.txn_timeout_ms,
                        "Transaction timed out, aborting"
                    );
                    // 自动中止超时事务
                    let _ = self.producer_state_manager.abort_transaction(entry.producer_id);
                    entry.state = TxnCoordinatorState::CompleteCommit;
                    entry.partitions.clear();
                    expired.push(entry.transactional_id.clone());
                }
            }
        }

        expired
    }

    // ───────────────────────────────────────────────────
    // 内部方法
    // ───────────────────────────────────────────────────

    /// 验证 producer 凭证
    fn validate_producer_credentials(
        &self,
        metadata: &TransactionMetadata,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<()> {
        if metadata.producer_id != producer_id {
            return Err(RkError::Protocol(format!(
                "Producer ID mismatch: expected {}, got {}",
                metadata.producer_id, producer_id
            )));
        }
        if metadata.producer_epoch != producer_epoch {
            return Err(RkError::Protocol(format!(
                "Producer epoch mismatch: expected {}, got {}",
                metadata.producer_epoch, producer_epoch
            )));
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════
// 辅助函数
// ═══════════════════════════════════════════════════════

/// 获取当前时间戳 (ms)
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_coordinator() -> TransactionCoordinator {
        let psm = Arc::new(ProducerStateManager::new());
        TransactionCoordinator::new(psm)
    }

    // ─── InitProducerId 测试 ───

    #[test]
    fn test_init_producer_id_new() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();
        assert!(pid >= 1000);
        assert_eq!(epoch, 0);

        let meta = coord.get_transaction("txn-1").unwrap();
        assert_eq!(meta.state, TxnCoordinatorState::Ready);
        assert_eq!(meta.producer_id, pid);
    }

    #[test]
    fn test_init_producer_id_reinit_bumps_epoch() {
        let coord = make_coordinator();
        let (pid1, epoch1) = coord.init_producer_id("txn-1", None).unwrap();
        assert_eq!(epoch1, 0);

        // 重新初始化 → epoch 递增
        let (pid2, epoch2) = coord.init_producer_id("txn-1", None).unwrap();
        assert_eq!(pid2, pid1); // 同一个 producer_id
        assert_eq!(epoch2, 1); // epoch 递增

        let meta = coord.get_transaction("txn-1").unwrap();
        assert_eq!(meta.producer_epoch, 1);
        assert_eq!(meta.state, TxnCoordinatorState::Ready);
    }

    #[test]
    fn test_init_producer_id_multiple_txns() {
        let coord = make_coordinator();
        let (pid1, _) = coord.init_producer_id("txn-1", None).unwrap();
        let (pid2, _) = coord.init_producer_id("txn-2", None).unwrap();
        assert_ne!(pid1, pid2);
        assert_eq!(coord.transaction_count(), 2);
    }

    #[test]
    fn test_init_producer_id_custom_timeout() {
        let coord = make_coordinator();
        let (_, _) = coord.init_producer_id("txn-1", Some(30_000)).unwrap();
        let meta = coord.get_transaction("txn-1").unwrap();
        assert_eq!(meta.txn_timeout_ms, 30_000);
    }

    #[test]
    fn test_init_producer_id_timeout_capped() {
        let coord = make_coordinator();
        // 超过最大值应被截断
        let (_, _) = coord.init_producer_id("txn-1", Some(9_999_999)).unwrap();
        let meta = coord.get_transaction("txn-1").unwrap();
        assert_eq!(meta.txn_timeout_ms, 900_000); // max_txn_timeout_ms
    }

    // ─── 事务生命周期测试 ───

    #[test]
    fn test_full_txn_lifecycle_commit() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        // 添加分区 → 自动开始事务
        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0, 1]).unwrap();
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::Ongoing);

        // 添加更多分区
        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-b", &[0]).unwrap();
        let parts = coord.get_txn_partitions("txn-1").unwrap();
        assert_eq!(parts.len(), 3); // topic-a:0, topic-a:1, topic-b:0

        // 提交
        coord.commit_transaction("txn-1", pid, epoch).unwrap();
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::CompleteCommit);

        // 提交后分区被清除
        let parts = coord.get_txn_partitions("txn-1").unwrap();
        assert!(parts.is_empty());
    }

    #[test]
    fn test_full_txn_lifecycle_abort() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0]).unwrap();
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::Ongoing);

        // 中止
        coord.abort_transaction("txn-1", pid, epoch).unwrap();
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::CompleteCommit);
    }

    #[test]
    fn test_txn_commit_then_new_txn() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        // 第一个事务
        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0]).unwrap();
        coord.commit_transaction("txn-1", pid, epoch).unwrap();

        // 第二个事务 (同一个 producer)
        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-b", &[0]).unwrap();
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::Ongoing);
        coord.commit_transaction("txn-1", pid, epoch).unwrap();
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::CompleteCommit);
    }

    // ─── 错误场景测试 ───

    #[test]
    fn test_wrong_producer_id_rejected() {
        let coord = make_coordinator();
        let (_pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        let result = coord.add_partitions_to_txn("txn-1", 9999, epoch, "topic-a", &[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_epoch_rejected() {
        let coord = make_coordinator();
        let (pid, _epoch) = coord.init_producer_id("txn-1", None).unwrap();

        let result = coord.add_partitions_to_txn("txn-1", pid, 99, "topic-a", &[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_transactional_id() {
        let coord = make_coordinator();
        let result = coord.commit_transaction("unknown", 1000, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_without_start_rejected() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        // Ready 状态直接提交 → 失败
        let result = coord.commit_transaction("txn-1", pid, epoch);
        assert!(result.is_err());
    }

    #[test]
    fn test_double_commit_rejected() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0]).unwrap();
        coord.commit_transaction("txn-1", pid, epoch).unwrap();

        // 再次提交 → 失败 (CompleteCommit 状态)
        let result = coord.commit_transaction("txn-1", pid, epoch);
        assert!(result.is_err());
    }

    // ─── 事务日志测试 ───

    #[test]
    fn test_txn_log_records_all_events() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0, 1]).unwrap();
        coord.commit_transaction("txn-1", pid, epoch).unwrap();

        // 日志应包含: Register + Start + AddPartitions(0) + AddPartitions(1) + Commit = 5
        let log_len = coord.txn_log_len();
        assert_eq!(log_len, 5, "Expected 5 log entries, got {}", log_len);
    }

    #[test]
    fn test_txn_log_abort_records() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0]).unwrap();
        coord.abort_transaction("txn-1", pid, epoch).unwrap();

        // Register + Start + AddPartitions + Abort = 4
        assert_eq!(coord.txn_log_len(), 4);
    }

    #[test]
    fn test_txn_log_epoch_bump() {
        let coord = make_coordinator();
        coord.init_producer_id("txn-1", None).unwrap();
        coord.init_producer_id("txn-1", None).unwrap(); // re-init → epoch bump

        // Register + EpochBump = 2
        assert_eq!(coord.txn_log_len(), 2);
    }

    // ─── 并发事务测试 ───

    #[test]
    fn test_concurrent_transactions_isolated() {
        let coord = make_coordinator();
        let (pid1, epoch1) = coord.init_producer_id("txn-1", None).unwrap();
        let (pid2, epoch2) = coord.init_producer_id("txn-2", None).unwrap();

        // 两个事务同时进行
        coord.add_partitions_to_txn("txn-1", pid1, epoch1, "topic-a", &[0]).unwrap();
        coord.add_partitions_to_txn("txn-2", pid2, epoch2, "topic-b", &[0]).unwrap();

        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::Ongoing);
        assert_eq!(coord.get_state("txn-2").unwrap(), TxnCoordinatorState::Ongoing);

        // 提交 txn-1, 中止 txn-2
        coord.commit_transaction("txn-1", pid1, epoch1).unwrap();
        coord.abort_transaction("txn-2", pid2, epoch2).unwrap();

        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::CompleteCommit);
        assert_eq!(coord.get_state("txn-2").unwrap(), TxnCoordinatorState::CompleteCommit);
    }

    // ─── 查询接口测试 ───

    #[test]
    fn test_list_transactions() {
        let coord = make_coordinator();
        let (pid1, epoch1) = coord.init_producer_id("txn-1", None).unwrap();
        let (_pid2, _epoch2) = coord.init_producer_id("txn-2", None).unwrap();

        coord.add_partitions_to_txn("txn-1", pid1, epoch1, "topic-a", &[0]).unwrap();

        let all = coord.list_transactions();
        assert_eq!(all.len(), 2);

        let ongoing = coord.list_transactions_by_state(&TxnCoordinatorState::Ongoing);
        assert_eq!(ongoing.len(), 1);
        assert_eq!(ongoing[0].transactional_id, "txn-1");
    }

    #[test]
    fn test_reinit_aborts_ongoing_txn() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        // 开始事务
        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0]).unwrap();
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::Ongoing);

        // 重新初始化 → 应自动中止进行中的事务
        let (pid2, epoch2) = coord.init_producer_id("txn-1", None).unwrap();
        assert_eq!(pid2, pid);
        assert_eq!(epoch2, epoch + 1);
        assert_eq!(coord.get_state("txn-1").unwrap(), TxnCoordinatorState::Ready);
    }

    // ─── TransactionLog 单元测试 ───

    #[test]
    fn test_transaction_log_basic() {
        let mut log = TransactionLog::new();
        assert!(log.is_empty());

        log.append(TxnLogEntryType::TxnRegister {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            txn_timeout_ms: 60000,
        });

        assert_eq!(log.len(), 1);
        assert!(!log.is_empty());

        let entries = log.latest_entries_for_txn("txn-1");
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_transaction_log_filter_by_txn_id() {
        let mut log = TransactionLog::new();

        log.append(TxnLogEntryType::TxnRegister {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            txn_timeout_ms: 60000,
        });
        log.append(TxnLogEntryType::TxnRegister {
            transactional_id: "txn-2".to_string(),
            producer_id: 1001,
            producer_epoch: 0,
            txn_timeout_ms: 60000,
        });
        log.append(TxnLogEntryType::TxnCommit {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
        });

        assert_eq!(log.latest_entries_for_txn("txn-1").len(), 2);
        assert_eq!(log.latest_entries_for_txn("txn-2").len(), 1);
        assert_eq!(log.latest_entries_for_txn("txn-3").len(), 0);
    }

    // ─── TxnCoordinatorState Display ───

    #[test]
    fn test_state_display() {
        assert_eq!(format!("{}", TxnCoordinatorState::Empty), "Empty");
        assert_eq!(format!("{}", TxnCoordinatorState::Ongoing), "Ongoing");
        assert_eq!(
            format!("{}", TxnCoordinatorState::Dead("test".to_string())),
            "Dead(test)"
        );
    }

    // ─── Duplicate partition 测试 ───

    #[test]
    fn test_duplicate_partition_not_added_twice() {
        let coord = make_coordinator();
        let (pid, epoch) = coord.init_producer_id("txn-1", None).unwrap();

        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0, 1]).unwrap();
        // 再次添加相同的分区
        coord.add_partitions_to_txn("txn-1", pid, epoch, "topic-a", &[0, 1]).unwrap();

        let parts = coord.get_txn_partitions("txn-1").unwrap();
        assert_eq!(parts.len(), 2); // 不重复
    }
}
