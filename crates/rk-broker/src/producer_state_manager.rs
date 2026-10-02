//! Producer State Manager
//!
//! 管理幂等生产者和事务状态:
//! - Producer ID + Epoch 分配
//! - 序列号验证 (检测重复/乱序)
//! - 事务状态跟踪
//!
//! Phase 1: 内存状态，Phase 3 持久化到 __producer_id 内部 topic。

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;
use rk_core::error::{Result, RkError};
use tracing::{debug, warn};

/// 生产者状态
#[derive(Debug, Clone)]
pub struct ProducerState {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// 每个 (topic, partition) 的最后序列号
    pub last_sequence: HashMap<(String, i32), i32>,
    /// 关联的 transactional_id (如果有)
    pub transactional_id: Option<String>,
    /// 事务状态
    pub txn_state: TransactionState,
    /// 事务开始偏移量 (如果有)
    pub txn_start_offset: Option<i64>,
}

/// 活跃事务信息 (用于 ListTransactions)
#[derive(Debug, Clone)]
pub struct ActiveTransactionInfo {
    pub transactional_id: String,
    pub producer_id: i64,
    pub state_str: String,
    pub timeout_ms: i32,
    pub start_time_ms: i64,
}

/// 事务状态
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    /// 无事务 (幂等生产者)
    None,
    /// 事务进行中
    Ongoing,
    /// 正在提交
    Committing,
    /// 正在中止
    Aborting,
    /// 事务已完成
    Complete,
}

/// Producer State Manager
pub struct ProducerStateManager {
    /// producer_id → ProducerState
    producers: DashMap<i64, ProducerState>,
    /// transactional_id → producer_id 映射
    txn_to_producer: DashMap<String, i64>,
    /// 下一个 producer_id
    next_producer_id: AtomicI64,
}

impl Default for ProducerStateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProducerStateManager {
    pub fn new() -> Self {
        Self {
            producers: DashMap::new(),
            txn_to_producer: DashMap::new(),
            next_producer_id: AtomicI64::new(1000),
        }
    }

    /// 分配新的 Producer ID (非事务)
    pub fn allocate_producer_id(&self) -> i64 {
        self.next_producer_id.fetch_add(1, Ordering::SeqCst)
    }

    /// 注册新的生产者
    pub fn register_producer(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        transactional_id: Option<String>,
    ) {
        let state = ProducerState {
            producer_id,
            producer_epoch,
            last_sequence: HashMap::new(),
            transactional_id: transactional_id.clone(),
            txn_state: TransactionState::None,
            txn_start_offset: None,
        };
        self.producers.insert(producer_id, state);

        if let Some(ref txn_id) = transactional_id {
            self.txn_to_producer.insert(txn_id.clone(), producer_id);
        }

        debug!(
            producer_id = producer_id,
            producer_epoch = producer_epoch,
            transactional_id = ?transactional_id,
            "Producer registered"
        );
    }

    /// 验证序列号 (幂等检查)
    ///
    /// 返回:
    /// - Ok(true): 序列号有效，可以写入
    /// - Ok(false): 重复序列号，应跳过 (但返回成功)
    /// - Err: 乱序序列号，返回错误
    pub fn validate_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partition: i32,
        base_sequence: i32,
    ) -> Result<bool> {
        let mut entry = self
            .producers
            .get_mut(&producer_id)
            .ok_or_else(|| RkError::Protocol(format!("Unknown producer_id: {}", producer_id)))?;

        // 验证 epoch
        if entry.producer_epoch != producer_epoch {
            return Err(RkError::Protocol(format!(
                "Producer epoch mismatch: expected {}, got {}",
                entry.producer_epoch, producer_epoch
            )));
        }

        let key = (topic.to_string(), partition);
        let last_seq = entry.last_sequence.get(&key).copied().unwrap_or(-1);

        let expected_seq = if last_seq == -1 { 0 } else { last_seq + 1 };

        if base_sequence == expected_seq {
            // 正常序列号，更新
            entry.last_sequence.insert(key, base_sequence);
            Ok(true)
        } else if base_sequence <= last_seq && last_seq != -1 {
            // 重复序列号 (已处理过)
            debug!(
                producer_id = producer_id,
                base_sequence = base_sequence,
                last_sequence = last_seq,
                "Duplicate sequence number"
            );
            Ok(false) // 重复，跳过
        } else {
            // 乱序序列号
            warn!(
                producer_id = producer_id,
                base_sequence = base_sequence,
                expected_sequence = expected_seq,
                "Out of order sequence number"
            );
            Err(RkError::Protocol(format!(
                "Out of order sequence: expected {}, got {}",
                expected_seq, base_sequence
            )))
        }
    }

    /// 开始事务
    pub fn begin_transaction(&self, producer_id: i64) -> Result<()> {
        let mut entry = self
            .producers
            .get_mut(&producer_id)
            .ok_or_else(|| RkError::Protocol(format!("Unknown producer_id: {}", producer_id)))?;

        match entry.txn_state {
            TransactionState::None | TransactionState::Complete => {
                entry.txn_state = TransactionState::Ongoing;
                debug!(producer_id = producer_id, "Transaction started");
                Ok(())
            }
            TransactionState::Ongoing => {
                // 已有事务进行中，允许继续添加分区
                Ok(())
            }
            _ => Err(RkError::Protocol(format!(
                "Cannot begin transaction in state {:?}",
                entry.txn_state
            ))),
        }
    }

    /// 提交事务
    pub fn commit_transaction(&self, producer_id: i64) -> Result<()> {
        let mut entry = self
            .producers
            .get_mut(&producer_id)
            .ok_or_else(|| RkError::Protocol(format!("Unknown producer_id: {}", producer_id)))?;

        match entry.txn_state {
            TransactionState::Ongoing => {
                entry.txn_state = TransactionState::Complete;
                // 清除序列号状态 (新事务重新开始)
                entry.last_sequence.clear();
                debug!(producer_id = producer_id, "Transaction committed");
                Ok(())
            }
            _ => Err(RkError::Protocol(format!(
                "Cannot commit transaction in state {:?}",
                entry.txn_state
            ))),
        }
    }

    /// 中止事务
    pub fn abort_transaction(&self, producer_id: i64) -> Result<()> {
        let mut entry = self
            .producers
            .get_mut(&producer_id)
            .ok_or_else(|| RkError::Protocol(format!("Unknown producer_id: {}", producer_id)))?;

        match entry.txn_state {
            TransactionState::Ongoing => {
                entry.txn_state = TransactionState::Complete;
                entry.last_sequence.clear();
                debug!(producer_id = producer_id, "Transaction aborted");
                Ok(())
            }
            _ => Err(RkError::Protocol(format!(
                "Cannot abort transaction in state {:?}",
                entry.txn_state
            ))),
        }
    }

    /// 通过 transactional_id 查找 producer_id
    pub fn get_producer_id_by_txn(&self, transactional_id: &str) -> Option<i64> {
        self.txn_to_producer.get(transactional_id).map(|v| *v)
    }

    /// 获取生产者状态
    pub fn get_producer_state(&self, producer_id: i64) -> Option<ProducerState> {
        self.producers.get(&producer_id).map(|v| v.clone())
    }

    /// 获取事务状态
    pub fn get_transaction_state(&self, producer_id: i64) -> Option<TransactionState> {
        self.producers
            .get(&producer_id)
            .map(|v| v.txn_state.clone())
    }

    /// 获取指定 partition 上的活跃生产者列表
    pub fn get_producers_for_partition(&self, topic: &str, partition: i32) -> Vec<ProducerState> {
        let mut result = Vec::new();
        for entry in self.producers.iter() {
            let state = entry.value();
            if state
                .last_sequence
                .contains_key(&(topic.to_string(), partition))
            {
                result.push(state.clone());
            }
        }
        result
    }

    /// 列出所有活跃事务
    pub fn list_active_transactions(&self) -> Vec<ActiveTransactionInfo> {
        let mut result = Vec::new();
        for entry in self.producers.iter() {
            let state = entry.value();
            if state.txn_state != TransactionState::None
                && state.txn_state != TransactionState::Complete
            {
                if let Some(ref txn_id) = state.transactional_id {
                    let state_str = match state.txn_state {
                        TransactionState::Ongoing => "Ongoing",
                        TransactionState::Committing => "PrepareCommit",
                        TransactionState::Aborting => "PrepareAbort",
                        _ => "Dead",
                    };
                    result.push(ActiveTransactionInfo {
                        transactional_id: txn_id.clone(),
                        producer_id: state.producer_id,
                        state_str: state_str.to_string(),
                        timeout_ms: 60000, // Phase 1: 默认超时
                        start_time_ms: 0,  // Phase 1: 不跟踪启动时间
                    });
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_producer_id() {
        let mgr = ProducerStateManager::new();
        let id1 = mgr.allocate_producer_id();
        let id2 = mgr.allocate_producer_id();
        assert_eq!(id2, id1 + 1);
    }

    #[test]
    fn test_register_and_validate_sequence() {
        let mgr = ProducerStateManager::new();
        mgr.register_producer(1000, 0, None);

        // 第一个序列号应该是 0
        let result = mgr.validate_sequence(1000, 0, "topic1", 0, 0);
        assert!(result.unwrap());

        // 下一个应该是 1
        let result = mgr.validate_sequence(1000, 0, "topic1", 0, 1);
        assert!(result.unwrap());

        // 重复序列号 0 → 返回 false (跳过)
        let result = mgr.validate_sequence(1000, 0, "topic1", 0, 0);
        assert!(!result.unwrap());
    }

    #[test]
    fn test_out_of_order_sequence() {
        let mgr = ProducerStateManager::new();
        mgr.register_producer(1000, 0, None);

        // 第一个序列号 0
        mgr.validate_sequence(1000, 0, "topic1", 0, 0).unwrap();

        // 跳到 5 → 乱序
        let result = mgr.validate_sequence(1000, 0, "topic1", 0, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_epoch_rejected() {
        let mgr = ProducerStateManager::new();
        mgr.register_producer(1000, 0, None);

        let result = mgr.validate_sequence(1000, 1, "topic1", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_lifecycle() {
        let mgr = ProducerStateManager::new();
        mgr.register_producer(1000, 0, Some("txn-1".to_string()));

        // 开始事务
        mgr.begin_transaction(1000).unwrap();
        assert_eq!(
            mgr.get_transaction_state(1000).unwrap(),
            TransactionState::Ongoing
        );

        // 写入序列号
        mgr.validate_sequence(1000, 0, "topic1", 0, 0).unwrap();

        // 提交
        mgr.commit_transaction(1000).unwrap();
        assert_eq!(
            mgr.get_transaction_state(1000).unwrap(),
            TransactionState::Complete
        );

        // 序列号被清除，新事务从 0 开始
        mgr.begin_transaction(1000).unwrap();
        mgr.validate_sequence(1000, 0, "topic1", 0, 0).unwrap();
    }

    #[test]
    fn test_abort_transaction() {
        let mgr = ProducerStateManager::new();
        mgr.register_producer(1000, 0, Some("txn-1".to_string()));

        mgr.begin_transaction(1000).unwrap();
        mgr.validate_sequence(1000, 0, "topic1", 0, 0).unwrap();

        // 中止
        mgr.abort_transaction(1000).unwrap();
        assert_eq!(
            mgr.get_transaction_state(1000).unwrap(),
            TransactionState::Complete
        );
    }

    #[test]
    fn test_txn_to_producer_lookup() {
        let mgr = ProducerStateManager::new();
        mgr.register_producer(1000, 0, Some("txn-1".to_string()));

        assert_eq!(mgr.get_producer_id_by_txn("txn-1"), Some(1000));
        assert_eq!(mgr.get_producer_id_by_txn("txn-2"), None);
    }

    #[test]
    fn test_unknown_producer_rejected() {
        let mgr = ProducerStateManager::new();
        let result = mgr.validate_sequence(9999, 0, "topic1", 0, 0);
        assert!(result.is_err());
    }
}
