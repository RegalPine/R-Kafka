//! EndTxn Handler
//!
//! 处理 EndTxn 请求 (API Key = 26)。
//! Phase 1: 简化事务提交/中止。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::end_txn::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::producer_state_manager::ProducerStateManager;

/// EndTxn 请求处理器
pub struct EndTxnHandler {
    producer_state_manager: Arc<ProducerStateManager>,
}

impl EndTxnHandler {
    pub fn new(producer_state_manager: Arc<ProducerStateManager>) -> Self {
        Self {
            producer_state_manager,
        }
    }

    /// 处理 EndTxn 请求
    pub fn handle(&self, request: EndTxnRequest, _version: i16) -> Result<EndTxnResponse> {
        debug!(
            transactional_id = %request.transactional_id,
            producer_id = request.producer_id,
            producer_epoch = request.producer_epoch,
            committed = request.committed,
            "EndTxn request"
        );

        // 验证 producer_id 存在
        let state = match self
            .producer_state_manager
            .get_producer_state(request.producer_id)
        {
            Some(s) => s,
            None => {
                return Ok(EndTxnResponse {
                    throttle_time_ms: 0,
                    error_code: KafkaErrorCode::InvalidProducerId,
                });
            }
        };

        // 验证 epoch
        if state.producer_epoch != request.producer_epoch {
            return Ok(EndTxnResponse {
                throttle_time_ms: 0,
                error_code: KafkaErrorCode::InvalidProducerEpoch,
            });
        }

        // 验证有 transactional_id
        if state.transactional_id.is_none() {
            return Ok(EndTxnResponse {
                throttle_time_ms: 0,
                error_code: KafkaErrorCode::InvalidProducerId,
            });
        }

        // 提交或中止
        let result = if request.committed {
            self.producer_state_manager
                .commit_transaction(request.producer_id)
        } else {
            self.producer_state_manager
                .abort_transaction(request.producer_id)
        };

        let error_code = match result {
            Ok(()) => KafkaErrorCode::None,
            Err(_) => KafkaErrorCode::InvalidProducerEpoch,
        };

        debug!(
            producer_id = request.producer_id,
            committed = request.committed,
            error_code = ?error_code,
            "EndTxn completed"
        );

        Ok(EndTxnResponse {
            throttle_time_ms: 0,
            error_code,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer_state_manager::TransactionState;

    fn setup_txn(mgr: &ProducerStateManager, pid: i64, txn_id: &str) {
        mgr.register_producer(pid, 0, Some(txn_id.to_string()));
        mgr.begin_transaction(pid).unwrap();
    }

    #[test]
    fn test_end_txn_commit() {
        let mgr = Arc::new(ProducerStateManager::new());
        let handler = EndTxnHandler::new(mgr.clone());
        setup_txn(&mgr, 1000, "txn-1");

        let req = EndTxnRequest {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            committed: true,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(
            mgr.get_transaction_state(1000).unwrap(),
            TransactionState::Complete
        );
    }

    #[test]
    fn test_end_txn_abort() {
        let mgr = Arc::new(ProducerStateManager::new());
        let handler = EndTxnHandler::new(mgr.clone());
        setup_txn(&mgr, 1000, "txn-1");

        let req = EndTxnRequest {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            committed: false,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_end_txn_unknown_producer() {
        let mgr = Arc::new(ProducerStateManager::new());
        let handler = EndTxnHandler::new(mgr);

        let req = EndTxnRequest {
            transactional_id: "txn-1".to_string(),
            producer_id: 9999,
            producer_epoch: 0,
            committed: true,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::InvalidProducerId);
    }

    #[test]
    fn test_end_txn_wrong_epoch() {
        let mgr = Arc::new(ProducerStateManager::new());
        let handler = EndTxnHandler::new(mgr.clone());
        setup_txn(&mgr, 1000, "txn-1");

        let req = EndTxnRequest {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 5, // 错误 epoch
            committed: true,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::InvalidProducerEpoch);
    }

    #[test]
    fn test_end_txn_no_transaction() {
        let mgr = Arc::new(ProducerStateManager::new());
        let handler = EndTxnHandler::new(mgr.clone());
        // 注册但没有开始事务
        mgr.register_producer(1000, 0, Some("txn-1".to_string()));

        let req = EndTxnRequest {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            committed: true,
        };
        let resp = handler.handle(req, 0).unwrap();
        // 不在 Ongoing 状态 → 错误
        assert_eq!(resp.error_code, KafkaErrorCode::InvalidProducerEpoch);
    }
}
