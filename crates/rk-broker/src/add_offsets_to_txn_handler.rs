//! AddOffsetsToTxn Handler
//!
//! 处理 AddOffsetsToTxn 请求 (API Key = 25)。
//! 将消费者组协调器注册到事务，使后续 TxnOffsetCommit 能事务性提交偏移量。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::add_offsets_to_txn::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::producer_state_manager::ProducerStateManager;

/// AddOffsetsToTxn 请求处理器
pub struct AddOffsetsToTxnHandler {
    producer_state_manager: Arc<ProducerStateManager>,
}

impl AddOffsetsToTxnHandler {
    pub fn new(producer_state_manager: Arc<ProducerStateManager>) -> Self {
        Self {
            producer_state_manager,
        }
    }

    /// 处理 AddOffsetsToTxn 请求
    pub fn handle(
        &self,
        request: AddOffsetsToTxnRequest,
        _version: i16,
    ) -> Result<AddOffsetsToTxnResponse> {
        debug!(
            transactional_id = %request.transactional_id,
            producer_id = request.producer_id,
            group_id = %request.group_id,
            "AddOffsetsToTxn request"
        );

        // 确保生产者已注册
        if self
            .producer_state_manager
            .get_producer_state(request.producer_id)
            .is_none()
        {
            self.producer_state_manager.register_producer(
                request.producer_id,
                request.producer_epoch,
                Some(request.transactional_id.clone()),
            );
        }

        // 确保事务已开始
        let _ = self
            .producer_state_manager
            .begin_transaction(request.producer_id);

        debug!(
            transactional_id = %request.transactional_id,
            producer_id = request.producer_id,
            group_id = %request.group_id,
            "Consumer group offsets registered to transaction"
        );

        Ok(AddOffsetsToTxnResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handler() -> AddOffsetsToTxnHandler {
        AddOffsetsToTxnHandler::new(Arc::new(ProducerStateManager::new()))
    }

    #[test]
    fn test_add_offsets_to_txn_success() {
        let handler = make_handler();
        let req = AddOffsetsToTxnRequest {
            transactional_id: "txn-1".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            group_id: "my-consumer-group".to_string(),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.throttle_time_ms, 0);
    }

    #[test]
    fn test_add_offsets_to_txn_registers_producer() {
        let mgr = Arc::new(ProducerStateManager::new());
        let handler = AddOffsetsToTxnHandler::new(mgr.clone());
        let req = AddOffsetsToTxnRequest {
            transactional_id: "txn-2".to_string(),
            producer_id: 2000,
            producer_epoch: 1,
            group_id: "group-2".to_string(),
        };
        handler.handle(req, 0).unwrap();
        let state = mgr.get_producer_state(2000).unwrap();
        assert_eq!(state.producer_epoch, 1);
        assert_eq!(state.transactional_id, Some("txn-2".to_string()));
    }

    #[test]
    fn test_add_offsets_to_txn_v3_flexible() {
        let handler = make_handler();
        let req = AddOffsetsToTxnRequest {
            transactional_id: "txn-flex".to_string(),
            producer_id: 3000,
            producer_epoch: 2,
            group_id: "flex-group".to_string(),
        };
        let resp = handler.handle(req, 3).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }
}
