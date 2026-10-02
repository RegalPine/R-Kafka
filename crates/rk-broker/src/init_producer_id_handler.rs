//! InitProducerId Handler
//!
//! 处理 InitProducerId 请求 (API Key = 22)。
//! 支持事务性生产者: 通过 TransactionCoordinator 分配 ProducerId + Epoch。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::init_producer_id::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::transaction_coordinator::TransactionCoordinator;

/// InitProducerId 请求处理器
pub struct InitProducerIdHandler {
    txn_coordinator: Option<Arc<TransactionCoordinator>>,
    /// 非事务性生产者的回退 (当无 coordinator 时)
    fallback_next_id: std::sync::atomic::AtomicI64,
}

impl Default for InitProducerIdHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl InitProducerIdHandler {
    pub fn new() -> Self {
        Self {
            txn_coordinator: None,
            fallback_next_id: std::sync::atomic::AtomicI64::new(1000),
        }
    }

    /// 创建带事务协调器的处理器
    pub fn with_coordinator(txn_coordinator: Arc<TransactionCoordinator>) -> Self {
        Self {
            txn_coordinator: Some(txn_coordinator),
            fallback_next_id: std::sync::atomic::AtomicI64::new(1000),
        }
    }

    /// 处理 InitProducerId 请求
    pub fn handle(
        &self,
        request: InitProducerIdRequest,
        _version: i16,
    ) -> Result<InitProducerIdResponse> {
        debug!(
            transactional_id = ?request.transactional_id,
            "InitProducerId request"
        );

        // 如果有事务协调器且提供了 transactional_id
        if let Some(ref coordinator) = self.txn_coordinator {
            if let Some(ref txn_id) = request.transactional_id {
                let timeout = if request.transaction_timeout_ms > 0 {
                    Some(request.transaction_timeout_ms as u32)
                } else {
                    None
                };

                match coordinator.init_producer_id(txn_id, timeout) {
                    Ok((producer_id, producer_epoch)) => {
                        debug!(
                            producer_id = producer_id,
                            producer_epoch = producer_epoch,
                            transactional_id = %txn_id,
                            "Transaction producer initialized"
                        );
                        return Ok(InitProducerIdResponse {
                            throttle_time_ms: 0,
                            error_code: KafkaErrorCode::None,
                            producer_id,
                            producer_epoch,
                        });
                    }
                    Err(e) => {
                        debug!(error = %e, "Failed to init transaction producer");
                        return Ok(InitProducerIdResponse {
                            throttle_time_ms: 0,
                            error_code: KafkaErrorCode::UnknownServerError,
                            producer_id: -1,
                            producer_epoch: -1,
                        });
                    }
                }
            }
        }

        // 无协调器时，事务性请求返回不支持
        if request.transactional_id.is_some() {
            return Ok(InitProducerIdResponse {
                throttle_time_ms: 0,
                error_code: KafkaErrorCode::UnsupportedForMessageFormat,
                producer_id: -1,
                producer_epoch: -1,
            });
        }

        // v3+: 如果客户端提供了 producer_id，尝试重用
        let producer_id = if request.producer_id >= 0 {
            request.producer_id
        } else {
            self.fallback_next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        };

        let producer_epoch = if request.producer_epoch >= 0 {
            request.producer_epoch
        } else {
            0
        };

        debug!(
            producer_id = producer_id,
            producer_epoch = producer_epoch,
            "InitProducerId assigned"
        );

        Ok(InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            producer_id,
            producer_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_producer_id_basic() {
        let handler = InitProducerIdHandler::new();
        let req = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.producer_id >= 1000);
        assert_eq!(resp.producer_epoch, 0);
    }

    #[test]
    fn test_init_producer_id_incrementing() {
        let handler = InitProducerIdHandler::new();
        let req1 = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let req2 = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let resp1 = handler.handle(req1, 0).unwrap();
        let resp2 = handler.handle(req2, 0).unwrap();
        assert_eq!(resp2.producer_id, resp1.producer_id + 1);
    }

    #[test]
    fn test_init_producer_id_transactional_rejected() {
        let handler = InitProducerIdHandler::new();
        let req = InitProducerIdRequest {
            transactional_id: Some("my-txn".to_string()),
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::UnsupportedForMessageFormat);
    }

    #[test]
    fn test_init_producer_id_v3_reuse_pid() {
        let handler = InitProducerIdHandler::new();
        let req = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: 30000,
            producer_id: 5000,
            producer_epoch: 2,
        };
        let resp = handler.handle(req, 3).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.producer_id, 5000);
        assert_eq!(resp.producer_epoch, 2);
    }

    #[test]
    fn test_init_producer_id_with_coordinator() {
        use crate::producer_state_manager::ProducerStateManager;
        use crate::transaction_coordinator::TransactionCoordinator;

        let psm = Arc::new(ProducerStateManager::new());
        let coordinator = Arc::new(TransactionCoordinator::new(psm));
        let handler = InitProducerIdHandler::with_coordinator(coordinator);

        let req = InitProducerIdRequest {
            transactional_id: Some("my-txn".to_string()),
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.producer_id >= 1000);
        assert_eq!(resp.producer_epoch, 0);
    }

    #[test]
    fn test_init_producer_id_with_coordinator_reinit() {
        use crate::producer_state_manager::ProducerStateManager;
        use crate::transaction_coordinator::TransactionCoordinator;

        let psm = Arc::new(ProducerStateManager::new());
        let coordinator = Arc::new(TransactionCoordinator::new(psm));
        let handler = InitProducerIdHandler::with_coordinator(coordinator);

        // 第一次初始化
        let req1 = InitProducerIdRequest {
            transactional_id: Some("my-txn".to_string()),
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let resp1 = handler.handle(req1, 0).unwrap();
        assert_eq!(resp1.producer_epoch, 0);

        // 重新初始化 → epoch 递增
        let req2 = InitProducerIdRequest {
            transactional_id: Some("my-txn".to_string()),
            transaction_timeout_ms: 30000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let resp2 = handler.handle(req2, 0).unwrap();
        assert_eq!(resp2.producer_id, resp1.producer_id);
        assert_eq!(resp2.producer_epoch, 1);
    }
}
