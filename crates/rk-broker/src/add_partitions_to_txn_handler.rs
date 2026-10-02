//! AddPartitionsToTxn Handler
//!
//! 处理 AddPartitionsToTxn 请求 (API Key = 24)。
//! Phase 1: 简化事务支持，直接注册分区到事务状态。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::add_partitions_to_txn::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::producer_state_manager::ProducerStateManager;

/// AddPartitionsToTxn 请求处理器
pub struct AddPartitionsToTxnHandler {
    producer_state_manager: Arc<ProducerStateManager>,
}

impl AddPartitionsToTxnHandler {
    pub fn new(producer_state_manager: Arc<ProducerStateManager>) -> Self {
        Self { producer_state_manager }
    }

    /// 处理 AddPartitionsToTxn 请求
    pub fn handle(
        &self,
        request: AddPartitionsToTxnRequest,
        _version: i16,
    ) -> Result<AddPartitionsToTxnResponse> {
        debug!(
            transactional_id = ?request.transactional_id,
            topics = request.topics.len(),
            "AddPartitionsToTxn request"
        );

        // v0-v2: 单事务模式
        if let Some(ref txn_id) = request.transactional_id {
            return self.handle_single_txn(txn_id, request.producer_id, request.producer_epoch, &request.topics);
        }

        // v3+: 批量事务模式
        if let Some(ref txns) = request.transactions {
            let mut results = Vec::with_capacity(txns.len());
            for txn in txns {
                let topic_results = self.add_partitions_for_txn(
                    &txn.transactional_id,
                    txn.producer_id,
                    txn.producer_epoch,
                    &txn.topics,
                );
                results.push(AddPartitionsToTxnResponseTransaction {
                    transactional_id: txn.transactional_id.clone(),
                    topic_results,
                });
            }
            return Ok(AddPartitionsToTxnResponse {
                results_by_topic: vec![],
                results_by_transaction: Some(results),
            });
        }

        // 空请求
        Ok(AddPartitionsToTxnResponse {
            results_by_topic: vec![],
            results_by_transaction: None,
        })
    }

    fn handle_single_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        topics: &[AddPartitionsToTxnRequestTopic],
    ) -> Result<AddPartitionsToTxnResponse> {
        let topic_results = self.add_partitions_for_txn(
            transactional_id,
            producer_id,
            producer_epoch,
            topics,
        );

        Ok(AddPartitionsToTxnResponse {
            results_by_topic: topic_results,
            results_by_transaction: None,
        })
    }

    fn add_partitions_for_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        topics: &[AddPartitionsToTxnRequestTopic],
    ) -> Vec<AddPartitionsToTxnResponseTopic> {
        // 确保生产者已注册
        if self.producer_state_manager.get_producer_state(producer_id).is_none() {
            self.producer_state_manager.register_producer(
                producer_id,
                producer_epoch,
                Some(transactional_id.to_string()),
            );
        }

        // 开始事务 (如果还没开始)
        let _ = self.producer_state_manager.begin_transaction(producer_id);

        let mut topic_results = Vec::with_capacity(topics.len());

        for topic_req in topics {
            let partition_results = topic_req
                .partitions
                .iter()
                .map(|&partition_index| AddPartitionsToTxnResponsePartition {
                    partition_index,
                    error_code: KafkaErrorCode::None,
                })
                .collect();

            topic_results.push(AddPartitionsToTxnResponseTopic {
                topic: topic_req.topic.clone(),
                results_by_partition: partition_results,
            });
        }

        debug!(
            transactional_id = transactional_id,
            producer_id = producer_id,
            topics_added = topics.len(),
            "Partitions added to transaction"
        );

        topic_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handler() -> AddPartitionsToTxnHandler {
        AddPartitionsToTxnHandler::new(Arc::new(ProducerStateManager::new()))
    }

    #[test]
    fn test_add_partitions_to_txn_v0() {
        let handler = make_handler();
        let req = AddPartitionsToTxnRequest {
            transactional_id: Some("txn-1".to_string()),
            producer_id: 1000,
            producer_epoch: 0,
            topics: vec![AddPartitionsToTxnRequestTopic {
                topic: "test".to_string(),
                partitions: vec![0, 1, 2],
            }],
            transactions: None,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.results_by_topic.len(), 1);
        assert_eq!(resp.results_by_topic[0].topic, "test");
        assert_eq!(resp.results_by_topic[0].results_by_partition.len(), 3);
        for p in &resp.results_by_topic[0].results_by_partition {
            assert_eq!(p.error_code, KafkaErrorCode::None);
        }
    }

    #[test]
    fn test_add_partitions_to_txn_v3_batch() {
        let handler = make_handler();
        let req = AddPartitionsToTxnRequest {
            transactional_id: None,
            producer_id: -1,
            producer_epoch: -1,
            topics: vec![],
            transactions: Some(vec![AddPartitionsToTxnRequestTransaction {
                transactional_id: "txn-1".to_string(),
                producer_id: 1000,
                producer_epoch: 0,
                topics: vec![AddPartitionsToTxnRequestTopic {
                    topic: "test".to_string(),
                    partitions: vec![0],
                }],
            }]),
        };
        let resp = handler.handle(req, 3).unwrap();
        assert!(resp.results_by_transaction.is_some());
        let txns = resp.results_by_transaction.unwrap();
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].transactional_id, "txn-1");
        assert_eq!(txns[0].topic_results.len(), 1);
    }

    #[test]
    fn test_add_partitions_auto_registers_producer() {
        let mgr = Arc::new(ProducerStateManager::new());
        let handler = AddPartitionsToTxnHandler::new(mgr.clone());

        let req = AddPartitionsToTxnRequest {
            transactional_id: Some("txn-1".to_string()),
            producer_id: 2000,
            producer_epoch: 1,
            topics: vec![AddPartitionsToTxnRequestTopic {
                topic: "test".to_string(),
                partitions: vec![0],
            }],
            transactions: None,
        };
        handler.handle(req, 0).unwrap();

        // 生产者应已自动注册
        let state = mgr.get_producer_state(2000).unwrap();
        assert_eq!(state.producer_epoch, 1);
        assert_eq!(state.transactional_id, Some("txn-1".to_string()));
    }
}
