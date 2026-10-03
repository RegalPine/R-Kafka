//! TxnOffsetCommit Handler
//!
//! 处理 TxnOffsetCommit 请求 (API Key = 27)。
//! 事务性消费者偏移量提交：将消费者组偏移量作为事务的一部分提交。
//! 与 AddOffsetsToTxn (25) + EndTxn (26) 配合使用。
//! Phase 1: 直接写入 OffsetManager（事务提交在 EndTxn 时生效）。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::txn_offset_commit::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::offset_manager::OffsetManager;
use crate::producer_state_manager::ProducerStateManager;

/// TxnOffsetCommit 请求处理器
pub struct TxnOffsetCommitHandler {
    offset_manager: Arc<OffsetManager>,
    producer_state_manager: Arc<ProducerStateManager>,
}

impl TxnOffsetCommitHandler {
    pub fn new(
        offset_manager: Arc<OffsetManager>,
        producer_state_manager: Arc<ProducerStateManager>,
    ) -> Self {
        Self {
            offset_manager,
            producer_state_manager,
        }
    }

    /// 处理 TxnOffsetCommit 请求
    pub fn handle(
        &self,
        request: TxnOffsetCommitRequest,
        _version: i16,
    ) -> Result<TxnOffsetCommitResponse> {
        debug!(
            transactional_id = %request.transactional_id,
            group_id = %request.group_id,
            producer_id = request.producer_id,
            topics = request.topics.len(),
            "TxnOffsetCommit request"
        );

        // 验证生产者是否已注册
        if self
            .producer_state_manager
            .get_producer_state(request.producer_id)
            .is_none()
        {
            // 生产者未注册，自动注册（简化实现）
            self.producer_state_manager.register_producer(
                request.producer_id,
                request.producer_epoch,
                Some(request.transactional_id.clone()),
            );
        }

        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic.partitions.len());

            for partition in &topic.partitions {
                // 提交偏移量到 OffsetManager
                // Phase 1: 直接提交，不做事务隔离（EndTxn 提交时无需额外操作）
                let result = self.offset_manager.commit_offset(
                    &request.group_id,
                    &topic.name,
                    partition.partition_index,
                    partition.committed_offset,
                    partition.committed_leader_epoch,
                    partition.committed_metadata.clone(),
                );

                let error_code = match result {
                    Ok(()) => KafkaErrorCode::None,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to commit transactional offset");
                        KafkaErrorCode::UnknownServerError
                    }
                };

                response_partitions.push(TxnOffsetCommitResponsePartition {
                    partition_index: partition.partition_index,
                    error_code,
                });
            }

            response_topics.push(TxnOffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: response_partitions,
            });
        }

        debug!(
            transactional_id = %request.transactional_id,
            group_id = %request.group_id,
            "TxnOffsetCommit completed"
        );

        Ok(TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handler() -> TxnOffsetCommitHandler {
        TxnOffsetCommitHandler::new(
            Arc::new(OffsetManager::new(None)),
            Arc::new(ProducerStateManager::new()),
        )
    }

    #[test]
    fn test_txn_offset_commit_success() {
        let handler = make_handler();
        let req = TxnOffsetCommitRequest {
            transactional_id: "txn-1".to_string(),
            group_id: "my-group".to_string(),
            producer_id: 1000,
            producer_epoch: 0,
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
            topics: vec![TxnOffsetCommitRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![
                    TxnOffsetCommitRequestPartition {
                        partition_index: 0,
                        committed_offset: 42,
                        committed_leader_epoch: -1,
                        committed_metadata: None,
                    },
                    TxnOffsetCommitRequestPartition {
                        partition_index: 1,
                        committed_offset: 100,
                        committed_leader_epoch: -1,
                        committed_metadata: Some("meta".to_string()),
                    },
                ],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "test-topic");
        assert_eq!(resp.topics[0].partitions.len(), 2);
        for part in &resp.topics[0].partitions {
            assert_eq!(part.error_code, KafkaErrorCode::None);
        }
    }

    #[test]
    fn test_txn_offset_commit_empty_topics() {
        let handler = make_handler();
        let req = TxnOffsetCommitRequest {
            transactional_id: "txn-empty".to_string(),
            group_id: "empty-group".to_string(),
            producer_id: 2000,
            producer_epoch: 1,
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
            topics: vec![],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert!(resp.topics.is_empty());
        assert_eq!(resp.throttle_time_ms, 0);
    }

    #[test]
    fn test_txn_offset_commit_v3_flexible() {
        let handler = make_handler();
        let req = TxnOffsetCommitRequest {
            transactional_id: "txn-flex".to_string(),
            group_id: "flex-group".to_string(),
            producer_id: 3000,
            producer_epoch: 2,
            generation_id: 5,
            member_id: "member-flex".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            topics: vec![TxnOffsetCommitRequestTopic {
                name: "flex-topic".to_string(),
                partitions: vec![TxnOffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 99,
                    committed_leader_epoch: 0,
                    committed_metadata: None,
                }],
            }],
        };
        let resp = handler.handle(req, 3).unwrap();
        assert_eq!(resp.topics[0].partitions[0].error_code, KafkaErrorCode::None);
    }
}
