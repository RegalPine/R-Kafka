//! OffsetCommit Handler
//!
//! 处理 OffsetCommit 请求 (API Key = 8)。
//! Phase 1: 写入内存 OffsetManager。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::offset_commit::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::offset_manager::OffsetManager;

/// OffsetCommit 请求处理器
pub struct OffsetCommitHandler {
    offset_manager: Arc<OffsetManager>,
}

impl OffsetCommitHandler {
    pub fn new(offset_manager: Arc<OffsetManager>) -> Self {
        Self { offset_manager }
    }

    /// 处理 OffsetCommit 请求
    pub fn handle(
        &self,
        request: OffsetCommitRequest,
        _version: i16,
    ) -> Result<OffsetCommitResponse> {
        let group_id = &request.group_id;

        debug!(
            group_id = %group_id,
            topics = request.topics.len(),
            "OffsetCommit request"
        );

        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic.partitions.len());

            for partition in &topic.partitions {
                let result = self.offset_manager.commit_offset(
                    group_id,
                    &topic.name,
                    partition.index,
                    partition.committed_offset,
                    partition.committed_leader_epoch,
                    partition.metadata.clone(),
                );

                let error_code = match result {
                    Ok(()) => KafkaErrorCode::None,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to commit offset");
                        KafkaErrorCode::UnknownServerError
                    }
                };

                response_partitions.push(OffsetCommitResponsePartition {
                    index: partition.index,
                    error_code,
                });
            }

            response_topics.push(OffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: response_partitions,
            });
        }

        Ok(OffsetCommitResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        })
    }

    /// 获取 OffsetManager 引用
    pub fn offset_manager(&self) -> &Arc<OffsetManager> {
        &self.offset_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handler() -> OffsetCommitHandler {
        OffsetCommitHandler::new(Arc::new(OffsetManager::new(None)))
    }

    #[test]
    fn test_offset_commit_handler_basic() {
        let handler = make_handler();
        let request = OffsetCommitRequest {
            group_id: "group-1".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: "topic-a".to_string(),
                partitions: vec![OffsetCommitRequestPartition {
                    index: 0,
                    committed_offset: 42,
                    committed_leader_epoch: -1,
                    commit_timestamp: -1,
                    metadata: None,
                }],
            }],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );

        // 验证已提交
        let offset = handler
            .offset_manager()
            .fetch_offset("group-1", "topic-a", 0)
            .unwrap();
        assert_eq!(offset.offset, 42);
    }

    #[test]
    fn test_offset_commit_handler_multiple_partitions() {
        let handler = make_handler();
        let request = OffsetCommitRequest {
            group_id: "group-1".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: "topic-a".to_string(),
                partitions: vec![
                    OffsetCommitRequestPartition {
                        index: 0,
                        committed_offset: 10,
                        committed_leader_epoch: -1,
                        commit_timestamp: -1,
                        metadata: None,
                    },
                    OffsetCommitRequestPartition {
                        index: 1,
                        committed_offset: 20,
                        committed_leader_epoch: -1,
                        commit_timestamp: -1,
                        metadata: Some("meta".to_string()),
                    },
                ],
            }],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].partitions.len(), 2);
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(
            response.topics[0].partitions[1].error_code,
            KafkaErrorCode::None
        );
    }

    #[test]
    fn test_offset_commit_handler_overwrite() {
        let handler = make_handler();

        // 第一次提交
        let request1 = OffsetCommitRequest {
            group_id: "group-1".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: "topic-a".to_string(),
                partitions: vec![OffsetCommitRequestPartition {
                    index: 0,
                    committed_offset: 10,
                    committed_leader_epoch: -1,
                    commit_timestamp: -1,
                    metadata: None,
                }],
            }],
        };
        handler.handle(request1, 0).unwrap();

        // 第二次提交 (覆盖)
        let request2 = OffsetCommitRequest {
            group_id: "group-1".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: "topic-a".to_string(),
                partitions: vec![OffsetCommitRequestPartition {
                    index: 0,
                    committed_offset: 50,
                    committed_leader_epoch: -1,
                    commit_timestamp: -1,
                    metadata: None,
                }],
            }],
        };
        handler.handle(request2, 0).unwrap();

        let offset = handler
            .offset_manager()
            .fetch_offset("group-1", "topic-a", 0)
            .unwrap();
        assert_eq!(offset.offset, 50);
    }
}
