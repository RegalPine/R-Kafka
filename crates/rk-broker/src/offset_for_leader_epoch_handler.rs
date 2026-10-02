//! OffsetForLeaderEpoch Handler
//!
//! 处理 OffsetForLeaderEpoch 请求 (API Key = 23)。
//! Phase 1: 单 Broker，返回当前 log end offset。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::offset_for_leader_epoch::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// OffsetForLeaderEpoch 请求处理器
pub struct OffsetForLeaderEpochHandler {
    partition_manager: Arc<PartitionManager>,
}

impl OffsetForLeaderEpochHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 OffsetForLeaderEpoch 请求
    pub fn handle(
        &self,
        request: OffsetForLeaderEpochRequest,
        _version: i16,
    ) -> Result<OffsetForLeaderEpochResponse> {
        debug!(
            topics = request.topics.len(),
            "OffsetForLeaderEpoch request"
        );

        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());

            for part_req in &topic_req.partitions {
                let key = crate::partition::PartitionKey {
                    topic: rk_core::types::TopicName(topic_req.topic.clone()),
                    partition: rk_core::types::PartitionId(part_req.partition),
                };

                // Phase 1: 返回当前 log end offset (LEO)
                let end_offset = self.partition_manager.get_log_end_offset(&key);

                match end_offset {
                    Some(offset) => {
                        response_partitions.push(OffsetForLeaderEpochResponsePartition {
                            error_code: KafkaErrorCode::None,
                            leader_epoch: part_req.leader_epoch,
                            end_offset: offset.0,
                        });
                    }
                    None => {
                        response_partitions.push(OffsetForLeaderEpochResponsePartition {
                            error_code: KafkaErrorCode::UnknownTopicOrPartition,
                            leader_epoch: -1,
                            end_offset: -1,
                        });
                    }
                }
            }

            response_topics.push(OffsetForLeaderEpochResponseTopic {
                topic: topic_req.topic.clone(),
                partitions: response_partitions,
            });
        }

        Ok(OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_storage::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_handler() -> OffsetForLeaderEpochHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        pm.get_or_create_topic("test", 3);
        // 写入一些数据
        let records = vec![0u8; 50];
        let batch = build_batch_bytes(0, 1, 0, 1000, 2000, -1, -1, -1, &records, 5);
        pm.append_batch("test", 0, &batch).unwrap();
        OffsetForLeaderEpochHandler::new(pm)
    }

    #[test]
    fn test_offset_for_leader_epoch_basic() {
        let handler = make_handler();
        let req = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochRequestTopic {
                topic: "test".to_string(),
                partitions: vec![OffsetForLeaderEpochRequestPartition {
                    current_leader_epoch: -1,
                    leader_epoch: 0,
                    partition: 0,
                }],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(
            resp.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(resp.topics[0].partitions[0].end_offset, 5);
    }

    #[test]
    fn test_offset_for_leader_epoch_unknown_topic() {
        let handler = make_handler();
        let req = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochRequestTopic {
                topic: "nonexistent".to_string(),
                partitions: vec![OffsetForLeaderEpochRequestPartition {
                    current_leader_epoch: -1,
                    leader_epoch: 0,
                    partition: 0,
                }],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(
            resp.topics[0].partitions[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
    }
}
