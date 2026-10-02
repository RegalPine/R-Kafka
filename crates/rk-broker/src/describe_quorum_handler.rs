//! DescribeQuorum Handler
//!
//! 处理 DescribeQuorum 请求 (API Key = 56)。
//! Phase 1: 单 Broker，当前 Broker 就是唯一 leader。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::describe_quorum::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// DescribeQuorum 请求处理器
pub struct DescribeQuorumHandler {
    broker_id: i32,
    partition_manager: Arc<PartitionManager>,
}

impl DescribeQuorumHandler {
    pub fn new(broker_id: i32, partition_manager: Arc<PartitionManager>) -> Self {
        Self {
            broker_id,
            partition_manager,
        }
    }

    /// 处理 DescribeQuorum 请求
    pub fn handle(
        &self,
        request: DescribeQuorumRequest,
        _version: i16,
    ) -> Result<DescribeQuorumResponse> {
        debug!(topics = request.topics.len(), "DescribeQuorum request");

        let mut response_topics = Vec::new();

        for req_topic in &request.topics {
            let mut response_partitions = Vec::new();

            for req_part in &req_topic.partitions {
                // Phase 1: 当前 Broker 是唯一的 leader 和 voter
                let high_watermark = self
                    .partition_manager
                    .high_watermark(&req_topic.topic_name, req_part.partition_index)
                    .map(|o| o.0)
                    .unwrap_or(0i64);

                response_partitions.push(DescribeQuorumResponsePartition {
                    partition_index: req_part.partition_index,
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                    leader_id: self.broker_id,
                    leader_epoch: 0,
                    high_watermark,
                    current_voters: vec![DescribeQuorumResponseVoter {
                        voter_id: self.broker_id,
                        log_end_offset: high_watermark,
                    }],
                    observers: vec![],
                });
            }

            response_topics.push(DescribeQuorumResponseTopic {
                topic_name: req_topic.topic_name.clone(),
                partitions: response_partitions,
            });
        }

        Ok(DescribeQuorumResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            topics: response_topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_describe_quorum_basic() {
        let pm = PartitionManager::new(PathBuf::from("/tmp/dq_basic"), 1024 * 1024, 0);
        pm.get_or_create_topic("test", 2);
        let handler = DescribeQuorumHandler::new(1, Arc::new(pm));

        let request = DescribeQuorumRequest {
            topics: vec![DescribeQuorumRequestTopic {
                topic_name: "test".to_string(),
                partitions: vec![
                    DescribeQuorumRequestPartition { partition_index: 0 },
                    DescribeQuorumRequestPartition { partition_index: 1 },
                ],
            }],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions.len(), 2);
        assert_eq!(response.topics[0].partitions[0].leader_id, 1);
        assert_eq!(response.topics[0].partitions[0].current_voters.len(), 1);
        assert_eq!(
            response.topics[0].partitions[0].current_voters[0].voter_id,
            1
        );
    }

    #[test]
    fn test_describe_quorum_empty() {
        let pm = PartitionManager::new(PathBuf::from("/tmp/dq_empty"), 1024 * 1024, 0);
        let handler = DescribeQuorumHandler::new(0, Arc::new(pm));

        let request = DescribeQuorumRequest { topics: vec![] };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 0);
    }
}
