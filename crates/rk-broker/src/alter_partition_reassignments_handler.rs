//! AlterPartitionReassignments Handler
//!
//! 处理 AlterPartitionReassignments 请求 (API Key = 45)。
//! Phase 1: 单 Broker，不支持副本重新分配，返回 Unsupported 错误。

use rk_core::error::Result;
use rk_protocol::apis::alter_partition_reassignments::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

/// AlterPartitionReassignments 请求处理器
pub struct AlterPartitionReassignmentsHandler;

impl AlterPartitionReassignmentsHandler {
    pub fn new() -> Self {
        Self
    }

    /// 处理 AlterPartitionReassignments 请求
    pub fn handle(
        &self,
        request: AlterPartitionReassignmentsRequest,
        _version: i16,
    ) -> Result<AlterPartitionReassignmentsResponse> {
        debug!(
            topics = request.topics.len(),
            timeout_ms = request.timeout_ms,
            "AlterPartitionReassignments request"
        );

        // Phase 1: 单 Broker，不支持副本重新分配
        let mut response_topics = Vec::with_capacity(request.topics.len());
        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());
            for part_req in &topic_req.partitions {
                response_partitions.push(AlterPartitionReassignmentsResponsePartition {
                    index: part_req.index,
                    error_code: KafkaErrorCode::UnsupportedVersion,
                    error_message: Some("Partition reassignment is not supported in single-broker mode".to_string()),
                });
            }
            response_topics.push(AlterPartitionReassignmentsResponseTopic {
                name: topic_req.name.clone(),
                partitions: response_partitions,
            });
        }

        Ok(AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            topics: response_topics,
        })
    }
}

impl Default for AlterPartitionReassignmentsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alter_partition_reassignments_unsupported() {
        let handler = AlterPartitionReassignmentsHandler::new();
        let req = AlterPartitionReassignmentsRequest {
            timeout_ms: 30000,
            topics: vec![AlterPartitionReassignmentsRequestTopic {
                name: "test".to_string(),
                partitions: vec![AlterPartitionReassignmentsRequestPartition {
                    index: 0,
                    replicas: Some(vec![1, 2]),
                }],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.topics[0].partitions[0].error_code, KafkaErrorCode::UnsupportedVersion);
    }
}
