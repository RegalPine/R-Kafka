//! ListPartitionReassignments Handler
//!
//! 处理 ListPartitionReassignments 请求 (API Key = 46)。
//! Phase 1: 单 Broker，没有正在进行的重新分配，返回空列表。

use rk_core::error::Result;
use rk_protocol::apis::list_partition_reassignments::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

/// ListPartitionReassignments 请求处理器
pub struct ListPartitionReassignmentsHandler;

impl ListPartitionReassignmentsHandler {
    pub fn new() -> Self {
        Self
    }

    /// 处理 ListPartitionReassignments 请求
    pub fn handle(
        &self,
        request: ListPartitionReassignmentsRequest,
        _version: i16,
    ) -> Result<ListPartitionReassignmentsResponse> {
        debug!(
            timeout_ms = request.timeout_ms,
            has_topics = request.topics.is_some(),
            "ListPartitionReassignments request"
        );

        // Phase 1: 单 Broker，没有正在进行的重新分配
        // 如果请求指定了 topics，返回空列表；如果 null，也返回空列表
        let response_topics = match request.topics {
            Some(topics) => topics
                .into_iter()
                .map(|topic_req| ListPartitionReassignmentsResponseTopic {
                    name: topic_req.name,
                    partitions: topic_req
                        .partitions
                        .into_iter()
                        .map(|idx| ListPartitionReassignmentsResponsePartition {
                            index: idx,
                            replicas: vec![],
                            adding_replicas: vec![],
                            removing_replicas: vec![],
                        })
                        .collect(),
                })
                .collect(),
            None => vec![],
        };

        Ok(ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            error_message: None,
            topics: response_topics,
        })
    }
}

impl Default for ListPartitionReassignmentsHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_partition_reassignments_null_topics() {
        let handler = ListPartitionReassignmentsHandler::new();
        let req = ListPartitionReassignmentsRequest {
            timeout_ms: 30000,
            topics: None,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_list_partition_reassignments_with_topics() {
        let handler = ListPartitionReassignmentsHandler::new();
        let req = ListPartitionReassignmentsRequest {
            timeout_ms: 30000,
            topics: Some(vec![ListPartitionReassignmentsRequestTopic {
                name: "test".to_string(),
                partitions: vec![0, 1],
            }]),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].partitions.len(), 2);
        // Phase 1: 没有正在进行的重新分配
        assert!(resp.topics[0].partitions[0].adding_replicas.is_empty());
    }
}
