//! ElectLeaders Handler
//!
//! 处理 ElectLeaders 请求 (API Key = 43)。
//! Phase 1: 单 Broker，所有 partition 的 leader 就是当前 Broker，
//! 因此 preferred leader 选举总是 "成功" (无操作)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::elect_leaders::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// ElectLeaders 请求处理器
pub struct ElectLeadersHandler {
    partition_manager: Arc<PartitionManager>,
}

impl ElectLeadersHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 ElectLeaders 请求
    pub fn handle(
        &self,
        request: ElectLeadersRequest,
        _version: i16,
    ) -> Result<ElectLeadersResponse> {
        debug!(
            election_type = request.election_type,
            has_topics = request.topic_partitions.is_some(),
            timeout_ms = request.timeout_ms,
            "ElectLeaders request"
        );

        // Phase 1: 单 Broker，所有 partition 的 leader 就是当前 Broker
        // Preferred leader 选举总是成功 (无操作)
        let mut results = Vec::new();

        match &request.topic_partitions {
            Some(topics) => {
                for topic_req in topics {
                    let mut response_partitions = Vec::new();
                    for &partition_idx in &topic_req.partitions {
                        // 检查 topic/partition 是否存在
                        let exists = partition_manager_has_partition(
                            &self.partition_manager,
                            &topic_req.topic,
                            partition_idx,
                        );
                        let error_code = if exists {
                            KafkaErrorCode::None // Phase 1: 已经是 leader
                        } else {
                            KafkaErrorCode::UnknownTopicOrPartition
                        };
                        response_partitions.push(ElectLeadersResponsePartition {
                            partition: partition_idx,
                            error_code,
                        });
                    }
                    results.push(ElectLeadersResponseTopic {
                        topic: topic_req.topic.clone(),
                        partitions: response_partitions,
                    });
                }
            }
            None => {
                // null = 所有 partition (Phase 1: 返回空结果，因为已经是 leader)
            }
        }

        Ok(ElectLeadersResponse {
            throttle_time_ms: 0,
            results,
            error_code: KafkaErrorCode::None,
        })
    }
}

/// 检查 topic/partition 是否存在
fn partition_manager_has_partition(pm: &PartitionManager, topic: &str, partition: i32) -> bool {
    pm.get_partition_count(topic)
        .map(|count| partition >= 0 && partition < count)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> ElectLeadersHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        pm.get_or_create_topic("test", 3);
        ElectLeadersHandler::new(pm)
    }

    #[test]
    fn test_elect_leaders_preferred() {
        let handler = make_handler();
        let req = ElectLeadersRequest {
            election_type: 0, // Preferred
            topic_partitions: Some(vec![ElectLeadersRequestTopic {
                topic: "test".to_string(),
                partitions: vec![0, 1, 2],
            }]),
            timeout_ms: 30000,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].partitions.len(), 3);
        for p in &resp.results[0].partitions {
            assert_eq!(p.error_code, KafkaErrorCode::None);
        }
    }

    #[test]
    fn test_elect_leaders_unknown_topic() {
        let handler = make_handler();
        let req = ElectLeadersRequest {
            election_type: 0,
            topic_partitions: Some(vec![ElectLeadersRequestTopic {
                topic: "nonexistent".to_string(),
                partitions: vec![0],
            }]),
            timeout_ms: 30000,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(
            resp.results[0].partitions[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
    }

    #[test]
    fn test_elect_leaders_null_topics() {
        let handler = make_handler();
        let req = ElectLeadersRequest {
            election_type: 0,
            topic_partitions: None, // 所有 partition
            timeout_ms: 30000,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert!(resp.results.is_empty()); // Phase 1: 不返回详细结果
    }
}
