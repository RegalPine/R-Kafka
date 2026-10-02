//! DescribeTopics Handler
//!
//! 处理 DescribeTopics 请求 (API Key = 70)。
//! Phase 1: 从 PartitionManager 查询 Topic 详细信息。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::describe_topics::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// DescribeTopics 请求处理器
pub struct DescribeTopicsHandler {
    broker_id: i32,
    partition_manager: Arc<PartitionManager>,
}

impl DescribeTopicsHandler {
    pub fn new(broker_id: i32, partition_manager: Arc<PartitionManager>) -> Self {
        Self { broker_id, partition_manager }
    }

    /// 处理 DescribeTopics 请求
    pub fn handle(
        &self,
        request: DescribeTopicsRequest,
        _version: i16,
    ) -> Result<DescribeTopicsResponse> {
        let topic_names: Option<Vec<&str>> = request.topics.as_ref().map(|topics| {
            topics.iter().map(|t| t.name.as_str()).collect()
        });

        debug!(
            has_filter = topic_names.is_some(),
            filter_count = topic_names.as_ref().map(|v| v.len()).unwrap_or(0),
            "DescribeTopics request"
        );

        let all_topics = self.partition_manager.list_topics();

        let topics_to_describe: Vec<String> = match topic_names {
            Some(names) => names.into_iter().map(|s| s.to_string()).collect(),
            None => all_topics.iter().map(|tn| tn.0.clone()).collect(),
        };

        let mut response_topics = Vec::new();

        for topic_name in &topics_to_describe {
            if let Some(partition_count) = self.partition_manager.get_partition_count(topic_name) {
                let mut partitions = Vec::new();
                for p in 0..partition_count {
                    partitions.push(DescribeTopicsResponsePartition {
                        partition_index: p,
                        leader_id: self.broker_id,
                        leader_epoch: 0,
                        replica_nodes: vec![self.broker_id],
                        isr_nodes: vec![self.broker_id],
                        offline_replicas: vec![],
                        error_code: KafkaErrorCode::None,
                        error_message: None,
                    });
                }

                response_topics.push(DescribeTopicsResponseTopic {
                    name: topic_name.to_string(),
                    topic_id: [0u8; 16], // Phase 1: 不支持 topic ID
                    is_internal: false,
                    partitions,
                    topic_authorized_operations: -2147483648, // NOT_AUTHORIZED
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                });
            } else {
                response_topics.push(DescribeTopicsResponseTopic {
                    name: topic_name.to_string(),
                    topic_id: [0u8; 16],
                    is_internal: false,
                    partitions: vec![],
                    topic_authorized_operations: -2147483648,
                    error_code: KafkaErrorCode::UnknownTopicOrPartition,
                    error_message: Some(format!("Topic '{}' not found", topic_name)),
                });
            }
        }

        Ok(DescribeTopicsResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_handler() -> DescribeTopicsHandler {
        let pm = PartitionManager::new(PathBuf::from("/tmp/dt_test"), 1024 * 1024, 0);
        DescribeTopicsHandler::new(0, Arc::new(pm))
    }

    #[test]
    fn test_describe_existing_topic() {
        let pm = PartitionManager::new(PathBuf::from("/tmp/dt_existing"), 1024 * 1024, 0);
        pm.get_or_create_topic("test", 3);
        let handler = DescribeTopicsHandler::new(0, Arc::new(pm));

        let req = DescribeTopicsRequest {
            topics: Some(vec![DescribeTopicsRequestTopic {
                name: "test".to_string(),
            }]),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "test");
        assert_eq!(resp.topics[0].partitions.len(), 3);
        assert_eq!(resp.topics[0].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_describe_unknown_topic() {
        let handler = make_handler();
        let req = DescribeTopicsRequest {
            topics: Some(vec![DescribeTopicsRequestTopic {
                name: "nonexistent".to_string(),
            }]),
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].error_code, KafkaErrorCode::UnknownTopicOrPartition);
    }

    #[test]
    fn test_describe_null_topics_returns_all() {
        let pm = PartitionManager::new(PathBuf::from("/tmp/dt_null"), 1024 * 1024, 0);
        pm.get_or_create_topic("t1", 1);
        pm.get_or_create_topic("t2", 2);
        let handler = DescribeTopicsHandler::new(0, Arc::new(pm));

        let req = DescribeTopicsRequest { topics: None };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics.len(), 2);
    }
}
