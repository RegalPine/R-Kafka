//! CreatePartitions Handler
//!
//! 处理 CreatePartitions 请求 (API Key = 37)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::create_partitions::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// CreatePartitions 请求处理器
pub struct CreatePartitionsHandler {
    partition_manager: Arc<PartitionManager>,
}

impl CreatePartitionsHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 CreatePartitions 请求
    pub fn handle(
        &self,
        request: CreatePartitionsRequest,
        _version: i16,
    ) -> Result<CreatePartitionsResponse> {
        debug!(
            topics = request.topics.len(),
            validate_only = request.validate_only,
            "CreatePartitions request"
        );

        let mut results = Vec::with_capacity(request.topics.len());

        for topic_req in &request.topics {
            let result = self.create_partitions_for_topic(
                &topic_req.name,
                topic_req.new_partitions_count,
                request.validate_only,
            );
            results.push(result);
        }

        Ok(CreatePartitionsResponse {
            throttle_time_ms: 0,
            results,
        })
    }

    fn create_partitions_for_topic(
        &self,
        topic_name: &str,
        new_count: i32,
        validate_only: bool,
    ) -> CreatePartitionsResponseResult {
        // 检查 topic 是否存在
        let current_count = self.partition_manager.get_partition_count(topic_name);
        match current_count {
            None => {
                return CreatePartitionsResponseResult {
                    name: topic_name.to_string(),
                    error_code: KafkaErrorCode::UnknownTopicOrPartition,
                    error_message: Some(format!("Topic '{}' not found", topic_name)),
                };
            }
            Some(current) => {
                if new_count < current {
                    return CreatePartitionsResponseResult {
                        name: topic_name.to_string(),
                        error_code: KafkaErrorCode::InvalidPartitions,
                        error_message: Some(format!(
                            "Partition count {} is less than current {}",
                            new_count, current
                        )),
                    };
                }
                if new_count == current {
                    return CreatePartitionsResponseResult {
                        name: topic_name.to_string(),
                        error_code: KafkaErrorCode::InvalidPartitions,
                        error_message: Some(format!("Topic already has {} partitions", current)),
                    };
                }
            }
        }

        if validate_only {
            return CreatePartitionsResponseResult {
                name: topic_name.to_string(),
                error_code: KafkaErrorCode::None,
                error_message: None,
            };
        }

        // 增加分区
        let added = self.partition_manager.add_partitions(topic_name, new_count);
        if added {
            debug!(topic = %topic_name, new_count = new_count, "Partitions added");
            CreatePartitionsResponseResult {
                name: topic_name.to_string(),
                error_code: KafkaErrorCode::None,
                error_message: None,
            }
        } else {
            CreatePartitionsResponseResult {
                name: topic_name.to_string(),
                error_code: KafkaErrorCode::UnknownServerError,
                error_message: Some("Failed to add partitions".to_string()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> CreatePartitionsHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        pm.get_or_create_topic("test", 3);
        CreatePartitionsHandler::new(pm)
    }

    #[test]
    fn test_create_partitions_success() {
        let handler = make_handler();
        let req = CreatePartitionsRequest {
            topics: vec![CreatePartitionsRequestTopic {
                name: "test".to_string(),
                new_partitions_count: 6,
                assignments: None,
            }],
            timeout_ms: 30000,
            validate_only: false,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_create_partitions_unknown_topic() {
        let handler = make_handler();
        let req = CreatePartitionsRequest {
            topics: vec![CreatePartitionsRequestTopic {
                name: "nonexistent".to_string(),
                new_partitions_count: 6,
                assignments: None,
            }],
            timeout_ms: 30000,
            validate_only: false,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(
            resp.results[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
    }

    #[test]
    fn test_create_partitions_validate_only() {
        let handler = make_handler();
        let req = CreatePartitionsRequest {
            topics: vec![CreatePartitionsRequestTopic {
                name: "test".to_string(),
                new_partitions_count: 6,
                assignments: None,
            }],
            timeout_ms: 30000,
            validate_only: true,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.results[0].error_code, KafkaErrorCode::None);
        // 实际分区数未变
        assert_eq!(
            handler.partition_manager.get_partition_count("test"),
            Some(3)
        );
    }

    #[test]
    fn test_create_partitions_reduced_count() {
        let handler = make_handler();
        let req = CreatePartitionsRequest {
            topics: vec![CreatePartitionsRequestTopic {
                name: "test".to_string(),
                new_partitions_count: 2, // 小于当前 3
                assignments: None,
            }],
            timeout_ms: 30000,
            validate_only: false,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(
            resp.results[0].error_code,
            KafkaErrorCode::InvalidPartitions
        );
    }
}
