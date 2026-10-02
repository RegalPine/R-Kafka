//! CreateTopics Handler
//!
//! 处理 CreateTopics 请求 (API Key = 19)。
//! Phase 1: 单副本，仅支持 partition_count 参数。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::create_topics::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// CreateTopics 请求处理器
pub struct CreateTopicsHandler {
    partition_manager: Arc<PartitionManager>,
}

impl CreateTopicsHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 CreateTopics 请求
    pub fn handle(
        &self,
        request: CreateTopicsRequest,
        _version: i16,
    ) -> Result<CreateTopicsResponse> {
        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic in &request.topics {
            let result = self.create_topic(topic, request.validate_only);
            response_topics.push(result);
        }

        debug!(
            topic_count = request.topics.len(),
            "CreateTopics handled"
        );

        Ok(CreateTopicsResponse::success(response_topics))
    }

    fn create_topic(
        &self,
        topic: &CreateTopicsRequestTopic,
        validate_only: bool,
    ) -> CreateTopicsResponseTopic {
        // 检查 topic 是否已存在
        if self.partition_manager.get_topic_metadata(&topic.name).is_some() {
            return CreateTopicsResponseTopic {
                name: topic.name.clone(),
                error_code: KafkaErrorCode::TopicAlreadyExists,
                error_message: Some(format!("Topic '{}' already exists", topic.name)),
            };
        }

        // 确定 partition 数量
        let num_partitions = if topic.num_partitions == -1 {
            1 // 默认 1 个 partition
        } else if topic.num_partitions <= 0 {
            return CreateTopicsResponseTopic {
                name: topic.name.clone(),
                error_code: KafkaErrorCode::InvalidPartitions,
                error_message: Some(format!(
                    "Invalid partition count: {}", topic.num_partitions
                )),
            };
        } else {
            topic.num_partitions as u32
        };

        // 检查 replication_factor
        if topic.replication_factor != -1 && topic.replication_factor != 1 {
            return CreateTopicsResponseTopic {
                name: topic.name.clone(),
                error_code: KafkaErrorCode::InvalidReplicationFactor,
                error_message: Some(format!(
                    "Phase 1 only supports replication_factor=1, got {}",
                    topic.replication_factor
                )),
            };
        }

        // validate_only 模式: 只验证不创建
        if validate_only {
            return CreateTopicsResponseTopic {
                name: topic.name.clone(),
                error_code: KafkaErrorCode::None,
                error_message: None,
            };
        }

        // 创建 topic
        self.partition_manager.get_or_create_topic(&topic.name, num_partitions);

        debug!(
            topic = %topic.name,
            partitions = num_partitions,
            "Topic created"
        );

        CreateTopicsResponseTopic {
            name: topic.name.clone(),
            error_code: KafkaErrorCode::None,
            error_message: None,
        }
    }

    /// 获取 PartitionManager 引用
    pub fn partition_manager(&self) -> &Arc<PartitionManager> {
        &self.partition_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> CreateTopicsHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1));
        CreateTopicsHandler::new(pm)
    }

    #[test]
    fn test_create_topic_success() {
        let handler = make_handler();
        let request = CreateTopicsRequest {
            topics: vec![CreateTopicsRequestTopic {
                name: "new-topic".to_string(),
                num_partitions: 3,
                replication_factor: 1,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30000,
            validate_only: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);

        // 验证 topic 确实创建了
        let meta = handler.partition_manager.get_topic_metadata("new-topic");
        assert!(meta.is_some());
        assert_eq!(meta.unwrap().partition_count, 3);
    }

    #[test]
    fn test_create_topic_already_exists() {
        let handler = make_handler();
        handler.partition_manager.get_or_create_topic("existing", 1);

        let request = CreateTopicsRequest {
            topics: vec![CreateTopicsRequestTopic {
                name: "existing".to_string(),
                num_partitions: 1,
                replication_factor: 1,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30000,
            validate_only: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::TopicAlreadyExists);
    }

    #[test]
    fn test_create_topic_auto_partitions() {
        let handler = make_handler();
        let request = CreateTopicsRequest {
            topics: vec![CreateTopicsRequestTopic {
                name: "auto-topic".to_string(),
                num_partitions: -1, // auto
                replication_factor: -1, // auto
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30000,
            validate_only: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);

        let meta = handler.partition_manager.get_topic_metadata("auto-topic");
        assert_eq!(meta.unwrap().partition_count, 1); // default 1
    }

    #[test]
    fn test_create_topic_validate_only() {
        let handler = make_handler();
        let request = CreateTopicsRequest {
            topics: vec![CreateTopicsRequestTopic {
                name: "validate-topic".to_string(),
                num_partitions: 2,
                replication_factor: 1,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30000,
            validate_only: true,
        };

        let response = handler.handle(request, 1).unwrap();
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);

        // validate_only: topic 不应被创建
        let meta = handler.partition_manager.get_topic_metadata("validate-topic");
        assert!(meta.is_none());
    }

    #[test]
    fn test_create_topic_invalid_replication_factor() {
        let handler = make_handler();
        let request = CreateTopicsRequest {
            topics: vec![CreateTopicsRequestTopic {
                name: "bad-rf".to_string(),
                num_partitions: 1,
                replication_factor: 3, // Phase 1 only supports 1
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30000,
            validate_only: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::InvalidReplicationFactor);
    }

    #[test]
    fn test_create_multiple_topics() {
        let handler = make_handler();
        let request = CreateTopicsRequest {
            topics: vec![
                CreateTopicsRequestTopic {
                    name: "topic-a".to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                    assignments: vec![],
                    configs: vec![],
                },
                CreateTopicsRequestTopic {
                    name: "topic-b".to_string(),
                    num_partitions: 2,
                    replication_factor: 1,
                    assignments: vec![],
                    configs: vec![],
                },
            ],
            timeout_ms: 30000,
            validate_only: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.topics[1].error_code, KafkaErrorCode::None);
    }
}
