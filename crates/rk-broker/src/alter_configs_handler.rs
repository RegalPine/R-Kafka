//! AlterConfigs Handler
//!
//! 处理 AlterConfigs 请求 (API Key = 33)。
//! Phase 1: 仅支持 Topic 配置运行时修改 (内存中, 不持久化)。

use std::collections::HashMap;
use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::alter_configs::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// 可修改的 Topic 配置项白名单
const MUTABLE_TOPIC_CONFIGS: &[&str] = &[
    "retention.ms",
    "retention.bytes",
    "max.message.bytes",
    "min.insync.replicas",
    "unclean.leader.election.enable",
    "cleanup.policy",
    "compression.type",
    "delete.retention.ms",
    "max.compaction.lag.ms",
    "min.cleanable.dirty.ratio",
    "min.compaction.lag.ms",
    "segment.ms",
];

/// AlterConfigs 请求处理器
pub struct AlterConfigsHandler {
    partition_manager: Arc<PartitionManager>,
}

impl AlterConfigsHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 AlterConfigs 请求
    pub fn handle(
        &self,
        request: AlterConfigsRequest,
        _version: i16,
    ) -> Result<AlterConfigsResponse> {
        let mut responses = Vec::with_capacity(request.resources.len());

        for resource in &request.resources {
            match resource.resource_type {
                2 => {
                    // Topic
                    let result = self.alter_topic_config(
                        &resource.resource_name,
                        &resource.configs,
                        request.validate_only,
                    );
                    responses.push(AlterConfigsResponseResource {
                        error_code: result.0,
                        error_message: result.1,
                        resource_type: 2,
                        resource_name: resource.resource_name.clone(),
                    });
                }
                4 => {
                    // Broker
                    // Phase 1: Broker 配置只读
                    responses.push(AlterConfigsResponseResource {
                        error_code: KafkaErrorCode::InvalidConfig,
                        error_message: Some("Broker config is read-only in Phase 1".to_string()),
                        resource_type: 4,
                        resource_name: resource.resource_name.clone(),
                    });
                }
                _ => {
                    responses.push(AlterConfigsResponseResource {
                        error_code: KafkaErrorCode::InvalidRequest,
                        error_message: Some(format!(
                            "Unsupported resource type: {}",
                            resource.resource_type
                        )),
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                    });
                }
            }
        }

        Ok(AlterConfigsResponse {
            throttle_time_ms: 0,
            responses,
        })
    }

    /// 修改 Topic 配置
    fn alter_topic_config(
        &self,
        topic_name: &str,
        configs: &[AlterConfigsRequestConfig],
        validate_only: bool,
    ) -> (KafkaErrorCode, Option<String>) {
        // 检查 topic 是否存在
        if self
            .partition_manager
            .get_topic_metadata(topic_name)
            .is_none()
        {
            return (
                KafkaErrorCode::UnknownTopicOrPartition,
                Some(format!("Topic '{}' not found", topic_name)),
            );
        }

        // 验证全部配置项
        for config in configs {
            if !MUTABLE_TOPIC_CONFIGS.contains(&config.name.as_str()) {
                return (
                    KafkaErrorCode::InvalidConfig,
                    Some(format!(
                        "Config '{}' is not mutable or unknown",
                        config.name
                    )),
                );
            }
        }

        if validate_only {
            debug!(
                topic = topic_name,
                "Validate-only AlterConfigs, no changes applied"
            );
            return (KafkaErrorCode::None, None);
        }

        // 应用配置
        let mut config_map = HashMap::new();
        for config in configs {
            if let Some(value) = &config.value {
                config_map.insert(config.name.clone(), value.clone());
            } else {
                // null value = 删除覆盖, 恢复默认
                self.partition_manager
                    .remove_topic_config(topic_name, &config.name);
            }
        }

        if !config_map.is_empty() {
            self.partition_manager
                .set_topic_configs(topic_name, config_map);
        }

        debug!(topic = topic_name, "AlterConfigs applied");
        (KafkaErrorCode::None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> AlterConfigsHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        pm.get_or_create_topic("test-topic", 1);
        AlterConfigsHandler::new(pm)
    }

    #[test]
    fn test_alter_topic_config_success() {
        let handler = make_handler();
        let request = AlterConfigsRequest {
            resources: vec![AlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                configs: vec![AlterConfigsRequestConfig {
                    name: "retention.ms".to_string(),
                    value: Some("3600000".to_string()),
                }],
            }],
            validate_only: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.responses[0].error_code, KafkaErrorCode::None);

        // 验证配置已生效
        let val = handler
            .partition_manager
            .get_topic_config("test-topic", "retention.ms");
        assert_eq!(val, Some("3600000".to_string()));
    }

    #[test]
    fn test_alter_topic_config_validate_only() {
        let handler = make_handler();
        let request = AlterConfigsRequest {
            resources: vec![AlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                configs: vec![AlterConfigsRequestConfig {
                    name: "retention.ms".to_string(),
                    value: Some("3600000".to_string()),
                }],
            }],
            validate_only: true,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.responses[0].error_code, KafkaErrorCode::None);

        // validate_only 不应修改配置
        let val = handler
            .partition_manager
            .get_topic_config("test-topic", "retention.ms");
        assert_eq!(val, None);
    }

    #[test]
    fn test_alter_topic_config_unknown_topic() {
        let handler = make_handler();
        let request = AlterConfigsRequest {
            resources: vec![AlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "nonexistent".to_string(),
                configs: vec![],
            }],
            validate_only: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(
            response.responses[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
    }

    #[test]
    fn test_alter_topic_config_immutable() {
        let handler = make_handler();
        let request = AlterConfigsRequest {
            resources: vec![AlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                configs: vec![AlterConfigsRequestConfig {
                    name: "log.dirs".to_string(), // 不可修改
                    value: Some("/tmp".to_string()),
                }],
            }],
            validate_only: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(
            response.responses[0].error_code,
            KafkaErrorCode::InvalidConfig
        );
    }

    #[test]
    fn test_alter_broker_config_readonly() {
        let handler = make_handler();
        let request = AlterConfigsRequest {
            resources: vec![AlterConfigsRequestResource {
                resource_type: 4,
                resource_name: "1".to_string(),
                configs: vec![],
            }],
            validate_only: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(
            response.responses[0].error_code,
            KafkaErrorCode::InvalidConfig
        );
    }

    #[test]
    fn test_alter_topic_config_null_value_removes() {
        let handler = make_handler();
        // 先设置
        handler
            .partition_manager
            .set_topic_config("test-topic", "retention.ms", "3600000");

        // null value 应删除覆盖
        let request = AlterConfigsRequest {
            resources: vec![AlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                configs: vec![AlterConfigsRequestConfig {
                    name: "retention.ms".to_string(),
                    value: None,
                }],
            }],
            validate_only: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.responses[0].error_code, KafkaErrorCode::None);

        let val = handler
            .partition_manager
            .get_topic_config("test-topic", "retention.ms");
        assert_eq!(val, None);
    }
}
