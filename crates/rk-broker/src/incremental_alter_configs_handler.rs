//! IncrementalAlterConfigs Handler
//!
//! 处理 IncrementalAlterConfigs 请求 (API Key = 44)。
//! Phase 1: 支持 Topic 配置增量修改 (运行时, 不持久化)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::incremental_alter_configs::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// IncrementalAlterConfigs 请求处理器
pub struct IncrementalAlterConfigsHandler {
    partition_manager: Arc<PartitionManager>,
}

impl IncrementalAlterConfigsHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 IncrementalAlterConfigs 请求
    pub fn handle(
        &self,
        request: IncrementalAlterConfigsRequest,
        _version: i16,
    ) -> Result<IncrementalAlterConfigsResponse> {
        debug!(
            resources = request.resources.len(),
            validate_only = request.validate_only,
            "IncrementalAlterConfigs request"
        );

        let mut responses = Vec::new();

        for resource in &request.resources {
            let (error_code, error_message) = if request.validate_only {
                // validate_only: 仅验证，不实际修改
                (KafkaErrorCode::None, None)
            } else {
                self.apply_resource_config(resource)
            };

            responses.push(IncrementalAlterConfigsResponseResource {
                error_code,
                error_message,
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
            });
        }

        Ok(IncrementalAlterConfigsResponse {
            throttle_time_ms: 0,
            responses,
        })
    }

    /// 应用单个资源配置的增量修改
    fn apply_resource_config(
        &self,
        resource: &IncrementalAlterConfigsRequestResource,
    ) -> (KafkaErrorCode, Option<String>) {
        match resource.resource_type {
            // Topic = 2
            2 => {
                let topic_name = &resource.resource_name;

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

                // 应用每个配置变更
                for config in &resource.configs {
                    let op = match IncrementalAlterConfigsOp::from_i8(config.config_operation) {
                        Some(op) => op,
                        None => {
                            return (
                                KafkaErrorCode::InvalidConfig,
                                Some(format!(
                                    "Unknown config operation: {}",
                                    config.config_operation
                                )),
                            );
                        }
                    };

                    match op {
                        IncrementalAlterConfigsOp::Set => {
                            if let Some(ref value) = config.value {
                                self.partition_manager.set_topic_config(
                                    topic_name,
                                    &config.name,
                                    value,
                                );
                            }
                        }
                        IncrementalAlterConfigsOp::Delete => {
                            self.partition_manager
                                .remove_topic_config(topic_name, &config.name);
                        }
                        IncrementalAlterConfigsOp::Append => {
                            // 追加到现有值 (逗号分隔)
                            let current = self
                                .partition_manager
                                .get_topic_config(topic_name, &config.name)
                                .unwrap_or_default();
                            if let Some(ref value) = config.value {
                                let new_val = if current.is_empty() {
                                    value.clone()
                                } else {
                                    format!("{},{}", current, value)
                                };
                                self.partition_manager.set_topic_config(
                                    topic_name,
                                    &config.name,
                                    &new_val,
                                );
                            }
                        }
                        IncrementalAlterConfigsOp::Subtract => {
                            // 从现有值中移除 (逗号分隔)
                            if let Some(ref value) = config.value {
                                let current = self
                                    .partition_manager
                                    .get_topic_config(topic_name, &config.name)
                                    .unwrap_or_default();
                                let new_val: Vec<&str> = current
                                    .split(',')
                                    .filter(|s| s.trim() != value.trim())
                                    .collect();
                                let new_val = new_val.join(",");
                                if new_val.is_empty() {
                                    self.partition_manager
                                        .remove_topic_config(topic_name, &config.name);
                                } else {
                                    self.partition_manager.set_topic_config(
                                        topic_name,
                                        &config.name,
                                        &new_val,
                                    );
                                }
                            }
                        }
                    }
                }

                (KafkaErrorCode::None, None)
            }
            // Broker = 4: Phase 1 不支持 Broker 配置修改
            4 => (KafkaErrorCode::None, None),
            _ => (
                KafkaErrorCode::InvalidConfig,
                Some(format!(
                    "Unsupported resource type: {}",
                    resource.resource_type
                )),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> IncrementalAlterConfigsHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        pm.get_or_create_topic("test-topic", 3);
        IncrementalAlterConfigsHandler::new(pm)
    }

    #[test]
    fn test_incremental_alter_configs_set() {
        let handler = make_handler();

        let request = IncrementalAlterConfigsRequest {
            resources: vec![IncrementalAlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                configs: vec![IncrementalAlterConfigsRequestConfig {
                    name: "retention.ms".to_string(),
                    value: Some("86400000".to_string()),
                    config_operation: 0, // Set
                }],
            }],
            validate_only: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.responses[0].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_incremental_alter_configs_delete() {
        let handler = make_handler();

        // 先设置一个配置
        handler
            .partition_manager
            .set_topic_config("test-topic", "retention.ms", "1000");

        let request = IncrementalAlterConfigsRequest {
            resources: vec![IncrementalAlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                configs: vec![IncrementalAlterConfigsRequestConfig {
                    name: "retention.ms".to_string(),
                    value: None,
                    config_operation: 1, // Delete
                }],
            }],
            validate_only: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.responses[0].error_code, KafkaErrorCode::None);
        assert!(handler
            .partition_manager
            .get_topic_config("test-topic", "retention.ms")
            .is_none());
    }

    #[test]
    fn test_incremental_alter_configs_unknown_topic() {
        let handler = make_handler();

        let request = IncrementalAlterConfigsRequest {
            resources: vec![IncrementalAlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "nonexistent-topic".to_string(),
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
    fn test_incremental_alter_configs_validate_only() {
        let handler = make_handler();

        let request = IncrementalAlterConfigsRequest {
            resources: vec![IncrementalAlterConfigsRequestResource {
                resource_type: 2,
                resource_name: "nonexistent-topic".to_string(),
                configs: vec![IncrementalAlterConfigsRequestConfig {
                    name: "retention.ms".to_string(),
                    value: Some("1000".to_string()),
                    config_operation: 0,
                }],
            }],
            validate_only: true, // 仅验证，不实际修改
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.responses[0].error_code, KafkaErrorCode::None);
    }
}
