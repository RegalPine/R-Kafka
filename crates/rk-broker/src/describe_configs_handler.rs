//! DescribeConfigs Handler
//!
//! 处理 DescribeConfigs 请求 (API Key = 32)。
//! Phase 1: 返回 Topic/Broker 默认配置 + 运行时覆盖。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::describe_configs::*;
use rk_protocol::error_codes::KafkaErrorCode;

use crate::partition::PartitionManager;

/// Topic 默认配置表
const TOPIC_DEFAULT_CONFIGS: &[(&str, &str, bool, bool)] = &[
    ("cleanup.policy", "delete", true, false),
    ("compression.type", "producer", true, false),
    ("delete.retention.ms", "86400000", true, false),
    ("file.delete.delay.ms", "60000", true, false),
    ("flush.messages", "9223372036854775807", true, false),
    ("flush.ms", "9223372036854775807", true, false),
    ("index.interval.bytes", "4096", true, false),
    ("max.compaction.lag.ms", "9223372036854775807", true, false),
    ("max.message.bytes", "1048588", true, false),
    ("message.downconversion.enable", "true", true, false),
    ("message.format.version", "3.0-IV1", true, false),
    (
        "message.timestamp.difference.max.ms",
        "9223372036854775807",
        true,
        false,
    ),
    ("message.timestamp.type", "CreateTime", true, false),
    ("min.cleanable.dirty.ratio", "0.5", true, false),
    ("min.compaction.lag.ms", "0", true, false),
    ("min.insync.replicas", "1", true, false),
    ("preallocate", "false", true, false),
    ("retention.bytes", "-1", false, false),
    ("retention.ms", "604800000", false, false),
    ("segment.bytes", "1073741824", false, false),
    ("segment.index.bytes", "10485760", true, false),
    ("segment.jitter.ms", "0", true, false),
    ("segment.ms", "1800000", true, false),
    ("unclean.leader.election.enable", "false", false, false),
];

/// Broker 默认配置表
const BROKER_DEFAULT_CONFIGS: &[(&str, &str, bool, bool)] = &[
    ("log.dirs", "/data/r-kafka", true, false),
    ("log.retention.bytes", "-1", false, false),
    ("log.retention.hours", "168", false, false),
    ("log.retention.ms", "604800000", false, false),
    ("log.segment.bytes", "1073741824", false, false),
    ("num.io.threads", "8", true, false),
    ("num.network.threads", "3", true, false),
    ("num.partitions", "1", false, false),
    ("num.replica.fetchers", "1", true, false),
    ("socket.receive.buffer.bytes", "102400", true, false),
    ("socket.request.max.bytes", "104857600", true, false),
    ("socket.send.buffer.bytes", "102400", true, false),
];

/// DescribeConfigs 请求处理器
pub struct DescribeConfigsHandler {
    partition_manager: Arc<PartitionManager>,
}

impl DescribeConfigsHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 DescribeConfigs 请求
    pub fn handle(
        &self,
        request: DescribeConfigsRequest,
        _version: i16,
    ) -> Result<DescribeConfigsResponse> {
        let mut response_resources = Vec::with_capacity(request.resources.len());

        for resource in &request.resources {
            match resource.resource_type {
                2 => {
                    // Topic
                    let configs = self.describe_topic_config(
                        &resource.resource_name,
                        resource.config_names.as_deref(),
                    );
                    response_resources.push(DescribeConfigsResponseResource {
                        error_code: KafkaErrorCode::None,
                        error_message: None,
                        resource_type: 2,
                        resource_name: resource.resource_name.clone(),
                        configs,
                    });
                }
                4 => {
                    // Broker
                    let configs = self.describe_broker_config(
                        &resource.resource_name,
                        resource.config_names.as_deref(),
                    );
                    response_resources.push(DescribeConfigsResponseResource {
                        error_code: KafkaErrorCode::None,
                        error_message: None,
                        resource_type: 4,
                        resource_name: resource.resource_name.clone(),
                        configs,
                    });
                }
                _ => {
                    response_resources.push(DescribeConfigsResponseResource {
                        error_code: KafkaErrorCode::InvalidRequest,
                        error_message: Some(format!(
                            "Unsupported resource type: {}",
                            resource.resource_type
                        )),
                        resource_type: resource.resource_type,
                        resource_name: resource.resource_name.clone(),
                        configs: vec![],
                    });
                }
            }
        }

        Ok(DescribeConfigsResponse {
            throttle_time_ms: 0,
            resources: response_resources,
        })
    }

    /// 描述 Topic 配置
    fn describe_topic_config(
        &self,
        topic_name: &str,
        filter_names: Option<&[String]>,
    ) -> Vec<DescribeConfigsResponseConfig> {
        let overrides = self.partition_manager.get_topic_all_configs(topic_name);
        let mut configs = Vec::new();

        for &(name, default_val, read_only, _is_sensitive) in TOPIC_DEFAULT_CONFIGS {
            // 如果指定了 config_names 过滤，仅返回匹配的
            if let Some(names) = filter_names {
                if !names.iter().any(|n| n == name) {
                    continue;
                }
            }

            // 运行时覆盖优先于默认值
            let (value, is_default) = match overrides.get(name) {
                Some(override_val) => (Some(override_val.clone()), false),
                None => (Some(default_val.to_string()), true),
            };

            // config_source: 1=DynamicTopicConfig, 5=Default
            let config_source = if is_default { 5i8 } else { 1i8 };

            configs.push(DescribeConfigsResponseConfig {
                name: name.to_string(),
                value,
                read_only,
                is_default,
                is_sensitive: false,
                config_source,
                synonyms: vec![],
            });
        }

        configs
    }

    /// 描述 Broker 配置
    fn describe_broker_config(
        &self,
        _broker_id: &str,
        filter_names: Option<&[String]>,
    ) -> Vec<DescribeConfigsResponseConfig> {
        let mut configs = Vec::new();

        for &(name, default_val, read_only, _is_sensitive) in BROKER_DEFAULT_CONFIGS {
            if let Some(names) = filter_names {
                if !names.iter().any(|n| n == name) {
                    continue;
                }
            }

            configs.push(DescribeConfigsResponseConfig {
                name: name.to_string(),
                value: Some(default_val.to_string()),
                read_only,
                is_default: true,
                is_sensitive: false,
                config_source: 5, // Default
                synonyms: vec![],
            });
        }

        configs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> DescribeConfigsHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        pm.get_or_create_topic("test-topic", 1);
        DescribeConfigsHandler::new(pm)
    }

    #[test]
    fn test_describe_topic_all_configs() {
        let handler = make_handler();
        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                config_names: None,
            }],
            include_synonyms: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.resources.len(), 1);
        assert_eq!(response.resources[0].error_code, KafkaErrorCode::None);
        assert!(!response.resources[0].configs.is_empty());
        // 应包含 retention.ms
        assert!(response.resources[0]
            .configs
            .iter()
            .any(|c| c.name == "retention.ms"));
    }

    #[test]
    fn test_describe_topic_specific_configs() {
        let handler = make_handler();
        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                config_names: Some(vec![
                    "retention.ms".to_string(),
                    "retention.bytes".to_string(),
                ]),
            }],
            include_synonyms: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.resources[0].configs.len(), 2);
    }

    #[test]
    fn test_describe_topic_with_override() {
        let handler = make_handler();
        // 设置覆盖
        handler
            .partition_manager
            .set_topic_config("test-topic", "retention.ms", "3600000");

        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsRequestResource {
                resource_type: 2,
                resource_name: "test-topic".to_string(),
                config_names: Some(vec!["retention.ms".to_string()]),
            }],
            include_synonyms: false,
        };
        let response = handler.handle(request, 0).unwrap();
        let config = &response.resources[0].configs[0];
        assert_eq!(config.value, Some("3600000".to_string()));
        assert!(!config.is_default);
        assert_eq!(config.config_source, 1); // DynamicTopicConfig
    }

    #[test]
    fn test_describe_broker_configs() {
        let handler = make_handler();
        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsRequestResource {
                resource_type: 4,
                resource_name: "1".to_string(),
                config_names: None,
            }],
            include_synonyms: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.resources.len(), 1);
        assert!(!response.resources[0].configs.is_empty());
    }

    #[test]
    fn test_describe_unknown_resource_type() {
        let handler = make_handler();
        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsRequestResource {
                resource_type: 99,
                resource_name: "x".to_string(),
                config_names: None,
            }],
            include_synonyms: false,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(
            response.resources[0].error_code,
            KafkaErrorCode::InvalidRequest
        );
    }
}
