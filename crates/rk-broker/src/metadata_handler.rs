//! Metadata Handler (API Key = 3)
//!
//! 处理 MetadataRequest:
//! 1. 解码请求体
//! 2. 查询 PartitionManager 获取 topic/partition 信息
//! 3. 构建 MetadataResponse

use rk_core::error::Result;
use rk_protocol::apis::metadata::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// Metadata Handler: 处理 Metadata API 请求
pub struct MetadataHandler {
    partition_manager: std::sync::Arc<PartitionManager>,
    /// Broker 配置
    broker_id: i32,
    broker_host: String,
    broker_port: i32,
    broker_rack: Option<String>,
    cluster_id: Option<String>,
}

impl MetadataHandler {
    pub fn new(
        partition_manager: std::sync::Arc<PartitionManager>,
        broker_id: i32,
        broker_host: String,
        broker_port: i32,
        broker_rack: Option<String>,
        cluster_id: Option<String>,
    ) -> Self {
        Self {
            partition_manager,
            broker_id,
            broker_host,
            broker_port,
            broker_rack,
            cluster_id,
        }
    }

    /// 处理 Metadata 请求
    pub fn handle(&self, request: MetadataRequest, version: i16) -> Result<MetadataResponse> {
        // 构建 Broker 列表 (Phase 1: 只有自身)
        let brokers = vec![MetadataBroker {
            node_id: self.broker_id,
            host: self.broker_host.clone(),
            port: self.broker_port,
            rack: self.broker_rack.clone(),
        }];

        // 确定要返回哪些 topics
        let topic_metas: Vec<MetadataTopic> = if let Some(ref topic_names) = request.topics {
            // 请求指定 topics
            topic_names.iter().map(|name| {
                self.build_topic_metadata(name, version)
            }).collect()
        } else {
            // null = 请求全部 topics (v1+)
            let all_topics = self.partition_manager.list_topics();
            all_topics.iter().map(|name| {
                self.build_topic_metadata(&name.0, version)
            }).collect()
        };

        debug!(
            "Metadata response: {} brokers, {} topics",
            brokers.len(),
            topic_metas.len()
        );

        Ok(MetadataResponse {
            throttle_time_ms: 0,
            brokers: brokers.clone(),
            cluster_id: self.cluster_id.clone(),
            controller_id: self.broker_id, // Phase 1: 自身即 controller
            topics: topic_metas,
            node_endpoints: MetadataResponse::default_node_endpoints(&brokers),
        })
    }

    /// 构建单个 topic 的元数据
    fn build_topic_metadata(&self, topic_name: &str, _version: i16) -> MetadataTopic {
        // 尝试获取已有 topic
        let meta = self.partition_manager.get_topic_metadata(topic_name);

        match meta {
            Some(topic_meta) => {
                let partition_infos = self.partition_manager.get_partition_infos(topic_name);

                let partitions = partition_infos.iter().map(|info| {
                    MetadataPartition {
                        error_code: KafkaErrorCode::None,
                        partition_index: info.partition_id,
                        leader_id: info.leader,
                        leader_epoch: info.leader_epoch,
                        replica_nodes: info.replicas.clone(),
                        isr_nodes: info.isr.clone(),
                    }
                }).collect();

                MetadataTopic {
                    error_code: KafkaErrorCode::None,
                    name: topic_name.to_string(),
                    is_internal: topic_meta.is_internal,
                    partitions,
                }
            }
            None => {
                // Topic 不存在
                MetadataTopic {
                    error_code: KafkaErrorCode::UnknownTopicOrPartition,
                    name: topic_name.to_string(),
                    is_internal: false,
                    partitions: vec![],
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_storage::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(base_offset, 1, 0, 1000, 2000, -1, -1, -1, &records, record_count)
    }

    fn make_handler() -> MetadataHandler {
        let dir = tempdir().unwrap();
        let pm = std::sync::Arc::new(
            PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1)
        );
        pm.get_or_create_topic("test-topic", 3);
        pm.append_batch("test-topic", 0, &make_batch(0, 5)).unwrap();

        MetadataHandler::new(
            pm, 1, "localhost".to_string(), 9092, None, Some("r-kafka-cluster".to_string()),
        )
    }

    #[test]
    fn test_metadata_handler_all_topics() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: None, // 请求全部
            allow_auto_topic_creation: true,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.brokers[0].node_id, 1);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name, "test-topic");
        assert_eq!(response.topics[0].partitions.len(), 3);
    }

    #[test]
    fn test_metadata_handler_specific_topics() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: Some(vec!["test-topic".to_string()]),
            allow_auto_topic_creation: true,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.topics[0].partitions.len(), 3);
    }

    #[test]
    fn test_metadata_handler_unknown_topic() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: Some(vec!["nonexistent".to_string()]),
            allow_auto_topic_creation: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::UnknownTopicOrPartition);
    }

    #[test]
    fn test_metadata_handler_cluster_id() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
        };

        let response = handler.handle(request, 2).unwrap();
        assert_eq!(response.cluster_id, Some("r-kafka-cluster".to_string()));
        assert_eq!(response.controller_id, 1);
    }

    #[test]
    fn test_metadata_handler_partition_leader() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: Some(vec!["test-topic".to_string()]),
            allow_auto_topic_creation: true,
        };

        let response = handler.handle(request, 7).unwrap();
        for p in &response.topics[0].partitions {
            assert_eq!(p.leader_id, 1);
            assert_eq!(p.replica_nodes, vec![1]);
            assert_eq!(p.isr_nodes, vec![1]);
        }
    }

    #[test]
    fn test_metadata_v12_node_endpoints() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
        };

        // v12 response should include node_endpoints
        let response = handler.handle(request, 12).unwrap();
        assert_eq!(response.node_endpoints.len(), 1);
        assert_eq!(response.node_endpoints[0].node_id, 1);
        assert_eq!(response.node_endpoints[0].host, "localhost");
        assert_eq!(response.node_endpoints[0].port, 9092);
        assert_eq!(response.node_endpoints[0].listener_name, "PLAINTEXT");
    }

    #[test]
    fn test_metadata_v11_no_node_endpoints_encoded() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
        };

        // v11 (< v12): node_endpoints 字段存在但不会被编码到响应中
        // (编码由 version >= 12 条件控制)
        let response = handler.handle(request, 11).unwrap();
        // struct 中仍有数据 (统一构建), 但 v11 编码器不写入
        assert_eq!(response.node_endpoints.len(), 1);
        // 验证 v11 响应其他字段正常
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.topics.len(), 1);
    }

    #[test]
    fn test_metadata_v13_latest() {
        let handler = make_handler();

        let request = MetadataRequest {
            topics: Some(vec!["test-topic".to_string()]),
            allow_auto_topic_creation: true,
        };

        // v13 (latest) should work with all v12+ fields
        let response = handler.handle(request, 13).unwrap();
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.node_endpoints.len(), 1);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions.len(), 3);
        assert_eq!(response.cluster_id, Some("r-kafka-cluster".to_string()));
        assert_eq!(response.controller_id, 1);
    }

    #[test]
    fn test_default_node_endpoints() {
        let brokers = vec![
            MetadataBroker {
                node_id: 1,
                host: "broker1.example.com".to_string(),
                port: 9092,
                rack: Some("rack-a".to_string()),
            },
            MetadataBroker {
                node_id: 2,
                host: "broker2.example.com".to_string(),
                port: 9093,
                rack: Some("rack-b".to_string()),
            },
        ];

        let endpoints = MetadataResponse::default_node_endpoints(&brokers);
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].node_id, 1);
        assert_eq!(endpoints[0].host, "broker1.example.com");
        assert_eq!(endpoints[0].listener_name, "PLAINTEXT");
        assert_eq!(endpoints[1].node_id, 2);
        assert_eq!(endpoints[1].port, 9093);
    }
}
