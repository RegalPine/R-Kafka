//! OffsetFetch Handler
//!
//! 处理 OffsetFetch 请求 (API Key = 9)。
//! Phase 1: 从内存 OffsetManager 读取。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::offset_fetch::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::offset_manager::OffsetManager;

/// OffsetFetch 请求处理器
pub struct OffsetFetchHandler {
    offset_manager: Arc<OffsetManager>,
}

impl OffsetFetchHandler {
    pub fn new(offset_manager: Arc<OffsetManager>) -> Self {
        Self { offset_manager }
    }

    /// 处理 OffsetFetch 请求
    pub fn handle(
        &self,
        request: OffsetFetchRequest,
        _version: i16,
    ) -> Result<OffsetFetchResponse> {
        // v8+: multi-group
        if let Some(groups) = &request.groups {
            let mut response_groups = Vec::with_capacity(groups.len());
            for group in groups {
                let topics = self.fetch_group_offsets(
                    &group.group_id,
                    group.topics.as_deref(),
                );
                response_groups.push(OffsetFetchResponseGroup {
                    group_id: group.group_id.clone(),
                    topics,
                    error_code: KafkaErrorCode::None,
                });
            }
            return Ok(OffsetFetchResponse {
                throttle_time_ms: 0,
                topics: vec![],
                error_code: KafkaErrorCode::None,
                groups: Some(response_groups),
            });
        }

        // v0-v7: single group
        let group_id = &request.group_id;

        debug!(
            group_id = %group_id,
            has_topics = request.topics.is_some(),
            "OffsetFetch request"
        );

        let topics = self.fetch_group_offsets(group_id, request.topics.as_deref());

        Ok(OffsetFetchResponse {
            throttle_time_ms: 0,
            topics,
            error_code: KafkaErrorCode::None,
            groups: None,
        })
    }

    /// 查询指定 group 的偏移量
    fn fetch_group_offsets(
        &self,
        group_id: &str,
        topics_filter: Option<&[OffsetFetchRequestTopic]>,
    ) -> Vec<OffsetFetchResponseTopic> {
        match topics_filter {
            Some(request_topics) => {
                // 查询指定 topic-partition
                let mut response_topics = Vec::with_capacity(request_topics.len());

                for req_topic in request_topics {
                    let mut response_partitions = Vec::with_capacity(req_topic.partition_indexes.len());

                    for &partition_idx in &req_topic.partition_indexes {
                        let committed = self.offset_manager.fetch_offset(
                            group_id,
                            &req_topic.name,
                            partition_idx,
                        );

                        match committed {
                            Some(offset) => {
                                response_partitions.push(OffsetFetchResponsePartition {
                                    index: partition_idx,
                                    committed_offset: offset.offset,
                                    leader_epoch: offset.leader_epoch,
                                    metadata: offset.metadata,
                                    error_code: KafkaErrorCode::None,
                                });
                            }
                            None => {
                                // 未找到: 返回 offset=-1, error_code=None
                                response_partitions.push(OffsetFetchResponsePartition {
                                    index: partition_idx,
                                    committed_offset: -1,
                                    leader_epoch: -1,
                                    metadata: None,
                                    error_code: KafkaErrorCode::None,
                                });
                            }
                        }
                    }

                    response_topics.push(OffsetFetchResponseTopic {
                        name: req_topic.name.clone(),
                        partitions: response_partitions,
                    });
                }

                response_topics
            }
            None => {
                // 查询该 group 的全部 topic 偏移量
                let all = self.offset_manager.fetch_all_offsets_for_group(group_id);
                let mut response_topics = Vec::new();

                for (topic_name, partition_map) in &all {
                    let mut response_partitions = Vec::new();
                    for (&partition_idx, offset) in partition_map {
                        response_partitions.push(OffsetFetchResponsePartition {
                            index: partition_idx,
                            committed_offset: offset.offset,
                            leader_epoch: offset.leader_epoch,
                            metadata: offset.metadata.clone(),
                            error_code: KafkaErrorCode::None,
                        });
                    }
                    response_partitions.sort_by_key(|p| p.index);

                    response_topics.push(OffsetFetchResponseTopic {
                        name: topic_name.clone(),
                        partitions: response_partitions,
                    });
                }

                response_topics.sort_by(|a, b| a.name.cmp(&b.name));
                response_topics
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset_manager::OffsetManager;

    fn make_handler_with_data() -> OffsetFetchHandler {
        let om = Arc::new(OffsetManager::new(None));
        om.commit_offset("group-1", "topic-a", 0, 42, -1, None).unwrap();
        om.commit_offset("group-1", "topic-a", 1, 100, -1, Some("meta".to_string())).unwrap();
        om.commit_offset("group-1", "topic-b", 0, 200, -1, None).unwrap();
        om.commit_offset("group-2", "topic-a", 0, 300, -1, None).unwrap();
        OffsetFetchHandler::new(om)
    }

    #[test]
    fn test_offset_fetch_specific_partitions() {
        let handler = make_handler_with_data();
        let request = OffsetFetchRequest {
            group_id: "group-1".to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "topic-a".to_string(),
                partition_indexes: vec![0, 1],
            }]),
            groups: None,
            require_stable: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions.len(), 2);
        assert_eq!(response.topics[0].partitions[0].committed_offset, 42);
        assert_eq!(response.topics[0].partitions[1].committed_offset, 100);
        assert_eq!(
            response.topics[0].partitions[1].metadata,
            Some("meta".to_string())
        );
    }

    #[test]
    fn test_offset_fetch_nonexistent_offset() {
        let handler = make_handler_with_data();
        let request = OffsetFetchRequest {
            group_id: "group-1".to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "topic-a".to_string(),
                partition_indexes: vec![5], // 不存在
            }]),
            groups: None,
            require_stable: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_offset_fetch_all_topics() {
        let handler = make_handler_with_data();
        let request = OffsetFetchRequest {
            group_id: "group-1".to_string(),
            topics: None, // 查询全部
            groups: None,
            require_stable: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 2); // topic-a, topic-b
        // 按名称排序
        assert_eq!(response.topics[0].name, "topic-a");
        assert_eq!(response.topics[1].name, "topic-b");
    }

    #[test]
    fn test_offset_fetch_empty_group() {
        let handler = make_handler_with_data();
        let request = OffsetFetchRequest {
            group_id: "nonexistent-group".to_string(),
            topics: None,
            groups: None,
            require_stable: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert!(response.topics.is_empty());
    }

    #[test]
    fn test_offset_fetch_multi_group_v8() {
        let handler = make_handler_with_data();
        let request = OffsetFetchRequest {
            group_id: String::new(),
            topics: None,
            groups: Some(vec![
                OffsetFetchRequestGroup {
                    group_id: "group-1".to_string(),
                    topics: Some(vec![OffsetFetchRequestTopic {
                        name: "topic-a".to_string(),
                        partition_indexes: vec![0],
                    }]),
                },
                OffsetFetchRequestGroup {
                    group_id: "group-2".to_string(),
                    topics: Some(vec![OffsetFetchRequestTopic {
                        name: "topic-a".to_string(),
                        partition_indexes: vec![0],
                    }]),
                },
            ]),
            require_stable: false,
        };

        let response = handler.handle(request, 8).unwrap();
        let groups = response.groups.unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].topics[0].partitions[0].committed_offset, 42);
        assert_eq!(groups[1].topics[0].partitions[0].committed_offset, 300);
    }
}
