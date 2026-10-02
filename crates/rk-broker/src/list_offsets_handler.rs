//! ListOffsets Handler (API Key = 2)
//!
//! 处理 ListOffsetsRequest:
//! 1. 解码请求体
//! 2. 对每个 partition:
//!    - timestamp = -1 → LATEST (返回 LEO)
//!    - timestamp = -2 → EARLIEST (返回 log_start_offset)
//!    - timestamp >= 0 → 按时间查找 offset
//! 3. 构建 ListOffsetsResponse

use rk_core::error::Result;
use rk_protocol::apis::list_offsets::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// ListOffsets Handler
pub struct ListOffsetsHandler {
    partition_manager: std::sync::Arc<PartitionManager>,
}

impl ListOffsetsHandler {
    pub fn new(partition_manager: std::sync::Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 ListOffsets 请求
    pub fn handle(&self, request: ListOffsetsRequest, _version: i16) -> Result<ListOffsetsResponse> {
        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());

            for partition_req in &topic_req.partitions {
                let result = self.resolve_offset(
                    &topic_req.name,
                    partition_req.index,
                    partition_req.timestamp,
                );

                let response_partition = match result {
                    Ok((timestamp, offset)) => ListOffsetsResponsePartition {
                        index: partition_req.index,
                        error_code: KafkaErrorCode::None,
                        timestamp,
                        offset,
                    },
                    Err(e) => {
                        debug!(
                            topic = %topic_req.name,
                            partition = partition_req.index,
                            error = %e,
                            "Failed to resolve offset"
                        );
                        ListOffsetsResponsePartition {
                            index: partition_req.index,
                            error_code: KafkaErrorCode::UnknownTopicOrPartition,
                            timestamp: -1,
                            offset: -1,
                        }
                    }
                };

                response_partitions.push(response_partition);
            }

            response_topics.push(ListOffsetsResponseTopic {
                name: topic_req.name.clone(),
                partitions: response_partitions,
            });
        }

        debug!("ListOffsets handled: {} topics", response_topics.len());

        Ok(ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        })
    }

    /// 解析指定 partition 的 offset
    ///
    /// 返回 (timestamp, offset)
    fn resolve_offset(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<(i64, i64)> {
        // 确保 topic/partition 存在
        self.partition_manager.get_or_create_topic(topic, 1);
        let _ = self.partition_manager.ensure_partition(topic, partition);

        match timestamp {
            -1 => {
                // LATEST: 返回 Log End Offset
                let leo = self.partition_manager.log_end_offset(topic, partition)?;
                Ok((-1, leo.0))
            }
            -2 => {
                // EARLIEST: 返回 log_start_offset (Phase 1: 始终为 0)
                Ok((-1, 0))
            }
            ts if ts >= 0 => {
                // 按时间查找
                match self.partition_manager.offset_for_timestamp(topic, partition, ts)? {
                    Some(offset) => Ok((ts, offset.0)),
                    None => {
                        // 未找到 → 返回 LATEST
                        let leo = self.partition_manager.log_end_offset(topic, partition)?;
                        Ok((-1, leo.0))
                    }
                }
            }
            _ => {
                // 无效 timestamp
                Err(rk_core::error::RkError::Protocol(
                    format!("Invalid timestamp: {}", timestamp),
                ))
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

    fn make_handler() -> ListOffsetsHandler {
        let dir = tempdir().unwrap();
        let pm = std::sync::Arc::new(
            PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1)
        );
        pm.get_or_create_topic("test-topic", 1);
        pm.append_batch("test-topic", 0, &make_batch(0, 5)).unwrap();
        pm.append_batch("test-topic", 0, &make_batch(5, 3)).unwrap();

        ListOffsetsHandler::new(pm)
    }

    #[test]
    fn test_list_offsets_latest() {
        let handler = make_handler();

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    index: 0,
                    timestamp: -1, // LATEST
                }],
            }],
        };

        let response = handler.handle(request, 1).unwrap();
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.topics[0].partitions[0].offset, 8); // LEO = 5 + 3 = 8
    }

    #[test]
    fn test_list_offsets_earliest() {
        let handler = make_handler();

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    index: 0,
                    timestamp: -2, // EARLIEST
                }],
            }],
        };

        let response = handler.handle(request, 1).unwrap();
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.topics[0].partitions[0].offset, 0);
    }

    #[test]
    fn test_list_offsets_by_timestamp() {
        let handler = make_handler();

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    index: 0,
                    timestamp: 1500, // 在 base_timestamp=1000 和 max_timestamp=2000 之间
                }],
            }],
        };

        let response = handler.handle(request, 1).unwrap();
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaErrorCode::None);
        // 应返回某个 offset (具体取决于时间索引实现)
        assert!(response.topics[0].partitions[0].offset >= 0);
    }

    #[test]
    fn test_list_offsets_unknown_topic() {
        let handler = make_handler();

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsRequestTopic {
                name: "nonexistent".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    index: 0,
                    timestamp: -1,
                }],
            }],
        };

        let response = handler.handle(request, 1).unwrap();
        // 自动创建 topic 后返回 LEO=0
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaErrorCode::None);
    }
}
