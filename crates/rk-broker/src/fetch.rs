//! Fetch Handler (API Key = 1)
//!
//! 处理 FetchRequest:
//! 1. 解码请求体
//! 2. 遍历每个 topic-partition 的 fetch 参数
//! 3. 从 PartitionManager 读取数据
//! 4. 构建 FetchResponse

use rk_core::error::Result;
use rk_core::types::Offset;
use rk_protocol::apis::fetch::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// Fetch Handler: 处理 Fetch API 请求
pub struct FetchHandler {
    partition_manager: std::sync::Arc<PartitionManager>,
}

impl FetchHandler {
    pub fn new(partition_manager: std::sync::Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 Fetch 请求
    pub fn handle(&self, request: FetchRequest, _version: i16) -> Result<FetchResponse> {
        let mut response_topics = Vec::with_capacity(request.topics.len());
        let mut total_bytes = 0i64;

        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());

            for partition_req in &topic_req.partitions {
                let result = self.fetch_partition(
                    &topic_req.name,
                    partition_req.index,
                    partition_req.fetch_offset,
                    partition_req.max_bytes,
                    request.max_bytes.saturating_sub(total_bytes as i32) as usize,
                );

                let response_partition = match result {
                    Ok((record_set, high_watermark, log_start_offset)) => {
                        total_bytes += record_set.len() as i64;
                        FetchResponsePartition {
                            index: partition_req.index,
                            error_code: KafkaErrorCode::None,
                            high_watermark,
                            last_stable_offset: high_watermark,
                            log_start_offset,
                            record_set,
                        }
                    }
                    Err(e) => {
                        debug!(
                            topic = %topic_req.name,
                            partition = partition_req.index,
                            error = %e,
                            "Failed to fetch partition"
                        );
                        FetchResponsePartition {
                            index: partition_req.index,
                            error_code: error_from_fetch_error(&e),
                            high_watermark: -1,
                            last_stable_offset: -1,
                            log_start_offset: -1,
                            record_set: vec![],
                        }
                    }
                };

                response_partitions.push(response_partition);

                // 检查全局 max_bytes 限制
                if total_bytes >= request.max_bytes as i64 {
                    break;
                }
            }

            response_topics.push(FetchResponseTopic {
                name: topic_req.name.clone(),
                partitions: response_partitions,
            });
        }

        debug!(
            "Fetch handled: {} topics, {} bytes",
            response_topics.len(),
            total_bytes
        );

        Ok(FetchResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            session_id: 0, // Phase 1: 不支持 fetch session
            topics: response_topics,
        })
    }

    /// 从单个 partition 获取数据
    ///
    /// 返回 (record_set_bytes, high_watermark, log_start_offset)
    fn fetch_partition(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        partition_max_bytes: i32,
        global_max_bytes: usize,
    ) -> Result<(Vec<u8>, i64, i64)> {
        // 确保 topic/partition 存在 (自动创建)
        self.partition_manager.get_or_create_topic(topic, 1);
        let _ = self.partition_manager.ensure_partition(topic, partition);

        // 获取 HW 和 LEO
        let hw = self
            .partition_manager
            .high_watermark(topic, partition)
            .unwrap_or(Offset(0));
        let leo = self
            .partition_manager
            .log_end_offset(topic, partition)
            .unwrap_or(Offset(0));

        // 如果 fetch_offset 超出范围
        if fetch_offset < 0 || fetch_offset > leo.0 {
            return Ok((vec![], hw.0, 0));
        }

        // 计算读取字节限制
        let max_bytes = std::cmp::min(partition_max_bytes as usize, global_max_bytes);

        // 从 PartitionManager 读取 batches
        let batches = self.partition_manager.read_batches(
            topic,
            partition,
            Offset(fetch_offset),
            max_bytes,
        )?;

        if batches.is_empty() {
            return Ok((vec![], hw.0, 0));
        }

        // 拼接所有 batch bytes (Kafka 协议中 record_set 是连续的 batch)
        let total_size: usize = batches.iter().map(|b| b.len()).sum();
        let mut record_set = Vec::with_capacity(total_size);
        for batch in batches {
            record_set.extend_from_slice(&batch);
        }

        Ok((record_set, hw.0, 0))
    }
}

fn error_from_fetch_error(err: &rk_core::error::RkError) -> KafkaErrorCode {
    match err {
        rk_core::error::RkError::LogDirNotFound(_) => KafkaErrorCode::UnknownTopicOrPartition,
        rk_core::error::RkError::Protocol(_) => KafkaErrorCode::CorruptMessage,
        _ => KafkaErrorCode::UnknownServerError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_storage::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(
            base_offset,
            1,
            0,
            1000,
            2000,
            -1,
            -1,
            -1,
            &records,
            record_count,
        )
    }

    fn make_handler_with_data() -> FetchHandler {
        let dir = tempdir().unwrap();
        let pm = std::sync::Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));

        // 写入测试数据
        pm.get_or_create_topic("test-topic", 1);
        pm.append_batch("test-topic", 0, &make_batch(0, 5)).unwrap();
        pm.append_batch("test-topic", 0, &make_batch(5, 3)).unwrap();

        FetchHandler::new(pm)
    }

    #[test]
    fn test_fetch_handler_basic() {
        let handler = make_handler_with_data();

        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_000_000,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![FetchRequestPartition {
                    index: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 0,
                    log_start_offset: -1,
                    max_bytes: 1_000_000,
                }],
            }],
            rack_id: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions.len(), 1);
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert!(!response.topics[0].partitions[0].record_set.is_empty());
    }

    #[test]
    fn test_fetch_handler_from_offset() {
        let handler = make_handler_with_data();

        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_000_000,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![FetchRequestPartition {
                    index: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 5, // 从第二个 batch 开始
                    log_start_offset: -1,
                    max_bytes: 1_000_000,
                }],
            }],
            rack_id: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert!(!response.topics[0].partitions[0].record_set.is_empty());
    }

    #[test]
    fn test_fetch_handler_beyond_end() {
        let handler = make_handler_with_data();

        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_000_000,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![FetchRequestPartition {
                    index: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100, // 超出 LEO
                    log_start_offset: -1,
                    max_bytes: 1_000_000,
                }],
            }],
            rack_id: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert!(response.topics[0].partitions[0].record_set.is_empty());
    }

    #[test]
    fn test_fetch_handler_unknown_partition() {
        let handler = make_handler_with_data();

        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_000_000,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchRequestTopic {
                name: "nonexistent-topic".to_string(),
                partitions: vec![FetchRequestPartition {
                    index: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 0,
                    log_start_offset: -1,
                    max_bytes: 1_000_000,
                }],
            }],
            rack_id: None,
        };

        let response = handler.handle(request, 0).unwrap();
        // 自动创建 topic，但无数据
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert!(response.topics[0].partitions[0].record_set.is_empty());
    }
}
