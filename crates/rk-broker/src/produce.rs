//! Produce Handler (API Key = 0)
//!
//! 处理 ProduceRequest:
//! 1. 解码请求体
//! 2. 遍历每个 topic-partition 的 RecordBatch
//! 3. 写入 PartitionManager
//! 4. 构建 ProduceResponse

use rk_core::error::{RkError, Result};
use rk_protocol::apis::produce::*;
use rk_protocol::error_codes::KafkaErrorCode;
use rk_protocol::record::{HEADER_SIZE, decode_batch_header};
use rk_protocol::types::KafkaReader;
use tracing::{debug, warn};

use crate::partition::PartitionManager;

/// Produce Handler: 处理 Produce API 请求
pub struct ProduceHandler {
    partition_manager: std::sync::Arc<PartitionManager>,
}

impl ProduceHandler {
    pub fn new(partition_manager: std::sync::Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 获取 PartitionManager 引用
    pub fn partition_manager(&self) -> &std::sync::Arc<PartitionManager> {
        &self.partition_manager
    }

    /// 处理 Produce 请求
    pub fn handle(&self, request: ProduceRequest, _version: i16) -> Result<ProduceResponse> {
        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());

            for partition_req in &topic_req.partitions {
                let result = self.append_partition(
                    &topic_req.name,
                    partition_req.index,
                    &partition_req.record_set,
                );

                let response_partition = match result {
                    Ok((base_offset, log_append_time)) => ProduceResponsePartition {
                        index: partition_req.index,
                        error_code: KafkaErrorCode::None,
                        base_offset,
                        log_append_time_ms: log_append_time,
                        log_start_offset: 0,
                    },
                    Err(e) => {
                        warn!(
                            topic = %topic_req.name,
                            partition = partition_req.index,
                            error = %e,
                            "Failed to append batch"
                        );
                        ProduceResponsePartition {
                            index: partition_req.index,
                            error_code: error_from_rk_error(&e),
                            base_offset: -1,
                            log_append_time_ms: -1,
                            log_start_offset: 0,
                        }
                    }
                };

                response_partitions.push(response_partition);
            }

            response_topics.push(ProduceResponseTopic {
                name: topic_req.name.clone(),
                partitions: response_partitions,
            });
        }

        debug!("Produce handled: {} topics", response_topics.len());

        Ok(ProduceResponse {
            topics: response_topics,
            throttle_time_ms: 0,
        })
    }

    /// 追加单个 partition 的 RecordBatch
    ///
    /// 返回 (base_offset, log_append_time_ms)
    fn append_partition(
        &self,
        topic: &str,
        partition: i32,
        record_set: &[u8],
    ) -> Result<(i64, i64)> {
        if record_set.is_empty() {
            return Ok((-1, -1));
        }

        // 确保 topic 存在 (自动创建)
        self.partition_manager.get_or_create_topic(topic, 1);

        // 解析 RecordBatch 获取记录数
        let _record_count = if record_set.len() >= HEADER_SIZE {
            let mut reader = KafkaReader::new(record_set);
            match decode_batch_header(&mut reader) {
                Ok(hdr) => hdr.records_count,
                Err(_) => 1,
            }
        } else {
            return Err(RkError::Protocol("RecordBatch too small".to_string()));
        };

        // 写入 CommitLog
        let base_offset = self.partition_manager.append_batch(topic, partition, record_set)?;

        // 获取当前时间作为 log_append_time (Phase 1: 使用系统时间)
        let log_append_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Phase 1: 单副本，HW = LEO
        // 实际 HW 更新在 Phase 3 副本复制中实现

        Ok((base_offset.0, log_append_time))
    }
}

/// 将 RkError 映射为 Kafka 错误码
fn error_from_rk_error(err: &RkError) -> KafkaErrorCode {
    match err {
        RkError::Protocol(_) => KafkaErrorCode::CorruptMessage,
        RkError::Storage(_) => KafkaErrorCode::KafkaStorageError,
        RkError::LogDirNotFound(_) => KafkaErrorCode::UnknownTopicOrPartition,
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
        build_batch_bytes(base_offset, 1, 0, 1000, 2000, -1, -1, -1, &records, record_count)
    }

    fn make_handler() -> ProduceHandler {
        let dir = tempdir().unwrap();
        let pm = std::sync::Arc::new(
            PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1)
        );
        ProduceHandler::new(pm)
    }

    #[test]
    fn test_produce_handler_basic() {
        let handler = make_handler();

        let batch = make_batch(0, 5);
        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30000,
            topics: vec![ProduceRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ProduceRequestPartition {
                    index: 0,
                    record_set: batch,
                }],
            }],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions.len(), 1);
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.topics[0].partitions[0].base_offset, 0);
    }

    #[test]
    fn test_produce_handler_multiple_partitions() {
        let handler = make_handler();

        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30000,
            topics: vec![ProduceRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![
                    ProduceRequestPartition {
                        index: 0,
                        record_set: make_batch(0, 3),
                    },
                    ProduceRequestPartition {
                        index: 1,
                        record_set: make_batch(0, 5),
                    },
                ],
            }],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].partitions.len(), 2);
        assert_eq!(response.topics[0].partitions[0].base_offset, 0);
        assert_eq!(response.topics[0].partitions[1].base_offset, 0);
    }

    #[test]
    fn test_produce_handler_empty_record_set() {
        let handler = make_handler();

        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30000,
            topics: vec![ProduceRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ProduceRequestPartition {
                    index: 0,
                    record_set: vec![],
                }],
            }],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.topics[0].partitions[0].base_offset, -1);
    }

    #[test]
    fn test_produce_handler_sequential_offsets() {
        let handler = make_handler();

        // 第一次写入
        let request1 = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30000,
            topics: vec![ProduceRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ProduceRequestPartition {
                    index: 0,
                    record_set: make_batch(0, 5),
                }],
            }],
        };
        let resp1 = handler.handle(request1, 0).unwrap();
        assert_eq!(resp1.topics[0].partitions[0].base_offset, 0);

        // 第二次写入
        let request2 = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30000,
            topics: vec![ProduceRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ProduceRequestPartition {
                    index: 0,
                    record_set: make_batch(5, 3),
                }],
            }],
        };
        let resp2 = handler.handle(request2, 0).unwrap();
        assert_eq!(resp2.topics[0].partitions[0].base_offset, 5);
    }
}
