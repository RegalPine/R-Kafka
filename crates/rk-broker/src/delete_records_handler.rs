//! DeleteRecords Handler
//!
//! 处理 DeleteRecords 请求 (API Key = 21)。
//! 将 partition 的 log_start_offset 前移到指定偏移量。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::delete_records::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::{PartitionKey, PartitionManager};
use rk_core::types::{PartitionId, TopicName};

/// DeleteRecords 请求处理器
pub struct DeleteRecordsHandler {
    partition_manager: Arc<PartitionManager>,
}

impl DeleteRecordsHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 DeleteRecords 请求
    pub fn handle(
        &self,
        request: DeleteRecordsRequest,
        _version: i16,
    ) -> Result<DeleteRecordsResponse> {
        debug!(
            topics = request.topics.len(),
            timeout_ms = request.timeout_ms,
            "DeleteRecords request"
        );

        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());

            for part_req in &topic_req.partitions {
                let key = PartitionKey::new(
                    TopicName(topic_req.name.clone()),
                    PartitionId(part_req.index),
                );

                match self.partition_manager.delete_records(&key, part_req.offset) {
                    Some(low_watermark) => {
                        response_partitions.push(DeleteRecordsResponsePartition {
                            index: part_req.index,
                            low_watermark,
                            error_code: KafkaErrorCode::None,
                        });
                    }
                    None => {
                        response_partitions.push(DeleteRecordsResponsePartition {
                            index: part_req.index,
                            low_watermark: -1,
                            error_code: KafkaErrorCode::UnknownTopicOrPartition,
                        });
                    }
                }
            }

            response_topics.push(DeleteRecordsResponseTopic {
                name: topic_req.name.clone(),
                partitions: response_partitions,
            });
        }

        Ok(DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_storage::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_handler() -> (DeleteRecordsHandler, Arc<PartitionManager>) {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1));
        pm.get_or_create_topic("test", 2);
        (DeleteRecordsHandler::new(pm.clone()), pm)
    }

    #[test]
    fn test_delete_records_success() {
        let (handler, pm) = make_handler();
        // 写入数据
        let batch = build_batch_bytes(0, 1, 0, 1000, 2000, -1, -1, -1, &[0u8; 50], 5);
        pm.append_batch("test", 0, &batch).unwrap();

        let req = DeleteRecordsRequest {
            topics: vec![DeleteRecordsRequestTopic {
                name: "test".to_string(),
                partitions: vec![DeleteRecordsRequestPartition {
                    index: 0,
                    offset: 3,
                }],
            }],
            timeout_ms: 30000,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics[0].partitions[0].error_code, KafkaErrorCode::None);
        assert_eq!(resp.topics[0].partitions[0].low_watermark, 3);
    }

    #[test]
    fn test_delete_records_unknown_topic() {
        let (handler, _) = make_handler();
        let req = DeleteRecordsRequest {
            topics: vec![DeleteRecordsRequestTopic {
                name: "nonexistent".to_string(),
                partitions: vec![DeleteRecordsRequestPartition {
                    index: 0,
                    offset: 5,
                }],
            }],
            timeout_ms: 30000,
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics[0].partitions[0].error_code, KafkaErrorCode::UnknownTopicOrPartition);
    }
}
