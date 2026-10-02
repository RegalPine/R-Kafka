//! DescribeProducers Handler
//!
//! 处理 DescribeProducers 请求 (API Key = 61)。
//! Phase 1: 从 ProducerStateManager 查询活跃生产者。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::describe_producers::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;
use crate::producer_state_manager::ProducerStateManager;

/// DescribeProducers 请求处理器
pub struct DescribeProducersHandler {
    partition_manager: Arc<PartitionManager>,
    producer_state_manager: Arc<ProducerStateManager>,
}

impl DescribeProducersHandler {
    pub fn new(
        partition_manager: Arc<PartitionManager>,
        producer_state_manager: Arc<ProducerStateManager>,
    ) -> Self {
        Self { partition_manager, producer_state_manager }
    }

    /// 处理 DescribeProducers 请求
    pub fn handle(
        &self,
        request: DescribeProducersRequest,
        _version: i16,
    ) -> Result<DescribeProducersResponse> {
        debug!(topics = request.topics.len(), "DescribeProducers request");

        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());

            for &partition_idx in &topic_req.partitions {
                let exists = self.partition_manager
                    .get_partition_count(&topic_req.name)
                    .map(|count| partition_idx >= 0 && partition_idx < count)
                    .unwrap_or(false);

                if !exists {
                    response_partitions.push(DescribeProducersResponsePartition {
                        index: partition_idx,
                        error_code: KafkaErrorCode::UnknownTopicOrPartition,
                        error_message: None,
                        active_producers: vec![],
                    });
                    continue;
                }

                // Phase 1: 从 ProducerStateManager 查询该 partition 的活跃生产者
                let producers = self.producer_state_manager
                    .get_producers_for_partition(&topic_req.name, partition_idx);

                let active_producers = producers.into_iter().map(|state| {
                    DescribeProducersResponseProducer {
                        producer_id: state.producer_id,
                        producer_epoch: state.producer_epoch as i32,
                        last_sequence: state.last_sequence.get(&(topic_req.name.clone(), partition_idx)).copied().unwrap_or(-1),
                        last_timestamp: 0, // Phase 1: 不跟踪时间戳
                        current_txn_start_offset: state.txn_start_offset.unwrap_or(-1),
                    }
                }).collect();

                response_partitions.push(DescribeProducersResponsePartition {
                    index: partition_idx,
                    error_code: KafkaErrorCode::None,
                    error_message: None,
                    active_producers,
                });
            }

            response_topics.push(DescribeProducersResponseTopic {
                name: topic_req.name.clone(),
                partitions: response_partitions,
            });
        }

        Ok(DescribeProducersResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> DescribeProducersHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1));
        pm.get_or_create_topic("test", 3);
        let psm = Arc::new(ProducerStateManager::new());
        DescribeProducersHandler::new(pm, psm)
    }

    #[test]
    fn test_describe_producers_empty() {
        let handler = make_handler();
        let req = DescribeProducersRequest {
            topics: vec![DescribeProducersRequestTopic {
                name: "test".to_string(),
                partitions: vec![0, 1],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics[0].partitions.len(), 2);
        assert_eq!(resp.topics[0].partitions[0].error_code, KafkaErrorCode::None);
        assert!(resp.topics[0].partitions[0].active_producers.is_empty());
    }

    #[test]
    fn test_describe_producers_unknown_partition() {
        let handler = make_handler();
        let req = DescribeProducersRequest {
            topics: vec![DescribeProducersRequestTopic {
                name: "test".to_string(),
                partitions: vec![99],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.topics[0].partitions[0].error_code, KafkaErrorCode::UnknownTopicOrPartition);
    }
}
