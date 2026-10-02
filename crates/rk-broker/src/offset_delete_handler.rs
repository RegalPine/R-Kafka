//! OffsetDelete Handler
//!
//! 处理 OffsetDelete 请求 (API Key = 47)。
//! 删除消费者组的已提交偏移量。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::offset_delete::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::offset_manager::OffsetManager;

/// OffsetDelete 请求处理器
pub struct OffsetDeleteHandler {
    offset_manager: Arc<OffsetManager>,
}

impl OffsetDeleteHandler {
    pub fn new(offset_manager: Arc<OffsetManager>) -> Self {
        Self { offset_manager }
    }

    /// 处理 OffsetDelete 请求
    pub fn handle(
        &self,
        request: OffsetDeleteRequest,
        _version: i16,
    ) -> Result<OffsetDeleteResponse> {
        debug!(
            group_id = %request.group_id,
            topics = request.topics.len(),
            "OffsetDelete request"
        );

        let mut response_topics = Vec::with_capacity(request.topics.len());

        for topic_req in &request.topics {
            let mut response_partitions = Vec::with_capacity(topic_req.partitions.len());

            for part_req in &topic_req.partitions {
                // 删除该 partition 的已提交偏移量
                self.offset_manager.delete_offset(
                    &request.group_id,
                    &topic_req.name,
                    part_req.index,
                );

                response_partitions.push(OffsetDeleteResponsePartition {
                    index: part_req.index,
                    error_code: KafkaErrorCode::None,
                });
            }

            response_topics.push(OffsetDeleteResponseTopic {
                name: topic_req.name.clone(),
                partitions: response_partitions,
            });
        }

        Ok(OffsetDeleteResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            topics: response_topics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_delete() {
        let om = Arc::new(OffsetManager::new(None));
        // 先提交一个偏移量
        let _ = om.commit_offset("group1", "test", 0, 42, -1, None);
        assert_eq!(om.get_offset("group1", "test", 0), Some(42));

        let handler = OffsetDeleteHandler::new(om.clone());
        let req = OffsetDeleteRequest {
            group_id: "group1".to_string(),
            topics: vec![OffsetDeleteRequestTopic {
                name: "test".to_string(),
                partitions: vec![OffsetDeleteRequestPartition { index: 0 }],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
        assert_eq!(resp.topics[0].partitions[0].error_code, KafkaErrorCode::None);

        // 验证偏移量已删除
        assert_eq!(om.get_offset("group1", "test", 0), None);
    }

    #[test]
    fn test_offset_delete_nonexistent() {
        let om = Arc::new(OffsetManager::new(None));
        let handler = OffsetDeleteHandler::new(om);
        let req = OffsetDeleteRequest {
            group_id: "group1".to_string(),
            topics: vec![OffsetDeleteRequestTopic {
                name: "test".to_string(),
                partitions: vec![OffsetDeleteRequestPartition { index: 0 }],
            }],
        };
        let resp = handler.handle(req, 0).unwrap();
        assert_eq!(resp.error_code, KafkaErrorCode::None);
    }
}
