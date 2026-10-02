//! DeleteTopics Handler
//!
//! 处理 DeleteTopics 请求 (API Key = 20)。
//! Phase 1: 从内存中移除 topic 元数据和 partition 映射。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::delete_topics::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::partition::PartitionManager;

/// DeleteTopics 请求处理器
pub struct DeleteTopicsHandler {
    partition_manager: Arc<PartitionManager>,
}

impl DeleteTopicsHandler {
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 处理 DeleteTopics 请求
    pub fn handle(
        &self,
        request: DeleteTopicsRequest,
        _version: i16,
    ) -> Result<DeleteTopicsResponse> {
        let mut response_topics = Vec::with_capacity(request.topic_names.len());

        for topic_name in &request.topic_names {
            let result = self.delete_topic(topic_name);
            response_topics.push(result);
        }

        debug!(
            topic_count = request.topic_names.len(),
            "DeleteTopics handled"
        );

        Ok(DeleteTopicsResponse::success(response_topics))
    }

    fn delete_topic(&self, topic_name: &str) -> DeleteTopicsResponseTopic {
        // 检查 topic 是否存在
        if self
            .partition_manager
            .get_topic_metadata(topic_name)
            .is_none()
        {
            return DeleteTopicsResponseTopic {
                name: topic_name.to_string(),
                error_code: KafkaErrorCode::UnknownTopicOrPartition,
                error_message: Some(format!("Topic '{}' not found", topic_name)),
            };
        }

        // 删除 topic
        self.partition_manager.delete_topic(topic_name);

        debug!(topic = %topic_name, "Topic deleted");

        DeleteTopicsResponseTopic {
            name: topic_name.to_string(),
            error_code: KafkaErrorCode::None,
            error_message: None,
        }
    }

    /// 获取 PartitionManager 引用
    pub fn partition_manager(&self) -> &Arc<PartitionManager> {
        &self.partition_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_handler() -> DeleteTopicsHandler {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        DeleteTopicsHandler::new(pm)
    }

    #[test]
    fn test_delete_topic_success() {
        let handler = make_handler();
        handler
            .partition_manager
            .get_or_create_topic("to-delete", 2);

        let request = DeleteTopicsRequest {
            topic_names: vec!["to-delete".to_string()],
            timeout_ms: 30000,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);

        // 验证 topic 已删除
        let meta = handler.partition_manager.get_topic_metadata("to-delete");
        assert!(meta.is_none());
    }

    #[test]
    fn test_delete_topic_not_found() {
        let handler = make_handler();

        let request = DeleteTopicsRequest {
            topic_names: vec!["nonexistent".to_string()],
            timeout_ms: 30000,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(
            response.topics[0].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
    }

    #[test]
    fn test_delete_multiple_topics() {
        let handler = make_handler();
        handler.partition_manager.get_or_create_topic("topic-a", 1);
        handler.partition_manager.get_or_create_topic("topic-b", 1);

        let request = DeleteTopicsRequest {
            topic_names: vec![
                "topic-a".to_string(),
                "topic-b".to_string(),
                "topic-c".to_string(),
            ],
            timeout_ms: 30000,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 3);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.topics[1].error_code, KafkaErrorCode::None);
        assert_eq!(
            response.topics[2].error_code,
            KafkaErrorCode::UnknownTopicOrPartition
        );
    }

    #[test]
    fn test_delete_topic_then_recreate() {
        let handler = make_handler();
        handler.partition_manager.get_or_create_topic("recycle", 3);

        // 删除
        let request = DeleteTopicsRequest {
            topic_names: vec!["recycle".to_string()],
            timeout_ms: 30000,
        };
        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);

        // 重新创建 (不同 partition 数)
        handler.partition_manager.get_or_create_topic("recycle", 5);
        let meta = handler.partition_manager.get_topic_metadata("recycle");
        assert!(meta.is_some());
        assert_eq!(meta.unwrap().partition_count, 5);
    }
}
