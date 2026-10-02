//! Heartbeat Handler
//!
//! 处理 Heartbeat 请求 (API Key = 12)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::heartbeat::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::group_manager::GroupManager;

/// Heartbeat 请求处理器
pub struct HeartbeatHandler {
    group_manager: Arc<GroupManager>,
}

impl HeartbeatHandler {
    pub fn new(group_manager: Arc<GroupManager>) -> Self {
        Self { group_manager }
    }

    /// 处理 Heartbeat 请求
    pub fn handle(
        &self,
        request: HeartbeatRequest,
        _version: i16,
    ) -> Result<HeartbeatResponse> {
        debug!(
            group_id = %request.group_id,
            member_id = %request.member_id,
            generation_id = request.generation_id,
            "Heartbeat request"
        );

        match self.group_manager.heartbeat(
            &request.group_id,
            &request.member_id,
            request.generation_id,
        ) {
            Ok(()) => Ok(HeartbeatResponse {
                throttle_time_ms: 0,
                error_code: KafkaErrorCode::None,
            }),
            Err(e) => {
                debug!(error = %e, "Heartbeat failed");
                Ok(HeartbeatResponse {
                    throttle_time_ms: 0,
                    error_code: KafkaErrorCode::RebalanceInProgress,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_handler() {
        let gm = Arc::new(GroupManager::new());

        let (gen, mid, _leader, _proto, _members) = gm.join_group(
            "test-group", "", None, "consumer", vec![],
        ).unwrap();

        let handler = HeartbeatHandler::new(gm);

        let request = HeartbeatRequest {
            group_id: "test-group".to_string(),
            generation_id: gen,
            member_id: mid,
            group_instance_id: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_heartbeat_handler_unknown_member() {
        let gm = Arc::new(GroupManager::new());

        gm.join_group("test-group", "", None, "consumer", vec![]).unwrap();

        let handler = HeartbeatHandler::new(gm);

        let request = HeartbeatRequest {
            group_id: "test-group".to_string(),
            generation_id: 1,
            member_id: "unknown-member".to_string(),
            group_instance_id: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::RebalanceInProgress);
    }
}
