//! SyncGroup Handler
//!
//! 处理 SyncGroup 请求 (API Key = 14)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::sync_group::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::group_manager::GroupManager;

/// SyncGroup 请求处理器
pub struct SyncGroupHandler {
    group_manager: Arc<GroupManager>,
}

impl SyncGroupHandler {
    pub fn new(group_manager: Arc<GroupManager>) -> Self {
        Self { group_manager }
    }

    /// 处理 SyncGroup 请求
    pub fn handle(
        &self,
        request: SyncGroupRequest,
        _version: i16,
    ) -> Result<SyncGroupResponse> {
        debug!(
            group_id = %request.group_id,
            member_id = %request.member_id,
            generation_id = request.generation_id,
            assignments = request.assignments.len(),
            "SyncGroup request"
        );

        let assignments: Vec<(String, Vec<u8>)> = request
            .assignments
            .iter()
            .map(|a| (a.member_id.clone(), a.assignment.clone()))
            .collect();

        match self.group_manager.sync_group(
            &request.group_id,
            &request.member_id,
            request.generation_id,
            assignments,
        ) {
            Ok(assignment) => Ok(SyncGroupResponse {
                throttle_time_ms: 0,
                error_code: KafkaErrorCode::None,
                protocol_type: request.protocol_type,
                protocol_name: request.protocol_name,
                assignment,
            }),
            Err(e) => {
                tracing::error!(error = %e, "SyncGroup failed");
                Ok(SyncGroupResponse {
                    throttle_time_ms: 0,
                    error_code: KafkaErrorCode::RebalanceInProgress,
                    protocol_type: None,
                    protocol_name: None,
                    assignment: vec![],
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_group_handler() {
        let gm = Arc::new(GroupManager::new());

        // 先 join
        let (gen, mid, _leader, _proto, _members) = gm.join_group(
            "test-group", "", None, "consumer", vec![],
        ).unwrap();

        let handler = SyncGroupHandler::new(gm);

        let request = SyncGroupRequest {
            group_id: "test-group".to_string(),
            generation_id: gen,
            member_id: mid.clone(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![SyncGroupRequestAssignment {
                member_id: mid.clone(),
                assignment: vec![10, 20, 30],
            }],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.assignment, vec![10, 20, 30]);
    }

    #[test]
    fn test_sync_group_handler_wrong_generation() {
        let gm = Arc::new(GroupManager::new());

        let (_gen, mid, _leader, _proto, _members) = gm.join_group(
            "test-group", "", None, "consumer", vec![],
        ).unwrap();

        let handler = SyncGroupHandler::new(gm);

        let request = SyncGroupRequest {
            group_id: "test-group".to_string(),
            generation_id: 999,
            member_id: mid,
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![],
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::RebalanceInProgress);
    }
}
