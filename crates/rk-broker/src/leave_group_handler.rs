//! LeaveGroup Handler
//!
//! 处理 LeaveGroup 请求 (API Key = 13)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::leave_group::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::group_manager::GroupManager;

/// LeaveGroup 请求处理器
pub struct LeaveGroupHandler {
    group_manager: Arc<GroupManager>,
}

impl LeaveGroupHandler {
    pub fn new(group_manager: Arc<GroupManager>) -> Self {
        Self { group_manager }
    }

    /// 处理 LeaveGroup 请求
    pub fn handle(
        &self,
        request: LeaveGroupRequest,
        _version: i16,
    ) -> Result<LeaveGroupResponse> {
        debug!(
            group_id = %request.group_id,
            member_id = %request.member_id,
            has_members = request.members.is_some(),
            "LeaveGroup request"
        );

        // v3+: 批量成员
        if let Some(members) = &request.members {
            let mut response_members = Vec::with_capacity(members.len());
            for m in members {
                let result = self.group_manager.leave_group(&request.group_id, &m.member_id);
                response_members.push(LeaveGroupResponseMember {
                    member_id: m.member_id.clone(),
                    group_instance_id: m.group_instance_id.clone(),
                    error_code: if result.is_ok() {
                        KafkaErrorCode::None
                    } else {
                        KafkaErrorCode::UnknownMemberId
                    },
                });
            }
            return Ok(LeaveGroupResponse {
                throttle_time_ms: 0,
                error_code: KafkaErrorCode::None,
                members: response_members,
            });
        }

        // v0-v2: 单个成员
        let result = self.group_manager.leave_group(&request.group_id, &request.member_id);
        let error_code = if result.is_ok() {
            KafkaErrorCode::None
        } else {
            KafkaErrorCode::UnknownMemberId
        };

        Ok(LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code,
            members: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leave_group_handler_v0() {
        let gm = Arc::new(GroupManager::new());

        let (_gen, mid, _leader, _proto, _members) = gm.join_group(
            "test-group", "", None, "consumer", vec![],
        ).unwrap();

        let handler = LeaveGroupHandler::new(gm);

        let request = LeaveGroupRequest {
            group_id: "test-group".to_string(),
            member_id: mid,
            members: None,
            reason: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_leave_group_handler_v3_batch() {
        let gm = Arc::new(GroupManager::new());

        let (_gen, mid1, _leader, _proto, _members) = gm.join_group(
            "test-group", "", None, "consumer", vec![],
        ).unwrap();
        let (_gen, mid2, _leader, _proto, _members) = gm.join_group(
            "test-group", "", None, "consumer", vec![],
        ).unwrap();

        let handler = LeaveGroupHandler::new(gm);

        let request = LeaveGroupRequest {
            group_id: "test-group".to_string(),
            member_id: String::new(),
            members: Some(vec![
                LeaveGroupRequestMember {
                    member_id: mid1,
                    group_instance_id: None,
                    reason: None,
                },
                LeaveGroupRequestMember {
                    member_id: mid2,
                    group_instance_id: None,
                    reason: None,
                },
            ]),
            reason: None,
        };

        let response = handler.handle(request, 3).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.members.len(), 2);
        assert_eq!(response.members[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.members[1].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_leave_group_handler_unknown_member() {
        let gm = Arc::new(GroupManager::new());

        gm.join_group("test-group", "", None, "consumer", vec![]).unwrap();

        let handler = LeaveGroupHandler::new(gm);

        let request = LeaveGroupRequest {
            group_id: "test-group".to_string(),
            member_id: "unknown".to_string(),
            members: None,
            reason: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::UnknownMemberId);
    }
}
