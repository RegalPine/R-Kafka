//! JoinGroup Handler
//!
//! 处理 JoinGroup 请求 (API Key = 11)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::join_group::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::group_manager::GroupManager;

/// JoinGroup 请求处理器
pub struct JoinGroupHandler {
    group_manager: Arc<GroupManager>,
}

impl JoinGroupHandler {
    pub fn new(group_manager: Arc<GroupManager>) -> Self {
        Self { group_manager }
    }

    /// 处理 JoinGroup 请求
    pub fn handle(&self, request: JoinGroupRequest, _version: i16) -> Result<JoinGroupResponse> {
        debug!(
            group_id = %request.group_id,
            member_id = %request.member_id,
            protocol_type = %request.protocol_type,
            protocols = request.protocols.len(),
            "JoinGroup request"
        );

        // 选择第一个协议
        let protocol_name = request.protocols.first().map(|p| p.name.clone());
        let metadata = request
            .protocols
            .first()
            .map(|p| p.metadata.clone())
            .unwrap_or_default();

        match self.group_manager.join_group(
            &request.group_id,
            &request.member_id,
            request.group_instance_id.as_deref(),
            &request.protocol_type,
            metadata,
        ) {
            Ok((generation_id, member_id, leader_id, _proto, members)) => {
                let response_members = members
                    .into_iter()
                    .map(|m| JoinGroupResponseMember {
                        member_id: m.member_id,
                        group_instance_id: m.group_instance_id,
                        metadata: m.metadata,
                    })
                    .collect();

                Ok(JoinGroupResponse {
                    throttle_time_ms: 0,
                    error_code: KafkaErrorCode::None,
                    generation_id,
                    protocol_type: Some(request.protocol_type),
                    protocol_name,
                    leader: leader_id,
                    skip_assignment: false,
                    member_id,
                    members: response_members,
                })
            }
            Err(e) => {
                tracing::error!(error = %e, "JoinGroup failed");
                Ok(JoinGroupResponse {
                    throttle_time_ms: 0,
                    error_code: KafkaErrorCode::UnknownServerError,
                    generation_id: -1,
                    protocol_type: None,
                    protocol_name: None,
                    leader: String::new(),
                    skip_assignment: false,
                    member_id: String::new(),
                    members: vec![],
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_group_handler() {
        let gm = Arc::new(GroupManager::new());
        let handler = JoinGroupHandler::new(gm);

        let request = JoinGroupRequest {
            group_id: "test-group".to_string(),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            member_id: String::new(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: vec![1, 2, 3],
            }],
            reason: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.generation_id, 1);
        assert!(!response.member_id.is_empty());
        assert_eq!(response.leader, response.member_id);
        assert_eq!(response.members.len(), 1);
    }

    #[test]
    fn test_join_group_handler_multiple_members() {
        let gm = Arc::new(GroupManager::new());
        let handler = JoinGroupHandler::new(gm);

        // 第一个成员
        let request1 = JoinGroupRequest {
            group_id: "test-group".to_string(),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            member_id: String::new(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: vec![],
            }],
            reason: None,
        };
        let resp1 = handler.handle(request1, 0).unwrap();
        assert_eq!(resp1.generation_id, 1);

        // 第二个成员
        let request2 = JoinGroupRequest {
            group_id: "test-group".to_string(),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            member_id: String::new(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: vec![],
            }],
            reason: None,
        };
        let resp2 = handler.handle(request2, 0).unwrap();
        assert_eq!(resp2.generation_id, 2);
        assert_eq!(resp2.members.len(), 2);
    }
}
