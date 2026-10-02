//! DescribeGroups Handler
//!
//! 处理 DescribeGroups 请求 (API Key = 15)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::describe_groups::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::group_manager::{GroupManager, GroupState};

/// DescribeGroups 请求处理器
pub struct DescribeGroupsHandler {
    group_manager: Arc<GroupManager>,
}

impl DescribeGroupsHandler {
    pub fn new(group_manager: Arc<GroupManager>) -> Self {
        Self { group_manager }
    }

    /// 处理 DescribeGroups 请求
    pub fn handle(
        &self,
        request: DescribeGroupsRequest,
        _version: i16,
    ) -> Result<DescribeGroupsResponse> {
        debug!(
            groups = request.groups.len(),
            "DescribeGroups request"
        );

        let mut response_groups = Vec::with_capacity(request.groups.len());

        for group_id in &request.groups {
            match self.group_manager.get_group(group_id) {
                Some(group) => {
                    let members = group
                        .members
                        .values()
                        .map(|m| DescribeGroupsResponseMember {
                            member_id: m.member_id.clone(),
                            group_instance_id: m.group_instance_id.clone(),
                            client_id: String::new(), // Phase 1: 不跟踪 client 信息
                            client_host: String::new(),
                            member_metadata: m.protocol_metadata.clone(),
                            member_assignment: m.assignment.clone(),
                        })
                        .collect();

                    let group_state = match group.state {
                        GroupState::Empty => "Empty",
                        GroupState::PreparingRebalance => "PreparingRebalance",
                        GroupState::CompletingRebalance => "CompletingRebalance",
                        GroupState::Stable => "Stable",
                        GroupState::Dead => "Dead",
                    };

                    response_groups.push(DescribeGroupsResponseGroup {
                        error_code: KafkaErrorCode::None,
                        group_id: group_id.clone(),
                        group_state: group_state.to_string(),
                        protocol_type: group.protocol_type.clone(),
                        protocol_data: group.protocol_name.clone().unwrap_or_default(),
                        members,
                        authorized_operations: 0,
                    });
                }
                None => {
                    response_groups.push(DescribeGroupsResponseGroup {
                        error_code: KafkaErrorCode::GroupIdNotFound,
                        group_id: group_id.clone(),
                        group_state: "Dead".to_string(),
                        protocol_type: String::new(),
                        protocol_data: String::new(),
                        members: vec![],
                        authorized_operations: 0,
                    });
                }
            }
        }

        Ok(DescribeGroupsResponse {
            throttle_time_ms: 0,
            groups: response_groups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_groups_handler() {
        let gm = Arc::new(GroupManager::new());

        // 创建一个组
        gm.join_group("test-group", "", None, "consumer", vec![1, 2, 3]).unwrap();

        let handler = DescribeGroupsHandler::new(gm);

        let request = DescribeGroupsRequest {
            groups: vec!["test-group".to_string()],
            include_authorized_operations: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.groups.len(), 1);
        assert_eq!(response.groups[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.groups[0].group_state, "Stable");
        assert_eq!(response.groups[0].protocol_type, "consumer");
        assert_eq!(response.groups[0].members.len(), 1);
    }

    #[test]
    fn test_describe_groups_handler_not_found() {
        let gm = Arc::new(GroupManager::new());
        let handler = DescribeGroupsHandler::new(gm);

        let request = DescribeGroupsRequest {
            groups: vec!["nonexistent".to_string()],
            include_authorized_operations: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.groups.len(), 1);
        assert_eq!(response.groups[0].error_code, KafkaErrorCode::GroupIdNotFound);
    }

    #[test]
    fn test_describe_groups_handler_multiple() {
        let gm = Arc::new(GroupManager::new());

        gm.join_group("group-1", "", None, "consumer", vec![]).unwrap();
        gm.join_group("group-2", "", None, "consumer", vec![]).unwrap();

        let handler = DescribeGroupsHandler::new(gm);

        let request = DescribeGroupsRequest {
            groups: vec!["group-1".to_string(), "group-2".to_string()],
            include_authorized_operations: false,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.groups.len(), 2);
        assert_eq!(response.groups[0].error_code, KafkaErrorCode::None);
        assert_eq!(response.groups[1].error_code, KafkaErrorCode::None);
    }
}
