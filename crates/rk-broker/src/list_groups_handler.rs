//! ListGroups Handler
//!
//! 处理 ListGroups 请求 (API Key = 16)。

use std::sync::Arc;

use rk_core::error::Result;
use rk_protocol::apis::list_groups::*;
use rk_protocol::error_codes::KafkaErrorCode;
use tracing::debug;

use crate::group_manager::GroupManager;

/// ListGroups 请求处理器
pub struct ListGroupsHandler {
    group_manager: Arc<GroupManager>,
}

impl ListGroupsHandler {
    pub fn new(group_manager: Arc<GroupManager>) -> Self {
        Self { group_manager }
    }

    /// 处理 ListGroups 请求
    pub fn handle(
        &self,
        request: ListGroupsRequest,
        _version: i16,
    ) -> Result<ListGroupsResponse> {
        debug!("ListGroups request");

        let groups = self.group_manager.list_groups();

        let response_groups = groups
            .into_iter()
            .filter(|g| {
                // 如果有状态过滤，只返回匹配的组
                if let Some(states) = &request.states_filter {
                    let state_str = match g.state {
                        crate::group_manager::GroupState::Empty => "Empty",
                        crate::group_manager::GroupState::PreparingRebalance => "PreparingRebalance",
                        crate::group_manager::GroupState::CompletingRebalance => "CompletingRebalance",
                        crate::group_manager::GroupState::Stable => "Stable",
                        crate::group_manager::GroupState::Dead => "Dead",
                    };
                    states.iter().any(|s| s == state_str)
                } else {
                    true
                }
            })
            .map(|g| {
                let state_str = match g.state {
                    crate::group_manager::GroupState::Empty => "Empty",
                    crate::group_manager::GroupState::PreparingRebalance => "PreparingRebalance",
                    crate::group_manager::GroupState::CompletingRebalance => "CompletingRebalance",
                    crate::group_manager::GroupState::Stable => "Stable",
                    crate::group_manager::GroupState::Dead => "Dead",
                };
                ListGroupsResponseGroup {
                    group_id: g.group_id,
                    protocol_type: g.protocol_type,
                    group_state: Some(state_str.to_string()),
                }
            })
            .collect();

        Ok(ListGroupsResponse {
            throttle_time_ms: 0,
            error_code: KafkaErrorCode::None,
            groups: response_groups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_groups_handler_empty() {
        let gm = Arc::new(GroupManager::new());
        let handler = ListGroupsHandler::new(gm);

        let request = ListGroupsRequest {
            states_filter: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert!(response.groups.is_empty());
    }

    #[test]
    fn test_list_groups_handler_with_groups() {
        let gm = Arc::new(GroupManager::new());

        gm.join_group("group-1", "", None, "consumer", vec![]).unwrap();
        gm.join_group("group-2", "", None, "consumer", vec![]).unwrap();

        let handler = ListGroupsHandler::new(gm);

        let request = ListGroupsRequest {
            states_filter: None,
        };

        let response = handler.handle(request, 0).unwrap();
        assert_eq!(response.groups.len(), 2);
    }

    #[test]
    fn test_list_groups_handler_with_filter() {
        let gm = Arc::new(GroupManager::new());

        gm.join_group("group-1", "", None, "consumer", vec![]).unwrap();

        let handler = ListGroupsHandler::new(gm);

        // 过滤 Stable 状态
        let request = ListGroupsRequest {
            states_filter: Some(vec!["Stable".to_string()]),
        };

        let response = handler.handle(request, 4).unwrap();
        assert_eq!(response.groups.len(), 1);
        assert_eq!(response.groups[0].group_state, Some("Stable".to_string()));

        // 过滤 Empty 状态 (应该为空)
        let request2 = ListGroupsRequest {
            states_filter: Some(vec!["Empty".to_string()]),
        };

        let response2 = handler.handle(request2, 4).unwrap();
        assert!(response2.groups.is_empty());
    }
}
