//! Group Manager
//!
//! 管理消费者组状态: 成员、generation、leader、分区分配。
//! Phase 1: 简化实现，单 Broker 协调，无 rebalance 超时。

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};

use dashmap::DashMap;
use rk_core::error::{Result, RkError};
use tracing::{debug, info};

/// JoinGroup 响应元组类型 (generation_id, member_id, leader_id, protocol_name, members)
pub type JoinGroupResult = (
    i32,
    String,
    String,
    Option<String>,
    Vec<JoinGroupMemberInfo>,
);

/// 消费者组成员信息
#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_metadata: Vec<u8>,
    /// 该 member 的分区分配 (SyncGroup 后填充)
    pub assignment: Vec<u8>,
}

/// 消费者组状态
#[derive(Debug, Clone, PartialEq)]
pub enum GroupState {
    /// 空组
    Empty,
    /// 等待成员加入
    PreparingRebalance,
    /// 等待 leader 分配
    CompletingRebalance,
    /// 稳定运行
    Stable,
    /// 已死亡
    Dead,
}

/// 消费者组信息
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub state: GroupState,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: Option<String>,
    pub leader_id: String,
    pub members: HashMap<String, GroupMember>,
}

impl ConsumerGroup {
    fn new(group_id: String) -> Self {
        Self {
            group_id,
            state: GroupState::Empty,
            generation_id: 0,
            protocol_type: String::new(),
            protocol_name: None,
            leader_id: String::new(),
            members: HashMap::new(),
        }
    }
}

/// Group Manager: 管理所有消费者组
pub struct GroupManager {
    groups: DashMap<String, ConsumerGroup>,
    /// 全局 member_id 生成器
    member_id_counter: AtomicI32,
}

impl Default for GroupManager {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupManager {
    pub fn new() -> Self {
        Self {
            groups: DashMap::new(),
            member_id_counter: AtomicI32::new(1),
        }
    }

    /// 生成新的 member_id
    pub fn generate_member_id(&self) -> String {
        let id = self.member_id_counter.fetch_add(1, Ordering::Relaxed);
        format!("rk-consumer-{}", id)
    }

    /// 获取或创建消费者组
    pub fn get_or_create_group(&self, group_id: &str) -> ConsumerGroup {
        self.groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id.to_string()))
            .clone()
    }

    /// 处理 JoinGroup: 添加/更新成员
    pub fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocol_metadata: Vec<u8>,
    ) -> Result<JoinGroupResult> {
        let mut group = self
            .groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id.to_string()));

        // 确定实际 member_id
        let actual_member_id = if member_id.is_empty() {
            self.generate_member_id()
        } else {
            member_id.to_string()
        };

        // 添加或更新成员
        let member = GroupMember {
            member_id: actual_member_id.clone(),
            group_instance_id: group_instance_id.map(|s| s.to_string()),
            protocol_metadata,
            assignment: vec![],
        };

        group.members.insert(actual_member_id.clone(), member);

        // 如果组是空的或是新成员，设置 leader
        if group.leader_id.is_empty() || !group.members.contains_key(&group.leader_id) {
            group.leader_id = actual_member_id.clone();
        }

        // 递增 generation
        group.generation_id += 1;
        group.protocol_type = protocol_type.to_string();
        group.state = GroupState::Stable;

        let generation_id = group.generation_id;
        let leader_id = group.leader_id.clone();
        let protocol_name = group.protocol_name.clone();

        // 收集成员信息
        let member_infos: Vec<JoinGroupMemberInfo> = group
            .members
            .values()
            .map(|m| JoinGroupMemberInfo {
                member_id: m.member_id.clone(),
                group_instance_id: m.group_instance_id.clone(),
                metadata: m.protocol_metadata.clone(),
            })
            .collect();

        debug!(
            group_id = group_id,
            member_id = %actual_member_id,
            generation = generation_id,
            members = group.members.len(),
            "Member joined group"
        );

        Ok((
            generation_id,
            actual_member_id,
            leader_id,
            protocol_name,
            member_infos,
        ))
    }

    /// 处理 SyncGroup: 设置成员分区分配
    pub fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<u8>> {
        let mut group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| RkError::Protocol(format!("Group {} not found", group_id)))?;

        // 验证 generation
        if group.generation_id != generation_id {
            return Err(RkError::Protocol(format!(
                "Generation mismatch: expected {}, got {}",
                group.generation_id, generation_id
            )));
        }

        // 应用 assignments (仅 leader 提交)
        for (mid, assignment) in &assignments {
            if let Some(member) = group.members.get_mut(mid) {
                member.assignment = assignment.clone();
            }
        }

        group.state = GroupState::Stable;

        // 返回该 member 的分配
        let my_assignment = group
            .members
            .get(member_id)
            .map(|m| m.assignment.clone())
            .unwrap_or_default();

        debug!(
            group_id = group_id,
            member_id = member_id,
            assignments = assignments.len(),
            "Group synced"
        );

        Ok(my_assignment)
    }

    /// 处理 Heartbeat: 验证成员活跃
    pub fn heartbeat(&self, group_id: &str, member_id: &str, generation_id: i32) -> Result<()> {
        let group = self
            .groups
            .get(group_id)
            .ok_or_else(|| RkError::Protocol(format!("Group {} not found", group_id)))?;

        if group.generation_id != generation_id {
            return Err(RkError::Protocol(format!(
                "Generation mismatch: expected {}, got {}",
                group.generation_id, generation_id
            )));
        }

        if !group.members.contains_key(member_id) {
            return Err(RkError::Protocol(format!(
                "Member {} not in group {}",
                member_id, group_id
            )));
        }

        Ok(())
    }

    /// 处理 LeaveGroup: 移除成员
    pub fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let mut group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| RkError::Protocol(format!("Group {} not found", group_id)))?;

        // 检查成员是否存在
        if !group.members.contains_key(member_id) {
            return Err(RkError::Protocol(format!(
                "Member {} not in group {}",
                member_id, group_id
            )));
        }

        group.members.remove(member_id);

        // 如果 leader 离开，选新 leader
        if group.leader_id == member_id {
            group.leader_id = group.members.keys().next().cloned().unwrap_or_default();
        }

        // 如果组空了，重置状态
        if group.members.is_empty() {
            group.state = GroupState::Empty;
            group.generation_id = 0;
            group.leader_id = String::new();
        }

        info!(
            group_id = group_id,
            member_id = member_id,
            remaining = group.members.len(),
            "Member left group"
        );

        Ok(())
    }

    /// 获取组信息
    pub fn get_group(&self, group_id: &str) -> Option<ConsumerGroup> {
        self.groups.get(group_id).map(|g| g.clone())
    }

    /// 列出所有组
    pub fn list_groups(&self) -> Vec<ConsumerGroup> {
        self.groups.iter().map(|g| g.clone()).collect()
    }

    /// 获取组数量
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

/// JoinGroup 返回的成员信息
#[derive(Debug, Clone)]
pub struct JoinGroupMemberInfo {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_manager_join() {
        let gm = GroupManager::new();

        let (gen, mid, leader, _proto, members) = gm
            .join_group("group-1", "", None, "consumer", vec![1, 2, 3])
            .unwrap();

        assert_eq!(gen, 1);
        assert!(!mid.is_empty());
        assert_eq!(leader, mid);
        assert_eq!(members.len(), 1);
    }

    #[test]
    fn test_group_manager_join_multiple() {
        let gm = GroupManager::new();

        gm.join_group("group-1", "", None, "consumer", vec![])
            .unwrap();
        let (gen, _mid, _leader, _proto, members) = gm
            .join_group("group-1", "", None, "consumer", vec![])
            .unwrap();

        assert_eq!(gen, 2);
        assert_eq!(members.len(), 2);
    }

    #[test]
    fn test_group_manager_sync() {
        let gm = GroupManager::new();

        let (gen, mid, _leader, _proto, _members) = gm
            .join_group("group-1", "", None, "consumer", vec![])
            .unwrap();

        let assignment = vec![10, 20, 30];
        let result = gm
            .sync_group(
                "group-1",
                &mid,
                gen,
                vec![(mid.clone(), assignment.clone())],
            )
            .unwrap();

        assert_eq!(result, assignment);
    }

    #[test]
    fn test_group_manager_heartbeat() {
        let gm = GroupManager::new();

        let (gen, mid, _leader, _proto, _members) = gm
            .join_group("group-1", "", None, "consumer", vec![])
            .unwrap();

        gm.heartbeat("group-1", &mid, gen).unwrap();
    }

    #[test]
    fn test_group_manager_heartbeat_wrong_generation() {
        let gm = GroupManager::new();

        let (_gen, mid, _leader, _proto, _members) = gm
            .join_group("group-1", "", None, "consumer", vec![])
            .unwrap();

        let result = gm.heartbeat("group-1", &mid, 999);
        assert!(result.is_err());
    }

    #[test]
    fn test_group_manager_leave() {
        let gm = GroupManager::new();

        let (_gen, mid, _leader, _proto, _members) = gm
            .join_group("group-1", "", None, "consumer", vec![])
            .unwrap();

        gm.leave_group("group-1", &mid).unwrap();

        let group = gm.get_group("group-1").unwrap();
        assert!(group.members.is_empty());
        assert_eq!(group.state, GroupState::Empty);
    }

    #[test]
    fn test_group_manager_leave_leader() {
        let gm = GroupManager::new();

        let (_gen, _mid1, _leader, _proto, _members) = gm
            .join_group("group-1", "", None, "consumer", vec![])
            .unwrap();
        let (_gen, _mid2, _leader, _proto, _members) = gm
            .join_group("group-1", "", None, "consumer", vec![])
            .unwrap();

        let group = gm.get_group("group-1").unwrap();
        let leader = group.leader_id.clone();

        // leader 离开
        gm.leave_group("group-1", &leader).unwrap();

        let group = gm.get_group("group-1").unwrap();
        assert_ne!(group.leader_id, leader);
        assert_eq!(group.members.len(), 1);
    }

    #[test]
    fn test_group_manager_list_groups() {
        let gm = GroupManager::new();

        gm.join_group("group-1", "", None, "consumer", vec![])
            .unwrap();
        gm.join_group("group-2", "", None, "consumer", vec![])
            .unwrap();

        let groups = gm.list_groups();
        assert_eq!(groups.len(), 2);
    }
}
