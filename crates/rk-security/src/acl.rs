//! ACL Authorization — ACL 授权引擎
//!
//! 实现 Kafka 风格的 ACL (Access Control List) 授权:
//!
//! - **Resource Pattern**: 资源匹配 (Topic, Group, Cluster 等)
//! - **Operation**: 操作类型 (Read, Write, Create, Delete 等)
//! - **Permission**: 允许/拒绝
//! - **Principal**: 用户/组标识
//!
//! ```text
//! ACL 检查流程:
//!
//! Request → 提取 (Principal, Resource, Operation)
//!              │
//!              ▼
//!     查找匹配的 ACL 规则
//!              │
//!        ┌─────┴─────┐
//!        │           │
//!     Allow       Deny → 拒绝
//!        │
//!        ▼
//!     允许操作
//!
//! 优先级: Deny > Allow (显式拒绝优先)
//! ```

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

// ─── 资源类型 ───────────────────────────────────────────────────────

/// ACL 资源类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    /// Topic
    Topic,
    /// 消费组
    Group,
    /// 集群
    Cluster,
    /// 事务 ID
    TransactionalId,
    /// Delegation Token
    DelegationToken,
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceType::Topic => write!(f, "Topic"),
            ResourceType::Group => write!(f, "Group"),
            ResourceType::Cluster => write!(f, "Cluster"),
            ResourceType::TransactionalId => write!(f, "TransactionalId"),
            ResourceType::DelegationToken => write!(f, "DelegationToken"),
        }
    }
}

// ─── 资源匹配模式 ───────────────────────────────────────────────────

/// 资源名称匹配模式
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourcePattern {
    /// 精确匹配
    Literal(String),
    /// 前缀匹配
    Prefixed(String),
    /// 匹配所有 (*)
    Any,
}

impl ResourcePattern {
    /// 检查名称是否匹配
    pub fn matches(&self, name: &str) -> bool {
        match self {
            ResourcePattern::Literal(pattern) => pattern == name,
            ResourcePattern::Prefixed(prefix) => name.starts_with(prefix),
            ResourcePattern::Any => true,
        }
    }
}

impl std::fmt::Display for ResourcePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourcePattern::Literal(s) => write!(f, "Literal({})", s),
            ResourcePattern::Prefixed(s) => write!(f, "Prefixed({}*)", s),
            ResourcePattern::Any => write!(f, "Any(*)"),
        }
    }
}

// ─── 操作类型 ───────────────────────────────────────────────────────

/// ACL 操作类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AclOperation {
    /// 任意操作 (通配)
    Any,
    /// 读取
    Read,
    /// 写入
    Write,
    /// 创建
    Create,
    /// 删除
    Delete,
    /// Alter
    Alter,
    /// Describe
    Describe,
    /// ClusterAction
    ClusterAction,
    /// DescribeConfigs
    DescribeConfigs,
    /// AlterConfigs
    AlterConfigs,
    /// IdempotentWrite
    IdempotentWrite,
}

impl std::fmt::Display for AclOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AclOperation::Any => write!(f, "Any"),
            AclOperation::Read => write!(f, "Read"),
            AclOperation::Write => write!(f, "Write"),
            AclOperation::Create => write!(f, "Create"),
            AclOperation::Delete => write!(f, "Delete"),
            AclOperation::Alter => write!(f, "Alter"),
            AclOperation::Describe => write!(f, "Describe"),
            AclOperation::ClusterAction => write!(f, "ClusterAction"),
            AclOperation::DescribeConfigs => write!(f, "DescribeConfigs"),
            AclOperation::AlterConfigs => write!(f, "AlterConfigs"),
            AclOperation::IdempotentWrite => write!(f, "IdempotentWrite"),
        }
    }
}

// ─── 权限类型 ───────────────────────────────────────────────────────

/// 权限类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PermissionType {
    /// 允许
    Allow,
    /// 拒绝
    Deny,
}

impl std::fmt::Display for PermissionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PermissionType::Allow => write!(f, "Allow"),
            PermissionType::Deny => write!(f, "Deny"),
        }
    }
}

// ─── ACL 条目 ───────────────────────────────────────────────────────

/// ACL 条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    /// 主体 (User:alice, User:*)
    pub principal: String,
    /// 主机 (允许连接的来源主机, * 表示任意)
    pub host: String,
    /// 操作
    pub operation: AclOperation,
    /// 权限类型
    pub permission: PermissionType,
    /// 资源类型
    pub resource_type: ResourceType,
    /// 资源匹配模式
    pub resource_pattern: ResourcePattern,
}

impl AclEntry {
    /// 创建 Allow 规则
    pub fn allow(
        principal: &str,
        resource_type: ResourceType,
        pattern: ResourcePattern,
        operation: AclOperation,
    ) -> Self {
        Self {
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission: PermissionType::Allow,
            resource_type,
            resource_pattern: pattern,
        }
    }

    /// 创建 Deny 规则
    pub fn deny(
        principal: &str,
        resource_type: ResourceType,
        pattern: ResourcePattern,
        operation: AclOperation,
    ) -> Self {
        Self {
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission: PermissionType::Deny,
            resource_type,
            resource_pattern: pattern,
        }
    }

    /// 设置主机限制
    pub fn with_host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    /// 检查主体是否匹配
    pub fn matches_principal(&self, principal: &str) -> bool {
        self.principal == "*" || self.principal == principal || self.principal == "User:*"
    }

    /// 检查操作是否匹配
    pub fn matches_operation(&self, op: &AclOperation) -> bool {
        self.operation == AclOperation::Any || self.operation == *op
    }

    /// 检查主机是否匹配
    pub fn matches_host(&self, host: &str) -> bool {
        self.host == "*" || self.host == host
    }
}

impl std::fmt::Display for AclEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AclEntry(principal={}, {}, {} {}, {}={}, {})",
            self.principal,
            self.host,
            self.permission,
            self.operation,
            self.resource_type,
            self.resource_pattern,
            self.permission,
        )
    }
}

// ─── ACL 引擎 ───────────────────────────────────────────────────────

/// ACL 资源键 (用于索引)
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct AclResourceKey {
    resource_type: ResourceType,
    resource_name: String,
}

/// ACL 授权引擎
pub struct AclEngine {
    /// ACL 规则存储: resource_key → Vec<AclEntry>
    rules: DashMap<AclResourceKey, Vec<AclEntry>>,
    /// 是否启用 ACL
    enabled: bool,
}

impl AclEngine {
    /// 创建新的 ACL 引擎
    pub fn new(enabled: bool) -> Self {
        Self {
            rules: DashMap::new(),
            enabled,
        }
    }

    /// 是否启用
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// 添加 ACL 规则
    pub fn add_acl(&self, entry: AclEntry) {
        let resource_name = match &entry.resource_pattern {
            ResourcePattern::Literal(s) => s.clone(),
            ResourcePattern::Prefixed(s) => s.clone(),
            ResourcePattern::Any => "*".to_string(),
        };
        let key = AclResourceKey {
            resource_type: entry.resource_type.clone(),
            resource_name,
        };
        self.rules.entry(key).or_insert_with(Vec::new).push(entry);
        debug!("ACL rule added");
    }

    /// 移除匹配的 ACL 规则
    pub fn remove_acl(
        &self,
        resource_type: &ResourceType,
        resource_name: &str,
        principal: &str,
        operation: &AclOperation,
    ) -> bool {
        let key = AclResourceKey {
            resource_type: resource_type.clone(),
            resource_name: resource_name.to_string(),
        };
        if let Some(mut entries) = self.rules.get_mut(&key) {
            let before = entries.len();
            entries.retain(|e| {
                !(e.principal == principal && e.operation == *operation)
            });
            let removed = entries.len() < before;
            if entries.is_empty() {
                drop(entries);
                self.rules.remove(&key);
            }
            removed
        } else {
            false
        }
    }

    /// 检查授权
    ///
    /// 返回 `true` 表示允许，`false` 表示拒绝。
    /// 规则:
    /// 1. 如果 ACL 未启用，始终允许
    /// 2. 显式 Deny 优先
    /// 3. 显式 Allow 通过
    /// 4. 无匹配规则时拒绝 (默认拒绝)
    pub fn authorize(
        &self,
        principal: &str,
        host: &str,
        resource_type: &ResourceType,
        resource_name: &str,
        operation: &AclOperation,
    ) -> bool {
        if !self.enabled {
            return true;
        }

        let mut has_allow = false;

        // 遍历所有规则，查找匹配
        for entry in self.rules.iter() {
            let key = entry.key();
            let entries = entry.value();

            // 检查资源类型匹配
            if key.resource_type != *resource_type {
                continue;
            }

            // 检查资源名称匹配
            let name_matches = entries.iter().any(|e| {
                match &e.resource_pattern {
                    ResourcePattern::Literal(s) => s == resource_name,
                    ResourcePattern::Prefixed(prefix) => resource_name.starts_with(prefix),
                    ResourcePattern::Any => true,
                }
            });

            if !name_matches {
                continue;
            }

            for acl in entries {
                // 检查主体、主机、操作匹配
                if !acl.matches_principal(principal)
                    || !acl.matches_host(host)
                    || !acl.matches_operation(operation)
                {
                    continue;
                }

                match acl.permission {
                    PermissionType::Deny => {
                        // 显式拒绝 → 立即拒绝
                        warn!(
                            principal = principal,
                            operation = %operation,
                            resource = %resource_name,
                            "ACL DENY"
                        );
                        return false;
                    }
                    PermissionType::Allow => {
                        has_allow = true;
                    }
                }
            }
        }

        if has_allow {
            debug!(
                principal = principal,
                operation = %operation,
                resource = %resource_name,
                "ACL ALLOW"
            );
            true
        } else {
            // 默认拒绝
            debug!(
                principal = principal,
                operation = %operation,
                resource = %resource_name,
                "ACL DEFAULT DENY (no matching rule)"
            );
            false
        }
    }

    /// 获取所有 ACL 规则
    pub fn list_acls(&self) -> Vec<AclEntry> {
        self.rules
            .iter()
            .flat_map(|entry| entry.value().clone())
            .collect()
    }

    /// 获取指定资源的 ACL
    pub fn list_acls_for_resource(
        &self,
        resource_type: &ResourceType,
        resource_name: &str,
    ) -> Vec<AclEntry> {
        let key = AclResourceKey {
            resource_type: resource_type.clone(),
            resource_name: resource_name.to_string(),
        };
        self.rules
            .get(&key)
            .map(|entries| entries.clone())
            .unwrap_or_default()
    }

    /// ACL 规则总数
    pub fn acl_count(&self) -> usize {
        self.rules.iter().map(|e| e.value().len()).sum()
    }

    /// 清空所有 ACL
    pub fn clear(&self) {
        self.rules.clear();
    }

    /// 批量添加 ACL
    pub fn add_acls(&self, entries: Vec<AclEntry>) {
        for entry in entries {
            self.add_acl(entry);
        }
    }
}

impl std::fmt::Debug for AclEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AclEngine")
            .field("enabled", &self.enabled)
            .field("rule_count", &self.acl_count())
            .finish()
    }
}

// ─── 便捷方法 ───────────────────────────────────────────────────────

/// 创建超级用户 ACL (允许所有操作)
pub fn super_user_acl(principal: &str) -> AclEntry {
    AclEntry {
        principal: principal.to_string(),
        host: "*".to_string(),
        operation: AclOperation::Any,
        permission: PermissionType::Allow,
        resource_type: ResourceType::Topic,
        resource_pattern: ResourcePattern::Any,
    }
}

/// 创建 Topic 读取 ACL
pub fn topic_read_acl(principal: &str, topic: &str) -> AclEntry {
    AclEntry::allow(
        principal,
        ResourceType::Topic,
        ResourcePattern::Literal(topic.to_string()),
        AclOperation::Read,
    )
}

/// 创建 Topic 写入 ACL
pub fn topic_write_acl(principal: &str, topic: &str) -> AclEntry {
    AclEntry::allow(
        principal,
        ResourceType::Topic,
        ResourcePattern::Literal(topic.to_string()),
        AclOperation::Write,
    )
}

/// 创建 Group 读取 ACL
pub fn group_read_acl(principal: &str, group: &str) -> AclEntry {
    AclEntry::allow(
        principal,
        ResourceType::Group,
        ResourcePattern::Literal(group.to_string()),
        AclOperation::Read,
    )
}

/// 创建前缀匹配 ACL
pub fn prefix_acl(
    principal: &str,
    resource_type: ResourceType,
    prefix: &str,
    operation: AclOperation,
) -> AclEntry {
    AclEntry::allow(
        principal,
        resource_type,
        ResourcePattern::Prefixed(prefix.to_string()),
        operation,
    )
}

// ─── 测试 ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_type_display() {
        assert_eq!(ResourceType::Topic.to_string(), "Topic");
        assert_eq!(ResourceType::Group.to_string(), "Group");
        assert_eq!(ResourceType::Cluster.to_string(), "Cluster");
    }

    #[test]
    fn test_resource_pattern_literal() {
        let pattern = ResourcePattern::Literal("test-topic".to_string());
        assert!(pattern.matches("test-topic"));
        assert!(!pattern.matches("other-topic"));
    }

    #[test]
    fn test_resource_pattern_prefixed() {
        let pattern = ResourcePattern::Prefixed("test-".to_string());
        assert!(pattern.matches("test-topic"));
        assert!(pattern.matches("test-"));
        assert!(!pattern.matches("other-topic"));
    }

    #[test]
    fn test_resource_pattern_any() {
        let pattern = ResourcePattern::Any;
        assert!(pattern.matches("anything"));
        assert!(pattern.matches(""));
    }

    #[test]
    fn test_acl_entry_allow() {
        let entry = AclEntry::allow(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Literal("test".to_string()),
            AclOperation::Read,
        );
        assert_eq!(entry.permission, PermissionType::Allow);
        assert_eq!(entry.principal, "User:alice");
    }

    #[test]
    fn test_acl_entry_deny() {
        let entry = AclEntry::deny(
            "User:bob",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Write,
        );
        assert_eq!(entry.permission, PermissionType::Deny);
    }

    #[test]
    fn test_acl_entry_with_host() {
        let entry = AclEntry::allow(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Read,
        )
        .with_host("10.0.0.1");
        assert_eq!(entry.host, "10.0.0.1");
        assert!(entry.matches_host("10.0.0.1"));
        assert!(!entry.matches_host("10.0.0.2"));
    }

    #[test]
    fn test_acl_entry_matches_principal() {
        let entry = AclEntry::allow(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Read,
        );
        assert!(entry.matches_principal("User:alice"));
        assert!(!entry.matches_principal("User:bob"));

        // 通配符
        let wildcard = AclEntry::allow(
            "User:*",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Read,
        );
        assert!(wildcard.matches_principal("User:anyone"));
    }

    #[test]
    fn test_acl_entry_matches_operation() {
        let entry = AclEntry::allow(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Read,
        );
        assert!(entry.matches_operation(&AclOperation::Read));
        assert!(!entry.matches_operation(&AclOperation::Write));

        // Any 匹配所有
        let any_op = AclEntry::allow(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Any,
        );
        assert!(any_op.matches_operation(&AclOperation::Read));
        assert!(any_op.matches_operation(&AclOperation::Write));
    }

    #[test]
    fn test_acl_engine_disabled() {
        let engine = AclEngine::new(false);
        // 未启用时始终允许
        assert!(engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "test",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_acl_engine_allow() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "test-topic"));

        assert!(engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "test-topic",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_acl_engine_deny_overrides_allow() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "test-topic"));
        engine.add_acl(AclEntry::deny(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Literal("test-topic".to_string()),
            AclOperation::Read,
        ));

        // Deny 优先
        assert!(!engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "test-topic",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_acl_engine_default_deny() {
        let engine = AclEngine::new(true);
        // 无规则 → 默认拒绝
        assert!(!engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "test-topic",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_acl_engine_wrong_operation() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "test-topic"));

        // alice 有 Read 权限，但尝试 Write
        assert!(!engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "test-topic",
            &AclOperation::Write,
        ));
    }

    #[test]
    fn test_acl_engine_wrong_resource() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "test-topic"));

        // alice 有 test-topic 权限，但尝试 other-topic
        assert!(!engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "other-topic",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_acl_engine_prefixed() {
        let engine = AclEngine::new(true);
        engine.add_acl(prefix_acl(
            "User:alice",
            ResourceType::Topic,
            "dev-",
            AclOperation::Read,
        ));

        assert!(engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "dev-topic-1",
            &AclOperation::Read,
        ));
        assert!(engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "dev-topic-2",
            &AclOperation::Read,
        ));
        assert!(!engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "prod-topic",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_acl_engine_remove() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "test-topic"));
        assert_eq!(engine.acl_count(), 1);

        let removed = engine.remove_acl(
            &ResourceType::Topic,
            "test-topic",
            "User:alice",
            &AclOperation::Read,
        );
        assert!(removed);
        assert_eq!(engine.acl_count(), 0);
    }

    #[test]
    fn test_acl_engine_list() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "topic1"));
        engine.add_acl(topic_write_acl("User:bob", "topic2"));

        let acls = engine.list_acls();
        assert_eq!(acls.len(), 2);
    }

    #[test]
    fn test_acl_engine_list_for_resource() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "topic1"));
        engine.add_acl(topic_write_acl("User:bob", "topic1"));
        engine.add_acl(topic_read_acl("User:alice", "topic2"));

        let acls = engine.list_acls_for_resource(&ResourceType::Topic, "topic1");
        assert_eq!(acls.len(), 2);
    }

    #[test]
    fn test_acl_engine_clear() {
        let engine = AclEngine::new(true);
        engine.add_acl(topic_read_acl("User:alice", "topic1"));
        engine.add_acl(topic_write_acl("User:bob", "topic2"));
        assert_eq!(engine.acl_count(), 2);

        engine.clear();
        assert_eq!(engine.acl_count(), 0);
    }

    #[test]
    fn test_acl_engine_batch_add() {
        let engine = AclEngine::new(true);
        engine.add_acls(vec![
            topic_read_acl("User:alice", "t1"),
            topic_write_acl("User:alice", "t1"),
            group_read_acl("User:alice", "g1"),
        ]);
        assert_eq!(engine.acl_count(), 3);
    }

    #[test]
    fn test_super_user_acl() {
        let entry = super_user_acl("User:admin");
        assert_eq!(entry.principal, "User:admin");
        assert_eq!(entry.operation, AclOperation::Any);
        assert_eq!(entry.permission, PermissionType::Allow);
    }

    #[test]
    fn test_acl_engine_wildcard_principal() {
        let engine = AclEngine::new(true);
        engine.add_acl(AclEntry::allow(
            "User:*",
            ResourceType::Topic,
            ResourcePattern::Literal("public".to_string()),
            AclOperation::Read,
        ));

        assert!(engine.authorize(
            "User:anyone",
            "10.0.0.1",
            &ResourceType::Topic,
            "public",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_acl_engine_host_restriction() {
        let engine = AclEngine::new(true);
        engine.add_acl(
            AclEntry::allow(
                "User:alice",
                ResourceType::Topic,
                ResourcePattern::Literal("secret".to_string()),
                AclOperation::Read,
            )
            .with_host("10.0.0.1"),
        );

        assert!(engine.authorize(
            "User:alice",
            "10.0.0.1",
            &ResourceType::Topic,
            "secret",
            &AclOperation::Read,
        ));
        assert!(!engine.authorize(
            "User:alice",
            "10.0.0.2",
            &ResourceType::Topic,
            "secret",
            &AclOperation::Read,
        ));
    }

    #[test]
    fn test_operation_display() {
        assert_eq!(AclOperation::Read.to_string(), "Read");
        assert_eq!(AclOperation::Write.to_string(), "Write");
        assert_eq!(AclOperation::Any.to_string(), "Any");
    }

    #[test]
    fn test_permission_type_display() {
        assert_eq!(PermissionType::Allow.to_string(), "Allow");
        assert_eq!(PermissionType::Deny.to_string(), "Deny");
    }
}
