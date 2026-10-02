//! API Permissions — Kafka API → ACL 操作映射
//!
//! 将每个 Kafka API Key 映射到所需的 ACL 资源和操作:
//!
//! ```text
//! API Key → (ResourceType, ResourceName, AclOperation)
//!
//! 例如:
//!   Produce (0)       → (Topic, topic_name, Write)
//!   Fetch (1)         → (Topic, topic_name, Read)
//!   CreateTopics (19) → (Cluster, cluster_name, Create)
//!   DeleteTopics (20) → (Topic, topic_name, Delete)
//! ```
//!
//! 用于 Router 中间件在处理请求前进行 ACL 检查。

use crate::acl::{AclOperation, ResourceType};

/// API 权限需求
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiPermission {
    /// 所需资源类型
    pub resource_type: ResourceType,
    /// 所需操作
    pub operation: AclOperation,
    /// 是否需要在请求中提取资源名称 (true = 需要从请求体解析)
    pub needs_resource_name: bool,
    /// 是否为认证前允许的 API
    pub is_pre_auth: bool,
}

/// 获取 API Key 对应的权限需求
///
/// 返回 `None` 表示该 API 不需要 ACL 检查 (内部/控制协议)。
pub fn api_permission(api_key: i16) -> Option<ApiPermission> {
    match api_key {
        // Produce (0) — 需要 Topic Write
        0 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Write,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // Fetch (1) — 需要 Topic Read
        1 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Read,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // ListOffsets (2) — 需要 Topic Describe
        2 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // Metadata (3) — 需要 Topic Describe
        3 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // LeaderAndIsr (4) — ClusterAction
        4 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // StopReplica (5) — ClusterAction
        5 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // UpdateMetadata (6) — ClusterAction
        6 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // ControlledShutdown (7) — ClusterAction
        7 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // OffsetCommit (8) — Group Read
        8 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Read,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // OffsetFetch (9) — Group Describe
        9 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // FindCoordinator (10) — Group Describe
        10 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // JoinGroup (11) — Group Read
        11 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Read,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // Heartbeat (12) — Group Read
        12 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Read,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // LeaveGroup (13) — Group Read
        13 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Read,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // SyncGroup (14) — Group Read
        14 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Read,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // DescribeGroups (15) — Group Describe
        15 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // ListGroups (16) — Group Describe
        16 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Describe,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // SaslHandshake (17) — 认证前允许
        17 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::Any,
            needs_resource_name: false,
            is_pre_auth: true,
        }),

        // ApiVersions (18) — 认证前允许
        18 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::Any,
            needs_resource_name: false,
            is_pre_auth: true,
        }),

        // CreateTopics (19) — Cluster Create 或 Topic Create
        19 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::Create,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // DeleteTopics (20) — Topic Delete
        20 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Delete,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // DeleteRecords (21) — Topic Delete
        21 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Delete,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // InitProducerId (22) — 无需 ACL (幂等)
        22 => None,

        // OffsetForLeaderEpoch (23) — Topic Describe
        23 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // AddPartitionsToTxn (24) — TransactionalId Write
        24 => Some(ApiPermission {
            resource_type: ResourceType::TransactionalId,
            operation: AclOperation::Write,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // EndTxn (26) — TransactionalId Write
        26 => Some(ApiPermission {
            resource_type: ResourceType::TransactionalId,
            operation: AclOperation::Write,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // DescribeConfigs (32) — Topic DescribeConfigs
        32 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::DescribeConfigs,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // AlterConfigs (33) — Topic AlterConfigs
        33 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::AlterConfigs,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // SaslAuthenticate (36) — 认证前允许
        36 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::Any,
            needs_resource_name: false,
            is_pre_auth: true,
        }),

        // CreatePartitions (37) — Topic Alter
        37 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Alter,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // IncrementalAlterConfigs (44) — Topic AlterConfigs
        44 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::AlterConfigs,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // AlterPartitionReassignments (45) — ClusterAction
        45 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // ListPartitionReassignments (46) — Topic Describe
        46 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // OffsetDelete (47) — Group Delete
        47 => Some(ApiPermission {
            resource_type: ResourceType::Group,
            operation: AclOperation::Delete,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // DescribeCluster (60) — Cluster Describe
        60 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::Describe,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // DescribeProducers (61) — Topic Describe
        61 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // ListTransactions (65) — Cluster Describe
        65 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::Describe,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // DescribeTopics (70) — Topic Describe
        70 => Some(ApiPermission {
            resource_type: ResourceType::Topic,
            operation: AclOperation::Describe,
            needs_resource_name: true,
            is_pre_auth: false,
        }),

        // Vote (51), BeginQuorumEpoch (52), EndQuorumEpoch (53) — ClusterAction
        51 | 52 | 53 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // BrokerRegistration (54), BrokerHeartbeat (55) — ClusterAction
        54 | 55 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // DescribeQuorum (56) — Cluster Describe
        56 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::Describe,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // ElectLeaders (43) — ClusterAction
        43 => Some(ApiPermission {
            resource_type: ResourceType::Cluster,
            operation: AclOperation::ClusterAction,
            needs_resource_name: false,
            is_pre_auth: false,
        }),

        // 未知 API — 不检查
        _ => None,
    }
}

/// 检查 API 是否为认证前允许的 (pre-auth)
pub fn is_pre_auth_api(api_key: i16) -> bool {
    api_permission(api_key)
        .map(|p| p.is_pre_auth)
        .unwrap_or(false)
}

/// 获取 API 所需的 AclOperation
pub fn api_required_operation(api_key: i16) -> Option<AclOperation> {
    api_permission(api_key).map(|p| p.operation)
}

/// 获取 API 所需的 ResourceType
pub fn api_required_resource_type(api_key: i16) -> Option<ResourceType> {
    api_permission(api_key).map(|p| p.resource_type)
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_requires_write() {
        let perm = api_permission(0).unwrap();
        assert_eq!(perm.resource_type, ResourceType::Topic);
        assert_eq!(perm.operation, AclOperation::Write);
        assert!(perm.needs_resource_name);
        assert!(!perm.is_pre_auth);
    }

    #[test]
    fn test_fetch_requires_read() {
        let perm = api_permission(1).unwrap();
        assert_eq!(perm.resource_type, ResourceType::Topic);
        assert_eq!(perm.operation, AclOperation::Read);
    }

    #[test]
    fn test_metadata_requires_describe() {
        let perm = api_permission(3).unwrap();
        assert_eq!(perm.operation, AclOperation::Describe);
    }

    #[test]
    fn test_create_topics_requires_cluster_create() {
        let perm = api_permission(19).unwrap();
        assert_eq!(perm.resource_type, ResourceType::Cluster);
        assert_eq!(perm.operation, AclOperation::Create);
    }

    #[test]
    fn test_delete_topics_requires_topic_delete() {
        let perm = api_permission(20).unwrap();
        assert_eq!(perm.resource_type, ResourceType::Topic);
        assert_eq!(perm.operation, AclOperation::Delete);
    }

    #[test]
    fn test_pre_auth_apis() {
        assert!(is_pre_auth_api(17)); // SaslHandshake
        assert!(is_pre_auth_api(18)); // ApiVersions
        assert!(is_pre_auth_api(36)); // SaslAuthenticate
        assert!(!is_pre_auth_api(0)); // Produce
        assert!(!is_pre_auth_api(1)); // Fetch
    }

    #[test]
    fn test_group_apis_require_group_resource() {
        // JoinGroup
        let perm = api_permission(11).unwrap();
        assert_eq!(perm.resource_type, ResourceType::Group);

        // Heartbeat
        let perm = api_permission(12).unwrap();
        assert_eq!(perm.resource_type, ResourceType::Group);

        // OffsetCommit
        let perm = api_permission(8).unwrap();
        assert_eq!(perm.resource_type, ResourceType::Group);
    }

    #[test]
    fn test_cluster_action_apis() {
        // LeaderAndIsr
        let perm = api_permission(4).unwrap();
        assert_eq!(perm.operation, AclOperation::ClusterAction);

        // Vote
        let perm = api_permission(51).unwrap();
        assert_eq!(perm.operation, AclOperation::ClusterAction);
    }

    #[test]
    fn test_transactional_apis() {
        // AddPartitionsToTxn
        let perm = api_permission(24).unwrap();
        assert_eq!(perm.resource_type, ResourceType::TransactionalId);
        assert_eq!(perm.operation, AclOperation::Write);

        // EndTxn
        let perm = api_permission(26).unwrap();
        assert_eq!(perm.resource_type, ResourceType::TransactionalId);
    }

    #[test]
    fn test_unknown_api_returns_none() {
        assert!(api_permission(999).is_none());
    }

    #[test]
    fn test_init_producer_id_no_acl() {
        assert!(api_permission(22).is_none());
    }

    #[test]
    fn test_config_apis() {
        // DescribeConfigs
        let perm = api_permission(32).unwrap();
        assert_eq!(perm.operation, AclOperation::DescribeConfigs);

        // AlterConfigs
        let perm = api_permission(33).unwrap();
        assert_eq!(perm.operation, AclOperation::AlterConfigs);
    }

    #[test]
    fn test_helper_functions() {
        assert_eq!(api_required_operation(0), Some(AclOperation::Write));
        assert_eq!(api_required_resource_type(0), Some(ResourceType::Topic));
        assert_eq!(api_required_operation(999), None);
    }
}
