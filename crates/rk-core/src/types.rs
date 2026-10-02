//! R-Kafka 核心公共类型
//!
//! 所有 newtype 包装提供类型安全，防止在不同语义场景混用原始数值。

use serde::{Deserialize, Serialize};
use std::fmt;

/// Broker 节点标识
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BrokerId(pub i32);

/// Partition 编号
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionId(pub i32);

/// 消息偏移量 (单调递增)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Offset(pub i64);

/// Leader Epoch (KRaft 纪元)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch(pub i32);

/// Topic 名称 (newtype for String)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicName(pub String);

/// Topic UUID (Kafka 4.0+ 使用 UUID 标识 Topic)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicId(pub u128);

/// Node ID (KRaft 中 Broker 或 Controller 的统一标识)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub i32);

/// Correlation ID (请求-响应配对)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorrelationId(pub i32);

/// Producer ID (幂等/事务 Producer)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProducerId(pub i64);

/// Producer Epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProducerEpoch(pub i16);

/// Sequence Number (幂等去重)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SequenceNumber(pub i32);

/// Group ID (Consumer Group)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(pub String);

/// Member ID (Consumer Group 成员)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MemberId(pub String);

// ─── Kafka API Key ────────────────────────────────────────────────────

/// Kafka 协议 API Key
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    EndTxn = 26,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    OffsetDelete = 47,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    // KRaft APIs
    Vote = 51,
    BeginQuorumEpoch = 52,
    EndQuorumEpoch = 53,
    BrokerRegistration = 54,
    BrokerHeartbeat = 55,
    DescribeQuorum = 56,
    DescribeCluster = 60,
    DescribeProducers = 61,
    ListTransactions = 65,
    DescribeTopics = 70,
}

impl ApiKey {
    /// 从 i16 解析 API Key
    pub fn from_i16(v: i16) -> Option<Self> {
        match v {
            0 => Some(Self::Produce),
            1 => Some(Self::Fetch),
            2 => Some(Self::ListOffsets),
            3 => Some(Self::Metadata),
            4 => Some(Self::LeaderAndIsr),
            5 => Some(Self::StopReplica),
            6 => Some(Self::UpdateMetadata),
            7 => Some(Self::ControlledShutdown),
            8 => Some(Self::OffsetCommit),
            9 => Some(Self::OffsetFetch),
            10 => Some(Self::FindCoordinator),
            11 => Some(Self::JoinGroup),
            12 => Some(Self::Heartbeat),
            13 => Some(Self::LeaveGroup),
            14 => Some(Self::SyncGroup),
            15 => Some(Self::DescribeGroups),
            16 => Some(Self::ListGroups),
            17 => Some(Self::SaslHandshake),
            18 => Some(Self::ApiVersions),
            19 => Some(Self::CreateTopics),
            20 => Some(Self::DeleteTopics),
            21 => Some(Self::DeleteRecords),
            22 => Some(Self::InitProducerId),
            23 => Some(Self::OffsetForLeaderEpoch),
            24 => Some(Self::AddPartitionsToTxn),
            26 => Some(Self::EndTxn),
            32 => Some(Self::DescribeConfigs),
            33 => Some(Self::AlterConfigs),
            36 => Some(Self::SaslAuthenticate),
            37 => Some(Self::CreatePartitions),
            43 => Some(Self::ElectLeaders),
            44 => Some(Self::IncrementalAlterConfigs),
            45 => Some(Self::AlterPartitionReassignments),
            46 => Some(Self::ListPartitionReassignments),
            47 => Some(Self::OffsetDelete),
            51 => Some(Self::Vote),
            52 => Some(Self::BeginQuorumEpoch),
            53 => Some(Self::EndQuorumEpoch),
            54 => Some(Self::BrokerRegistration),
            55 => Some(Self::BrokerHeartbeat),
            56 => Some(Self::DescribeQuorum),
            60 => Some(Self::DescribeCluster),
            61 => Some(Self::DescribeProducers),
            65 => Some(Self::ListTransactions),
            70 => Some(Self::DescribeTopics),
            _ => None,
        }
    }

    /// 该 API 是否使用 Flexible 版本 (header v2+)
    ///
    /// Kafka 从 KIP-482 开始，部分 API 版本使用 Flexible 格式:
    /// - Request Header v2 带 tagged fields
    /// - Response Header v1 带 tagged fields
    pub fn is_flexible(&self, version: i16) -> bool {
        match self {
            // ApiVersions v3+ is flexible
            ApiKey::ApiVersions => version >= 3,
            // Metadata v9+ is flexible
            ApiKey::Metadata => version >= 9,
            // Produce v9+ is flexible
            ApiKey::Produce => version >= 9,
            // Fetch v12+ is flexible
            ApiKey::Fetch => version >= 12,
            // ListOffsets v7+ is flexible
            ApiKey::ListOffsets => version >= 7,
            // OffsetCommit v8+ is flexible
            ApiKey::OffsetCommit => version >= 8,
            // OffsetFetch v8+ is flexible
            ApiKey::OffsetFetch => version >= 8,
            // FindCoordinator v4+ is flexible
            ApiKey::FindCoordinator => version >= 4,
            // JoinGroup v6+ is flexible
            ApiKey::JoinGroup => version >= 6,
            // Heartbeat v4+ is flexible
            ApiKey::Heartbeat => version >= 4,
            // LeaveGroup v4+ is flexible
            ApiKey::LeaveGroup => version >= 4,
            // SyncGroup v4+ is flexible
            ApiKey::SyncGroup => version >= 4,
            // DescribeGroups v5+ is flexible
            ApiKey::DescribeGroups => version >= 5,
            // ListGroups v4+ is flexible
            ApiKey::ListGroups => version >= 4,
            // SaslHandshake v1+ is flexible
            ApiKey::SaslHandshake => version >= 1,
            // InitProducerId v3+ is flexible
            ApiKey::InitProducerId => version >= 3,
            // OffsetForLeaderEpoch v3+ is flexible
            ApiKey::OffsetForLeaderEpoch => version >= 3,
            // AddPartitionsToTxn v3+ is flexible
            ApiKey::AddPartitionsToTxn => version >= 3,
            // EndTxn v3+ is flexible
            ApiKey::EndTxn => version >= 3,
            // CreateTopics v3+ (actually not in Kafka, but for future)
            // DeleteTopics v4+ is flexible
            ApiKey::DeleteTopics => version >= 4,
            // DeleteRecords v2+ is flexible
            ApiKey::DeleteRecords => version >= 2,
            // DescribeConfigs v3+ is flexible
            ApiKey::DescribeConfigs => version >= 3,
            // AlterConfigs v2+ is flexible
            ApiKey::AlterConfigs => version >= 2,
            // SaslAuthenticate v2+ is flexible
            ApiKey::SaslAuthenticate => version >= 2,
            // CreatePartitions v2+ is flexible
            ApiKey::CreatePartitions => version >= 2,
            // ElectLeaders v2+ is flexible
            ApiKey::ElectLeaders => version >= 2,
            // IncrementalAlterConfigs v0+ is always flexible
            ApiKey::IncrementalAlterConfigs => true,
            // OffsetDelete v0+ is always flexible
            ApiKey::OffsetDelete => true,
            // AlterPartitionReassignments v0+ is always flexible
            ApiKey::AlterPartitionReassignments => true,
            // ListPartitionReassignments v0+ is always flexible
            ApiKey::ListPartitionReassignments => true,
            // DescribeCluster v0+ is always flexible
            ApiKey::DescribeCluster => true,
            // DescribeProducers v0+ is always flexible
            ApiKey::DescribeProducers => true,
            // ListTransactions v0+ is always flexible
            ApiKey::ListTransactions => true,
            // DescribeTopics v0+ is always flexible
            ApiKey::DescribeTopics => true,
            // DescribeQuorum v0+ is always flexible
            ApiKey::DescribeQuorum => true,
            _ => false,
        }
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}({})", self, *self as i16)
    }
}

// ─── From/Into 转换 ──────────────────────────────────────────────────

impl From<i32> for BrokerId {
    fn from(v: i32) -> Self {
        Self(v)
    }
}
impl From<BrokerId> for i32 {
    fn from(v: BrokerId) -> Self {
        v.0
    }
}

impl From<i32> for PartitionId {
    fn from(v: i32) -> Self {
        Self(v)
    }
}
impl From<PartitionId> for i32 {
    fn from(v: PartitionId) -> Self {
        v.0
    }
}

impl From<i64> for Offset {
    fn from(v: i64) -> Self {
        Self(v)
    }
}
impl From<Offset> for i64 {
    fn from(v: Offset) -> Self {
        v.0
    }
}

impl From<i32> for Epoch {
    fn from(v: i32) -> Self {
        Self(v)
    }
}
impl From<Epoch> for i32 {
    fn from(v: Epoch) -> Self {
        v.0
    }
}

impl From<String> for TopicName {
    fn from(v: String) -> Self {
        Self(v)
    }
}
impl From<&str> for TopicName {
    fn from(v: &str) -> Self {
        Self(v.to_string())
    }
}

impl From<String> for GroupId {
    fn from(v: String) -> Self {
        Self(v)
    }
}
impl From<&str> for GroupId {
    fn from(v: &str) -> Self {
        Self(v.to_string())
    }
}

impl From<String> for MemberId {
    fn from(v: String) -> Self {
        Self(v)
    }
}

// ─── 常量 ────────────────────────────────────────────────────────────

/// 无效偏移量哨兵值
pub const INVALID_OFFSET: Offset = Offset(-1);

/// 最早偏移量特殊值 (用于 ListOffsets)
pub const EARLIEST_OFFSET: Offset = Offset(-2);

/// 最新偏移量特殊值 (用于 ListOffsets)
pub const LATEST_OFFSET: Offset = Offset(-1);

/// 最大消息大小 (默认 1MB)
pub const MAX_MESSAGE_SIZE: usize = 1_048_576;

/// Kafka 协议帧长度前缀大小
pub const FRAME_LENGTH_PREFIX_SIZE: usize = 4;

/// RecordBatch v2 头部固定部分大小 (不含 records)
/// base_offset(8) + batch_length(4) + partition_leader_epoch(4) + magic(1) + crc(4)
/// + attributes(2) + last_offset_delta(4) + base_timestamp(8) + max_timestamp(8)
/// + producer_id(8) + producer_epoch(2) + base_sequence(4) + records_count(4) = 61
pub const RECORDBATCH_HEADER_FIXED_SIZE: usize = 61;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_roundtrip() {
        for key in [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::Metadata,
            ApiKey::ApiVersions,
            ApiKey::SaslHandshake,
            ApiKey::SaslAuthenticate,
            ApiKey::Vote,
        ] {
            let v = key as i16;
            assert_eq!(ApiKey::from_i16(v), Some(key));
        }
        assert_eq!(ApiKey::from_i16(9999), None);
    }

    #[test]
    fn test_newtype_conversions() {
        let broker = BrokerId::from(42);
        assert_eq!(i32::from(broker), 42);

        let offset = Offset::from(100i64);
        assert_eq!(i64::from(offset), 100);

        let topic = TopicName::from("test-topic");
        assert_eq!(topic.0, "test-topic");
    }
}
