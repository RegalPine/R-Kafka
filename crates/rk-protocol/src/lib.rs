//! rk-protocol: Kafka Wire Protocol 编解码引擎
//!
//! 实现 Kafka 协议的全部数据类型、请求/响应头、API 路由、
//! RecordBatch v2 格式编解码和标准错误码。

pub mod types;
pub mod codec;
pub mod request;
pub mod response;
pub mod api;
pub mod api_versions;
pub mod record;
pub mod error_codes;
pub mod legacy_message;
pub mod apis;

// Re-exports
pub use types::{KafkaReader, KafkaWriter, TaggedField};
pub use codec::{KafkaRequestDecoder, KafkaResponseEncoder};
pub use request::RequestHeader;
pub use response::ResponseHeader;
pub use api::{ApiRouter, RequestContext, RequestHandler};
pub use api_versions::{ApiVersionsRequest, ApiVersionsResponse, ApiVersion, SUPPORTED_API_VERSIONS};
pub use error_codes::KafkaErrorCode;
pub use record::{RecordBatchHeader, CompressionType, TimestampType, RECORDBATCH_MAGIC};
pub use legacy_message::{
    LegacyMessage, LegacyMessageSet, DecodedMessages,
    decode_message_set, convert_to_v2_batch, detect_and_decode,
};
pub use apis::{
    ProduceRequest, ProduceResponse, ProduceRequestTopic, ProduceRequestPartition,
    ProduceResponseTopic, ProduceResponsePartition,
    FetchRequest, FetchResponse, FetchRequestTopic, FetchRequestPartition,
    FetchResponseTopic, FetchResponsePartition,
    MetadataRequest, MetadataResponse, MetadataBroker, MetadataPartition, MetadataTopic,
    ListOffsetsRequest, ListOffsetsResponse, ListOffsetsRequestTopic, ListOffsetsRequestPartition,
    ListOffsetsResponseTopic, ListOffsetsResponsePartition,
    FindCoordinatorRequest, FindCoordinatorResponse, FindCoordinatorResponseCoordinator,
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetCommitRequestTopic, OffsetCommitRequestPartition,
    OffsetCommitResponseTopic, OffsetCommitResponsePartition,
    OffsetFetchRequest, OffsetFetchResponse,
    OffsetFetchRequestTopic, OffsetFetchRequestGroup,
    OffsetFetchResponseTopic, OffsetFetchResponsePartition, OffsetFetchResponseGroup,
    JoinGroupRequest, JoinGroupResponse, JoinGroupRequestProtocol, JoinGroupResponseMember,
    SyncGroupRequest, SyncGroupResponse, SyncGroupRequestAssignment,
    HeartbeatRequest, HeartbeatResponse,
    LeaveGroupRequest, LeaveGroupResponse,
    LeaveGroupRequestMember, LeaveGroupResponseMember,
    DescribeGroupsRequest, DescribeGroupsResponse,
    DescribeGroupsResponseGroup, DescribeGroupsResponseMember,
    ListGroupsRequest, ListGroupsResponse, ListGroupsResponseGroup,
    SaslHandshakeRequest, SaslHandshakeResponse,
    SaslAuthenticateRequest, SaslAuthenticateResponse,
    CreateTopicsRequest, CreateTopicsResponse, CreateTopicsRequestTopic,
    CreateTopicsRequestAssignment, CreateTopicsRequestConfig, CreateTopicsResponseTopic,
    DeleteTopicsRequest, DeleteTopicsResponse, DeleteTopicsResponseTopic,
    DeleteRecordsRequest, DeleteRecordsResponse,
    DeleteRecordsRequestTopic, DeleteRecordsRequestPartition,
    DeleteRecordsResponseTopic, DeleteRecordsResponsePartition,
    InitProducerIdRequest, InitProducerIdResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
    OffsetForLeaderEpochRequestTopic, OffsetForLeaderEpochRequestPartition,
    OffsetForLeaderEpochResponseTopic, OffsetForLeaderEpochResponsePartition,
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse,
    AddPartitionsToTxnRequestTopic, AddPartitionsToTxnRequestTransaction,
    AddPartitionsToTxnResponseTopic, AddPartitionsToTxnResponsePartition,
    AddPartitionsToTxnResponseTransaction,
    EndTxnRequest, EndTxnResponse,
    ElectLeadersRequest, ElectLeadersResponse,
    ElectLeadersRequestTopic, ElectLeadersResponseTopic, ElectLeadersResponsePartition,
    CreatePartitionsRequest, CreatePartitionsResponse,
    CreatePartitionsRequestTopic, CreatePartitionsResponseResult,
    DescribeConfigsRequest, DescribeConfigsResponse,
    DescribeConfigsRequestResource, DescribeConfigsResponseResource,
    DescribeConfigsResponseConfig, DescribeConfigsConfigSynonym,
    AlterConfigsRequest, AlterConfigsResponse,
    AlterConfigsRequestResource, AlterConfigsRequestConfig, AlterConfigsResponseResource,
    DescribeProducersRequest, DescribeProducersResponse,
    DescribeProducersRequestTopic, DescribeProducersResponseTopic,
    DescribeProducersResponsePartition, DescribeProducersResponseProducer,
    ListTransactionsRequest, ListTransactionsResponse, ListTransactionsResponseState,
    OffsetDeleteRequest, OffsetDeleteResponse,
    OffsetDeleteRequestTopic, OffsetDeleteRequestPartition,
    OffsetDeleteResponseTopic, OffsetDeleteResponsePartition,
    DescribeClusterRequest, DescribeClusterResponse, DescribeClusterResponseBroker,
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse,
    AlterPartitionReassignmentsRequestTopic, AlterPartitionReassignmentsRequestPartition,
    AlterPartitionReassignmentsResponseTopic, AlterPartitionReassignmentsResponsePartition,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    ListPartitionReassignmentsRequestTopic, ListPartitionReassignmentsResponseTopic,
    ListPartitionReassignmentsResponsePartition,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    IncrementalAlterConfigsRequestResource, IncrementalAlterConfigsRequestConfig,
    IncrementalAlterConfigsResponseResource, IncrementalAlterConfigsOp,
    DescribeTopicsRequest, DescribeTopicsResponse,
    DescribeTopicsRequestTopic, DescribeTopicsResponseTopic,
    DescribeTopicsResponsePartition,
    DescribeQuorumRequest, DescribeQuorumResponse,
    DescribeQuorumRequestTopic, DescribeQuorumRequestPartition,
    DescribeQuorumResponseTopic, DescribeQuorumResponsePartition,
    DescribeQuorumResponseVoter,
};
