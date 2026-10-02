//! rk-protocol: Kafka Wire Protocol 编解码引擎
//!
//! 实现 Kafka 协议的全部数据类型、请求/响应头、API 路由、
//! RecordBatch v2 格式编解码和标准错误码。

pub mod api;
pub mod api_versions;
pub mod apis;
pub mod codec;
pub mod error_codes;
pub mod legacy_message;
pub mod record;
pub mod request;
pub mod response;
pub mod types;

// Re-exports
pub use api::{ApiRouter, RequestContext, RequestHandler};
pub use api_versions::{
    ApiVersion, ApiVersionsRequest, ApiVersionsResponse, SUPPORTED_API_VERSIONS,
};
pub use apis::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnRequestTopic,
    AddPartitionsToTxnRequestTransaction, AddPartitionsToTxnResponse,
    AddPartitionsToTxnResponsePartition, AddPartitionsToTxnResponseTopic,
    AddPartitionsToTxnResponseTransaction, AlterConfigsRequest, AlterConfigsRequestConfig,
    AlterConfigsRequestResource, AlterConfigsResponse, AlterConfigsResponseResource,
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsRequestPartition,
    AlterPartitionReassignmentsRequestTopic, AlterPartitionReassignmentsResponse,
    AlterPartitionReassignmentsResponsePartition, AlterPartitionReassignmentsResponseTopic,
    CreatePartitionsRequest, CreatePartitionsRequestTopic, CreatePartitionsResponse,
    CreatePartitionsResponseResult, CreateTopicsRequest, CreateTopicsRequestAssignment,
    CreateTopicsRequestConfig, CreateTopicsRequestTopic, CreateTopicsResponse,
    CreateTopicsResponseTopic, DeleteRecordsRequest, DeleteRecordsRequestPartition,
    DeleteRecordsRequestTopic, DeleteRecordsResponse, DeleteRecordsResponsePartition,
    DeleteRecordsResponseTopic, DeleteTopicsRequest, DeleteTopicsResponse,
    DeleteTopicsResponseTopic, DescribeClusterRequest, DescribeClusterResponse,
    DescribeClusterResponseBroker, DescribeConfigsConfigSynonym, DescribeConfigsRequest,
    DescribeConfigsRequestResource, DescribeConfigsResponse, DescribeConfigsResponseConfig,
    DescribeConfigsResponseResource, DescribeGroupsRequest, DescribeGroupsResponse,
    DescribeGroupsResponseGroup, DescribeGroupsResponseMember, DescribeProducersRequest,
    DescribeProducersRequestTopic, DescribeProducersResponse, DescribeProducersResponsePartition,
    DescribeProducersResponseProducer, DescribeProducersResponseTopic, DescribeQuorumRequest,
    DescribeQuorumRequestPartition, DescribeQuorumRequestTopic, DescribeQuorumResponse,
    DescribeQuorumResponsePartition, DescribeQuorumResponseTopic, DescribeQuorumResponseVoter,
    DescribeTopicsRequest, DescribeTopicsRequestTopic, DescribeTopicsResponse,
    DescribeTopicsResponsePartition, DescribeTopicsResponseTopic, ElectLeadersRequest,
    ElectLeadersRequestTopic, ElectLeadersResponse, ElectLeadersResponsePartition,
    ElectLeadersResponseTopic, EndTxnRequest, EndTxnResponse, FetchRequest, FetchRequestPartition,
    FetchRequestTopic, FetchResponse, FetchResponsePartition, FetchResponseTopic,
    FindCoordinatorRequest, FindCoordinatorResponse, FindCoordinatorResponseCoordinator,
    HeartbeatRequest, HeartbeatResponse, IncrementalAlterConfigsOp, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsRequestConfig, IncrementalAlterConfigsRequestResource,
    IncrementalAlterConfigsResponse, IncrementalAlterConfigsResponseResource,
    InitProducerIdRequest, InitProducerIdResponse, JoinGroupRequest, JoinGroupRequestProtocol,
    JoinGroupResponse, JoinGroupResponseMember, LeaveGroupRequest, LeaveGroupRequestMember,
    LeaveGroupResponse, LeaveGroupResponseMember, ListGroupsRequest, ListGroupsResponse,
    ListGroupsResponseGroup, ListOffsetsRequest, ListOffsetsRequestPartition,
    ListOffsetsRequestTopic, ListOffsetsResponse, ListOffsetsResponsePartition,
    ListOffsetsResponseTopic, ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsRequestTopic, ListPartitionReassignmentsResponse,
    ListPartitionReassignmentsResponsePartition, ListPartitionReassignmentsResponseTopic,
    ListTransactionsRequest, ListTransactionsResponse, ListTransactionsResponseState,
    MetadataBroker, MetadataPartition, MetadataRequest, MetadataResponse, MetadataTopic,
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    OffsetDeleteRequest, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchResponse,
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochRequestPartition,
    OffsetForLeaderEpochRequestTopic, OffsetForLeaderEpochResponse,
    OffsetForLeaderEpochResponsePartition, OffsetForLeaderEpochResponseTopic, ProduceRequest,
    ProduceRequestPartition, ProduceRequestTopic, ProduceResponse, ProduceResponsePartition,
    ProduceResponseTopic, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, SyncGroupRequest, SyncGroupRequestAssignment, SyncGroupResponse,
};
pub use codec::{KafkaRequestDecoder, KafkaResponseEncoder};
pub use error_codes::KafkaErrorCode;
pub use legacy_message::{
    convert_to_v2_batch, decode_message_set, detect_and_decode, DecodedMessages, LegacyMessage,
    LegacyMessageSet,
};
pub use record::{CompressionType, RecordBatchHeader, TimestampType, RECORDBATCH_MAGIC};
pub use request::RequestHeader;
pub use response::ResponseHeader;
pub use types::{KafkaReader, KafkaWriter, TaggedField};
