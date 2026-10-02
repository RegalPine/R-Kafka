//! Broker Request Router
//!
//! 将协议层 ApiRouter 与 Broker 层各 Handler 桥接:
//! 1. 解码 Request Body → 具体请求类型
//! 2. 调用对应 Handler 处理
//! 3. 编码 Response → 字节流返回

use std::sync::Arc;

use bytes::BytesMut;
use rk_core::error::{Result, RkError};
use rk_protocol::api_versions::ApiVersionsRequest;
use rk_protocol::apis::add_partitions_to_txn::AddPartitionsToTxnRequest;
use rk_protocol::apis::alter_configs::AlterConfigsRequest;
use rk_protocol::apis::alter_partition_reassignments::AlterPartitionReassignmentsRequest;
use rk_protocol::apis::begin_quorum_epoch::BeginQuorumEpochRequest;
use rk_protocol::apis::broker_heartbeat::BrokerHeartbeatRequest;
use rk_protocol::apis::broker_registration::BrokerRegistrationRequest;
use rk_protocol::apis::controlled_shutdown::ControlledShutdownRequest;
use rk_protocol::apis::create_partitions::CreatePartitionsRequest;
use rk_protocol::apis::create_topics::CreateTopicsRequest;
use rk_protocol::apis::delete_records::DeleteRecordsRequest;
use rk_protocol::apis::delete_topics::DeleteTopicsRequest;
use rk_protocol::apis::describe_cluster::DescribeClusterRequest;
use rk_protocol::apis::describe_configs::DescribeConfigsRequest;
use rk_protocol::apis::describe_groups::DescribeGroupsRequest;
use rk_protocol::apis::describe_producers::DescribeProducersRequest;
use rk_protocol::apis::describe_quorum::DescribeQuorumRequest;
use rk_protocol::apis::describe_topics::DescribeTopicsRequest;
use rk_protocol::apis::elect_leaders::ElectLeadersRequest;
use rk_protocol::apis::end_quorum_epoch::EndQuorumEpochRequest;
use rk_protocol::apis::end_txn::EndTxnRequest;
use rk_protocol::apis::fetch::FetchRequest;
use rk_protocol::apis::find_coordinator::FindCoordinatorRequest;
use rk_protocol::apis::heartbeat::HeartbeatRequest;
use rk_protocol::apis::incremental_alter_configs::IncrementalAlterConfigsRequest;
use rk_protocol::apis::init_producer_id::InitProducerIdRequest;
use rk_protocol::apis::join_group::JoinGroupRequest;
use rk_protocol::apis::leader_and_isr::LeaderAndIsrRequest;
use rk_protocol::apis::leave_group::LeaveGroupRequest;
use rk_protocol::apis::list_groups::ListGroupsRequest;
use rk_protocol::apis::list_offsets::ListOffsetsRequest;
use rk_protocol::apis::list_partition_reassignments::ListPartitionReassignmentsRequest;
use rk_protocol::apis::list_transactions::ListTransactionsRequest;
use rk_protocol::apis::metadata::MetadataRequest;
use rk_protocol::apis::offset_commit::OffsetCommitRequest;
use rk_protocol::apis::offset_delete::OffsetDeleteRequest;
use rk_protocol::apis::offset_fetch::OffsetFetchRequest;
use rk_protocol::apis::offset_for_leader_epoch::OffsetForLeaderEpochRequest;
use rk_protocol::apis::produce::ProduceRequest;
use rk_protocol::apis::sasl_authenticate::SaslAuthenticateRequest;
use rk_protocol::apis::sasl_handshake::SaslHandshakeRequest;
use rk_protocol::apis::stop_replica::StopReplicaRequest;
use rk_protocol::apis::sync_group::SyncGroupRequest;
use rk_protocol::apis::update_metadata::UpdateMetadataRequest;
use rk_protocol::apis::vote::VoteRequest;
use rk_protocol::codec::{encode_response, KafkaRequestDecoder};
use rk_protocol::types::{KafkaReader, KafkaWriter};
use rk_protocol::{RequestContext, ResponseHeader};
use tracing::error;

use crate::add_partitions_to_txn_handler::AddPartitionsToTxnHandler;
use crate::alter_configs_handler::AlterConfigsHandler;
use crate::alter_partition_reassignments_handler::AlterPartitionReassignmentsHandler;
use crate::api_versions_handler::ApiVersionsHandler;
use crate::begin_quorum_epoch_handler::BeginQuorumEpochHandler;
use crate::broker_heartbeat_handler::BrokerHeartbeatHandler;
use crate::broker_registration_handler::BrokerRegistrationHandler;
use crate::controlled_shutdown_handler::ControlledShutdownHandler;
use crate::create_partitions_handler::CreatePartitionsHandler;
use crate::create_topics_handler::CreateTopicsHandler;
use crate::delete_records_handler::DeleteRecordsHandler;
use crate::delete_topics_handler::DeleteTopicsHandler;
use crate::describe_cluster_handler::DescribeClusterHandler;
use crate::describe_configs_handler::DescribeConfigsHandler;
use crate::describe_groups_handler::DescribeGroupsHandler;
use crate::describe_producers_handler::DescribeProducersHandler;
use crate::describe_quorum_handler::DescribeQuorumHandler;
use crate::describe_topics_handler::DescribeTopicsHandler;
use crate::elect_leaders_handler::ElectLeadersHandler;
use crate::end_quorum_epoch_handler::EndQuorumEpochHandler;
use crate::end_txn_handler::EndTxnHandler;
use crate::fetch::FetchHandler;
use crate::find_coordinator_handler::FindCoordinatorHandler;
use crate::group_manager::GroupManager;
use crate::heartbeat_handler::HeartbeatHandler;
use crate::incremental_alter_configs_handler::IncrementalAlterConfigsHandler;
use crate::init_producer_id_handler::InitProducerIdHandler;
use crate::join_group_handler::JoinGroupHandler;
use crate::leader_and_isr_handler::LeaderAndIsrHandler;
use crate::leave_group_handler::LeaveGroupHandler;
use crate::list_groups_handler::ListGroupsHandler;
use crate::list_offsets_handler::ListOffsetsHandler;
use crate::list_partition_reassignments_handler::ListPartitionReassignmentsHandler;
use crate::list_transactions_handler::ListTransactionsHandler;
use crate::metadata_handler::MetadataHandler;
use crate::offset_commit_handler::OffsetCommitHandler;
use crate::offset_delete_handler::OffsetDeleteHandler;
use crate::offset_fetch_handler::OffsetFetchHandler;
use crate::offset_for_leader_epoch_handler::OffsetForLeaderEpochHandler;
use crate::offset_manager::OffsetManager;
use crate::partition::PartitionManager;
use crate::produce::ProduceHandler;
use crate::producer_state_manager::ProducerStateManager;
use crate::sasl_authenticate_handler::SaslAuthenticateHandler;
use crate::sasl_authenticator::SaslAuthenticator;
use crate::sasl_handshake_handler::SaslHandshakeHandler;
use crate::stop_replica_handler::StopReplicaHandler;
use crate::sync_group_handler::SyncGroupHandler;
use crate::update_metadata_handler::UpdateMetadataHandler;
use crate::vote_handler::VoteHandler;

/// Broker 请求路由器: 聚合所有 Handler
pub struct BrokerRouter {
    pub(crate) produce_handler: ProduceHandler,
    pub(crate) fetch_handler: FetchHandler,
    pub(crate) metadata_handler: MetadataHandler,
    pub(crate) list_offsets_handler: ListOffsetsHandler,
    pub(crate) api_versions_handler: ApiVersionsHandler,
    pub(crate) create_topics_handler: CreateTopicsHandler,
    pub(crate) delete_topics_handler: DeleteTopicsHandler,
    pub(crate) find_coordinator_handler: FindCoordinatorHandler,
    pub(crate) describe_configs_handler: DescribeConfigsHandler,
    pub(crate) alter_configs_handler: AlterConfigsHandler,
    pub(crate) offset_commit_handler: OffsetCommitHandler,
    pub(crate) offset_fetch_handler: OffsetFetchHandler,
    pub(crate) join_group_handler: JoinGroupHandler,
    pub(crate) sync_group_handler: SyncGroupHandler,
    pub(crate) heartbeat_handler: HeartbeatHandler,
    pub(crate) leave_group_handler: LeaveGroupHandler,
    pub(crate) describe_groups_handler: DescribeGroupsHandler,
    pub(crate) list_groups_handler: ListGroupsHandler,
    pub(crate) sasl_handshake_handler: SaslHandshakeHandler,
    pub(crate) sasl_authenticate_handler: SaslAuthenticateHandler,
    pub(crate) init_producer_id_handler: InitProducerIdHandler,
    pub(crate) offset_for_leader_epoch_handler: OffsetForLeaderEpochHandler,
    pub(crate) add_partitions_to_txn_handler: AddPartitionsToTxnHandler,
    pub(crate) end_txn_handler: EndTxnHandler,
    pub(crate) delete_records_handler: DeleteRecordsHandler,
    pub(crate) elect_leaders_handler: ElectLeadersHandler,
    pub(crate) offset_delete_handler: OffsetDeleteHandler,
    pub(crate) describe_producers_handler: DescribeProducersHandler,
    pub(crate) list_transactions_handler: ListTransactionsHandler,
    pub(crate) describe_cluster_handler: DescribeClusterHandler,
    pub(crate) alter_partition_reassignments_handler: AlterPartitionReassignmentsHandler,
    pub(crate) list_partition_reassignments_handler: ListPartitionReassignmentsHandler,
    pub(crate) create_partitions_handler: CreatePartitionsHandler,
    pub(crate) incremental_alter_configs_handler: IncrementalAlterConfigsHandler,
    pub(crate) describe_topics_handler: DescribeTopicsHandler,
    pub(crate) describe_quorum_handler: DescribeQuorumHandler,
    pub(crate) leader_and_isr_handler: LeaderAndIsrHandler,
    pub(crate) stop_replica_handler: StopReplicaHandler,
    pub(crate) update_metadata_handler: UpdateMetadataHandler,
    pub(crate) controlled_shutdown_handler: ControlledShutdownHandler,
    pub(crate) broker_registration_handler: BrokerRegistrationHandler,
    pub(crate) broker_heartbeat_handler: BrokerHeartbeatHandler,
    pub(crate) vote_handler: VoteHandler,
    pub(crate) begin_quorum_epoch_handler: BeginQuorumEpochHandler,
    pub(crate) end_quorum_epoch_handler: EndQuorumEpochHandler,
    /// 是否启用 SASL 认证
    sasl_enabled: bool,
    /// 共享的 SASL 认证器
    authenticator: Arc<SaslAuthenticator>,
    /// 是否启用 ACL 授权
    acl_enabled: bool,
    /// Broker 运行时指标
    metrics: Arc<crate::metrics::BrokerMetrics>,
}

impl BrokerRouter {
    /// 创建 BrokerRouter
    pub fn new(
        partition_manager: Arc<PartitionManager>,
        broker_id: i32,
        broker_host: String,
        broker_port: i32,
        broker_rack: Option<String>,
        cluster_id: Option<String>,
    ) -> Self {
        let offset_manager = Arc::new(OffsetManager::new(None));
        Self::with_offset_manager(
            partition_manager,
            broker_id,
            broker_host,
            broker_port,
            broker_rack,
            cluster_id,
            offset_manager,
        )
    }

    /// 创建 BrokerRouter (指定 OffsetManager)
    pub fn with_offset_manager(
        partition_manager: Arc<PartitionManager>,
        broker_id: i32,
        broker_host: String,
        broker_port: i32,
        broker_rack: Option<String>,
        cluster_id: Option<String>,
        offset_manager: Arc<OffsetManager>,
    ) -> Self {
        Self::with_sasl_config(
            partition_manager,
            broker_id,
            broker_host,
            broker_port,
            broker_rack,
            cluster_id,
            offset_manager,
            false,
            Arc::new(SaslAuthenticator::new()),
        )
    }

    /// 创建 BrokerRouter (完整配置，含 SASL)
    #[allow(clippy::too_many_arguments)]
    pub fn with_sasl_config(
        partition_manager: Arc<PartitionManager>,
        broker_id: i32,
        broker_host: String,
        broker_port: i32,
        broker_rack: Option<String>,
        cluster_id: Option<String>,
        offset_manager: Arc<OffsetManager>,
        sasl_enabled: bool,
        authenticator: Arc<SaslAuthenticator>,
    ) -> Self {
        let group_manager = Arc::new(GroupManager::new());
        let producer_state_manager = Arc::new(ProducerStateManager::new());
        Self {
            produce_handler: ProduceHandler::new(partition_manager.clone()),
            fetch_handler: FetchHandler::new(partition_manager.clone()),
            metadata_handler: MetadataHandler::new(
                partition_manager.clone(),
                broker_id,
                broker_host.clone(),
                broker_port,
                broker_rack,
                cluster_id.clone(),
            ),
            list_offsets_handler: ListOffsetsHandler::new(partition_manager.clone()),
            api_versions_handler: ApiVersionsHandler::new(),
            create_topics_handler: CreateTopicsHandler::new(partition_manager.clone()),
            delete_topics_handler: DeleteTopicsHandler::new(partition_manager.clone()),
            find_coordinator_handler: FindCoordinatorHandler::new(
                broker_id,
                broker_host.clone(),
                broker_port,
            ),
            describe_configs_handler: DescribeConfigsHandler::new(partition_manager.clone()),
            alter_configs_handler: AlterConfigsHandler::new(partition_manager.clone()),
            offset_commit_handler: OffsetCommitHandler::new(offset_manager.clone()),
            offset_fetch_handler: OffsetFetchHandler::new(offset_manager.clone()),
            join_group_handler: JoinGroupHandler::new(group_manager.clone()),
            sync_group_handler: SyncGroupHandler::new(group_manager.clone()),
            heartbeat_handler: HeartbeatHandler::new(group_manager.clone()),
            leave_group_handler: LeaveGroupHandler::new(group_manager.clone()),
            describe_groups_handler: DescribeGroupsHandler::new(group_manager.clone()),
            list_groups_handler: ListGroupsHandler::new(group_manager),
            sasl_handshake_handler: SaslHandshakeHandler::new(authenticator.clone()),
            sasl_authenticate_handler: SaslAuthenticateHandler::new(authenticator.clone()),
            init_producer_id_handler: InitProducerIdHandler::new(),
            offset_for_leader_epoch_handler: OffsetForLeaderEpochHandler::new(
                partition_manager.clone(),
            ),
            add_partitions_to_txn_handler: AddPartitionsToTxnHandler::new(
                producer_state_manager.clone(),
            ),
            end_txn_handler: EndTxnHandler::new(producer_state_manager.clone()),
            delete_records_handler: DeleteRecordsHandler::new(partition_manager.clone()),
            elect_leaders_handler: ElectLeadersHandler::new(partition_manager.clone()),
            offset_delete_handler: OffsetDeleteHandler::new(offset_manager.clone()),
            describe_producers_handler: DescribeProducersHandler::new(
                partition_manager.clone(),
                producer_state_manager.clone(),
            ),
            list_transactions_handler: ListTransactionsHandler::new(producer_state_manager.clone()),
            describe_cluster_handler: DescribeClusterHandler::new(
                broker_id,
                broker_host.clone(),
                broker_port,
                cluster_id
                    .clone()
                    .unwrap_or_else(|| "r-kafka-cluster".to_string()),
            ),
            alter_partition_reassignments_handler: AlterPartitionReassignmentsHandler::new(),
            list_partition_reassignments_handler: ListPartitionReassignmentsHandler::new(),
            incremental_alter_configs_handler: IncrementalAlterConfigsHandler::new(
                partition_manager.clone(),
            ),
            describe_topics_handler: DescribeTopicsHandler::new(
                broker_id,
                partition_manager.clone(),
            ),
            describe_quorum_handler: DescribeQuorumHandler::new(
                broker_id,
                partition_manager.clone(),
            ),
            create_partitions_handler: CreatePartitionsHandler::new(partition_manager),
            leader_and_isr_handler: LeaderAndIsrHandler::new(broker_id),
            stop_replica_handler: StopReplicaHandler::new(broker_id),
            update_metadata_handler: UpdateMetadataHandler::new(broker_id),
            controlled_shutdown_handler: ControlledShutdownHandler::new(broker_id),
            broker_registration_handler: BrokerRegistrationHandler::new(broker_id),
            broker_heartbeat_handler: BrokerHeartbeatHandler::new(broker_id),
            vote_handler: VoteHandler::new(broker_id),
            begin_quorum_epoch_handler: BeginQuorumEpochHandler::new(broker_id),
            end_quorum_epoch_handler: EndQuorumEpochHandler::new(broker_id),
            sasl_enabled,
            authenticator,
            acl_enabled: false,
            metrics: Arc::new(crate::metrics::BrokerMetrics::new()),
        }
    }

    /// 是否启用 SASL 认证
    pub fn is_sasl_enabled(&self) -> bool {
        self.sasl_enabled
    }

    /// 是否启用 ACL 授权
    pub fn is_acl_enabled(&self) -> bool {
        self.acl_enabled
    }

    /// 设置 ACL 启用状态
    pub fn set_acl_enabled(&mut self, enabled: bool) {
        self.acl_enabled = enabled;
    }

    /// 检查 API 请求的 ACL 授权
    ///
    /// 使用 `rk_security::api_permissions` 确定 API 所需的权限，
    /// 并通过 `rk_security::AclEngine` 执行检查。
    ///
    /// 返回 `Ok(())` 表示允许，`Err` 表示拒绝。
    pub fn check_authorization(
        &self,
        api_key: i16,
        principal: &str,
        client_host: &str,
        resource_name: Option<&str>,
    ) -> Result<()> {
        // ACL 未启用 → 跳过
        if !self.acl_enabled {
            return Ok(());
        }

        // 获取 API 权限需求
        let permission = match rk_security::api_permission(api_key) {
            Some(p) => p,
            None => return Ok(()), // 未知 API 不检查
        };

        // 认证前 API → 跳过
        if permission.is_pre_auth {
            return Ok(());
        }

        // 使用 AclEngine 检查
        let engine = rk_security::AclEngine::new(true);
        // 注意: 实际使用时需要共享 AclEngine 实例
        // 这里只是提供接口框架

        let rname = resource_name.unwrap_or("*");
        if engine.authorize(
            principal,
            client_host,
            &permission.resource_type,
            rname,
            &permission.operation,
        ) {
            Ok(())
        } else {
            Err(RkError::Protocol(format!(
                "ACL denied: {} {} on {} {}",
                principal, permission.operation, permission.resource_type, rname
            )))
        }
    }

    /// 获取 Broker 运行时指标快照
    pub fn metrics_snapshot(&self) -> crate::metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// 获取共享的 SASL 认证器
    pub fn authenticator(&self) -> &Arc<SaslAuthenticator> {
        &self.authenticator
    }

    /// 从原始帧字节中提取 api_key (不解析完整请求)
    ///
    /// 帧格式: [api_key(i16)] [api_version(i16)] [correlation_id(i32)] ...
    /// Flexible v2+: [api_key(i16)] [api_version(i16)] [correlation_id(i32)] [client_id(compact_string)] [tagged_fields]
    pub fn extract_api_key(frame_bytes: &[u8]) -> Option<i16> {
        if frame_bytes.len() < 4 {
            return None;
        }
        let api_key = i16::from_be_bytes([frame_bytes[0], frame_bytes[1]]);
        Some(api_key)
    }

    /// 检查某个 API 在认证前是否允许调用
    ///
    /// 认证前只允许: ApiVersions(18), SaslHandshake(17), SaslAuthenticate(36)
    pub fn is_pre_auth_api(api_key: i16) -> bool {
        matches!(api_key, 17 | 18 | 36)
    }

    /// 处理原始请求字节，返回完整响应帧 (ResponseHeader + ResponseBody)
    ///
    /// `body_bytes` 为 Request Header 之后的请求体
    pub fn handle_request(&self, ctx: &RequestContext, body_bytes: &[u8]) -> Result<Vec<u8>> {
        // 记录请求指标
        self.metrics.record_request(ctx.api_key);

        // 1. 编码响应体
        let response_body = match self.encode_response_body(ctx, body_bytes) {
            Ok(body) => body,
            Err(e) => {
                self.metrics.record_error();
                return Err(e);
            }
        };

        // 记录响应指标
        self.metrics.record_response();

        // 2. 判断是否为 Flexible 版本 (决定 ResponseHeader 格式)
        let is_flexible = rk_core::ApiKey::from_i16(ctx.api_key)
            .map(|k| k.is_flexible(ctx.api_version))
            .unwrap_or(false);

        // 3. 组装完整响应: ResponseHeader + ResponseBody
        let mut result = BytesMut::with_capacity(4 + response_body.len());
        let mut writer = KafkaWriter::new(&mut result);
        let header = ResponseHeader::new(ctx.correlation_id);
        header.encode(&mut writer, is_flexible);
        result.extend_from_slice(&response_body);

        Ok(result.to_vec())
    }

    /// 从完整帧字节 (含 RequestHeader) 解析并处理请求
    ///
    /// `frame_bytes` 为 4 字节长度前缀之前的完整帧数据
    pub fn handle_frame(&self, frame_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut reader = KafkaReader::new(frame_bytes);
        let req_header = rk_protocol::RequestHeader::decode(&mut reader)?;

        let ctx = RequestContext {
            correlation_id: req_header.correlation_id,
            client_id: req_header.client_id.clone(),
            api_key: req_header.api_key,
            api_version: req_header.api_version,
        };

        // reader 当前位置即请求体起始位置
        let body_bytes = &frame_bytes[reader.position()..];
        self.handle_request(&ctx, body_bytes)
    }

    /// 编码响应体 (不含 ResponseHeader)
    fn encode_response_body(&self, ctx: &RequestContext, body_bytes: &[u8]) -> Result<Vec<u8>> {
        match ctx.api_key {
            // Produce (0)
            0 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = ProduceRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.produce_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // Fetch (1)
            1 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = FetchRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.fetch_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // ListOffsets (2)
            2 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = ListOffsetsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.list_offsets_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // Metadata (3)
            3 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = MetadataRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.metadata_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // OffsetCommit (8)
            8 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = OffsetCommitRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .offset_commit_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // OffsetFetch (9)
            9 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = OffsetFetchRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.offset_fetch_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // FindCoordinator (10)
            10 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = FindCoordinatorRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .find_coordinator_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // JoinGroup (11)
            11 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = JoinGroupRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.join_group_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // Heartbeat (12)
            12 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = HeartbeatRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.heartbeat_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // LeaveGroup (13)
            13 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = LeaveGroupRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.leave_group_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // SyncGroup (14)
            14 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = SyncGroupRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.sync_group_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DescribeGroups (15)
            15 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DescribeGroupsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .describe_groups_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // ListGroups (16)
            16 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = ListGroupsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.list_groups_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // SaslHandshake (17)
            17 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = SaslHandshakeRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .sasl_handshake_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // ApiVersions (18)
            18 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = ApiVersionsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.api_versions_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // CreateTopics (19)
            19 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = CreateTopicsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .create_topics_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DeleteTopics (20)
            20 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DeleteTopicsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .delete_topics_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DeleteRecords (21)
            21 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DeleteRecordsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .delete_records_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // InitProducerId (22)
            22 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = InitProducerIdRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .init_producer_id_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // OffsetForLeaderEpoch (23)
            23 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = OffsetForLeaderEpochRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .offset_for_leader_epoch_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // AddPartitionsToTxn (24)
            24 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = AddPartitionsToTxnRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .add_partitions_to_txn_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // EndTxn (26)
            26 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = EndTxnRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.end_txn_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DescribeConfigs (32)
            32 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DescribeConfigsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .describe_configs_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // AlterConfigs (33)
            33 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = AlterConfigsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .alter_configs_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // SaslAuthenticate (36)
            36 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = SaslAuthenticateRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .sasl_authenticate_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // CreatePartitions (37)
            37 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = CreatePartitionsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .create_partitions_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // ElectLeaders (43)
            43 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = ElectLeadersRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .elect_leaders_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // OffsetDelete (47)
            47 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = OffsetDeleteRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .offset_delete_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DescribeProducers (61)
            61 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DescribeProducersRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .describe_producers_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // ListTransactions (65)
            65 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = ListTransactionsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .list_transactions_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // AlterPartitionReassignments (45)
            45 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request =
                    AlterPartitionReassignmentsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .alter_partition_reassignments_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // ListPartitionReassignments (46)
            46 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request =
                    ListPartitionReassignmentsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .list_partition_reassignments_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DescribeCluster (60)
            60 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DescribeClusterRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .describe_cluster_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // IncrementalAlterConfigs (44)
            44 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = IncrementalAlterConfigsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .incremental_alter_configs_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DescribeTopics (70)
            70 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DescribeTopicsRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .describe_topics_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // Vote (51)
            51 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = VoteRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.vote_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // BeginQuorumEpoch (52)
            52 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = BeginQuorumEpochRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .begin_quorum_epoch_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // EndQuorumEpoch (53)
            53 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = EndQuorumEpochRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .end_quorum_epoch_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // DescribeQuorum (56)
            56 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = DescribeQuorumRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .describe_quorum_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // LeaderAndIsr (4)
            4 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = LeaderAndIsrRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .leader_and_isr_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // StopReplica (5)
            5 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = StopReplicaRequest::decode(&mut reader, ctx.api_version)?;
                let response = self.stop_replica_handler.handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // UpdateMetadata (6)
            6 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = UpdateMetadataRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .update_metadata_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // ControlledShutdown (7)
            7 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = ControlledShutdownRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .controlled_shutdown_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // BrokerRegistration (54)
            54 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = BrokerRegistrationRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .broker_registration_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            // BrokerHeartbeat (55)
            55 => {
                let mut reader = KafkaReader::new(body_bytes);
                let request = BrokerHeartbeatRequest::decode(&mut reader, ctx.api_version)?;
                let response = self
                    .broker_heartbeat_handler
                    .handle(request, ctx.api_version)?;
                let encoded = encode_response(&response, ctx.api_version)?;
                Ok(encoded.to_vec())
            }
            _ => {
                error!("Unsupported API key: {}", ctx.api_key);
                Err(RkError::UnsupportedApiKey(ctx.api_key))
            }
        }
    }

    /// 获取 PartitionManager 引用
    pub fn partition_manager(&self) -> &Arc<PartitionManager> {
        self.produce_handler.partition_manager()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_protocol::apis::offset_commit::*;
    use rk_protocol::apis::offset_fetch::*;
    use rk_protocol::apis::produce::*;
    use rk_protocol::error_codes::KafkaErrorCode;
    use rk_storage::log_io::build_batch_bytes;
    use tempfile::tempdir;

    fn make_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
        let records = vec![0u8; record_count as usize * 10];
        build_batch_bytes(
            base_offset,
            1,
            0,
            1000,
            2000,
            -1,
            -1,
            -1,
            &records,
            record_count,
        )
    }

    fn make_router() -> BrokerRouter {
        let dir = tempdir().unwrap();
        let pm = Arc::new(PartitionManager::new(
            dir.path().to_path_buf(),
            1_073_741_824,
            1,
        ));
        pm.get_or_create_topic("test-topic", 3);

        BrokerRouter::new(
            pm,
            1,
            "localhost".to_string(),
            9092,
            None,
            Some("test-cluster".to_string()),
        )
    }

    #[test]
    fn test_broker_router_produce_handler() {
        let router = make_router();

        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30000,
            topics: vec![ProduceRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ProduceRequestPartition {
                    index: 0,
                    record_set: make_batch(0, 5),
                }],
            }],
        };

        let response = router.produce_handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(response.topics[0].partitions[0].base_offset, 0);
    }

    #[test]
    fn test_broker_router_metadata_handler() {
        let router = make_router();

        let request = MetadataRequest {
            topics: Some(vec!["test-topic".to_string()]),
            allow_auto_topic_creation: true,
        };

        let response = router.metadata_handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name, "test-topic");
        assert_eq!(response.topics[0].partitions.len(), 3);
    }

    #[test]
    fn test_broker_router_unsupported_api() {
        let router = make_router();
        let ctx = RequestContext {
            correlation_id: 1,
            client_id: Some("test".to_string()),
            api_key: 999,
            api_version: 0,
        };
        let result = router.handle_request(&ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_broker_router_end_to_end() {
        let router = make_router();

        // 1. Produce 写入数据
        let produce_req = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 30000,
            topics: vec![ProduceRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ProduceRequestPartition {
                    index: 0,
                    record_set: make_batch(0, 5),
                }],
            }],
        };
        let produce_resp = router.produce_handler.handle(produce_req, 0).unwrap();
        assert_eq!(produce_resp.topics[0].partitions[0].base_offset, 0);

        // 2. Fetch 读取数据
        let fetch_req = rk_protocol::apis::fetch::FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_000_000,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![rk_protocol::apis::fetch::FetchRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![rk_protocol::apis::fetch::FetchRequestPartition {
                    index: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 0,
                    log_start_offset: -1,
                    max_bytes: 1_000_000,
                }],
            }],
            rack_id: None,
        };
        let fetch_resp = router.fetch_handler.handle(fetch_req, 0).unwrap();
        assert!(!fetch_resp.topics[0].partitions[0].record_set.is_empty());

        // 3. ListOffsets 查询 LATEST
        let list_req = rk_protocol::apis::list_offsets::ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![rk_protocol::apis::list_offsets::ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![
                    rk_protocol::apis::list_offsets::ListOffsetsRequestPartition {
                        index: 0,
                        timestamp: -1,
                    },
                ],
            }],
        };
        let list_resp = router.list_offsets_handler.handle(list_req, 1).unwrap();
        assert_eq!(list_resp.topics[0].partitions[0].offset, 5); // LEO = 5
    }

    #[test]
    fn test_broker_router_api_versions() {
        let router = make_router();
        let request = ApiVersionsRequest {
            client_software_name: None,
            client_software_version: None,
        };
        let response = router.api_versions_handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert!(!response.api_versions.is_empty());
    }

    #[test]
    fn test_broker_router_create_topics() {
        let router = make_router();
        let request = CreateTopicsRequest {
            topics: vec![rk_protocol::apis::create_topics::CreateTopicsRequestTopic {
                name: "new-topic".to_string(),
                num_partitions: 2,
                replication_factor: 1,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30000,
            validate_only: false,
        };
        let response = router.create_topics_handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_broker_router_delete_topics() {
        let router = make_router();
        router
            .partition_manager()
            .get_or_create_topic("to-delete", 1);

        let request = DeleteTopicsRequest {
            topic_names: vec!["to-delete".to_string()],
            timeout_ms: 30000,
        };
        let response = router.delete_topics_handler.handle(request, 0).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_broker_router_find_coordinator() {
        let router = make_router();
        let request = FindCoordinatorRequest {
            key: "my-group".to_string(),
            key_type: 0,
            coordinator_keys: None,
        };
        let response = router.find_coordinator_handler.handle(request, 0).unwrap();
        assert_eq!(response.error_code, KafkaErrorCode::None);
        assert_eq!(response.node_id, 1);
    }

    #[test]
    fn test_broker_router_describe_configs() {
        let router = make_router();
        let request = DescribeConfigsRequest {
            resources: vec![
                rk_protocol::apis::describe_configs::DescribeConfigsRequestResource {
                    resource_type: 2,
                    resource_name: "test-topic".to_string(),
                    config_names: Some(vec!["retention.ms".to_string()]),
                },
            ],
            include_synonyms: false,
        };
        let response = router.describe_configs_handler.handle(request, 0).unwrap();
        assert_eq!(response.resources.len(), 1);
        assert_eq!(response.resources[0].error_code, KafkaErrorCode::None);
        assert!(!response.resources[0].configs.is_empty());
    }

    #[test]
    fn test_broker_router_alter_configs() {
        let router = make_router();
        let request = AlterConfigsRequest {
            resources: vec![
                rk_protocol::apis::alter_configs::AlterConfigsRequestResource {
                    resource_type: 2,
                    resource_name: "test-topic".to_string(),
                    configs: vec![
                        rk_protocol::apis::alter_configs::AlterConfigsRequestConfig {
                            name: "retention.ms".to_string(),
                            value: Some("3600000".to_string()),
                        },
                    ],
                },
            ],
            validate_only: false,
        };
        let response = router.alter_configs_handler.handle(request, 0).unwrap();
        assert_eq!(response.responses[0].error_code, KafkaErrorCode::None);
    }

    #[test]
    fn test_broker_router_offset_commit_fetch() {
        let router = make_router();

        // 1. OffsetCommit: 提交偏移量
        let commit_req = OffsetCommitRequest {
            group_id: "test-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![
                    OffsetCommitRequestPartition {
                        index: 0,
                        committed_offset: 42,
                        committed_leader_epoch: -1,
                        commit_timestamp: -1,
                        metadata: None,
                    },
                    OffsetCommitRequestPartition {
                        index: 1,
                        committed_offset: 100,
                        committed_leader_epoch: -1,
                        commit_timestamp: -1,
                        metadata: Some("meta".to_string()),
                    },
                ],
            }],
        };
        let commit_resp = router.offset_commit_handler.handle(commit_req, 0).unwrap();
        assert_eq!(
            commit_resp.topics[0].partitions[0].error_code,
            KafkaErrorCode::None
        );
        assert_eq!(
            commit_resp.topics[0].partitions[1].error_code,
            KafkaErrorCode::None
        );

        // 2. OffsetFetch: 查询指定 partition
        let fetch_req = OffsetFetchRequest {
            group_id: "test-group".to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "test-topic".to_string(),
                partition_indexes: vec![0, 1],
            }]),
            groups: None,
            require_stable: false,
        };
        let fetch_resp = router.offset_fetch_handler.handle(fetch_req, 0).unwrap();
        assert_eq!(fetch_resp.topics.len(), 1);
        assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, 42);
        assert_eq!(fetch_resp.topics[0].partitions[1].committed_offset, 100);
        assert_eq!(
            fetch_resp.topics[0].partitions[1].metadata,
            Some("meta".to_string())
        );

        // 3. OffsetFetch: 查询不存在的 partition → offset=-1
        let fetch_req2 = OffsetFetchRequest {
            group_id: "test-group".to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "test-topic".to_string(),
                partition_indexes: vec![2], // 未提交
            }]),
            groups: None,
            require_stable: false,
        };
        let fetch_resp2 = router.offset_fetch_handler.handle(fetch_req2, 0).unwrap();
        assert_eq!(fetch_resp2.topics[0].partitions[0].committed_offset, -1);
    }
}
