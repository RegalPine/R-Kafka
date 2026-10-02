//! rk-broker: R-Kafka Broker 核心逻辑
//!
//! Produce/Fetch/Metadata/ListOffsets/ApiVersions/CreateTopics/DeleteTopics
//! FindCoordinator/DescribeConfigs/AlterConfigs/OffsetCommit/OffsetFetch
//! JoinGroup/SyncGroup/Heartbeat/LeaveGroup/DescribeGroups/ListGroups
//! SaslHandshake/SaslAuthenticate/InitProducerId/OffsetForLeaderEpoch
//! AddPartitionsToTxn/EndTxn/DeleteRecords/ElectLeaders/CreatePartitions
//! OffsetDelete/DescribeProducers/ListTransactions/DescribeCluster
//! AlterPartitionReassignments/ListPartitionReassignments
//! IncrementalAlterConfigs/PersistentOffsetManager
//! DescribeTopics/BrokerMetrics
//! DescribeQuorum/Prometheus
//! ConfigReloader/Banner
//! 处理、Partition 管理、偏移量管理、消费者组管理、SASL 认证、
//! 幂等生产者/事务管理、请求路由。

pub mod produce;
pub mod fetch;
pub mod partition;
pub mod metadata_handler;
pub mod list_offsets_handler;
pub mod api_versions_handler;
pub mod create_topics_handler;
pub mod delete_topics_handler;
pub mod find_coordinator_handler;
pub mod describe_configs_handler;
pub mod alter_configs_handler;
pub mod offset_manager;
pub mod offset_commit_handler;
pub mod offset_fetch_handler;
pub mod group_manager;
pub mod join_group_handler;
pub mod sync_group_handler;
pub mod heartbeat_handler;
pub mod leave_group_handler;
pub mod describe_groups_handler;
pub mod list_groups_handler;
pub mod sasl_authenticator;
pub mod sasl_handshake_handler;
pub mod sasl_authenticate_handler;
pub mod connection_session;
pub mod init_producer_id_handler;
pub mod offset_for_leader_epoch_handler;
pub mod producer_state_manager;
pub mod add_partitions_to_txn_handler;
pub mod end_txn_handler;
pub mod delete_records_handler;
pub mod elect_leaders_handler;
pub mod create_partitions_handler;
pub mod offset_delete_handler;
pub mod describe_producers_handler;
pub mod list_transactions_handler;
pub mod describe_cluster_handler;
pub mod alter_partition_reassignments_handler;
pub mod list_partition_reassignments_handler;
pub mod incremental_alter_configs_handler;
pub mod persistent_offset_manager;
pub mod describe_topics_handler;
pub mod describe_quorum_handler;
pub mod vote_handler;
pub mod begin_quorum_epoch_handler;
pub mod end_quorum_epoch_handler;
pub mod leader_and_isr_handler;
pub mod stop_replica_handler;
pub mod update_metadata_handler;
pub mod controlled_shutdown_handler;
pub mod broker_registration_handler;
pub mod broker_heartbeat_handler;
pub mod metrics;
pub mod config_reloader;
pub mod batch_accumulator;
pub mod router;
pub mod transaction_coordinator;

// Re-exports
pub use partition::{
    PartitionManager, PartitionManagerRecoveryResult,
    PartitionKey, PartitionInfo, TopicMetadata,
};
pub use produce::ProduceHandler;
pub use fetch::FetchHandler;
pub use metadata_handler::MetadataHandler;
pub use list_offsets_handler::ListOffsetsHandler;
pub use api_versions_handler::ApiVersionsHandler;
pub use create_topics_handler::CreateTopicsHandler;
pub use delete_topics_handler::DeleteTopicsHandler;
pub use find_coordinator_handler::FindCoordinatorHandler;
pub use describe_configs_handler::DescribeConfigsHandler;
pub use alter_configs_handler::AlterConfigsHandler;
pub use offset_manager::OffsetManager;
pub use offset_commit_handler::OffsetCommitHandler;
pub use offset_fetch_handler::OffsetFetchHandler;
pub use group_manager::GroupManager;
pub use join_group_handler::JoinGroupHandler;
pub use sync_group_handler::SyncGroupHandler;
pub use heartbeat_handler::HeartbeatHandler;
pub use leave_group_handler::LeaveGroupHandler;
pub use describe_groups_handler::DescribeGroupsHandler;
pub use list_groups_handler::ListGroupsHandler;
pub use sasl_authenticator::SaslAuthenticator;
pub use sasl_handshake_handler::SaslHandshakeHandler;
pub use sasl_authenticate_handler::SaslAuthenticateHandler;
pub use connection_session::ConnectionSession;
pub use init_producer_id_handler::InitProducerIdHandler;
pub use offset_for_leader_epoch_handler::OffsetForLeaderEpochHandler;
pub use producer_state_manager::ProducerStateManager;
pub use add_partitions_to_txn_handler::AddPartitionsToTxnHandler;
pub use end_txn_handler::EndTxnHandler;
pub use delete_records_handler::DeleteRecordsHandler;
pub use elect_leaders_handler::ElectLeadersHandler;
pub use create_partitions_handler::CreatePartitionsHandler;
pub use offset_delete_handler::OffsetDeleteHandler;
pub use describe_producers_handler::DescribeProducersHandler;
pub use list_transactions_handler::ListTransactionsHandler;
pub use describe_cluster_handler::DescribeClusterHandler;
pub use alter_partition_reassignments_handler::AlterPartitionReassignmentsHandler;
pub use list_partition_reassignments_handler::ListPartitionReassignmentsHandler;
pub use incremental_alter_configs_handler::IncrementalAlterConfigsHandler;
pub use persistent_offset_manager::PersistentOffsetManager;
pub use describe_topics_handler::DescribeTopicsHandler;
pub use describe_quorum_handler::DescribeQuorumHandler;
pub use vote_handler::VoteHandler;
pub use begin_quorum_epoch_handler::BeginQuorumEpochHandler;
pub use end_quorum_epoch_handler::EndQuorumEpochHandler;
pub use leader_and_isr_handler::LeaderAndIsrHandler;
pub use stop_replica_handler::StopReplicaHandler;
pub use update_metadata_handler::UpdateMetadataHandler;
pub use controlled_shutdown_handler::ControlledShutdownHandler;
pub use broker_registration_handler::BrokerRegistrationHandler;
pub use broker_heartbeat_handler::BrokerHeartbeatHandler;
pub use metrics::{BrokerMetrics, MetricsSnapshot};
pub use config_reloader::{ConfigReloader, ConfigReloadReport};
pub use batch_accumulator::{AccumulatorConfig, BatchAccumulatorManager, BatchReady};
pub use router::BrokerRouter;
pub use transaction_coordinator::{
    TransactionCoordinator, TxnCoordinatorState, TransactionMetadata,
    TransactionLog, TxnLogEntry, TxnLogEntryType,
};
