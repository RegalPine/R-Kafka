//! rk-storage: R-Kafka 存储引擎
//!
//! Append-Only CommitLog + 稀疏索引 + Crash Recovery + Retention。
//! Phase 1 使用标准文件 I/O，Phase 2 引入 io_uring Direct I/O。

pub mod commitlog;
pub mod compaction;
pub mod flush;
pub mod index;
pub mod log_io;
pub mod recovery;
pub mod retention;
pub mod segment;
pub mod tiered_storage;
pub mod uring_io;

// Re-exports
pub use commitlog::CommitLog;
pub use compaction::{
    CleanupPolicy, CompactionConfig, CompactionResult, KeyIndex, KeyPosition, KeyValueRecord,
    LogCleaner,
};
pub use flush::{FlushMode, FlushPolicy};
pub use index::{OffsetIndex, OffsetIndexEntry, TimeIndex, TimeIndexEntry};
pub use recovery::{recover_partition, RecoveryResult};
pub use retention::{apply_retention, RetentionConfig, RetentionResult};
pub use segment::{LogSegment, SegmentState};
pub use tiered_storage::{
    LocalFsRemoteStorage, MigrationState, MigrationTask, RemoteLogManifest,
    RemoteLogManifestSummary, RemoteSegmentHandle, RemoteSegmentMeta, RemoteStorage,
    RemoteStorageStats, TieredStorageConfig, TieredStorageManager, TieredStorageSummary,
};
pub use uring_io::{best_available_engine, create_writer, IoEngine, IoStats, UringWriter};
