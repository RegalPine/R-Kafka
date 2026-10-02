//! rk-storage: R-Kafka 存储引擎
//!
//! Append-Only CommitLog + 稀疏索引 + Crash Recovery + Retention。
//! Phase 1 使用标准文件 I/O，Phase 2 引入 io_uring Direct I/O。

pub mod commitlog;
pub mod segment;
pub mod index;
pub mod log_io;
pub mod recovery;
pub mod retention;
pub mod flush;
pub mod uring_io;
pub mod compaction;
pub mod tiered_storage;

// Re-exports
pub use commitlog::CommitLog;
pub use segment::{LogSegment, SegmentState};
pub use index::{OffsetIndex, TimeIndex, OffsetIndexEntry, TimeIndexEntry};
pub use flush::{FlushMode, FlushPolicy};
pub use recovery::{RecoveryResult, recover_partition};
pub use retention::{RetentionConfig, RetentionResult, apply_retention};
pub use uring_io::{IoEngine, IoStats, UringWriter, create_writer, best_available_engine};
pub use compaction::{
    CleanupPolicy, CompactionConfig, CompactionResult,
    KeyValueRecord, KeyIndex, KeyPosition, LogCleaner,
};
pub use tiered_storage::{
    RemoteStorage, RemoteSegmentHandle, RemoteStorageStats,
    RemoteLogManifest, RemoteSegmentMeta, RemoteLogManifestSummary,
    TieredStorageConfig, TieredStorageManager, TieredStorageSummary,
    MigrationTask, MigrationState,
    LocalFsRemoteStorage,
};
