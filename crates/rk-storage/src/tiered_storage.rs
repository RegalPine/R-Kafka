//! Tiered Storage — 冷热分离存储
//!
//! 将 sealed segment 异步迁移到远程存储 (本地文件系统 mock)，实现存储分层:
//!
//! ```text
//! 存储分层架构:
//!
//! ┌─────────────────────────────────────────────┐
//! │           TieredStorageManager              │
//! │                                              │
//! │  ┌──────────────┐   ┌────────────────────┐  │
//! │  │  Local Tier   │   │   Remote Tier       │  │
//! │  │  (hot data)   │   │   (cold data)       │  │
//! │  │               │   │                     │  │
//! │  │  Active Seg   │   │  RemoteLogManifest  │  │
//! │  │  Sealed Segs  │   │  ┌───┐ ┌───┐ ┌───┐ │  │
//! │  │  (recent)     │   │  │S0 │ │S1 │ │S2 │ │  │
//! │  └──────────────┘   │  └───┘ └───┘ └───┘ │  │
//! │                      │  RemoteStorage      │  │
//! │                      │  (local fs mock)    │  │
//! │                      └────────────────────┘  │
//! └─────────────────────────────────────────────┘
//!
//! 迁移流程:
//! 1. Segment sealed → 标记为迁移候选
//! 2. TieredStorageManager 检查迁移条件
//! 3. RemoteStorage.upload_segment() → 上传到远程
//! 4. RemoteLogManifest 记录远程段元数据
//! 5. 本地段可选删除 (保留 or 删除)
//!
//! 读取流程:
//! 1. 先查本地 segments
//! 2. 未命中 → 查 RemoteLogManifest
//! 3. RemoteStorage.fetch_segment() → 从远程拉取
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use rk_core::error::{Result, RkError};

// ─── RemoteStorage Trait ──────────────────────────────────

/// 远程存储抽象 trait
///
/// 生产环境可对接 S3/GCS/ADLS 等对象存储。
/// 当前实现使用本地文件系统 mock。
pub trait RemoteStorage: Send + Sync {
    /// 上传 segment 数据到远程存储
    fn upload_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
        data: &[u8],
    ) -> Result<RemoteSegmentHandle>;

    /// 从远程存储下载 segment 数据
    fn fetch_segment(&self, handle: &RemoteSegmentHandle) -> Result<Vec<u8>>;

    /// 删除远程 segment
    fn delete_segment(&self, handle: &RemoteSegmentHandle) -> Result<()>;

    /// 检查 segment 是否存在
    fn segment_exists(&self, handle: &RemoteSegmentHandle) -> Result<bool>;

    /// 获取远程存储统计
    fn stats(&self) -> Result<RemoteStorageStats>;
}

/// 远程 segment 句柄 (定位远程对象)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteSegmentHandle {
    /// 存储后端类型 (e.g., "local", "s3", "gcs")
    pub backend: String,
    /// 对象 key / 路径
    pub key: String,
    /// 数据大小 (字节)
    pub size_bytes: u64,
    /// 上传时间戳 (ms)
    pub uploaded_at_ms: i64,
}

impl RemoteSegmentHandle {
    pub fn new(backend: &str, key: &str, size_bytes: u64) -> Self {
        Self {
            backend: backend.to_string(),
            key: key.to_string(),
            size_bytes,
            uploaded_at_ms: now_ms(),
        }
    }
}

/// 远程存储统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RemoteStorageStats {
    /// 总 segment 数
    pub total_segments: u64,
    /// 总字节数
    pub total_bytes: u64,
    /// 上传次数
    pub upload_count: u64,
    /// 下载次数
    pub fetch_count: u64,
    /// 删除次数
    pub delete_count: u64,
}

// ─── LocalFsRemoteStorage ─────────────────────────────────

/// 基于本地文件系统的 RemoteStorage mock 实现
///
/// 将 "远程" 数据存储到本地目录，用于测试和开发。
pub struct LocalFsRemoteStorage {
    base_dir: PathBuf,
    upload_count: AtomicU64,
    fetch_count: AtomicU64,
    delete_count: AtomicU64,
}

impl LocalFsRemoteStorage {
    pub fn new(base_dir: impl Into<PathBuf>) -> Result<Self> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir).map_err(|e| {
            RkError::Io(std::io::Error::other(format!("Failed to create remote storage dir: {}", e)))
        })?;
        info!(?base_dir, "LocalFsRemoteStorage initialized");
        Ok(Self {
            base_dir,
            upload_count: AtomicU64::new(0),
            fetch_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
        })
    }

    /// 构建 segment 的远程存储路径
    fn segment_path(&self, topic: &str, partition: u32, base_offset: u64) -> PathBuf {
        self.base_dir
            .join(topic)
            .join(format!("{}", partition))
            .join(format!("{:020}.log", base_offset))
    }

    /// 从 handle 解析路径
    fn handle_path(&self, handle: &RemoteSegmentHandle) -> PathBuf {
        self.base_dir.join(&handle.key)
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl RemoteStorage for LocalFsRemoteStorage {
    fn upload_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
        data: &[u8],
    ) -> Result<RemoteSegmentHandle> {
        let path = self.segment_path(topic, partition, base_offset);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                RkError::Io(std::io::Error::other(format!("Failed to create dir: {}", e)))
            })?;
        }
        std::fs::write(&path, data).map_err(|e| {
            RkError::Io(std::io::Error::other(format!("Failed to write segment: {}", e)))
        })?;

        let relative_key = path
            .strip_prefix(&self.base_dir)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();

        let size = data.len() as u64;
        let handle = RemoteSegmentHandle {
            backend: "local".to_string(),
            key: relative_key.clone(),
            size_bytes: size,
            uploaded_at_ms: now_ms(),
        };

        self.upload_count.fetch_add(1, Ordering::Relaxed);
        debug!(key = %relative_key, size = data.len(), "Segment uploaded to remote");
        Ok(handle)
    }

    fn fetch_segment(&self, handle: &RemoteSegmentHandle) -> Result<Vec<u8>> {
        let path = self.handle_path(handle);
        let data = std::fs::read(&path).map_err(|e| {
            RkError::Io(std::io::Error::other(format!("Failed to read remote segment: {}", e)))
        })?;
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        debug!(key = %handle.key, size = data.len(), "Segment fetched from remote");
        Ok(data)
    }

    fn delete_segment(&self, handle: &RemoteSegmentHandle) -> Result<()> {
        let path = self.handle_path(handle);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| {
                RkError::Io(std::io::Error::other(format!("Failed to delete remote segment: {}", e)))
            })?;
        }
        self.delete_count.fetch_add(1, Ordering::Relaxed);
        debug!(key = %handle.key, "Remote segment deleted");
        Ok(())
    }

    fn segment_exists(&self, handle: &RemoteSegmentHandle) -> Result<bool> {
        let path = self.handle_path(handle);
        Ok(path.exists())
    }

    fn stats(&self) -> Result<RemoteStorageStats> {
        let mut total_segments = 0u64;
        let mut total_bytes = 0u64;

        // Walk directory tree to count
        if self.base_dir.exists() {
            if let Ok(entries) = walk_dir_count(&self.base_dir) {
                total_segments = entries.0;
                total_bytes = entries.1;
            }
        }

        Ok(RemoteStorageStats {
            total_segments,
            total_bytes,
            upload_count: self.upload_count.load(Ordering::Relaxed),
            fetch_count: self.fetch_count.load(Ordering::Relaxed),
            delete_count: self.delete_count.load(Ordering::Relaxed),
        })
    }
}

/// 递归统计目录中的文件数和总字节数
fn walk_dir_count(dir: &Path) -> std::io::Result<(u64, u64)> {
    let mut count = 0u64;
    let mut bytes = 0u64;
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let (sub_count, sub_bytes) = walk_dir_count(&path)?;
                count += sub_count;
                bytes += sub_bytes;
            } else {
                count += 1;
                bytes += entry.metadata()?.len();
            }
        }
    }
    Ok((count, bytes))
}

// ─── RemoteLogManifest ────────────────────────────────────

/// 远程日志段元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSegmentMeta {
    /// base offset
    pub base_offset: u64,
    /// 最大 offset (含)
    pub max_offset: u64,
    /// segment 大小 (字节)
    pub size_bytes: u64,
    /// 最大时间戳
    pub max_timestamp_ms: i64,
    /// 远程存储句柄
    pub handle: RemoteSegmentHandle,
    /// 迁移时间戳 (ms)
    pub migrated_at_ms: i64,
    /// Topic 名称
    pub topic: String,
    /// Partition ID
    pub partition: u32,
}

/// 远程日志清单 — 跟踪已迁移到远程存储的 segment
///
/// 类似 Kafka 的 remote log manifest log (RLMM)。
#[derive(Debug, Clone)]
pub struct RemoteLogManifest {
    /// topic-partition → 远程 segment 列表 (按 base_offset 排序)
    segments: BTreeMap<(String, u32), Vec<RemoteSegmentMeta>>,
    /// 总远程字节数
    total_remote_bytes: u64,
}

impl RemoteLogManifest {
    pub fn new() -> Self {
        Self {
            segments: BTreeMap::new(),
            total_remote_bytes: 0,
        }
    }

    /// 添加远程 segment 记录
    pub fn add_segment(&mut self, meta: RemoteSegmentMeta) {
        let key = (meta.topic.clone(), meta.partition);
        let segments = self.segments.entry(key).or_default();

        // 按 base_offset 有序插入
        let pos = segments
            .binary_search_by(|s| s.base_offset.cmp(&meta.base_offset))
            .unwrap_or_else(|e| e);
        self.total_remote_bytes += meta.size_bytes;
        segments.insert(pos, meta);
    }

    /// 移除远程 segment 记录
    pub fn remove_segment(
        &mut self,
        topic: &str,
        partition: u32,
        base_offset: u64,
    ) -> Option<RemoteSegmentMeta> {
        let key = (topic.to_string(), partition);
        if let Some(segments) = self.segments.get_mut(&key) {
            if let Ok(idx) = segments.binary_search_by(|s| s.base_offset.cmp(&base_offset)) {
                let removed = segments.remove(idx);
                self.total_remote_bytes -= removed.size_bytes;
                if segments.is_empty() {
                    self.segments.remove(&key);
                }
                return Some(removed);
            }
        }
        None
    }

    /// 查找包含指定 offset 的远程 segment
    pub fn find_segment(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Option<&RemoteSegmentMeta> {
        let key = (topic.to_string(), partition);
        self.segments.get(&key).and_then(|segments| {
            // 找到 base_offset <= offset 的最后一个 segment
            segments
                .iter()
                .rev()
                .find(|s| s.base_offset <= offset && offset <= s.max_offset)
        })
    }

    /// 获取指定 topic-partition 的所有远程 segments
    pub fn get_segments(&self, topic: &str, partition: u32) -> &[RemoteSegmentMeta] {
        let key = (topic.to_string(), partition);
        self.segments.get(&key).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// 获取指定 topic-partition 的远程段数量
    pub fn segment_count(&self, topic: &str, partition: u32) -> usize {
        let key = (topic.to_string(), partition);
        self.segments.get(&key).map(|v| v.len()).unwrap_or(0)
    }

    /// 总远程 segment 数
    pub fn total_segments(&self) -> usize {
        self.segments.values().map(|v| v.len()).sum()
    }

    /// 总远程字节数
    pub fn total_bytes(&self) -> u64 {
        self.total_remote_bytes
    }

    /// 所有 topic-partition 列表
    pub fn topic_partitions(&self) -> Vec<(String, u32)> {
        self.segments.keys().cloned().collect()
    }

    /// 清单摘要
    pub fn summary(&self) -> RemoteLogManifestSummary {
        let tp_count = self.segments.len();
        let total_segs = self.total_segments();
        RemoteLogManifestSummary {
            topic_partition_count: tp_count,
            total_segments: total_segs as u64,
            total_bytes: self.total_remote_bytes,
        }
    }
}

impl Default for RemoteLogManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// 清单摘要
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RemoteLogManifestSummary {
    pub topic_partition_count: usize,
    pub total_segments: u64,
    pub total_bytes: u64,
}

// ─── TieredStorageConfig ──────────────────────────────────

/// 分层存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredStorageConfig {
    /// 是否启用分层存储
    pub enabled: bool,
    /// 远程存储目录 (LocalFs mock)
    pub remote_storage_dir: String,
    /// Segment sealed 后延迟迁移时间 (毫秒)
    pub migration_delay_ms: u64,
    /// 迁移后是否删除本地副本
    pub delete_local_after_migration: bool,
    /// 最大远程 segment 数 (0 = 无限)
    pub max_remote_segments: usize,
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            remote_storage_dir: "/tmp/rk-remote-storage".to_string(),
            migration_delay_ms: 60_000,
            delete_local_after_migration: false,
            max_remote_segments: 0,
        }
    }
}

// ─── MigrationTask ────────────────────────────────────────

/// 迁移任务状态
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MigrationState {
    /// 等待迁移
    Pending,
    /// 正在上传
    Uploading,
    /// 迁移完成
    Completed,
    /// 迁移失败
    Failed(String),
}

/// 迁移任务
#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub topic: String,
    pub partition: u32,
    pub base_offset: u64,
    pub state: MigrationState,
    pub created_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

// ─── TieredStorageManager ─────────────────────────────────

/// 分层存储管理器
///
/// 管理 sealed segment 到远程存储的迁移。
pub struct TieredStorageManager {
    config: TieredStorageConfig,
    storage: Arc<dyn RemoteStorage>,
    manifest: std::sync::Mutex<RemoteLogManifest>,
    pending_migrations: std::sync::Mutex<Vec<MigrationTask>>,
    migrated_count: AtomicU64,
    failed_count: AtomicU64,
}

impl TieredStorageManager {
    /// 创建分层存储管理器
    pub fn new(config: TieredStorageConfig, storage: Arc<dyn RemoteStorage>) -> Self {
        info!(
            enabled = config.enabled,
            remote_dir = %config.remote_storage_dir,
            "TieredStorageManager initialized"
        );
        Self {
            config,
            storage,
            manifest: std::sync::Mutex::new(RemoteLogManifest::new()),
            pending_migrations: std::sync::Mutex::new(Vec::new()),
            migrated_count: AtomicU64::new(0),
            failed_count: AtomicU64::new(0),
        }
    }

    /// 创建使用本地文件系统的默认管理器
    pub fn with_local_storage(config: TieredStorageConfig) -> Result<Self> {
        let storage = Arc::new(LocalFsRemoteStorage::new(&config.remote_storage_dir)?);
        Ok(Self::new(config, storage))
    }

    /// 是否启用
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// 获取配置
    pub fn config(&self) -> &TieredStorageConfig {
        &self.config
    }

    /// 提交迁移任务
    pub fn submit_migration(&self, topic: &str, partition: u32, base_offset: u64) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let task = MigrationTask {
            topic: topic.to_string(),
            partition,
            base_offset,
            state: MigrationState::Pending,
            created_at_ms: now_ms(),
            completed_at_ms: None,
        };

        debug!(topic, partition, base_offset, "Migration task submitted");
        self.pending_migrations
            .lock()
            .map_err(|e| RkError::Storage(format!("pending_migrations lock poisoned: {}", e)))?
            .push(task);
        Ok(())
    }

    /// 执行迁移 — 将 segment 数据上传到远程存储
    pub fn migrate_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
        max_offset: u64,
        max_timestamp_ms: i64,
        data: &[u8],
    ) -> Result<RemoteSegmentHandle> {
        if !self.config.enabled {
            return Err(RkError::Config("Tiered storage is not enabled".to_string()));
        }

        // 更新 pending task 状态
        {
            let mut pending = self.pending_migrations
                .lock()
                .map_err(|e| RkError::Storage(format!("pending_migrations lock poisoned: {}", e)))?;
            for task in pending.iter_mut() {
                if task.topic == topic
                    && task.partition == partition
                    && task.base_offset == base_offset
                {
                    task.state = MigrationState::Uploading;
                    break;
                }
            }
        }

        // 上传到远程存储
        match self
            .storage
            .upload_segment(topic, partition, base_offset, data)
        {
            Ok(handle) => {
                // 添加到 manifest
                let meta = RemoteSegmentMeta {
                    base_offset,
                    max_offset,
                    size_bytes: data.len() as u64,
                    max_timestamp_ms,
                    handle: handle.clone(),
                    migrated_at_ms: now_ms(),
                    topic: topic.to_string(),
                    partition,
                };
                self.manifest
                    .lock()
                    .map_err(|e| RkError::Storage(format!("manifest lock poisoned: {}", e)))?
                    .add_segment(meta);

                // 更新 pending task
                {
                    let mut pending = self.pending_migrations
                        .lock()
                        .map_err(|e| RkError::Storage(format!("pending_migrations lock poisoned: {}", e)))?;
                    for task in pending.iter_mut() {
                        if task.topic == topic
                            && task.partition == partition
                            && task.base_offset == base_offset
                        {
                            task.state = MigrationState::Completed;
                            task.completed_at_ms = Some(now_ms());
                            break;
                        }
                    }
                }

                self.migrated_count.fetch_add(1, Ordering::Relaxed);
                info!(
                    topic,
                    partition,
                    base_offset,
                    size = data.len(),
                    "Segment migrated to remote storage"
                );
                Ok(handle)
            }
            Err(e) => {
                // 更新 pending task 为失败
                {
                    let mut pending = self.pending_migrations
                        .lock()
                        .map_err(|e| RkError::Storage(format!("pending_migrations lock poisoned: {}", e)))?;
                    for task in pending.iter_mut() {
                        if task.topic == topic
                            && task.partition == partition
                            && task.base_offset == base_offset
                        {
                            task.state = MigrationState::Failed(e.to_string());
                            break;
                        }
                    }
                }
                self.failed_count.fetch_add(1, Ordering::Relaxed);
                warn!(
                    topic,
                    partition,
                    base_offset,
                    error = %e,
                    "Segment migration failed"
                );
                Err(e)
            }
        }
    }

    /// 从远程存储读取 segment
    pub fn fetch_remote_segment(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<Vec<u8>> {
        let manifest = self.manifest
            .lock()
            .map_err(|e| RkError::Storage(format!("manifest lock poisoned: {}", e)))?;
        let seg_meta = manifest
            .find_segment(topic, partition, offset)
            .ok_or_else(|| {
                RkError::InvalidRequest(format!(
                    "No remote segment found for {}-{} at offset {}",
                    topic, partition, offset
                ))
            })?;

        let handle = seg_meta.handle.clone();
        drop(manifest);

        self.storage.fetch_segment(&handle)
    }

    /// 从远程存储删除 segment
    pub fn delete_remote_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
    ) -> Result<()> {
        let mut manifest = self.manifest
            .lock()
            .map_err(|e| RkError::Storage(format!("manifest lock poisoned: {}", e)))?;
        if let Some(meta) = manifest.remove_segment(topic, partition, base_offset) {
            self.storage.delete_segment(&meta.handle)?;
            info!(topic, partition, base_offset, "Remote segment deleted");
            Ok(())
        } else {
            Err(RkError::InvalidRequest(format!(
                "No remote segment for {}-{} at offset {}",
                topic, partition, base_offset
            )))
        }
    }

    /// 处理所有 pending 迁移任务 (批量执行)
    pub fn process_pending_migrations<F>(&self, data_provider: F) -> Result<usize>
    where
        F: Fn(&str, u32, u64) -> Option<(u64, i64, Vec<u8>)>,
    {
        let tasks: Vec<MigrationTask> = {
            let pending = self.pending_migrations
                .lock()
                .map_err(|e| RkError::Storage(format!("pending_migrations lock poisoned: {}", e)))?;
            pending
                .iter()
                .filter(|t| t.state == MigrationState::Pending)
                .cloned()
                .collect()
        };

        let mut migrated = 0;
        for task in &tasks {
            if let Some((max_offset, max_ts, data)) =
                data_provider(&task.topic, task.partition, task.base_offset)
            {
                match self.migrate_segment(
                    &task.topic,
                    task.partition,
                    task.base_offset,
                    max_offset,
                    max_ts,
                    &data,
                ) {
                    Ok(_) => migrated += 1,
                    Err(e) => {
                        warn!(error = %e, "Failed to migrate pending segment");
                    }
                }
            }
        }

        Ok(migrated)
    }

    /// 获取远程清单引用
    pub fn manifest(&self) -> &std::sync::Mutex<RemoteLogManifest> {
        &self.manifest
    }

    /// 获取清单快照
    pub fn manifest_snapshot(&self) -> RemoteLogManifest {
        self.manifest.lock().expect("manifest lock should not be poisoned").clone()
    }

    /// 获取 pending 迁移任务数
    pub fn pending_count(&self) -> usize {
        self.pending_migrations
            .lock()
            .expect("pending_migrations lock should not be poisoned")
            .iter()
            .filter(|t| t.state == MigrationState::Pending)
            .count()
    }

    /// 获取已迁移 segment 数
    pub fn migrated_count(&self) -> u64 {
        self.migrated_count.load(Ordering::Relaxed)
    }

    /// 获取失败数
    pub fn failed_count(&self) -> u64 {
        self.failed_count.load(Ordering::Relaxed)
    }

    /// 获取远程存储统计
    pub fn storage_stats(&self) -> Result<RemoteStorageStats> {
        self.storage.stats()
    }

    /// 管理器摘要
    pub fn summary(&self) -> TieredStorageSummary {
        let manifest_summary = self.manifest.lock().expect("manifest lock should not be poisoned").summary();
        TieredStorageSummary {
            enabled: self.config.enabled,
            manifest: manifest_summary,
            pending_migrations: self.pending_count() as u64,
            migrated_count: self.migrated_count(),
            failed_count: self.failed_count(),
        }
    }
}

/// 分层存储摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredStorageSummary {
    pub enabled: bool,
    pub manifest: RemoteLogManifestSummary,
    pub pending_migrations: u64,
    pub migrated_count: u64,
    pub failed_count: u64,
}

// ─── Helpers ──────────────────────────────────────────────

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ─── Tests ────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_config(dir: &Path) -> TieredStorageConfig {
        TieredStorageConfig {
            enabled: true,
            remote_storage_dir: dir.to_string_lossy().to_string(),
            migration_delay_ms: 0,
            delete_local_after_migration: false,
            max_remote_segments: 0,
        }
    }

    // ── LocalFsRemoteStorage ──

    #[test]
    fn test_local_fs_upload_and_fetch() {
        let tmp = TempDir::new().unwrap();
        let storage = LocalFsRemoteStorage::new(tmp.path()).unwrap();

        let data = b"hello tiered storage";
        let handle = storage.upload_segment("topic-a", 0, 100, data).unwrap();

        assert_eq!(handle.backend, "local");
        assert_eq!(handle.size_bytes, data.len() as u64);

        // Fetch
        let fetched = storage.fetch_segment(&handle).unwrap();
        assert_eq!(fetched, data);

        // Exists
        assert!(storage.segment_exists(&handle).unwrap());
    }

    #[test]
    fn test_local_fs_delete() {
        let tmp = TempDir::new().unwrap();
        let storage = LocalFsRemoteStorage::new(tmp.path()).unwrap();

        let data = b"delete me";
        let handle = storage.upload_segment("topic-b", 1, 200, data).unwrap();

        assert!(storage.segment_exists(&handle).unwrap());
        storage.delete_segment(&handle).unwrap();
        assert!(!storage.segment_exists(&handle).unwrap());
    }

    #[test]
    fn test_local_fs_stats() {
        let tmp = TempDir::new().unwrap();
        let storage = LocalFsRemoteStorage::new(tmp.path()).unwrap();

        storage.upload_segment("t1", 0, 0, b"seg0").unwrap();
        storage.upload_segment("t1", 0, 100, b"seg100").unwrap();
        storage.upload_segment("t1", 1, 0, b"p1-seg0").unwrap();

        let stats = storage.stats().unwrap();
        assert_eq!(stats.total_segments, 3);
        assert_eq!(stats.upload_count, 3);
        assert!(stats.total_bytes > 0);
    }

    #[test]
    fn test_local_fs_multiple_topics() {
        let tmp = TempDir::new().unwrap();
        let storage = LocalFsRemoteStorage::new(tmp.path()).unwrap();

        let h1 = storage
            .upload_segment("orders", 0, 0, b"orders-data")
            .unwrap();
        let h2 = storage
            .upload_segment("payments", 0, 0, b"payments-data")
            .unwrap();

        assert_ne!(h1.key, h2.key);
        assert_eq!(storage.fetch_segment(&h1).unwrap(), b"orders-data");
        assert_eq!(storage.fetch_segment(&h2).unwrap(), b"payments-data");
    }

    // ── RemoteLogManifest ──

    #[test]
    fn test_manifest_add_and_find() {
        let mut manifest = RemoteLogManifest::new();

        let handle = RemoteSegmentHandle::new("local", "t/0/00000000000000000000.log", 100);
        manifest.add_segment(RemoteSegmentMeta {
            base_offset: 0,
            max_offset: 99,
            size_bytes: 100,
            max_timestamp_ms: 1000,
            handle,
            migrated_at_ms: now_ms(),
            topic: "t".to_string(),
            partition: 0,
        });

        let handle2 = RemoteSegmentHandle::new("local", "t/0/00000000000000000100.log", 200);
        manifest.add_segment(RemoteSegmentMeta {
            base_offset: 100,
            max_offset: 299,
            size_bytes: 200,
            max_timestamp_ms: 2000,
            handle: handle2,
            migrated_at_ms: now_ms(),
            topic: "t".to_string(),
            partition: 0,
        });

        assert_eq!(manifest.total_segments(), 2);
        assert_eq!(manifest.total_bytes(), 300);

        // Find offset 50 → first segment
        let found = manifest.find_segment("t", 0, 50).unwrap();
        assert_eq!(found.base_offset, 0);

        // Find offset 150 → second segment
        let found = manifest.find_segment("t", 0, 150).unwrap();
        assert_eq!(found.base_offset, 100);

        // Find offset 300 → not found
        assert!(manifest.find_segment("t", 0, 300).is_none());
    }

    #[test]
    fn test_manifest_remove() {
        let mut manifest = RemoteLogManifest::new();

        let handle = RemoteSegmentHandle::new("local", "t/0/0.log", 50);
        manifest.add_segment(RemoteSegmentMeta {
            base_offset: 0,
            max_offset: 49,
            size_bytes: 50,
            max_timestamp_ms: 1000,
            handle,
            migrated_at_ms: now_ms(),
            topic: "t".to_string(),
            partition: 0,
        });

        assert_eq!(manifest.total_segments(), 1);
        let removed = manifest.remove_segment("t", 0, 0);
        assert!(removed.is_some());
        assert_eq!(manifest.total_segments(), 0);
        assert_eq!(manifest.total_bytes(), 0);
    }

    #[test]
    fn test_manifest_get_segments() {
        let mut manifest = RemoteLogManifest::new();

        for i in 0..3 {
            let base = i * 100;
            let handle = RemoteSegmentHandle::new("local", &format!("t/0/{:020}.log", base), 100);
            manifest.add_segment(RemoteSegmentMeta {
                base_offset: base,
                max_offset: base + 99,
                size_bytes: 100,
                max_timestamp_ms: 1000 * (i as i64 + 1),
                handle,
                migrated_at_ms: now_ms(),
                topic: "t".to_string(),
                partition: 0,
            });
        }

        let segs = manifest.get_segments("t", 0);
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[0].base_offset, 0);
        assert_eq!(segs[1].base_offset, 100);
        assert_eq!(segs[2].base_offset, 200);

        // Non-existent
        assert!(manifest.get_segments("t", 99).is_empty());
    }

    #[test]
    fn test_manifest_summary() {
        let mut manifest = RemoteLogManifest::new();

        let handle = RemoteSegmentHandle::new("local", "t/0/0.log", 50);
        manifest.add_segment(RemoteSegmentMeta {
            base_offset: 0,
            max_offset: 49,
            size_bytes: 50,
            max_timestamp_ms: 1000,
            handle,
            migrated_at_ms: now_ms(),
            topic: "t".to_string(),
            partition: 0,
        });

        let summary = manifest.summary();
        assert_eq!(summary.topic_partition_count, 1);
        assert_eq!(summary.total_segments, 1);
        assert_eq!(summary.total_bytes, 50);
    }

    // ── TieredStorageManager ──

    #[test]
    fn test_manager_migrate_segment() {
        let tmp = TempDir::new().unwrap();
        let config = make_config(tmp.path());
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        assert!(manager.is_enabled());

        let data = b"segment data for migration";
        let handle = manager
            .migrate_segment("orders", 0, 0, 99, 1000, data)
            .unwrap();

        assert_eq!(handle.size_bytes, data.len() as u64);
        assert_eq!(manager.migrated_count(), 1);
        assert_eq!(manager.failed_count(), 0);

        // Verify in manifest
        let manifest = manager.manifest_snapshot();
        assert_eq!(manifest.total_segments(), 1);
        let found = manifest.find_segment("orders", 0, 50).unwrap();
        assert_eq!(found.base_offset, 0);
    }

    #[test]
    fn test_manager_fetch_remote() {
        let tmp = TempDir::new().unwrap();
        let config = make_config(tmp.path());
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        let data = b"fetch me from remote";
        manager
            .migrate_segment("events", 1, 500, 599, 2000, data)
            .unwrap();

        let fetched = manager.fetch_remote_segment("events", 1, 550).unwrap();
        assert_eq!(fetched, data);
    }

    #[test]
    fn test_manager_fetch_not_found() {
        let tmp = TempDir::new().unwrap();
        let config = make_config(tmp.path());
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        let result = manager.fetch_remote_segment("no-topic", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_manager_delete_remote() {
        let tmp = TempDir::new().unwrap();
        let config = make_config(tmp.path());
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        manager
            .migrate_segment("del-topic", 0, 0, 99, 1000, b"data")
            .unwrap();

        let manifest = manager.manifest_snapshot();
        assert_eq!(manifest.total_segments(), 1);

        manager.delete_remote_segment("del-topic", 0, 0).unwrap();

        let manifest = manager.manifest_snapshot();
        assert_eq!(manifest.total_segments(), 0);
    }

    #[test]
    fn test_manager_disabled() {
        let tmp = TempDir::new().unwrap();
        let mut config = make_config(tmp.path());
        config.enabled = false;
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        assert!(!manager.is_enabled());

        // migrate_segment should fail when disabled
        let result = manager.migrate_segment("t", 0, 0, 99, 1000, b"data");
        assert!(result.is_err());
    }

    #[test]
    fn test_manager_submit_and_process() {
        let tmp = TempDir::new().unwrap();
        let config = make_config(tmp.path());
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        // Submit migration tasks
        manager.submit_migration("t", 0, 0).unwrap();
        manager.submit_migration("t", 0, 100).unwrap();
        manager.submit_migration("t", 0, 200).unwrap();

        assert_eq!(manager.pending_count(), 3);

        // Process with data provider
        let migrated = manager
            .process_pending_migrations(|_topic, _partition, base_offset| {
                let max_offset = base_offset + 99;
                let data = format!("data-{}", base_offset).into_bytes();
                Some((max_offset, now_ms(), data))
            })
            .unwrap();

        assert_eq!(migrated, 3);
        assert_eq!(manager.migrated_count(), 3);
        assert_eq!(manager.pending_count(), 0);

        let manifest = manager.manifest_snapshot();
        assert_eq!(manifest.total_segments(), 3);
    }

    #[test]
    fn test_manager_summary() {
        let tmp = TempDir::new().unwrap();
        let config = make_config(tmp.path());
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        manager
            .migrate_segment("t", 0, 0, 99, 1000, b"seg-0")
            .unwrap();
        manager
            .migrate_segment("t", 0, 100, 199, 2000, b"seg-100")
            .unwrap();

        let summary = manager.summary();
        assert!(summary.enabled);
        assert_eq!(summary.migrated_count, 2);
        assert_eq!(summary.manifest.total_segments, 2);
        assert_eq!(summary.pending_migrations, 0);
    }

    #[test]
    fn test_manager_storage_stats() {
        let tmp = TempDir::new().unwrap();
        let config = make_config(tmp.path());
        let manager = TieredStorageManager::with_local_storage(config).unwrap();

        manager
            .migrate_segment("t", 0, 0, 99, 1000, b"hello")
            .unwrap();

        let stats = manager.storage_stats().unwrap();
        assert_eq!(stats.total_segments, 1);
        assert_eq!(stats.upload_count, 1);
    }

    // ── RemoteSegmentHandle ──

    #[test]
    fn test_remote_segment_handle() {
        let handle = RemoteSegmentHandle::new("s3", "bucket/key.log", 1024);
        assert_eq!(handle.backend, "s3");
        assert_eq!(handle.key, "bucket/key.log");
        assert_eq!(handle.size_bytes, 1024);
        assert!(handle.uploaded_at_ms > 0);
    }

    #[test]
    fn test_handle_serialization() {
        let handle = RemoteSegmentHandle::new("local", "t/0/0.log", 100);
        let json = serde_json::to_string(&handle).unwrap();
        let deserialized: RemoteSegmentHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(handle, deserialized);
    }

    // ── TieredStorageConfig ──

    #[test]
    fn test_config_default() {
        let config = TieredStorageConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.migration_delay_ms, 60_000);
        assert!(!config.delete_local_after_migration);
        assert_eq!(config.max_remote_segments, 0);
    }
}
