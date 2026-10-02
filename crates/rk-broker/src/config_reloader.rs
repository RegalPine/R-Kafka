//! Config Reloader
//!
//! 支持运行时热加载配置 (SIGHUP 信号触发)。
//! 通过 Arc<RwLock<BrokerConfig>> 共享配置，各组件按需读取。
//! 仅部分配置支持热加载 (retention, network limits, flush 等)。
//! 不可变配置 (broker.id, broker.port, data_dir) 热加载时忽略并告警。

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use rk_core::error::Result;
use rk_core::BrokerConfig;
use tracing::{info, warn};

/// 配置热加载器
pub struct ConfigReloader {
    /// 配置文件路径
    config_path: PathBuf,
    /// 共享的可变配置
    shared_config: Arc<RwLock<BrokerConfig>>,
}

impl ConfigReloader {
    /// 创建配置热加载器
    pub fn new(config_path: PathBuf, shared_config: Arc<RwLock<BrokerConfig>>) -> Self {
        Self { config_path, shared_config }
    }

    /// 从文件重新加载配置
    ///
    /// 对比新旧配置，仅更新支持热加载的字段。
    /// 不可变字段的变更会被记录为 warning。
    pub fn reload(&self) -> Result<ConfigReloadReport> {
        info!(path = %self.config_path.display(), "Reloading configuration from file");

        let new_config = BrokerConfig::from_file(
            &self.config_path.to_string_lossy(),
        )?;

        let mut report = ConfigReloadReport::default();

        // 读取当前配置进行比较
        let (immutable_id, immutable_port, immutable_data_dir) = {
            let current = self.shared_config.read().map_err(|e| {
                rk_core::RkError::Config(format!("Config lock poisoned: {e}"))
            })?;

            // 检查不可变字段变更
            if new_config.broker.id != current.broker.id {
                warn!(
                    old = current.broker.id,
                    new = new_config.broker.id,
                    "broker.id changed but is immutable at runtime, ignoring"
                );
                report.immutable_changes.push("broker.id".to_string());
            }
            if new_config.broker.port != current.broker.port {
                warn!(
                    old = current.broker.port,
                    new = new_config.broker.port,
                    "broker.port changed but is immutable at runtime, ignoring"
                );
                report.immutable_changes.push("broker.port".to_string());
            }
            if new_config.storage.data_dir != current.storage.data_dir {
                warn!(
                    old = %current.storage.data_dir,
                    new = %new_config.storage.data_dir,
                    "storage.data_dir changed but is immutable at runtime, ignoring"
                );
                report.immutable_changes.push("storage.data_dir".to_string());
            }

            // 热加载 retention 配置
            if new_config.retention.max_bytes != current.retention.max_bytes {
                info!(
                    old = current.retention.max_bytes,
                    new = new_config.retention.max_bytes,
                    "retention.max_bytes updated"
                );
                report.updated_fields.push("retention.max_bytes".to_string());
            }
            if new_config.retention.max_ms != current.retention.max_ms {
                info!(
                    old = current.retention.max_ms,
                    new = new_config.retention.max_ms,
                    "retention.max_ms updated"
                );
                report.updated_fields.push("retention.max_ms".to_string());
            }
            if new_config.retention.compaction_enabled != current.retention.compaction_enabled {
                info!(
                    old = current.retention.compaction_enabled,
                    new = new_config.retention.compaction_enabled,
                    "retention.compaction_enabled updated"
                );
                report.updated_fields.push("retention.compaction_enabled".to_string());
            }

            // 热加载 network 配置
            if new_config.network.max_connections != current.network.max_connections {
                info!(
                    old = current.network.max_connections,
                    new = new_config.network.max_connections,
                    "network.max_connections updated"
                );
                report.updated_fields.push("network.max_connections".to_string());
            }
            if new_config.network.max_request_size != current.network.max_request_size {
                info!(
                    old = current.network.max_request_size,
                    new = new_config.network.max_request_size,
                    "network.max_request_size updated"
                );
                report.updated_fields.push("network.max_request_size".to_string());
            }
            if new_config.network.max_pending_bytes != current.network.max_pending_bytes {
                info!(
                    old = current.network.max_pending_bytes,
                    new = new_config.network.max_pending_bytes,
                    "network.max_pending_bytes updated"
                );
                report.updated_fields.push("network.max_pending_bytes".to_string());
            }

            // 热加载 storage flush 配置
            if new_config.storage.flush_interval_ms != current.storage.flush_interval_ms {
                info!(
                    old = current.storage.flush_interval_ms,
                    new = new_config.storage.flush_interval_ms,
                    "storage.flush_interval_ms updated"
                );
                report.updated_fields.push("storage.flush_interval_ms".to_string());
            }

            // 热加载 observability 配置
            if new_config.observability.metrics_enabled != current.observability.metrics_enabled {
                info!(
                    old = current.observability.metrics_enabled,
                    new = new_config.observability.metrics_enabled,
                    "observability.metrics_enabled updated"
                );
                report.updated_fields.push("observability.metrics_enabled".to_string());
            }

            // 保存不可变字段
            (current.broker.id, current.broker.port, current.storage.data_dir.clone())
        }; // read guard 在此 drop

        // 应用新配置 (保留不可变字段)
        let mut updated_config = new_config;
        updated_config.broker.id = immutable_id;
        updated_config.broker.port = immutable_port;
        updated_config.storage.data_dir = immutable_data_dir;

        // 写入新配置
        {
            let mut current = self.shared_config.write().map_err(|e| {
                rk_core::RkError::Config(format!("Config lock poisoned: {e}"))
            })?;
            *current = updated_config;
        }

        report.success = true;
        info!(
            updated = report.updated_fields.len(),
            immutable_ignored = report.immutable_changes.len(),
            "Configuration reload complete"
        );

        Ok(report)
    }
}

/// 配置热加载报告
#[derive(Debug, Default)]
pub struct ConfigReloadReport {
    /// 是否成功
    pub success: bool,
    /// 已更新的字段
    pub updated_fields: Vec<String>,
    /// 被忽略的不可变字段变更
    pub immutable_changes: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_reload_retention_change() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("test.toml");

        // 写入初始配置
        let initial = r#"
            [broker]
            id = 1
            port = 9092

            [retention]
            max_bytes = 1000000
            max_ms = 60000
        "#;
        std::fs::write(&config_path, initial).unwrap();

        let config = BrokerConfig::from_file(&config_path.to_string_lossy()).unwrap();
        let shared = Arc::new(RwLock::new(config));
        let reloader = ConfigReloader::new(config_path.clone(), shared.clone());

        // 修改配置文件
        let updated = r#"
            [broker]
            id = 1
            port = 9092

            [retention]
            max_bytes = 2000000
            max_ms = 120000
        "#;
        std::fs::write(&config_path, updated).unwrap();

        // 热加载
        let report = reloader.reload().unwrap();
        assert!(report.success);
        assert!(report.updated_fields.contains(&"retention.max_bytes".to_string()));
        assert!(report.updated_fields.contains(&"retention.max_ms".to_string()));
        assert!(report.immutable_changes.is_empty());

        // 验证配置已更新
        let current = shared.read().unwrap();
        assert_eq!(current.retention.max_bytes, 2_000_000);
        assert_eq!(current.retention.max_ms, 120_000);
    }

    #[test]
    fn test_config_reload_ignores_immutable_changes() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("test_imm.toml");

        let initial = r#"
            [broker]
            id = 1
            port = 9092
        "#;
        std::fs::write(&config_path, initial).unwrap();

        let config = BrokerConfig::from_file(&config_path.to_string_lossy()).unwrap();
        let shared = Arc::new(RwLock::new(config));
        let reloader = ConfigReloader::new(config_path.clone(), shared.clone());

        // 尝试修改不可变字段
        let updated = r#"
            [broker]
            id = 99
            port = 19092
        "#;
        std::fs::write(&config_path, updated).unwrap();

        let report = reloader.reload().unwrap();
        assert!(report.success);
        assert!(report.immutable_changes.contains(&"broker.id".to_string()));
        assert!(report.immutable_changes.contains(&"broker.port".to_string()));

        // 不可变字段保持不变
        let current = shared.read().unwrap();
        assert_eq!(current.broker.id, 1);
        assert_eq!(current.broker.port, 9092);
    }

    #[test]
    fn test_config_reload_no_changes() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("test_noop.toml");

        let config_str = r#"
            [broker]
            id = 1
            port = 9092
        "#;
        std::fs::write(&config_path, config_str).unwrap();

        let config = BrokerConfig::from_file(&config_path.to_string_lossy()).unwrap();
        let shared = Arc::new(RwLock::new(config));
        let reloader = ConfigReloader::new(config_path.clone(), shared.clone());

        let report = reloader.reload().unwrap();
        assert!(report.success);
        assert!(report.updated_fields.is_empty());
        assert!(report.immutable_changes.is_empty());
    }
}
