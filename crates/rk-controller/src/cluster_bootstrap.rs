//! Cluster Bootstrap — 集群引导
//!
//! 管理 R-Kafka 集群的启动流程:
//!
//! 1. **单节点模式**: 首个 Broker 自动成为 Controller (combined mode)
//! 2. **多节点模式**: 首个 Broker 成为 Controller，后续 Broker 注册加入
//! 3. **集群就绪检测**: 等待足够 Broker 注册后开始接受请求
//!
//! ```text
//! 集群引导流程:
//!
//! Broker 1 启动 ──→ 无现有集群? ──Yes──→ 成为 Controller (combined)
//!     │                                      │
//!     │                                      ├──→ 初始化 Metadata SM
//!     │                                      ├──→ 注册自身到 BrokerRegistry
//!     │                                      └──→ 集群状态: Bootstrapping
//!     │
//! Broker 2 启动 ──→ 发现 Controller ──→ 发送 BrokerRegistration (API 54)
//!     │                                      │
//!     │                                      └──→ Controller 注册 Broker 2
//!     │
//! Broker 3 启动 ──→ 同上 ──→ Controller 注册 Broker 3
//!                                                │
//!                                     所有 Broker 注册完成 ──→ Active
//! ```

use std::time::{Duration, Instant};

use tracing::{info, warn};

use rk_core::error::{Result, RkError};
use rk_core::types::BrokerId;

use crate::broker_reg::{BrokerRegistration, BrokerRegistry};
use crate::metadata_sm::MetadataStateMachine;
use crate::raft_node::{LocalNode, LogEntry, NodeRole};

// ─── 集群状态 ───────────────────────────────────────────────────────

/// 集群状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    /// 正在引导 (等待 Broker 注册)
    Bootstrapping,
    /// 活跃 (足够 Broker 在线)
    Active,
    /// 降级 (部分 Broker 离线)
    Degraded,
    /// 不可用 (Broker 不足)
    Unavailable,
}

impl std::fmt::Display for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterState::Bootstrapping => write!(f, "bootstrapping"),
            ClusterState::Active => write!(f, "active"),
            ClusterState::Degraded => write!(f, "degraded"),
            ClusterState::Unavailable => write!(f, "unavailable"),
        }
    }
}

// ─── 集群配置 ───────────────────────────────────────────────────────

/// 集群引导配置
#[derive(Debug, Clone)]
pub struct ClusterBootstrapConfig {
    /// 集群 ID
    pub cluster_id: String,
    /// 预期 Broker 数量 (0 = 动态，任意数量即可)
    pub expected_broker_count: u32,
    /// 最小可用 Broker 数 (低于此值集群不可用)
    pub min_broker_count: u32,
    /// Broker 心跳超时 (毫秒)
    pub heartbeat_timeout_ms: u64,
    /// 引导超时 (超过此时间未凑够 Broker 则告警)
    pub bootstrap_timeout_ms: u64,
    /// 是否启用 combined 模式 (Controller + Broker 同进程)
    pub combined_mode: bool,
}

impl Default for ClusterBootstrapConfig {
    fn default() -> Self {
        Self {
            cluster_id: "r-kafka-cluster".to_string(),
            expected_broker_count: 0,
            min_broker_count: 1,
            heartbeat_timeout_ms: 30_000,
            bootstrap_timeout_ms: 60_000,
            combined_mode: true,
        }
    }
}

// ─── ClusterBootstrap ───────────────────────────────────────────────

/// 集群引导器
///
/// 管理集群启动流程，协调 Broker 注册和 Controller 选举。
pub struct ClusterBootstrap {
    /// 配置
    config: ClusterBootstrapConfig,
    /// 集群状态
    state: ClusterState,
    /// Controller 节点信息
    controller: Option<LocalNode>,
    /// Broker 注册表
    broker_registry: BrokerRegistry,
    /// 引导开始时间
    bootstrap_started_at: Instant,
    /// 集群变为 Active 的时间
    activated_at: Option<Instant>,
}

impl ClusterBootstrap {
    /// 创建集群引导器
    pub fn new(config: ClusterBootstrapConfig) -> Self {
        let registry = BrokerRegistry::new(config.heartbeat_timeout_ms);
        Self {
            config,
            state: ClusterState::Bootstrapping,
            controller: None,
            broker_registry: registry,
            bootstrap_started_at: Instant::now(),
            activated_at: None,
        }
    }

    /// 引导首个 Broker (成为 Controller)
    ///
    /// 首个启动的 Broker 自动成为 Controller (combined mode)。
    /// 初始化 Metadata 状态机并注册自身。
    pub fn bootstrap_first_broker(
        &mut self,
        broker_id: BrokerId,
        host: &str,
        port: u16,
        rack: Option<String>,
    ) -> Result<LocalNode> {
        if self.controller.is_some() {
            return Err(RkError::Protocol(
                "Controller already exists in cluster".to_string(),
            ));
        }

        let node = LocalNode {
            node_id: broker_id.0 as u64,
            address: format!("{}:{}", host, port),
            role: if self.config.combined_mode {
                NodeRole::ControllerBroker
            } else {
                NodeRole::Controller
            },
        };

        // 注册到 BrokerRegistry
        let registration = BrokerRegistration {
            broker_id: broker_id.0,
            rack: rack.clone(),
            host: host.to_string(),
            port,
            api_versions: vec![],
            registered_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.broker_registry.register(registration);

        self.controller = Some(node.clone());

        info!(
            cluster_id = %self.config.cluster_id,
            broker_id = broker_id.0,
            host = host,
            port = port,
            role = %node.role,
            "First broker bootstrapped as Controller"
        );

        // 检查是否可以激活
        self.check_activation();

        Ok(node)
    }

    /// 注册新 Broker 到集群
    ///
    /// 后续 Broker 启动时调用，注册到 Controller 的 BrokerRegistry。
    pub fn register_broker(
        &mut self,
        broker_id: BrokerId,
        host: &str,
        port: u16,
        rack: Option<String>,
    ) -> Result<()> {
        if self.controller.is_none() {
            return Err(RkError::Protocol(
                "Cluster not yet bootstrapped, no Controller".to_string(),
            ));
        }

        let registration = BrokerRegistration {
            broker_id: broker_id.0,
            rack,
            host: host.to_string(),
            port,
            api_versions: vec![],
            registered_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.broker_registry.register(registration);

        info!(
            broker_id = broker_id.0,
            host = host,
            port = port,
            alive_count = self.broker_registry.alive_count(),
            "Broker registered to cluster"
        );

        self.check_activation();
        Ok(())
    }

    /// 注销 Broker
    pub fn unregister_broker(&mut self, broker_id: BrokerId) -> Result<()> {
        self.broker_registry.unregister(broker_id.0);
        info!(broker_id = broker_id.0, "Broker unregistered from cluster");
        self.check_activation();
        Ok(())
    }

    /// Broker 心跳
    pub fn heartbeat(&self, broker_id: BrokerId) -> Result<()> {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if self.broker_registry.heartbeat(broker_id.0, timestamp_ms) {
            Ok(())
        } else {
            Err(RkError::Protocol(format!(
                "Heartbeat failed for broker {}",
                broker_id.0
            )))
        }
    }

    /// 检测超时 Broker 并更新集群状态
    pub fn detect_and_update_state(&mut self) -> Vec<i32> {
        let stale = self.broker_registry.detect_stale_brokers();
        if !stale.is_empty() {
            warn!(stale_brokers = ?stale, "Stale brokers detected");
        }
        self.check_activation();
        stale
    }

    /// 检查是否满足激活条件
    fn check_activation(&mut self) {
        let alive = self.broker_registry.alive_count();

        let new_state = if alive == 0 || alive < self.config.min_broker_count as usize {
            ClusterState::Unavailable
        } else if self.config.expected_broker_count > 0
            && alive < self.config.expected_broker_count as usize
        {
            ClusterState::Degraded
        } else if self.config.expected_broker_count > 0
            && alive >= self.config.expected_broker_count as usize
        {
            ClusterState::Active
        } else {
            // expected_broker_count == 0: 动态模式，有 >= min 即可
            ClusterState::Active
        };

        if new_state != self.state {
            info!(
                old_state = %self.state,
                new_state = %new_state,
                alive_brokers = alive,
                "Cluster state changed"
            );
            if new_state == ClusterState::Active && self.activated_at.is_none() {
                self.activated_at = Some(Instant::now());
            }
            self.state = new_state;
        }
    }

    /// 获取集群状态
    pub fn state(&self) -> ClusterState {
        self.state
    }

    /// 是否已激活
    pub fn is_active(&self) -> bool {
        self.state == ClusterState::Active
    }

    /// 获取 Controller 节点
    pub fn controller(&self) -> Option<&LocalNode> {
        self.controller.as_ref()
    }

    /// 获取集群 ID
    pub fn cluster_id(&self) -> &str {
        &self.config.cluster_id
    }

    /// 获取存活的 Broker 数量
    pub fn alive_broker_count(&self) -> usize {
        self.broker_registry.alive_count()
    }

    /// 获取所有注册的 Broker ID
    pub fn registered_broker_ids(&self) -> Vec<i32> {
        self.broker_registry
            .all_brokers()
            .iter()
            .map(|r| r.broker_id)
            .collect()
    }

    /// 获取 BrokerRegistry 引用
    pub fn broker_registry(&self) -> &BrokerRegistry {
        &self.broker_registry
    }

    /// 获取引导耗时
    pub fn bootstrap_duration(&self) -> Option<Duration> {
        self.activated_at
            .map(|t| t.duration_since(self.bootstrap_started_at))
    }

    /// 获取集群摘要
    pub fn summary(&self) -> ClusterBootstrapSummary {
        ClusterBootstrapSummary {
            cluster_id: self.config.cluster_id.clone(),
            state: self.state,
            controller_broker_id: self.controller.as_ref().map(|n| n.node_id as i32),
            alive_broker_count: self.broker_registry.alive_count() as u32,
            expected_broker_count: self.config.expected_broker_count,
            bootstrap_duration: self.bootstrap_duration(),
        }
    }
}

// ─── 摘要 ────────────────────────────────────────────────────────────

/// 集群引导摘要
#[derive(Debug, Clone)]
pub struct ClusterBootstrapSummary {
    /// 集群 ID
    pub cluster_id: String,
    /// 集群状态
    pub state: ClusterState,
    /// Controller Broker ID
    pub controller_broker_id: Option<i32>,
    /// 存活 Broker 数
    pub alive_broker_count: u32,
    /// 预期 Broker 数
    pub expected_broker_count: u32,
    /// 引导耗时
    pub bootstrap_duration: Option<Duration>,
}

impl std::fmt::Display for ClusterBootstrapSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cluster '{}': state={}, controller={:?}, alive={}/{}, bootstrap={:?}",
            self.cluster_id,
            self.state,
            self.controller_broker_id,
            self.alive_broker_count,
            self.expected_broker_count,
            self.bootstrap_duration,
        )
    }
}

// ─── 集群 Metadata 初始化 ────────────────────────────────────────────

/// 初始化集群 Metadata
///
/// 在集群引导完成后，将初始 Broker 信息写入 Metadata 状态机。
pub fn initialize_cluster_metadata(
    sm: &mut MetadataStateMachine,
    broker_reg: &BrokerRegistry,
) -> Result<()> {
    let alive_brokers = broker_reg.alive_brokers();
    let start_index = sm.last_applied() + 1;

    for (i, reg) in alive_brokers.iter().enumerate() {
        let entry = LogEntry::RegisterBroker {
            broker_id: reg.broker_id,
            rack: reg.rack.clone(),
            host: reg.host.clone(),
            port: reg.port,
        };
        sm.apply(start_index + i as u64, &entry);
    }

    info!(
        brokers_registered = alive_brokers.len(),
        "Cluster metadata initialized"
    );
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(id: i32) -> BrokerId {
        BrokerId(id)
    }

    fn default_config() -> ClusterBootstrapConfig {
        ClusterBootstrapConfig {
            cluster_id: "test-cluster".to_string(),
            expected_broker_count: 3,
            min_broker_count: 1,
            heartbeat_timeout_ms: 30_000,
            bootstrap_timeout_ms: 60_000,
            combined_mode: true,
        }
    }

    #[test]
    fn test_cluster_state_display() {
        assert_eq!(ClusterState::Bootstrapping.to_string(), "bootstrapping");
        assert_eq!(ClusterState::Active.to_string(), "active");
        assert_eq!(ClusterState::Degraded.to_string(), "degraded");
        assert_eq!(ClusterState::Unavailable.to_string(), "unavailable");
    }

    #[test]
    fn test_default_config() {
        let config = ClusterBootstrapConfig::default();
        assert_eq!(config.cluster_id, "r-kafka-cluster");
        assert_eq!(config.expected_broker_count, 0);
        assert_eq!(config.min_broker_count, 1);
        assert!(config.combined_mode);
    }

    #[test]
    fn test_bootstrap_first_broker() {
        let config = ClusterBootstrapConfig {
            expected_broker_count: 0, // 动态模式
            ..default_config()
        };
        let mut bootstrap = ClusterBootstrap::new(config);
        assert_eq!(bootstrap.state(), ClusterState::Bootstrapping);

        let node = bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();

        assert_eq!(node.node_id, 1);
        assert_eq!(node.role, NodeRole::ControllerBroker);
        assert_eq!(bootstrap.state(), ClusterState::Active);
        assert!(bootstrap.is_active());
        assert_eq!(bootstrap.alive_broker_count(), 1);
    }

    #[test]
    fn test_bootstrap_first_broker_twice_fails() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();

        let result = bootstrap.bootstrap_first_broker(broker(2), "127.0.0.1", 9093, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_register_additional_brokers() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();

        // expected=3, 只有 1 个 → Degraded
        assert_eq!(bootstrap.state(), ClusterState::Degraded);

        bootstrap
            .register_broker(broker(2), "127.0.0.1", 9093, Some("rack-a".to_string()))
            .unwrap();
        // expected=3, 有 2 个 → Degraded
        assert_eq!(bootstrap.state(), ClusterState::Degraded);

        bootstrap
            .register_broker(broker(3), "127.0.0.1", 9094, Some("rack-b".to_string()))
            .unwrap();

        assert_eq!(bootstrap.alive_broker_count(), 3);
        // 3/3 达到预期 → Active
        assert_eq!(bootstrap.state(), ClusterState::Active);
    }

    #[test]
    fn test_register_before_bootstrap_fails() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        let result = bootstrap.register_broker(broker(2), "127.0.0.1", 9093, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_degraded_state() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();
        bootstrap
            .register_broker(broker(2), "127.0.0.1", 9093, None)
            .unwrap();

        // 只有 2/3 → Degraded
        assert_eq!(bootstrap.state(), ClusterState::Degraded);

        bootstrap
            .register_broker(broker(3), "127.0.0.1", 9094, None)
            .unwrap();
        // 3/3 → Active
        assert_eq!(bootstrap.state(), ClusterState::Active);
    }

    #[test]
    fn test_unregister_broker() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();
        bootstrap
            .register_broker(broker(2), "127.0.0.1", 9093, None)
            .unwrap();
        bootstrap
            .register_broker(broker(3), "127.0.0.1", 9094, None)
            .unwrap();
        assert_eq!(bootstrap.state(), ClusterState::Active);

        bootstrap.unregister_broker(broker(3)).unwrap();
        // 2/3 → Degraded
        assert_eq!(bootstrap.state(), ClusterState::Degraded);
    }

    #[test]
    fn test_heartbeat() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();

        assert!(bootstrap.heartbeat(broker(1)).is_ok());
    }

    #[test]
    fn test_controller_info() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        assert!(bootstrap.controller().is_none());

        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();

        let ctrl = bootstrap.controller().unwrap();
        assert_eq!(ctrl.node_id, 1);
        assert_eq!(ctrl.address, "127.0.0.1:9092");
    }

    #[test]
    fn test_registered_broker_ids() {
        let mut bootstrap = ClusterBootstrap::new(default_config());
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();
        bootstrap
            .register_broker(broker(2), "127.0.0.1", 9093, None)
            .unwrap();

        let ids = bootstrap.registered_broker_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    #[test]
    fn test_bootstrap_duration() {
        let mut bootstrap = ClusterBootstrap::new(ClusterBootstrapConfig {
            expected_broker_count: 0,
            ..default_config()
        });
        assert!(bootstrap.bootstrap_duration().is_none());

        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();

        // 立即激活，duration 应该很短
        let dur = bootstrap.bootstrap_duration().unwrap();
        assert!(dur.as_secs() < 1);
    }

    #[test]
    fn test_summary() {
        let mut bootstrap = ClusterBootstrap::new(ClusterBootstrapConfig {
            expected_broker_count: 0,
            ..default_config()
        });
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();

        let summary = bootstrap.summary();
        assert_eq!(summary.cluster_id, "test-cluster");
        assert_eq!(summary.state, ClusterState::Active);
        assert_eq!(summary.controller_broker_id, Some(1));
        assert_eq!(summary.alive_broker_count, 1);

        let display = summary.to_string();
        assert!(display.contains("test-cluster"));
        assert!(display.contains("active"));
    }

    #[test]
    fn test_combined_mode_false() {
        let config = ClusterBootstrapConfig {
            expected_broker_count: 0,
            combined_mode: false,
            ..default_config()
        };
        let mut bootstrap = ClusterBootstrap::new(config);
        let node = bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();
        assert_eq!(node.role, NodeRole::Controller);
    }

    #[test]
    fn test_initialize_cluster_metadata() {
        let mut bootstrap = ClusterBootstrap::new(ClusterBootstrapConfig {
            expected_broker_count: 0,
            ..default_config()
        });
        bootstrap
            .bootstrap_first_broker(broker(1), "127.0.0.1", 9092, None)
            .unwrap();
        bootstrap
            .register_broker(broker(2), "127.0.0.1", 9093, Some("rack-a".to_string()))
            .unwrap();

        let mut sm = MetadataStateMachine::new();
        initialize_cluster_metadata(&mut sm, bootstrap.broker_registry()).unwrap();

        // 验证 Broker 已写入 SM
        assert!(sm.get_broker(1).is_some());
        assert!(sm.get_broker(2).is_some());
        assert_eq!(sm.get_broker(1).unwrap().host, "127.0.0.1");
        assert_eq!(sm.get_broker(2).unwrap().rack, Some("rack-a".to_string()));
    }

    #[test]
    fn test_full_bootstrap_flow() {
        // 模拟完整的 3 节点集群引导
        let config = ClusterBootstrapConfig {
            cluster_id: "prod-cluster".to_string(),
            expected_broker_count: 3,
            min_broker_count: 1,
            heartbeat_timeout_ms: 30_000,
            bootstrap_timeout_ms: 60_000,
            combined_mode: true,
        };
        let mut bootstrap = ClusterBootstrap::new(config);

        // Step 1: Broker 1 启动 → Controller
        let node1 = bootstrap
            .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, Some("rack-a".to_string()))
            .unwrap();
        assert_eq!(node1.role, NodeRole::ControllerBroker);
        // 1/3 → Degraded
        assert_eq!(bootstrap.state(), ClusterState::Degraded);

        // Step 2: Broker 2 加入
        bootstrap
            .register_broker(broker(2), "10.0.0.2", 9092, Some("rack-b".to_string()))
            .unwrap();
        // 2/3 → Degraded
        assert_eq!(bootstrap.state(), ClusterState::Degraded);

        // Step 3: Broker 3 加入
        bootstrap
            .register_broker(broker(3), "10.0.0.3", 9092, Some("rack-c".to_string()))
            .unwrap();
        // 3/3 → Active
        assert_eq!(bootstrap.state(), ClusterState::Active);
        assert!(bootstrap.is_active());

        // 初始化 Metadata
        let mut sm = MetadataStateMachine::new();
        initialize_cluster_metadata(&mut sm, bootstrap.broker_registry()).unwrap();

        let alive = sm.list_alive_brokers();
        assert_eq!(alive.len(), 3);

        // 验证引导摘要
        let summary = bootstrap.summary();
        assert_eq!(summary.cluster_id, "prod-cluster");
        assert_eq!(summary.alive_broker_count, 3);
        assert!(summary.bootstrap_duration.is_some());
    }
}
