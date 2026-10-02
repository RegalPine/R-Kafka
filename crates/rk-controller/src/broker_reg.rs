//! Broker 注册表 — 集群成员管理
//!
//! 管理集群中所有 Broker 的注册、注销和心跳追踪。
//! Controller 使用 BrokerRegistry 检测 Broker 存活状态，
//! 触发 ISR 收缩和 Leader 选举。
//!
//! 核心功能:
//! - `register(broker)`: Broker 注册
//! - `unregister(broker_id)`: Broker 注销
//! - `heartbeat(broker_id)`: 更新心跳时间戳
//! - `detect_stale_brokers()`: 检测超时的 Broker
//! - `alive_brokers()`: 获取存活 Broker 列表

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

// ─── Broker 注册信息 ─────────────────────────────────────────────────

/// Broker 注册信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerRegistration {
    /// Broker ID
    pub broker_id: i32,
    /// 机架标识 (可选)
    pub rack: Option<String>,
    /// 主机地址
    pub host: String,
    /// 端口
    pub port: u16,
    /// 支持的 API 版本列表 (可选)
    pub api_versions: Vec<(i16, i16, i16)>,
    /// 注册时间 (Unix 时间戳 ms)
    pub registered_at_ms: u64,
}

/// Broker 心跳状态
#[derive(Debug, Clone)]
pub struct BrokerHeartbeatState {
    /// Broker ID
    pub broker_id: i32,
    /// 最后一次心跳时间
    pub last_heartbeat: Instant,
    /// 最后心跳的时间戳 (ms)
    pub last_heartbeat_ms: u64,
    /// 是否存活
    pub is_alive: bool,
    /// 连续未响应次数
    pub missed_heartbeats: u32,
}

// ─── Broker 注册表 ────────────────────────────────────────────────────

/// Broker 注册表
///
/// 管理集群中所有 Broker 的注册信息和心跳状态。
pub struct BrokerRegistry {
    /// 注册信息: broker_id → BrokerRegistration
    registrations: dashmap::DashMap<i32, BrokerRegistration>,
    /// 心跳状态: broker_id → BrokerHeartbeatState
    heartbeats: dashmap::DashMap<i32, BrokerHeartbeatState>,
    /// 心跳超时阈值
    heartbeat_timeout: Duration,
}

impl BrokerRegistry {
    /// 创建新的 Broker 注册表
    ///
    /// # Arguments
    /// * `heartbeat_timeout_ms` - Broker 心跳超时时间 (毫秒)
    pub fn new(heartbeat_timeout_ms: u64) -> Self {
        Self {
            registrations: dashmap::DashMap::new(),
            heartbeats: dashmap::DashMap::new(),
            heartbeat_timeout: Duration::from_millis(heartbeat_timeout_ms),
        }
    }

    /// 注册 Broker
    ///
    /// 如果 Broker 已存在，更新注册信息并重置心跳。
    pub fn register(&self, registration: BrokerRegistration) {
        let broker_id = registration.broker_id;
        let now = Instant::now();
        let now_ms = registration.registered_at_ms;

        info!(
            broker_id = broker_id,
            host = %registration.host,
            port = registration.port,
            rack = ?registration.rack,
            "Broker registered"
        );

        self.registrations.insert(broker_id, registration);

        self.heartbeats.insert(
            broker_id,
            BrokerHeartbeatState {
                broker_id,
                last_heartbeat: now,
                last_heartbeat_ms: now_ms,
                is_alive: true,
                missed_heartbeats: 0,
            },
        );
    }

    /// 注销 Broker
    ///
    /// 标记 Broker 为离线状态。
    pub fn unregister(&self, broker_id: i32) -> bool {
        if let Some(mut state) = self.heartbeats.get_mut(&broker_id) {
            state.is_alive = false;
            info!(broker_id = broker_id, "Broker unregistered");
            true
        } else {
            warn!(broker_id = broker_id, "Broker not found for unregistration");
            false
        }
    }

    /// 处理 Broker 心跳
    ///
    /// 更新最后心跳时间，重置未响应计数。
    pub fn heartbeat(&self, broker_id: i32, timestamp_ms: u64) -> bool {
        if let Some(mut state) = self.heartbeats.get_mut(&broker_id) {
            state.last_heartbeat = Instant::now();
            state.last_heartbeat_ms = timestamp_ms;
            state.missed_heartbeats = 0;
            state.is_alive = true;
            true
        } else {
            warn!(broker_id = broker_id, "Heartbeat from unregistered broker");
            false
        }
    }

    /// 检测超时的 Broker
    ///
    /// 扫描所有注册 Broker，将超过心跳阈值的标记为离线。
    /// 返回新检测到的离线 Broker ID 列表。
    pub fn detect_stale_brokers(&self) -> Vec<i32> {
        let now = Instant::now();
        let mut stale = Vec::new();

        for mut entry in self.heartbeats.iter_mut() {
            let state = entry.value_mut();
            if state.is_alive && now.duration_since(state.last_heartbeat) > self.heartbeat_timeout {
                state.is_alive = false;
                state.missed_heartbeats += 1;
                warn!(
                    broker_id = state.broker_id,
                    elapsed_ms = now.duration_since(state.last_heartbeat).as_millis() as u64,
                    "Broker heartbeat timeout, marking offline"
                );
                stale.push(state.broker_id);
            }
        }

        stale
    }

    /// 获取所有存活的 Broker
    pub fn alive_brokers(&self) -> Vec<BrokerRegistration> {
        self.heartbeats
            .iter()
            .filter(|r| r.value().is_alive)
            .filter_map(|r| {
                self.registrations
                    .get(&r.key())
                    .map(|reg| reg.value().clone())
            })
            .collect()
    }

    /// 获取所有注册的 Broker (含离线)
    pub fn all_brokers(&self) -> Vec<BrokerRegistration> {
        self.registrations
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    /// 获取指定 Broker 的注册信息
    pub fn get_broker(&self, broker_id: i32) -> Option<BrokerRegistration> {
        self.registrations
            .get(&broker_id)
            .map(|r| r.value().clone())
    }

    /// 检查 Broker 是否存活
    pub fn is_alive(&self, broker_id: i32) -> bool {
        self.heartbeats
            .get(&broker_id)
            .map(|r| r.is_alive)
            .unwrap_or(false)
    }

    /// 获取 Broker 数量 (含离线)
    pub fn total_count(&self) -> usize {
        self.registrations.len()
    }

    /// 获取存活 Broker 数量
    pub fn alive_count(&self) -> usize {
        self.heartbeats
            .iter()
            .filter(|r| r.value().is_alive)
            .count()
    }

    /// 获取 Broker 心跳状态
    pub fn get_heartbeat_state(&self, broker_id: i32) -> Option<BrokerHeartbeatState> {
        self.heartbeats.get(&broker_id).map(|r| r.value().clone())
    }

    /// 移除 Broker 的所有记录 (完全删除)
    pub fn remove(&self, broker_id: i32) -> Option<BrokerRegistration> {
        self.heartbeats.remove(&broker_id);
        self.registrations
            .remove(&broker_id)
            .map(|(_, v)| v)
    }

    /// 获取心跳超时阈值
    pub fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_timeout
    }
}

impl Default for BrokerRegistry {
    fn default() -> Self {
        // 默认 30 秒超时
        Self::new(30_000)
    }
}

impl std::fmt::Debug for BrokerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerRegistry")
            .field("total", &self.total_count())
            .field("alive", &self.alive_count())
            .field("timeout", &self.heartbeat_timeout)
            .finish()
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_registration(broker_id: i32) -> BrokerRegistration {
        BrokerRegistration {
            broker_id,
            rack: Some(format!("rack-{}", broker_id)),
            host: format!("192.168.1.{}", broker_id),
            port: 9092,
            api_versions: vec![],
            registered_at_ms: 1000,
        }
    }

    #[test]
    fn test_registry_new() {
        let reg = BrokerRegistry::new(30_000);
        assert_eq!(reg.total_count(), 0);
        assert_eq!(reg.alive_count(), 0);
    }

    #[test]
    fn test_registry_register() {
        let reg = BrokerRegistry::new(30_000);
        reg.register(make_registration(1));
        reg.register(make_registration(2));

        assert_eq!(reg.total_count(), 2);
        assert_eq!(reg.alive_count(), 2);

        let broker = reg.get_broker(1).unwrap();
        assert_eq!(broker.broker_id, 1);
        assert_eq!(broker.host, "192.168.1.1");
    }

    #[test]
    fn test_registry_unregister() {
        let reg = BrokerRegistry::new(30_000);
        reg.register(make_registration(1));
        assert!(reg.is_alive(1));

        assert!(reg.unregister(1));
        assert!(!reg.is_alive(1));
        assert_eq!(reg.alive_count(), 0);
        assert_eq!(reg.total_count(), 1); // 记录仍在
    }

    #[test]
    fn test_registry_unregister_nonexistent() {
        let reg = BrokerRegistry::new(30_000);
        assert!(!reg.unregister(999));
    }

    #[test]
    fn test_registry_heartbeat() {
        let reg = BrokerRegistry::new(30_000);
        reg.register(make_registration(1));

        assert!(reg.heartbeat(1, 2000));

        let state = reg.get_heartbeat_state(1).unwrap();
        assert_eq!(state.missed_heartbeats, 0);
        assert!(state.is_alive);
    }

    #[test]
    fn test_registry_heartbeat_unregistered() {
        let reg = BrokerRegistry::new(30_000);
        assert!(!reg.heartbeat(999, 2000));
    }

    #[test]
    fn test_registry_alive_brokers() {
        let reg = BrokerRegistry::new(30_000);
        reg.register(make_registration(1));
        reg.register(make_registration(2));
        reg.register(make_registration(3));

        reg.unregister(2);

        let alive = reg.alive_brokers();
        assert_eq!(alive.len(), 2);

        let all = reg.all_brokers();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_registry_is_alive() {
        let reg = BrokerRegistry::new(30_000);
        assert!(!reg.is_alive(1)); // 未注册

        reg.register(make_registration(1));
        assert!(reg.is_alive(1));

        reg.unregister(1);
        assert!(!reg.is_alive(1));
    }

    #[test]
    fn test_registry_remove() {
        let reg = BrokerRegistry::new(30_000);
        reg.register(make_registration(1));

        let removed = reg.remove(1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().broker_id, 1);
        assert_eq!(reg.total_count(), 0);
    }

    #[test]
    fn test_registry_remove_nonexistent() {
        let reg = BrokerRegistry::new(30_000);
        assert!(reg.remove(999).is_none());
    }

    #[test]
    fn test_registry_re_register() {
        let reg = BrokerRegistry::new(30_000);

        reg.register(make_registration(1));
        reg.unregister(1);
        assert!(!reg.is_alive(1));

        // 重新注册
        let mut new_reg = make_registration(1);
        new_reg.host = "10.0.0.1".to_string();
        reg.register(new_reg);

        assert!(reg.is_alive(1));
        let broker = reg.get_broker(1).unwrap();
        assert_eq!(broker.host, "10.0.0.1");
    }

    #[test]
    fn test_registry_detect_stale_brokers() {
        // 使用非常短的超时 (1ms) 来测试
        let reg = BrokerRegistry::new(1);
        reg.register(make_registration(1));
        reg.register(make_registration(2));

        // 等待超时
        std::thread::sleep(Duration::from_millis(10));

        let stale = reg.detect_stale_brokers();
        assert_eq!(stale.len(), 2);
        assert!(stale.contains(&1));
        assert!(stale.contains(&2));

        // 检测后 Broker 应该标记为离线
        assert!(!reg.is_alive(1));
        assert!(!reg.is_alive(2));
    }

    #[test]
    fn test_registry_detect_stale_after_heartbeat() {
        let reg = BrokerRegistry::new(100);
        reg.register(make_registration(1));

        std::thread::sleep(Duration::from_millis(10));

        // 心跳续命
        reg.heartbeat(1, 2000);

        let stale = reg.detect_stale_brokers();
        assert_eq!(stale.len(), 0);
        assert!(reg.is_alive(1));
    }

    #[test]
    fn test_broker_registration_serde() {
        let reg = make_registration(1);
        let json = serde_json::to_string(&reg).unwrap();
        let decoded: BrokerRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.broker_id, 1);
        assert_eq!(decoded.host, "192.168.1.1");
    }

    #[test]
    fn test_registry_debug() {
        let reg = BrokerRegistry::new(30_000);
        reg.register(make_registration(1));
        let debug_str = format!("{:?}", reg);
        assert!(debug_str.contains("BrokerRegistry"));
        assert!(debug_str.contains("total: 1"));
    }

    #[test]
    fn test_registry_default() {
        let reg = BrokerRegistry::default();
        assert_eq!(reg.heartbeat_timeout(), Duration::from_millis(30_000));
    }

    #[test]
    fn test_registry_heartbeat_state() {
        let reg = BrokerRegistry::new(30_000);
        reg.register(make_registration(1));

        let state = reg.get_heartbeat_state(1).unwrap();
        assert_eq!(state.broker_id, 1);
        assert_eq!(state.missed_heartbeats, 0);
        assert!(state.is_alive);

        // 不存在的 broker
        assert!(reg.get_heartbeat_state(999).is_none());
    }
}
