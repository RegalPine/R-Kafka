//! Rack Awareness — 机架感知拓扑管理
//!
//! 管理集群的机架拓扑信息，为副本放置提供决策依据:
//!
//! ```text
//! 机架拓扑:
//!
//! ┌─────────────────────────────────────────────┐
//! │              Cluster Topology                │
//! │                                              │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
//! │  │  rack-a  │  │  rack-b  │  │  rack-c  │   │
//! │  │          │  │          │  │          │   │
//! │  │ B1  B2   │  │ B3  B4   │  │ B5  B6   │   │
//! │  └──────────┘  └──────────┘  └──────────┘   │
//! │                                              │
//! │  RackTopology:                               │
//! │    rack_count: 3                             │
//! │    total_brokers: 6                          │
//! │    brokers_per_rack: {a:2, b:2, c:2}        │
//! │    alive_brokers_per_rack: {a:2, b:1, c:2}  │
//! └─────────────────────────────────────────────┘
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};

// ─── RackId ──────────────────────────────────────────────────────────

/// 机架标识
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RackId(pub String);

impl RackId {
    /// 创建新的机架标识
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 获取机架标识字符串
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for RackId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for RackId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// ─── BrokerRackInfo ──────────────────────────────────────────────────

/// Broker 的机架信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerRackInfo {
    /// Broker ID
    pub broker_id: i32,
    /// 所在机架
    pub rack_id: RackId,
    /// 主机地址
    pub host: String,
    /// 是否存活
    pub is_alive: bool,
    /// 当前承载的 partition 数量
    pub partition_load: u32,
}

// ─── RackTopology ────────────────────────────────────────────────────

/// 集群机架拓扑
///
/// 跟踪所有 Broker 的机架分布，提供拓扑查询能力。
pub struct RackTopology {
    /// rack_id → 该机架上的 Broker 列表
    rack_brokers: HashMap<RackId, Vec<BrokerRackInfo>>,
    /// broker_id → rack_id (快速反查)
    broker_rack: HashMap<i32, RackId>,
    /// 无机架标识的 Broker (rack = None)
    no_rack_brokers: Vec<BrokerRackInfo>,
}

impl RackTopology {
    /// 创建空拓扑
    pub fn new() -> Self {
        Self {
            rack_brokers: HashMap::new(),
            broker_rack: HashMap::new(),
            no_rack_brokers: Vec::new(),
        }
    }

    /// 从 Broker 列表构建拓扑
    pub fn from_brokers(brokers: &[BrokerRackInfo]) -> Self {
        let mut topology = Self::new();
        for broker in brokers {
            topology.add_broker(broker.clone());
        }
        topology
    }

    /// 添加 Broker 到拓扑
    pub fn add_broker(&mut self, broker: BrokerRackInfo) {
        let broker_id = broker.broker_id;
        let rack_id = broker.rack_id.clone();

        // 如果已存在，先移除旧记录
        self.remove_broker(broker_id);

        self.rack_brokers
            .entry(rack_id.clone())
            .or_default()
            .push(broker);
        self.broker_rack.insert(broker_id, rack_id);
    }

    /// 添加无机架标识的 Broker
    pub fn add_broker_no_rack(&mut self, broker: BrokerRackInfo) {
        let broker_id = broker.broker_id;
        self.remove_broker(broker_id);
        self.no_rack_brokers.push(broker);
    }

    /// 移除 Broker
    pub fn remove_broker(&mut self, broker_id: i32) {
        if let Some(rack_id) = self.broker_rack.remove(&broker_id) {
            if let Some(brokers) = self.rack_brokers.get_mut(&rack_id) {
                brokers.retain(|b| b.broker_id != broker_id);
                if brokers.is_empty() {
                    self.rack_brokers.remove(&rack_id);
                }
            }
        }
        self.no_rack_brokers.retain(|b| b.broker_id != broker_id);
    }

    /// 更新 Broker 存活状态
    pub fn set_alive(&mut self, broker_id: i32, is_alive: bool) {
        if let Some(rack_id) = self.broker_rack.get(&broker_id) {
            if let Some(brokers) = self.rack_brokers.get_mut(rack_id) {
                for b in brokers.iter_mut() {
                    if b.broker_id == broker_id {
                        b.is_alive = is_alive;
                        return;
                    }
                }
            }
        }
        for b in self.no_rack_brokers.iter_mut() {
            if b.broker_id == broker_id {
                b.is_alive = is_alive;
                return;
            }
        }
    }

    /// 更新 Broker 的 partition 负载
    pub fn set_partition_load(&mut self, broker_id: i32, load: u32) {
        if let Some(rack_id) = self.broker_rack.get(&broker_id) {
            if let Some(brokers) = self.rack_brokers.get_mut(rack_id) {
                for b in brokers.iter_mut() {
                    if b.broker_id == broker_id {
                        b.partition_load = load;
                        return;
                    }
                }
            }
        }
    }

    /// 获取机架数量
    pub fn rack_count(&self) -> usize {
        self.rack_brokers.len()
    }

    /// 获取所有机架 ID (排序)
    pub fn rack_ids(&self) -> Vec<RackId> {
        let mut ids: Vec<RackId> = self.rack_brokers.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// 获取指定机架上的所有 Broker
    pub fn brokers_in_rack(&self, rack_id: &RackId) -> Vec<&BrokerRackInfo> {
        self.rack_brokers
            .get(rack_id)
            .map(|bs| bs.iter().collect())
            .unwrap_or_default()
    }

    /// 获取指定机架上的存活 Broker
    pub fn alive_brokers_in_rack(&self, rack_id: &RackId) -> Vec<&BrokerRackInfo> {
        self.brokers_in_rack(rack_id)
            .into_iter()
            .filter(|b| b.is_alive)
            .collect()
    }

    /// 获取 Broker 所在机架
    pub fn rack_for_broker(&self, broker_id: i32) -> Option<&RackId> {
        self.broker_rack.get(&broker_id)
    }

    /// 获取总 Broker 数 (含无机架)
    pub fn total_broker_count(&self) -> usize {
        self.broker_rack.len() + self.no_rack_brokers.len()
    }

    /// 获取存活 Broker 数
    pub fn alive_broker_count(&self) -> usize {
        let rack_alive = self
            .rack_brokers
            .values()
            .flat_map(|bs| bs.iter())
            .filter(|b| b.is_alive)
            .count();
        let no_rack_alive = self.no_rack_brokers.iter().filter(|b| b.is_alive).count();
        rack_alive + no_rack_alive
    }

    /// 获取每个机架的 Broker 数量
    pub fn brokers_per_rack(&self) -> BTreeMap<RackId, usize> {
        self.rack_brokers
            .iter()
            .map(|(rack, brokers)| (rack.clone(), brokers.len()))
            .collect()
    }

    /// 获取每个机架的存活 Broker 数量
    pub fn alive_per_rack(&self) -> BTreeMap<RackId, usize> {
        self.rack_brokers
            .iter()
            .map(|(rack, brokers)| {
                let alive = brokers.iter().filter(|b| b.is_alive).count();
                (rack.clone(), alive)
            })
            .collect()
    }

    /// 检查两个 Broker 是否在不同机架上
    pub fn are_on_different_racks(&self, broker_a: i32, broker_b: i32) -> bool {
        match (
            self.rack_for_broker(broker_a),
            self.rack_for_broker(broker_b),
        ) {
            (Some(rack_a), Some(rack_b)) => rack_a != rack_b,
            _ => false, // 无机架标识的认为在同一"逻辑机架"
        }
    }

    /// 检查一组 Broker 是否全部在不同机架上
    pub fn all_on_different_racks(&self, broker_ids: &[i32]) -> bool {
        let mut seen_racks = HashSet::new();
        for &bid in broker_ids {
            if let Some(rack) = self.rack_for_broker(bid) {
                if !seen_racks.insert(rack.clone()) {
                    return false;
                }
            }
        }
        true
    }

    /// 获取负载最低的机架 (用于均衡分配)
    pub fn least_loaded_rack(&self) -> Option<RackId> {
        self.rack_brokers
            .iter()
            .filter(|(_, brokers)| brokers.iter().any(|b| b.is_alive))
            .min_by_key(|(_, brokers)| brokers.iter().map(|b| b.partition_load).sum::<u32>())
            .map(|(rack, _)| rack.clone())
    }

    /// 获取机架分布摘要
    pub fn summary(&self) -> RackTopologySummary {
        let rack_count = self.rack_count();
        let total_brokers = self.total_broker_count();
        let alive_brokers = self.alive_broker_count();
        let brokers_per_rack = self.brokers_per_rack();
        let alive_per_rack = self.alive_per_rack();

        RackTopologySummary {
            rack_count,
            total_brokers,
            alive_brokers,
            brokers_per_rack,
            alive_per_rack,
        }
    }

    /// 验证副本放置是否满足机架感知约束
    ///
    /// 返回违反约束的 partition 列表 (同一 partition 的多个副本在同一机架上)
    pub fn validate_replica_spread(
        &self,
        assignments: &HashMap<i32, Vec<i32>>,
    ) -> Vec<RackViolation> {
        let mut violations = Vec::new();

        for (partition_id, replicas) in assignments {
            let mut rack_counts: HashMap<&RackId, Vec<i32>> = HashMap::new();
            for &bid in replicas {
                if let Some(rack) = self.rack_for_broker(bid) {
                    rack_counts.entry(rack).or_default().push(bid);
                }
            }

            for (rack, brokers) in &rack_counts {
                if brokers.len() > 1 {
                    violations.push(RackViolation {
                        partition_id: *partition_id,
                        rack_id: (*rack).clone(),
                        brokers_on_rack: brokers.clone(),
                    });
                }
            }
        }

        violations
    }
}

impl Default for RackTopology {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for RackTopology {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let summary = self.summary();
        f.debug_struct("RackTopology")
            .field("rack_count", &summary.rack_count)
            .field("total_brokers", &summary.total_brokers)
            .field("alive_brokers", &summary.alive_brokers)
            .finish()
    }
}

// ─── 辅助类型 ────────────────────────────────────────────────────────

/// 机架拓扑摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackTopologySummary {
    pub rack_count: usize,
    pub total_brokers: usize,
    pub alive_brokers: usize,
    pub brokers_per_rack: BTreeMap<RackId, usize>,
    pub alive_per_rack: BTreeMap<RackId, usize>,
}

impl fmt::Display for RackTopologySummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RackTopology(racks={}, brokers={}/{}, per_rack={:?})",
            self.rack_count, self.alive_brokers, self.total_brokers, self.brokers_per_rack
        )
    }
}

/// 机架违规记录
#[derive(Debug, Clone)]
pub struct RackViolation {
    pub partition_id: i32,
    pub rack_id: RackId,
    pub brokers_on_rack: Vec<i32>,
}

impl fmt::Display for RackViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Partition {} has {} replicas on rack {}: {:?}",
            self.partition_id,
            self.brokers_on_rack.len(),
            self.rack_id,
            self.brokers_on_rack
        )
    }
}

/// 机架感知配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackAwareConfig {
    /// 是否启用机架感知
    pub enabled: bool,
    /// 本机 Broker 的机架标识
    pub broker_rack_id: Option<String>,
    /// 当机架数不足时是否回退到轮询
    pub fallback_to_round_robin: bool,
    /// 首选 Leader 分布策略
    pub leader_distribution: LeaderDistribution,
}

/// Leader 分布策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaderDistribution {
    /// 均匀分布 (所有机架均衡)
    Even,
    /// 优先选择负载最低的机架
    LeastLoaded,
}

impl Default for RackAwareConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            broker_rack_id: None,
            fallback_to_round_robin: true,
            leader_distribution: LeaderDistribution::Even,
        }
    }
}

// ─── 单元测试 ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_broker(id: i32, rack: &str, alive: bool) -> BrokerRackInfo {
        BrokerRackInfo {
            broker_id: id,
            rack_id: RackId::new(rack),
            host: format!("192.168.1.{}", id),
            is_alive: alive,
            partition_load: 0,
        }
    }

    fn make_broker_with_load(id: i32, rack: &str, alive: bool, load: u32) -> BrokerRackInfo {
        BrokerRackInfo {
            broker_id: id,
            rack_id: RackId::new(rack),
            host: format!("192.168.1.{}", id),
            is_alive: alive,
            partition_load: load,
        }
    }

    // ─── RackId 测试 ────────────────────────────────────────────────

    #[test]
    fn test_rack_id_creation() {
        let rack = RackId::new("rack-a");
        assert_eq!(rack.as_str(), "rack-a");
        assert_eq!(format!("{}", rack), "rack-a");
    }

    #[test]
    fn test_rack_id_equality() {
        let a = RackId::new("rack-a");
        let b = RackId::new("rack-a");
        let c = RackId::new("rack-b");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_rack_id_from() {
        let r1: RackId = "rack-a".into();
        let r2: RackId = String::from("rack-a").into();
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_rack_id_serde() {
        let rack = RackId::new("rack-a");
        let json = serde_json::to_string(&rack).unwrap();
        let decoded: RackId = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, rack);
    }

    // ─── RackTopology 构建测试 ──────────────────────────────────────

    #[test]
    fn test_topology_empty() {
        let topo = RackTopology::new();
        assert_eq!(topo.rack_count(), 0);
        assert_eq!(topo.total_broker_count(), 0);
        assert_eq!(topo.alive_broker_count(), 0);
    }

    #[test]
    fn test_topology_from_brokers() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-a", true),
            make_broker(3, "rack-b", true),
            make_broker(4, "rack-c", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        assert_eq!(topo.rack_count(), 3);
        assert_eq!(topo.total_broker_count(), 4);
        assert_eq!(topo.alive_broker_count(), 4);
    }

    #[test]
    fn test_topology_add_remove_broker() {
        let mut topo = RackTopology::new();
        topo.add_broker(make_broker(1, "rack-a", true));
        topo.add_broker(make_broker(2, "rack-b", true));
        assert_eq!(topo.rack_count(), 2);
        assert_eq!(topo.total_broker_count(), 2);

        topo.remove_broker(1);
        assert_eq!(topo.rack_count(), 1);
        assert_eq!(topo.total_broker_count(), 1);
        assert!(topo.rack_for_broker(1).is_none());
    }

    #[test]
    fn test_topology_brokers_per_rack() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-a", true),
            make_broker(3, "rack-b", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let per_rack = topo.brokers_per_rack();
        assert_eq!(per_rack[&RackId::new("rack-a")], 2);
        assert_eq!(per_rack[&RackId::new("rack-b")], 1);
    }

    // ─── 存活状态测试 ────────────────────────────────────────────────

    #[test]
    fn test_topology_alive_count() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-a", false),
            make_broker(3, "rack-b", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        assert_eq!(topo.alive_broker_count(), 2);
        assert_eq!(topo.total_broker_count(), 3);

        let alive_per_rack = topo.alive_per_rack();
        assert_eq!(alive_per_rack[&RackId::new("rack-a")], 1);
        assert_eq!(alive_per_rack[&RackId::new("rack-b")], 1);
    }

    #[test]
    fn test_topology_set_alive() {
        let mut topo = RackTopology::new();
        topo.add_broker(make_broker(1, "rack-a", true));
        assert_eq!(topo.alive_broker_count(), 1);

        topo.set_alive(1, false);
        assert_eq!(topo.alive_broker_count(), 0);

        topo.set_alive(1, true);
        assert_eq!(topo.alive_broker_count(), 1);
    }

    #[test]
    fn test_alive_brokers_in_rack() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-a", false),
            make_broker(3, "rack-a", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let alive = topo.alive_brokers_in_rack(&RackId::new("rack-a"));
        assert_eq!(alive.len(), 2);
    }

    // ─── 跨机架检查测试 ──────────────────────────────────────────────

    #[test]
    fn test_are_on_different_racks() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-b", true),
            make_broker(3, "rack-a", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        assert!(topo.are_on_different_racks(1, 2));
        assert!(!topo.are_on_different_racks(1, 3));
    }

    #[test]
    fn test_all_on_different_racks() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-b", true),
            make_broker(3, "rack-c", true),
            make_broker(4, "rack-a", true), // 与 broker 1 同机架
        ];
        let topo = RackTopology::from_brokers(&brokers);
        assert!(topo.all_on_different_racks(&[1, 2, 3]));
        assert!(!topo.all_on_different_racks(&[1, 4])); // 1,4 都在 rack-a
    }

    // ─── 负载均衡测试 ────────────────────────────────────────────────

    #[test]
    fn test_least_loaded_rack() {
        let brokers = vec![
            make_broker_with_load(1, "rack-a", true, 10),
            make_broker_with_load(2, "rack-a", true, 10),
            make_broker_with_load(3, "rack-b", true, 5),
            make_broker_with_load(4, "rack-c", true, 20),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let least = topo.least_loaded_rack().unwrap();
        assert_eq!(least, RackId::new("rack-b"));
    }

    #[test]
    fn test_least_loaded_rack_skips_dead() {
        let brokers = vec![
            make_broker_with_load(1, "rack-a", true, 100),
            make_broker_with_load(2, "rack-b", false, 0), // dead
            make_broker_with_load(3, "rack-c", true, 5),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let least = topo.least_loaded_rack().unwrap();
        assert_eq!(least, RackId::new("rack-c"));
    }

    // ─── 副本分布验证测试 ────────────────────────────────────────────

    #[test]
    fn test_validate_replica_spread_no_violations() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-b", true),
            make_broker(3, "rack-c", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);

        let mut assignments = HashMap::new();
        assignments.insert(0, vec![1, 2, 3]); // 每个副本在不同机架
        assignments.insert(1, vec![2, 3, 1]);

        let violations = topo.validate_replica_spread(&assignments);
        assert!(violations.is_empty(), "Should have no violations");
    }

    #[test]
    fn test_validate_replica_spread_with_violations() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-a", true), // 同机架
            make_broker(3, "rack-b", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);

        let mut assignments = HashMap::new();
        assignments.insert(0, vec![1, 2, 3]); // 1,2 在 rack-a → 违规

        let violations = topo.validate_replica_spread(&assignments);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].partition_id, 0);
        assert_eq!(violations[0].rack_id, RackId::new("rack-a"));
        assert_eq!(violations[0].brokers_on_rack, vec![1, 2]);
    }

    // ─── Summary 测试 ───────────────────────────────────────────────

    #[test]
    fn test_topology_summary() {
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-a", true),
            make_broker(3, "rack-b", false),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let summary = topo.summary();

        assert_eq!(summary.rack_count, 2);
        assert_eq!(summary.total_brokers, 3);
        assert_eq!(summary.alive_brokers, 2);
        assert_eq!(summary.brokers_per_rack[&RackId::new("rack-a")], 2);
        assert_eq!(summary.alive_per_rack[&RackId::new("rack-b")], 0);
    }

    #[test]
    fn test_summary_display() {
        let summary = RackTopologySummary {
            rack_count: 3,
            total_brokers: 6,
            alive_brokers: 5,
            brokers_per_rack: BTreeMap::new(),
            alive_per_rack: BTreeMap::new(),
        };
        let s = format!("{}", summary);
        assert!(s.contains("racks=3"));
        assert!(s.contains("brokers=5/6"));
    }

    // ─── Config 测试 ────────────────────────────────────────────────

    #[test]
    fn test_rack_aware_config_default() {
        let config = RackAwareConfig::default();
        assert!(!config.enabled);
        assert!(config.broker_rack_id.is_none());
        assert!(config.fallback_to_round_robin);
        assert_eq!(config.leader_distribution, LeaderDistribution::Even);
    }

    #[test]
    fn test_rack_aware_config_serde() {
        let config = RackAwareConfig {
            enabled: true,
            broker_rack_id: Some("rack-a".to_string()),
            fallback_to_round_robin: false,
            leader_distribution: LeaderDistribution::LeastLoaded,
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: RackAwareConfig = serde_json::from_str(&json).unwrap();
        assert!(decoded.enabled);
        assert_eq!(decoded.broker_rack_id, Some("rack-a".to_string()));
    }

    // ─── RackViolation 测试 ─────────────────────────────────────────

    #[test]
    fn test_rack_violation_display() {
        let v = RackViolation {
            partition_id: 0,
            rack_id: RackId::new("rack-a"),
            brokers_on_rack: vec![1, 2],
        };
        let s = format!("{}", v);
        assert!(s.contains("Partition 0"));
        assert!(s.contains("rack-a"));
    }

    // ─── 综合场景测试 ────────────────────────────────────────────────

    #[test]
    fn test_3_rack_topology_full_scenario() {
        // 3 机架，每机架 2 Broker
        let brokers = vec![
            make_broker(1, "rack-a", true),
            make_broker(2, "rack-a", true),
            make_broker(3, "rack-b", true),
            make_broker(4, "rack-b", true),
            make_broker(5, "rack-c", true),
            make_broker(6, "rack-c", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);

        assert_eq!(topo.rack_count(), 3);
        assert_eq!(topo.total_broker_count(), 6);
        assert_eq!(topo.alive_broker_count(), 6);

        // 验证跨机架
        assert!(topo.all_on_different_racks(&[1, 3, 5]));
        assert!(!topo.all_on_different_racks(&[1, 2, 3]));

        // 验证副本分布 (1,3,5 各在一个机架 → 无违规)
        let mut assignments = HashMap::new();
        assignments.insert(0, vec![1, 3, 5]);
        assignments.insert(1, vec![2, 4, 6]);
        let violations = topo.validate_replica_spread(&assignments);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_topology_debug() {
        let mut topo = RackTopology::new();
        topo.add_broker(make_broker(1, "rack-a", true));
        let debug_str = format!("{:?}", topo);
        assert!(debug_str.contains("RackTopology"));
        assert!(debug_str.contains("rack_count: 1"));
    }

    #[test]
    fn test_set_partition_load() {
        let mut topo = RackTopology::new();
        topo.add_broker(make_broker(1, "rack-a", true));
        topo.set_partition_load(1, 42);

        let brokers = topo.brokers_in_rack(&RackId::new("rack-a"));
        assert_eq!(brokers[0].partition_load, 42);
    }

    #[test]
    fn test_rack_ids_sorted() {
        let brokers = vec![
            make_broker(1, "rack-c", true),
            make_broker(2, "rack-a", true),
            make_broker(3, "rack-b", true),
        ];
        let topo = RackTopology::from_brokers(&brokers);
        let ids = topo.rack_ids();
        assert_eq!(ids[0], RackId::new("rack-a"));
        assert_eq!(ids[1], RackId::new("rack-b"));
        assert_eq!(ids[2], RackId::new("rack-c"));
    }
}
