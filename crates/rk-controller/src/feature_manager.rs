//! FeatureManager — Feature Version 协商
//!
//! 管理混合集群中各 Broker 支持的 Feature 版本，计算公共可用版本 (finalized)。
//! 用于 KRaft 模式下确保所有节点兼容同一 Metadata 版本。
//!
//! ## 协商流程
//!
//! ```text
//! Broker 1: metadata.version [0, 17]
//! Broker 2: metadata.version [0, 16]
//! Broker 3: metadata.version [0, 17]
//!
//! Finalized: metadata.version = 16 (所有 Broker 的 min(max_version))
//! ```
//!
//! ## 兼容性检查
//!
//! 如果某 Broker 的 min_version > 集群 finalized version，则该 Broker 不兼容。

use std::collections::HashMap;
use std::sync::RwLock;

use rk_core::BrokerId;
use serde::{Deserialize, Serialize};

/// 单个 Broker 上报的 Feature 版本范围
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerFeature {
    /// Feature 名称 (如 "metadata.version", "group.version")
    pub name: String,
    /// 最低支持版本
    pub min_version: i16,
    /// 最高支持版本
    pub max_version: i16,
}

/// 集群最终确定的 Feature 版本
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizedFeature {
    /// Feature 名称
    pub name: String,
    /// 最终确定的版本
    pub version: i16,
}

/// Feature 协商管理器
///
/// 收集各 Broker 上报的 Feature 版本范围，计算集群公共版本。
pub struct FeatureManager {
    /// 各 Broker 上报的 Feature 列表
    broker_features: RwLock<HashMap<BrokerId, Vec<BrokerFeature>>>,
}

impl FeatureManager {
    /// 创建空的 FeatureManager
    pub fn new() -> Self {
        Self {
            broker_features: RwLock::new(HashMap::new()),
        }
    }

    /// 注册/更新某 Broker 的 Feature 列表
    pub fn update_broker_features(&self, broker_id: BrokerId, features: Vec<BrokerFeature>) {
        let mut map = self.broker_features.write().unwrap();
        map.insert(broker_id, features);
    }

    /// 移除某 Broker 的 Feature 信息 (Broker 下线时)
    pub fn remove_broker(&self, broker_id: BrokerId) {
        let mut map = self.broker_features.write().unwrap();
        map.remove(&broker_id);
    }

    /// 计算集群最终确定的 Feature 版本
    ///
    /// 对于每个 Feature:
    /// - finalized_version = min(all brokers' max_version)
    /// - 如果任何 broker 的 min_version > finalized_version → 不兼容
    ///
    /// 只返回所有 Broker 都支持的 Feature。
    pub fn compute_finalized(&self) -> Vec<FinalizedFeature> {
        let map = self.broker_features.read().unwrap();
        if map.is_empty() {
            return Vec::new();
        }

        // 收集所有 Feature 名称
        let mut all_names: Vec<String> = Vec::new();
        for features in map.values() {
            for f in features {
                if !all_names.contains(&f.name) {
                    all_names.push(f.name.clone());
                }
            }
        }

        let mut result = Vec::new();
        for name in &all_names {
            // 收集所有 Broker 对该 Feature 的版本范围
            let mut max_versions: Vec<i16> = Vec::new();
            let mut min_versions: Vec<i16> = Vec::new();
            let mut all_brokers_have = true;

            for features in map.values() {
                match features.iter().find(|f| &f.name == name) {
                    Some(f) => {
                        max_versions.push(f.max_version);
                        min_versions.push(f.min_version);
                    }
                    None => {
                        // 该 Broker 不支持此 Feature
                        all_brokers_have = false;
                        break;
                    }
                }
            }

            if !all_brokers_have {
                continue;
            }

            // finalized = min(max_versions)
            let finalized = *max_versions.iter().min().unwrap();

            // 检查兼容性: 所有 broker 的 min_version <= finalized
            let compatible = min_versions.iter().all(|&min_v| min_v <= finalized);
            if compatible {
                result.push(FinalizedFeature {
                    name: name.clone(),
                    version: finalized,
                });
            }
        }

        result
    }

    /// 检查集群是否兼容 (所有 Feature 都有公共版本)
    pub fn is_compatible(&self) -> bool {
        let map = self.broker_features.read().unwrap();
        if map.is_empty() {
            return true;
        }

        // 收集所有 Feature 名称
        let mut all_names: Vec<String> = Vec::new();
        for features in map.values() {
            for f in features {
                if !all_names.contains(&f.name) {
                    all_names.push(f.name.clone());
                }
            }
        }

        for name in &all_names {
            let mut max_versions: Vec<i16> = Vec::new();
            let mut min_versions: Vec<i16> = Vec::new();

            for features in map.values() {
                match features.iter().find(|f| &f.name == name) {
                    Some(f) => {
                        max_versions.push(f.max_version);
                        min_versions.push(f.min_version);
                    }
                    None => return false, // 不是所有 Broker 都支持
                }
            }

            let finalized = *max_versions.iter().min().unwrap();
            if !min_versions.iter().all(|&min_v| min_v <= finalized) {
                return false;
            }
        }

        true
    }

    /// 获取某 Broker 的 Feature 列表
    pub fn get_broker_features(&self, broker_id: BrokerId) -> Option<Vec<BrokerFeature>> {
        let map = self.broker_features.read().unwrap();
        map.get(&broker_id).cloned()
    }

    /// 获取已注册 Broker 数量
    pub fn broker_count(&self) -> usize {
        let map = self.broker_features.read().unwrap();
        map.len()
    }

    /// 获取不兼容的 Broker 列表
    pub fn incompatible_brokers(&self) -> Vec<BrokerId> {
        let map = self.broker_features.read().unwrap();
        if map.is_empty() {
            return Vec::new();
        }

        let finalized = self.compute_finalized_internal(&map);
        let mut incompatible = Vec::new();

        for (broker_id, features) in map.iter() {
            for f in features {
                if let Some(ff) = finalized.iter().find(|ff| ff.name == f.name) {
                    if f.min_version > ff.version {
                        incompatible.push(*broker_id);
                        break;
                    }
                }
            }
        }

        incompatible
    }

    /// 内部方法: 从已有 map 计算 finalized
    fn compute_finalized_internal(
        &self,
        map: &HashMap<BrokerId, Vec<BrokerFeature>>,
    ) -> Vec<FinalizedFeature> {
        if map.is_empty() {
            return Vec::new();
        }

        let mut all_names: Vec<String> = Vec::new();
        for features in map.values() {
            for f in features {
                if !all_names.contains(&f.name) {
                    all_names.push(f.name.clone());
                }
            }
        }

        let mut result = Vec::new();
        for name in &all_names {
            let mut max_versions: Vec<i16> = Vec::new();
            let mut all_brokers_have = true;

            for features in map.values() {
                match features.iter().find(|f| &f.name == name) {
                    Some(f) => max_versions.push(f.max_version),
                    None => {
                        all_brokers_have = false;
                        break;
                    }
                }
            }

            if all_brokers_have {
                let finalized = *max_versions.iter().min().unwrap();
                result.push(FinalizedFeature {
                    name: name.clone(),
                    version: finalized,
                });
            }
        }

        result
    }
}

impl Default for FeatureManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_feature(name: &str, min: i16, max: i16) -> BrokerFeature {
        BrokerFeature {
            name: name.to_string(),
            min_version: min,
            max_version: max,
        }
    }

    #[test]
    fn test_empty_manager() {
        let fm = FeatureManager::new();
        assert!(fm.compute_finalized().is_empty());
        assert!(fm.is_compatible());
        assert_eq!(fm.broker_count(), 0);
    }

    #[test]
    fn test_single_broker() {
        let fm = FeatureManager::new();
        fm.update_broker_features(BrokerId(1), vec![make_feature("metadata.version", 0, 17)]);

        let finalized = fm.compute_finalized();
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].name, "metadata.version");
        assert_eq!(finalized[0].version, 17);
        assert!(fm.is_compatible());
    }

    #[test]
    fn test_two_brokers_compatible() {
        let fm = FeatureManager::new();
        fm.update_broker_features(BrokerId(1), vec![make_feature("metadata.version", 0, 17)]);
        fm.update_broker_features(BrokerId(2), vec![make_feature("metadata.version", 0, 16)]);

        let finalized = fm.compute_finalized();
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].version, 16); // min(17, 16) = 16
        assert!(fm.is_compatible());
    }

    #[test]
    fn test_incompatible_broker() {
        let fm = FeatureManager::new();
        fm.update_broker_features(BrokerId(1), vec![make_feature("metadata.version", 0, 16)]);
        // Broker 3 要求最低版本 17, 但集群最高只支持 16
        fm.update_broker_features(BrokerId(3), vec![make_feature("metadata.version", 17, 17)]);

        assert!(!fm.is_compatible());
        let incompatible = fm.incompatible_brokers();
        assert!(incompatible.contains(&BrokerId(3)));
    }

    #[test]
    fn test_multiple_features() {
        let fm = FeatureManager::new();
        fm.update_broker_features(
            BrokerId(1),
            vec![
                make_feature("metadata.version", 0, 17),
                make_feature("group.version", 0, 5),
            ],
        );
        fm.update_broker_features(
            BrokerId(2),
            vec![
                make_feature("metadata.version", 0, 16),
                make_feature("group.version", 0, 4),
            ],
        );

        let finalized = fm.compute_finalized();
        assert_eq!(finalized.len(), 2);

        let mv = finalized
            .iter()
            .find(|f| f.name == "metadata.version")
            .unwrap();
        assert_eq!(mv.version, 16);

        let gv = finalized
            .iter()
            .find(|f| f.name == "group.version")
            .unwrap();
        assert_eq!(gv.version, 4);
    }

    #[test]
    fn test_broker_missing_feature() {
        let fm = FeatureManager::new();
        fm.update_broker_features(
            BrokerId(1),
            vec![
                make_feature("metadata.version", 0, 17),
                make_feature("group.version", 0, 5),
            ],
        );
        // Broker 2 不支持 group.version
        fm.update_broker_features(BrokerId(2), vec![make_feature("metadata.version", 0, 16)]);

        let finalized = fm.compute_finalized();
        // group.version 不在 finalized 中 (不是所有 Broker 都支持)
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].name, "metadata.version");
    }

    #[test]
    fn test_remove_broker() {
        let fm = FeatureManager::new();
        fm.update_broker_features(BrokerId(1), vec![make_feature("metadata.version", 0, 17)]);
        fm.update_broker_features(BrokerId(2), vec![make_feature("metadata.version", 0, 16)]);

        assert_eq!(fm.broker_count(), 2);
        assert_eq!(fm.compute_finalized()[0].version, 16);

        fm.remove_broker(BrokerId(2));
        assert_eq!(fm.broker_count(), 1);
        assert_eq!(fm.compute_finalized()[0].version, 17);
    }

    #[test]
    fn test_get_broker_features() {
        let fm = FeatureManager::new();
        let features = vec![make_feature("metadata.version", 0, 17)];
        fm.update_broker_features(BrokerId(1), features.clone());

        let retrieved = fm.get_broker_features(BrokerId(1));
        assert_eq!(retrieved, Some(features));
        assert_eq!(fm.get_broker_features(BrokerId(99)), None);
    }
}
