//! Consumer Partition Assignor
//!
//! 实现 Kafka Consumer Group 的三种 Partition 分配策略:
//! - **RangeAssignor**: 按 Topic 连续分段分配 (默认)
//! - **RoundRobinAssignor**: 全部 Partition 轮询分配
//! - **CooperativeStickyAssignor**: 粘性分配，减少 Rebalance 抖动
//!
//! 协议名称与 Kafka Java Client 完全一致，确保混合消费组互操作。

use std::collections::HashMap;

use crate::metadata::TopicPartition;

// ─── 协议名称常量 (与 Kafka Java Client 一致) ──────────────────────

/// Range Assignor 协议名称
pub const RANGE_ASSIGNOR: &str = "range";
/// RoundRobin Assignor 协议名称
pub const ROUNDROBIN_ASSIGNOR: &str = "roundrobin";
/// CooperativeSticky Assignor 协议名称
pub const COOPERATIVE_STICKY_ASSIGNOR: &str = "cooperative-sticky";

// ─── 输入数据结构 ──────────────────────────────────────────────────

/// 消费组成员订阅信息
#[derive(Debug, Clone)]
pub struct MemberSubscription {
    /// 成员 ID
    pub member_id: String,
    /// 订阅的 Topic 列表
    pub topics: Vec<String>,
}

/// 分配结果: member_id → 分配的 TopicPartition 列表
pub type AssignmentResult = HashMap<String, Vec<TopicPartition>>;

// ─── PartitionAssignor Trait ────────────────────────────────────────

/// Partition 分配策略接口
///
/// 与 Kafka Consumer Group Protocol 对齐:
/// Consumer Leader 在 SyncGroup 前调用此接口计算分配方案，
/// 将结果编码后通过 SyncGroup 响应分发给各成员。
pub trait PartitionAssignor: Send + Sync {
    /// 协议名称 (与 Kafka 客户端上报的 protocol_name 一致)
    fn name(&self) -> &str;

    /// 计算 Partition 分配方案
    ///
    /// # Arguments
    /// * `members` - 消费组全部成员的订阅信息
    /// * `partitions` - 所有订阅 Topic 的全部 Partition
    ///
    /// # Returns
    /// member_id → 分配的 TopicPartition 列表
    fn assign(
        &self,
        members: &[MemberSubscription],
        partitions: &[TopicPartition],
    ) -> AssignmentResult;
}

// ─── RangeAssignor ──────────────────────────────────────────────────

/// Range Assignor — 按 Topic 连续分段分配
///
/// 对每个 Topic，将订阅了该 Topic 的成员排序后，
/// 将 Partition 连续分段分配给各成员。
///
/// 特点: 简单高效，但可能在 Topic 数量不均时产生倾斜。
/// Kafka 默认 assignor。
pub struct RangeAssignor;

impl PartitionAssignor for RangeAssignor {
    fn name(&self) -> &str {
        RANGE_ASSIGNOR
    }

    fn assign(
        &self,
        members: &[MemberSubscription],
        partitions: &[TopicPartition],
    ) -> AssignmentResult {
        let mut result: AssignmentResult = HashMap::new();
        // 初始化: 每个 member 至少有空列表
        for m in members {
            result.insert(m.member_id.clone(), Vec::new());
        }

        // 按 Topic 分组 partitions
        let mut topic_partitions: HashMap<&str, Vec<&TopicPartition>> = HashMap::new();
        for tp in partitions {
            topic_partitions
                .entry(&tp.topic)
                .or_default()
                .push(tp);
        }

        // 对每个 Topic 的 partitions 排序
        for tps in topic_partitions.values_mut() {
            tps.sort_by_key(|tp| tp.partition);
        }

        // 对每个 Topic 独立分配
        for (topic, tps) in &topic_partitions {
            // 找到订阅了该 Topic 的成员，排序
            let mut subscribed_members: Vec<&MemberSubscription> = members
                .iter()
                .filter(|m| m.topics.iter().any(|t| t == topic))
                .collect();
            subscribed_members.sort_by(|a, b| a.member_id.cmp(&b.member_id));

            if subscribed_members.is_empty() || tps.is_empty() {
                continue;
            }

            let num_members = subscribed_members.len();
            let num_partitions = tps.len();
            let partitions_per_member = num_partitions / num_members;
            let extra = num_partitions % num_members;

            let mut offset = 0;
            for (i, member) in subscribed_members.iter().enumerate() {
                // 前 extra 个成员多分一个 partition
                let count = partitions_per_member + if i < extra { 1 } else { 0 };
                let member_partitions = &tps[offset..offset + count];
                result
                    .entry(member.member_id.clone())
                    .or_default()
                    .extend(member_partitions.iter().map(|tp| (*tp).clone()));
                offset += count;
            }
        }

        result
    }
}

// ─── RoundRobinAssignor ─────────────────────────────────────────────

/// RoundRobin Assignor — 全部 Partition 轮询分配
///
/// 将所有 Topic 的全部 Partition 排序后，依次轮询分配给各成员。
///
/// 特点: 分配更均匀，但要求所有成员订阅相同 Topic 集合时效果最佳。
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &str {
        ROUNDROBIN_ASSIGNOR
    }

    fn assign(
        &self,
        members: &[MemberSubscription],
        partitions: &[TopicPartition],
    ) -> AssignmentResult {
        let mut result: AssignmentResult = HashMap::new();
        for m in members {
            result.insert(m.member_id.clone(), Vec::new());
        }

        if members.is_empty() || partitions.is_empty() {
            return result;
        }

        // 构建 member 订阅查找表
        let member_topics: HashMap<&str, &[String]> = members
            .iter()
            .map(|m| (m.member_id.as_str(), m.topics.as_slice()))
            .collect();

        // 排序成员 (保证确定性)
        let mut sorted_members: Vec<&MemberSubscription> = members.iter().collect();
        sorted_members.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        // 排序全部 partitions (topic 字典序, partition 升序)
        let mut sorted_partitions = partitions.to_vec();
        sorted_partitions.sort_by(|a, b| {
            a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition))
        });

        // 轮询分配: 跳过不订阅该 partition 所在 topic 的成员
        let mut member_idx = 0;
        for tp in &sorted_partitions {
            let mut attempts = 0;
            while attempts < sorted_members.len() {
                let member = sorted_members[member_idx];
                member_idx = (member_idx + 1) % sorted_members.len();
                attempts += 1;

                // 检查该成员是否订阅了这个 topic
                if let Some(topics) = member_topics.get(member.member_id.as_str()) {
                    if topics.iter().any(|t| t == &tp.topic) {
                        result
                            .entry(member.member_id.clone())
                            .or_default()
                            .push(tp.clone());
                        break;
                    }
                }
            }
        }

        result
    }
}

// ─── CooperativeStickyAssignor ──────────────────────────────────────

/// CooperativeSticky Assignor — 粘性分配，减少 Rebalance 抖动
///
/// 尽量保持成员原有的 Partition 分配不变 (sticky)，
/// 仅在成员变化时将多余的 Partition 从过载成员迁移到不足成员。
///
/// 特点:
/// - 最小化 Rebalance 时的 Partition 迁移量
/// - 使用 cooperative protocol (增量式 Rebalance)
/// - 推荐用于生产环境
pub struct CooperativeStickyAssignor;

impl CooperativeStickyAssignor {
    /// 带当前分配方案的分配 (用于 Rebalance 场景)
    ///
    /// # Arguments
    /// * `members` - 当前成员列表
    /// * `partitions` - 全部 Partition
    /// * `current_assignment` - 上一轮分配方案 (member_id → partitions)
    pub fn assign_with_current(
        &self,
        members: &[MemberSubscription],
        partitions: &[TopicPartition],
        current_assignment: &HashMap<String, Vec<TopicPartition>>,
    ) -> AssignmentResult {
        let mut result: AssignmentResult = HashMap::new();
        for m in members {
            result.insert(m.member_id.clone(), Vec::new());
        }

        if members.is_empty() {
            return result;
        }

        let active_member_ids: Vec<&str> = {
            let mut ids: Vec<&str> = members.iter().map(|m| m.member_id.as_str()).collect();
            ids.sort();
            ids
        };

        // 构建 member → subscribed topics 查找表
        let member_subscribed_topics: HashMap<&str, &Vec<String>> = members
            .iter()
            .map(|m| (m.member_id.as_str(), &m.topics))
            .collect();

        // Step 1: 尽量保留当前分配 (sticky)
        let mut unassigned: Vec<TopicPartition> = Vec::new();
        let mut current_owner: HashMap<TopicPartition, String> = HashMap::new();

        for (member_id, assigned_tps) in current_assignment {
            // 只保留成员仍然活跃且仍然订阅的 partition
            if !active_member_ids.contains(&member_id.as_str()) {
                // 成员已离开，其 partition 变为未分配
                unassigned.extend(assigned_tps.iter().cloned());
                continue;
            }

            let subscribed = member_subscribed_topics
                .get(member_id.as_str())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            for tp in assigned_tps {
                if subscribed.iter().any(|t| t == &tp.topic) {
                    // 成员仍然活跃且仍然订阅 → 保留
                    result
                        .entry(member_id.clone())
                        .or_default()
                        .push(tp.clone());
                    current_owner.insert(tp.clone(), member_id.clone());
                } else {
                    // 成员不再订阅该 topic → 释放
                    unassigned.push(tp.clone());
                }
            }
        }

        // 添加新增的 partition (不在 current_assignment 中的)
        // 使用 original_assignment (原始全部) 来判断哪些 partition 是全新的
        let all_known: std::collections::HashSet<_> = current_assignment
            .values()
            .flat_map(|tps| tps.iter())
            .collect();
        for tp in partitions {
            if !all_known.contains(tp) {
                unassigned.push(tp.clone());
            }
        }

        // 排序未分配 partition 保证确定性
        unassigned.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));

        // Step 2: 将未分配的 partition 分配给当前拥有最少 partition 的成员
        for tp in unassigned {
            // 找到订阅了该 topic 且拥有最少 partition 的成员
            let best_member = active_member_ids
                .iter()
                .filter(|mid| {
                    member_subscribed_topics
                        .get(**mid)
                        .map(|topics| topics.iter().any(|t| t == &tp.topic))
                        .unwrap_or(false)
                })
                .min_by_key(|mid| result.get(**mid).map(|v| v.len()).unwrap_or(0))
                .map(|mid| mid.to_string());

            if let Some(member_id) = best_member {
                result
                    .entry(member_id)
                    .or_default()
                    .push(tp);
            }
        }

        result
    }
}

impl PartitionAssignor for CooperativeStickyAssignor {
    fn name(&self) -> &str {
        COOPERATIVE_STICKY_ASSIGNOR
    }

    fn assign(
        &self,
        members: &[MemberSubscription],
        partitions: &[TopicPartition],
    ) -> AssignmentResult {
        // 无当前分配方案时，退化为空分配后重新分配
        self.assign_with_current(members, partitions, &HashMap::new())
    }
}

// ─── 工具函数 ──────────────────────────────────────────────────────

/// 根据协议名称获取对应的 Assignor 实例
pub fn get_assignor(name: &str) -> Option<Box<dyn PartitionAssignor>> {
    match name {
        RANGE_ASSIGNOR => Some(Box::new(RangeAssignor)),
        ROUNDROBIN_ASSIGNOR => Some(Box::new(RoundRobinAssignor)),
        COOPERATIVE_STICKY_ASSIGNOR => Some(Box::new(CooperativeStickyAssignor)),
        _ => None,
    }
}

/// 从成员上报的协议列表中选择所有成员都支持的一个协议
///
/// 与 Kafka GroupCoordinator 的选择逻辑一致:
/// 找到所有成员都支持的第一个协议 (按第一个成员上报的顺序)。
pub fn select_protocol(
    members_protocols: &[Vec<String>],
) -> Option<String> {
    if members_protocols.is_empty() {
        return None;
    }

    // 取第一个成员上报的协议顺序作为优先级
    for protocol in &members_protocols[0] {
        let all_support = members_protocols
            .iter()
            .all(|protos| protos.iter().any(|p| p == protocol));
        if all_support {
            return Some(protocol.clone());
        }
    }

    None
}

// ─── 测试 ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_members(ids: &[&str], topics: &[Vec<&str>]) -> Vec<MemberSubscription> {
        ids.iter()
            .zip(topics.iter())
            .map(|(id, tops)| MemberSubscription {
                member_id: id.to_string(),
                topics: tops.iter().map(|t| t.to_string()).collect(),
            })
            .collect()
    }

    fn make_partitions(topic: &str, count: i32) -> Vec<TopicPartition> {
        (0..count)
            .map(|p| TopicPartition::new(topic, p))
            .collect()
    }

    // ─── RangeAssignor 测试 ──────────────────────────────────────

    #[test]
    fn test_range_single_topic_even() {
        let assignor = RangeAssignor;
        let members = make_members(
            &["m1", "m2"],
            &[vec!["t1"], vec!["t1"]],
        );
        let partitions = make_partitions("t1", 4);
        let result = assignor.assign(&members, &partitions);

        assert_eq!(result["m1"].len(), 2);
        assert_eq!(result["m2"].len(), 2);
        // m1 应该拿到前两个
        assert_eq!(result["m1"][0].partition, 0);
        assert_eq!(result["m1"][1].partition, 1);
        // m2 应该拿到后两个
        assert_eq!(result["m2"][0].partition, 2);
        assert_eq!(result["m2"][1].partition, 3);
    }

    #[test]
    fn test_range_single_topic_uneven() {
        let assignor = RangeAssignor;
        let members = make_members(
            &["m1", "m2", "m3"],
            &[vec!["t1"], vec!["t1"], vec!["t1"]],
        );
        let partitions = make_partitions("t1", 5);
        let result = assignor.assign(&members, &partitions);

        // 5 / 3 = 1 余 2 → 前两个成员各 2 个, 最后一个 1 个
        assert_eq!(result["m1"].len(), 2);
        assert_eq!(result["m2"].len(), 2);
        assert_eq!(result["m3"].len(), 1);
    }

    #[test]
    fn test_range_multiple_topics() {
        let assignor = RangeAssignor;
        let members = make_members(
            &["m1", "m2"],
            &[vec!["t1", "t2"], vec!["t1", "t2"]],
        );
        let mut partitions = make_partitions("t1", 3);
        partitions.extend(make_partitions("t2", 3));
        let result = assignor.assign(&members, &partitions);

        // 每个 topic 独立分配: t1 的 3 个分给 m1(2)+m2(1), t2 同理
        assert_eq!(result["m1"].len(), 4); // t1:2 + t2:2
        assert_eq!(result["m2"].len(), 2); // t1:1 + t2:1
    }

    // ─── RoundRobinAssignor 测试 ─────────────────────────────────

    #[test]
    fn test_roundrobin_even() {
        let assignor = RoundRobinAssignor;
        let members = make_members(
            &["m1", "m2"],
            &[vec!["t1"], vec!["t1"]],
        );
        let partitions = make_partitions("t1", 4);
        let result = assignor.assign(&members, &partitions);

        assert_eq!(result["m1"].len(), 2);
        assert_eq!(result["m2"].len(), 2);
    }

    #[test]
    fn test_roundrobin_uneven() {
        let assignor = RoundRobinAssignor;
        let members = make_members(
            &["m1", "m2", "m3"],
            &[vec!["t1"], vec!["t1"], vec!["t1"]],
        );
        let partitions = make_partitions("t1", 7);
        let result = assignor.assign(&members, &partitions);

        // 7 / 3 = 2 余 1
        assert_eq!(result["m1"].len(), 3);
        assert_eq!(result["m2"].len(), 2);
        assert_eq!(result["m3"].len(), 2);
    }

    // ─── CooperativeStickyAssignor 测试 ──────────────────────────

    #[test]
    fn test_sticky_initial_assignment() {
        let assignor = CooperativeStickyAssignor;
        let members = make_members(
            &["m1", "m2"],
            &[vec!["t1"], vec!["t1"]],
        );
        let partitions = make_partitions("t1", 4);
        let result = assignor.assign(&members, &partitions);

        // 初始分配 (无 current) 应均衡分配
        assert_eq!(result["m1"].len(), 2);
        assert_eq!(result["m2"].len(), 2);
    }

    #[test]
    fn test_sticky_preserves_assignment() {
        let assignor = CooperativeStickyAssignor;
        let members = make_members(
            &["m1", "m2"],
            &[vec!["t1"], vec!["t1"]],
        );
        let partitions = make_partitions("t1", 4);

        // 模拟上一轮分配: m1 有 [0,1], m2 有 [2,3]
        let mut current = HashMap::new();
        current.insert("m1".to_string(), vec![
            TopicPartition::new("t1", 0),
            TopicPartition::new("t1", 1),
        ]);
        current.insert("m2".to_string(), vec![
            TopicPartition::new("t1", 2),
            TopicPartition::new("t1", 3),
        ]);

        let result = assignor.assign_with_current(&members, &partitions, &current);

        // 成员不变 → 分配应该完全不变 (sticky)
        assert_eq!(result["m1"].len(), 2);
        assert_eq!(result["m2"].len(), 2);
        assert!(result["m1"].iter().any(|tp| tp.partition == 0));
        assert!(result["m1"].iter().any(|tp| tp.partition == 1));
        assert!(result["m2"].iter().any(|tp| tp.partition == 2));
        assert!(result["m2"].iter().any(|tp| tp.partition == 3));
    }

    #[test]
    fn test_sticky_member_leaves() {
        let assignor = CooperativeStickyAssignor;
        // m2 离开，只剩 m1
        let members = make_members(&["m1"], &[vec!["t1"]]);
        let partitions = make_partitions("t1", 4);

        let mut current = HashMap::new();
        current.insert("m1".to_string(), vec![
            TopicPartition::new("t1", 0),
            TopicPartition::new("t1", 1),
        ]);
        current.insert("m2".to_string(), vec![
            TopicPartition::new("t1", 2),
            TopicPartition::new("t1", 3),
        ]);

        let result = assignor.assign_with_current(&members, &partitions, &current);

        // m1 应该拿到全部 4 个 partition
        assert_eq!(result["m1"].len(), 4);
    }

    // ─── 协议选择测试 ────────────────────────────────────────────

    #[test]
    fn test_select_protocol_common() {
        let protocols = vec![
            vec!["range".to_string(), "roundrobin".to_string()],
            vec!["roundrobin".to_string(), "range".to_string()],
        ];
        // 第一个成员的优先级: range > roundrobin
        // 两个成员都支持 range → 选择 range
        assert_eq!(select_protocol(&protocols), Some("range".to_string()));
    }

    #[test]
    fn test_select_protocol_no_common() {
        let protocols = vec![
            vec!["range".to_string()],
            vec!["roundrobin".to_string()],
        ];
        // 没有共同协议
        assert_eq!(select_protocol(&protocols), None);
    }

    #[test]
    fn test_get_assignor() {
        assert!(get_assignor("range").is_some());
        assert!(get_assignor("roundrobin").is_some());
        assert!(get_assignor("cooperative-sticky").is_some());
        assert!(get_assignor("unknown").is_none());
    }

    #[test]
    fn test_assignor_names() {
        assert_eq!(RangeAssignor.name(), "range");
        assert_eq!(RoundRobinAssignor.name(), "roundrobin");
        assert_eq!(CooperativeStickyAssignor.name(), "cooperative-sticky");
    }
}
