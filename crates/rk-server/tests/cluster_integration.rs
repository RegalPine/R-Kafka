//! Cluster Integration Tests — 多节点集群集成测试
//!
//! 模拟 3 节点集群的引导、注册、心跳和状态转换流程。
//! 验证 ClusterBootstrap + BrokerRegistry + MetadataStateMachine 协同工作。

use rk_controller::{
    initialize_cluster_metadata, ClusterBootstrap, ClusterBootstrapConfig, ClusterState,
    MetadataStateMachine,
};
use rk_core::types::BrokerId;

fn broker(id: i32) -> BrokerId {
    BrokerId(id)
}

fn cluster_config(expected: u32) -> ClusterBootstrapConfig {
    ClusterBootstrapConfig {
        cluster_id: "integration-test-cluster".to_string(),
        expected_broker_count: expected,
        min_broker_count: 1,
        heartbeat_timeout_ms: 30_000,
        bootstrap_timeout_ms: 60_000,
        combined_mode: true,
    }
}

/// 测试 1: 3 节点集群引导
///
/// 验证: 首个 Broker 成为 Controller，后续 Broker 注册，集群最终 Active。
#[test]
fn test_3_node_cluster_bootstrap() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(3));

    // Step 1: Broker 1 启动 → Controller
    let node1 = bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, Some("rack-a".to_string()))
        .unwrap();
    assert_eq!(node1.node_id, 1);
    assert_eq!(bootstrap.state(), ClusterState::Degraded); // 1/3
    assert_eq!(bootstrap.alive_broker_count(), 1);

    // Step 2: Broker 2 加入
    bootstrap
        .register_broker(broker(2), "10.0.0.2", 9092, Some("rack-b".to_string()))
        .unwrap();
    assert_eq!(bootstrap.state(), ClusterState::Degraded); // 2/3
    assert_eq!(bootstrap.alive_broker_count(), 2);

    // Step 3: Broker 3 加入 → Active
    bootstrap
        .register_broker(broker(3), "10.0.0.3", 9092, Some("rack-c".to_string()))
        .unwrap();
    assert_eq!(bootstrap.state(), ClusterState::Active); // 3/3
    assert!(bootstrap.is_active());
    assert_eq!(bootstrap.alive_broker_count(), 3);

    // 验证 Controller 信息
    let ctrl = bootstrap.controller().unwrap();
    assert_eq!(ctrl.node_id, 1);
    assert_eq!(ctrl.address, "10.0.0.1:9092");

    // 验证引导耗时
    let dur = bootstrap.bootstrap_duration().unwrap();
    assert!(dur.as_secs() < 1);
}

/// 测试 2: 动态模式集群 (expected=0)
///
/// 验证: 不预设 Broker 数量，有 >= min_broker_count 即 Active。
#[test]
fn test_dynamic_mode_cluster() {
    let config = ClusterBootstrapConfig {
        expected_broker_count: 0, // 动态模式
        min_broker_count: 1,
        ..cluster_config(0)
    };
    let mut bootstrap = ClusterBootstrap::new(config);

    // 1 个 Broker 即 Active
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();
    assert!(bootstrap.is_active());

    // 继续加入更多 Broker 仍然 Active
    bootstrap
        .register_broker(broker(2), "10.0.0.2", 9092, None)
        .unwrap();
    assert!(bootstrap.is_active());
    assert_eq!(bootstrap.alive_broker_count(), 2);
}

/// 测试 3: Broker 注销导致降级
#[test]
fn test_broker_unregister_causes_degradation() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(3));

    // 引导 3 节点 → Active
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();
    bootstrap
        .register_broker(broker(2), "10.0.0.2", 9092, None)
        .unwrap();
    bootstrap
        .register_broker(broker(3), "10.0.0.3", 9092, None)
        .unwrap();
    assert!(bootstrap.is_active());

    // Broker 3 注销 → Degraded
    bootstrap.unregister_broker(broker(3)).unwrap();
    assert_eq!(bootstrap.state(), ClusterState::Degraded);
    assert_eq!(bootstrap.alive_broker_count(), 2);
}

/// 测试 4: 心跳保活
#[test]
fn test_heartbeat_keepalive() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(0));
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();
    bootstrap
        .register_broker(broker(2), "10.0.0.2", 9092, None)
        .unwrap();

    // 心跳正常
    assert!(bootstrap.heartbeat(broker(1)).is_ok());
    assert!(bootstrap.heartbeat(broker(2)).is_ok());
}

/// 测试 5: 心跳未知 Broker 失败
#[test]
fn test_heartbeat_unknown_broker_fails() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(0));
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();

    // Broker 99 未注册
    assert!(bootstrap.heartbeat(broker(99)).is_err());
}

/// 测试 6: Metadata 初始化
#[test]
fn test_cluster_metadata_initialization() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(0));
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, Some("rack-a".to_string()))
        .unwrap();
    bootstrap
        .register_broker(broker(2), "10.0.0.2", 9092, Some("rack-b".to_string()))
        .unwrap();
    bootstrap
        .register_broker(broker(3), "10.0.0.3", 9092, Some("rack-c".to_string()))
        .unwrap();

    // 初始化 Metadata
    let mut sm = MetadataStateMachine::new();
    initialize_cluster_metadata(&mut sm, bootstrap.broker_registry()).unwrap();

    // 验证所有 Broker 已写入 SM
    let alive = sm.list_alive_brokers();
    assert_eq!(alive.len(), 3);

    // 验证 Broker 信息
    let b1 = sm.get_broker(1).unwrap();
    assert_eq!(b1.host, "10.0.0.1");
    assert_eq!(b1.rack, Some("rack-a".to_string()));

    let b2 = sm.get_broker(2).unwrap();
    assert_eq!(b2.host, "10.0.0.2");
    assert_eq!(b2.rack, Some("rack-b".to_string()));

    let b3 = sm.get_broker(3).unwrap();
    assert_eq!(b3.host, "10.0.0.3");
    assert_eq!(b3.rack, Some("rack-c".to_string()));
}

/// 测试 7: 重复引导失败
#[test]
fn test_duplicate_bootstrap_fails() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(3));
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();

    // 再次引导应失败
    let result = bootstrap.bootstrap_first_broker(broker(2), "10.0.0.2", 9092, None);
    assert!(result.is_err());
}

/// 测试 8: 未引导时注册失败
#[test]
fn test_register_before_bootstrap_fails() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(3));

    // 未引导，注册应失败
    let result = bootstrap.register_broker(broker(2), "10.0.0.2", 9092, None);
    assert!(result.is_err());
}

/// 测试 9: 集群摘要输出
#[test]
fn test_cluster_summary_display() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(0));
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();

    let summary = bootstrap.summary();
    let display = format!("{}", summary);
    assert!(display.contains("integration-test-cluster"));
    assert!(display.contains("active"));
    // Summary contains controller_broker_id, not address
    assert!(display.contains("Some(1)"));
}

/// 测试 10: 完整集群生命周期
///
/// 模拟: 引导 → 注册 → 心跳 → 注销 → 重新注册 → 验证状态
#[test]
fn test_full_cluster_lifecycle() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(3));

    // Phase 1: 引导
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, Some("rack-a".to_string()))
        .unwrap();
    bootstrap
        .register_broker(broker(2), "10.0.0.2", 9092, Some("rack-b".to_string()))
        .unwrap();
    bootstrap
        .register_broker(broker(3), "10.0.0.3", 9092, Some("rack-c".to_string()))
        .unwrap();
    assert!(bootstrap.is_active());

    // Phase 2: 心跳
    for id in [1, 2, 3] {
        assert!(bootstrap.heartbeat(broker(id)).is_ok());
    }

    // Phase 3: Broker 2 注销
    bootstrap.unregister_broker(broker(2)).unwrap();
    assert_eq!(bootstrap.state(), ClusterState::Degraded);
    assert_eq!(bootstrap.alive_broker_count(), 2);

    // Phase 4: Broker 2 重新注册
    bootstrap
        .register_broker(broker(2), "10.0.0.2", 9092, Some("rack-b".to_string()))
        .unwrap();
    assert_eq!(bootstrap.state(), ClusterState::Active);
    assert_eq!(bootstrap.alive_broker_count(), 3);

    // Phase 5: 初始化 Metadata
    let mut sm = MetadataStateMachine::new();
    initialize_cluster_metadata(&mut sm, bootstrap.broker_registry()).unwrap();
    assert_eq!(sm.list_alive_brokers().len(), 3);
}

/// 测试 11: 注册 Broker ID 列表
#[test]
fn test_registered_broker_ids() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(0));
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();
    bootstrap
        .register_broker(broker(5), "10.0.0.5", 9092, None)
        .unwrap();
    bootstrap
        .register_broker(broker(10), "10.0.0.10", 9092, None)
        .unwrap();

    let ids = bootstrap.registered_broker_ids();
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&1));
    assert!(ids.contains(&5));
    assert!(ids.contains(&10));
}

/// 测试 12: detect_and_update_state 返回空列表 (无超时)
#[test]
fn test_detect_no_stale_brokers() {
    let mut bootstrap = ClusterBootstrap::new(cluster_config(0));
    bootstrap
        .bootstrap_first_broker(broker(1), "10.0.0.1", 9092, None)
        .unwrap();

    // 刚注册，不会超时
    let stale = bootstrap.detect_and_update_state();
    assert!(stale.is_empty());
}
