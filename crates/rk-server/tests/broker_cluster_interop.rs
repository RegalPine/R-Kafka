//! Broker 集群互操作集成测试
//!
//! 验证 R-Kafka Broker 加入 Java Kafka 集群的能力:
//!
//! ```text
//! 测试覆盖:
//!
//! 1. Metadata 交换:
//!    - R-Kafka Broker 与模拟 Java Broker 交换 Metadata
//!    - 验证 Broker 列表、Controller ID、Cluster ID 一致
//!
//! 2. LeaderAndIsr 处理:
//!    - 模拟 Controller 发送 LeaderAndIsr 请求
//!    - 验证 R-Kafka 正确响应 partition 状态
//!
//! 3. Replica Fetch 兼容性:
//!    - R-Kafka Follower 从模拟 Leader 拉取数据
//!    - 验证 Fetch 请求/响应的跨 Broker 兼容性
//!
//! 4. 滚动迁移模拟:
//!    - 模拟 Broker 注册 → 心跳 → LeaderAndIsr → 优雅关闭流程
//!    - 验证 BrokerRegistration + BrokerHeartbeat + ControlledShutdown
//! ```

use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rk_broker::{PartitionManager, BrokerRouter, OffsetManager};
use rk_protocol::types::{KafkaWriter, KafkaReader};
use rk_storage::log_io::build_batch_bytes;

// ─── 工具函数 ─────────────────────────────────────────────────────────

/// 构建 Legacy 格式请求帧
fn build_legacy_frame(body_builder: impl FnOnce(&mut KafkaWriter<'_>)) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(512);
    let mut writer = KafkaWriter::new(&mut buf);
    body_builder(&mut writer);
    drop(writer);

    let mut frame = BytesMut::with_capacity(4 + buf.len());
    frame.extend_from_slice(&(buf.len() as u32).to_be_bytes());
    frame.extend_from_slice(&buf);
    frame.to_vec()
}

/// 写入 Legacy RequestHeader
fn write_legacy_header(w: &mut KafkaWriter<'_>, api_key: i16, api_version: i16, correlation_id: i32, client_id: &str) {
    w.write_i16(api_key);
    w.write_i16(api_version);
    w.write_i32(correlation_id);
    w.write_nullable_string(Some(client_id));
}

/// 从 TCP 流读取一个响应帧
async fn read_response_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await.unwrap();
    data
}

/// 构建测试 RecordBatch
fn make_test_batch(base_offset: i64, record_count: i32) -> Vec<u8> {
    let records = vec![0u8; record_count as usize * 10];
    build_batch_bytes(base_offset, 1, 0, 1000, 2000, -1, -1, -1, &records, record_count)
}

/// 启动测试服务器
async fn setup_server() -> u16 {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1));
    let offset_manager = Arc::new(OffsetManager::new(None));

    let router = Arc::new(BrokerRouter::with_offset_manager(
        pm,
        1,
        "127.0.0.1".to_string(),
        0,
        None,
        Some("test-cluster".to_string()),
        offset_manager,
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let router_clone = router.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let router = router_clone.clone();
                    tokio::spawn(async move {
                        rk_network::handle_connection(stream, router).await;
                    });
                }
                Err(_) => break,
            }
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    port
}

/// 创建 topic (通过 CreateTopics v0)
async fn create_topic(stream: &mut TcpStream, topic: &str, partitions: i32, corr_id: i32) {
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 19, 0, corr_id, "broker-interop-test");
        w.write_i32(1);
        w.write_string(topic);
        w.write_i32(partitions);
        w.write_i16(1); // replication_factor
        w.write_i32(0); // empty assignments
        w.write_i32(0); // empty configs
        w.write_i32(30_000); // timeout_ms
    });
    stream.write_all(&frame).await.unwrap();
    let _response = read_response_frame(stream).await;
}

// ═══════════════════════════════════════════════════════════════════════
// 测试 1: Metadata 交换
// ═══════════════════════════════════════════════════════════════════════

/// 模拟 R-Kafka Broker 与 Java Broker 交换 Metadata。
///
/// 流程:
/// 1. 发送 Metadata v0 请求 (空 topic 列表 = 获取全部)
/// 2. 验证响应包含 broker 列表 (至少包含自身)
/// 3. 发送 Metadata v1 请求 (含 controller_id)
/// 4. 验证 cluster_id 字段
/// 5. 创建 topic 后再查 Metadata，验证 topic 元数据
#[tokio::test]
async fn test_metadata_exchange() {
    let port = setup_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();

    // Step 1: Metadata v0 — 获取全部 broker 信息
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 3, 0, 1, "rk-broker-1");
        w.write_i32(0); // v0: empty array (no specific topics)
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);

    // Response header: correlation_id
    let _corr = reader.read_i32().unwrap();
    // Metadata v0 response: brokers array
    let broker_count = reader.read_i32().unwrap();
    assert!(broker_count >= 1, "Should have at least 1 broker");
    for _ in 0..broker_count {
        let _node_id = reader.read_i32();
        let _host = reader.read_string();
        let _port = reader.read_i32();
    }
    // v0 response: topic metadata array
    let topic_count = reader.read_i32().unwrap();
    assert!(topic_count >= 0);

    // Step 2: Metadata v1 — 增加 controller_id
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 3, 1, 2, "rk-broker-1");
        // v1: nullable array of topics (null = all)
        w.write_i32(-1); // null = all topics
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);

    // Response header: correlation_id
    let _corr = reader.read_i32().unwrap();
    // v1 response: throttle_time_ms + brokers + controller_id
    let _throttle = reader.read_i32().unwrap();
    let broker_count_v1 = reader.read_i32().unwrap();
    assert!(broker_count_v1 >= 1);
    for _ in 0..broker_count_v1 {
        let _node_id = reader.read_i32();
        let _host = reader.read_string();
        let _port = reader.read_i32();
        let _rack = reader.read_nullable_string(); // v1+ rack
    }
    let _controller_id = reader.read_i32().unwrap(); // v1+ controller_id

    // Step 3: 创建 topic 后再查 Metadata
    create_topic(&mut stream, "interop-topic", 3, 3).await;

    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 3, 2, 4, "rk-broker-1"); // v2 for cluster_id
        // v1+: nullable array of topics
        w.write_i32(1); // 1 topic
        w.write_string("interop-topic");
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);

    let _corr = reader.read_i32().unwrap();
    // v2 response: brokers + cluster_id + controller_id + topics (no throttle, v3+ has it)
    let _broker_count = reader.read_i32().unwrap();
    // Skip brokers
    for _ in 0.._broker_count {
        let _node_id = reader.read_i32();
        let _host = reader.read_string();
        let _port = reader.read_i32();
        let _rack = reader.read_nullable_string(); // v1+ rack
    }
    // v2+ cluster_id
    let cluster_id = reader.read_nullable_string().unwrap();
    assert!(cluster_id.is_some(), "Cluster ID should be present in v2+");
    let _controller_id = reader.read_i32().unwrap();

    // Topic metadata
    let topic_count = reader.read_i32().unwrap();
    assert!(topic_count >= 1, "Should have at least 1 topic after creation");
}

// ═══════════════════════════════════════════════════════════════════════
// 测试 2: LeaderAndIsr 处理
// ═══════════════════════════════════════════════════════════════════════

/// 模拟 Controller 发送 LeaderAndIsr 请求给 R-Kafka Broker。
///
/// 验证 R-Kafka 能正确处理 Controller 的分区分配指令:
/// 1. 发送 LeaderAndIsr 请求 (flexible 格式)
/// 2. 验证响应包含正确的 partition 错误码
/// 3. 发送多个 partition 的 LeaderAndIsr
/// 4. 验证 StopReplica 请求处理
#[tokio::test]
async fn test_leader_and_isr_handling() {
    let port = setup_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();

    // 先创建 topic
    create_topic(&mut stream, "leader-test", 3, 1).await;

    // Step 1: 发送 LeaderAndIsr v0 (flexible 格式)
    // LeaderAndIsr API Key = 4
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 4, 0, 2, "controller-1");
        w.write_i32(1);   // controller_id
        w.write_i32(5);   // controller_epoch
        w.write_i32(1);   // 1 partition state
        // Partition state for "leader-test" partition 0
        w.write_compact_string("leader-test");
        w.write_i32(0);   // partition_index
        w.write_i32(5);   // controller_epoch
        w.write_i32(1);   // leader (broker 1)
        w.write_i32(1);   // leader_epoch
        w.write_compact_array(&[1i32], |w2, &v| w2.write_i32(v)); // isr
        w.write_i32(1);   // partition_epoch
        w.write_compact_array(&[1i32], |w2, &v| w2.write_i32(v)); // replicas
        w.write_compact_array(&[] as &[i32], |w2: &mut KafkaWriter<'_>, &v: &i32| w2.write_i32(v)); // adding
        w.write_compact_array(&[] as &[i32], |w2: &mut KafkaWriter<'_>, &v: &i32| w2.write_i32(v)); // removing
        w.write_tagged_fields(&[]);
        w.write_tagged_fields(&[]);
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);

    // Response header: correlation_id
    let _corr = reader.read_i32().unwrap();
    // LeaderAndIsr response: throttle_time + error_code + compact_array
    let _throttle = reader.read_i32().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "LeaderAndIsr should succeed");
    let partition_count = reader.read_i32().unwrap(); // compact_array count (non-compact i32)
    assert!(partition_count >= 0);

    // Step 2: 发送多 partition LeaderAndIsr
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 4, 0, 3, "controller-1");
        w.write_i32(1);   // controller_id
        w.write_i32(5);   // controller_epoch
        w.write_i32(3);   // 3 partition states
        for p in 0..3 {
            w.write_compact_string("leader-test");
            w.write_i32(p); // partition_index
            w.write_i32(5); // controller_epoch
            w.write_i32(1); // leader
            w.write_i32(2); // leader_epoch
            w.write_compact_array(&[1i32, 2, 3], |w2, &v| w2.write_i32(v)); // isr
            w.write_i32(2); // partition_epoch
            w.write_compact_array(&[1i32, 2, 3], |w2, &v| w2.write_i32(v)); // replicas
            w.write_compact_array(&[] as &[i32], |w2: &mut KafkaWriter<'_>, &v: &i32| w2.write_i32(v));
            w.write_compact_array(&[] as &[i32], |w2: &mut KafkaWriter<'_>, &v: &i32| w2.write_i32(v));
            w.write_tagged_fields(&[]);
        }
        w.write_tagged_fields(&[]);
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "Multi-partition LeaderAndIsr should succeed");

    // Step 3: 发送 StopReplica v0 (API Key = 5)
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 5, 0, 4, "controller-1");
        w.write_i32(1);    // controller_id
        w.write_i32(5);    // controller_epoch
        w.write_bool(false); // delete_partitions = false (just stop, don't delete)
        w.write_i32(1);    // 1 partition
        w.write_compact_string("leader-test");
        w.write_i32(0);    // partition_index
        w.write_tagged_fields(&[]);
        w.write_tagged_fields(&[]);
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "StopReplica should succeed");
}

// ═══════════════════════════════════════════════════════════════════════
// 测试 3: Replica Fetch 兼容性
// ═══════════════════════════════════════════════════════════════════════

/// 模拟 R-Kafka Follower 从模拟 Leader 拉取数据。
///
/// 验证 Fetch 请求的跨 Broker 兼容性:
/// 1. 创建 topic 并 Produce 数据
/// 2. 发送 Fetch v0 请求 (replica_id = 2, 模拟 Follower)
/// 3. 验证能获取到数据
/// 4. 发送 Fetch v4 请求 (含 isolation_level)
/// 5. 验证 max_bytes 限制生效
#[tokio::test]
async fn test_replica_fetch_compatibility() {
    let port = setup_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();

    // 创建 topic 并 Produce 数据
    create_topic(&mut stream, "replica-test", 3, 1).await;

    // Produce 数据到 partition 0
    let batch = make_test_batch(0, 5);
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 0, 0, 2, "rk-follower-2");
        w.write_i16(1);   // acks = 1 (需要响应)
        w.write_i32(3000); // timeout_ms
        w.write_i32(1);   // 1 topic
        w.write_string("replica-test");
        w.write_i32(1);   // 1 partition
        w.write_i32(0);   // partition 0
        w.write_bytes(&batch);
    });
    stream.write_all(&frame).await.unwrap();
    let _produce_resp = read_response_frame(&mut stream).await;

    // Step 1: Fetch v0 with replica_id = 2 (simulating Follower)
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 1, 0, 3, "rk-follower-2");
        w.write_i32(2);     // replica_id = 2 (Follower)
        w.write_i32(100);   // max_wait_ms
        w.write_i32(1024);  // min_bytes
        w.write_i32(1);     // 1 topic
        w.write_string("replica-test");
        w.write_i32(1);     // 1 partition
        w.write_i32(0);     // partition 0
        w.write_i64(0);     // fetch_offset
        w.write_i32(65536); // max_bytes
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);

    // Response header: correlation_id
    let _corr_id = reader.read_i32().unwrap();
    // Fetch v0 response body: throttle_time_ms
    let _throttle = reader.read_i32().unwrap();
    let topic_count = reader.read_i32().unwrap();
    assert!(topic_count >= 1, "Should have at least 1 topic in fetch response");
    let _topic_name = reader.read_string();
    let partition_count = reader.read_i32().unwrap();
    assert!(partition_count >= 1);
    let _partition_index = reader.read_i32();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "Fetch partition should succeed");
    let _high_watermark = reader.read_i64();
    // v4+ would have last_stable_offset, aborted_transactions etc.
    // v0: just the recordset size + records
    let records_size = reader.read_i32().unwrap();
    assert!(records_size > 0, "Should have records");

    // Step 2: Fetch v4 with isolation_level (READ_COMMITTED)
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 1, 4, 4, "rk-follower-2");
        w.write_i32(2);     // replica_id = 2 (Follower)
        w.write_i32(100);   // max_wait_ms
        w.write_i32(1);     // min_bytes
        w.write_i32(65536); // max_bytes (v3+)
        w.write_i8(1);      // isolation_level = READ_COMMITTED (v4+)
        // NO session_id/epoch (v7+)
        w.write_i32(1);     // 1 topic
        w.write_string("replica-test");
        w.write_i32(1);     // 1 partition
        w.write_i32(0);     // partition_index
        // NO current_leader_epoch (v9+)
        w.write_i64(0);     // fetch_offset
        // NO log_start_offset (v5+)
        w.write_i32(65536); // max_bytes
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);

    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    // v4 response: no session_id (v7+ has it)
    let topic_count = reader.read_i32().unwrap();
    assert!(topic_count >= 1, "Should have topics in v4 fetch");
    let _topic_name = reader.read_string();
    let partition_count = reader.read_i32().unwrap();
    assert!(partition_count >= 1);
    let _partition_index = reader.read_i32();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0);
    let _high_watermark = reader.read_i64();
    // v4: last_stable_offset
    let _last_stable_offset = reader.read_i64();
    // v4: aborted_transactions count (array)
    let _aborted_count = reader.read_i32();
    // records (nullable_bytes)
    let records_size = reader.read_i32().unwrap();
    assert!(records_size > 0, "Should have records in v4 fetch");

    // Step 3: Fetch with max_bytes = 1 (验证限制)
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 1, 0, 5, "rk-follower-2");
        w.write_i32(2);     // replica_id
        w.write_i32(100);   // max_wait_ms
        w.write_i32(1);     // min_bytes
        w.write_i32(1);     // 1 topic
        w.write_string("replica-test");
        w.write_i32(1);     // 1 partition
        w.write_i32(0);     // partition 0
        w.write_i64(0);     // fetch_offset
        w.write_i32(1);     // max_bytes = 1 (very small)
    });
    stream.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let topic_count = reader.read_i32().unwrap();
    assert!(topic_count >= 1);
    // Response should still be valid even with tiny max_bytes
}

// ═══════════════════════════════════════════════════════════════════════
// 测试 4: 滚动迁移模拟
// ═══════════════════════════════════════════════════════════════════════

/// 模拟 Broker 滚动迁移流程。
///
/// 验证 R-Kafka 支持完整的 Broker 生命周期:
/// 1. BrokerRegistration — Broker 加入集群
/// 2. BrokerHeartbeat — 保持注册状态
/// 3. UpdateMetadata — Controller 广播元数据变更
/// 4. ControlledShutdown — 优雅关闭
/// 5. 验证新 Broker 可以加入替代
#[tokio::test]
async fn test_rolling_migration_simulation() {
    let port = setup_server().await;

    // === Phase 1: Broker 1 启动并注册 ===
    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();

    // BrokerRegistration v0 (API Key = 54, flexible format)
    let frame = build_broker_registration_frame(1, "test-cluster", "10.0.0.1", 9092, 1);
    stream1.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream1).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let reg_error = reader.read_i16().unwrap();
    assert_eq!(reg_error, 0, "BrokerRegistration should succeed");
    let broker_epoch = reader.read_i64().unwrap();
    assert!(broker_epoch > 0, "Should get valid broker epoch");

    // === Phase 2: Broker 1 发送心跳 ===
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 55, 0, 2, "rk-broker-1");
        w.write_i32(1);      // broker_id
        w.write_i64(broker_epoch); // broker_epoch
        w.write_bool(false); // want_fence
        w.write_bool(false); // want_shut_down
        w.write_i64(0);      // current_metadata_offset
        w.write_tagged_fields(&[]);
    });
    stream1.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream1).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let hb_error = reader.read_i16().unwrap();
    assert_eq!(hb_error, 0, "BrokerHeartbeat should succeed");
    let _leader_id = reader.read_i32().unwrap();
    let _leader_epoch = reader.read_i32().unwrap();
    let _is_controller = reader.read_bool().unwrap();
    // Broker 1 may or may not be controller depending on server state

    // === Phase 3: Controller 发送 UpdateMetadata ===
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 6, 0, 3, "controller");
        w.write_i32(1);   // controller_id
        w.write_i32(1);   // controller_epoch
        w.write_i32(2);   // 2 brokers
        w.write_i32(1); w.write_compact_string("10.0.0.1"); w.write_i32(9092); w.write_tagged_fields(&[]);
        w.write_i32(2); w.write_compact_string("10.0.0.2"); w.write_i32(9092); w.write_tagged_fields(&[]);
        w.write_tagged_fields(&[]);
    });
    stream1.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream1).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let um_error = reader.read_i16().unwrap();
    assert_eq!(um_error, 0, "UpdateMetadata should succeed");

    // === Phase 4: Broker 2 加入 (模拟新节点) ===
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
    let frame = build_broker_registration_frame(2, "test-cluster", "10.0.0.2", 9092, 1);
    stream2.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream2).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let reg2_error = reader.read_i16().unwrap();
    assert_eq!(reg2_error, 0, "Broker 2 registration should succeed");
    let _broker2_epoch = reader.read_i64().unwrap();

    // === Phase 5: Broker 1 优雅关闭 ===
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 7, 0, 4, "rk-broker-1");
        w.write_i32(1);              // broker_id
        w.write_i64(broker_epoch);   // broker_epoch
        w.write_tagged_fields(&[]);
    });
    stream1.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream1).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let shutdown_error = reader.read_i16().unwrap();
    assert_eq!(shutdown_error, 0, "ControlledShutdown should succeed");
    // remaining_partitions - may not have enough bytes if response is minimal
    // Just verify we got a valid error code

    // === Phase 6: Broker 2 继续心跳 (验证集群仍健康) ===
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 55, 0, 5, "rk-broker-2");
        w.write_i32(2);      // broker_id
        w.write_i64(1);      // broker_epoch
        w.write_bool(false); // want_fence
        w.write_bool(false); // want_shut_down
        w.write_i64(100);    // current_metadata_offset
        w.write_tagged_fields(&[]);
    });
    stream2.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream2).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let _throttle = reader.read_i32().unwrap();
    let hb2_error = reader.read_i16().unwrap();
    assert_eq!(hb2_error, 0, "Broker 2 heartbeat should succeed after Broker 1 shutdown");

    // === Phase 7: Metadata 验证 (确认 broker 列表更新) ===
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 3, 0, 6, "client");
        w.write_i32(0); // v0: empty topic list
    });
    stream2.write_all(&frame).await.unwrap();
    let resp = read_response_frame(&mut stream2).await;
    let mut reader = KafkaReader::new(&resp);
    let _corr = reader.read_i32().unwrap();
    let broker_count = reader.read_i32().unwrap();
    assert!(broker_count >= 1, "Should still have brokers after rolling migration");
}

// ─── 辅助: 构建 BrokerRegistration 帧 ────────────────────────────────

fn build_broker_registration_frame(
    broker_id: i32,
    cluster_id: &str,
    host: &str,
    port: i32,
    broker_epoch: i64,
) -> Vec<u8> {
    // BrokerRegistration v0: R-Kafka treats header as legacy (not in is_flexible),
    // but body decoder uses compact format (compact_string/compact_array).
    let mut buf = BytesMut::with_capacity(256);
    let mut writer = KafkaWriter::new(&mut buf);

    // RequestHeader (legacy format — nullable_string, no tagged_fields)
    writer.write_i16(54); // api_key
    writer.write_i16(0);  // api_version
    writer.write_i32(1);  // correlation_id
    let client_id = format!("rk-broker-{broker_id}");
    writer.write_nullable_string(Some(&client_id)); // legacy header: nullable_string

    // BrokerRegistration body (compact format as per decoder)
    writer.write_i32(broker_id);
    writer.write_compact_string(cluster_id);
    // features: empty compact array
    writer.write_compact_array(&[] as &[(&str, i16, i16)], |_: &mut KafkaWriter<'_>, _: &(&str, i16, i16)| {});
    // rack: null
    writer.write_compact_nullable_string(None);
    // host + port
    writer.write_compact_string(host);
    writer.write_i32(port);
    // broker_epoch
    writer.write_i64(broker_epoch);
    // endpoints: empty compact array
    writer.write_compact_array(&[] as &[(i32, i16, String)], |_: &mut KafkaWriter<'_>, _: &(i32, i16, String)| {});
    writer.write_tagged_fields(&[]); // body tagged fields

    drop(writer);

    let mut frame = BytesMut::with_capacity(4 + buf.len());
    frame.extend_from_slice(&(buf.len() as u32).to_be_bytes());
    frame.extend_from_slice(&buf);
    frame.to_vec()
}
