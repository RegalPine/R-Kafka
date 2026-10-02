//! Java Client 互操作集成测试
//!
//! 模拟现代 Java Kafka Client (3.x) 的完整交互流程。
//! 验证 R-Kafka 的 Wire Protocol 与 Kafka 协议规范完全兼容。
//!
//! Flexible 版本阈值 (来自 is_flexible):
//! - ApiVersions: v3+, Metadata: v9+, Produce: v9+, Fetch: v12+
//! - ListOffsets: v7+, OffsetCommit: v8+, OffsetFetch: v8+
//! - FindCoordinator: v4+, JoinGroup: v6+, Heartbeat: v4+
//! - SyncGroup: v4+, LeaveGroup: v4+
//!
//! 测试流程:
//! 1. ApiVersions v3 (flexible) — 版本发现
//! 2. Metadata v0 (legacy) — 集群元数据
//! 3. CreateTopics v0 (legacy) — 创建 Topic
//! 4. Produce v0 (legacy) — 发送消息
//! 5. Fetch v0 (legacy) — 消费消息
//! 6. ListOffsets v1 (legacy) — 查询偏移量
//! 7. FindCoordinator v0 (legacy) — 查找协调器
//! 8. JoinGroup v0 (legacy) — 加入消费者组
//! 9. Heartbeat v0 (legacy) — 心跳
//! 10. OffsetCommit v0 (legacy) — 提交偏移量
//! 11. OffsetFetch v0 (legacy) — 查询偏移量
//! 12. ApiVersions v3 (flexible) — 验证 flexible 格式

use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rk_broker::{BrokerRouter, OffsetManager, PartitionManager};
use rk_protocol::types::{KafkaReader, KafkaWriter};
use rk_storage::log_io::build_batch_bytes;

// ─── 工具函数 ─────────────────────────────────────────────────────────

/// 构建 Legacy 格式请求帧 (RequestHeader v0/v1)
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

/// 构建 Flexible 格式请求帧 (RequestHeader v2)
fn build_flexible_frame(body_builder: impl FnOnce(&mut KafkaWriter<'_>)) -> Vec<u8> {
    build_legacy_frame(body_builder) // 帧格式相同，区别在 header 编码
}

/// 写入 Legacy RequestHeader v0/v1
fn write_legacy_header(
    w: &mut KafkaWriter<'_>,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
) {
    w.write_i16(api_key);
    w.write_i16(api_version);
    w.write_i32(correlation_id);
    w.write_nullable_string(Some("r-kafka-java-client")); // client_id (legacy format)
}

/// 写入 Flexible RequestHeader v2
fn write_flexible_header(
    w: &mut KafkaWriter<'_>,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
) {
    w.write_i16(api_key);
    w.write_i16(api_version);
    w.write_i32(correlation_id);
    w.write_compact_nullable_string(Some("r-kafka-java-client")); // client_id (compact)
    w.write_tagged_fields(&[]); // empty tagged fields
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
    build_batch_bytes(
        base_offset,
        1,
        0,
        1000,
        2000,
        -1,
        -1,
        -1,
        &records,
        record_count,
    )
}

/// 启动测试服务器
async fn setup_server() -> u16 {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(PartitionManager::new(
        dir.path().to_path_buf(),
        1_073_741_824,
        1,
    ));
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

// ─── 测试 1: Java Client Produce/Consume 完整流程 ────────────────────

/// 模拟 Java Client 完整连接生命周期 (混合 legacy + flexible):
/// ApiVersions v3 (flex) → Metadata v0 → CreateTopics v0 → Produce v0 → Fetch v0 → ListOffsets v1
#[tokio::test]
async fn test_java_client_produce_consume_flow() {
    let port = setup_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // ═══ Step 1: ApiVersions v3 (Flexible) ═══
    let frame = build_flexible_frame(|w| {
        write_flexible_header(w, 18, 3, 1);
        w.write_compact_nullable_string(Some("Apache Kafka")); // client_software_name
        w.write_compact_nullable_string(Some("3.7.0")); // client_software_version
        w.write_tagged_fields(&[]);
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 1, "ApiVersions correlation_id");
    // Flexible ResponseHeader v1: correlation_id + tagged_fields
    let _header_tags = reader.read_tagged_fields().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "ApiVersions should succeed");
    let _throttle = reader.read_i32().unwrap(); // v1+
                                                // v3 flexible: compact_array (unsigned varint len+1)
    let api_count_varint = reader.read_unsigned_varint().unwrap() as usize;
    let api_count = api_count_varint - 1; // compact array: N+1 = count
    assert!(api_count >= 30, "Should report 30+ APIs, got {}", api_count);
    // 读取第一个 API entry
    let first_api_key = reader.read_i16().unwrap();
    let _first_min = reader.read_i16().unwrap();
    let first_max = reader.read_i16().unwrap();
    assert_eq!(first_api_key, 0); // Produce
    assert!(first_max >= 0);
    let _item_tags = reader.read_tagged_fields().unwrap();

    // ═══ Step 2: Metadata v0 (Legacy) ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 3, 0, 2);
        w.write_i32(-1); // v0: non-nullable array, -1 = empty (no topics)
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 2, "Metadata correlation_id");
    // v0: brokers array
    let broker_count = reader.read_i32().unwrap();
    assert_eq!(broker_count, 1, "Should have 1 broker");
    let _node_id = reader.read_i32().unwrap();
    let _host = reader.read_string().unwrap();
    let _port = reader.read_i32().unwrap();
    // topics array
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 0, "No topics yet");

    // ═══ Step 3: CreateTopics v0 (Legacy) ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 19, 0, 3);
        // v0: array of (topic, num_partitions, replication_factor, assignments, configs)
        w.write_i32(1); // 1 topic
        w.write_string("interop-test");
        w.write_i32(2); // 2 partitions
        w.write_i16(1); // replication_factor = 1
                        // assignments: array of (partition, replicas)
        w.write_i32(0); // empty assignments
                        // configs: array of (key, value)
        w.write_i32(0); // empty configs
                        // timeout_ms
        w.write_i32(30_000);
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 3, "CreateTopics correlation_id");
    // v0: topics array of (name, error_code)
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "interop-test");
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "CreateTopics should succeed");

    // ═══ Step 4: Produce v0 (Legacy) ═══
    let batch = make_test_batch(0, 10);
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 0, 0, 4);
        w.write_i16(1); // acks = 1
        w.write_i32(30_000); // timeout_ms
        w.write_i32(1); // 1 topic
        w.write_string("interop-test");
        w.write_i32(1); // 1 partition
        w.write_i32(0); // partition 0
        w.write_bytes(&batch); // record_set
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 4, "Produce correlation_id");
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "interop-test");
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let _part_index = reader.read_i32().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "Produce should succeed");
    let base_offset = reader.read_i64().unwrap();
    assert_eq!(base_offset, 0, "First produce starts at offset 0");

    // ═══ Step 5: Fetch v0 (Legacy) ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 1, 0, 5);
        w.write_i32(-1); // replica_id
        w.write_i32(500); // max_wait_ms
        w.write_i32(1); // min_bytes
        w.write_i32(1); // 1 topic
        w.write_string("interop-test");
        w.write_i32(1); // 1 partition
        w.write_i32(0); // partition 0
        w.write_i64(0); // fetch_offset
        w.write_i32(1_000_000); // max_bytes
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 5, "Fetch correlation_id");
    let _throttle = reader.read_i32().unwrap();
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "interop-test");
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let _part_index = reader.read_i32().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "Fetch should succeed");
    let high_watermark = reader.read_i64().unwrap();
    assert_eq!(high_watermark, 10, "Should have 10 records");
    let record_set_len = reader.read_i32().unwrap();
    assert!(record_set_len > 0, "Should have record data");

    // ═══ Step 6: ListOffsets v1 (Legacy) — LATEST ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 2, 1, 6);
        w.write_i32(-1); // replica_id
        w.write_i32(1); // 1 topic
        w.write_string("interop-test");
        w.write_i32(1); // 1 partition
        w.write_i32(0); // partition 0
        w.write_i64(-1); // timestamp = -1 (LATEST)
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 6, "ListOffsets correlation_id");
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let _topic_name = reader.read_string().unwrap();
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let _part_index = reader.read_i32().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0);
    let _timestamp = reader.read_i64().unwrap();
    let offset = reader.read_i64().unwrap();
    assert_eq!(offset, 10, "LATEST offset should be 10");
}

// ─── 测试 2: Consumer Group 完整流程 ─────────────────────────────────

#[tokio::test]
async fn test_java_client_consumer_group_flow() {
    let port = setup_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // 先创建 topic
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 19, 0, 1);
        w.write_i32(1);
        w.write_string("group-test");
        w.write_i32(2); // 2 partitions
        w.write_i16(1);
        w.write_i32(0); // empty assignments
        w.write_i32(0); // empty configs
        w.write_i32(30_000);
    });
    stream.write_all(&frame).await.unwrap();
    let _ = read_response_frame(&mut stream).await;

    // ═══ FindCoordinator v0 (Legacy) ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 10, 0, 10);
        w.write_string("test-group"); // coordinator_key
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 10, "FindCoordinator correlation_id");
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "FindCoordinator should succeed");
    let _node_id = reader.read_i32().unwrap();
    let _host = reader.read_string().unwrap();
    let _port = reader.read_i32().unwrap();

    // ═══ JoinGroup v0 (Legacy) ═══
    // v0: group_id, member_id, protocol_type, protocols (NO session_timeout_ms — v1+ only)
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 11, 0, 11);
        w.write_string("test-group"); // group_id
        w.write_string("consumer-1"); // member_id
        w.write_string("consumer"); // protocol_type
                                    // protocols: array of (name, metadata)
        w.write_i32(1);
        w.write_string("range");
        w.write_bytes(&[0u8; 0]); // empty metadata
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 11, "JoinGroup correlation_id");
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0, "JoinGroup should succeed");
    let _generation_id = reader.read_i32().unwrap();
    let _protocol = reader.read_nullable_string().unwrap();
    let _leader = reader.read_string().unwrap();
    let _member_id = reader.read_string().unwrap();

    // ═══ Heartbeat v0 (Legacy) ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 12, 0, 12);
        w.write_string("test-group");
        w.write_i32(1); // generation_id
        w.write_string("consumer-1"); // member_id
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 12, "Heartbeat correlation_id");
    let error_code = reader.read_i16().unwrap();
    // 可能返回 0 或 UNKNOWN_MEMBER_ID (25)
    assert!(
        error_code == 0 || error_code == 25,
        "Heartbeat error: {}",
        error_code
    );

    // ═══ OffsetCommit v0 (Legacy) ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 8, 0, 13);
        w.write_string("test-group"); // group_id
        w.write_i32(1); // 1 topic
        w.write_string("group-test");
        w.write_i32(1); // 1 partition
        w.write_i32(0); // partition 0
        w.write_i64(42); // offset
        w.write_nullable_string(None); // metadata
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 13, "OffsetCommit correlation_id");

    // ═══ OffsetFetch v0 (Legacy) ═══
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 9, 0, 14);
        w.write_string("test-group"); // group_id
        w.write_i32(1); // 1 topic
        w.write_string("group-test");
        w.write_i32(1); // 1 partition
        w.write_i32(0); // partition 0
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    let corr_id = reader.read_i32().unwrap();
    assert_eq!(corr_id, 14, "OffsetFetch correlation_id");
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "group-test");
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let _part_index = reader.read_i32().unwrap();
    let offset = reader.read_i64().unwrap();
    assert_eq!(offset, 42, "Should fetch committed offset 42");
    let _metadata = reader.read_nullable_string().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0);
}

// ─── 测试 3: Flexible 格式 ApiVersions 管道复用 ──────────────────────

#[tokio::test]
async fn test_java_client_flexible_pipelining() {
    let port = setup_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Java client 在同一连接上发送多个 flexible 请求
    for i in 0..10 {
        let frame = build_flexible_frame(|w| {
            write_flexible_header(w, 18, 3, i);
            w.write_compact_nullable_string(Some("test"));
            w.write_compact_nullable_string(Some("1.0"));
            w.write_tagged_fields(&[]);
        });
        stream.write_all(&frame).await.unwrap();

        let response = read_response_frame(&mut stream).await;
        let mut reader = KafkaReader::new(&response);
        let corr_id = reader.read_i32().unwrap();
        assert_eq!(corr_id, i, "Pipelined request {} correlation_id", i);
        let error_code = reader.read_i16().unwrap();
        assert_eq!(error_code, 0);
    }
}

// ─── 测试 4: Legacy + Flexible 混合格式连接 ──────────────────────────

#[tokio::test]
async fn test_java_client_mixed_legacy_flexible() {
    let port = setup_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // 1. Legacy ApiVersions v0
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 18, 0, 100);
        // v0 has no body
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    assert_eq!(reader.read_i32().unwrap(), 100);
    assert_eq!(reader.read_i16().unwrap(), 0); // error_code

    // 2. Flexible ApiVersions v3 (same connection)
    let frame = build_flexible_frame(|w| {
        write_flexible_header(w, 18, 3, 101);
        w.write_compact_nullable_string(Some("flex-client"));
        w.write_compact_nullable_string(Some("3.0"));
        w.write_tagged_fields(&[]);
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    assert_eq!(reader.read_i32().unwrap(), 101);
    assert_eq!(reader.read_i16().unwrap(), 0);

    // 3. Legacy Metadata v0 (same connection)
    let frame = build_legacy_frame(|w| {
        write_legacy_header(w, 3, 0, 102);
        w.write_i32(-1); // empty topics array
    });
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = KafkaReader::new(&response);
    assert_eq!(reader.read_i32().unwrap(), 102);
}
