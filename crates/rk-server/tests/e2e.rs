//! E2E 集成测试: 完整 TCP 请求-响应链路
//!
//! 启动真实 TCP 服务器，通过 TCP 客户端发送 Kafka 协议帧，验证完整链路。

use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rk_broker::BrokerRouter;
use rk_broker::PartitionManager;
use rk_protocol::types::KafkaWriter;
use rk_storage::log_io::build_batch_bytes;

/// 构建 Kafka 请求帧: 4字节长度前缀 + RequestHeader + Body
fn build_frame(body_builder: impl FnOnce(&mut KafkaWriter<'_>)) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(256);
    let mut writer = KafkaWriter::new(&mut buf);
    body_builder(&mut writer);
    drop(writer);

    // 加 4 字节长度前缀
    let mut frame = BytesMut::with_capacity(4 + buf.len());
    frame.extend_from_slice(&(buf.len() as u32).to_be_bytes());
    frame.extend_from_slice(&buf);
    frame.to_vec()
}

/// 从 TCP 流读取一个响应帧 (4字节长度前缀 + 数据)
async fn read_response_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await.unwrap();
    data
}

/// 构建 Metadata 请求帧 (api_key=3, version=0)
fn build_metadata_request(correlation_id: i32, topics: Option<&[&str]>) -> Vec<u8> {
    build_frame(|w| {
        // RequestHeader v0
        w.write_i16(3); // api_key = Metadata
        w.write_i16(0); // api_version = 0
        w.write_i32(correlation_id);
        w.write_nullable_string(None); // client_id = null

        // MetadataRequestBody v0
        match topics {
            None => {
                w.write_i32(-1); // null array = all topics
            }
            Some(topics) => {
                w.write_i32(topics.len() as i32);
                for t in topics {
                    w.write_string(t);
                }
            }
        }
    })
}

/// 构建 Produce 请求帧 (api_key=0, version=0)
fn build_produce_request(
    correlation_id: i32,
    topic: &str,
    partition: i32,
    batch: &[u8],
) -> Vec<u8> {
    build_frame(|w| {
        // RequestHeader v0
        w.write_i16(0); // api_key = Produce
        w.write_i16(0); // api_version = 0
        w.write_i32(correlation_id);
        w.write_nullable_string(None); // client_id = null

        // ProduceRequestBody v0 (无 transactional_id, v3+ 才有)
        w.write_i16(1); // acks = 1
        w.write_i32(30_000); // timeout_ms

        w.write_i32(1); // 1 topic
        w.write_string(topic);
        w.write_i32(1); // 1 partition
        w.write_i32(partition);
        w.write_bytes(batch); // record_set
    })
}

/// 构建 Fetch 请求帧 (api_key=1, version=0)
fn build_fetch_request(
    correlation_id: i32,
    topic: &str,
    partition: i32,
    fetch_offset: i64,
) -> Vec<u8> {
    build_frame(|w| {
        // RequestHeader v0
        w.write_i16(1); // api_key = Fetch
        w.write_i16(0); // api_version = 0
        w.write_i32(correlation_id);
        w.write_nullable_string(None); // client_id = null

        // FetchRequestBody v0
        w.write_i32(-1); // replica_id = -1 (consumer)
        w.write_i32(500); // max_wait_ms
        w.write_i32(1); // min_bytes

        w.write_i32(1); // 1 topic
        w.write_string(topic);
        w.write_i32(1); // 1 partition
        w.write_i32(partition);
        w.write_i64(fetch_offset);
        w.write_i32(1_000_000); // max_bytes
    })
}

/// 构建 ListOffsets 请求帧 (api_key=2, version=1)
fn build_list_offsets_request(
    correlation_id: i32,
    topic: &str,
    partition: i32,
    timestamp: i64,
) -> Vec<u8> {
    build_frame(|w| {
        // RequestHeader v0
        w.write_i16(2); // api_key = ListOffsets
        w.write_i16(1); // api_version = 1
        w.write_i32(correlation_id);
        w.write_nullable_string(None); // client_id = null

        // ListOffsetsRequestBody v1
        w.write_i32(-1); // replica_id = -1
        w.write_i32(1); // 1 topic
        w.write_string(topic);
        w.write_i32(1); // 1 partition
        w.write_i32(partition);
        w.write_i64(timestamp); // -1 = LATEST, -2 = EARLIEST
    })
}

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

async fn setup_server() -> (u16, Arc<BrokerRouter>) {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(PartitionManager::new(
        dir.path().to_path_buf(),
        1_073_741_824,
        1,
    ));

    let router = Arc::new(BrokerRouter::new(
        pm,
        1,
        "127.0.0.1".to_string(),
        0, // will use actual port
        None,
        Some("test-cluster".to_string()),
    ));

    // 绑定到随机端口
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

    // 等待服务器就绪
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (port, router)
}

#[tokio::test]
async fn test_e2e_metadata_request() {
    let (port, _router) = setup_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // 发送 Metadata 请求 (所有 topics)
    let frame = build_metadata_request(1, None);
    stream.write_all(&frame).await.unwrap();

    // 读取响应
    let response = read_response_frame(&mut stream).await;
    assert!(!response.is_empty());

    // 解析响应: ResponseHeader (correlation_id i32) + MetadataResponseBody
    let mut reader = rk_protocol::types::KafkaReader::new(&response);
    let correlation_id = reader.read_i32().unwrap();
    assert_eq!(correlation_id, 1);

    // MetadataResponseBody v0:
    // brokers: array of (node_id i32, host string, port i32)
    let broker_count = reader.read_i32().unwrap();
    assert_eq!(broker_count, 1);
    let _node_id = reader.read_i32().unwrap();
    let _host = reader.read_string().unwrap();
    let _port = reader.read_i32().unwrap();

    // controller_id (v1+, 但 v0 没有)
    // topics: array
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 0); // 没有 topics (因为还没有创建任何 topic)
}

#[tokio::test]
async fn test_e2e_produce_fetch_listoffsets() {
    let (port, _router) = setup_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // 1. Produce 写入数据
    let batch = make_test_batch(0, 5);
    let frame = build_produce_request(10, "e2e-topic", 0, &batch);
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = rk_protocol::types::KafkaReader::new(&response);

    // ResponseHeader
    let correlation_id = reader.read_i32().unwrap();
    assert_eq!(correlation_id, 10);

    // ProduceResponseBody v0:
    // topics: array of (name string, partitions: array of (index i32, error_code i16, offset i64))
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "e2e-topic");
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let part_index = reader.read_i32().unwrap();
    assert_eq!(part_index, 0);
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0); // No error
    let base_offset = reader.read_i64().unwrap();
    assert_eq!(base_offset, 0); // First write starts at offset 0
                                // v0 没有 log_append_time_ms (v1+ 才有)

    // 2. Fetch 读取数据
    let frame = build_fetch_request(11, "e2e-topic", 0, 0);
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = rk_protocol::types::KafkaReader::new(&response);

    let correlation_id = reader.read_i32().unwrap();
    assert_eq!(correlation_id, 11);

    // FetchResponseBody v0:
    // throttle_time_ms i32
    let _throttle = reader.read_i32().unwrap();
    // topics: array
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "e2e-topic");
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let part_index = reader.read_i32().unwrap();
    assert_eq!(part_index, 0);
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0);
    let high_watermark = reader.read_i64().unwrap();
    assert_eq!(high_watermark, 5); // 5 records written
                                   // record_set: bytes (nullable)
    let record_set_len = reader.read_i32().unwrap();
    assert!(record_set_len > 0); // Should have data

    // 3. ListOffsets 查询 LATEST
    let frame = build_list_offsets_request(12, "e2e-topic", 0, -1);
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = rk_protocol::types::KafkaReader::new(&response);

    let correlation_id = reader.read_i32().unwrap();
    assert_eq!(correlation_id, 12);

    // ListOffsetsResponseBody v1:
    // v1 没有 throttle_time_ms (v2+ 才有)
    // topics: array
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "e2e-topic");
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let part_index = reader.read_i32().unwrap();
    assert_eq!(part_index, 0);
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0);
    let _timestamp = reader.read_i64().unwrap();
    let offset = reader.read_i64().unwrap();
    assert_eq!(offset, 5); // LEO = 5 (5 records written)
}

#[tokio::test]
async fn test_e2e_multiple_requests_same_connection() {
    let (port, _router) = setup_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // 在同一连接上发送多个请求
    for i in 0..5 {
        let frame = build_metadata_request(i, Some(&["test"]));
        stream.write_all(&frame).await.unwrap();

        let response = read_response_frame(&mut stream).await;
        let mut reader = rk_protocol::types::KafkaReader::new(&response);
        let correlation_id = reader.read_i32().unwrap();
        assert_eq!(correlation_id, i);
    }
}

#[tokio::test]
async fn test_e2e_auto_topic_creation() {
    let (port, _router) = setup_server().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // 直接 Fetch 一个不存在的 topic (应自动创建)
    let frame = build_fetch_request(20, "auto-created-topic", 0, 0);
    stream.write_all(&frame).await.unwrap();

    let response = read_response_frame(&mut stream).await;
    let mut reader = rk_protocol::types::KafkaReader::new(&response);

    let correlation_id = reader.read_i32().unwrap();
    assert_eq!(correlation_id, 20);

    // 跳过 throttle_time
    let _throttle = reader.read_i32().unwrap();
    let topic_count = reader.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = reader.read_string().unwrap();
    assert_eq!(topic_name, "auto-created-topic");
    let part_count = reader.read_i32().unwrap();
    assert_eq!(part_count, 1);
    let _part_index = reader.read_i32().unwrap();
    let error_code = reader.read_i16().unwrap();
    assert_eq!(error_code, 0); // 成功 (自动创建)
}
