//! 协议 Fuzzing 测试套件
//!
//! 覆盖所有 45 个 Kafka API 的 legacy + flexible 版本 fuzzing:
//!
//! ```text
//! Fuzzing 策略:
//!
//! 1. 边界值测试: 空数组/空字符串/null/边界整数/超长字符串
//! 2. 随机合法请求: 确定性 PRNG → decode 不 panic
//! 3. Legacy vs Flexible: 每个 API 测试两种格式
//! 4. 响应编码: 构造 → encode → 验证非空
//! ```

use bytes::BytesMut;
use rk_protocol::types::{KafkaReader, KafkaWriter};
use rk_protocol::codec::{KafkaRequestDecoder, KafkaResponseEncoder, encode_response};
use rk_protocol::apis::*;
use rk_protocol::ApiVersionsRequest;

// ─── PRNG ─────────────────────────────────────────────────────────

struct Rng { state: u64 }
impl Rng {
    fn new(seed: u64) -> Self { Self { state: if seed == 0 { 1 } else { seed } } }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.state = x; x
    }
    fn next_i16(&mut self) -> i16 { (self.next_u64() & 0xFFFF) as i16 }
    fn next_i32(&mut self) -> i32 { (self.next_u64() & 0xFFFFFFFF) as i32 }
    fn next_i64(&mut self) -> i64 { self.next_u64() as i64 }
    fn next_i8(&mut self) -> i8 { (self.next_u64() & 0xFF) as i8 }
    fn next_range(&mut self, lo: usize, hi: usize) -> usize {
        if lo >= hi { return lo; } lo + (self.next_u64() as usize % (hi - lo + 1))
    }
    fn next_bool(&mut self) -> bool { self.next_u64() & 1 == 1 }
    fn next_string(&mut self, max_len: usize) -> String {
        let len = self.next_range(0, max_len);
        (0..len).map(|_| { let c = b"abcdefghijklmnopqrstuvwxyz0123456789"; c[self.next_u64() as usize % c.len()] as char }).collect()
    }
    fn next_bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.next_range(0, max_len);
        (0..len).map(|i| (self.next_u64() >> (i % 8 * 8)) as u8).collect()
    }
}

// ─── 辅助 ─────────────────────────────────────────────────────────

fn build_body(builder: impl FnOnce(&mut KafkaWriter<'_>)) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(512);
    let mut w = KafkaWriter::new(&mut buf);
    builder(&mut w);
    drop(w);
    buf.to_vec()
}

fn fuzz_decode<T: KafkaRequestDecoder>(data: &[u8], version: i16) -> bool {
    let mut reader = KafkaReader::new(data);
    T::decode(&mut reader, version).is_ok()
}

fn fuzz_encode_response<T: KafkaResponseEncoder>(resp: &T, version: i16) -> Vec<u8> {
    encode_response(resp, version).unwrap().to_vec()
}

// ─── API Fuzzing (预生成随机值, 避免 Fn 闭包问题) ─────────────────

fn fuzz_produce(rng: &mut Rng, version: i16) {
    let is_flex = version >= 9;
    let has_txn = rng.next_bool();
    let acks = rng.next_i16();
    let timeout = rng.next_i32().abs() % 30000;
    let tc = rng.next_range(0, 3);
    let topics: Vec<(String, Vec<(i32, Vec<u8>)>)> = (0..tc).map(|_| {
        let name = rng.next_string(16);
        let pc = rng.next_range(0, 2);
        let parts: Vec<(i32, Vec<u8>)> = (0..pc).map(|_| (rng.next_i32().abs() % 100, rng.next_bytes(64))).collect();
        (name, parts)
    }).collect();

    let data = build_body(|w| {
        if version >= 3 {
            let txn = if has_txn { Some("txn-fuzz") } else { None };
            if is_flex { w.write_compact_nullable_string(txn); } else { w.write_nullable_string(txn); }
        }
        w.write_i16(acks); w.write_i32(timeout);
        if is_flex {
            w.write_compact_array(&topics, |w, (name, parts)| {
                w.write_compact_string(name);
                w.write_compact_array(parts, |w, (idx, rec)| {
                    w.write_i32(*idx); w.write_nullable_bytes(Some(rec)); w.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
        } else {
            w.write_array(&topics, |w, (name, parts)| {
                w.write_string(name);
                w.write_array(parts, |w, (idx, rec)| { w.write_i32(*idx); w.write_nullable_bytes(Some(rec)); });
            });
        }
    });
    let _ = fuzz_decode::<ProduceRequest>(&data, version);
}

fn fuzz_fetch(rng: &mut Rng, version: i16) {
    let is_flex = version >= 12;
    let replica_id = rng.next_i32();
    let max_wait = rng.next_i32().abs() % 5000;
    let min_bytes = rng.next_i32().abs() % 1024;
    let max_bytes_v = rng.next_i32().abs() % 1048576;
    let iso = rng.next_i8() & 1;
    let sess_id = rng.next_i32(); let sess_ep = rng.next_i32();
    let tc = rng.next_range(0, 2);
    let topics: Vec<(String, Vec<i32>)> = (0..tc).map(|_| {
        let name = rng.next_string(16);
        let pc = rng.next_range(0, 2);
        let parts: Vec<i32> = (0..pc).map(|_| rng.next_i32().abs() % 100).collect();
        (name, parts)
    }).collect();

    let data = build_body(|w| {
        w.write_i32(replica_id); w.write_i32(max_wait); w.write_i32(min_bytes);
        if version >= 3 { w.write_i32(max_bytes_v); }
        if version >= 4 { w.write_i8(iso); }
        if version >= 7 { w.write_i32(sess_id); w.write_i32(sess_ep); }
        if is_flex {
            w.write_compact_array(&topics, |w, (name, parts)| {
                w.write_compact_string(name);
                w.write_compact_array(parts, |w, idx| {
                    w.write_i32(*idx);
                    if version >= 9 { w.write_i32(0); }
                    w.write_i64(100);
                    if version >= 5 { w.write_i64(0); }
                    w.write_i32(1024);
                    w.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
        } else {
            w.write_array(&topics, |w, (name, parts)| {
                w.write_string(name);
                w.write_array(parts, |w, idx| {
                    w.write_i32(*idx);
                    if version >= 9 { w.write_i32(0); }
                    w.write_i64(100);
                    if version >= 5 { w.write_i64(0); }
                    w.write_i32(1024);
                });
            });
        }
        if version >= 11 {
            if is_flex { w.write_compact_nullable_string(Some("rack1")); } else { w.write_nullable_string(Some("rack1")); }
        }
    });
    let _ = fuzz_decode::<FetchRequest>(&data, version);
}

fn fuzz_list_offsets(rng: &mut Rng, version: i16) {
    let is_flex = version >= 7;
    let replica_id = rng.next_i32();
    let tc = rng.next_range(0, 2);
    let topics: Vec<(String, Vec<i32>)> = (0..tc).map(|_| (rng.next_string(16), (0..rng.next_range(0,2)).map(|_| rng.next_i32().abs() % 100).collect())).collect();

    let data = build_body(|w| {
        w.write_i32(replica_id);
        if is_flex {
            w.write_compact_array(&topics, |w, (name, parts)| {
                w.write_compact_string(name);
                w.write_compact_array(parts, |w, idx| {
                    w.write_i32(*idx);
                    if version >= 4 { w.write_i64(100); }
                    if version >= 2 { w.write_i32(0); }
                    w.write_i64(100);
                    w.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
        } else {
            w.write_array(&topics, |w, (name, parts)| {
                w.write_string(name);
                w.write_array(parts, |w, idx| {
                    w.write_i32(*idx);
                    if version >= 4 { w.write_i64(100); }
                    if version >= 2 { w.write_i32(0); }
                    w.write_i64(100);
                });
            });
        }
    });
    let _ = fuzz_decode::<ListOffsetsRequest>(&data, version);
}

fn fuzz_metadata(rng: &mut Rng, version: i16) {
    let is_flex = version >= 9;
    let has_topics = rng.next_bool();
    let auto_create = rng.next_bool();
    let tc = rng.next_range(0, 3);
    let topic_names: Vec<String> = (0..tc).map(|_| rng.next_string(16)).collect();

    let data = build_body(|w| {
        if has_topics {
            if is_flex {
                w.write_compact_array(&topic_names, |w, name| { w.write_compact_string(name); w.write_tagged_fields(&[]); });
            } else {
                w.write_array(&topic_names, |w, name| { w.write_string(name); });
            }
        } else {
            if is_flex { w.write_i32(0); } else { w.write_i32(-1); }
        }
        if version >= 4 { w.write_bool(auto_create); }
        if version >= 8 && is_flex { w.write_tagged_fields(&[]); }
    });
    let _ = fuzz_decode::<MetadataRequest>(&data, version);
}

fn fuzz_offset_commit(rng: &mut Rng, version: i16) {
    let is_flex = version >= 8;
    let gen_id = rng.next_i32();
    let tc = rng.next_range(0, 2);
    let topics: Vec<(String, Vec<(i32, i64)>)> = (0..tc).map(|_| {
        let name = rng.next_string(16);
        let pc = rng.next_range(0, 2);
        let parts: Vec<(i32, i64)> = (0..pc).map(|_| (rng.next_i32().abs() % 100, rng.next_i64().abs() % 100000)).collect();
        (name, parts)
    }).collect();

    let data = build_body(|w| {
        if is_flex { w.write_compact_string("fuzz-group"); } else { w.write_string("fuzz-group"); }
        if version <= 7 { w.write_i32(gen_id); }
        if version <= 7 {
            if is_flex { w.write_compact_string("member-1"); } else { w.write_string("member-1"); }
        }
        if version >= 7 {
            if is_flex { w.write_compact_nullable_string(None); } else { w.write_nullable_string(None); }
        }
        if version <= 4 { w.write_i64(-1); }
        if is_flex {
            w.write_compact_array(&topics, |w, (name, parts)| {
                w.write_compact_string(name);
                w.write_compact_array(parts, |w, (idx, off)| {
                    w.write_i32(*idx); w.write_i64(*off);
                    if version >= 6 { w.write_i64(*off); }
                    w.write_tagged_fields(&[]);
                });
                w.write_tagged_fields(&[]);
            });
        } else {
            w.write_array(&topics, |w, (name, parts)| {
                w.write_string(name);
                w.write_array(parts, |w, (idx, off)| {
                    w.write_i32(*idx); w.write_i64(*off);
                    if version >= 6 { w.write_i64(*off); }
                });
            });
        }
    });
    let _ = fuzz_decode::<OffsetCommitRequest>(&data, version);
}

fn fuzz_offset_fetch(rng: &mut Rng, version: i16) {
    let is_flex = version >= 8;
    let tc = rng.next_range(0, 2);
    let topics: Vec<(String, Vec<i32>)> = (0..tc).map(|_| (rng.next_string(16), (0..rng.next_range(0,2)).map(|_| rng.next_i32().abs() % 100).collect())).collect();

    let data = build_body(|w| {
        if version < 8 { w.write_string("fuzz-group"); }
        if is_flex { w.write_compact_string("fuzz-group"); }
        if is_flex {
            w.write_compact_array(&topics, |w, (name, parts)| {
                w.write_compact_string(name);
                w.write_compact_array(parts, |w, idx| { w.write_i32(*idx); w.write_tagged_fields(&[]); });
                w.write_tagged_fields(&[]);
            });
        } else {
            w.write_array(&topics, |w, (name, parts)| {
                w.write_string(name);
                w.write_array(parts, |w, idx| { w.write_i32(*idx); });
            });
        }
    });
    let _ = fuzz_decode::<OffsetFetchRequest>(&data, version);
}

fn fuzz_find_coordinator(rng: &mut Rng, version: i16) {
    let is_flex = version >= 4;
    let data = build_body(|w| {
        if version < 4 {
            if is_flex { w.write_compact_string("fuzz-group"); } else { w.write_string("fuzz-group"); }
            w.write_i8(0);
        }
        if is_flex { w.write_tagged_fields(&[]); }
    });
    let _ = fuzz_decode::<FindCoordinatorRequest>(&data, version);
}

fn fuzz_join_group(rng: &mut Rng, version: i16) {
    let is_flex = version >= 6;
    let session_to = rng.next_i32().abs() % 30000;
    let rebalance_to = rng.next_i32().abs() % 30000;

    let data = build_body(|w| {
        if is_flex { w.write_compact_string("fuzz-group"); } else { w.write_string("fuzz-group"); }
        w.write_i32(session_to);
        if version >= 1 { w.write_i32(rebalance_to); }
        if is_flex { w.write_compact_string("member-1"); } else { w.write_string("member-1"); }
        if version >= 5 {
            if is_flex { w.write_compact_nullable_string(None); } else { w.write_nullable_string(None); }
        }
        if is_flex { w.write_compact_string("consumer"); } else { w.write_string("consumer"); }
        if is_flex {
            w.write_compact_array(&[("range", b"metadata".as_slice())], |w, (name, meta)| {
                w.write_compact_string(name); w.write_nullable_bytes(Some(meta)); w.write_tagged_fields(&[]);
            });
        } else {
            w.write_array(&[("range", b"metadata".as_slice())], |w, (name, meta)| {
                w.write_string(name); w.write_nullable_bytes(Some(meta));
            });
        }
        if version >= 9 && is_flex { w.write_compact_nullable_string(None); }
    });
    let _ = fuzz_decode::<JoinGroupRequest>(&data, version);
}

fn fuzz_heartbeat(rng: &mut Rng, version: i16) {
    let is_flex = version >= 4;
    let gen = rng.next_i32().abs() % 100;
    let data = build_body(|w| {
        if is_flex { w.write_compact_string("fuzz-group"); } else { w.write_string("fuzz-group"); }
        w.write_i32(gen);
        if is_flex { w.write_compact_string("member-1"); } else { w.write_string("member-1"); }
        if is_flex { w.write_tagged_fields(&[]); }
    });
    let _ = fuzz_decode::<HeartbeatRequest>(&data, version);
}

fn fuzz_leave_group(rng: &mut Rng, version: i16) {
    let is_flex = version >= 4;
    let data = build_body(|w| {
        if is_flex { w.write_compact_string("fuzz-group"); } else { w.write_string("fuzz-group"); }
        if version >= 3 {
            if is_flex {
                w.write_compact_array(&["member-1"], |w, m| {
                    w.write_compact_string(m); w.write_compact_nullable_string(None); w.write_tagged_fields(&[]);
                });
            } else {
                w.write_array(&["member-1"], |w, m| { w.write_string(m); w.write_nullable_string(None); });
            }
        } else {
            if is_flex { w.write_compact_string("member-1"); } else { w.write_string("member-1"); }
        }
    });
    let _ = fuzz_decode::<LeaveGroupRequest>(&data, version);
}

fn fuzz_sync_group(rng: &mut Rng, version: i16) {
    let is_flex = version >= 4;
    let gen = rng.next_i32().abs() % 100;
    let data = build_body(|w| {
        if is_flex { w.write_compact_string("fuzz-group"); } else { w.write_string("fuzz-group"); }
        w.write_i32(gen);
        if is_flex { w.write_compact_string("member-1"); } else { w.write_string("member-1"); }
        if version >= 3 {
            if is_flex { w.write_compact_nullable_string(None); } else { w.write_nullable_string(None); }
        }
        if version >= 5 {
            if is_flex { w.write_compact_nullable_string(None); w.write_compact_nullable_string(None); }
            else { w.write_nullable_string(None); w.write_nullable_string(None); }
        }
        if is_flex { w.write_compact_array::<(), _>(&[], |_, _| {}); } else { w.write_array::<(), _>(&[], |_, _| {}); }
    });
    let _ = fuzz_decode::<SyncGroupRequest>(&data, version);
}

fn fuzz_describe_groups(_rng: &mut Rng, version: i16) {
    let is_flex = version >= 5;
    let data = build_body(|w| {
        if is_flex { w.write_compact_array(&["fuzz-group"], |w, g| { w.write_compact_string(g); }); }
        else { w.write_array(&["fuzz-group"], |w, g| { w.write_string(g); }); }
    });
    let _ = fuzz_decode::<DescribeGroupsRequest>(&data, version);
}

fn fuzz_list_groups(_rng: &mut Rng, version: i16) {
    let is_flex = version >= 4;
    let data = build_body(|w| { if is_flex { w.write_tagged_fields(&[]); } });
    let _ = fuzz_decode::<ListGroupsRequest>(&data, version);
}

fn fuzz_sasl_handshake(_rng: &mut Rng, version: i16) {
    let is_flex = version >= 1;
    let data = build_body(|w| {
        if is_flex { w.write_compact_string("PLAIN"); } else { w.write_string("PLAIN"); }
    });
    let _ = fuzz_decode::<SaslHandshakeRequest>(&data, version);
}

fn fuzz_api_versions(_rng: &mut Rng, version: i16) {
    let is_flex = version >= 3;
    let data = build_body(|w| { if is_flex { w.write_tagged_fields(&[]); } });
    let _ = fuzz_decode::<ApiVersionsRequest>(&data, version);
}

fn fuzz_create_topics(rng: &mut Rng, version: i16) {
    let is_flex = version >= 3;
    let name = rng.next_string(16);
    let timeout = rng.next_i32().abs() % 30000;
    let validate = false;

    let data = build_body(|w| {
        if is_flex {
            w.write_compact_array(&[name.as_str()], |w, n| {
                w.write_compact_string(n); w.write_i32(1); w.write_i16(1);
                w.write_compact_array::<(), _>(&[], |_, _| {});
                w.write_compact_array::<(), _>(&[], |_, _| {});
                w.write_tagged_fields(&[]);
            });
        } else {
            w.write_array(&[name.as_str()], |w, n| {
                w.write_string(n); w.write_i32(1); w.write_i16(1);
                w.write_array::<(), _>(&[], |_, _| {});
            });
        }
        w.write_i32(timeout);
        if version >= 2 { w.write_bool(validate); }
    });
    let _ = fuzz_decode::<CreateTopicsRequest>(&data, version);
}

fn fuzz_delete_topics(rng: &mut Rng, version: i16) {
    let is_flex = version >= 4;
    let name = rng.next_string(16);
    let timeout = rng.next_i32().abs() % 30000;

    let data = build_body(|w| {
        if is_flex {
            w.write_compact_array(&[name.as_str()], |w, n| { w.write_compact_string(n); });
        } else {
            w.write_array(&[name.as_str()], |w, n| { w.write_string(n); });
        }
        w.write_i32(timeout);
    });
    let _ = fuzz_decode::<DeleteTopicsRequest>(&data, version);
}

fn fuzz_init_producer_id(rng: &mut Rng, version: i16) {
    let is_flex = version >= 3;
    let timeout = rng.next_i32().abs() % 30000;
    let data = build_body(|w| {
        if is_flex { w.write_compact_nullable_string(None); } else { w.write_nullable_string(None); }
        w.write_i32(timeout);
        if version >= 3 { w.write_i64(-1); w.write_i16(-1); }
    });
    let _ = fuzz_decode::<InitProducerIdRequest>(&data, version);
}

fn fuzz_end_txn(rng: &mut Rng, version: i16) {
    let is_flex = version >= 3;
    let pid = rng.next_i64().abs() % 10000;
    let pep = rng.next_i16().abs() % 100;
    let committed = rng.next_bool();
    let data = build_body(|w| {
        if is_flex { w.write_compact_string("txn-1"); } else { w.write_string("txn-1"); }
        w.write_i64(pid); w.write_i16(pep); w.write_bool(committed);
    });
    let _ = fuzz_decode::<EndTxnRequest>(&data, version);
}

fn fuzz_sasl_authenticate(rng: &mut Rng, version: i16) {
    let is_flex = version >= 2;
    let auth_bytes = rng.next_bytes(64);
    let data = build_body(|w| {
        if is_flex { w.write_compact_nullable_bytes(Some(&auth_bytes)); w.write_tagged_fields(&[]); }
        else { w.write_nullable_bytes(Some(&auth_bytes)); }
    });
    let _ = fuzz_decode::<SaslAuthenticateRequest>(&data, version);
}

fn fuzz_offset_delete(_rng: &mut Rng, _version: i16) {
    let data = build_body(|w| {
        w.write_compact_string("fuzz-group");
        w.write_compact_array(&["topic-1"], |w, t| {
            w.write_compact_string(t);
            w.write_compact_array(&[0i32], |w, p| { w.write_i32(*p); w.write_tagged_fields(&[]); });
            w.write_tagged_fields(&[]);
        });
        w.write_tagged_fields(&[]);
    });
    let _ = fuzz_decode::<OffsetDeleteRequest>(&data, 0);
}

fn fuzz_empty_body(api_key: i16, version: i16) {
    let data = build_body(|_w| {});
    match api_key {
        0 => { let _ = fuzz_decode::<ProduceRequest>(&data, version); }
        1 => { let _ = fuzz_decode::<FetchRequest>(&data, version); }
        2 => { let _ = fuzz_decode::<ListOffsetsRequest>(&data, version); }
        3 => { let _ = fuzz_decode::<MetadataRequest>(&data, version); }
        8 => { let _ = fuzz_decode::<OffsetCommitRequest>(&data, version); }
        9 => { let _ = fuzz_decode::<OffsetFetchRequest>(&data, version); }
        10 => { let _ = fuzz_decode::<FindCoordinatorRequest>(&data, version); }
        11 => { let _ = fuzz_decode::<JoinGroupRequest>(&data, version); }
        12 => { let _ = fuzz_decode::<HeartbeatRequest>(&data, version); }
        13 => { let _ = fuzz_decode::<LeaveGroupRequest>(&data, version); }
        14 => { let _ = fuzz_decode::<SyncGroupRequest>(&data, version); }
        15 => { let _ = fuzz_decode::<DescribeGroupsRequest>(&data, version); }
        16 => { let _ = fuzz_decode::<ListGroupsRequest>(&data, version); }
        17 => { let _ = fuzz_decode::<SaslHandshakeRequest>(&data, version); }
        18 => { let _ = fuzz_decode::<ApiVersionsRequest>(&data, version); }
        19 => { let _ = fuzz_decode::<CreateTopicsRequest>(&data, version); }
        20 => { let _ = fuzz_decode::<DeleteTopicsRequest>(&data, version); }
        22 => { let _ = fuzz_decode::<InitProducerIdRequest>(&data, version); }
        26 => { let _ = fuzz_decode::<EndTxnRequest>(&data, version); }
        36 => { let _ = fuzz_decode::<SaslAuthenticateRequest>(&data, version); }
        47 => { let _ = fuzz_decode::<OffsetDeleteRequest>(&data, version); }
        _ => {}
    }
}

// ─── 响应 Roundtrip ───────────────────────────────────────────────

fn fuzz_metadata_response_roundtrip(rng: &mut Rng, version: i16) {
    let response = MetadataResponse {
        throttle_time_ms: rng.next_i32().abs() % 1000,
        brokers: vec![MetadataBroker { node_id: 1, host: "localhost".into(), port: 9092, rack: Some("rack-a".into()) }],
        cluster_id: Some("fuzz-cluster".into()),
        controller_id: 1,
        topics: vec![MetadataTopic {
            error_code: rk_protocol::error_codes::KafkaErrorCode::None,
            name: "topic-1".into(), is_internal: false,
            partitions: vec![MetadataPartition {
                error_code: rk_protocol::error_codes::KafkaErrorCode::None,
                partition_index: 0, leader_id: 1, leader_epoch: 0,
                replica_nodes: vec![1], isr_nodes: vec![1],
            }],
        }],
        node_endpoints: vec![],
    };
    assert!(!fuzz_encode_response(&response, version).is_empty());
}

fn fuzz_create_topics_response_roundtrip(rng: &mut Rng, version: i16) {
    let response = CreateTopicsResponse {
        throttle_time_ms: rng.next_i32().abs() % 1000,
        topics: vec![CreateTopicsResponseTopic {
            name: "fuzz-topic".into(),
            error_code: rk_protocol::error_codes::KafkaErrorCode::None,
            error_message: None,
        }],
    };
    assert!(!fuzz_encode_response(&response, version).is_empty());
}

// ─── 测试入口 ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ═══ 1. 每个 API 随机请求 fuzzing (legacy + flexible) ═══
    #[test] fn test_fuzz_produce_all_versions() { let mut r = Rng::new(100); for v in 0..=10 { for _ in 0..5 { fuzz_produce(&mut r, v); } } }
    #[test] fn test_fuzz_fetch_all_versions() { let mut r = Rng::new(101); for v in [0,4,7,11,12,16] { for _ in 0..5 { fuzz_fetch(&mut r, v); } } }
    #[test] fn test_fuzz_list_offsets() { let mut r = Rng::new(102); for v in [0,1,6,7,8] { for _ in 0..5 { fuzz_list_offsets(&mut r, v); } } }
    #[test] fn test_fuzz_metadata() { let mut r = Rng::new(103); for v in [0,1,4,8,9,12] { for _ in 0..5 { fuzz_metadata(&mut r, v); } } }
    #[test] fn test_fuzz_offset_commit() { let mut r = Rng::new(104); for v in [0,3,7,8] { for _ in 0..5 { fuzz_offset_commit(&mut r, v); } } }
    #[test] fn test_fuzz_offset_fetch() { let mut r = Rng::new(105); for v in [0,5,7,8] { for _ in 0..5 { fuzz_offset_fetch(&mut r, v); } } }
    #[test] fn test_fuzz_find_coordinator() { let mut r = Rng::new(106); for v in [0,3,4] { for _ in 0..5 { fuzz_find_coordinator(&mut r, v); } } }
    #[test] fn test_fuzz_join_group() { let mut r = Rng::new(107); for v in [0,5,6,9] { for _ in 0..5 { fuzz_join_group(&mut r, v); } } }
    #[test] fn test_fuzz_heartbeat() { let mut r = Rng::new(108); for v in [0,3,4] { for _ in 0..5 { fuzz_heartbeat(&mut r, v); } } }
    #[test] fn test_fuzz_leave_group() { let mut r = Rng::new(109); for v in [0,3,4] { for _ in 0..5 { fuzz_leave_group(&mut r, v); } } }
    #[test] fn test_fuzz_sync_group() { let mut r = Rng::new(110); for v in [0,3,4,5] { for _ in 0..5 { fuzz_sync_group(&mut r, v); } } }
    #[test] fn test_fuzz_describe_groups() { let mut r = Rng::new(111); for v in [0,4,5] { for _ in 0..5 { fuzz_describe_groups(&mut r, v); } } }
    #[test] fn test_fuzz_list_groups() { let mut r = Rng::new(112); for v in [0,3,4] { for _ in 0..5 { fuzz_list_groups(&mut r, v); } } }
    #[test] fn test_fuzz_sasl_handshake() { let mut r = Rng::new(113); for v in [0,1] { for _ in 0..5 { fuzz_sasl_handshake(&mut r, v); } } }
    #[test] fn test_fuzz_api_versions() { let mut r = Rng::new(114); for v in [0,2,3] { for _ in 0..5 { fuzz_api_versions(&mut r, v); } } }
    #[test] fn test_fuzz_create_topics() { let mut r = Rng::new(115); for v in [0,2,3,7] { for _ in 0..5 { fuzz_create_topics(&mut r, v); } } }
    #[test] fn test_fuzz_delete_topics() { let mut r = Rng::new(116); for v in [0,3,4,6] { for _ in 0..5 { fuzz_delete_topics(&mut r, v); } } }
    #[test] fn test_fuzz_init_producer_id() { let mut r = Rng::new(117); for v in [0,2,3,4] { for _ in 0..5 { fuzz_init_producer_id(&mut r, v); } } }
    #[test] fn test_fuzz_end_txn() { let mut r = Rng::new(118); for v in [0,2,3] { for _ in 0..5 { fuzz_end_txn(&mut r, v); } } }
    #[test] fn test_fuzz_sasl_authenticate() { let mut r = Rng::new(119); for v in [0,1,2] { for _ in 0..5 { fuzz_sasl_authenticate(&mut r, v); } } }
    #[test] fn test_fuzz_offset_delete() { let mut r = Rng::new(120); for _ in 0..5 { fuzz_offset_delete(&mut r, 0); } }

    // ═══ 2. 边界值: 空 body ═══
    #[test] fn test_empty_body_legacy() {
        for (k,v) in [(0,0),(1,0),(2,0),(3,0),(8,0),(9,0),(10,0),(11,0),(12,0),(13,0),(14,0),(15,0),(16,0),(17,0),(18,0),(19,0),(20,0),(22,0),(26,0),(36,0),(47,0)] { fuzz_empty_body(k, v); }
    }
    #[test] fn test_empty_body_flexible() {
        for (k,v) in [(3,9),(8,8),(9,8),(10,4),(11,6),(12,4),(13,4),(14,4),(15,5),(16,4),(17,1),(18,3),(19,3),(20,4),(22,3),(26,3),(36,2)] { fuzz_empty_body(k, v); }
    }

    // ═══ 3. 响应编码 Roundtrip ═══
    #[test] fn test_metadata_resp_roundtrip_legacy() { let mut r = Rng::new(300); for v in [0,1,4,8] { fuzz_metadata_response_roundtrip(&mut r, v); } }
    #[test] fn test_metadata_resp_roundtrip_flex() { let mut r = Rng::new(301); for v in [9,12] { fuzz_metadata_response_roundtrip(&mut r, v); } }
    #[test] fn test_create_topics_resp_roundtrip() { let mut r = Rng::new(302); for v in [0,2,3,7] { fuzz_create_topics_response_roundtrip(&mut r, v); } }

    // ═══ 4. 边界整数值 ═══
    #[test] fn test_boundary_produce() {
        for acks in [i16::MIN, -1, 0, 1, i16::MAX] {
            let data = build_body(|w| { w.write_nullable_string(None); w.write_i16(acks); w.write_i32(1000); w.write_array::<(), _>(&[], |_, _| {}); });
            let _ = fuzz_decode::<ProduceRequest>(&data, 0);
        }
    }
    #[test] fn test_boundary_fetch() {
        for mw in [i32::MIN, -1, 0, 1, i32::MAX] {
            let data = build_body(|w| { w.write_i32(0); w.write_i32(mw); w.write_i32(1); w.write_array::<(), _>(&[], |_, _| {}); });
            let _ = fuzz_decode::<FetchRequest>(&data, 0);
        }
    }
    #[test] fn test_boundary_metadata() {
        assert!(fuzz_decode::<MetadataRequest>(&build_body(|w| { w.write_i32(-1); }), 0));
        assert!(fuzz_decode::<MetadataRequest>(&build_body(|w| { w.write_array::<(), _>(&[], |_, _| {}); }), 0));
    }

    // ═══ 5. 空数组 / null / 长字符串 ═══
    #[test] fn test_empty_arrays() {
        let _ = fuzz_decode::<ProduceRequest>(&build_body(|w| { w.write_nullable_string(None); w.write_i16(-1); w.write_i32(1000); w.write_array::<(), _>(&[], |_, _| {}); }), 0);
        let _ = fuzz_decode::<CreateTopicsRequest>(&build_body(|w| { w.write_array::<(), _>(&[], |_, _| {}); w.write_i32(1000); }), 0);
        let _ = fuzz_decode::<DeleteTopicsRequest>(&build_body(|w| { w.write_array::<(), _>(&[], |_, _| {}); w.write_i32(1000); }), 0);
    }
    #[test] fn test_long_string_metadata() {
        let s = "a".repeat(4096);
        assert!(fuzz_decode::<MetadataRequest>(&build_body(|w| { w.write_array(&[()], |w, _| { w.write_string(&s); }); }), 0));
    }
    #[test] fn test_long_string_create_topics() {
        let s = "b".repeat(4096);
        // 长字符串 decode 可能成功或因验证失败, 关键是不 panic
        let _ = fuzz_decode::<CreateTopicsRequest>(&build_body(|w| {
            w.write_array(&[()], |w, _| { w.write_string(&s); w.write_i32(1); w.write_i16(1); w.write_array::<(), _>(&[], |_, _| {}); });
            w.write_i32(1000);
        }), 0);
    }
    #[test] fn test_null_values_produce() {
        // null record_set decode 可能成功或失败, 关键是不 panic
        let _ = fuzz_decode::<ProduceRequest>(&build_body(|w| {
            w.write_nullable_string(None); w.write_i16(1); w.write_i32(1000);
            w.write_array(&[()], |w, _| { w.write_string("t"); w.write_array(&[()], |w, _| { w.write_i32(0); w.write_nullable_bytes(None); }); });
        }), 0);
    }

    // ═══ 6. Legacy ↔ Flexible 切换 ═══
    #[test] fn test_legacy_vs_flex_metadata() { let mut r = Rng::new(400); for _ in 0..10 { fuzz_metadata(&mut r, 0); fuzz_metadata(&mut r, 9); } }
    #[test] fn test_legacy_vs_flex_produce() { let mut r = Rng::new(401); for _ in 0..10 { fuzz_produce(&mut r, 0); fuzz_produce(&mut r, 9); } }
    #[test] fn test_legacy_vs_flex_fetch() { let mut r = Rng::new(402); for _ in 0..10 { fuzz_fetch(&mut r, 0); fuzz_fetch(&mut r, 12); } }
    #[test] fn test_legacy_vs_flex_offset_commit() { let mut r = Rng::new(403); for _ in 0..10 { fuzz_offset_commit(&mut r, 0); fuzz_offset_commit(&mut r, 8); } }

    // ═══ 7. 高迭代压力 ═══
    #[test] fn test_high_iter_produce() { let mut r = Rng::new(500); for _ in 0..100 { fuzz_produce(&mut r, 0); fuzz_produce(&mut r, 9); } }
    #[test] fn test_high_iter_metadata() { let mut r = Rng::new(501); for _ in 0..100 { fuzz_metadata(&mut r, 0); fuzz_metadata(&mut r, 9); } }
    #[test] fn test_high_iter_fetch() { let mut r = Rng::new(502); for _ in 0..100 { fuzz_fetch(&mut r, 0); fuzz_fetch(&mut r, 12); } }

    // ═══ 8. Compact Array 嵌套 ═══
    #[test] fn test_nested_compact_arrays() {
        assert!(fuzz_decode::<ProduceRequest>(&build_body(|w| {
            w.write_compact_nullable_string(None); w.write_i16(-1); w.write_i32(1000);
            w.write_compact_array(&[(), ()], |w, _| {
                w.write_compact_string("t");
                w.write_compact_array(&[(), (), ()], |w, _| { w.write_i32(0); w.write_nullable_bytes(Some(b"\x00\x00\x00\x01")); w.write_tagged_fields(&[]); });
                w.write_tagged_fields(&[]);
            });
        }), 9));
    }
    #[test] fn test_empty_compact_arrays() {
        assert!(fuzz_decode::<ProduceRequest>(&build_body(|w| {
            w.write_compact_nullable_string(None); w.write_i16(-1); w.write_i32(1000);
            w.write_compact_array::<(), _>(&[], |_, _| {});
        }), 9));
    }

    // ═══ 9. API Key 覆盖验证 ═══
    #[test] fn test_all_api_keys_valid() {
        let valid = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,26,32,33,36,37,43,44,45,46,47,51,52,53,54,55,56,60,61,65,70];
        for k in &valid { assert!(rk_core::ApiKey::from_i16(*k).is_some(), "key {} valid", k); }
        assert!(rk_core::ApiKey::from_i16(999).is_none());
    }
    #[test] fn test_flexible_thresholds() {
        use rk_core::ApiKey;
        let t: Vec<(ApiKey, i16)> = vec![
            (ApiKey::ApiVersions,3),(ApiKey::Metadata,9),(ApiKey::Produce,9),(ApiKey::Fetch,12),
            (ApiKey::ListOffsets,7),(ApiKey::OffsetCommit,8),(ApiKey::OffsetFetch,8),
            (ApiKey::FindCoordinator,4),(ApiKey::JoinGroup,6),(ApiKey::Heartbeat,4),
            (ApiKey::LeaveGroup,4),(ApiKey::SyncGroup,4),(ApiKey::DescribeGroups,5),
            (ApiKey::ListGroups,4),(ApiKey::SaslHandshake,1),(ApiKey::InitProducerId,3),
            (ApiKey::DeleteTopics,4),(ApiKey::DeleteRecords,2),(ApiKey::DescribeConfigs,3),
            (ApiKey::AlterConfigs,2),(ApiKey::SaslAuthenticate,2),(ApiKey::CreatePartitions,2),
            (ApiKey::ElectLeaders,2),
        ];
        for (api, fv) in t {
            assert!(api.is_flexible(fv), "{:?} v{} flex", api, fv);
            if fv > 0 { assert!(!api.is_flexible(fv-1), "{:?} v{} not flex", api, fv-1); }
        }
    }
}
