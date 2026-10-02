//! 协议 Fuzzing 测试: 随机帧解码健壮性
//!
//! 生成大量随机字节序列，尝试解码为各种请求/响应类型。
//! 关键断言: 解码器要么返回 Ok，要么返回 Err，绝不能 panic。

use rand::Rng;
use rk_protocol::apis::*;
use rk_protocol::codec::KafkaRequestDecoder;
use rk_protocol::types::KafkaReader;

/// 对每种解码器运行 fuzzing: 确保不 panic
fn fuzz_decoder<T: KafkaRequestDecoder>(_name: &str, version: i16, data: &[u8]) -> bool {
    let mut reader = KafkaReader::new(data);
    match T::decode(&mut reader, version) {
        Ok(_) => true,
        Err(_) => false, // 错误可接受，panic 不可接受
    }
}

/// 生成随机字节数据 (限制大小避免巨大分配)
fn random_data(rng: &mut impl Rng, max_len: usize) -> Vec<u8> {
    let len = rng.gen_range(0..=max_len.min(128));
    (0..len).map(|_| rng.gen()).collect()
}

/// 生成带结构的随机数据 (更可能触发边界条件)
fn semi_structured_data(rng: &mut impl Rng, max_len: usize) -> Vec<u8> {
    let len = rng.gen_range(0..=max_len);
    let mut data = Vec::with_capacity(len);

    // 混合: 部分随机字节 + 部分结构化片段
    while data.len() < len {
        match rng.gen_range(0..4) {
            0 => data.push(rng.gen()), // 随机字节
            1 => {
                // 小长度前缀 (避免巨大分配)
                let l: i32 = rng.gen_range(-1..=10);
                data.extend_from_slice(&l.to_be_bytes());
            }
            2 => {
                // 小整数
                let v: i16 = rng.gen_range(-2..=10);
                data.extend_from_slice(&v.to_be_bytes());
            }
            3 => {
                // 全零块
                let block_len = rng.gen_range(1..=16).min(len - data.len());
                data.extend(std::iter::repeat(0u8).take(block_len));
            }
            _ => unreachable!(),
        }
    }

    data.truncate(len);
    data
}

#[test]
fn test_fuzz_produce_request() {
    let mut rng = rand::thread_rng();
    let mut ok_count = 0u32;

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 3, 5, 9] {
            if fuzz_decoder::<ProduceRequest>("ProduceRequest", version, &data) {
                ok_count += 1;
            }
        }
    }
    // 关键是没 panic; 统计仅用于信息
    assert!(ok_count > 0);
}

#[test]
fn test_fuzz_fetch_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 4, 7, 12] {
            let _ = fuzz_decoder::<FetchRequest>("FetchRequest", version, &data);
        }
    }
}

#[test]
fn test_fuzz_metadata_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 1, 4, 9] {
            let _ = fuzz_decoder::<MetadataRequest>("MetadataRequest", version, &data);
        }
    }
}

#[test]
fn test_fuzz_list_offsets_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 1, 4, 7] {
            let _ = fuzz_decoder::<ListOffsetsRequest>("ListOffsetsRequest", version, &data);
        }
    }
}

#[test]
fn test_fuzz_create_topics_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 1, 2] {
            let _ = fuzz_decoder::<CreateTopicsRequest>("CreateTopicsRequest", version, &data);
        }
    }
}

#[test]
fn test_fuzz_delete_topics_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 1, 3] {
            let _ = fuzz_decoder::<DeleteTopicsRequest>("DeleteTopicsRequest", version, &data);
        }
    }
}

#[test]
fn test_fuzz_find_coordinator_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 1, 4] {
            let _ = fuzz_decoder::<FindCoordinatorRequest>("FindCoordinatorRequest", version, &data);
        }
    }
}

#[test]
fn test_fuzz_describe_configs_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 1, 3] {
            let _ = fuzz_decoder::<DescribeConfigsRequest>("DescribeConfigsRequest", version, &data);
        }
    }
}

#[test]
fn test_fuzz_alter_configs_request() {
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let data = if rng.gen_bool(0.5) {
            random_data(&mut rng, 256)
        } else {
            semi_structured_data(&mut rng, 256)
        };

        for version in [0i16, 1, 2] {
            let _ = fuzz_decoder::<AlterConfigsRequest>("AlterConfigsRequest", version, &data);
        }
    }
}

/// 空数据 fuzzing: 确保空输入不 panic
#[test]
fn test_fuzz_empty_data() {
    let empty: &[u8] = &[];
    for version in 0..10i16 {
        let _ = fuzz_decoder::<ProduceRequest>("ProduceRequest", version, empty);
        let _ = fuzz_decoder::<FetchRequest>("FetchRequest", version, empty);
        let _ = fuzz_decoder::<MetadataRequest>("MetadataRequest", version, empty);
        let _ = fuzz_decoder::<ListOffsetsRequest>("ListOffsetsRequest", version, empty);
        let _ = fuzz_decoder::<CreateTopicsRequest>("CreateTopicsRequest", version, empty);
        let _ = fuzz_decoder::<DeleteTopicsRequest>("DeleteTopicsRequest", version, empty);
        let _ = fuzz_decoder::<FindCoordinatorRequest>("FindCoordinatorRequest", version, empty);
        let _ = fuzz_decoder::<DescribeConfigsRequest>("DescribeConfigsRequest", version, empty);
        let _ = fuzz_decoder::<AlterConfigsRequest>("AlterConfigsRequest", version, empty);
    }
}

/// 单字节 fuzzing: 确保 1 字节输入不 panic
#[test]
fn test_fuzz_single_byte() {
    for b in 0..=255u8 {
        let data = [b];
        for version in 0..5i16 {
            let _ = fuzz_decoder::<ProduceRequest>("ProduceRequest", version, &data);
            let _ = fuzz_decoder::<FetchRequest>("FetchRequest", version, &data);
            let _ = fuzz_decoder::<MetadataRequest>("MetadataRequest", version, &data);
            let _ = fuzz_decoder::<ListOffsetsRequest>("ListOffsetsRequest", version, &data);
            let _ = fuzz_decoder::<CreateTopicsRequest>("CreateTopicsRequest", version, &data);
            let _ = fuzz_decoder::<DeleteTopicsRequest>("DeleteTopicsRequest", version, &data);
            let _ = fuzz_decoder::<FindCoordinatorRequest>("FindCoordinatorRequest", version, &data);
            let _ = fuzz_decoder::<DescribeConfigsRequest>("DescribeConfigsRequest", version, &data);
            let _ = fuzz_decoder::<AlterConfigsRequest>("AlterConfigsRequest", version, &data);
        }
    }
}

/// 极端长度 fuzzing: 确保超大长度前缀不 panic
#[test]
fn test_fuzz_extreme_lengths() {
    // 长度前缀 = i32::MAX
    let mut data = Vec::new();
    data.extend_from_slice(&i32::MAX.to_be_bytes());
    data.extend_from_slice(&i32::MAX.to_be_bytes());
    data.extend_from_slice(&[0u8; 8]);

    for version in 0..5i16 {
        let _ = fuzz_decoder::<ProduceRequest>("ProduceRequest", version, &data);
        let _ = fuzz_decoder::<FetchRequest>("FetchRequest", version, &data);
        let _ = fuzz_decoder::<MetadataRequest>("MetadataRequest", version, &data);
        let _ = fuzz_decoder::<ListOffsetsRequest>("ListOffsetsRequest", version, &data);
    }

    // 长度前缀 = -1 (null)
    let mut data2 = Vec::new();
    data2.extend_from_slice(&(-1i32).to_be_bytes());
    data2.extend_from_slice(&(-1i32).to_be_bytes());

    for version in 0..5i16 {
        let _ = fuzz_decoder::<CreateTopicsRequest>("CreateTopicsRequest", version, &data2);
        let _ = fuzz_decoder::<DeleteTopicsRequest>("DeleteTopicsRequest", version, &data2);
    }
}
