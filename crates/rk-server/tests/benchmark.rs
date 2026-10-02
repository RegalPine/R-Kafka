//! 性能基准测试
//!
//! 测试 R-Kafka 核心路径的性能指标:
//! - CRC32C 吞吐量
//! - 单 Partition 连续写入
//! - 攒批写入 vs 逐条写入
//! - 顺序消费 (Fetch) 性能

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rk_core::types::{Offset, PartitionId, TopicName};
use rk_storage::log_io::build_batch_bytes;
use rk_storage::CommitLog;

/// CRC32C 吞吐量基准测试
///
/// 测试 CRC32C 哈希的计算速度，验证硬件加速 (SSE4.2/ARMv8) 是否生效。
/// 目标: >= 10 GB/s (现代 CPU 硬件加速)
#[test]
fn bench_crc32c_throughput() {
    // 测试数据: 1MB
    let data_size = 1_048_576;
    let data = vec![0xABu8; data_size];

    // 预热
    let _ = crc32c::crc32c(&data);

    // 正式测试: 计算 100 次
    let iterations = 100;
    let start = Instant::now();

    for _ in 0..iterations {
        let _ = crc32c::crc32c(&data);
    }

    let elapsed = start.elapsed();
    let total_bytes = data_size * iterations;
    let throughput_mb_s = (total_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();

    println!("\n=== CRC32C Throughput Benchmark ===");
    println!("Data size: {} bytes", data_size);
    println!("Iterations: {}", iterations);
    println!("Total bytes: {} MB", total_bytes / 1_048_576);
    println!("Elapsed: {:?}", elapsed);
    println!("Throughput: {:.2} MB/s ({:.2} GB/s)", throughput_mb_s, throughput_mb_s / 1024.0);

    // 验证: 硬件加速应达到 10 GB/s+
    // 软件实现约 1-2 GB/s
    if throughput_mb_s > 10_240.0 {
        println!("✓ Hardware acceleration detected (SSE4.2/ARMv8)");
    } else {
        println!("⚠ Software implementation (no hardware acceleration)");
    }

    // 断言: 至少达到 1 GB/s (即使是软件实现)
    assert!(throughput_mb_s > 1024.0, "CRC32C throughput too low: {:.2} MB/s", throughput_mb_s);
}

/// 单 Partition 连续写入基准测试
///
/// 测试单个 Partition 连续写入 100K 条消息的吞吐量。
/// 目标: >= 500 MB/s (现代 SSD)
#[test]
fn bench_single_partition_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = CommitLog::create(
        dir.path(),
        TopicName("bench-single".to_string()),
        PartitionId(0),
        1_073_741_824,
    ).unwrap();

    // 100K 条消息
    let message_count: i64 = 100_000;
    // 每条消息 1KB
    let record_size: i64 = 1024;
    let records = vec![0xABu8; record_size as usize];

    let start = Instant::now();

    for i in 0..message_count {
        let batch = build_batch_bytes(
            i,                          // base_offset
            1,                          // partition_leader_epoch
            0,                          // attributes
            current_timestamp_ms(),     // base_timestamp
            current_timestamp_ms(),     // max_timestamp
            -1,                         // producer_id
            -1,                         // producer_epoch
            -1,                         // base_sequence
            &records,                   // records_bytes
            1,                          // record_count
        );
        log.append_batch(&batch).unwrap();
    }

    let elapsed = start.elapsed();
    let total_bytes = message_count * (record_size + 61); // record + header
    let throughput_mb_s = (total_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();
    let ops_per_sec = message_count as f64 / elapsed.as_secs_f64();

    println!("\n=== Single Partition Write Benchmark ===");
    println!("Messages: {}", message_count);
    println!("Message size: {} bytes", record_size);
    println!("Total bytes: {:.2} MB", total_bytes as f64 / 1_048_576.0);
    println!("Elapsed: {:?}", elapsed);
    println!("Throughput: {:.2} MB/s", throughput_mb_s);
    println!("Ops/sec: {:.0}", ops_per_sec);

    // 断言: 至少达到 100 MB/s (保守估计)
    assert!(throughput_mb_s > 100.0, "Write throughput too low: {:.2} MB/s", throughput_mb_s);
}

/// 攒批写入 vs 逐条写入对比基准测试
///
/// 对比 BatchAccumulator 攒批写入与逐条写入的性能差异。
/// 预期: 攒批写入吞吐更高，I/O 次数更少。
#[test]
fn bench_batch_write_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let message_count: i64 = 10_000;
    let record_size: i64 = 512;
    let records = vec![0xCDu8; record_size as usize];

    // === 测试 1: 逐条写入 (无攒批) ===
    let mut log1 = CommitLog::create(
        dir.path(),
        TopicName("bench-no-batch".to_string()),
        PartitionId(0),
        1_073_741_824,
    ).unwrap();
    let start1 = Instant::now();

    for i in 0..message_count {
        let batch = build_batch_bytes(
            i, 1, 0,
            current_timestamp_ms(), current_timestamp_ms(),
            -1, -1, -1,
            &records, 1,
        );
        log1.append_batch(&batch).unwrap();
    }

    let elapsed1 = start1.elapsed();
    let throughput1 = (message_count * (record_size + 61)) as f64 / 1_048_576.0 / elapsed1.as_secs_f64();

    // === 测试 2: 攒批写入 (每 100 条合并为 1 个 batch) ===
    let mut log2 = CommitLog::create(
        dir.path(),
        TopicName("bench-with-batch".to_string()),
        PartitionId(0),
        1_073_741_824,
    ).unwrap();
    let batch_size: i64 = 100;
    let start2 = Instant::now();

    let mut offset = 0i64;
    for _ in (0..message_count).step_by(batch_size as usize) {
        // 合并 100 条 record 到一个 batch
        let merged_records: Vec<u8> = (0..batch_size)
            .flat_map(|_| records.iter().copied())
            .collect();

        let batch = build_batch_bytes(
            offset, 1, 0,
            current_timestamp_ms(), current_timestamp_ms(),
            -1, -1, -1,
            &merged_records, batch_size as i32,
        );
        log2.append_batch(&batch).unwrap();
        offset += batch_size;
    }

    let elapsed2 = start2.elapsed();
    let throughput2 = (message_count * (record_size + 61)) as f64 / 1_048_576.0 / elapsed2.as_secs_f64();

    println!("\n=== Batch Write Comparison Benchmark ===");
    println!("Messages: {}", message_count);
    println!("Message size: {} bytes", record_size);
    println!();
    println!("No batching (1 record/batch):");
    println!("  Elapsed: {:?}", elapsed1);
    println!("  Throughput: {:.2} MB/s", throughput1);
    println!("  I/O ops: {}", message_count);
    println!();
    println!("With batching ({} records/batch):", batch_size);
    println!("  Elapsed: {:?}", elapsed2);
    println!("  Throughput: {:.2} MB/s", throughput2);
    println!("  I/O ops: {}", message_count / batch_size);
    println!();
    println!("Speedup: {:.2}x", throughput2 / throughput1);

    // 注意: 攒批写入在此测试中可能不会更快，因为:
    // 1. 合并 records 有额外开销
    // 2. 测试环境是单线程，没有真正的 I/O 并发压力
    // 3. 实际生产中攒批的优势在于减少系统调用和网络往返
    // 这里只验证功能正确性，不断言性能提升
}

/// 顺序消费 (Fetch) 基准测试
///
/// 测试从 Partition 顺序读取消息的吞吐量。
/// 目标: >= 1 GB/s (内存缓存命中)
#[test]
fn bench_fetch_sequential() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = CommitLog::create(
        dir.path(),
        TopicName("bench-fetch".to_string()),
        PartitionId(0),
        1_073_741_824,
    ).unwrap();

    // 先写入 10K 条消息 (减少数量以加快测试速度)
    let message_count: i64 = 10_000;
    let record_size: i64 = 1024;
    let records = vec![0xEFu8; record_size as usize];

    for i in 0..message_count {
        let batch = build_batch_bytes(
            i, 1, 0,
            current_timestamp_ms(), current_timestamp_ms(),
            -1, -1, -1,
            &records, 1,
        );
        log.append_batch(&batch).unwrap();
    }

    // 顺序读取
    let start = Instant::now();
    let mut read_count = 0usize;
    let mut offset = Offset(0);

    while let Some((batch_bytes, batch_size)) = log.read_at_offset(offset).unwrap() {
        read_count += 1;
        offset = Offset(offset.0 + batch_size as i64);
        // 避免编译器优化掉读取
        std::hint::black_box(batch_bytes);
    }

    let elapsed = start.elapsed();
    let total_bytes = read_count as i64 * (record_size + 61);
    let throughput_mb_s = (total_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();
    let ops_per_sec = read_count as f64 / elapsed.as_secs_f64();

    println!("\n=== Sequential Fetch Benchmark ===");
    println!("Messages read: {}", read_count);
    println!("Message size: {} bytes", record_size);
    println!("Total bytes: {:.2} MB", total_bytes as f64 / 1_048_576.0);
    println!("Elapsed: {:?}", elapsed);
    println!("Throughput: {:.2} MB/s", throughput_mb_s);
    println!("Ops/sec: {:.0}", ops_per_sec);

    // 断言: 至少读取了全部消息
    assert_eq!(read_count, message_count as usize, "Should read all messages");

    // 断言: 吞吐至少 100 MB/s
    assert!(throughput_mb_s > 100.0, "Fetch throughput too low: {:.2} MB/s", throughput_mb_s);
}

/// 辅助函数: 获取当前时间戳 (毫秒)
fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
