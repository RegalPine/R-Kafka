//! 基准测试: 单 Partition 写入吞吐
//!
//! 测量 CommitLog 顺序写入性能。
//! 运行: cargo bench -p rk-storage

use std::time::Instant;

use rk_core::types::{PartitionId, TopicName};
use rk_storage::log_io::build_batch_bytes;
use rk_storage::CommitLog;
use tempfile::tempdir;

/// 生成指定大小的 batch
fn make_batch_with_size(base_offset: i64, record_count: i32, record_size: usize) -> Vec<u8> {
    let records = vec![0xABu8; record_count as usize * record_size];
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

/// 基准测试: 顺序写入吞吐
///
/// 写入 N 个 batch，每个 batch 包含 M 条 record，每条 record 大小 S 字节。
/// 测量总吞吐 (MB/s) 和延迟 (μs/batch)。
fn bench_sequential_write(
    batch_count: usize,
    records_per_batch: i32,
    record_size: usize,
) -> (f64, f64) {
    let dir = tempdir().unwrap();
    let mut cl = CommitLog::create(
        dir.path(),
        TopicName("bench".into()),
        PartitionId(0),
        1_073_741_824,
    )
    .unwrap();

    let total_records = batch_count as i64 * records_per_batch as i64;
    let bytes_per_batch = (records_per_batch as usize) * record_size;
    let total_bytes = batch_count * bytes_per_batch;

    // 预热
    let warmup_batch = make_batch_with_size(0, records_per_batch, record_size);
    cl.append_batch(&warmup_batch).unwrap();

    // 正式测试
    let start = Instant::now();
    let mut offset = records_per_batch as i64; // 从预热后开始
    for _ in 1..batch_count {
        let batch = make_batch_with_size(offset, records_per_batch, record_size);
        cl.append_batch(&batch).unwrap();
        offset += records_per_batch as i64;
    }
    let elapsed = start.elapsed();

    let elapsed_secs = elapsed.as_secs_f64();
    let throughput_mb = (total_bytes as f64) / (1024.0 * 1024.0) / elapsed_secs;
    let latency_us = elapsed.as_micros() as f64 / batch_count as f64;

    // 刷盘
    cl.flush_all().unwrap();

    assert_eq!(cl.log_end_offset().0, total_records);

    (throughput_mb, latency_us)
}

#[test]
fn bench_write_128b() {
    let (throughput, latency) = bench_sequential_write(10_000, 10, 128);
    println!("\n=== Write Benchmark: 128B records ===");
    println!("  Batches:    10,000");
    println!("  Records:    10/batch (128B each)");
    println!(
        "  Total:      {:.2} MB",
        10_000.0 * 10.0 * 128.0 / 1024.0 / 1024.0
    );
    println!("  Throughput: {:.2} MB/s", throughput);
    println!("  Latency:    {:.2} μs/batch", latency);
}

#[test]
fn bench_write_1kb() {
    let (throughput, latency) = bench_sequential_write(10_000, 10, 1024);
    println!("\n=== Write Benchmark: 1KB records ===");
    println!("  Batches:    10,000");
    println!("  Records:    10/batch (1KB each)");
    println!(
        "  Total:      {:.2} MB",
        10_000.0 * 10.0 * 1024.0 / 1024.0 / 1024.0
    );
    println!("  Throughput: {:.2} MB/s", throughput);
    println!("  Latency:    {:.2} μs/batch", latency);
}

#[test]
fn bench_write_10kb() {
    let (throughput, latency) = bench_sequential_write(5_000, 10, 10_240);
    println!("\n=== Write Benchmark: 10KB records ===");
    println!("  Batches:    5,000");
    println!("  Records:    10/batch (10KB each)");
    println!(
        "  Total:      {:.2} MB",
        5_000.0 * 10.0 * 10_240.0 / 1024.0 / 1024.0
    );
    println!("  Throughput: {:.2} MB/s", throughput);
    println!("  Latency:    {:.2} μs/batch", latency);
}

#[test]
fn bench_write_large_batch() {
    let (throughput, latency) = bench_sequential_write(1_000, 100, 1024);
    println!("\n=== Write Benchmark: 100 records × 1KB per batch ===");
    println!("  Batches:    1,000");
    println!("  Records:    100/batch (1KB each, 100KB total/batch)");
    println!(
        "  Total:      {:.2} MB",
        1_000.0 * 100.0 * 1024.0 / 1024.0 / 1024.0
    );
    println!("  Throughput: {:.2} MB/s", throughput);
    println!("  Latency:    {:.2} μs/batch", latency);
}
