//! 磁盘级 RecordBatch 读写 (文件 I/O)
//!
//! 负责将 RecordBatch 序列化到磁盘文件和从磁盘文件反序列化。
//! 磁盘格式与 Kafka RecordBatch v2 完全一致。
//!
//! 与 rk-protocol/record.rs 的区别:
//! - rk-protocol/record.rs: wire format 编解码 (网络传输)
//! - rk-storage/log_io.rs: 磁盘文件级 I/O (追加写、随机读)

use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};

use bytes::{BytesMut, BufMut};
use rk_core::error::Result;
use rk_protocol::record::{
    RecordBatchHeader, HEADER_SIZE, RECORDBATCH_MAGIC, CRC_OFFSET,
    decode_batch_header,
};
use rk_protocol::types::KafkaReader;

/// 磁盘上一个 RecordBatch 的最小可读大小 (仅头部)
pub const MIN_BATCH_HEADER_SIZE: usize = HEADER_SIZE;

/// 将完整 RecordBatch 写入文件 (追加模式)
///
/// `batch_bytes` 包含从 base_offset 到 records 末尾的全部字节。
/// 返回写入的字节数。
pub fn write_batch_to_file(file: &mut File, batch_bytes: &[u8]) -> Result<u64> {
    let pos = file.seek(SeekFrom::End(0))?;
    file.write_all(batch_bytes)?;
    Ok(pos)
}

/// 从文件指定偏移读取一个完整 RecordBatch
///
/// 返回 (batch_bytes, batch_total_size)
pub fn read_batch_from_file(file: &mut File, offset: u64) -> Result<(Vec<u8>, u64)> {
    file.seek(SeekFrom::Start(offset))?;

    // 先读头部 61 字节 (含 base_offset + batch_length + ... + record_count)
    let mut header_buf = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf)?;

    // 解析 batch_length (offset 8..12) 来确定整个 batch 的大小
    let batch_length = i32::from_be_bytes([
        header_buf[8], header_buf[9], header_buf[10], header_buf[11],
    ]);

    // 整个 batch 在磁盘上的总大小 = base_offset(8) + batch_length(4) + batch_length
    // batch_length 字段本身不包含 base_offset(8) 和 batch_length(4) 自身的长度
    let total_size = 8 + 4 + batch_length as u64;

    // 如果 batch 还有 records 部分没读完
    let mut batch_bytes = header_buf;
    if total_size > HEADER_SIZE as u64 {
        let remaining = (total_size - HEADER_SIZE as u64) as usize;
        let mut rest = vec![0u8; remaining];
        file.read_exact(&mut rest)?;
        batch_bytes.extend_from_slice(&rest);
    }

    Ok((batch_bytes, total_size))
}

/// 从文件指定偏移仅读取 RecordBatch 头部 (不读 records)
pub fn read_batch_header_from_file(file: &mut File, offset: u64) -> Result<RecordBatchHeader> {
    file.seek(SeekFrom::Start(offset))?;
    let mut header_buf = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf)?;

    let mut reader = KafkaReader::new(&header_buf);
    decode_batch_header(&mut reader)
}

/// 构建一个完整的 RecordBatch 磁盘字节
///
/// 参数:
/// - `base_offset`: 本批次第一条消息的 offset
/// - `partition_leader_epoch`: 当前 leader epoch
/// - `attributes`: 压缩类型等属性
/// - `base_timestamp`: 第一条消息时间戳
/// - `max_timestamp`: 最大消息时间戳
/// - `producer_id`: 幂等 producer id (-1 = 未启用)
/// - `producer_epoch`: 幂等 producer epoch
/// - `base_sequence`: 幂等 base sequence (-1 = 未启用)
/// - `records_bytes`: 已编码的 records 字节 (record_count 条 record 的连续编码)
/// - `record_count`: record 数量
///
/// 返回完整的 batch 字节 (含 CRC32C 自动计算)
pub fn build_batch_bytes(
    base_offset: i64,
    partition_leader_epoch: i32,
    attributes: i16,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records_bytes: &[u8],
    record_count: i32,
) -> Vec<u8> {
    let last_offset_delta = if record_count > 0 { record_count - 1 } else { 0 };

    // batch_length = partition_leader_epoch(4) + magic(1) + crc(4)
    //   + attributes(2) + last_offset_delta(4) + base_timestamp(8) + max_timestamp(8)
    //   + producer_id(8) + producer_epoch(2) + base_sequence(4) + records_count(4)
    //   + records_bytes.len()
    // = 49 + records_bytes.len()
    let batch_length: i32 = (49 + records_bytes.len()) as i32;

    // 先编码从 magic 开始到 records 结束的部分 (用于 CRC 计算)
    let crc_payload_size = 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + records_bytes.len();
    let mut crc_buf = BytesMut::with_capacity(crc_payload_size);
    crc_buf.put_i8(RECORDBATCH_MAGIC);
    crc_buf.put_i32(0); // placeholder for CRC
    crc_buf.put_i16(attributes);
    crc_buf.put_i32(last_offset_delta);
    crc_buf.put_i64(base_timestamp);
    crc_buf.put_i64(max_timestamp);
    crc_buf.put_i64(producer_id);
    crc_buf.put_i16(producer_epoch);
    crc_buf.put_i32(base_sequence);
    crc_buf.put_i32(record_count);
    crc_buf.extend_from_slice(records_bytes);

    // 计算 CRC32C (从 magic 开始)
    let crc = crc32c::crc32c(&crc_buf);

    // 回填 CRC
    let crc_bytes = crc.to_be_bytes();
    crc_buf[1] = crc_bytes[0];
    crc_buf[2] = crc_bytes[1];
    crc_buf[3] = crc_bytes[2];
    crc_buf[4] = crc_bytes[3];

    // 组装完整 batch: base_offset(8) + batch_length(4) + partition_leader_epoch(4) + crc_payload
    let mut batch = Vec::with_capacity(8 + 4 + 4 + crc_payload_size);
    batch.extend_from_slice(&base_offset.to_be_bytes());
    batch.extend_from_slice(&batch_length.to_be_bytes());
    batch.extend_from_slice(&partition_leader_epoch.to_be_bytes());
    batch.extend_from_slice(&crc_buf);

    batch
}

/// 验证磁盘上一个 RecordBatch 的 CRC32C
///
/// `batch_bytes` 为从 base_offset 开始的完整 batch 字节
pub fn verify_batch_crc(batch_bytes: &[u8]) -> bool {
    if batch_bytes.len() < HEADER_SIZE {
        return false;
    }

    // 验证 magic
    if batch_bytes[16] != RECORDBATCH_MAGIC as u8 {
        return false;
    }

    // 读取存储的 CRC (offset 17..21, 即 magic(1) 之后的 4 字节)
    let stored_crc = i32::from_be_bytes([
        batch_bytes[17], batch_bytes[18], batch_bytes[19], batch_bytes[20],
    ]);

    // 计算 CRC (从 magic 开始 = offset 16)，但 CRC 字段本身置零
    let mut compute_buf = batch_bytes[CRC_OFFSET..].to_vec();
    compute_buf[1] = 0; // zero out CRC field
    compute_buf[2] = 0;
    compute_buf[3] = 0;
    compute_buf[4] = 0;
    let computed = crc32c::crc32c(&compute_buf);

    computed == stored_crc as u32
}

/// 从文件偏移顺序扫描，读取所有完整 RecordBatch
///
/// 用于 Crash Recovery 和索引重建。
/// 返回 Vec<(文件偏移, batch_bytes)>
/// 遇到 CRC 错误或不完整数据时停止扫描。
pub fn scan_batches_from_file(
    file: &mut File,
    start_offset: u64,
    file_size: u64,
) -> Vec<(u64, Vec<u8>)> {
    let mut results = Vec::new();
    let mut pos = start_offset;

    while pos < file_size {
        // 尝试读取头部
        if file.seek(SeekFrom::Start(pos)).is_err() {
            break;
        }

        let mut header_buf = vec![0u8; HEADER_SIZE];
        if file.read_exact(&mut header_buf).is_err() {
            break; // 不完整头部
        }

        // 读取 batch_length
        let batch_length = i32::from_be_bytes([
            header_buf[8], header_buf[9], header_buf[10], header_buf[11],
        ]);

        if batch_length < 49 {
            // batch_length = partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2)
            //   + last_offset_delta(4) + base_timestamp(8) + max_timestamp(8)
            //   + producer_id(8) + producer_epoch(2) + base_sequence(4) + records_count(4)
            //   + records
            // 最小值 = 49 (无 records)
            break;
        }

        let total_size = 8 + 4 + batch_length as u64;
        if pos + total_size > file_size {
            break; // 不完整 batch
        }

        // 读取完整 batch
        let mut batch_bytes = header_buf;
        if total_size > HEADER_SIZE as u64 {
            let remaining = (total_size - HEADER_SIZE as u64) as usize;
            let mut rest = vec![0u8; remaining];
            if file.read_exact(&mut rest).is_err() {
                break;
            }
            batch_bytes.extend_from_slice(&rest);
        }

        // 验证 CRC
        if !verify_batch_crc(&batch_bytes) {
            break; // CRC 错误，停止扫描
        }

        results.push((pos, batch_bytes));
        pos += total_size;
    }

    results
}

/// 将 base_offset 格式化为 Segment 文件名前缀 (20 位零填充)
pub fn format_segment_base_name(base_offset: u64) -> String {
    format!("{:020}", base_offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_format_segment_base_name() {
        assert_eq!(format_segment_base_name(0), "00000000000000000000");
        assert_eq!(format_segment_base_name(12345), "00000000000000012345");
    }

    #[test]
    fn test_build_and_verify_batch() {
        // 构建一个空 records 的 batch
        let batch = build_batch_bytes(
            0,    // base_offset
            1,    // partition_leader_epoch
            0,    // attributes (no compression)
            1000, // base_timestamp
            2000, // max_timestamp
            -1,   // producer_id
            -1,   // producer_epoch
            -1,   // base_sequence
            &[],  // records_bytes (empty)
            0,    // record_count
        );

        // 验证大小
        assert_eq!(batch.len(), HEADER_SIZE); // 无 records 时 = 61

        // 验证 CRC
        assert!(verify_batch_crc(&batch));

        // 篡改一个字节后 CRC 应失败
        let mut corrupted = batch.clone();
        corrupted[30] ^= 0xFF;
        assert!(!verify_batch_crc(&corrupted));
    }

    #[test]
    fn test_build_batch_with_records() {
        // 模拟一些 record 字节
        let records = vec![0u8; 100]; // 100 bytes of fake records
        let batch = build_batch_bytes(
            0, 1, 0, 1000, 2000, -1, -1, -1, &records, 5,
        );

        // 大小 = 61 + 100 = 161
        assert_eq!(batch.len(), HEADER_SIZE + 100);
        assert!(verify_batch_crc(&batch));
    }

    #[test]
    fn test_write_and_read_batch_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");

        let records = vec![0xABu8; 50];
        let batch = build_batch_bytes(0, 1, 0, 1000, 2000, -1, -1, -1, &records, 3);

        // 写入
        let mut file = File::create(&path).unwrap();
        let write_pos = write_batch_to_file(&mut file, &batch).unwrap();
        assert_eq!(write_pos, 0);

        // 读取
        let mut file = File::open(&path).unwrap();
        let (read_batch, total_size) = read_batch_from_file(&mut file, 0).unwrap();
        assert_eq!(read_batch, batch);
        assert_eq!(total_size, batch.len() as u64);

        // 仅读头部
        let mut file = File::open(&path).unwrap();
        let hdr = read_batch_header_from_file(&mut file, 0).unwrap();
        assert_eq!(hdr.base_offset, 0);
        assert_eq!(hdr.partition_leader_epoch, 1);
        assert_eq!(hdr.magic, RECORDBATCH_MAGIC);
        assert_eq!(hdr.records_count, 3);
    }

    #[test]
    fn test_scan_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut file = File::create(&path).unwrap();

        // 写入 3 个 batch
        let batch1 = build_batch_bytes(0, 1, 0, 1000, 2000, -1, -1, -1, &[0u8; 20], 2);
        let batch2 = build_batch_bytes(2, 1, 0, 3000, 4000, -1, -1, -1, &[0u8; 30], 3);
        let batch3 = build_batch_bytes(5, 1, 0, 5000, 6000, -1, -1, -1, &[0u8; 10], 1);

        file.write_all(&batch1).unwrap();
        file.write_all(&batch2).unwrap();
        file.write_all(&batch3).unwrap();
        file.flush().unwrap();

        let file_size = file.metadata().unwrap().len();

        // 扫描全部
        let mut file = File::open(&path).unwrap();
        let batches = scan_batches_from_file(&mut file, 0, file_size);
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].0, 0);
        assert_eq!(batches[1].0, batch1.len() as u64);
        assert_eq!(batches[2].0, (batch1.len() + batch2.len()) as u64);
    }

    #[test]
    fn test_scan_batches_stops_at_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut file = File::create(&path).unwrap();

        let batch1 = build_batch_bytes(0, 1, 0, 1000, 2000, -1, -1, -1, &[0u8; 20], 2);
        file.write_all(&batch1).unwrap();

        // 写入损坏数据
        file.write_all(&[0xFFu8; 100]).unwrap();
        file.flush().unwrap();

        let file_size = file.metadata().unwrap().len();
        let mut file = File::open(&path).unwrap();
        let batches = scan_batches_from_file(&mut file, 0, file_size);

        // 应该只读到 1 个有效 batch
        assert_eq!(batches.len(), 1);
    }
}
