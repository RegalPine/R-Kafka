//! RequestHandler trait 及辅助工具函数
//!
//! 定义网络层与业务层之间的解耦接口。
//! RequestHandler trait 定义在 rk-core 中以避免循环依赖，此处重新导出。

// 从 rk-core 重新导出 RequestHandler trait
pub use rk_core::RequestHandler;

/// 从原始帧字节提取 api_key（不解析完整请求）
///
/// 帧格式: [api_key(i16)] [api_version(i16)] [correlation_id(i32)] ...
/// 若帧长度不足则返回 None。
pub fn extract_api_key(frame: &[u8]) -> Option<i16> {
    if frame.len() < 2 {
        return None;
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    Some(api_key)
}

/// 检查某个 API 在 SASL 认证前是否允许调用
///
/// 认证前只允许: ApiVersions(18), SaslHandshake(17), SaslAuthenticate(36)
pub fn is_pre_auth_api(api_key: i16) -> bool {
    matches!(api_key, 17 | 18 | 36)
}
