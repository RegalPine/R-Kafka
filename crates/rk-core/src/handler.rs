//! RequestHandler trait
//!
//! 定义网络层与业务层之间的解耦接口。
//! 放置在 rk-core 中，使 rk-network 和 rk-broker 均可引用而不产生循环依赖。

use crate::error::Result;

/// 请求处理器 trait
///
/// 网络层通过此接口分发请求帧，由具体实现（如 BrokerRouter）完成处理。
/// 实现者负责解析帧、路由到对应 Handler、编码响应后返回。
pub trait RequestHandler: Send + Sync + 'static {
    /// 处理原始请求帧（含 RequestHeader），返回完整响应帧（含 ResponseHeader）
    ///
    /// `frame` 为 4 字节长度前缀之后的完整帧数据（RequestHeader + RequestBody）。
    fn handle_frame(&self, frame: &[u8]) -> Result<Vec<u8>>;

    /// 是否启用 SASL 认证
    fn is_sasl_enabled(&self) -> bool;
}
