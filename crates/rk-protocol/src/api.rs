//! API Router
//!
//! 根据 Request Header 中的 api_key 分发到对应 Handler。

use rk_core::error::{Result, RkError};
use std::collections::HashMap;
use std::sync::Arc;

/// 请求处理上下文
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub api_key: i16,
    pub api_version: i16,
}

/// 异步请求处理 trait (object-safe)
pub trait RequestHandler: Send + Sync {
    fn handle(&self, ctx: RequestContext, request_body: &[u8]) -> Result<Vec<u8>>;
}

/// API Router: 根据 api_key 分发到对应 Handler
pub struct ApiRouter {
    handlers: HashMap<i16, Arc<dyn RequestHandler>>,
}

impl ApiRouter {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, api_key: i16, handler: Arc<dyn RequestHandler>) {
        self.handlers.insert(api_key, handler);
    }

    pub fn route(&self, ctx: RequestContext, body: &[u8]) -> Result<Vec<u8>> {
        let handler = self
            .handlers
            .get(&ctx.api_key)
            .ok_or(RkError::UnsupportedApiKey(ctx.api_key))?;
        handler.handle(ctx, body)
    }

    pub fn has_handler(&self, api_key: i16) -> bool {
        self.handlers.contains_key(&api_key)
    }
}

impl Default for ApiRouter {
    fn default() -> Self {
        Self::new()
    }
}
