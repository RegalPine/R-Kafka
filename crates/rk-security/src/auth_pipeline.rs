//! Auth Pipeline — 统一认证授权管道
//!
//! 将 SASL 认证 + ACL 授权 + 审计日志组合为统一的安全管道:
//!
//! ```text
//! 请求进入 ──→ AuthPipeline
//!                │
//!                ├── 1. 认证检查 (SASL session)
//!                │      ├── 未认证? → 只允许 ApiVersions/SaslHandshake/SaslAuthenticate
//!                │      └── 已认证 → 提取 Principal
//!                │
//!                ├── 2. 授权检查 (ACL engine)
//!                │      ├── 查找 API 对应的 (Resource, Operation)
//!                │      ├── Deny → 拒绝 + 审计
//!                │      └── Allow → 继续
//!                │
//!                └── 3. 审计记录
//!                       └── 记录认证/授权事件
//! ```

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

use rk_core::error::Result;

use crate::acl::{AclEngine, AclOperation, ResourceType};
use crate::audit::{AuditEvent, AuditEventType, AuditLogger};
use crate::sasl::{AuthState, SaslMechanism, SaslSession, UserDatabase};

// ─── 连接安全上下文 ─────────────────────────────────────────────────

/// 连接级安全上下文
///
/// 每个客户端连接维护一个 SecurityContext，跟踪认证状态和授权信息。
#[derive(Debug)]
pub struct SecurityContext {
    /// SASL 认证会话
    session: SaslSession,
    /// 已认证主体 (User:xxx)
    principal: Option<String>,
    /// 客户端主机
    client_host: String,
    /// 是否使用 TLS
    tls_enabled: bool,
    /// 认证时间戳 (ms)
    auth_timestamp_ms: Option<u64>,
}

impl SecurityContext {
    /// 创建新的安全上下文
    pub fn new(mechanism: SaslMechanism, client_host: &str, tls_enabled: bool) -> Self {
        Self {
            session: SaslSession::new(mechanism),
            principal: None,
            client_host: client_host.to_string(),
            tls_enabled,
            auth_timestamp_ms: None,
        }
    }

    /// 获取当前认证状态
    pub fn auth_state(&self) -> &AuthState {
        self.session.state()
    }

    /// 是否已认证
    pub fn is_authenticated(&self) -> bool {
        *self.session.state() == AuthState::Authenticated
    }

    /// 获取已认证主体
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// 获取客户端主机
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// 是否使用 TLS
    pub fn is_tls(&self) -> bool {
        self.tls_enabled
    }

    /// 获取 SASL 会话引用
    pub fn session(&self) -> &SaslSession {
        &self.session
    }

    /// 获取 SASL 会话可变引用
    pub fn session_mut(&mut self) -> &mut SaslSession {
        &mut self.session
    }

    /// 认证成功后设置主体
    fn set_authenticated(&mut self, principal: String) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.principal = Some(principal);
        self.auth_timestamp_ms = Some(ts);
    }
}

// ─── 认证授权管道 ───────────────────────────────────────────────────

/// 安全管道配置
#[derive(Debug, Clone)]
pub struct SecurityPipelineConfig {
    /// 是否启用 SASL 认证
    pub sasl_enabled: bool,
    /// 是否启用 ACL 授权
    pub acl_enabled: bool,
    /// 是否启用审计日志
    pub audit_enabled: bool,
    /// 默认 SASL 机制
    pub default_mechanism: SaslMechanism,
    /// 超级用户列表 (跳过 ACL 检查)
    pub super_users: Vec<String>,
}

impl Default for SecurityPipelineConfig {
    fn default() -> Self {
        Self {
            sasl_enabled: false,
            acl_enabled: false,
            audit_enabled: true,
            default_mechanism: SaslMechanism::Plain,
            super_users: vec![],
        }
    }
}

/// 授权检查结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzResult {
    /// 允许
    Allowed,
    /// 拒绝 (含原因)
    Denied(String),
    /// 跳过 (ACL 未启用或超级用户)
    Skipped,
}

/// 安全管道 — 统一管理认证 + 授权 + 审计
pub struct SecurityPipeline {
    config: SecurityPipelineConfig,
    user_db: Arc<UserDatabase>,
    acl_engine: Arc<AclEngine>,
    audit_logger: Arc<Mutex<AuditLogger>>,
}

impl SecurityPipeline {
    /// 创建新的安全管道
    pub fn new(
        config: SecurityPipelineConfig,
        user_db: Arc<UserDatabase>,
        acl_engine: Arc<AclEngine>,
        audit_logger: Arc<Mutex<AuditLogger>>,
    ) -> Self {
        info!(
            sasl_enabled = config.sasl_enabled,
            acl_enabled = config.acl_enabled,
            audit_enabled = config.audit_enabled,
            super_users = config.super_users.len(),
            "Security pipeline initialized"
        );
        Self {
            config,
            user_db,
            acl_engine,
            audit_logger,
        }
    }

    /// 获取用户数据库引用
    pub fn user_db(&self) -> &Arc<UserDatabase> {
        &self.user_db
    }

    /// 获取 ACL 引擎引用
    pub fn acl_engine(&self) -> &Arc<AclEngine> {
        &self.acl_engine
    }

    /// 获取审计日志引用
    pub fn audit_logger(&self) -> &Arc<Mutex<AuditLogger>> {
        &self.audit_logger
    }

    /// 获取配置引用
    pub fn config(&self) -> &SecurityPipelineConfig {
        &self.config
    }

    // ─── 认证 ───────────────────────────────────────────────────

    /// 处理 SASL PLAIN 认证
    pub fn authenticate_plain(
        &self,
        ctx: &mut SecurityContext,
        data: &[u8],
    ) -> Result<Vec<u8>> {
        let result = ctx.session_mut().authenticate_plain(data, &self.user_db);

        match &result {
            Ok(_) => {
                if let Some(user) = ctx.session().authenticated_user() {
                    let principal = format!("User:{}", user);
                    ctx.set_authenticated(principal.clone());

                    // 审计: 认证成功
                    if self.config.audit_enabled {
                        if let Ok(mut logger) = self.audit_logger.lock() {
                            logger.log(
                                AuditEvent::new(AuditEventType::AuthSuccess)
                                    .with_principal(&principal)
                                    .with_source_ip(ctx.client_host())
                                    .with_details("mechanism=PLAIN"),
                            );
                        }
                    }
                }
            }
            Err(e) => {
                // 审计: 认证失败
                if self.config.audit_enabled {
                    if let Ok(mut logger) = self.audit_logger.lock() {
                        logger.log(
                            AuditEvent::new(AuditEventType::AuthFailure)
                                .with_source_ip(ctx.client_host())
                                .with_details(&format!("mechanism=PLAIN reason={}", e)),
                        );
                    }
                }
            }
        }

        result
    }

    /// 处理 SCRAM client-first
    pub fn scram_client_first(
        &self,
        ctx: &mut SecurityContext,
        data: &[u8],
    ) -> Result<Vec<u8>> {
        let mechanism = format!("{}", ctx.session().mechanism());
        let result = ctx.session_mut().scram_client_first(data, &self.user_db);

        if result.is_err() && self.config.audit_enabled {
            if let Ok(mut logger) = self.audit_logger.lock() {
                logger.log(
                    AuditEvent::new(AuditEventType::AuthFailure)
                        .with_source_ip(ctx.client_host())
                        .with_details(&format!("mechanism={} phase=client-first", mechanism)),
                );
            }
        }

        result
    }

    /// 处理 SCRAM client-final
    pub fn scram_client_final(
        &self,
        ctx: &mut SecurityContext,
        data: &[u8],
    ) -> Result<Vec<u8>> {
        let mechanism = format!("{}", ctx.session().mechanism());
        let result = ctx.session_mut().scram_client_final(data, &self.user_db);

        match &result {
            Ok(_) => {
                if let Some(user) = ctx.session().authenticated_user() {
                    let principal = format!("User:{}", user);
                    ctx.set_authenticated(principal.clone());

                    if self.config.audit_enabled {
                        if let Ok(mut logger) = self.audit_logger.lock() {
                            logger.log(
                                AuditEvent::new(AuditEventType::AuthSuccess)
                                    .with_principal(&principal)
                                    .with_source_ip(ctx.client_host())
                                    .with_details(&format!("mechanism={}", mechanism)),
                            );
                        }
                    }
                }
            }
            Err(e) => {
                if self.config.audit_enabled {
                    if let Ok(mut logger) = self.audit_logger.lock() {
                        logger.log(
                            AuditEvent::new(AuditEventType::AuthFailure)
                                .with_source_ip(ctx.client_host())
                                .with_details(&format!("mechanism={} reason={}", mechanism, e)),
                        );
                    }
                }
            }
        }

        result
    }

    // ─── 授权 ───────────────────────────────────────────────────

    /// 检查 API 授权
    ///
    /// 根据 principal + API key + 资源名称检查 ACL。
    pub fn authorize(
        &self,
        ctx: &SecurityContext,
        resource_type: &ResourceType,
        resource_name: &str,
        operation: &AclOperation,
    ) -> AuthzResult {
        // ACL 未启用 → 跳过
        if !self.config.acl_enabled || !self.acl_engine.is_enabled() {
            return AuthzResult::Skipped;
        }

        // 未认证 → 拒绝
        let principal = match ctx.principal() {
            Some(p) => p,
            None => return AuthzResult::Denied("Not authenticated".to_string()),
        };

        // 超级用户 → 跳过
        if self.config.super_users.iter().any(|u| u == principal) {
            return AuthzResult::Skipped;
        }

        // ACL 检查
        if self.acl_engine.authorize(
            principal,
            ctx.client_host(),
            resource_type,
            resource_name,
            operation,
        ) {
            // 审计: 授权允许
            if self.config.audit_enabled {
                if let Ok(mut logger) = self.audit_logger.lock() {
                    logger.log(
                        AuditEvent::new(AuditEventType::AclAllow)
                            .with_principal(principal)
                            .with_source_ip(ctx.client_host())
                            .with_resource(resource_name)
                            .with_operation(&format!("{}", operation))
                            .with_details(&format!("resource_type={}", resource_type)),
                    );
                }
            }
            AuthzResult::Allowed
        } else {
            // 审计: 授权拒绝
            warn!(
                principal = principal,
                resource = resource_name,
                operation = %operation,
                "ACL DENY"
            );
            if self.config.audit_enabled {
                if let Ok(mut logger) = self.audit_logger.lock() {
                    logger.log(
                        AuditEvent::new(AuditEventType::AclDeny)
                            .with_principal(principal)
                            .with_source_ip(ctx.client_host())
                            .with_resource(resource_name)
                            .with_operation(&format!("{}", operation))
                            .with_details(&format!("resource_type={}", resource_type)),
                    );
                }
            }
            AuthzResult::Denied(format!(
                "ACL denied: {} {} on {} {}",
                principal, operation, resource_type, resource_name
            ))
        }
    }

    /// 检查是否为超级用户
    pub fn is_super_user(&self, principal: &str) -> bool {
        self.config.super_users.iter().any(|u| u == principal)
    }

    // ─── 统计 ───────────────────────────────────────────────────

    /// 获取安全管道摘要
    pub fn summary(&self) -> SecurityPipelineSummary {
        SecurityPipelineSummary {
            sasl_enabled: self.config.sasl_enabled,
            acl_enabled: self.config.acl_enabled,
            audit_enabled: self.config.audit_enabled,
            user_count: self.user_db.user_count(),
            acl_rule_count: self.acl_engine.acl_count(),
            super_user_count: self.config.super_users.len(),
        }
    }
}

/// 安全管道摘要
#[derive(Debug, Clone)]
pub struct SecurityPipelineSummary {
    pub sasl_enabled: bool,
    pub acl_enabled: bool,
    pub audit_enabled: bool,
    pub user_count: usize,
    pub acl_rule_count: usize,
    pub super_user_count: usize,
}

impl std::fmt::Display for SecurityPipelineSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SecurityPipeline(sasl={}, acl={}, audit={}, users={}, rules={}, super_users={})",
            self.sasl_enabled,
            self.acl_enabled,
            self.audit_enabled,
            self.user_count,
            self.acl_rule_count,
            self.super_user_count,
        )
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AclEntry, AclOperation, ResourcePattern, ResourceType};
    use crate::audit::AuditConfig;
    use crate::sasl::SaslMechanism;

    fn make_pipeline() -> SecurityPipeline {
        let config = SecurityPipelineConfig {
            sasl_enabled: true,
            acl_enabled: true,
            audit_enabled: true,
            default_mechanism: SaslMechanism::Plain,
            super_users: vec!["User:admin".to_string()],
        };
        let user_db = Arc::new(UserDatabase::new());
        user_db.add_user("alice", "password123");
        user_db.add_user("bob", "secret456");
        user_db.add_user("admin", "admin_pass");

        let acl_engine = Arc::new(AclEngine::new(true));
        // alice: 读取 topic-a
        acl_engine.add_acl(AclEntry::allow(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Literal("topic-a".to_string()),
            AclOperation::Read,
        ));
        // alice: 写入 topic-a
        acl_engine.add_acl(AclEntry::allow(
            "User:alice",
            ResourceType::Topic,
            ResourcePattern::Literal("topic-a".to_string()),
            AclOperation::Write,
        ));
        // bob: 读取所有 topic
        acl_engine.add_acl(AclEntry::allow(
            "User:bob",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Read,
        ));
        // admin: 所有操作
        acl_engine.add_acl(AclEntry::allow(
            "User:admin",
            ResourceType::Topic,
            ResourcePattern::Any,
            AclOperation::Any,
        ));

        let audit_logger = Arc::new(Mutex::new(AuditLogger::new(AuditConfig::enabled())));

        SecurityPipeline::new(config, user_db, acl_engine, audit_logger)
    }

    fn make_ctx(host: &str) -> SecurityContext {
        SecurityContext::new(SaslMechanism::Plain, host, false)
    }

    #[test]
    fn test_security_context_initial_state() {
        let ctx = make_ctx("10.0.0.1");
        assert!(!ctx.is_authenticated());
        assert!(ctx.principal().is_none());
        assert_eq!(ctx.client_host(), "10.0.0.1");
        assert!(!ctx.is_tls());
    }

    #[test]
    fn test_plain_auth_success() {
        let pipeline = make_pipeline();
        let mut ctx = make_ctx("10.0.0.1");

        // PLAIN: authzid\0username\0password
        let data = b"\0alice\0password123";
        let result = pipeline.authenticate_plain(&mut ctx, data);
        assert!(result.is_ok());
        assert!(ctx.is_authenticated());
        assert_eq!(ctx.principal(), Some("User:alice"));
    }

    #[test]
    fn test_plain_auth_failure() {
        let pipeline = make_pipeline();
        let mut ctx = make_ctx("10.0.0.1");

        let data = b"\0alice\0wrong_password";
        let result = pipeline.authenticate_plain(&mut ctx, data);
        assert!(result.is_err());
        assert!(!ctx.is_authenticated());
    }

    #[test]
    fn test_authorize_allowed() {
        let pipeline = make_pipeline();
        let mut ctx = make_ctx("10.0.0.1");

        // 先认证 alice
        pipeline.authenticate_plain(&mut ctx, b"\0alice\0password123").unwrap();

        // alice 读 topic-a → 允许
        let result = pipeline.authorize(
            &ctx,
            &ResourceType::Topic,
            "topic-a",
            &AclOperation::Read,
        );
        assert_eq!(result, AuthzResult::Allowed);
    }

    #[test]
    fn test_authorize_denied() {
        let pipeline = make_pipeline();
        let mut ctx = make_ctx("10.0.0.1");

        // 认证 alice
        pipeline.authenticate_plain(&mut ctx, b"\0alice\0password123").unwrap();

        // alice 读 topic-b → 无规则 → 默认拒绝
        let result = pipeline.authorize(
            &ctx,
            &ResourceType::Topic,
            "topic-b",
            &AclOperation::Read,
        );
        assert!(matches!(result, AuthzResult::Denied(_)));
    }

    #[test]
    fn test_authorize_super_user() {
        let pipeline = make_pipeline();
        let mut ctx = make_ctx("10.0.0.1");

        // 认证 admin (超级用户)
        pipeline.authenticate_plain(&mut ctx, b"\0admin\0admin_pass").unwrap();

        // admin 任何操作 → 跳过 ACL
        let result = pipeline.authorize(
            &ctx,
            &ResourceType::Topic,
            "any-topic",
            &AclOperation::Delete,
        );
        assert_eq!(result, AuthzResult::Skipped);
    }

    #[test]
    fn test_authorize_unauthenticated() {
        let pipeline = make_pipeline();
        let ctx = make_ctx("10.0.0.1");

        // 未认证 → 拒绝
        let result = pipeline.authorize(
            &ctx,
            &ResourceType::Topic,
            "topic-a",
            &AclOperation::Read,
        );
        assert!(matches!(result, AuthzResult::Denied(_)));
    }

    #[test]
    fn test_bob_read_any_topic() {
        let pipeline = make_pipeline();
        let mut ctx = make_ctx("10.0.0.2");

        // 认证 bob
        pipeline.authenticate_plain(&mut ctx, b"\0bob\0secret456").unwrap();

        // bob 读任何 topic → 允许 (ResourcePattern::Any)
        assert_eq!(
            pipeline.authorize(&ctx, &ResourceType::Topic, "topic-x", &AclOperation::Read),
            AuthzResult::Allowed
        );
        assert_eq!(
            pipeline.authorize(&ctx, &ResourceType::Topic, "topic-y", &AclOperation::Read),
            AuthzResult::Allowed
        );

        // bob 写 → 无规则 → 拒绝
        assert!(matches!(
            pipeline.authorize(&ctx, &ResourceType::Topic, "topic-x", &AclOperation::Write),
            AuthzResult::Denied(_)
        ));
    }

    #[test]
    fn test_audit_log_created() {
        let pipeline = make_pipeline();
        let mut ctx = make_ctx("10.0.0.1");

        // 认证 → 产生审计事件
        pipeline.authenticate_plain(&mut ctx, b"\0alice\0password123").unwrap();

        let logger = pipeline.audit_logger().lock().unwrap();
        let events = logger.recent_events(10);
        assert!(!events.is_empty());
        // 应该有 AuthSuccess 事件
        assert!(events.iter().any(|e| e.event_type == AuditEventType::AuthSuccess));
    }

    #[test]
    fn test_pipeline_summary() {
        let pipeline = make_pipeline();
        let summary = pipeline.summary();
        assert!(summary.sasl_enabled);
        assert!(summary.acl_enabled);
        assert_eq!(summary.user_count, 3);
        assert!(summary.acl_rule_count > 0);
        assert_eq!(summary.super_user_count, 1);

        let display = format!("{}", summary);
        assert!(display.contains("sasl=true"));
        assert!(display.contains("acl=true"));
    }

    #[test]
    fn test_is_super_user() {
        let pipeline = make_pipeline();
        assert!(pipeline.is_super_user("User:admin"));
        assert!(!pipeline.is_super_user("User:alice"));
    }
}
