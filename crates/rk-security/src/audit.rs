//! Audit Log — 审计日志
//!
//! 记录安全相关事件:
//! - 认证事件 (登录成功/失败)
//! - 授权事件 (ACL 允许/拒绝)
//! - 配置变更 (ACL 规则修改)
//! - 管理操作 (Topic 创建/删除)
//!
//! ```text
//! 审计日志架构:
//!
//! Event Source ──→ AuditLogger ──→ AuditSink
//!                                       │
//!                              ┌────────┼────────┐
//!                              ▼        ▼        ▼
//!                          Memory    File    (Remote)
//!                          Buffer
//! ```

use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::info;

// ─── 审计事件类型 ───────────────────────────────────────────────────

/// 审计事件类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEventType {
    /// 认证成功
    AuthSuccess,
    /// 认证失败
    AuthFailure,
    /// SASL 认证
    SaslAuth,
    /// TLS 握手
    TlsHandshake,
    /// ACL 允许
    AclAllow,
    /// ACL 拒绝
    AclDeny,
    /// ACL 规则变更
    AclChange,
    /// Topic 创建
    TopicCreate,
    /// Topic 删除
    TopicDelete,
    /// 配置变更
    ConfigChange,
    /// Broker 注册
    BrokerRegister,
    /// Broker 注销
    BrokerUnregister,
    /// 自定义事件
    Custom(String),
}

impl std::fmt::Display for AuditEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditEventType::AuthSuccess => write!(f, "AUTH_SUCCESS"),
            AuditEventType::AuthFailure => write!(f, "AUTH_FAILURE"),
            AuditEventType::SaslAuth => write!(f, "SASL_AUTH"),
            AuditEventType::TlsHandshake => write!(f, "TLS_HANDSHAKE"),
            AuditEventType::AclAllow => write!(f, "ACL_ALLOW"),
            AuditEventType::AclDeny => write!(f, "ACL_DENY"),
            AuditEventType::AclChange => write!(f, "ACL_CHANGE"),
            AuditEventType::TopicCreate => write!(f, "TOPIC_CREATE"),
            AuditEventType::TopicDelete => write!(f, "TOPIC_DELETE"),
            AuditEventType::ConfigChange => write!(f, "CONFIG_CHANGE"),
            AuditEventType::BrokerRegister => write!(f, "BROKER_REGISTER"),
            AuditEventType::BrokerUnregister => write!(f, "BROKER_UNREGISTER"),
            AuditEventType::Custom(s) => write!(f, "CUSTOM({})", s),
        }
    }
}

// ─── 审计事件 ───────────────────────────────────────────────────────

/// 审计事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// 事件类型
    pub event_type: AuditEventType,
    /// 时间戳
    pub timestamp: DateTime<Utc>,
    /// 主体 (用户名/Principal)
    pub principal: Option<String>,
    /// 来源 IP
    pub source_ip: Option<String>,
    /// 资源 (Topic 名称等)
    pub resource: Option<String>,
    /// 操作描述
    pub operation: Option<String>,
    /// 是否成功
    pub success: bool,
    /// 详细信息
    pub details: Option<String>,
    /// 事件 ID (递增)
    pub event_id: u64,
}

impl AuditEvent {
    /// 创建新审计事件
    pub fn new(event_type: AuditEventType) -> Self {
        Self {
            event_type,
            timestamp: Utc::now(),
            principal: None,
            source_ip: None,
            resource: None,
            operation: None,
            success: true,
            details: None,
            event_id: 0,
        }
    }

    /// 设置主体
    pub fn with_principal(mut self, principal: &str) -> Self {
        self.principal = Some(principal.to_string());
        self
    }

    /// 设置来源 IP
    pub fn with_source_ip(mut self, ip: &str) -> Self {
        self.source_ip = Some(ip.to_string());
        self
    }

    /// 设置资源
    pub fn with_resource(mut self, resource: &str) -> Self {
        self.resource = Some(resource.to_string());
        self
    }

    /// 设置操作
    pub fn with_operation(mut self, operation: &str) -> Self {
        self.operation = Some(operation.to_string());
        self
    }

    /// 设置成功状态
    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }

    /// 设置详细信息
    pub fn with_details(mut self, details: &str) -> Self {
        self.details = Some(details.to_string());
        self
    }

    /// 转换为 JSON 字符串
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// 格式化为可读字符串
    pub fn format_log_line(&self) -> String {
        format!(
            "[{}] {} principal={} source={} resource={} op={} success={} details={}",
            self.timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            self.event_type,
            self.principal.as_deref().unwrap_or("-"),
            self.source_ip.as_deref().unwrap_or("-"),
            self.resource.as_deref().unwrap_or("-"),
            self.operation.as_deref().unwrap_or("-"),
            self.success,
            self.details.as_deref().unwrap_or("-"),
        )
    }
}

impl std::fmt::Display for AuditEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_log_line())
    }
}

// ─── 审计日志配置 ───────────────────────────────────────────────────

/// 审计日志配置
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// 是否启用审计
    pub enabled: bool,
    /// 内存缓冲区大小
    pub buffer_size: usize,
    /// 是否记录 ACL 允许事件 (可能很频繁)
    pub log_acl_allow: bool,
    /// 是否记录 ACL 拒绝事件
    pub log_acl_deny: bool,
    /// 是否记录认证事件
    pub log_auth: bool,
    /// 是否记录管理操作
    pub log_admin: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            buffer_size: 10_000,
            log_acl_allow: false,
            log_acl_deny: true,
            log_auth: true,
            log_admin: true,
        }
    }
}

impl AuditConfig {
    /// 创建启用审计的配置
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// 是否应该记录指定类型的事件
    pub fn should_log(&self, event_type: &AuditEventType) -> bool {
        if !self.enabled {
            return false;
        }
        match event_type {
            AuditEventType::AclAllow => self.log_acl_allow,
            AuditEventType::AclDeny => self.log_acl_deny,
            AuditEventType::AuthSuccess
            | AuditEventType::AuthFailure
            | AuditEventType::SaslAuth
            | AuditEventType::TlsHandshake => self.log_auth,
            AuditEventType::TopicCreate
            | AuditEventType::TopicDelete
            | AuditEventType::ConfigChange
            | AuditEventType::BrokerRegister
            | AuditEventType::BrokerUnregister
            | AuditEventType::AclChange => self.log_admin,
            AuditEventType::Custom(_) => true,
        }
    }
}

// ─── 审计日志器 ─────────────────────────────────────────────────────

/// 审计日志器
pub struct AuditLogger {
    /// 配置
    config: AuditConfig,
    /// 内存缓冲区
    buffer: VecDeque<AuditEvent>,
    /// 事件计数器
    event_counter: u64,
    /// 事件类型统计
    type_counts: DashMap<String, u64>,
}

impl AuditLogger {
    /// 创建新的审计日志器
    pub fn new(config: AuditConfig) -> Self {
        Self {
            config,
            buffer: VecDeque::new(),
            event_counter: 0,
            type_counts: DashMap::new(),
        }
    }

    /// 记录审计事件
    pub fn log(&mut self, mut event: AuditEvent) {
        if !self.config.should_log(&event.event_type) {
            return;
        }

        self.event_counter += 1;
        event.event_id = self.event_counter;

        // 更新统计
        let type_key = event.event_type.to_string();
        *self.type_counts.entry(type_key).or_insert(0) += 1;

        // 输出到 tracing
        info!(
            event_type = %event.event_type,
            principal = event.principal.as_deref(),
            source_ip = event.source_ip.as_deref(),
            resource = event.resource.as_deref(),
            success = event.success,
            "Audit event"
        );

        // 存入缓冲区
        if self.buffer.len() >= self.config.buffer_size {
            self.buffer.pop_front(); // 淘汰最旧
        }
        self.buffer.push_back(event);
    }

    /// 记录认证成功
    pub fn log_auth_success(&mut self, principal: &str, source_ip: &str, mechanism: &str) {
        self.log(
            AuditEvent::new(AuditEventType::AuthSuccess)
                .with_principal(principal)
                .with_source_ip(source_ip)
                .with_details(mechanism),
        );
    }

    /// 记录认证失败
    pub fn log_auth_failure(&mut self, principal: &str, source_ip: &str, reason: &str) {
        self.log(
            AuditEvent::new(AuditEventType::AuthFailure)
                .with_principal(principal)
                .with_source_ip(source_ip)
                .with_success(false)
                .with_details(reason),
        );
    }

    /// 记录 ACL 拒绝
    pub fn log_acl_deny(
        &mut self,
        principal: &str,
        source_ip: &str,
        resource: &str,
        operation: &str,
    ) {
        self.log(
            AuditEvent::new(AuditEventType::AclDeny)
                .with_principal(principal)
                .with_source_ip(source_ip)
                .with_resource(resource)
                .with_operation(operation)
                .with_success(false),
        );
    }

    /// 记录管理操作
    pub fn log_admin_action(
        &mut self,
        principal: &str,
        event_type: AuditEventType,
        resource: &str,
        details: &str,
    ) {
        self.log(
            AuditEvent::new(event_type)
                .with_principal(principal)
                .with_resource(resource)
                .with_details(details),
        );
    }

    /// 获取最近 N 条事件
    pub fn recent_events(&self, count: usize) -> Vec<&AuditEvent> {
        self.buffer
            .iter()
            .rev()
            .take(count)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// 获取指定类型的事件
    pub fn events_by_type(&self, event_type: &AuditEventType) -> Vec<&AuditEvent> {
        self.buffer
            .iter()
            .filter(|e| e.event_type == *event_type)
            .collect()
    }

    /// 获取事件总数
    pub fn total_events(&self) -> u64 {
        self.event_counter
    }

    /// 获取缓冲区中的事件数
    pub fn buffered_events(&self) -> usize {
        self.buffer.len()
    }

    /// 获取事件类型统计
    pub fn type_statistics(&self) -> Vec<(String, u64)> {
        self.type_counts
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// 清空缓冲区
    pub fn flush(&mut self) -> Vec<AuditEvent> {
        self.buffer.drain(..).collect()
    }

    /// 是否启用
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// 获取配置
    pub fn config(&self) -> &AuditConfig {
        &self.config
    }
}

impl std::fmt::Debug for AuditLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogger")
            .field("enabled", &self.config.enabled)
            .field("total_events", &self.event_counter)
            .field("buffered", &self.buffer.len())
            .finish()
    }
}

// ─── 测试 ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_type_display() {
        assert_eq!(AuditEventType::AuthSuccess.to_string(), "AUTH_SUCCESS");
        assert_eq!(AuditEventType::AclDeny.to_string(), "ACL_DENY");
        assert_eq!(AuditEventType::TopicCreate.to_string(), "TOPIC_CREATE");
        assert_eq!(
            AuditEventType::Custom("test".to_string()).to_string(),
            "CUSTOM(test)"
        );
    }

    #[test]
    fn test_audit_event_builder() {
        let event = AuditEvent::new(AuditEventType::AuthSuccess)
            .with_principal("User:alice")
            .with_source_ip("10.0.0.1")
            .with_resource("test-topic")
            .with_operation("Read")
            .with_success(true)
            .with_details("SASL/PLAIN");

        assert_eq!(event.principal.as_deref(), Some("User:alice"));
        assert_eq!(event.source_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(event.resource.as_deref(), Some("test-topic"));
        assert_eq!(event.operation.as_deref(), Some("Read"));
        assert!(event.success);
        assert_eq!(event.details.as_deref(), Some("SASL/PLAIN"));
    }

    #[test]
    fn test_audit_event_to_json() {
        let event = AuditEvent::new(AuditEventType::AuthSuccess)
            .with_principal("User:alice");
        let json = event.to_json();
        // serde serializes enum variant as "AuthSuccess"
        assert!(json.contains("AuthSuccess"));
        assert!(json.contains("alice"));
    }

    #[test]
    fn test_audit_event_format_log_line() {
        let event = AuditEvent::new(AuditEventType::AclDeny)
            .with_principal("User:bob")
            .with_source_ip("10.0.0.2")
            .with_resource("secret-topic")
            .with_operation("Write")
            .with_success(false);
        let line = event.format_log_line();
        assert!(line.contains("ACL_DENY"));
        assert!(line.contains("User:bob"));
        assert!(line.contains("secret-topic"));
    }

    #[test]
    fn test_audit_config_default() {
        let config = AuditConfig::default();
        assert!(!config.enabled);
        assert!(config.log_acl_deny);
        assert!(!config.log_acl_allow);
        assert!(config.log_auth);
    }

    #[test]
    fn test_audit_config_should_log() {
        let config = AuditConfig::enabled();
        assert!(config.should_log(&AuditEventType::AuthSuccess));
        assert!(config.should_log(&AuditEventType::AclDeny));
        assert!(!config.should_log(&AuditEventType::AclAllow)); // default off
        assert!(config.should_log(&AuditEventType::TopicCreate));
    }

    #[test]
    fn test_audit_config_disabled() {
        let config = AuditConfig::default(); // disabled
        assert!(!config.should_log(&AuditEventType::AuthSuccess));
        assert!(!config.should_log(&AuditEventType::AclDeny));
    }

    #[test]
    fn test_audit_logger_basic() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        logger.log_auth_success("User:alice", "10.0.0.1", "PLAIN");
        assert_eq!(logger.total_events(), 1);
        assert_eq!(logger.buffered_events(), 1);
    }

    #[test]
    fn test_audit_logger_disabled() {
        let config = AuditConfig::default(); // disabled
        let mut logger = AuditLogger::new(config);

        logger.log_auth_success("User:alice", "10.0.0.1", "PLAIN");
        assert_eq!(logger.total_events(), 0);
        assert_eq!(logger.buffered_events(), 0);
    }

    #[test]
    fn test_audit_logger_auth_failure() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        logger.log_auth_failure("User:bob", "10.0.0.2", "bad password");
        assert_eq!(logger.total_events(), 1);

        let events = logger.events_by_type(&AuditEventType::AuthFailure);
        assert_eq!(events.len(), 1);
        assert!(!events[0].success);
    }

    #[test]
    fn test_audit_logger_acl_deny() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        logger.log_acl_deny("User:bob", "10.0.0.2", "secret", "Write");
        assert_eq!(logger.total_events(), 1);

        let events = logger.events_by_type(&AuditEventType::AclDeny);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_audit_logger_admin_action() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        logger.log_admin_action(
            "User:admin",
            AuditEventType::TopicCreate,
            "new-topic",
            "partitions=6, replication=3",
        );
        assert_eq!(logger.total_events(), 1);
    }

    #[test]
    fn test_audit_logger_recent_events() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        for i in 0..10 {
            logger.log(
                AuditEvent::new(AuditEventType::AuthSuccess)
                    .with_principal(&format!("User:user{}", i)),
            );
        }

        let recent = logger.recent_events(5);
        assert_eq!(recent.len(), 5);
        // 最近的 5 条应该是 user5..user9
        assert_eq!(recent[0].principal.as_deref(), Some("User:user5"));
        assert_eq!(recent[4].principal.as_deref(), Some("User:user9"));
    }

    #[test]
    fn test_audit_logger_buffer_overflow() {
        let config = AuditConfig {
            buffer_size: 5,
            ..AuditConfig::enabled()
        };
        let mut logger = AuditLogger::new(config);

        for i in 0..10 {
            logger.log(
                AuditEvent::new(AuditEventType::AuthSuccess)
                    .with_principal(&format!("User:user{}", i)),
            );
        }

        // 缓冲区只有 5 条 (最旧的被淘汰)
        assert_eq!(logger.buffered_events(), 5);
        // 但总数是 10
        assert_eq!(logger.total_events(), 10);
    }

    #[test]
    fn test_audit_logger_flush() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        logger.log_auth_success("User:alice", "10.0.0.1", "PLAIN");
        logger.log_auth_success("User:bob", "10.0.0.2", "SCRAM");

        let flushed = logger.flush();
        assert_eq!(flushed.len(), 2);
        assert_eq!(logger.buffered_events(), 0);
        assert_eq!(logger.total_events(), 2); // 总数不清零
    }

    #[test]
    fn test_audit_logger_type_statistics() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        logger.log_auth_success("User:alice", "10.0.0.1", "PLAIN");
        logger.log_auth_success("User:bob", "10.0.0.2", "SCRAM");
        logger.log_auth_failure("User:charlie", "10.0.0.3", "bad password");

        let stats = logger.type_statistics();
        assert_eq!(stats.len(), 2); // AUTH_SUCCESS + AUTH_FAILURE
    }

    #[test]
    fn test_audit_logger_event_id_increment() {
        let config = AuditConfig::enabled();
        let mut logger = AuditLogger::new(config);

        logger.log_auth_success("User:alice", "10.0.0.1", "PLAIN");
        logger.log_auth_success("User:bob", "10.0.0.2", "SCRAM");

        let events = logger.recent_events(2);
        assert_eq!(events[0].event_id, 1);
        assert_eq!(events[1].event_id, 2);
    }

    #[test]
    fn test_audit_logger_is_enabled() {
        let config = AuditConfig::enabled();
        let logger = AuditLogger::new(config);
        assert!(logger.is_enabled());

        let config2 = AuditConfig::default();
        let logger2 = AuditLogger::new(config2);
        assert!(!logger2.is_enabled());
    }

    #[test]
    fn test_audit_event_display() {
        let event = AuditEvent::new(AuditEventType::AuthSuccess)
            .with_principal("User:alice");
        let display = format!("{}", event);
        assert!(display.contains("AUTH_SUCCESS"));
        assert!(display.contains("User:alice"));
    }

    #[test]
    fn test_audit_config_custom_event_always_logged() {
        let config = AuditConfig::enabled();
        assert!(config.should_log(&AuditEventType::Custom("test".to_string())));
    }
}
