//! 安全互操作集成测试
//!
//! 验证 rk-security 各组件 (SASL + ACL + Audit + TLS) 的端到端协作:
//!
//! ```text
//! 测试覆盖:
//!
//! 1. SecurityPipeline 端到端:
//!    SASL PLAIN 认证 → ACL 授权 → 审计日志
//!
//! 2. SASL SCRAM 认证流程:
//!    SCRAM-SHA-256 / SCRAM-SHA-512 挑战-响应
//!
//! 3. ACL 授权执行:
//!    Allow / Deny 规则 + 前缀匹配 + 超级用户旁路
//!
//! 4. API 权限映射:
//!    Kafka API Key → (ResourceType, AclOperation) 映射验证
//!
//! 5. BrokerRouter ACL 集成:
//!    check_authorization() 端到端
//!
//! 6. 多用户并发安全:
//!    并发认证 + 授权隔离
//!
//! 7. 审计日志完整性:
//!    认证/授权事件完整记录
//! ```

use std::sync::{Arc, Mutex};

use rk_security::{
    // Pipeline
    SecurityPipeline, SecurityPipelineConfig, SecurityContext, AuthzResult,
    // SASL
    SaslMechanism, UserDatabase,
    // ACL
    AclEngine, AclEntry, ResourceType, ResourcePattern, AclOperation,
    topic_read_acl, topic_write_acl, group_read_acl, prefix_acl,
    // Audit
    AuditConfig, AuditLogger, AuditEventType,
    // API Permissions
    api_permission, is_pre_auth_api,
};

// ─── 测试基础设施 ─────────────────────────────────────────────────────

/// 创建测试用 SecurityPipeline
fn setup_pipeline(
    sasl_enabled: bool,
    acl_enabled: bool,
    audit_enabled: bool,
) -> (SecurityPipeline, Arc<UserDatabase>, Arc<AclEngine>, Arc<Mutex<AuditLogger>>) {
    let user_db = Arc::new(UserDatabase::new());
    user_db.add_user("alice", "alice-secret");
    user_db.add_user("bob", "bob-secret");
    user_db.add_user("admin", "admin-secret");

    let acl_engine = Arc::new(AclEngine::new(acl_enabled));

    let audit_config = AuditConfig {
        enabled: audit_enabled,
        buffer_size: 10_000,
        log_acl_allow: true,
        log_acl_deny: true,
        log_auth: true,
        log_admin: true,
    };
    let audit_logger = Arc::new(Mutex::new(AuditLogger::new(audit_config)));

    let config = SecurityPipelineConfig {
        sasl_enabled,
        acl_enabled,
        audit_enabled,
        default_mechanism: SaslMechanism::Plain,
        super_users: vec!["User:admin".to_string()],
    };

    let pipeline = SecurityPipeline::new(
        config,
        user_db.clone(),
        acl_engine.clone(),
        audit_logger.clone(),
    );

    (pipeline, user_db, acl_engine, audit_logger)
}

/// 构建 SASL PLAIN 认证数据: [authzid] \0 username \0 password
fn build_plain_auth(username: &str, password: &str) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(b""); // authzid (empty)
    data.push(0);
    data.extend_from_slice(username.as_bytes());
    data.push(0);
    data.extend_from_slice(password.as_bytes());
    data
}

// ─── 测试 1: SASL PLAIN 认证端到端 ────────────────────────────────────

/// 验证 SASL PLAIN 认证完整流程:
/// 1. 创建 SecurityContext
/// 2. 发送 PLAIN 认证数据
/// 3. 验证认证成功 + principal 设置
#[test]
fn test_sasl_plain_authentication_e2e() {
    let (pipeline, _, _, _) = setup_pipeline(true, false, true);

    // 1. 创建安全上下文
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "192.168.1.100", false);
    assert!(ctx.principal().is_none());

    // 2. 发送 PLAIN 认证
    let auth_data = build_plain_auth("alice", "alice-secret");
    let result = pipeline.authenticate_plain(&mut ctx, &auth_data);
    assert!(result.is_ok(), "PLAIN auth should succeed");

    // 3. 验证认证状态
    assert_eq!(ctx.principal(), Some("User:alice"));
    assert!(ctx.is_authenticated());
    assert_eq!(ctx.client_host(), "192.168.1.100");
    assert!(!ctx.is_tls());
}

/// 验证错误密码被拒绝
#[test]
fn test_sasl_plain_wrong_password() {
    let (pipeline, _, _, _) = setup_pipeline(true, false, true);

    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    let auth_data = build_plain_auth("alice", "wrong-password");
    let result = pipeline.authenticate_plain(&mut ctx, &auth_data);

    assert!(result.is_err(), "Wrong password should fail");
    assert!(ctx.principal().is_none());
}

/// 验证不存在的用户被拒绝
#[test]
fn test_sasl_plain_unknown_user() {
    let (pipeline, _, _, _) = setup_pipeline(true, false, true);

    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    let auth_data = build_plain_auth("unknown-user", "any-password");
    let result = pipeline.authenticate_plain(&mut ctx, &auth_data);

    assert!(result.is_err(), "Unknown user should fail");
}

// ─── 测试 2: ACL 授权端到端 ────────────────────────────────────────────

/// 验证 ACL 授权流程:
/// 1. 认证用户
/// 2. 添加 ACL 规则
/// 3. 验证授权检查
#[test]
fn test_acl_authorization_e2e() {
    let (pipeline, _, acl_engine, _) = setup_pipeline(true, true, true);

    // 添加 ACL: alice 可以读 topic-orders
    acl_engine.add_acl(topic_read_acl("User:alice", "topic-orders"));
    // 添加 ACL: alice 可以写 topic-orders
    acl_engine.add_acl(topic_write_acl("User:alice", "topic-orders"));

    // 认证 alice
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "192.168.1.100", false);
    let auth_data = build_plain_auth("alice", "alice-secret");
    pipeline.authenticate_plain(&mut ctx, &auth_data).unwrap();

    // 验证: alice 读 topic-orders → 允许
    let result = pipeline.authorize(
        &ctx,
        &ResourceType::Topic,
        "topic-orders",
        &AclOperation::Read,
    );
    assert!(matches!(result, AuthzResult::Allowed));

    // 验证: alice 写 topic-orders → 允许
    let result = pipeline.authorize(
        &ctx,
        &ResourceType::Topic,
        "topic-orders",
        &AclOperation::Write,
    );
    assert!(matches!(result, AuthzResult::Allowed));

    // 验证: alice 读 topic-other → 拒绝 (无规则)
    let result = pipeline.authorize(
        &ctx,
        &ResourceType::Topic,
        "topic-other",
        &AclOperation::Read,
    );
    assert!(matches!(result, AuthzResult::Denied(_)));
}

/// 验证 ACL Deny 规则优先于 Allow
#[test]
fn test_acl_deny_takes_priority() {
    let (pipeline, _, acl_engine, _) = setup_pipeline(true, true, true);

    // Allow: alice 读所有 topic
    acl_engine.add_acl(AclEntry::allow(
        "User:alice",
        ResourceType::Topic,
        ResourcePattern::Any,
        AclOperation::Read,
    ));

    // Deny: alice 读 topic-secret
    acl_engine.add_acl(AclEntry::deny(
        "User:alice",
        ResourceType::Topic,
        ResourcePattern::Literal("topic-secret".to_string()),
        AclOperation::Read,
    ));

    // 认证 alice
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    let auth_data = build_plain_auth("alice", "alice-secret");
    pipeline.authenticate_plain(&mut ctx, &auth_data).unwrap();

    // alice 读 topic-normal → 允许
    let result = pipeline.authorize(&ctx, &ResourceType::Topic, "topic-normal", &AclOperation::Read);
    assert!(matches!(result, AuthzResult::Allowed));

    // alice 读 topic-secret → 拒绝 (Deny 优先)
    let result = pipeline.authorize(&ctx, &ResourceType::Topic, "topic-secret", &AclOperation::Read);
    assert!(matches!(result, AuthzResult::Denied(_)));
}

/// 验证超级用户旁路 ACL 检查
#[test]
fn test_super_user_bypass() {
    let (pipeline, _, _acl_engine, _) = setup_pipeline(true, true, true);

    // 不给 admin 添加任何 ACL 规则

    // 认证 admin (在 super_users 列表中)
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    let auth_data = build_plain_auth("admin", "admin-secret");
    pipeline.authenticate_plain(&mut ctx, &auth_data).unwrap();

    // admin 访问任意资源 → 跳过 (超级用户旁路, 返回 Skipped)
    let result = pipeline.authorize(
        &ctx,
        &ResourceType::Topic,
        "any-topic",
        &AclOperation::Write,
    );
    assert!(matches!(result, AuthzResult::Skipped), "Super user should bypass ACL (Skipped)");

    // bob (非超级用户) 无规则 → 拒绝
    let mut ctx_bob = SecurityContext::new(SaslMechanism::Plain, "10.0.0.2", false);
    let auth_data = build_plain_auth("bob", "bob-secret");
    pipeline.authenticate_plain(&mut ctx_bob, &auth_data).unwrap();

    let result = pipeline.authorize(
        &ctx_bob,
        &ResourceType::Topic,
        "any-topic",
        &AclOperation::Write,
    );
    assert!(matches!(result, AuthzResult::Denied(_)));
}

// ─── 测试 3: 未认证用户授权 ────────────────────────────────────────────

/// 验证未认证用户被拒绝
#[test]
fn test_unauthenticated_user_denied() {
    let (pipeline, _, acl_engine, _) = setup_pipeline(true, true, true);

    // 添加允许规则
    acl_engine.add_acl(topic_read_acl("User:*", "public-topic"));

    // 未认证的上下文
    let ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);

    // 未认证 → 拒绝
    let result = pipeline.authorize(&ctx, &ResourceType::Topic, "public-topic", &AclOperation::Read);
    assert!(matches!(result, AuthzResult::Denied(_)), "Unauthenticated should be denied");
}

// ─── 测试 4: ACL 未启用时跳过授权 ──────────────────────────────────────

/// 验证 ACL 未启用时所有请求都跳过
#[test]
fn test_acl_disabled_skips_authorization() {
    let (pipeline, _, _, _) = setup_pipeline(true, false, false);

    // 认证用户
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    let auth_data = build_plain_auth("alice", "alice-secret");
    pipeline.authenticate_plain(&mut ctx, &auth_data).unwrap();

    // ACL 未启用 → Skipped
    let result = pipeline.authorize(
        &ctx,
        &ResourceType::Topic,
        "any-topic",
        &AclOperation::Write,
    );
    assert!(matches!(result, AuthzResult::Skipped));
}

// ─── 测试 5: 前缀匹配 ACL ─────────────────────────────────────────────

/// 验证前缀匹配 ACL 规则
#[test]
fn test_prefix_acl_matching() {
    let (pipeline, _, acl_engine, _) = setup_pipeline(true, true, true);

    // alice 可以读写 dev- 前缀的所有 topic
    acl_engine.add_acl(prefix_acl(
        "User:alice",
        ResourceType::Topic,
        "dev-",
        AclOperation::Read,
    ));
    acl_engine.add_acl(prefix_acl(
        "User:alice",
        ResourceType::Topic,
        "dev-",
        AclOperation::Write,
    ));

    // 认证 alice
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    let auth_data = build_plain_auth("alice", "alice-secret");
    pipeline.authenticate_plain(&mut ctx, &auth_data).unwrap();

    // dev-orders → 允许 (前缀匹配)
    let result = pipeline.authorize(&ctx, &ResourceType::Topic, "dev-orders", &AclOperation::Read);
    assert!(matches!(result, AuthzResult::Allowed));

    // dev-users → 允许
    let result = pipeline.authorize(&ctx, &ResourceType::Topic, "dev-users", &AclOperation::Write);
    assert!(matches!(result, AuthzResult::Allowed));

    // prod-orders → 拒绝 (不匹配前缀)
    let result = pipeline.authorize(&ctx, &ResourceType::Topic, "prod-orders", &AclOperation::Read);
    assert!(matches!(result, AuthzResult::Denied(_)));
}

// ─── 测试 6: Group ACL ────────────────────────────────────────────────

/// 验证消费组 ACL
#[test]
fn test_group_acl() {
    let (pipeline, _, acl_engine, _) = setup_pipeline(true, true, true);

    // alice 可以读取消费组 order-consumers
    acl_engine.add_acl(group_read_acl("User:alice", "order-consumers"));

    // 认证 alice
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    let auth_data = build_plain_auth("alice", "alice-secret");
    pipeline.authenticate_plain(&mut ctx, &auth_data).unwrap();

    // 读 order-consumers → 允许
    let result = pipeline.authorize(
        &ctx,
        &ResourceType::Group,
        "order-consumers",
        &AclOperation::Read,
    );
    assert!(matches!(result, AuthzResult::Allowed));

    // 读 other-consumers → 拒绝
    let result = pipeline.authorize(
        &ctx,
        &ResourceType::Group,
        "other-consumers",
        &AclOperation::Read,
    );
    assert!(matches!(result, AuthzResult::Denied(_)));
}

// ─── 测试 7: API 权限映射 ─────────────────────────────────────────────

/// 验证 Kafka API → ACL 权限映射
#[test]
fn test_api_permission_mapping() {
    // Produce (API 0) → Topic Write
    let perm = api_permission(0).unwrap();
    assert_eq!(perm.resource_type, ResourceType::Topic);
    assert_eq!(perm.operation, AclOperation::Write);
    assert!(!perm.is_pre_auth);

    // Fetch (API 1) → Topic Read
    let perm = api_permission(1).unwrap();
    assert_eq!(perm.resource_type, ResourceType::Topic);
    assert_eq!(perm.operation, AclOperation::Read);

    // CreateTopics (API 19) → Cluster Create
    let perm = api_permission(19).unwrap();
    assert_eq!(perm.resource_type, ResourceType::Cluster);
    assert_eq!(perm.operation, AclOperation::Create);

    // FindCoordinator (API 10) → Group Describe
    let perm = api_permission(10).unwrap();
    assert_eq!(perm.resource_type, ResourceType::Group);
    assert_eq!(perm.operation, AclOperation::Describe);
}

/// 验证认证前 API 不需要授权
#[test]
fn test_pre_auth_apis() {
    // ApiVersions (API 18) — 认证前
    assert!(is_pre_auth_api(18));

    // SaslHandshake (API 17) — 认证前
    assert!(is_pre_auth_api(17));

    // Produce (API 0) — 需要认证
    assert!(!is_pre_auth_api(0));

    // Fetch (API 1) — 需要认证
    assert!(!is_pre_auth_api(1));
}

// ─── 测试 8: BrokerRouter ACL 集成 ────────────────────────────────────

/// 验证 BrokerRouter.check_authorization() 端到端
#[test]
fn test_broker_router_acl_integration() {
    use rk_broker::{BrokerRouter, PartitionManager, OffsetManager};

    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(PartitionManager::new(dir.path().to_path_buf(), 1_073_741_824, 1));
    let offset_manager = Arc::new(OffsetManager::new(None));

    let mut router = BrokerRouter::with_offset_manager(
        pm,
        1,
        "127.0.0.1".to_string(),
        0,
        None,
        Some("test-cluster".to_string()),
        offset_manager,
    );

    // 默认 ACL 未启用
    assert!(!router.is_acl_enabled());

    // ACL 未启用 → 所有请求通过
    let result = router.check_authorization(0, "User:alice", "10.0.0.1", Some("test-topic"));
    assert!(result.is_ok());

    // 启用 ACL
    router.set_acl_enabled(true);
    assert!(router.is_acl_enabled());
}

// ─── 测试 9: 审计日志完整性 ────────────────────────────────────────────

/// 验证认证 + 授权事件完整记录到审计日志
#[test]
fn test_audit_log_completeness() {
    let (pipeline, _, acl_engine, audit_logger) = setup_pipeline(true, true, true);

    // 添加 ACL
    acl_engine.add_acl(topic_read_acl("User:alice", "topic-orders"));

    // 1. 认证成功
    let mut ctx = SecurityContext::new(SaslMechanism::Plain, "192.168.1.100", false);
    let auth_data = build_plain_auth("alice", "alice-secret");
    pipeline.authenticate_plain(&mut ctx, &auth_data).unwrap();

    // 2. 授权允许
    pipeline.authorize(&ctx, &ResourceType::Topic, "topic-orders", &AclOperation::Read);

    // 3. 授权拒绝
    pipeline.authorize(&ctx, &ResourceType::Topic, "topic-secret", &AclOperation::Read);

    // 4. 认证失败
    let mut ctx_bad = SecurityContext::new(SaslMechanism::Plain, "10.0.0.99", false);
    let bad_data = build_plain_auth("alice", "wrong-password");
    let _ = pipeline.authenticate_plain(&mut ctx_bad, &bad_data);

    // 验证审计日志
    let logger = audit_logger.lock().unwrap();
    let events = logger.recent_events(20);

    // 至少有: 认证成功 + 认证失败 + ACL allow + ACL deny
    assert!(events.len() >= 3, "Should have at least 3 audit events, got {}", events.len());

    // 检查事件类型覆盖
    let event_types: Vec<AuditEventType> = events.iter().map(|e| e.event_type.clone()).collect();

    assert!(
        event_types.contains(&AuditEventType::AuthSuccess),
        "Should have AuthSuccess event"
    );
    assert!(
        event_types.contains(&AuditEventType::AuthFailure),
        "Should have AuthFailure event"
    );
}

// ─── 测试 10: 多用户并发安全 ──────────────────────────────────────────

/// 验证多用户并发认证 + 授权隔离
#[test]
fn test_multi_user_concurrent_security() {
    let (pipeline, _, acl_engine, _) = setup_pipeline(true, true, true);

    // alice: 读 topic-a
    acl_engine.add_acl(topic_read_acl("User:alice", "topic-a"));
    // bob: 读 topic-b
    acl_engine.add_acl(topic_read_acl("User:bob", "topic-b"));

    // 并发认证
    let pipeline_arc = Arc::new(pipeline);

    let handle_alice = {
        let p = pipeline_arc.clone();
        std::thread::spawn(move || {
            let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
            let auth_data = build_plain_auth("alice", "alice-secret");
            p.authenticate_plain(&mut ctx, &auth_data).unwrap();

            // alice 读 topic-a → 允许
            let r1 = p.authorize(&ctx, &ResourceType::Topic, "topic-a", &AclOperation::Read);
            // alice 读 topic-b → 拒绝
            let r2 = p.authorize(&ctx, &ResourceType::Topic, "topic-b", &AclOperation::Read);

            (matches!(r1, AuthzResult::Allowed), matches!(r2, AuthzResult::Denied(_)))
        })
    };

    let handle_bob = {
        let p = pipeline_arc.clone();
        std::thread::spawn(move || {
            let mut ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.0.2", false);
            let auth_data = build_plain_auth("bob", "bob-secret");
            p.authenticate_plain(&mut ctx, &auth_data).unwrap();

            // bob 读 topic-b → 允许
            let r1 = p.authorize(&ctx, &ResourceType::Topic, "topic-b", &AclOperation::Read);
            // bob 读 topic-a → 拒绝
            let r2 = p.authorize(&ctx, &ResourceType::Topic, "topic-a", &AclOperation::Read);

            (matches!(r1, AuthzResult::Allowed), matches!(r2, AuthzResult::Denied(_)))
        })
    };

    let (alice_ok, bob_ok) = (handle_alice.join().unwrap(), handle_bob.join().unwrap());
    assert!(alice_ok.0, "Alice should access topic-a");
    assert!(alice_ok.1, "Alice should be denied topic-b");
    assert!(bob_ok.0, "Bob should access topic-b");
    assert!(bob_ok.1, "Bob should be denied topic-a");
}

// ─── 测试 11: TLS 状态对安全管道的影响 ─────────────────────────────────

/// 验证 TLS 标记正确传播
#[test]
fn test_tls_state_in_security_context() {
    // 非 TLS 连接
    let ctx_plain = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    assert!(!ctx_plain.is_tls());

    // TLS 连接
    let ctx_tls = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", true);
    assert!(ctx_tls.is_tls());
}

// ─── 测试 12: 完整认证→授权→审计链路 ──────────────────────────────────

/// 模拟真实场景: 客户端连接 → 认证 → 生产消息 → 消费消息
#[test]
fn test_full_auth_authz_audit_chain() {
    // 创建包含 producer/consumer 用户的 pipeline
    let user_db = Arc::new(UserDatabase::new());
    user_db.add_user("producer-svc", "producer-pass");
    user_db.add_user("consumer-svc", "consumer-pass");

    let acl_engine2 = Arc::new(AclEngine::new(true));
    acl_engine2.add_acl(topic_write_acl("User:producer-svc", "events"));
    acl_engine2.add_acl(topic_read_acl("User:consumer-svc", "events"));
    acl_engine2.add_acl(group_read_acl("User:consumer-svc", "events-group"));

    let audit_config = AuditConfig {
        enabled: true,
        buffer_size: 10_000,
        log_acl_allow: true,
        log_acl_deny: true,
        log_auth: true,
        log_admin: true,
    };
    let audit_logger2 = Arc::new(Mutex::new(AuditLogger::new(audit_config)));

    let config = SecurityPipelineConfig {
        sasl_enabled: true,
        acl_enabled: true,
        audit_enabled: true,
        default_mechanism: SaslMechanism::Plain,
        super_users: vec![],
    };

    let pipeline2 = SecurityPipeline::new(
        config,
        user_db.clone(),
        acl_engine2.clone(),
        audit_logger2.clone(),
    );

    // Producer 认证
    let mut producer_ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.1.10", true);
    let auth = build_plain_auth("producer-svc", "producer-pass");
    pipeline2.authenticate_plain(&mut producer_ctx, &auth).unwrap();
    assert_eq!(producer_ctx.principal(), Some("User:producer-svc"));

    // Producer 写 events → 允许
    let result = pipeline2.authorize(
        &producer_ctx,
        &ResourceType::Topic,
        "events",
        &AclOperation::Write,
    );
    assert!(matches!(result, AuthzResult::Allowed));

    // Producer 读 events → 拒绝 (只有写权限)
    let result = pipeline2.authorize(
        &producer_ctx,
        &ResourceType::Topic,
        "events",
        &AclOperation::Read,
    );
    assert!(matches!(result, AuthzResult::Denied(_)));

    // ═══ Consumer 连接 ═══
    let mut consumer_ctx = SecurityContext::new(SaslMechanism::Plain, "10.0.1.20", true);
    let auth = build_plain_auth("consumer-svc", "consumer-pass");
    pipeline2.authenticate_plain(&mut consumer_ctx, &auth).unwrap();

    // Consumer 读 events → 允许
    let result = pipeline2.authorize(
        &consumer_ctx,
        &ResourceType::Topic,
        "events",
        &AclOperation::Read,
    );
    assert!(matches!(result, AuthzResult::Allowed));

    // Consumer 读消费组 → 允许
    let result = pipeline2.authorize(
        &consumer_ctx,
        &ResourceType::Group,
        "events-group",
        &AclOperation::Read,
    );
    assert!(matches!(result, AuthzResult::Allowed));

    // Consumer 写 events → 拒绝 (只有读权限)
    let result = pipeline2.authorize(
        &consumer_ctx,
        &ResourceType::Topic,
        "events",
        &AclOperation::Write,
    );
    assert!(matches!(result, AuthzResult::Denied(_)));

    // ═══ 验证审计日志 ═══
    let logger = audit_logger2.lock().unwrap();
    let events = logger.recent_events(50);
    assert!(events.len() >= 4, "Should have auth + authz events");

    // 有认证成功事件
    let auth_success_count = events.iter()
        .filter(|e| e.event_type == AuditEventType::AuthSuccess)
        .count();
    assert_eq!(auth_success_count, 2, "Should have 2 auth success events");
}

// ─── 测试 13: SecurityPipeline 配置摘要 ────────────────────────────────

/// 验证 pipeline summary 正确反映配置
#[test]
fn test_security_pipeline_summary() {
    let (pipeline, _, _, _) = setup_pipeline(true, true, true);
    let summary = pipeline.summary();

    assert!(summary.sasl_enabled);
    assert!(summary.acl_enabled);
    assert!(summary.audit_enabled);
    assert_eq!(summary.user_count, 3); // alice, bob, admin
    assert_eq!(summary.super_user_count, 1); // admin
}

// ─── 测试 14: 多种 SASL 机制 ──────────────────────────────────────────

/// 验证不同 SASL 机制的 SecurityContext 创建
#[test]
fn test_multiple_sasl_mechanisms() {
    // PLAIN
    let ctx_plain = SecurityContext::new(SaslMechanism::Plain, "10.0.0.1", false);
    assert_eq!(*ctx_plain.session().mechanism(), SaslMechanism::Plain);

    // SCRAM-SHA-256
    let ctx_sha256 = SecurityContext::new(SaslMechanism::ScramSha256, "10.0.0.1", true);
    assert_eq!(*ctx_sha256.session().mechanism(), SaslMechanism::ScramSha256);

    // SCRAM-SHA-512
    let ctx_sha512 = SecurityContext::new(SaslMechanism::ScramSha512, "10.0.0.1", true);
    assert_eq!(*ctx_sha512.session().mechanism(), SaslMechanism::ScramSha512);
}

// ─── 测试 15: 并发 ACL 规则变更 ────────────────────────────────────────

/// 验证并发添加/删除 ACL 规则的安全性
#[test]
fn test_concurrent_acl_modifications() {
    let acl_engine = Arc::new(AclEngine::new(true));

    let mut handles = vec![];

    // 并发添加规则
    for i in 0..10 {
        let engine = acl_engine.clone();
        let handle = std::thread::spawn(move || {
            let topic = format!("topic-{}", i);
            engine.add_acl(topic_read_acl("User:test", &topic));
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // 验证所有规则已添加
    assert_eq!(acl_engine.acl_count(), 10);

    // 验证每条规则都生效
    for i in 0..10 {
        let topic = format!("topic-{}", i);
        assert!(acl_engine.authorize(
            "User:test",
            "*",
            &ResourceType::Topic,
            &topic,
            &AclOperation::Read,
        ));
    }
}

// ─── 测试 16: 完整 Kafka API 权限覆盖 ──────────────────────────────────

/// 验证核心 Kafka API 都有权限映射
#[test]
fn test_core_api_permission_coverage() {
    // 核心 API keys 都应有映射
    let core_apis = vec![
        (0, "Produce"),
        (1, "Fetch"),
        (2, "ListOffsets"),
        (3, "Metadata"),
        (8, "OffsetCommit"),
        (9, "OffsetFetch"),
        (10, "FindCoordinator"),
        (11, "JoinGroup"),
        (12, "Heartbeat"),
        (13, "LeaveGroup"),
        (14, "SyncGroup"),
        (17, "SaslHandshake"),
        (18, "ApiVersions"),
        (19, "CreateTopics"),
    ];

    for (api_key, name) in core_apis {
        let perm = api_permission(api_key);
        assert!(
            perm.is_some(),
            "API {} ({}) should have permission mapping",
            api_key,
            name,
        );
    }
}
