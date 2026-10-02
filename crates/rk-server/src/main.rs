//! R-Kafka Server 入口
//!
//! 配置加载、日志初始化、服务启动。
//! 集成: PartitionManager + OffsetManager + PersistentOffsetManager
//!        + BrokerRouter + TCP Server + HTTP Metrics Server + Graceful Shutdown
//!        + ConfigReloader (SIGHUP) + Startup Banner

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use clap::Parser;
use tokio::sync::watch;
use tracing::info;

#[derive(Parser)]
#[command(name = "r-kafka", about = "R-Kafka: Pure Rust Kafka-compatible broker")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config/r-kafka.toml")]
    config: String,
}

/// R-Kafka 版本号
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 打印启动 Banner
fn print_banner(config: &rk_core::BrokerConfig) {
    let banner = format!(
        r#"
    ____        _       _   __  __       _    _
   |  _ \      | |     | | |  \/  |     | |  | |
   | |_) | ___ | |_    | | | \  / | __ _| |__| | ___
   |  _ < / _ \| __|   | | | |\/| |/ _` |  __  |/ _ \
   | |_) | (_) | |_    | | | |  | | (_| | |  | |  __/
   |____/ \___/ \__|   |_| |_|  |_|\__,_|_|  |_|\___|

   Pure Rust Kafka-Compatible Distributed Messaging Engine
   Version: {version}
   ─────────────────────────────────────────────────
   Broker ID:     {broker_id}
   Listen:        {host}:{port}
   Data Dir:      {data_dir}
   Metrics:       {metrics_status}
   SASL:          {sasl_status}
   Seg Max Size:  {seg_size}
   Retention:     {retention}
   ─────────────────────────────────────────────────
"#,
        version = VERSION,
        broker_id = config.broker.id,
        host = config.broker.host,
        port = config.broker.port,
        data_dir = config.storage.data_dir,
        metrics_status = if config.observability.metrics_enabled {
            format!("port {}", config.observability.metrics_port)
        } else {
            "disabled".to_string()
        },
        sasl_status = if config.security.sasl_enabled {
            format!("enabled ({:?})", config.security.sasl_mechanisms)
        } else {
            "disabled".to_string()
        },
        seg_size = format_bytes(config.storage.segment_max_size),
        retention = format_bytes(config.retention.max_bytes),
    );
    println!("{}", banner);
}

/// 格式化字节数为人类可读格式
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // 初始化 tracing 日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // 加载配置
    let config = rk_core::BrokerConfig::from_file(&cli.config)?;

    // 打印启动 Banner
    print_banner(&config);

    info!(
        broker_id = config.broker.id,
        host = %config.broker.host,
        port = config.broker.port,
        data_dir = %config.storage.data_dir,
        "Configuration loaded"
    );

    // 创建共享可变配置 (用于热加载)
    let shared_config = Arc::new(RwLock::new(config.clone()));
    let config_reloader = rk_broker::ConfigReloader::new(
        std::path::PathBuf::from(&cli.config),
        shared_config.clone(),
    );

    // 初始化存储引擎: 从数据目录恢复 PartitionManager
    let data_dir = std::path::PathBuf::from(&config.storage.data_dir);
    let (partition_manager, recovery_result) = rk_broker::PartitionManager::recover(
        data_dir.clone(),
        config.storage.segment_max_size,
        config.broker.id,
    )?;
    let partition_manager = Arc::new(partition_manager);

    info!(
        topics = recovery_result.topics_recovered,
        partitions = recovery_result.partitions_recovered,
        "Storage engine recovered"
    );

    // 初始化 SASL 认证器
    let sasl_enabled = config.security.sasl_enabled;
    let authenticator = if sasl_enabled {
        let mut credentials = HashMap::new();
        for user in &config.security.sasl_users {
            credentials.insert(user.username.clone(), user.password.clone());
        }
        info!(
            mechanisms = ?config.security.sasl_mechanisms,
            users = credentials.len(),
            "SASL authentication enabled"
        );
        Arc::new(rk_broker::SaslAuthenticator::with_auth(credentials))
    } else {
        info!("SASL authentication disabled");
        Arc::new(rk_broker::SaslAuthenticator::new())
    };

    // 初始化 OffsetManager + PersistentOffsetManager
    let offset_manager = Arc::new(rk_broker::OffsetManager::new(None));
    let persist_dir = data_dir.join("offsets");
    let persistent_offset_manager = rk_broker::PersistentOffsetManager::new(
        offset_manager.clone(),
        persist_dir,
    );
    // 从磁盘恢复偏移量
    if let Err(e) = persistent_offset_manager.load_from_disk() {
        tracing::warn!(error = %e, "Failed to load offset snapshot");
    }
    info!("PersistentOffsetManager initialized");

    // 启动偏移量定期快照任务 (每 30 秒)
    let pom_for_flush = Arc::new(persistent_offset_manager);
    let pom_clone = pom_for_flush.clone();
    let flush_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(e) = pom_clone.save_to_disk() {
                tracing::warn!(error = %e, "Offset snapshot save failed");
            }
        }
    });

    // 初始化 Broker 路由 (含 SASL 配置)
    let router = Arc::new(rk_broker::BrokerRouter::with_sasl_config(
        partition_manager,
        config.broker.id,
        config.broker.host.clone(),
        config.broker.port as i32,
        config.broker.rack.clone(),
        Some(format!("rk-cluster-{}", config.broker.id)),
        offset_manager,
        sasl_enabled,
        authenticator,
    ));
    info!("Broker router initialized");

    // 创建优雅关闭信号通道
    let (shutdown_tx, shutdown_rx_tcp) = watch::channel(false);
    let shutdown_rx_http = shutdown_tx.subscribe();

    // 启动 HTTP 监控服务器 (如果启用)
    let flow_controller = Arc::new(rk_network::FlowController::new(&config));
    let http_handle = if config.observability.metrics_enabled {
        let metrics = Arc::new(rk_broker::BrokerMetrics::new());
        let http_server = rk_network::HttpMetricsServer::new(
            config.broker.host.clone(),
            config.observability.metrics_port,
            metrics,
            i32::from(config.broker.id),
        ).with_flow_controller(flow_controller.clone());
        let shutdown_rx = shutdown_rx_http;
        let handle = tokio::spawn(async move {
            if let Err(e) = http_server.run(shutdown_rx).await {
                tracing::error!(error = %e, "HTTP metrics server error");
            }
        });
        info!(
            port = config.observability.metrics_port,
            "HTTP metrics server started"
        );
        Some(handle)
    } else {
        info!("HTTP metrics server disabled");
        None
    };

    // 运行 TCP 服务器 (accept loop)
    let config_clone = config.clone();
    let router_clone = router.clone();
    let flow_controller_clone = flow_controller.clone();
    let server_handle = tokio::spawn(async move {
        // 使用带关闭信号的 run_server_with_shutdown
        let listener = match rk_network::start_listener(&config_clone).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "Failed to start TCP listener");
                return;
            }
        };

        // 创建 TLS acceptor (如果启用)
        let tls_acceptor = if config_clone.security.tls_enabled {
            match build_tls_acceptor(&config_clone) {
                Ok(acceptor) => {
                    info!("TLS enabled, mode={}", config_clone.security.tls_mode);
                    Some(acceptor)
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create TLS acceptor, falling back to plain TCP");
                    None
                }
            }
        } else {
            None
        };

        if let Err(e) = rk_network::run_server_with_shutdown(
            listener,
            router_clone,
            flow_controller_clone,
            tls_acceptor,
            shutdown_rx_tcp,
        ).await {
            tracing::error!(error = %e, "Server error");
        }
    });

    // 设置 SIGHUP 信号处理 (配置热加载)
    let config_reloader = Arc::new(config_reloader);
    let reloader_clone = config_reloader.clone();
    let sighup_handle = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sighup = signal(SignalKind::hangup()).expect("Failed to register SIGHUP handler");
            loop {
                sighup.recv().await;
                info!("Received SIGHUP, reloading configuration...");
                match reloader_clone.reload() {
                    Ok(report) => {
                        info!(
                            updated = report.updated_fields.len(),
                            immutable_ignored = report.immutable_changes.len(),
                            "Configuration reloaded successfully"
                        );
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to reload configuration");
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            // 非 Unix 平台不支持 SIGHUP，保持任务存活
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        }
    });

    // 打印启动信息
    info!(
        broker_id = config.broker.id,
        kafka_port = config.broker.port,
        metrics_port = config.observability.metrics_port,
        metrics_enabled = config.observability.metrics_enabled,
        "R-Kafka broker is ready and accepting connections"
    );

    // 等待 Ctrl-C 关闭信号
    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal, initiating graceful shutdown...");

    // 发送关闭信号
    let _ = shutdown_tx.send(true);

    // 等待服务器任务完成 (超时 5 秒)
    let shutdown_timeout = std::time::Duration::from_secs(5);
    let _ = tokio::time::timeout(shutdown_timeout, server_handle).await;
    info!("TCP server stopped");

    // 停止 HTTP 服务器
    if let Some(handle) = http_handle {
        let _ = tokio::time::timeout(shutdown_timeout, handle).await;
        info!("HTTP metrics server stopped");
    }

    // 停止 SIGHUP 处理任务和偏移量快照任务
    sighup_handle.abort();
    flush_handle.abort();

    // 最后一次保存偏移量快照
    if let Err(e) = pom_for_flush.save_to_disk() {
        tracing::warn!(error = %e, "Final offset snapshot save failed");
    } else {
        info!("Final offset snapshot saved");
    }

    // 打印最终指标
    let snapshot = router.metrics_snapshot();
    info!(%snapshot, "Final broker metrics");

    info!("R-Kafka broker stopped gracefully");
    Ok(())
}

/// 从 BrokerConfig 构建 TLS Acceptor
fn build_tls_acceptor(config: &rk_core::BrokerConfig) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    let tls_config = rk_security::tls::TlsConfig {
        enabled: true,
        mode: match config.security.tls_mode.as_str() {
            "mutual" | "mTLS" | "mtls" => rk_security::tls::TlsMode::Mutual,
            _ => rk_security::tls::TlsMode::OneWay,
        },
        cert_path: config.security.cert_path.as_ref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("certs/server.crt")),
        key_path: config.security.key_path.as_ref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("certs/server.key")),
        ca_path: config.security.ca_path.as_ref().map(std::path::PathBuf::from),
        protocol_versions: vec![rk_security::tls::TlsProtocolVersion::Tls13],
    };
    tls_config.validate()?;
    let acceptor = rk_security::tls::create_tls_acceptor(&tls_config)?;
    Ok(acceptor)
}
