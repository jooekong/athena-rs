//! Prometheus metrics for Athena MySQL proxy
//!
//! Exposes metrics via HTTP endpoint for Prometheus scraping.

use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::OnceLock;

/// Global metrics registry
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Get the global metrics instance
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

/// Athena metrics collection
pub struct Metrics {
    /// Registry for all metrics
    pub registry: Registry,

    // Connection metrics
    /// Total client connections accepted
    pub connections_total: IntCounter,
    /// Current active client connections
    pub connections_active: IntGauge,
    /// Total client connections closed
    pub connections_closed: IntCounter,

    // Query metrics
    /// Total queries processed
    pub queries_total: IntCounterVec,
    /// Query latency histogram (in seconds)
    pub query_duration_seconds: HistogramVec,
    /// Query errors by type
    pub query_errors_total: IntCounterVec,

    // Rate limiting metrics
    /// Rate limit permits acquired
    pub rate_limit_acquired_total: IntCounterVec,
    /// Rate limit rejections (queue full)
    pub rate_limit_rejected_queue_full: IntCounterVec,
    /// Rate limit rejections (timeout)
    pub rate_limit_rejected_timeout: IntCounterVec,

    // Routing metrics
    /// Queries routed by target (master/slave)
    pub queries_routed_total: IntCounterVec,
    /// Scatter queries (multi-shard)
    pub scatter_queries_total: IntCounter,

    // Health check metrics
    /// Health check results by status
    pub health_check_total: IntCounterVec,
    /// Current instance health status counts
    pub health_instances: IntGaugeVec,
}

impl Metrics {
    /// Create a new metrics collection
    pub fn new() -> Self {
        let registry = Registry::new();

        // Connection metrics
        let connections_total = IntCounter::new(
            "athena_connections_total",
            "Total number of client connections accepted",
        )
        .unwrap();

        let connections_active = IntGauge::new(
            "athena_connections_active",
            "Current number of active client connections",
        )
        .unwrap();

        let connections_closed = IntCounter::new(
            "athena_connections_closed_total",
            "Total number of client connections closed",
        )
        .unwrap();

        // Query metrics
        let queries_total = IntCounterVec::new(
            Opts::new("athena_queries_total", "Total number of queries processed"),
            &["type"], // select, insert, update, delete, etc.
        )
        .unwrap();

        let query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "athena_query_duration_seconds",
                "Query latency in seconds",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                10.0,
            ]),
            &["type", "shard"],
        )
        .unwrap();

        let query_errors_total = IntCounterVec::new(
            Opts::new("athena_query_errors_total", "Total number of query errors"),
            &["type"], // parse_error, backend_error, timeout, etc.
        )
        .unwrap();

        // Rate limiting metrics
        let rate_limit_acquired_total = IntCounterVec::new(
            Opts::new(
                "athena_rate_limit_acquired_total",
                "Total number of rate limit permits acquired",
            ),
            &["user", "shard"],
        )
        .unwrap();

        let rate_limit_rejected_queue_full = IntCounterVec::new(
            Opts::new(
                "athena_rate_limit_rejected_queue_full_total",
                "Total number of requests rejected due to queue full",
            ),
            &["user", "shard"],
        )
        .unwrap();

        let rate_limit_rejected_timeout = IntCounterVec::new(
            Opts::new(
                "athena_rate_limit_rejected_timeout_total",
                "Total number of requests rejected due to timeout",
            ),
            &["user", "shard"],
        )
        .unwrap();

        // Routing metrics
        let queries_routed_total = IntCounterVec::new(
            Opts::new(
                "athena_queries_routed_total",
                "Total number of queries routed by target",
            ),
            &["target"], // master/slave
        )
        .unwrap();

        let scatter_queries_total = IntCounter::new(
            "athena_scatter_queries_total",
            "Total number of scatter (multi-shard) queries",
        )
        .unwrap();

        // Health check metrics
        let health_check_total = IntCounterVec::new(
            Opts::new(
                "athena_health_check_total",
                "Total number of health checks by result",
            ),
            &["result"], // success, failure, timeout
        )
        .unwrap();

        let health_instances = IntGaugeVec::new(
            Opts::new(
                "athena_health_instances",
                "Current number of instances by health status",
            ),
            &["status"], // healthy, unhealthy, unknown
        )
        .unwrap();

        // Register all metrics
        registry
            .register(Box::new(connections_total.clone()))
            .unwrap();
        registry
            .register(Box::new(connections_active.clone()))
            .unwrap();
        registry
            .register(Box::new(connections_closed.clone()))
            .unwrap();
        registry
            .register(Box::new(queries_total.clone()))
            .unwrap();
        registry
            .register(Box::new(query_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(query_errors_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rate_limit_acquired_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rate_limit_rejected_queue_full.clone()))
            .unwrap();
        registry
            .register(Box::new(rate_limit_rejected_timeout.clone()))
            .unwrap();
        registry
            .register(Box::new(queries_routed_total.clone()))
            .unwrap();
        registry
            .register(Box::new(scatter_queries_total.clone()))
            .unwrap();
        registry
            .register(Box::new(health_check_total.clone()))
            .unwrap();
        registry
            .register(Box::new(health_instances.clone()))
            .unwrap();

        Self {
            registry,
            connections_total,
            connections_active,
            connections_closed,
            queries_total,
            query_duration_seconds,
            query_errors_total,
            rate_limit_acquired_total,
            rate_limit_rejected_queue_full,
            rate_limit_rejected_timeout,
            queries_routed_total,
            scatter_queries_total,
            health_check_total,
            health_instances,
        }
    }

    /// Record a query execution
    pub fn record_query(&self, query_type: &str, shard: &str, duration_secs: f64) {
        self.queries_total.with_label_values(&[query_type]).inc();
        self.query_duration_seconds
            .with_label_values(&[query_type, shard])
            .observe(duration_secs);
    }

    /// Record a query error
    pub fn record_query_error(&self, error_type: &str) {
        self.query_errors_total
            .with_label_values(&[error_type])
            .inc();
    }

    /// Record a new connection
    pub fn record_connection_accepted(&self) {
        self.connections_total.inc();
        self.connections_active.inc();
    }

    /// Record a connection closed
    pub fn record_connection_closed(&self) {
        self.connections_active.dec();
        self.connections_closed.inc();
    }

    /// Record route decision
    pub fn record_route(&self, target: &str, is_scatter: bool) {
        self.queries_routed_total.with_label_values(&[target]).inc();
        if is_scatter {
            self.scatter_queries_total.inc();
        }
    }

    /// Record rate limit acquired
    pub fn record_rate_limit_acquired(&self, user: &str, shard: &str) {
        self.rate_limit_acquired_total
            .with_label_values(&[user, shard])
            .inc();
    }

    /// Record rate limit rejected (queue full)
    pub fn record_rate_limit_queue_full(&self, user: &str, shard: &str) {
        self.rate_limit_rejected_queue_full
            .with_label_values(&[user, shard])
            .inc();
    }

    /// Record rate limit rejected (timeout)
    pub fn record_rate_limit_timeout(&self, user: &str, shard: &str) {
        self.rate_limit_rejected_timeout
            .with_label_values(&[user, shard])
            .inc();
    }

    /// Record a health check result
    pub fn record_health_check(&self, result: &str) {
        self.health_check_total.with_label_values(&[result]).inc();
    }

    /// Update health instance counts
    pub fn set_health_instances(&self, healthy: i64, unhealthy: i64, unknown: i64) {
        self.health_instances
            .with_label_values(&["healthy"])
            .set(healthy);
        self.health_instances
            .with_label_values(&["unhealthy"])
            .set(unhealthy);
        self.health_instances
            .with_label_values(&["unknown"])
            .set(unknown);
    }

    /// Get metrics as Prometheus text format
    pub fn gather(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the metrics HTTP server
pub async fn start_metrics_server(addr: &str) -> anyhow::Result<()> {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tracing::{error, info};

    async fn handle_request(
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        match req.uri().path() {
            "/metrics" => {
                let body = metrics().gather();
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap())
            }
            "/health" => Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from("OK")))
                .unwrap()),
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("Not Found")))
                .unwrap()),
        }
    }

    let addr: SocketAddr = addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!(addr = %addr, "Metrics server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service_fn(handle_request))
                .await
            {
                error!(error = %e, "Metrics server connection error");
            }
        });
    }
}
