# Athena-RS Performance Testing

## Overview

This document describes how to run performance tests for Athena-RS MySQL proxy.

## Prerequisites

1. **MySQL Backend**: Running MySQL instance accessible from the test machine
2. **Athena Proxy**: Compiled and configured
3. **Load Testing Tool**: `sysbench` or `mysqlslap`

## Quick Start

### 1. Start MySQL Backend

```bash
# Using Docker
docker run -d --name mysql-test \
  -e MYSQL_ROOT_PASSWORD=test \
  -e MYSQL_DATABASE=test \
  -p 3306:3306 \
  mysql:8.0
```

### 2. Start Athena Proxy

```bash
# Build release version
cargo build --release

# Configure config/athena.toml with MySQL backend settings
# Start proxy
./target/release/athena
```

### 3. Run Performance Tests

#### Using sysbench

```bash
# Prepare test data
sysbench oltp_read_write \
  --mysql-host=127.0.0.1 \
  --mysql-port=3307 \
  --mysql-user=root \
  --mysql-password=test \
  --mysql-db=test \
  --tables=4 \
  --table-size=10000 \
  prepare

# Run benchmark
sysbench oltp_read_write \
  --mysql-host=127.0.0.1 \
  --mysql-port=3307 \
  --mysql-user=root \
  --mysql-password=test \
  --mysql-db=test \
  --tables=4 \
  --table-size=10000 \
  --threads=16 \
  --time=60 \
  --report-interval=10 \
  run

# Cleanup
sysbench oltp_read_write \
  --mysql-host=127.0.0.1 \
  --mysql-port=3307 \
  --mysql-user=root \
  --mysql-password=test \
  --mysql-db=test \
  --tables=4 \
  cleanup
```

#### Using mysqlslap

```bash
# Simple read benchmark
mysqlslap \
  --host=127.0.0.1 \
  --port=3307 \
  --user=root \
  --password=test \
  --concurrency=50 \
  --iterations=10 \
  --number-int-cols=2 \
  --number-char-cols=3 \
  --auto-generate-sql

# Custom query benchmark
mysqlslap \
  --host=127.0.0.1 \
  --port=3307 \
  --user=root \
  --password=test \
  --concurrency=50 \
  --iterations=10 \
  --query="SELECT * FROM users WHERE user_id = 1"
```

## Metrics to Collect

### From sysbench

- Transactions per second (TPS)
- Queries per second (QPS)
- Latency (min, avg, max, 95th percentile)
- Errors

### From Athena Metrics Endpoint

```bash
# Fetch Prometheus metrics
curl http://127.0.0.1:4307/metrics
```

Key metrics to monitor:
- `athena_queries_total` - Total queries processed
- `athena_query_duration_seconds` - Query latency histogram
- `athena_query_errors_total` - Error count
- `athena_connections_active` - Active client connections
- `athena_rate_limit_*` - Rate limiting statistics

## Baseline Benchmarks

### Test Environment

Document your test environment:
- **Hardware**: CPU cores, RAM, disk type
- **MySQL Version**: e.g., 8.0.35
- **Network**: Local, remote, latency
- **Configuration**: Athena settings (pool size, limits, etc.)

### Expected Performance

| Workload | Threads | Direct MySQL | Via Athena | Overhead |
|----------|---------|--------------|------------|----------|
| OLTP R/W | 16      | X TPS        | Y TPS      | Z%       |
| Select   | 32      | X QPS        | Y QPS      | Z%       |
| Insert   | 16      | X TPS        | Y TPS      | Z%       |

*Fill in baseline numbers after running benchmarks*

## Performance Tuning

### Athena Configuration

```toml
[pool]
max_idle = 20          # Increase for high concurrency
max_age_secs = 3600
max_idle_time_secs = 300

[circuit]
max_concurrent_per_user_shard = 50  # Adjust based on MySQL capacity
queue_size = 200
queue_timeout_ms = 10000
```

### System Tuning

```bash
# Increase file descriptor limit
ulimit -n 65535

# TCP tuning (Linux)
sysctl -w net.core.somaxconn=4096
sysctl -w net.ipv4.tcp_max_syn_backlog=4096
```

## Profiling

### CPU Profiling

```bash
# Using perf (Linux)
perf record -g ./target/release/athena
perf report

# Using cargo-flamegraph
cargo install flamegraph
cargo flamegraph --bin athena
```

### Memory Profiling

```bash
# Using heaptrack
heaptrack ./target/release/athena
heaptrack_print heaptrack.athena.*.gz
```

## Continuous Performance Testing

For CI/CD integration, consider:

1. **Automated benchmarks**: Run sysbench after each release
2. **Regression detection**: Compare against baseline
3. **Grafana dashboard**: Visualize metrics over time

Example CI script:

```bash
#!/bin/bash
set -e

# Start MySQL
docker-compose up -d mysql

# Build and start Athena
cargo build --release
./target/release/athena &
ATHENA_PID=$!
sleep 2

# Run benchmark
sysbench oltp_read_write \
  --mysql-port=3307 \
  --threads=16 \
  --time=30 \
  run > benchmark_results.txt

# Parse results and compare to baseline
python3 compare_benchmark.py benchmark_results.txt baseline.txt

# Cleanup
kill $ATHENA_PID
docker-compose down
```
