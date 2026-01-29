# Athena-RS

A high-performance MySQL proxy middleware written in Rust, providing connection pooling, query routing, sharding, and read/write splitting for MySQL databases.

## Features

- **MySQL Protocol Support** - Full implementation of MySQL handshake, authentication, and command handling
- **Connection Pooling** - Dual-pool model (transaction pool + stateless pool) for efficient connection reuse
- **Read/Write Splitting** - Automatic routing of SELECT queries to replicas and writes to primary
- **Transparent Sharding** - SQL parsing and intelligent routing to correct shards
- **Rate Limiting** - Fine-grained concurrency control per (user, shard) tuple
- **Observability** - Prometheus metrics and structured tracing logs
- **Graceful Shutdown** - Proper handling of SIGTERM/SIGINT signals

## Quick Start

### Prerequisites

- Rust 1.70+ (2021 edition)
- A running MySQL instance

### Build

```bash
cargo build --release
```

### Configuration

Create a configuration file at `config/athena.toml`:

```toml
[server]
listen_addr = "127.0.0.1"
listen_port = 3307

[backend]
host = "127.0.0.1"
port = 3306
user = "root"
password = ""
# database = "test"

[circuit]
enabled = true
max_concurrent_per_user_shard = 10
queue_size = 100
queue_timeout_ms = 5000

# Optional: Sharding rules
# [[sharding]]
# name = "user_shard"
# table_pattern = "users"
# shard_column = "user_id"
# algorithm = "mod"      # mod, hash, or range
# shard_count = 4
```

### Run

```bash
# With default config path
cargo run --release

# Or specify config path
cargo run --release -- --config /path/to/athena.toml
```

### Connect

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Athena Proxy                           │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌───────────────┐  │
│  │Protocol │  │ Parser  │  │ Router  │  │    Circuit    │  │
│  │  Layer  │  │  Layer  │  │  Layer  │  │   (Limiter)   │  │
│  └────┬────┘  └────┬────┘  └────┬────┘  └───────┬───────┘  │
│       │            │            │                │          │
│       └────────────┴─────┬──────┴────────────────┘          │
│                          │                                  │
│                    ┌─────┴─────┐                            │
│                    │  Session  │                            │
│                    │  Manager  │                            │
│                    └─────┬─────┘                            │
│                          │                                  │
│       ┌──────────────────┼──────────────────┐               │
│       │                  │                  │               │
│  ┌────┴────┐       ┌─────┴─────┐      ┌─────┴─────┐        │
│  │Stateless│       │Transaction│      │  Metrics  │        │
│  │  Pool   │       │   Pool    │      │  (Prom)   │        │
│  └────┬────┘       └─────┬─────┘      └───────────┘        │
└───────┼──────────────────┼──────────────────────────────────┘
        │                  │
        └────────┬─────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
┌───┴───┐   ┌────┴────┐   ┌───┴───┐
│Primary│   │ Replica │   │ Shard │
│  DB   │   │   DB    │   │  DBs  │
└───────┘   └─────────┘   └───────┘
```

## Sharding Algorithms

| Algorithm | Description | Formula |
|-----------|-------------|---------|
| `mod` | Even distribution | `hash(key) % shard_count` |
| `hash` | FNV hash distribution | `fnv_hash(key) % shard_count` |
| `range` | Range-based sharding | Based on configured boundaries |

## Metrics

Athena exposes Prometheus metrics on port `listen_port + 1000` (default: 4307).

**Endpoints:**
- `GET /metrics` - Prometheus format metrics
- `GET /health` - Health check

**Key Metrics:**
- `athena_connections_total` - Total connections
- `athena_queries_total` - Total queries processed
- `athena_query_duration_seconds` - Query latency histogram
- `athena_queries_routed_total` - Queries routed by target
- `athena_rate_limit_rejected_total` - Rate-limited requests

## Project Structure

```
athena-rs/
├── src/
│   ├── main.rs          # Entry point
│   ├── protocol/        # MySQL protocol implementation
│   ├── config/          # Configuration loading and schema
│   ├── session/         # Client session management
│   ├── parser/          # SQL parsing and rewriting
│   ├── router/          # Query routing and sharding
│   ├── pool/            # Connection pool management
│   ├── circuit/         # Rate limiting
│   └── metrics/         # Prometheus metrics
├── config/
│   └── athena.toml      # Example configuration
└── docs/
    ├── DESIGN.md        # Architecture design document
    └── PROGRESS.md      # Implementation progress
```

## Documentation

- [Architecture Design](docs/DESIGN.md) - Detailed design documentation
- [Implementation Progress](docs/PROGRESS.md) - Development phases and status

## Development

### Run Tests

```bash
cargo test
```

### Enable Debug Logging

```bash
RUST_LOG=debug cargo run
```

## License

MIT License
