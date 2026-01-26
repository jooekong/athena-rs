mod state;

pub use state::SessionState;

use crate::circuit::{ConcurrencyController, LimitError, LimitKey, LimitPermit};
use crate::metrics::metrics;
use crate::parser::{SqlAnalyzer, StatementType};
use crate::pool::{ConnectionError, PoolManager, PooledConnection, ShardId};
use crate::protocol::{
    capabilities, is_eof_packet, is_err_packet, is_ok_packet, ClientCommand, ErrPacket,
    HandshakeResponse, InitialHandshake, OkPacket, Packet, PacketCodec,
};
use crate::router::{RouteResult, RouteTarget, Router, RouterConfig};
use futures::SinkExt;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, instrument, warn};

use futures::StreamExt;

/// Result of acquiring a rate limit permit
enum AcquireResult {
    /// Permit acquired, proceed with query
    Acquired(LimitPermit),
    /// Rate limiting disabled, proceed without permit
    Disabled,
    /// Rate limit exceeded, error sent to client, skip query
    Rejected,
}

/// Handle a single client session
pub struct Session {
    /// Unique session ID
    pub id: u32,
    /// Session state
    pub state: SessionState,
    /// Pool manager for backend connections
    pool_manager: Arc<PoolManager>,
    /// Concurrency controller for rate limiting
    concurrency_controller: Arc<ConcurrencyController>,
    /// SQL analyzer
    analyzer: SqlAnalyzer,
    /// SQL router
    router: Router,
}

impl Session {
    pub fn new(
        id: u32,
        pool_manager: Arc<PoolManager>,
        concurrency_controller: Arc<ConcurrencyController>,
    ) -> Self {
        Self {
            id,
            state: SessionState::new(),
            pool_manager,
            concurrency_controller,
            analyzer: SqlAnalyzer::new(),
            router: Router::default(),
        }
    }

    /// Create session with custom router configuration
    pub fn with_router(
        id: u32,
        pool_manager: Arc<PoolManager>,
        concurrency_controller: Arc<ConcurrencyController>,
        router_config: RouterConfig,
    ) -> Self {
        Self {
            id,
            state: SessionState::new(),
            pool_manager,
            concurrency_controller,
            analyzer: SqlAnalyzer::new(),
            router: Router::new(router_config),
        }
    }

    /// Run the session - handle client connection
    pub async fn run<S>(mut self, client_stream: S) -> Result<(), SessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut client = Framed::new(client_stream, PacketCodec);

        // Step 1: Send initial handshake to client
        let handshake = InitialHandshake::new(self.id);
        client.send(handshake.encode()).await?;

        // Step 2: Receive handshake response from client
        let response_packet = client
            .next()
            .await
            .ok_or(SessionError::ClientDisconnected)??;

        let response = HandshakeResponse::parse(&response_packet.payload)
            .ok_or_else(|| SessionError::Protocol("Invalid handshake response".into()))?;

        debug!(
            session_id = self.id,
            username = %response.username,
            database = ?response.database,
            "Received handshake response"
        );

        // Store session info
        self.state.set_from_handshake(
            response.username.clone(),
            response.database.clone(),
            response.capability_flags,
            response.character_set,
        );

        // Step 3: Validate connection by getting one from pool
        // This ensures we can connect to the backend before telling client OK
        let shard_id = ShardId::default_shard();
        let test_conn = self
            .pool_manager
            .get_master(&shard_id, self.state.database.clone())
            .await
            .map_err(|e| SessionError::BackendConnect(e.to_string()))?;
        // Return connection immediately
        self.pool_manager.put_master(&shard_id, test_conn).await;

        // Step 4: Send OK to client (authentication successful)
        let ok = OkPacket::new();
        client
            .send(ok.encode(2, self.state.capability_flags))
            .await?;

        info!(
            session_id = self.id,
            username = %self.state.username,
            "Client authenticated"
        );

        // Step 5: Main command loop
        let result = self.command_loop(&mut client).await;

        // Cleanup: release any bound transaction connection
        if self.state.in_transaction {
            self.pool_manager.end_transaction(self.id).await;
        }

        result
    }

    /// Main command processing loop
    async fn command_loop<C>(&mut self, client: &mut Framed<C, PacketCodec>) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        loop {
            // Wait for command from client
            let packet = match client.next().await {
                Some(Ok(p)) => p,
                Some(Err(e)) => {
                    error!(session_id = self.id, error = %e, "Client read error");
                    return Err(e.into());
                }
                None => {
                    info!(session_id = self.id, "Client disconnected");
                    return Ok(());
                }
            };

            let cmd = ClientCommand::parse(&packet.payload);
            debug!(session_id = self.id, command = ?cmd, "Received command");

            // Handle QUIT command
            if matches!(cmd, ClientCommand::Quit) {
                info!(session_id = self.id, "Client sent QUIT");
                return Ok(());
            }

            // Handle SQL queries with routing
            if let ClientCommand::Query(ref sql) = cmd {
                self.handle_query(client, sql, &packet).await?;
                continue;
            }

            // Non-query commands go to default shard
            let shard_id = ShardId::default_shard();

            // Determine if we need transaction connection or stateless connection
            let use_transaction = self.state.in_transaction || cmd.starts_transaction();

            // Handle transaction state changes
            if cmd.starts_transaction() {
                self.state.begin_transaction();
                // Bind a connection for this transaction
                self.pool_manager
                    .begin_transaction(self.id, &shard_id, self.state.database.clone())
                    .await
                    .map_err(|e| SessionError::BackendConnect(e.to_string()))?;
            }

            // Execute command
            if use_transaction || self.state.in_transaction {
                // Use transaction-bound connection
                self.execute_with_transaction(client, packet).await?;
            } else {
                // Use stateless connection from pool
                self.execute_with_pooled(client, packet, &shard_id, RouteTarget::Master)
                    .await?;
            }

            // Update state AFTER successful backend execution
            // This ensures state consistency on backend failure
            if let ClientCommand::InitDb(ref db) = cmd {
                self.state.change_database(db.clone());
            }

            // Handle transaction end
            if cmd.ends_transaction() {
                self.state.end_transaction();
                self.pool_manager.end_transaction(self.id).await;
            }
        }
    }

    /// Handle SQL query with routing
    #[instrument(skip(self, client, original_packet), fields(session_id = self.id, sql_preview = %truncate_sql(sql, 100)))]
    async fn handle_query<C>(
        &mut self,
        client: &mut Framed<C, PacketCodec>,
        sql: &str,
        original_packet: &Packet,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let query_start = Instant::now();

        // Analyze SQL
        let parse_start = Instant::now();
        let analysis = match self.analyzer.analyze(sql) {
            Ok(a) => a,
            Err(e) => {
                warn!(session_id = self.id, error = %e, sql = %sql, "SQL parse error");
                // Send error to client
                let err_msg = format!("SQL parse error: {}", e);
                let err = ErrPacket::new(1064, "42000", &err_msg);
                client
                    .send(err.encode(1, self.state.capability_flags))
                    .await?;
                metrics().record_query_error("parse_error");
                return Ok(());
            }
        };
        let parse_elapsed = parse_start.elapsed();

        debug!(
            session_id = self.id,
            stmt_type = ?analysis.stmt_type,
            tables = ?analysis.tables,
            shard_keys = ?analysis.shard_keys,
            parse_time_us = parse_elapsed.as_micros(),
            "SQL analyzed"
        );

        // Handle transaction control statements
        if analysis.stmt_type == StatementType::Begin {
            // Just mark state as in_transaction, actual connection binding is deferred
            // until the first query determines the target shard
            self.state.begin_transaction();
            // Send OK to client immediately (BEGIN doesn't need backend execution)
            let ok = OkPacket::new();
            client
                .send(ok.encode(1, self.state.capability_flags))
                .await?;
            return Ok(());
        }

        if analysis.stmt_type == StatementType::Commit
            || analysis.stmt_type == StatementType::Rollback
        {
            if self.state.transaction_shard.is_some() {
                // Transaction was bound to a shard, forward COMMIT/ROLLBACK
                self.execute_with_transaction(client, original_packet.clone())
                    .await?;
            } else {
                // Transaction was never bound (no queries executed), just send OK
                let ok = OkPacket::new();
                client
                    .send(ok.encode(1, self.state.capability_flags))
                    .await?;
            }
            self.state.end_transaction();
            self.pool_manager.end_transaction(self.id).await;
            return Ok(());
        }

        // Route the query
        let route_start = Instant::now();
        let route_result = self.router.route(sql, &analysis, self.state.in_transaction);
        let route_elapsed = route_start.elapsed();

        debug!(
            session_id = self.id,
            shards = ?route_result.shards,
            target = ?route_result.target,
            is_scatter = route_result.is_scatter,
            route_time_us = route_elapsed.as_micros(),
            "Query routed"
        );

        // Record routing metrics
        let target_str = match route_result.target {
            RouteTarget::Master => "master",
            RouteTarget::Slave => "slave",
        };
        metrics().record_route(target_str, route_result.is_scatter);

        // If in transaction, handle deferred binding and shard consistency
        if self.state.in_transaction {
            // Scatter queries not allowed in transaction
            if route_result.is_scatter {
                let err = ErrPacket::new(
                    1105, // ER_UNKNOWN_ERROR
                    "HY000",
                    "Scatter queries not allowed in transaction",
                );
                client
                    .send(err.encode(1, self.state.capability_flags))
                    .await?;
                return Ok(());
            }

            let target_shard = &route_result.shards[0];

            // First query in transaction - bind to shard
            // Transaction connection is created with autocommit=0, no explicit BEGIN needed
            if self.state.transaction_shard.is_none() {
                self.state.bind_transaction_shard(target_shard.0.clone());
                self.pool_manager
                    .begin_transaction(self.id, target_shard, self.state.database.clone())
                    .await
                    .map_err(|e| SessionError::BackendConnect(e.to_string()))?;
            } else {
                // Check shard consistency
                let bound_shard = self.state.transaction_shard.as_ref().unwrap();
                if bound_shard != &target_shard.0 {
                    let err = ErrPacket::new(
                        1105,
                        "HY000",
                        &format!(
                            "Cross-shard query in transaction not allowed (bound to {}, query targets {})",
                            bound_shard, target_shard.0
                        ),
                    );
                    client
                        .send(err.encode(1, self.state.capability_flags))
                        .await?;
                    return Ok(());
                }
            }

            self.execute_with_transaction(client, original_packet.clone())
                .await?;
            return Ok(());
        }

        // For scatter queries (multiple shards), we need to execute on all and merge results
        if route_result.is_scatter {
            let scatter_start = Instant::now();
            self.execute_scatter(client, &route_result).await?;
            let scatter_elapsed = scatter_start.elapsed();
            info!(
                session_id = self.id,
                shard_count = route_result.shards.len(),
                query_time_ms = query_start.elapsed().as_millis(),
                scatter_time_ms = scatter_elapsed.as_millis(),
                "Scatter query completed"
            );
        } else {
            // Single shard execution
            let shard_id = &route_result.shards[0];

            // Acquire rate limit permit
            let _permit = match self.acquire_permit(&shard_id.0, client).await? {
                AcquireResult::Acquired(permit) => Some(permit),
                AcquireResult::Disabled => None, // Proceed without permit
                AcquireResult::Rejected => {
                    // Error already sent to client, skip this query
                    return Ok(());
                }
            };

            let rewritten_sql = route_result
                .rewritten_sqls
                .get(shard_id)
                .map(|s| s.as_str())
                .unwrap_or(sql);

            debug!(
                session_id = self.id,
                shard = %shard_id.0,
                sql = %rewritten_sql,
                "Executing on single shard"
            );

            // Build new packet with rewritten SQL
            let mut new_payload = vec![0x03]; // COM_QUERY
            new_payload.extend(rewritten_sql.as_bytes());
            let new_packet = Packet {
                sequence_id: original_packet.sequence_id,
                payload: new_payload.into(),
            };

            self.execute_with_pooled(client, new_packet, shard_id, route_result.target)
                .await?;
            // permit is automatically released when dropped

            let query_elapsed = query_start.elapsed();
            info!(
                session_id = self.id,
                shard = %shard_id.0,
                target = ?route_result.target,
                query_time_ms = query_elapsed.as_millis(),
                parse_time_us = parse_elapsed.as_micros(),
                route_time_us = route_elapsed.as_micros(),
                "Query completed"
            );

            // Record query metrics
            let query_type = match analysis.stmt_type {
                StatementType::Select => "select",
                StatementType::Insert => "insert",
                StatementType::Update => "update",
                StatementType::Delete => "delete",
                _ => "other",
            };
            metrics().record_query(query_type, &shard_id.0, query_elapsed.as_secs_f64());
        }

        Ok(())
    }

    /// Acquire a rate limit permit for the given shard
    ///
    /// Returns:
    /// - `Ok(Acquired(permit))` - proceed with query, release permit when done
    /// - `Ok(Disabled)` - rate limiting disabled, proceed without permit
    /// - `Ok(Rejected)` - rate limit exceeded, error sent to client, skip query
    /// - `Err(_)` - IO error sending error to client
    async fn acquire_permit<C>(
        &self,
        shard: &str,
        client: &mut Framed<C, PacketCodec>,
    ) -> Result<AcquireResult, SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let key = LimitKey::new(&self.state.username, shard);

        match self.concurrency_controller.acquire(key.clone()).await {
            Ok(permit) => {
                metrics().record_rate_limit_acquired(&key.user, &key.shard);
                Ok(AcquireResult::Acquired(permit))
            }
            Err(LimitError::Disabled) => Ok(AcquireResult::Disabled),
            Err(e) => {
                // Send error to client but don't disconnect
                let (code, state, msg) = match &e {
                    LimitError::QueueFull { max } => {
                        metrics().record_rate_limit_queue_full(&key.user, &key.shard);
                        (
                            1040, // ER_CON_COUNT_ERROR
                            "08004",
                            format!("Too many connections: queue full (max {})", max),
                        )
                    }
                    LimitError::Timeout { timeout } => {
                        metrics().record_rate_limit_timeout(&key.user, &key.shard);
                        (
                            1205, // ER_LOCK_WAIT_TIMEOUT
                            "HY000",
                            format!("Rate limit timeout after {:?}", timeout),
                        )
                    }
                    LimitError::Disabled => unreachable!(),
                };

                warn!(
                    session_id = self.id,
                    user = %self.state.username,
                    shard = %shard,
                    error = %e,
                    "Rate limit exceeded"
                );

                let err = ErrPacket::new(code, state, &msg);
                client
                    .send(err.encode(1, self.state.capability_flags))
                    .await?;

                Ok(AcquireResult::Rejected)
            }
        }
    }

    /// Execute scatter query on multiple shards
    async fn execute_scatter<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        route_result: &RouteResult,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        // For now, execute sequentially on each shard and merge results
        // TODO: parallel execution and proper result merging

        let mut first_shard = true;
        let mut column_count = 0u64;
        let total_shards = route_result.shards.len();

        for (shard_idx, shard_id) in route_result.shards.iter().enumerate() {
            let is_last_shard = shard_idx == total_shards - 1;
            // Acquire rate limit permit for this shard
            let _permit = match self.acquire_permit(&shard_id.0, client).await? {
                AcquireResult::Acquired(permit) => Some(permit),
                AcquireResult::Disabled => None,
                AcquireResult::Rejected => {
                    // Error already sent to client
                    // For scatter queries, we abort the whole operation
                    return Ok(());
                }
            };

            let rewritten_sql = route_result
                .rewritten_sqls
                .get(shard_id)
                .ok_or_else(|| SessionError::Protocol("Missing rewritten SQL".into()))?;

            debug!(
                session_id = self.id,
                shard = %shard_id.0,
                sql = %rewritten_sql,
                "Executing scatter on shard"
            );

            // Build query packet
            let mut payload = vec![0x03]; // COM_QUERY
            payload.extend(rewritten_sql.as_bytes());
            let packet = Packet {
                sequence_id: 0,
                payload: payload.into(),
            };

            // Get connection from pool
            let mut conn = self
                .pool_manager
                .get_for_target(shard_id, route_result.target, self.state.database.clone())
                .await
                .map_err(|e| SessionError::BackendConnect(e.to_string()))?;

            // Send query
            if let Err(e) = conn.send(packet).await {
                conn.close();
                return Err(SessionError::BackendConnect(e.to_string()));
            }

            // Read response
            let result = self
                .forward_scatter_response(client, &mut conn, first_shard, is_last_shard, &mut column_count)
                .await;

            // Return connection to pool
            if conn.is_usable() {
                self.pool_manager
                    .put_for_target(shard_id, route_result.target, conn)
                    .await;
            }

            // If error occurred (already sent to client), abort remaining shards
            if let Err(e) = result {
                warn!(
                    session_id = self.id,
                    shard = %shard_id.0,
                    error = %e,
                    "Scatter query aborted due to shard error"
                );
                return Ok(()); // Error already sent to client, don't disconnect session
            }

            // permit is automatically released when it goes out of scope
            first_shard = false;
        }

        // Send final EOF if we haven't sent one
        // For now, the last shard's EOF will be the final one
        Ok(())
    }

    /// Forward scatter response, handling column definitions and rows
    ///
    /// # Arguments
    /// * `send_columns` - Whether to send column definitions (true for first shard only)
    /// * `is_last_shard` - Whether this is the last shard (determines if EOF is sent)
    async fn forward_scatter_response<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        conn: &mut PooledConnection,
        send_columns: bool,
        is_last_shard: bool,
        column_count: &mut u64,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        // Read first packet (OK/ERR or column count)
        let first = conn
            .recv()
            .await
            .map_err(|_| SessionError::BackendDisconnected)?;

        if is_err_packet(&first.payload) {
            // Error from backend - forward immediately and signal caller to abort
            client.send(first).await?;
            return Err(SessionError::Protocol("Scatter query failed on shard".into()));
        }

        if is_ok_packet(&first.payload) {
            // OK packet (for non-SELECT queries like UPDATE/DELETE)
            // Only forward on last shard to avoid confusing client
            // TODO: accumulate affected_rows across shards
            if is_last_shard {
                client.send(first).await?;
            }
            return Ok(());
        }

        // Result set - first packet is column count
        if send_columns {
            // Parse column count for future shards
            *column_count = parse_length_encoded_int(&first.payload).unwrap_or(0);
            client.send(first).await?;
        }

        // Read column definitions
        for _ in 0..*column_count {
            let col_def = conn
                .recv()
                .await
                .map_err(|_| SessionError::BackendDisconnected)?;
            if send_columns {
                client.send(col_def).await?;
            }
        }

        // Read EOF after columns (if not using deprecate EOF)
        // Use backend capabilities to determine EOF handling
        let backend_caps = conn.capabilities();
        if !(backend_caps & capabilities::CLIENT_DEPRECATE_EOF != 0) {
            let eof = conn
                .recv()
                .await
                .map_err(|_| SessionError::BackendDisconnected)?;
            if send_columns {
                client.send(eof).await?;
            }
        }

        // Read rows until EOF/OK
        loop {
            let packet = conn
                .recv()
                .await
                .map_err(|_| SessionError::BackendDisconnected)?;

            let is_end = is_ok_packet(&packet.payload)
                || is_err_packet(&packet.payload)
                || is_eof_packet(&packet.payload, backend_caps);

            // Always forward rows
            if !is_end {
                client.send(packet).await?;
            } else {
                // Only forward final EOF from last shard
                if is_last_shard {
                    client.send(packet).await?;
                }
                break;
            }
        }

        Ok(())
    }

    /// Execute command using transaction-bound connection
    async fn execute_with_transaction<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        packet: Packet,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let tx_pool = self.pool_manager.transaction_pool();

        // Send packet to backend
        tx_pool
            .send(self.id, packet)
            .await
            .map_err(|e| SessionError::BackendConnect(e.to_string()))?;

        // Forward response(s) back to client
        self.forward_transaction_response(client).await
    }

    /// Forward response from transaction connection to client
    async fn forward_transaction_response<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let tx_pool = self.pool_manager.transaction_pool();

        // Get backend capabilities for EOF detection
        let backend_caps = tx_pool
            .capabilities(self.id)
            .await
            .unwrap_or(self.state.capability_flags); // fallback to client caps if not found

        // Read first packet
        let first = tx_pool
            .recv(self.id)
            .await
            .map_err(|_e| SessionError::BackendDisconnected)?;

        if is_ok_packet(&first.payload) || is_err_packet(&first.payload) {
            client.send(first).await?;
            return Ok(());
        }

        // Result set - forward all packets
        client.send(first).await?;

        loop {
            let packet = tx_pool
                .recv(self.id)
                .await
                .map_err(|_e| SessionError::BackendDisconnected)?;

            let is_end = is_ok_packet(&packet.payload)
                || is_err_packet(&packet.payload)
                || is_eof_packet(&packet.payload, backend_caps);

            client.send(packet).await?;

            if is_end {
                break;
            }
        }

        // In non-DEPRECATE_EOF mode, read rows (second loop for rows + final EOF)
        // First loop above handled column definitions + first EOF
        if backend_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
            loop {
                let packet = tx_pool
                    .recv(self.id)
                    .await
                    .map_err(|_e| SessionError::BackendDisconnected)?;

                let is_end = is_ok_packet(&packet.payload)
                    || is_err_packet(&packet.payload)
                    || is_eof_packet(&packet.payload, backend_caps);

                client.send(packet).await?;

                if is_end {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Execute command using stateless pooled connection
    async fn execute_with_pooled<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        packet: Packet,
        shard_id: &ShardId,
        target: RouteTarget,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        // Get connection from pool based on target
        let mut conn = self
            .pool_manager
            .get_for_target(shard_id, target, self.state.database.clone())
            .await
            .map_err(|e| SessionError::BackendConnect(e.to_string()))?;

        // Send packet
        if let Err(e) = conn.send(packet).await {
            conn.close();
            return Err(SessionError::BackendConnect(e.to_string()));
        }

        // Forward response
        let result = self.forward_pooled_response(client, &mut conn).await;

        // Return connection to pool (if still usable)
        if conn.is_usable() {
            self.pool_manager
                .put_for_target(shard_id, target, conn)
                .await;
        }

        result
    }

    /// Forward response from pooled connection to client
    async fn forward_pooled_response<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        conn: &mut PooledConnection,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        // Use backend capabilities for EOF detection
        let backend_caps = conn.capabilities();

        // Read first packet
        let first = conn
            .recv()
            .await
            .map_err(|_| SessionError::BackendDisconnected)?;

        if is_ok_packet(&first.payload) || is_err_packet(&first.payload) {
            client.send(first).await?;
            return Ok(());
        }

        // Result set
        client.send(first).await?;

        loop {
            let packet = conn
                .recv()
                .await
                .map_err(|_| SessionError::BackendDisconnected)?;

            let is_end = is_ok_packet(&packet.payload)
                || is_err_packet(&packet.payload)
                || is_eof_packet(&packet.payload, backend_caps);

            client.send(packet).await?;

            if is_end {
                break;
            }
        }

        // In non-DEPRECATE_EOF mode, read rows (second loop for rows + final EOF)
        // First loop above handled column definitions + first EOF
        if backend_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
            loop {
                let packet = conn
                    .recv()
                    .await
                    .map_err(|_| SessionError::BackendDisconnected)?;

                let is_end = is_ok_packet(&packet.payload)
                    || is_err_packet(&packet.payload)
                    || is_eof_packet(&packet.payload, backend_caps);

                client.send(packet).await?;

                if is_end {
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Session errors
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Client disconnected")]
    ClientDisconnected,

    #[error("Backend disconnected")]
    BackendDisconnected,

    #[error("Backend connection failed: {0}")]
    BackendConnect(String),

    #[error("Backend authentication failed: {0}")]
    BackendAuth(String),

    #[error("Pool error: {0}")]
    Pool(#[from] ConnectionError),

    #[error("Rate limit error: {0}")]
    RateLimit(#[from] LimitError),
}

/// Truncate SQL for logging (avoid huge log entries)
fn truncate_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        sql.to_string()
    } else {
        format!("{}...", &sql[..max_len])
    }
}

/// Parse length-encoded integer from MySQL packet
fn parse_length_encoded_int(data: &[u8]) -> Option<u64> {
    if data.is_empty() {
        return None;
    }
    match data[0] {
        // Single byte value
        0..=0xFA => Some(data[0] as u64),
        // 2-byte value follows
        0xFC if data.len() >= 3 => {
            Some(u16::from_le_bytes([data[1], data[2]]) as u64)
        }
        // 3-byte value follows
        0xFD if data.len() >= 4 => {
            Some(u32::from_le_bytes([data[1], data[2], data[3], 0]) as u64)
        }
        // 8-byte value follows
        0xFE if data.len() >= 9 => {
            Some(u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]))
        }
        _ => None,
    }
}
