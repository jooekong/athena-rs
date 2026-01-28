mod state;

pub use state::SessionState;

use crate::circuit::{ConcurrencyController, LimitError, LimitKey, LimitPermit};
use crate::group::{GroupContext, GroupManager};
use crate::metrics::metrics;
use crate::parser::{
    AggregateInfo, AggregateMerger, AggregateValue, RowParser, SqlAnalysis, SqlAnalyzer,
    StatementType,
};
use crate::pool::{ConnectionError, DbGroupId, PoolManager, PooledConnection};
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
    /// Pool manager for backend connections (set after handshake in groups mode)
    pool_manager: Arc<PoolManager>,
    /// SQL router (set after handshake in groups mode)
    router: Router,
    /// Group manager (for groups mode, used to select group on handshake)
    group_manager: Option<Arc<GroupManager>>,
    /// Concurrency controller for rate limiting
    concurrency_controller: Arc<ConcurrencyController>,
    /// SQL analyzer
    analyzer: SqlAnalyzer,
    /// Auth scramble data (used for password verification)
    auth_scramble: Option<Vec<u8>>,
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
            router: Router::default(),
            group_manager: None,
            concurrency_controller,
            analyzer: SqlAnalyzer::new(),
            auth_scramble: None,
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
            router: Router::new(router_config),
            group_manager: None,
            concurrency_controller,
            analyzer: SqlAnalyzer::new(),
            auth_scramble: None,
        }
    }

    /// Create session with group manager (groups-only mode)
    ///
    /// Pool manager and router will be set after handshake based on database.
    pub fn with_group_manager(
        id: u32,
        group_manager: Arc<GroupManager>,
        concurrency_controller: Arc<ConcurrencyController>,
    ) -> Self {
        // Placeholder pool manager - will be replaced after handshake when group is selected
        let pool_manager = Arc::new(PoolManager::new(
            crate::config::BackendConfig::default(),
            crate::pool::StatelessPoolConfig::default(),
        ));
        let router = Router::default();

        Self {
            id,
            state: SessionState::new(),
            pool_manager,
            router,
            group_manager: Some(group_manager),
            concurrency_controller,
            analyzer: SqlAnalyzer::new(),
            auth_scramble: None,
        }
    }

    /// Select group and update pool manager/router based on database name
    ///
    /// Also validates proxy-level authentication (username + password).
    fn select_group(
        &mut self,
        database: Option<&str>,
        username: &str,
        auth_response: &[u8],
    ) -> Result<(), SessionError> {
        if let Some(ref gm) = self.group_manager {
            // Get group by database name
            let db_name = database.ok_or_else(|| {
                SessionError::Auth("Database name is required for group selection".into())
            })?;

            let ctx = gm.get_by_database(db_name).ok_or_else(|| {
                SessionError::Auth(format!("No group found for database '{}'", db_name))
            })?;

            // Validate username matches group's auth_user
            if ctx.auth_user != username {
                return Err(SessionError::Auth(format!(
                    "Access denied for user '{}' to database '{}'",
                    username, db_name
                )));
            }

            // Validate password using stored scramble
            if let Some(ref scramble) = self.auth_scramble {
                let expected = crate::protocol::compute_auth_response(&ctx.auth_password, scramble);
                if auth_response != expected {
                    return Err(SessionError::Auth(format!(
                        "Access denied for user '{}' (incorrect password)",
                        username
                    )));
                }
            }

            info!(
                session_id = self.id,
                username = %username,
                database = %db_name,
                group = %ctx.name,
                "Selected group and authenticated"
            );
            self.pool_manager = ctx.pool_manager.clone();
            self.router = Router::new(ctx.router_config.clone());
        }
        Ok(())
    }

    /// Run the session - handle client connection
    pub async fn run<S>(mut self, client_stream: S) -> Result<(), SessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut client = Framed::new(client_stream, PacketCodec);

        // Step 1: Send initial handshake to client
        let handshake = InitialHandshake::new(self.id);
        // Store scramble data for password verification
        self.auth_scramble = Some(handshake.auth_plugin_data());
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

        // Step 2.5: For groups mode, select group based on database and validate auth
        if let Err(e) = self.select_group(
            response.database.as_deref(),
            &response.username,
            &response.auth_response,
        ) {
            // Send ERR packet for auth failure
            let err = ErrPacket::new(1045, "28000", &e.to_string());
            client
                .send(err.encode(2, self.state.capability_flags))
                .await?;
            return Err(e);
        }

        // Step 3: Validate connection by getting one from pool
        // This ensures we can connect to the backend before telling client OK
        let home = DbGroupId::Home;
        let test_conn = self
            .pool_manager
            .get_master(&home)
            .await
            .map_err(|e| SessionError::BackendConnect(e.to_string()))?;
        // Return connection immediately
        self.pool_manager.put_master(&home, test_conn).await;

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

        // Cleanup: release any bound transaction connection (abnormal exit)
        if self.state.in_transaction {
            self.pool_manager
                .end_transaction(self.id, self.state.transaction_target.as_ref(), false)
                .await;
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

            // Block USE DATABASE command (COM_INIT_DB)
            // Database switching is not allowed after connection
            if let ClientCommand::InitDb(ref db) = cmd {
                warn!(
                    session_id = self.id,
                    database = %db,
                    "Blocked USE DATABASE command"
                );
                let err = ErrPacket::new(
                    1049,
                    "42000",
                    "Database switching is not allowed. Please reconnect with the desired database.",
                );
                client
                    .send(err.encode(1, self.state.capability_flags))
                    .await?;
                continue;
            }

            // Handle SQL queries with routing
            if let ClientCommand::Query(ref sql) = cmd {
                // Block USE statement in SQL
                if self.is_use_database_sql(sql) {
                    warn!(
                        session_id = self.id,
                        sql = %sql,
                        "Blocked USE DATABASE SQL statement"
                    );
                    let err = ErrPacket::new(
                        1049,
                        "42000",
                        "Database switching is not allowed. Please reconnect with the desired database.",
                    );
                    client
                        .send(err.encode(1, self.state.capability_flags))
                        .await?;
                    continue;
                }

                self.handle_query(client, sql, &packet).await?;
                continue;
            }

            // Non-query commands go to home group
            let home = DbGroupId::Home;

            // Determine if we need transaction connection or stateless connection
            let use_transaction = self.state.in_transaction || cmd.starts_transaction();

            // Handle transaction state changes
            if cmd.starts_transaction() {
                self.state.begin_transaction();
                // Bind a connection for this transaction
                self.pool_manager
                    .begin_transaction(self.id, &home, self.state.database.clone())
                    .await
                    .map_err(|e| SessionError::BackendConnect(e.to_string()))?;
            }

            // Check if command has single-EOF response (like COM_FIELD_LIST)
            let single_eof = matches!(cmd, ClientCommand::FieldList { .. });

            // Execute command
            if use_transaction || self.state.in_transaction {
                // Use transaction-bound connection
                self.execute_with_transaction_ex(client, packet, single_eof).await?;
            } else {
                // Use stateless connection from pool
                self.execute_with_pooled_ex(client, packet, &home, RouteTarget::Master, single_eof)
                    .await?;
            }

            // Handle transaction end
            if cmd.ends_transaction() {
                let target = self.state.transaction_target.clone();
                self.state.end_transaction();
                self.pool_manager
                    .end_transaction(self.id, target.as_ref(), true)
                    .await;
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
            if self.state.transaction_target.is_some() {
                // Transaction was bound to a group, forward COMMIT/ROLLBACK
                self.execute_with_transaction(client, original_packet.clone())
                    .await?;
            } else {
                // Transaction was never bound (no queries executed), just send OK
                let ok = OkPacket::new();
                client
                    .send(ok.encode(1, self.state.capability_flags))
                    .await?;
            }
            let target = self.state.transaction_target.clone();
            self.state.end_transaction();
            self.pool_manager
                .end_transaction(self.id, target.as_ref(), true)
                .await;
            return Ok(());
        }

        // Handle SET statements - intercept to prevent connection pollution
        if analysis.stmt_type == StatementType::Set {
            // Parse SET statement to extract variable and value
            if let Some((var_name, var_value)) = self.parse_set_statement(sql) {
                // Check if this SET should be forwarded (transaction-critical)
                let should_forward = self.state.in_transaction
                    && (var_name.eq_ignore_ascii_case("autocommit")
                        || var_name.to_lowercase().starts_with("transaction"));

                if should_forward {
                    // Forward to backend for transaction-bound connections
                    debug!(
                        session_id = self.id,
                        var = %var_name,
                        value = %var_value,
                        "Forwarding SET to transaction connection"
                    );
                    self.execute_with_transaction(client, original_packet.clone())
                        .await?;
                } else {
                    // Intercept: store locally and return mock OK
                    debug!(
                        session_id = self.id,
                        var = %var_name,
                        value = %var_value,
                        "Intercepted SET command (not forwarded to backend)"
                    );
                    self.state.set_session_var(var_name, var_value);
                    let ok = OkPacket::new();
                    client
                        .send(ok.encode(1, self.state.capability_flags))
                        .await?;
                }
            } else {
                // Couldn't parse SET, just return OK to avoid pollution
                debug!(
                    session_id = self.id,
                    sql = %sql,
                    "SET command not parsed, returning mock OK"
                );
                let ok = OkPacket::new();
                client
                    .send(ok.encode(1, self.state.capability_flags))
                    .await?;
            }
            return Ok(());
        }

        // Route the query
        let route_start = Instant::now();
        let route_result = self.router.route(sql, &analysis, self.state.in_transaction);
        let route_elapsed = route_start.elapsed();

        debug!(
            session_id = self.id,
            route_type = ?route_result.route_type,
            target = ?route_result.target,
            is_scatter = route_result.is_scatter(),
            route_time_us = route_elapsed.as_micros(),
            "Query routed"
        );

        // Record routing metrics
        let target_str = match route_result.target {
            RouteTarget::Master => "master",
            RouteTarget::Slave => "slave",
        };
        metrics().record_route(target_str, route_result.is_scatter());

        // Reject queries with empty shard intersection (conflicting shard keys in multi-table query)
        if route_result.empty_intersection {
            let err = ErrPacket::new(
                1105, // ER_UNKNOWN_ERROR
                "HY000",
                "Empty shard intersection: query involves multiple sharded tables with no common shard",
            );
            client
                .send(err.encode(1, self.state.capability_flags))
                .await?;
            return Ok(());
        }

        // If in transaction, handle deferred binding and shard consistency
        if self.state.in_transaction {
            // Scatter queries not allowed in transaction
            if route_result.is_scatter() {
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

            let target_group = route_result.target_group().unwrap();

            // First query in transaction - bind to group
            // Transaction connection is created with autocommit=0, no explicit BEGIN needed
            if self.state.transaction_target.is_none() {
                self.state.bind_transaction(target_group.clone());
                // Use session database for home queries, shard's database for sharded queries
                let tx_database = if route_result.is_home() {
                    self.state.database.clone()
                } else {
                    None
                };
                self.pool_manager
                    .begin_transaction(self.id, &target_group, tx_database)
                    .await
                    .map_err(|e| SessionError::BackendConnect(e.to_string()))?;
            } else {
                // Check group consistency
                let bound_target = self.state.transaction_target.as_ref().unwrap();
                if bound_target != &target_group {
                    let err = ErrPacket::new(
                        1105,
                        "HY000",
                        &format!(
                            "Cross-shard query in transaction not allowed (bound to {}, query targets {})",
                            bound_target, target_group
                        ),
                    );
                    client
                        .send(err.encode(1, self.state.capability_flags))
                        .await?;
                    return Ok(());
                }
            }

            // Use rewritten SQL for transaction queries
            let rewritten_sql = route_result.get_sql(0).unwrap_or(sql);

            // Build new packet with rewritten SQL
            let mut new_payload = vec![0x03]; // COM_QUERY
            new_payload.extend(rewritten_sql.as_bytes());
            let new_packet = Packet {
                sequence_id: original_packet.sequence_id,
                payload: new_payload.into(),
            };

            self.execute_with_transaction(client, new_packet)
                .await?;
            return Ok(());
        }

        // For scatter queries (multiple shards), we need to execute on all and merge results
        if route_result.is_scatter() {
            // Reject scatter writes - only SELECT is allowed for scatter queries
            if analysis.stmt_type.is_write() {
                let err = ErrPacket::new(
                    1105, // ER_UNKNOWN_ERROR
                    "HY000",
                    "Scatter writes not allowed: INSERT/UPDATE/DELETE must target a single shard",
                );
                client
                    .send(err.encode(1, self.state.capability_flags))
                    .await?;
                return Ok(());
            }

            let scatter_start = Instant::now();
            self.execute_scatter(client, &route_result, &analysis).await?;
            let scatter_elapsed = scatter_start.elapsed();
            info!(
                session_id = self.id,
                shard_count = route_result.shard_count(),
                query_time_ms = query_start.elapsed().as_millis(),
                scatter_time_ms = scatter_elapsed.as_millis(),
                "Scatter query completed"
            );
        } else {
            // Single target execution
            let target_group = route_result.target_group().unwrap();
            let target_str = target_group.to_string();

            // Acquire rate limit permit
            let _permit = match self.acquire_permit(&target_str, client).await? {
                AcquireResult::Acquired(permit) => Some(permit),
                AcquireResult::Disabled => None,
                AcquireResult::Rejected => return Ok(()),
            };

            let rewritten_sql = route_result.get_sql(0).unwrap_or(sql);

            debug!(
                session_id = self.id,
                target = %target_group,
                sql = %rewritten_sql,
                "Executing on single target"
            );

            // Build new packet with rewritten SQL
            let mut new_payload = vec![0x03]; // COM_QUERY
            new_payload.extend(rewritten_sql.as_bytes());
            let new_packet = Packet {
                sequence_id: original_packet.sequence_id,
                payload: new_payload.into(),
            };

            self.execute_with_pooled(client, new_packet, &target_group, route_result.target)
                .await?;

            let query_elapsed = query_start.elapsed();
            info!(
                session_id = self.id,
                target = %target_group,
                route_target = ?route_result.target,
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
            metrics().record_query(query_type, &target_str, query_elapsed.as_secs_f64());
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
        analysis: &SqlAnalysis,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        // Check if this query has aggregate functions that need merging
        let has_aggregates = !analysis.aggregates.is_empty();

        if has_aggregates {
            // Aggregation mode: collect results from all shards and merge
            self.execute_scatter_aggregate(client, route_result, &analysis.aggregates)
                .await
        } else {
            // Non-aggregation mode: stream results from all shards
            self.execute_scatter_stream(client, route_result).await
        }
    }

    /// Execute scatter query with aggregation merging
    async fn execute_scatter_aggregate<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        route_result: &RouteResult,
        aggregates: &[AggregateInfo],
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        // Initialize mergers for each aggregate
        let mut mergers: Vec<AggregateMerger> = aggregates
            .iter()
            .cloned()
            .map(AggregateMerger::new)
            .collect();

        let mut column_definitions: Vec<Packet> = Vec::new();
        let mut column_count = 0u64;
        let mut first_shard = true;

        // Collect data from all shards
        let target_groups = route_result.route_type.to_group_ids();
        for (idx, group_id) in target_groups.iter().enumerate() {
            let group_str = group_id.to_string();
            
            // Acquire rate limit permit
            let _permit = match self.acquire_permit(&group_str, client).await? {
                AcquireResult::Acquired(permit) => Some(permit),
                AcquireResult::Disabled => None,
                AcquireResult::Rejected => return Ok(()),
            };

            let rewritten_sql = route_result
                .get_sql(idx)
                .ok_or_else(|| SessionError::Protocol("Missing rewritten SQL".into()))?;

            debug!(
                session_id = self.id,
                target = %group_id,
                sql = %rewritten_sql,
                "Executing aggregate scatter"
            );

            // Build and send query
            let mut payload = vec![0x03]; // COM_QUERY
            payload.extend(rewritten_sql.as_bytes());
            let packet = Packet {
                sequence_id: 0,
                payload: payload.into(),
            };

            let mut conn = self
                .pool_manager
                .get_for_target(group_id, route_result.target)
                .await
                .map_err(|e| SessionError::BackendConnect(e.to_string()))?;

            if let Err(e) = conn.send(packet).await {
                conn.close();
                return Err(SessionError::BackendConnect(e.to_string()));
            }

            // Read response and collect aggregates
            let result = self
                .collect_aggregate_results(
                    &mut conn,
                    &mut mergers,
                    &mut column_definitions,
                    &mut column_count,
                    first_shard,
                )
                .await;

            // Return connection to pool
            if conn.is_usable() {
                self.pool_manager
                    .put_for_target(group_id, route_result.target, conn)
                    .await;
            }

            if let Err(e) = result {
                warn!(
                    session_id = self.id,
                    target = %group_id,
                    error = %e,
                    "Aggregate scatter failed"
                );
                metrics().record_query_error("scatter_shard_error");
                let err = ErrPacket::new(1105, "HY000", &format!("Shard error: {}", e));
                client.send(err.encode(1, self.state.capability_flags)).await?;
                return Ok(());
            }

            first_shard = false;
        }

        // Build and send merged result to client
        self.send_aggregated_result(client, &mergers, &column_definitions, column_count)
            .await
    }

    /// Collect aggregate results from a single shard
    async fn collect_aggregate_results(
        &self,
        conn: &mut PooledConnection,
        mergers: &mut [AggregateMerger],
        column_definitions: &mut Vec<Packet>,
        column_count: &mut u64,
        first_shard: bool,
    ) -> Result<(), SessionError> {
        let backend_caps = conn.capabilities();

        // Read first packet (column count or error)
        let first_packet = conn.recv().await.map_err(|_| SessionError::BackendDisconnected)?;

        // Check for error
        if is_err_packet(&first_packet.payload) {
            return Err(SessionError::Protocol("Backend error".into()));
        }

        // Check for OK packet (empty result)
        if is_ok_packet(&first_packet.payload) {
            return Ok(());
        }

        // Parse column count
        let count = self.read_length_encoded_int(&first_packet.payload);
        if first_shard {
            *column_count = count;
        }

        // Read column definitions
        for _ in 0..count {
            let col_packet = conn.recv().await.map_err(|_| SessionError::BackendDisconnected)?;
            if first_shard {
                column_definitions.push(col_packet);
            }
        }

        // Read EOF after columns (for non-DEPRECATE_EOF protocol)
        if backend_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
            let _eof = conn.recv().await.map_err(|_| SessionError::BackendDisconnected)?;
        }

        // Read rows and merge aggregates
        loop {
            let row_packet = conn.recv().await.map_err(|_| SessionError::BackendDisconnected)?;

            // Check for EOF or OK (end of results)
            if is_eof_packet(&row_packet.payload, backend_caps)
                || is_ok_packet(&row_packet.payload)
            {
                break;
            }

            // Check for error
            if is_err_packet(&row_packet.payload) {
                return Err(SessionError::Protocol("Backend error in row".into()));
            }

            // Parse row and merge aggregates
            let values = RowParser::parse_row(&row_packet.payload, *column_count as usize);

            // Merge each aggregate value
            for merger in mergers.iter_mut() {
                if merger.info.position < values.len() {
                    merger.merge(values[merger.info.position].clone());
                }
            }
        }

        Ok(())
    }

    /// Send aggregated result to client
    async fn send_aggregated_result<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        mergers: &[AggregateMerger],
        column_definitions: &[Packet],
        column_count: u64,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let mut seq_id: u8 = 1;

        // Send column count
        let count_packet = self.build_length_encoded_packet(column_count, seq_id);
        client.send(count_packet).await?;
        seq_id += 1;

        // Send column definitions
        for col_def in column_definitions {
            let mut packet = col_def.clone();
            packet.sequence_id = seq_id;
            client.send(packet).await?;
            seq_id += 1;
        }

        // Send EOF after columns
        let eof = self.build_eof_packet(seq_id);
        client.send(eof).await?;
        seq_id += 1;

        // Build merged row values
        let mut row_values: Vec<AggregateValue> = vec![AggregateValue::Null; column_count as usize];
        for merger in mergers {
            if merger.info.position < row_values.len() {
                row_values[merger.info.position] = merger.finalize();
            }
        }

        // Send merged row
        let row_packet = self.build_row_packet(&row_values, seq_id);
        client.send(row_packet).await?;
        seq_id += 1;

        // Send final EOF
        let final_eof = self.build_eof_packet(seq_id);
        client.send(final_eof).await?;

        debug!(
            session_id = self.id,
            aggregate_count = mergers.len(),
            "Aggregated scatter result sent"
        );

        Ok(())
    }

    /// Build a packet with length-encoded integer
    fn build_length_encoded_packet(&self, value: u64, sequence_id: u8) -> Packet {
        use bytes::BufMut;
        let mut payload = bytes::BytesMut::new();

        if value < 251 {
            payload.put_u8(value as u8);
        } else if value < 65536 {
            payload.put_u8(0xFC);
            payload.put_u16_le(value as u16);
        } else if value < 16777216 {
            payload.put_u8(0xFD);
            payload.put_u8((value & 0xFF) as u8);
            payload.put_u8(((value >> 8) & 0xFF) as u8);
            payload.put_u8(((value >> 16) & 0xFF) as u8);
        } else {
            payload.put_u8(0xFE);
            payload.put_u64_le(value);
        }

        Packet {
            sequence_id,
            payload: payload.freeze(),
        }
    }

    /// Build an EOF packet
    fn build_eof_packet(&self, sequence_id: u8) -> Packet {
        use bytes::BufMut;
        let mut payload = bytes::BytesMut::new();
        payload.put_u8(0xFE); // EOF marker
        payload.put_u16_le(0); // Warnings
        payload.put_u16_le(0x0002); // Server status: AUTOCOMMIT

        Packet {
            sequence_id,
            payload: payload.freeze(),
        }
    }

    /// Build a row packet from aggregate values
    fn build_row_packet(&self, values: &[AggregateValue], sequence_id: u8) -> Packet {
        use bytes::BufMut;
        let mut payload = bytes::BytesMut::new();

        for value in values {
            match value {
                AggregateValue::Null => {
                    payload.put_u8(0xFB); // NULL indicator
                }
                _ => {
                    let encoded = value.encode();
                    // Write length-encoded string
                    let len = encoded.len();
                    if len < 251 {
                        payload.put_u8(len as u8);
                    } else if len < 65536 {
                        payload.put_u8(0xFC);
                        payload.put_u16_le(len as u16);
                    } else {
                        payload.put_u8(0xFD);
                        payload.put_u8((len & 0xFF) as u8);
                        payload.put_u8(((len >> 8) & 0xFF) as u8);
                        payload.put_u8(((len >> 16) & 0xFF) as u8);
                    }
                    payload.extend_from_slice(&encoded);
                }
            }
        }

        Packet {
            sequence_id,
            payload: payload.freeze(),
        }
    }

    /// Read length-encoded integer from payload
    fn read_length_encoded_int(&self, data: &[u8]) -> u64 {
        if data.is_empty() {
            return 0;
        }

        match data[0] {
            0x00..=0xFA => data[0] as u64,
            0xFC if data.len() >= 3 => u16::from_le_bytes([data[1], data[2]]) as u64,
            0xFD if data.len() >= 4 => {
                u32::from_le_bytes([data[1], data[2], data[3], 0]) as u64
            }
            0xFE if data.len() >= 9 => u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]),
            _ => 0,
        }
    }

    /// Execute scatter query by streaming results (non-aggregation)
    async fn execute_scatter_stream<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        route_result: &RouteResult,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let mut first_shard = true;
        let mut column_count = 0u64;
        let mut sequence_id: u8 = 1;
        let target_groups = route_result.route_type.to_group_ids();
        let total_shards = target_groups.len();

        for (idx, group_id) in target_groups.iter().enumerate() {
            let is_last_shard = idx == total_shards - 1;
            let group_str = group_id.to_string();
            
            // Acquire rate limit permit
            let _permit = match self.acquire_permit(&group_str, client).await? {
                AcquireResult::Acquired(permit) => Some(permit),
                AcquireResult::Disabled => None,
                AcquireResult::Rejected => return Ok(()),
            };

            let rewritten_sql = route_result
                .get_sql(idx)
                .ok_or_else(|| SessionError::Protocol("Missing rewritten SQL".into()))?;

            debug!(
                session_id = self.id,
                target = %group_id,
                sql = %rewritten_sql,
                "Executing scatter"
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
                .get_for_target(group_id, route_result.target)
                .await
                .map_err(|e| SessionError::BackendConnect(e.to_string()))?;

            // Send query
            if let Err(e) = conn.send(packet).await {
                conn.close();
                return Err(SessionError::BackendConnect(e.to_string()));
            }

            // Read response with sequence ID tracking
            let result = self
                .forward_scatter_response(client, &mut conn, first_shard, is_last_shard, &mut column_count, &mut sequence_id)
                .await;

            // Return connection to pool
            if conn.is_usable() {
                self.pool_manager
                    .put_for_target(group_id, route_result.target, conn)
                    .await;
            }

            // If error occurred, abort remaining shards
            if let Err(e) = result {
                warn!(
                    session_id = self.id,
                    target = %group_id,
                    error = %e,
                    "Scatter query aborted due to error"
                );
                metrics().record_query_error("scatter_shard_error");
                return Ok(());
            }

            first_shard = false;
        }

        Ok(())
    }

    /// Forward scatter response, handling column definitions and rows
    ///
    /// # Arguments
    /// * `send_columns` - Whether to send column definitions (true for first shard only)
    /// * `is_last_shard` - Whether this is the last shard (determines if EOF is sent)
    /// * `sequence_id` - Current sequence ID tracker (mutable, incremented for each packet sent)
    async fn forward_scatter_response<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        conn: &mut PooledConnection,
        send_columns: bool,
        is_last_shard: bool,
        column_count: &mut u64,
        sequence_id: &mut u8,
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
            let err_packet = Packet {
                sequence_id: *sequence_id,
                payload: first.payload,
            };
            client.send(err_packet).await?;
            return Err(SessionError::Protocol("Scatter query failed on shard".into()));
        }

        if is_ok_packet(&first.payload) {
            // OK packet (for non-SELECT queries like UPDATE/DELETE)
            // Only forward on last shard to avoid confusing client
            // TODO: accumulate affected_rows across shards
            if is_last_shard {
                let ok_packet = Packet {
                    sequence_id: *sequence_id,
                    payload: first.payload,
                };
                *sequence_id = sequence_id.wrapping_add(1);
                client.send(ok_packet).await?;
            }
            return Ok(());
        }

        // Result set - first packet is column count
        // Always parse column count from this shard's response
        let shard_column_count = parse_length_encoded_int(&first.payload).unwrap_or(0);
        
        if send_columns {
            // Store column count for reference (though each shard should have same count)
            *column_count = shard_column_count;
            let col_count_packet = Packet {
                sequence_id: *sequence_id,
                payload: first.payload,
            };
            *sequence_id = sequence_id.wrapping_add(1);
            client.send(col_count_packet).await?;
        }

        // Read column definitions (use this shard's column count)
        for _ in 0..shard_column_count {
            let col_def = conn
                .recv()
                .await
                .map_err(|_| SessionError::BackendDisconnected)?;
            if send_columns {
                let col_packet = Packet {
                    sequence_id: *sequence_id,
                    payload: col_def.payload,
                };
                *sequence_id = sequence_id.wrapping_add(1);
                client.send(col_packet).await?;
            }
        }

        // Read EOF after columns (if not using deprecate EOF)
        // Use backend capabilities to determine EOF handling
        let backend_caps = conn.capabilities();
        if backend_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
            let eof = conn
                .recv()
                .await
                .map_err(|_| SessionError::BackendDisconnected)?;
            if send_columns {
                let eof_packet = Packet {
                    sequence_id: *sequence_id,
                    payload: eof.payload,
                };
                *sequence_id = sequence_id.wrapping_add(1);
                client.send(eof_packet).await?;
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

            // Always forward rows with correct sequence ID
            if !is_end {
                let row_packet = Packet {
                    sequence_id: *sequence_id,
                    payload: packet.payload,
                };
                *sequence_id = sequence_id.wrapping_add(1);
                client.send(row_packet).await?;
            } else {
                // Only forward final EOF from last shard
                if is_last_shard {
                    let eof_packet = Packet {
                        sequence_id: *sequence_id,
                        payload: packet.payload,
                    };
                    *sequence_id = sequence_id.wrapping_add(1);
                    client.send(eof_packet).await?;
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
        self.execute_with_transaction_ex(client, packet, false).await
    }

    /// Execute command using transaction-bound connection (extended version)
    ///
    /// `single_eof` - if true, response has only one EOF (e.g., COM_FIELD_LIST)
    async fn execute_with_transaction_ex<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        packet: Packet,
        single_eof: bool,
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
        self.forward_transaction_response_ex(client, single_eof).await
    }

    /// Forward response from transaction connection to client
    async fn forward_transaction_response<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        self.forward_transaction_response_ex(client, false).await
    }

    /// Forward response from transaction connection to client (extended version)
    ///
    /// `single_eof` - if true, response has only one EOF (e.g., COM_FIELD_LIST)
    async fn forward_transaction_response_ex<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        single_eof: bool,
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

        // Result set - forward all packets until first EOF
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

        // For single-EOF responses (like COM_FIELD_LIST), skip the second loop
        // For normal result sets in non-DEPRECATE_EOF mode, read rows + final EOF
        if !single_eof && backend_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
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
        group_id: &DbGroupId,
        target: RouteTarget,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        self.execute_with_pooled_ex(client, packet, group_id, target, false).await
    }

    /// Execute command using stateless pooled connection (extended version)
    async fn execute_with_pooled_ex<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        packet: Packet,
        group_id: &DbGroupId,
        target: RouteTarget,
        single_eof: bool,
    ) -> Result<(), SessionError>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        // Get connection from pool
        let mut conn = self
            .pool_manager
            .get_for_target(group_id, target)
            .await
            .map_err(|e| SessionError::BackendConnect(e.to_string()))?;

        // Send packet
        if let Err(e) = conn.send(packet).await {
            conn.close();
            return Err(SessionError::BackendConnect(e.to_string()));
        }

        // Forward response
        let result = self.forward_pooled_response_ex(client, &mut conn, single_eof).await;

        // Return connection to pool (if still usable)
        if conn.is_usable() {
            self.pool_manager
                .put_for_target(group_id, target, conn)
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
        self.forward_pooled_response_ex(client, conn, false).await
    }

    /// Forward response from pooled connection to client (extended version)
    /// 
    /// `single_eof` - if true, response has only one EOF (e.g., COM_FIELD_LIST)
    async fn forward_pooled_response_ex<C>(
        &self,
        client: &mut Framed<C, PacketCodec>,
        conn: &mut PooledConnection,
        single_eof: bool,
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

        // For single-EOF responses (like COM_FIELD_LIST), skip the second loop
        // For normal result sets in non-DEPRECATE_EOF mode, read rows + final EOF
        if !single_eof && backend_caps & capabilities::CLIENT_DEPRECATE_EOF == 0 {
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

    /// Parse a SET statement to extract variable name and value
    ///
    /// Handles formats like:
    /// - SET var = value
    /// - SET var := value
    /// - SET @@session.var = value
    /// - SET NAMES utf8mb4
    ///
    /// Returns (variable_name, value) or None if parsing fails
    fn parse_set_statement(&self, sql: &str) -> Option<(String, String)> {
        let sql = sql.trim();

        // Helper for case-insensitive prefix matching (avoids to_uppercase allocation)
        fn starts_with_ignore_case(s: &str, prefix: &str) -> bool {
            s.len() >= prefix.len()
                && s.as_bytes()[..prefix.len()]
                    .iter()
                    .zip(prefix.as_bytes())
                    .all(|(a, b)| a.to_ascii_uppercase() == b.to_ascii_uppercase())
        }

        // Must start with SET
        if !starts_with_ignore_case(sql, "SET ") {
            return None;
        }

        // Handle SET NAMES/CHARSET specially
        if starts_with_ignore_case(sql, "SET NAMES") {
            let value = sql[9..].trim().trim_matches(|c| c == '\'' || c == '"');
            return Some(("names".to_string(), value.to_string()));
        }
        if starts_with_ignore_case(sql, "SET CHARACTER SET") {
            let value = sql[17..].trim().trim_matches(|c| c == '\'' || c == '"');
            return Some(("charset".to_string(), value.to_string()));
        }
        if starts_with_ignore_case(sql, "SET CHARSET") {
            let value = sql[11..].trim().trim_matches(|c| c == '\'' || c == '"');
            return Some(("charset".to_string(), value.to_string()));
        }

        // Skip "SET " prefix (4 chars)
        let rest = sql[4..].trim();

        // Handle @@session.var or @@var (case-insensitive for prefix)
        let rest = if starts_with_ignore_case(rest, "@@session.") {
            &rest[10..]
        } else if starts_with_ignore_case(rest, "@@global.") {
            &rest[9..]
        } else if rest.starts_with("@@") {
            &rest[2..]
        } else {
            rest
        };

        // Find = or :=
        let (var_part, value_part) = if let Some(pos) = rest.find(":=") {
            (&rest[..pos], &rest[pos + 2..])
        } else if let Some(pos) = rest.find('=') {
            (&rest[..pos], &rest[pos + 1..])
        } else {
            return None;
        };

        let var_name = var_part.trim().to_string();
        let var_value = value_part
            .trim()
            .trim_end_matches(';')
            .trim_matches(|c| c == '\'' || c == '"')
            .to_string();

        if var_name.is_empty() {
            return None;
        }

        Some((var_name, var_value))
    }

    /// Check if SQL is a USE DATABASE statement
    fn is_use_database_sql(&self, sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();
        upper.starts_with("USE ") || upper == "USE"
    }
}

/// Session errors
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Authentication error: {0}")]
    Auth(String),

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
///
/// Uses Cow to avoid allocation when truncation is not needed.
fn truncate_sql(sql: &str, max_len: usize) -> std::borrow::Cow<'_, str> {
    if sql.len() <= max_len {
        std::borrow::Cow::Borrowed(sql)
    } else {
        std::borrow::Cow::Owned(format!("{}...", &sql[..max_len]))
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
