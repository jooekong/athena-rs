use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, error};

use crate::config::BackendConfig;
use crate::protocol::{
    capabilities, compute_auth_response, is_err_packet, is_ok_packet, ErrPacket,
    HandshakeResponse, InitialHandshake, OkPacket, Packet, PacketCodec,
};

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is available for use
    Idle,
    /// Connection is currently in use
    InUse,
    /// Connection is broken/closed
    Closed,
}

/// A wrapper around a MySQL backend connection
pub struct PooledConnection {
    /// Underlying framed connection
    pub(crate) framed: Framed<TcpStream, PacketCodec>,
    /// Connection state
    pub(crate) state: ConnectionState,
    /// When the connection was created
    pub(crate) created_at: Instant,
    /// When the connection was last used
    pub(crate) last_used_at: Instant,
    /// Server capability flags
    pub(crate) capabilities: u32,
    /// Current database
    pub(crate) database: Option<String>,
}

impl PooledConnection {
    /// Create a new connection to a backend
    pub async fn connect(config: &BackendConfig, database: Option<String>) -> Result<Self, ConnectionError> {
        let addr = format!("{}:{}", config.host, config.port);
        debug!(addr = %addr, "Connecting to backend");

        let stream = TcpStream::connect(&addr).await.map_err(|e| {
            error!(error = %e, "Failed to connect to backend");
            ConnectionError::Connect(e.to_string())
        })?;

        let mut framed = Framed::new(stream, PacketCodec);

        // Receive backend handshake
        let handshake_packet = framed
            .next()
            .await
            .ok_or(ConnectionError::Disconnected)?
            .map_err(|e| ConnectionError::Io(e.to_string()))?;

        let backend_handshake = InitialHandshake::parse(&handshake_packet.payload)
            .ok_or_else(|| ConnectionError::Protocol("Invalid backend handshake".into()))?;

        debug!(
            server_version = %backend_handshake.server_version,
            "Received backend handshake"
        );

        // Compute auth response
        let backend_auth_data = backend_handshake.auth_plugin_data();
        let auth_response = compute_auth_response(&config.password, &backend_auth_data);

        // Determine database to use
        let db = database.clone().or_else(|| config.database.clone());

        // Build handshake response
        let mut caps = capabilities::DEFAULT_CAPABILITIES & backend_handshake.capability_flags;
        if db.is_some() {
            caps |= capabilities::CLIENT_CONNECT_WITH_DB;
        }

        let backend_response = HandshakeResponse {
            capability_flags: caps,
            max_packet_size: 16 * 1024 * 1024,
            character_set: 0x21, // utf8_general_ci
            username: config.user.clone(),
            auth_response,
            database: db.clone(),
            auth_plugin_name: backend_handshake.auth_plugin_name.clone(),
        };

        framed.send(backend_response.encode(1)).await
            .map_err(|e| ConnectionError::Io(e.to_string()))?;

        // Receive OK or ERR
        let response = framed
            .next()
            .await
            .ok_or(ConnectionError::Disconnected)?
            .map_err(|e| ConnectionError::Io(e.to_string()))?;

        if is_err_packet(&response.payload) {
            let err = ErrPacket::parse(&response.payload, caps)
                .unwrap_or_else(|| ErrPacket::new(1045, "28000", "Access denied"));
            error!(
                error_code = err.error_code,
                error_message = %err.error_message,
                "Backend authentication failed"
            );
            return Err(ConnectionError::Auth(err.error_message));
        }

        if !is_ok_packet(&response.payload) {
            return Err(ConnectionError::Protocol(
                "Expected OK packet from backend".into(),
            ));
        }

        debug!("Backend authentication successful");

        let now = Instant::now();
        Ok(Self {
            framed,
            state: ConnectionState::Idle,
            created_at: now,
            last_used_at: now,
            capabilities: caps,
            database: db,
        })
    }

    /// Check if connection is healthy by sending a ping
    pub async fn ping(&mut self) -> bool {
        // Send COM_PING
        let ping_packet = Packet::new(0, vec![0x0e]); // COM_PING = 0x0e
        if self.framed.send(ping_packet).await.is_err() {
            self.state = ConnectionState::Closed;
            return false;
        }

        // Receive response
        match self.framed.next().await {
            Some(Ok(packet)) => {
                if is_ok_packet(&packet.payload) {
                    self.last_used_at = Instant::now();
                    true
                } else {
                    self.state = ConnectionState::Closed;
                    false
                }
            }
            _ => {
                self.state = ConnectionState::Closed;
                false
            }
        }
    }

    /// Reset connection state (send COM_RESET_CONNECTION)
    pub async fn reset(&mut self) -> bool {
        // Send COM_RESET_CONNECTION
        let reset_packet = Packet::new(0, vec![0x1f]); // COM_RESET_CONNECTION = 0x1f
        if self.framed.send(reset_packet).await.is_err() {
            self.state = ConnectionState::Closed;
            return false;
        }

        // Receive response
        match self.framed.next().await {
            Some(Ok(packet)) => {
                if is_ok_packet(&packet.payload) {
                    self.last_used_at = Instant::now();
                    true
                } else {
                    self.state = ConnectionState::Closed;
                    false
                }
            }
            _ => {
                self.state = ConnectionState::Closed;
                false
            }
        }
    }

    /// Change current database
    pub async fn change_database(&mut self, db: &str) -> Result<(), ConnectionError> {
        // Send COM_INIT_DB
        let mut payload = vec![0x02]; // COM_INIT_DB = 0x02
        payload.extend_from_slice(db.as_bytes());
        let packet = Packet::new(0, payload);

        self.framed.send(packet).await
            .map_err(|e| ConnectionError::Io(e.to_string()))?;

        // Receive response
        let response = self.framed
            .next()
            .await
            .ok_or(ConnectionError::Disconnected)?
            .map_err(|e| ConnectionError::Io(e.to_string()))?;

        if is_err_packet(&response.payload) {
            let err = ErrPacket::parse(&response.payload, self.capabilities)
                .unwrap_or_else(|| ErrPacket::new(1049, "42000", "Unknown database"));
            return Err(ConnectionError::Database(err.error_message));
        }

        self.database = Some(db.to_string());
        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Check if connection has exceeded max age
    pub fn is_expired(&self, max_age: Duration) -> bool {
        self.created_at.elapsed() > max_age
    }

    /// Check if connection has been idle too long
    pub fn is_idle_too_long(&self, max_idle: Duration) -> bool {
        self.last_used_at.elapsed() > max_idle
    }

    /// Mark connection as in use
    pub fn acquire(&mut self) {
        self.state = ConnectionState::InUse;
        self.last_used_at = Instant::now();
    }

    /// Mark connection as available
    pub fn release(&mut self) {
        self.state = ConnectionState::Idle;
        self.last_used_at = Instant::now();
    }

    /// Mark connection as closed
    pub fn close(&mut self) {
        self.state = ConnectionState::Closed;
    }

    /// Check if connection is usable
    pub fn is_usable(&self) -> bool {
        self.state != ConnectionState::Closed
    }

    /// Get backend capability flags
    pub fn capabilities(&self) -> u32 {
        self.capabilities
    }

    /// Send a packet to the backend
    pub async fn send(&mut self, packet: Packet) -> Result<(), ConnectionError> {
        match self.framed.send(packet).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Mark connection as closed on send error
                self.state = ConnectionState::Closed;
                Err(ConnectionError::Io(e.to_string()))
            }
        }
    }

    /// Receive a packet from the backend
    pub async fn recv(&mut self) -> Result<Packet, ConnectionError> {
        match self.framed.next().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(e)) => {
                // Mark connection as closed on receive error
                self.state = ConnectionState::Closed;
                Err(ConnectionError::Io(e.to_string()))
            }
            None => {
                // Mark connection as closed on EOF
                self.state = ConnectionState::Closed;
                Err(ConnectionError::Disconnected)
            }
        }
    }

    /// Get inner framed connection for direct access
    pub fn inner(&mut self) -> &mut Framed<TcpStream, PacketCodec> {
        &mut self.framed
    }
}

/// Connection errors
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Connection failed: {0}")]
    Connect(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Connection disconnected")]
    Disconnected,
}
