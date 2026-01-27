//! Master/Slave role detection for MySQL instances
//!
//! Detects whether a MySQL instance is a master or slave by:
//! 1. Checking @@read_only variable (master should have read_only=0)
//! 2. Checking SHOW SLAVE STATUS (slave will have non-empty result)

use tracing::debug;

use crate::config::DBInstanceRole;
use crate::pool::PooledConnection;
use crate::protocol::{is_err_packet, is_ok_packet, Packet};

/// Error during master detection
#[derive(Debug, thiserror::Error)]
pub enum DetectError {
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Query error: {0}")]
    Query(String),
    #[error("Parse error: {0}")]
    Parse(String),
}

/// Detector for MySQL master/slave role
pub struct MasterDetector;

impl MasterDetector {
    /// Detect the role of a MySQL instance
    ///
    /// Returns Master if:
    /// - @@read_only = 0 AND
    /// - SHOW SLAVE STATUS returns empty result
    ///
    /// Returns Slave otherwise.
    pub async fn detect_role(conn: &mut PooledConnection) -> Result<DBInstanceRole, DetectError> {
        // Check read_only first (faster)
        let read_only = Self::query_read_only(conn).await?;
        if read_only {
            debug!("Instance is read_only=1, detected as Slave");
            return Ok(DBInstanceRole::Slave);
        }

        // Check slave status
        let is_slave = Self::has_slave_status(conn).await?;
        if is_slave {
            debug!("Instance has slave status, detected as Slave");
            return Ok(DBInstanceRole::Slave);
        }

        debug!("Instance is read_only=0 and no slave status, detected as Master");
        Ok(DBInstanceRole::Master)
    }

    /// Combined ping and role detection
    ///
    /// Uses `SELECT 1, @@read_only` to verify connection and get read_only status
    /// in a single query, reducing RTT.
    ///
    /// Returns Master if:
    /// - @@read_only = 0 AND
    /// - SHOW SLAVE STATUS returns empty result
    ///
    /// Returns Slave otherwise.
    pub async fn ping_and_detect_role(
        conn: &mut PooledConnection,
    ) -> Result<DBInstanceRole, DetectError> {
        // Combined query: SELECT 1, @@read_only
        let read_only = Self::query_ping_with_read_only(conn).await?;

        if read_only {
            debug!("Instance is read_only=1, detected as Slave");
            return Ok(DBInstanceRole::Slave);
        }

        // Check slave status only if read_only=0
        let is_slave = Self::has_slave_status(conn).await?;
        if is_slave {
            debug!("Instance has slave status, detected as Slave");
            return Ok(DBInstanceRole::Slave);
        }

        debug!("Instance is read_only=0 and no slave status, detected as Master");
        Ok(DBInstanceRole::Master)
    }

    /// Check if this instance is a master
    pub async fn is_master(conn: &mut PooledConnection) -> Result<bool, DetectError> {
        let role = Self::detect_role(conn).await?;
        Ok(role == DBInstanceRole::Master)
    }

    /// Combined ping and read_only query
    ///
    /// Returns true if read_only=1 (slave), false if read_only=0 (master)
    async fn query_ping_with_read_only(conn: &mut PooledConnection) -> Result<bool, DetectError> {
        let sql = "SELECT 1, @@read_only";
        let result = Self::query_two_values(conn, sql).await?;

        // First value is "1" (ping), second is read_only
        match result.1.as_str() {
            "0" => Ok(false),
            "1" => Ok(true),
            other => {
                debug!(value = %other, "Unexpected read_only value, assuming slave");
                Ok(true)
            }
        }
    }

    /// Query @@read_only variable
    ///
    /// Returns true if read_only=1 (slave), false if read_only=0 (master)
    async fn query_read_only(conn: &mut PooledConnection) -> Result<bool, DetectError> {
        let sql = "SELECT @@read_only";
        let result = Self::query_single_value(conn, sql).await?;

        // Parse result: "0" or "1"
        match result.as_str() {
            "0" => Ok(false),
            "1" => Ok(true),
            other => {
                debug!(value = %other, "Unexpected read_only value, assuming slave");
                Ok(true)
            }
        }
    }

    /// Check if SHOW SLAVE STATUS returns any rows
    ///
    /// If it returns rows, the instance is a slave.
    async fn has_slave_status(conn: &mut PooledConnection) -> Result<bool, DetectError> {
        let sql = "SHOW SLAVE STATUS";
        let has_rows = Self::query_has_rows(conn, sql).await?;
        Ok(has_rows)
    }

    /// Execute a query and return the first two columns of the first row
    async fn query_two_values(
        conn: &mut PooledConnection,
        sql: &str,
    ) -> Result<(String, String), DetectError> {
        // Send query
        let mut payload = vec![0x03]; // COM_QUERY
        payload.extend_from_slice(sql.as_bytes());
        let packet = Packet::new(0, payload);

        conn.send(packet)
            .await
            .map_err(|e| DetectError::Connection(e.to_string()))?;

        // Read first response packet (column count)
        let first = conn
            .recv()
            .await
            .map_err(|_| DetectError::Connection("Disconnected".into()))?;

        if is_err_packet(&first.payload) {
            return Err(DetectError::Query(format!(
                "Query failed: {}",
                String::from_utf8_lossy(&first.payload[9..])
            )));
        }

        if is_ok_packet(&first.payload) {
            return Err(DetectError::Parse("Expected result set, got OK".into()));
        }

        // Skip 2 column definitions
        for _ in 0..2 {
            conn.recv()
                .await
                .map_err(|_| DetectError::Connection("Disconnected".into()))?;
        }

        // Read EOF or row
        let eof_or_row = conn
            .recv()
            .await
            .map_err(|_| DetectError::Connection("Disconnected".into()))?;

        let is_eof = eof_or_row.payload.first() == Some(&0xFE) && eof_or_row.payload.len() < 9;

        let row_packet = if is_eof {
            conn.recv()
                .await
                .map_err(|_| DetectError::Connection("Disconnected".into()))?
        } else {
            eof_or_row
        };

        // Parse two length-encoded strings from row
        let (val1, offset) = Self::parse_length_encoded_string_with_offset(&row_packet.payload)?;
        let (val2, _) = Self::parse_length_encoded_string_with_offset(&row_packet.payload[offset..])?;

        // Drain remaining packets
        Self::drain_result_set(conn).await?;

        Ok((val1, val2))
    }

    /// Execute a query and return the first column of the first row
    async fn query_single_value(
        conn: &mut PooledConnection,
        sql: &str,
    ) -> Result<String, DetectError> {
        // Send query
        let mut payload = vec![0x03]; // COM_QUERY
        payload.extend_from_slice(sql.as_bytes());
        let packet = Packet::new(0, payload);

        conn.send(packet)
            .await
            .map_err(|e| DetectError::Connection(e.to_string()))?;

        // Read first response packet
        let first = conn
            .recv()
            .await
            .map_err(|_| DetectError::Connection("Disconnected".into()))?;

        if is_err_packet(&first.payload) {
            return Err(DetectError::Query(format!(
                "Query failed: {}",
                String::from_utf8_lossy(&first.payload[9..])
            )));
        }

        if is_ok_packet(&first.payload) {
            // No result set (shouldn't happen for SELECT)
            return Err(DetectError::Parse("Expected result set, got OK".into()));
        }

        // First packet is column count, skip it
        // Read column definition
        let _col_def = conn
            .recv()
            .await
            .map_err(|_| DetectError::Connection("Disconnected".into()))?;

        // Read EOF (or next packet in DEPRECATE_EOF mode)
        let eof_or_row = conn
            .recv()
            .await
            .map_err(|_| DetectError::Connection("Disconnected".into()))?;

        // Check if this is EOF (0xFE with small payload) or a row
        let is_eof = eof_or_row.payload.first() == Some(&0xFE) && eof_or_row.payload.len() < 9;

        let row_packet = if is_eof {
            // Read actual row
            conn.recv()
                .await
                .map_err(|_| DetectError::Connection("Disconnected".into()))?
        } else {
            // This was the row
            eof_or_row
        };

        // Parse row value (length-encoded string)
        let value = Self::parse_length_encoded_string(&row_packet.payload)?;

        // Consume remaining packets until EOF
        Self::drain_result_set(conn).await?;

        Ok(value)
    }

    /// Execute a query and check if it returns any rows
    async fn query_has_rows(conn: &mut PooledConnection, sql: &str) -> Result<bool, DetectError> {
        // Send query
        let mut payload = vec![0x03]; // COM_QUERY
        payload.extend_from_slice(sql.as_bytes());
        let packet = Packet::new(0, payload);

        conn.send(packet)
            .await
            .map_err(|e| DetectError::Connection(e.to_string()))?;

        // Read first response packet
        let first = conn
            .recv()
            .await
            .map_err(|_| DetectError::Connection("Disconnected".into()))?;

        if is_err_packet(&first.payload) {
            // SHOW SLAVE STATUS may fail if no replication privilege
            // Treat as "no slave status" (master)
            debug!("SHOW SLAVE STATUS failed, assuming no slave status");
            return Ok(false);
        }

        if is_ok_packet(&first.payload) {
            // OK packet means no result set (empty)
            return Ok(false);
        }

        // Has result set, need to check if there are actual rows
        // First packet is column count
        let col_count = Self::parse_length_encoded_int(&first.payload).unwrap_or(0);
        if col_count == 0 {
            return Ok(false);
        }

        // Skip column definitions
        for _ in 0..col_count {
            conn.recv()
                .await
                .map_err(|_| DetectError::Connection("Disconnected".into()))?;
        }

        // Read EOF after columns (in non-DEPRECATE_EOF mode)
        let next = conn
            .recv()
            .await
            .map_err(|_| DetectError::Connection("Disconnected".into()))?;

        // Check if this is EOF or a row
        let is_eof = next.payload.first() == Some(&0xFE) && next.payload.len() < 9;

        if is_eof {
            // Read next packet - could be row or final EOF
            let row_or_eof = conn
                .recv()
                .await
                .map_err(|_| DetectError::Connection("Disconnected".into()))?;

            let is_final_eof =
                row_or_eof.payload.first() == Some(&0xFE) && row_or_eof.payload.len() < 9;

            if is_final_eof {
                // No rows
                return Ok(false);
            }

            // Has at least one row
            Self::drain_result_set(conn).await?;
            return Ok(true);
        }

        // The packet was a row (DEPRECATE_EOF mode)
        Self::drain_result_set(conn).await?;
        Ok(true)
    }

    /// Drain remaining packets from a result set
    async fn drain_result_set(conn: &mut PooledConnection) -> Result<(), DetectError> {
        loop {
            let packet = conn
                .recv()
                .await
                .map_err(|_| DetectError::Connection("Disconnected".into()))?;

            // Check for EOF or OK (end of result set)
            let is_eof = packet.payload.first() == Some(&0xFE) && packet.payload.len() < 9;
            let is_ok = is_ok_packet(&packet.payload);
            let is_err = is_err_packet(&packet.payload);

            if is_eof || is_ok || is_err {
                break;
            }
        }
        Ok(())
    }

    /// Parse a length-encoded string from packet payload
    fn parse_length_encoded_string(data: &[u8]) -> Result<String, DetectError> {
        Self::parse_length_encoded_string_with_offset(data).map(|(s, _)| s)
    }

    /// Parse a length-encoded string and return the total bytes consumed
    fn parse_length_encoded_string_with_offset(data: &[u8]) -> Result<(String, usize), DetectError> {
        if data.is_empty() {
            return Err(DetectError::Parse("Empty payload".into()));
        }

        let (len, header_size) = match data[0] {
            0..=0xFA => (data[0] as usize, 1),
            0xFC if data.len() >= 3 => {
                (u16::from_le_bytes([data[1], data[2]]) as usize, 3)
            }
            0xFD if data.len() >= 4 => {
                (u32::from_le_bytes([data[1], data[2], data[3], 0]) as usize, 4)
            }
            0xFE if data.len() >= 9 => {
                (
                    u64::from_le_bytes([
                        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
                    ]) as usize,
                    9,
                )
            }
            0xFB => return Ok((String::new(), 1)), // NULL
            _ => return Err(DetectError::Parse("Invalid length encoding".into())),
        };

        if data.len() < header_size + len {
            return Err(DetectError::Parse("Truncated string".into()));
        }

        let s = String::from_utf8(data[header_size..header_size + len].to_vec())
            .map_err(|_| DetectError::Parse("Invalid UTF-8".into()))?;

        Ok((s, header_size + len))
    }

    /// Parse a length-encoded integer
    fn parse_length_encoded_int(data: &[u8]) -> Option<u64> {
        if data.is_empty() {
            return None;
        }
        match data[0] {
            0..=0xFA => Some(data[0] as u64),
            0xFC if data.len() >= 3 => Some(u16::from_le_bytes([data[1], data[2]]) as u64),
            0xFD if data.len() >= 4 => {
                Some(u32::from_le_bytes([data[1], data[2], data[3], 0]) as u64)
            }
            0xFE if data.len() >= 9 => Some(u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ])),
            _ => None,
        }
    }
}
