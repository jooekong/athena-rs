use bytes::Bytes;

use super::packet::Command;

/// Parsed command from client
#[derive(Debug, Clone)]
pub enum ClientCommand {
    Query(String),
    InitDb(String),
    Quit,
    Ping,
    FieldList { table: String, wildcard: String },
    Unknown(u8, Bytes),
}

impl ClientCommand {
    /// Parse command from packet payload
    pub fn parse(payload: &Bytes) -> Self {
        if payload.is_empty() {
            return ClientCommand::Unknown(0, Bytes::new());
        }

        let cmd = Command::from(payload[0]);
        let data = payload.slice(1..);

        match cmd {
            Command::Query => {
                let sql = String::from_utf8_lossy(&data).to_string();
                ClientCommand::Query(sql)
            }
            Command::InitDb => {
                let db = String::from_utf8_lossy(&data).to_string();
                ClientCommand::InitDb(db)
            }
            Command::Quit => ClientCommand::Quit,
            Command::Ping => ClientCommand::Ping,
            Command::FieldList => {
                // Table name is null-terminated, followed by optional wildcard
                let null_pos = data.iter().position(|&b| b == 0).unwrap_or(data.len());
                let table = String::from_utf8_lossy(&data[..null_pos]).to_string();
                let wildcard = if null_pos + 1 < data.len() {
                    String::from_utf8_lossy(&data[null_pos + 1..]).to_string()
                } else {
                    String::new()
                };
                ClientCommand::FieldList { table, wildcard }
            }
            _ => ClientCommand::Unknown(payload[0], data),
        }
    }

    /// Check if this is a read-only query
    pub fn is_read_only(&self) -> bool {
        match self {
            ClientCommand::Query(sql) => {
                let sql_upper = sql.trim().to_uppercase();
                sql_upper.starts_with("SELECT")
                    || sql_upper.starts_with("SHOW")
                    || sql_upper.starts_with("DESCRIBE")
                    || sql_upper.starts_with("DESC")
                    || sql_upper.starts_with("EXPLAIN")
            }
            ClientCommand::Ping => true,
            _ => false,
        }
    }

    /// Check if this is a transaction control statement
    pub fn is_transaction_control(&self) -> bool {
        match self {
            ClientCommand::Query(sql) => {
                let sql_upper = sql.trim().to_uppercase();
                sql_upper.starts_with("BEGIN")
                    || sql_upper.starts_with("START TRANSACTION")
                    || sql_upper.starts_with("COMMIT")
                    || sql_upper.starts_with("ROLLBACK")
                    || sql_upper.starts_with("SET AUTOCOMMIT")
            }
            _ => false,
        }
    }

    /// Check if this starts a transaction
    pub fn starts_transaction(&self) -> bool {
        match self {
            ClientCommand::Query(sql) => {
                let sql_upper = sql.trim().to_uppercase();
                sql_upper.starts_with("BEGIN") || sql_upper.starts_with("START TRANSACTION")
            }
            _ => false,
        }
    }

    /// Check if this ends a transaction
    pub fn ends_transaction(&self) -> bool {
        match self {
            ClientCommand::Query(sql) => {
                let sql_upper = sql.trim().to_uppercase();
                sql_upper.starts_with("COMMIT") || sql_upper.starts_with("ROLLBACK")
            }
            _ => false,
        }
    }
}
