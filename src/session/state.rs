use std::collections::HashMap;

use crate::pool::DbGroupId;

/// Session state tracking
#[derive(Debug, Clone, Default)]
pub struct SessionState {
    /// Client username
    pub username: String,
    /// Current database
    pub database: Option<String>,
    /// Whether client is in a transaction
    pub in_transaction: bool,
    /// Target db group bound to current transaction
    pub transaction_target: Option<DbGroupId>,
    /// Client capability flags
    pub capability_flags: u32,
    /// Character set
    pub character_set: u8,
    /// Intercepted session variables (not sent to backend)
    session_vars: HashMap<String, String>,
}

impl SessionState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update state after parsing handshake response
    pub fn set_from_handshake(&mut self, username: String, database: Option<String>, capabilities: u32, charset: u8) {
        self.username = username;
        self.database = database;
        self.capability_flags = capabilities;
        self.character_set = charset;
    }

    /// Start a transaction
    pub fn begin_transaction(&mut self) {
        self.in_transaction = true;
        self.transaction_target = None;
    }

    /// Bind transaction to a specific db group
    pub fn bind_transaction(&mut self, target: DbGroupId) {
        self.transaction_target = Some(target);
    }

    /// End a transaction
    pub fn end_transaction(&mut self) {
        self.in_transaction = false;
        self.transaction_target = None;
    }

    /// Set a session variable (intercepted, not sent to backend)
    pub fn set_session_var(&mut self, name: String, value: String) {
        self.session_vars.insert(name.to_lowercase(), value);
    }

}
