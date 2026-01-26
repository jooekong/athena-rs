use std::collections::HashMap;

/// Session state tracking
#[derive(Debug, Clone, Default)]
pub struct SessionState {
    /// Client username
    pub username: String,
    /// Current database
    pub database: Option<String>,
    /// Whether client is in a transaction
    pub in_transaction: bool,
    /// Shard bound to current transaction (if any)
    pub transaction_shard: Option<String>,
    /// Client capability flags
    pub capability_flags: u32,
    /// Character set
    pub character_set: u8,
    /// Intercepted session variables (not sent to backend)
    ///
    /// These are SET commands that are intercepted to prevent connection pollution.
    /// The proxy returns OK to the client but doesn't forward to the backend.
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
        self.transaction_shard = None; // Will be bound on first query
    }

    /// Bind transaction to a specific shard
    pub fn bind_transaction_shard(&mut self, shard: String) {
        self.transaction_shard = Some(shard);
    }

    /// End a transaction
    pub fn end_transaction(&mut self) {
        self.in_transaction = false;
        self.transaction_shard = None;
    }

    /// Change current database
    pub fn change_database(&mut self, db: String) {
        self.database = Some(db);
    }

    /// Set a session variable (intercepted, not sent to backend)
    pub fn set_session_var(&mut self, name: String, value: String) {
        self.session_vars.insert(name.to_lowercase(), value);
    }

    /// Get a session variable
    pub fn get_session_var(&self, name: &str) -> Option<&String> {
        self.session_vars.get(&name.to_lowercase())
    }

    /// Check if a session variable is set
    pub fn has_session_var(&self, name: &str) -> bool {
        self.session_vars.contains_key(&name.to_lowercase())
    }
}
