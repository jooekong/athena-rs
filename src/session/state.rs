/// Session state tracking
#[derive(Debug, Clone, Default)]
pub struct SessionState {
    /// Client username
    pub username: String,
    /// Current database
    pub database: Option<String>,
    /// Whether client is in a transaction
    pub in_transaction: bool,
    /// Client capability flags
    pub capability_flags: u32,
    /// Character set
    pub character_set: u8,
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
    }

    /// End a transaction
    pub fn end_transaction(&mut self) {
        self.in_transaction = false;
    }

    /// Change current database
    pub fn change_database(&mut self, db: String) {
        self.database = Some(db);
    }
}
