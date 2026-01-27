use bytes::{Buf, BufMut, Bytes, BytesMut};

/// MySQL packet header size: 3 bytes length + 1 byte sequence
pub const PACKET_HEADER_SIZE: usize = 4;
/// Maximum packet payload size (16MB - 1)
pub const MAX_PACKET_SIZE: usize = 0xFF_FF_FF;

/// MySQL wire protocol packet
#[derive(Debug, Clone)]
pub struct Packet {
    pub sequence_id: u8,
    pub payload: Bytes,
}

impl Packet {
    pub fn new(sequence_id: u8, payload: impl Into<Bytes>) -> Self {
        Self {
            sequence_id,
            payload: payload.into(),
        }
    }

    /// Encode packet to bytes (header + payload)
    pub fn encode(&self, dst: &mut BytesMut) {
        let len = self.payload.len();
        // 3 bytes for length (little endian)
        dst.put_u8((len & 0xFF) as u8);
        dst.put_u8(((len >> 8) & 0xFF) as u8);
        dst.put_u8(((len >> 16) & 0xFF) as u8);
        // 1 byte for sequence id
        dst.put_u8(self.sequence_id);
        // Payload
        dst.extend_from_slice(&self.payload);
    }

    /// Try to decode packet from bytes, returns None if not enough data
    pub fn decode(src: &mut BytesMut) -> Option<Self> {
        if src.len() < PACKET_HEADER_SIZE {
            return None;
        }

        // Read length (3 bytes, little endian)
        let len = src[0] as usize | ((src[1] as usize) << 8) | ((src[2] as usize) << 16);

        let total_len = PACKET_HEADER_SIZE + len;
        if src.len() < total_len {
            return None;
        }

        // Read sequence id
        let sequence_id = src[3];

        // Advance past header
        src.advance(PACKET_HEADER_SIZE);

        // Read payload
        let payload = src.split_to(len).freeze();

        Some(Self {
            sequence_id,
            payload,
        })
    }
}

/// MySQL capability flags
#[allow(dead_code)]
pub mod capabilities {
    pub const CLIENT_LONG_PASSWORD: u32 = 1;
    pub const CLIENT_FOUND_ROWS: u32 = 1 << 1;
    pub const CLIENT_LONG_FLAG: u32 = 1 << 2;
    pub const CLIENT_CONNECT_WITH_DB: u32 = 1 << 3;
    pub const CLIENT_NO_SCHEMA: u32 = 1 << 4;
    pub const CLIENT_COMPRESS: u32 = 1 << 5;
    pub const CLIENT_ODBC: u32 = 1 << 6;
    pub const CLIENT_LOCAL_FILES: u32 = 1 << 7;
    pub const CLIENT_IGNORE_SPACE: u32 = 1 << 8;
    pub const CLIENT_PROTOCOL_41: u32 = 1 << 9;
    pub const CLIENT_INTERACTIVE: u32 = 1 << 10;
    pub const CLIENT_SSL: u32 = 1 << 11;
    pub const CLIENT_IGNORE_SIGPIPE: u32 = 1 << 13;
    pub const CLIENT_TRANSACTIONS: u32 = 1 << 14;
    pub const CLIENT_RESERVED: u32 = 1 << 15;
    pub const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
    pub const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
    pub const CLIENT_MULTI_RESULTS: u32 = 1 << 17;
    pub const CLIENT_PS_MULTI_RESULTS: u32 = 1 << 18;
    pub const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
    pub const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
    pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
    pub const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

    /// Default capabilities for proxy
    ///
    /// Note: CLIENT_MULTI_STATEMENTS is intentionally NOT included because
    /// our SQL analyzer only processes the first statement. This prevents
    /// clients from sending multi-statement queries.
    ///
    /// Note: CLIENT_DEPRECATE_EOF is intentionally NOT included because some
    /// MySQL backends advertise support but don't actually implement it properly,
    /// still sending EOF packets despite negotiating deprecation.
    pub const DEFAULT_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
        | CLIENT_FOUND_ROWS
        | CLIENT_LONG_FLAG
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_MULTI_RESULTS
        | CLIENT_PLUGIN_AUTH;
}

/// MySQL command types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Command {
    Sleep = 0x00,
    Quit = 0x01,
    InitDb = 0x02,
    Query = 0x03,
    FieldList = 0x04,
    CreateDb = 0x05,
    DropDb = 0x06,
    Refresh = 0x07,
    Shutdown = 0x08,
    Statistics = 0x09,
    ProcessInfo = 0x0a,
    Connect = 0x0b,
    ProcessKill = 0x0c,
    Debug = 0x0d,
    Ping = 0x0e,
    Time = 0x0f,
    DelayedInsert = 0x10,
    ChangeUser = 0x11,
    BinlogDump = 0x12,
    TableDump = 0x13,
    ConnectOut = 0x14,
    RegisterSlave = 0x15,
    StmtPrepare = 0x16,
    StmtExecute = 0x17,
    StmtSendLongData = 0x18,
    StmtClose = 0x19,
    StmtReset = 0x1a,
    SetOption = 0x1b,
    StmtFetch = 0x1c,
    Daemon = 0x1d,
    BinlogDumpGtid = 0x1e,
    ResetConnection = 0x1f,
    Unknown = 0xff,
}

impl From<u8> for Command {
    fn from(value: u8) -> Self {
        match value {
            0x00 => Command::Sleep,
            0x01 => Command::Quit,
            0x02 => Command::InitDb,
            0x03 => Command::Query,
            0x04 => Command::FieldList,
            0x05 => Command::CreateDb,
            0x06 => Command::DropDb,
            0x07 => Command::Refresh,
            0x08 => Command::Shutdown,
            0x09 => Command::Statistics,
            0x0a => Command::ProcessInfo,
            0x0b => Command::Connect,
            0x0c => Command::ProcessKill,
            0x0d => Command::Debug,
            0x0e => Command::Ping,
            0x0f => Command::Time,
            0x10 => Command::DelayedInsert,
            0x11 => Command::ChangeUser,
            0x12 => Command::BinlogDump,
            0x13 => Command::TableDump,
            0x14 => Command::ConnectOut,
            0x15 => Command::RegisterSlave,
            0x16 => Command::StmtPrepare,
            0x17 => Command::StmtExecute,
            0x18 => Command::StmtSendLongData,
            0x19 => Command::StmtClose,
            0x1a => Command::StmtReset,
            0x1b => Command::SetOption,
            0x1c => Command::StmtFetch,
            0x1d => Command::Daemon,
            0x1e => Command::BinlogDumpGtid,
            0x1f => Command::ResetConnection,
            _ => Command::Unknown,
        }
    }
}
