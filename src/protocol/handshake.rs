use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha1::{Digest, Sha1};

use super::packet::{capabilities::*, Packet};

/// MySQL initial handshake packet (server -> client)
#[derive(Debug, Clone)]
pub struct InitialHandshake {
    pub protocol_version: u8,
    pub server_version: String,
    pub connection_id: u32,
    pub auth_plugin_data_part1: [u8; 8],
    pub capability_flags: u32,
    pub character_set: u8,
    pub status_flags: u16,
    pub auth_plugin_data_part2: Vec<u8>,
    pub auth_plugin_name: String,
}

impl InitialHandshake {
    /// Create a new handshake packet for proxy
    pub fn new(connection_id: u32) -> Self {
        let mut auth_data1 = [0u8; 8];
        let mut auth_data2 = vec![0u8; 12];

        // Generate random auth data
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut auth_data1);
        rng.fill_bytes(&mut auth_data2);

        Self {
            protocol_version: 10,
            server_version: "8.0.0-athena-proxy".to_string(),
            connection_id,
            auth_plugin_data_part1: auth_data1,
            capability_flags: DEFAULT_CAPABILITIES,
            character_set: 0x21, // utf8_general_ci
            status_flags: 0x0002,
            auth_plugin_data_part2: auth_data2,
            auth_plugin_name: "mysql_native_password".to_string(),
        }
    }

    /// Get full auth plugin data (20 bytes)
    pub fn auth_plugin_data(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(20);
        data.extend_from_slice(&self.auth_plugin_data_part1);
        data.extend_from_slice(&self.auth_plugin_data_part2);
        data
    }

    /// Encode to packet
    pub fn encode(&self) -> Packet {
        let mut buf = BytesMut::new();

        // Protocol version
        buf.put_u8(self.protocol_version);

        // Server version (null-terminated)
        buf.extend_from_slice(self.server_version.as_bytes());
        buf.put_u8(0);

        // Connection ID
        buf.put_u32_le(self.connection_id);

        // Auth plugin data part 1 (8 bytes)
        buf.extend_from_slice(&self.auth_plugin_data_part1);

        // Filler
        buf.put_u8(0);

        // Capability flags (lower 2 bytes)
        buf.put_u16_le((self.capability_flags & 0xFFFF) as u16);

        // Character set
        buf.put_u8(self.character_set);

        // Status flags
        buf.put_u16_le(self.status_flags);

        // Capability flags (upper 2 bytes)
        buf.put_u16_le(((self.capability_flags >> 16) & 0xFFFF) as u16);

        // Auth plugin data length
        if self.capability_flags & CLIENT_PLUGIN_AUTH != 0 {
            buf.put_u8((self.auth_plugin_data_part1.len() + self.auth_plugin_data_part2.len() + 1) as u8);
        } else {
            buf.put_u8(0);
        }

        // Reserved (10 bytes)
        buf.extend_from_slice(&[0u8; 10]);

        // Auth plugin data part 2
        if self.capability_flags & CLIENT_SECURE_CONNECTION != 0 {
            buf.extend_from_slice(&self.auth_plugin_data_part2);
            buf.put_u8(0); // Null terminator
        }

        // Auth plugin name
        if self.capability_flags & CLIENT_PLUGIN_AUTH != 0 {
            buf.extend_from_slice(self.auth_plugin_name.as_bytes());
            buf.put_u8(0);
        }

        Packet::new(0, buf.freeze())
    }

    /// Parse from packet payload
    pub fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < 32 {
            return None;
        }

        let mut buf = payload;

        let protocol_version = buf.get_u8();

        // Server version (null-terminated string)
        let null_pos = buf.iter().position(|&b| b == 0)?;
        let server_version = String::from_utf8_lossy(&buf[..null_pos]).to_string();
        buf.advance(null_pos + 1);

        let connection_id = buf.get_u32_le();

        let mut auth_plugin_data_part1 = [0u8; 8];
        auth_plugin_data_part1.copy_from_slice(&buf[..8]);
        buf.advance(8);

        // Filler
        buf.advance(1);

        let capability_flags_lower = buf.get_u16_le() as u32;
        let character_set = buf.get_u8();
        let status_flags = buf.get_u16_le();
        let capability_flags_upper = buf.get_u16_le() as u32;
        let capability_flags = capability_flags_lower | (capability_flags_upper << 16);

        let auth_plugin_data_len = buf.get_u8();

        // Reserved
        buf.advance(10);

        // Auth plugin data part 2
        let mut auth_plugin_data_part2 = Vec::new();
        if capability_flags & CLIENT_SECURE_CONNECTION != 0 {
            let len = std::cmp::max(13, auth_plugin_data_len as usize - 8);
            let data_len = buf.iter().take(len).position(|&b| b == 0).unwrap_or(len);
            auth_plugin_data_part2.extend_from_slice(&buf[..data_len]);
            buf.advance(len);
        }

        // Auth plugin name
        let auth_plugin_name = if capability_flags & CLIENT_PLUGIN_AUTH != 0 && !buf.is_empty() {
            let null_pos = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            String::from_utf8_lossy(&buf[..null_pos]).to_string()
        } else {
            "mysql_native_password".to_string()
        };

        Some(Self {
            protocol_version,
            server_version,
            connection_id,
            auth_plugin_data_part1,
            capability_flags,
            character_set,
            status_flags,
            auth_plugin_data_part2,
            auth_plugin_name,
        })
    }
}

/// MySQL handshake response packet (client -> server)
#[derive(Debug, Clone)]
pub struct HandshakeResponse {
    pub capability_flags: u32,
    pub max_packet_size: u32,
    pub character_set: u8,
    pub username: String,
    pub auth_response: Vec<u8>,
    pub database: Option<String>,
    pub auth_plugin_name: String,
}

impl HandshakeResponse {
    /// Parse from packet payload
    pub fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < 32 {
            return None;
        }

        let mut buf = payload;

        let capability_flags = buf.get_u32_le();
        let max_packet_size = buf.get_u32_le();
        let character_set = buf.get_u8();

        // Reserved (23 bytes)
        buf.advance(23);

        // Username (null-terminated)
        let null_pos = buf.iter().position(|&b| b == 0)?;
        let username = String::from_utf8_lossy(&buf[..null_pos]).to_string();
        buf.advance(null_pos + 1);

        // Auth response
        let auth_response = if capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
            // Length-encoded
            let len = buf.get_u8() as usize;
            let data = buf[..len].to_vec();
            buf.advance(len);
            data
        } else if capability_flags & CLIENT_SECURE_CONNECTION != 0 {
            let len = buf.get_u8() as usize;
            let data = buf[..len].to_vec();
            buf.advance(len);
            data
        } else {
            // Null-terminated
            let null_pos = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            let data = buf[..null_pos].to_vec();
            buf.advance(null_pos + 1);
            data
        };

        // Database
        let database = if capability_flags & CLIENT_CONNECT_WITH_DB != 0 && !buf.is_empty() {
            let null_pos = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            let db = String::from_utf8_lossy(&buf[..null_pos]).to_string();
            buf.advance(null_pos + 1);
            if db.is_empty() {
                None
            } else {
                Some(db)
            }
        } else {
            None
        };

        // Auth plugin name
        let auth_plugin_name = if capability_flags & CLIENT_PLUGIN_AUTH != 0 && !buf.is_empty() {
            let null_pos = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            String::from_utf8_lossy(&buf[..null_pos]).to_string()
        } else {
            "mysql_native_password".to_string()
        };

        Some(Self {
            capability_flags,
            max_packet_size,
            character_set,
            username,
            auth_response,
            database,
            auth_plugin_name,
        })
    }

    /// Encode to packet
    pub fn encode(&self, sequence_id: u8) -> Packet {
        let mut buf = BytesMut::new();

        buf.put_u32_le(self.capability_flags);
        buf.put_u32_le(self.max_packet_size);
        buf.put_u8(self.character_set);

        // Reserved (23 bytes)
        buf.extend_from_slice(&[0u8; 23]);

        // Username
        buf.extend_from_slice(self.username.as_bytes());
        buf.put_u8(0);

        // Auth response (length-prefixed)
        if self.capability_flags & CLIENT_SECURE_CONNECTION != 0 {
            buf.put_u8(self.auth_response.len() as u8);
            buf.extend_from_slice(&self.auth_response);
        } else {
            buf.extend_from_slice(&self.auth_response);
            buf.put_u8(0);
        }

        // Database
        if self.capability_flags & CLIENT_CONNECT_WITH_DB != 0 {
            if let Some(ref db) = self.database {
                buf.extend_from_slice(db.as_bytes());
            }
            buf.put_u8(0);
        }

        // Auth plugin name
        if self.capability_flags & CLIENT_PLUGIN_AUTH != 0 {
            buf.extend_from_slice(self.auth_plugin_name.as_bytes());
            buf.put_u8(0);
        }

        Packet::new(sequence_id, buf.freeze())
    }
}

/// Compute mysql_native_password auth response
pub fn compute_auth_response(password: &str, auth_data: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    // SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let hash1 = hasher.finalize();

    // SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(&hash1);
    let hash2 = hasher.finalize();

    // SHA1(auth_data + SHA1(SHA1(password)))
    let mut hasher = Sha1::new();
    hasher.update(auth_data);
    hasher.update(&hash2);
    let hash3 = hasher.finalize();

    // XOR SHA1(password) with SHA1(auth_data + SHA1(SHA1(password)))
    hash1
        .iter()
        .zip(hash3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// OK packet
#[derive(Debug, Clone)]
pub struct OkPacket {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: u16,
    pub warnings: u16,
}

impl OkPacket {
    pub fn new() -> Self {
        Self {
            affected_rows: 0,
            last_insert_id: 0,
            status_flags: 0x0002, // SERVER_STATUS_AUTOCOMMIT
            warnings: 0,
        }
    }

    pub fn encode(&self, sequence_id: u8, capabilities: u32) -> Packet {
        let mut buf = BytesMut::new();

        // OK header
        buf.put_u8(0x00);

        // Affected rows (length-encoded int)
        encode_length_encoded_int(&mut buf, self.affected_rows);

        // Last insert id (length-encoded int)
        encode_length_encoded_int(&mut buf, self.last_insert_id);

        if capabilities & CLIENT_PROTOCOL_41 != 0 {
            buf.put_u16_le(self.status_flags);
            buf.put_u16_le(self.warnings);
        }

        Packet::new(sequence_id, buf.freeze())
    }
}

impl Default for OkPacket {
    fn default() -> Self {
        Self::new()
    }
}

/// ERR packet
#[derive(Debug, Clone)]
pub struct ErrPacket {
    pub error_code: u16,
    pub sql_state: String,
    pub error_message: String,
}

impl ErrPacket {
    pub fn new(error_code: u16, sql_state: &str, error_message: &str) -> Self {
        Self {
            error_code,
            sql_state: sql_state.to_string(),
            error_message: error_message.to_string(),
        }
    }

    pub fn encode(&self, sequence_id: u8, capabilities: u32) -> Packet {
        let mut buf = BytesMut::new();

        // ERR header
        buf.put_u8(0xFF);
        buf.put_u16_le(self.error_code);

        if capabilities & CLIENT_PROTOCOL_41 != 0 {
            buf.put_u8(b'#');
            buf.extend_from_slice(self.sql_state.as_bytes());
        }

        buf.extend_from_slice(self.error_message.as_bytes());

        Packet::new(sequence_id, buf.freeze())
    }

    /// Parse from packet payload
    pub fn parse(payload: &[u8], capabilities: u32) -> Option<Self> {
        if payload.is_empty() || payload[0] != 0xFF {
            return None;
        }

        let mut buf = &payload[1..];
        if buf.len() < 2 {
            return None;
        }

        let error_code = buf.get_u16_le();

        let (sql_state, error_message) = if capabilities & CLIENT_PROTOCOL_41 != 0 && !buf.is_empty() && buf[0] == b'#' {
            buf.advance(1);
            if buf.len() >= 5 {
                let sql_state = String::from_utf8_lossy(&buf[..5]).to_string();
                buf.advance(5);
                let error_message = String::from_utf8_lossy(buf).to_string();
                (sql_state, error_message)
            } else {
                ("HY000".to_string(), String::from_utf8_lossy(buf).to_string())
            }
        } else {
            ("HY000".to_string(), String::from_utf8_lossy(buf).to_string())
        };

        Some(Self {
            error_code,
            sql_state,
            error_message,
        })
    }
}

/// Encode a length-encoded integer
fn encode_length_encoded_int(buf: &mut BytesMut, value: u64) {
    if value < 251 {
        buf.put_u8(value as u8);
    } else if value < 65536 {
        buf.put_u8(0xFC);
        buf.put_u16_le(value as u16);
    } else if value < 16777216 {
        buf.put_u8(0xFD);
        buf.put_u8((value & 0xFF) as u8);
        buf.put_u8(((value >> 8) & 0xFF) as u8);
        buf.put_u8(((value >> 16) & 0xFF) as u8);
    } else {
        buf.put_u8(0xFE);
        buf.put_u64_le(value);
    }
}

/// Check if packet is OK packet
pub fn is_ok_packet(payload: &Bytes) -> bool {
    !payload.is_empty() && payload[0] == 0x00
}

/// Check if packet is ERR packet
pub fn is_err_packet(payload: &Bytes) -> bool {
    !payload.is_empty() && payload[0] == 0xFF
}

/// Check if packet is EOF packet
pub fn is_eof_packet(payload: &Bytes, capabilities: u32) -> bool {
    if capabilities & CLIENT_DEPRECATE_EOF != 0 {
        false
    } else {
        !payload.is_empty() && payload[0] == 0xFE && payload.len() < 9
    }
}
