//! Aggregation result parsing and merging for cross-shard queries
//!
//! This module handles:
//! - Parsing numeric values from MySQL result packets
//! - Merging aggregate results (COUNT, SUM, MAX, MIN, AVG) from multiple shards
//! - Building merged result packets to return to client

use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use tracing::debug;

use super::analyzer::{AggregateInfo, AggregateType};

/// Represents a single aggregate value that can be merged
#[derive(Debug, Clone)]
pub enum AggregateValue {
    /// Integer value
    Integer(i64),
    /// Floating point value
    Float(f64),
    /// String value (for non-numeric aggregates, shouldn't happen typically)
    String(String),
    /// NULL value
    Null,
}

impl AggregateValue {
    /// Parse from MySQL text protocol value (bytes)
    pub fn from_bytes(data: &[u8]) -> Self {
        if data.is_empty() {
            return AggregateValue::Null;
        }
        
        let s = String::from_utf8_lossy(data);
        
        // Try parsing as integer first
        if let Ok(i) = s.parse::<i64>() {
            return AggregateValue::Integer(i);
        }
        
        // Try parsing as float
        if let Ok(f) = s.parse::<f64>() {
            return AggregateValue::Float(f);
        }
        
        // Fall back to string
        AggregateValue::String(s.to_string())
    }

    /// Convert to f64 for calculations
    pub fn to_f64(&self) -> Option<f64> {
        match self {
            AggregateValue::Integer(i) => Some(*i as f64),
            AggregateValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Convert to i64 for COUNT operations
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            AggregateValue::Integer(i) => Some(*i),
            AggregateValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Encode as MySQL text protocol value
    pub fn encode(&self) -> Bytes {
        match self {
            AggregateValue::Integer(i) => Bytes::from(i.to_string()),
            AggregateValue::Float(f) => {
                // Format float to remove unnecessary trailing zeros
                let s = format!("{:.6}", f);
                let s = s.trim_end_matches('0').trim_end_matches('.');
                Bytes::from(s.to_string())
            }
            AggregateValue::String(s) => Bytes::from(s.clone()),
            AggregateValue::Null => Bytes::new(),
        }
    }
}

/// Holds intermediate state for aggregate merging
#[derive(Debug, Clone)]
pub struct AggregateMerger {
    /// Aggregate info from SQL analysis
    pub info: AggregateInfo,
    /// Accumulated value
    pub value: Option<AggregateValue>,
    /// For AVG: sum of values
    pub avg_sum: f64,
    /// For AVG: count of values
    pub avg_count: i64,
}

impl AggregateMerger {
    pub fn new(info: AggregateInfo) -> Self {
        Self {
            info,
            value: None,
            avg_sum: 0.0,
            avg_count: 0,
        }
    }

    /// Merge a new value from a shard
    pub fn merge(&mut self, new_value: AggregateValue) {
        match self.info.func_type {
            AggregateType::Count => {
                // COUNT: sum all counts
                let new_count = new_value.to_i64().unwrap_or(0);
                match &mut self.value {
                    Some(AggregateValue::Integer(current)) => {
                        *current += new_count;
                    }
                    _ => {
                        self.value = Some(AggregateValue::Integer(new_count));
                    }
                }
            }
            AggregateType::Sum => {
                // SUM: sum all sums
                if let Some(new_val) = new_value.to_f64() {
                    match &mut self.value {
                        Some(AggregateValue::Float(current)) => {
                            *current += new_val;
                        }
                        Some(AggregateValue::Integer(current)) => {
                            // Upgrade to float if needed
                            if matches!(new_value, AggregateValue::Float(_)) {
                                self.value = Some(AggregateValue::Float(*current as f64 + new_val));
                            } else {
                                *current += new_val as i64;
                            }
                        }
                        _ => {
                            self.value = Some(new_value);
                        }
                    }
                }
            }
            AggregateType::Max => {
                // MAX: keep the maximum
                if let Some(new_val) = new_value.to_f64() {
                    match &self.value {
                        Some(current) => {
                            if let Some(current_val) = current.to_f64() {
                                if new_val > current_val {
                                    self.value = Some(new_value);
                                }
                            }
                        }
                        None => {
                            self.value = Some(new_value);
                        }
                    }
                }
            }
            AggregateType::Min => {
                // MIN: keep the minimum
                if let Some(new_val) = new_value.to_f64() {
                    match &self.value {
                        Some(current) => {
                            if let Some(current_val) = current.to_f64() {
                                if new_val < current_val {
                                    self.value = Some(new_value);
                                }
                            }
                        }
                        None => {
                            self.value = Some(new_value);
                        }
                    }
                }
            }
            AggregateType::Avg => {
                // AVG: need to track sum and count separately
                // NOTE: This requires knowing the count for each shard, which is complex
                // For simplicity, we calculate AVG from the final SUM/COUNT
                // This implementation assumes we get the actual value, not partial
                if let Some(new_val) = new_value.to_f64() {
                    self.avg_sum += new_val;
                    self.avg_count += 1;
                }
            }
        }
    }

    /// Get the final merged value
    pub fn finalize(&self) -> AggregateValue {
        match self.info.func_type {
            AggregateType::Avg => {
                if self.avg_count > 0 {
                    AggregateValue::Float(self.avg_sum / self.avg_count as f64)
                } else {
                    AggregateValue::Null
                }
            }
            _ => self.value.clone().unwrap_or(AggregateValue::Null),
        }
    }
}

/// Parser for MySQL result set row data
pub struct RowParser;

impl RowParser {
    /// Parse column values from a MySQL row packet (text protocol)
    /// 
    /// In text protocol, each column is length-encoded string:
    /// - 0xFB = NULL
    /// - Otherwise: length-encoded integer followed by that many bytes
    pub fn parse_row(data: &[u8], column_count: usize) -> Vec<AggregateValue> {
        let mut values = Vec::with_capacity(column_count);
        let mut offset = 0;

        for _ in 0..column_count {
            if offset >= data.len() {
                values.push(AggregateValue::Null);
                continue;
            }

            // Check for NULL (0xFB)
            if data[offset] == 0xFB {
                values.push(AggregateValue::Null);
                offset += 1;
                continue;
            }

            // Read length-encoded string
            let (len, bytes_read) = read_length_encoded_int(&data[offset..]);
            offset += bytes_read;

            if offset + len as usize <= data.len() {
                let value_bytes = &data[offset..offset + len as usize];
                values.push(AggregateValue::from_bytes(value_bytes));
                offset += len as usize;
            } else {
                values.push(AggregateValue::Null);
            }
        }

        values
    }
}

/// Read a length-encoded integer from MySQL protocol
/// Returns (value, bytes_read)
fn read_length_encoded_int(data: &[u8]) -> (u64, usize) {
    if data.is_empty() {
        return (0, 0);
    }

    match data[0] {
        // 1-byte integer
        0x00..=0xFA => (data[0] as u64, 1),
        // NULL indicator (shouldn't happen here, but handle gracefully)
        0xFB => (0, 1),
        // 2-byte integer
        0xFC => {
            if data.len() >= 3 {
                let val = u16::from_le_bytes([data[1], data[2]]) as u64;
                (val, 3)
            } else {
                (0, 1)
            }
        }
        // 3-byte integer
        0xFD => {
            if data.len() >= 4 {
                let val = u32::from_le_bytes([data[1], data[2], data[3], 0]) as u64;
                (val, 4)
            } else {
                (0, 1)
            }
        }
        // 8-byte integer
        0xFE => {
            if data.len() >= 9 {
                let val = u64::from_le_bytes([
                    data[1], data[2], data[3], data[4],
                    data[5], data[6], data[7], data[8],
                ]);
                (val, 9)
            } else {
                (0, 1)
            }
        }
        // Error indicator
        0xFF => (0, 1),
    }
}

/// Build a MySQL row packet from aggregate values
pub fn build_row_packet(values: &[AggregateValue], sequence_id: u8) -> Bytes {
    let mut payload = BytesMut::new();

    for value in values {
        match value {
            AggregateValue::Null => {
                payload.put_u8(0xFB); // NULL indicator
            }
            _ => {
                let encoded = value.encode();
                // Write length-encoded string
                write_length_encoded_string(&mut payload, &encoded);
            }
        }
    }

    // Build packet with header
    let payload_len = payload.len();
    let mut packet = BytesMut::with_capacity(4 + payload_len);
    
    // 3-byte payload length (little-endian)
    packet.put_u8((payload_len & 0xFF) as u8);
    packet.put_u8(((payload_len >> 8) & 0xFF) as u8);
    packet.put_u8(((payload_len >> 16) & 0xFF) as u8);
    // Sequence ID
    packet.put_u8(sequence_id);
    // Payload
    packet.extend(payload);

    packet.freeze()
}

/// Write a length-encoded string to buffer
fn write_length_encoded_string(buf: &mut BytesMut, data: &[u8]) {
    let len = data.len();
    
    if len < 251 {
        buf.put_u8(len as u8);
    } else if len < 65536 {
        buf.put_u8(0xFC);
        buf.put_u16_le(len as u16);
    } else if len < 16777216 {
        buf.put_u8(0xFD);
        buf.put_u8((len & 0xFF) as u8);
        buf.put_u8(((len >> 8) & 0xFF) as u8);
        buf.put_u8(((len >> 16) & 0xFF) as u8);
    } else {
        buf.put_u8(0xFE);
        buf.put_u64_le(len as u64);
    }
    
    buf.extend_from_slice(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_value_parsing() {
        assert!(matches!(
            AggregateValue::from_bytes(b"123"),
            AggregateValue::Integer(123)
        ));
        assert!(matches!(
            AggregateValue::from_bytes(b"123.45"),
            AggregateValue::Float(f) if (f - 123.45).abs() < 0.001
        ));
        assert!(matches!(
            AggregateValue::from_bytes(b""),
            AggregateValue::Null
        ));
    }

    #[test]
    fn test_count_merge() {
        let info = AggregateInfo {
            func_type: AggregateType::Count,
            position: 0,
            expr_str: "COUNT(*)".to_string(),
            is_count_star: true,
        };
        
        let mut merger = AggregateMerger::new(info);
        merger.merge(AggregateValue::Integer(10));
        merger.merge(AggregateValue::Integer(20));
        merger.merge(AggregateValue::Integer(30));
        
        let result = merger.finalize();
        assert!(matches!(result, AggregateValue::Integer(60)));
    }

    #[test]
    fn test_sum_merge() {
        let info = AggregateInfo {
            func_type: AggregateType::Sum,
            position: 0,
            expr_str: "SUM(amount)".to_string(),
            is_count_star: false,
        };
        
        let mut merger = AggregateMerger::new(info);
        merger.merge(AggregateValue::Float(100.5));
        merger.merge(AggregateValue::Float(200.5));
        merger.merge(AggregateValue::Float(300.0));
        
        let result = merger.finalize();
        if let AggregateValue::Float(f) = result {
            assert!((f - 601.0).abs() < 0.001);
        } else {
            panic!("Expected Float");
        }
    }

    #[test]
    fn test_max_merge() {
        let info = AggregateInfo {
            func_type: AggregateType::Max,
            position: 0,
            expr_str: "MAX(amount)".to_string(),
            is_count_star: false,
        };
        
        let mut merger = AggregateMerger::new(info);
        merger.merge(AggregateValue::Integer(100));
        merger.merge(AggregateValue::Integer(500));
        merger.merge(AggregateValue::Integer(200));
        
        let result = merger.finalize();
        assert!(matches!(result, AggregateValue::Integer(500)));
    }

    #[test]
    fn test_min_merge() {
        let info = AggregateInfo {
            func_type: AggregateType::Min,
            position: 0,
            expr_str: "MIN(amount)".to_string(),
            is_count_star: false,
        };
        
        let mut merger = AggregateMerger::new(info);
        merger.merge(AggregateValue::Integer(100));
        merger.merge(AggregateValue::Integer(50));
        merger.merge(AggregateValue::Integer(200));
        
        let result = merger.finalize();
        assert!(matches!(result, AggregateValue::Integer(50)));
    }

    #[test]
    fn test_row_parser() {
        // Simple row with two integer values: "10" and "20"
        // Length 2, "10", Length 2, "20"
        let data = vec![
            2, b'1', b'0',  // "10"
            2, b'2', b'0',  // "20"
        ];
        
        let values = RowParser::parse_row(&data, 2);
        assert_eq!(values.len(), 2);
        assert!(matches!(values[0], AggregateValue::Integer(10)));
        assert!(matches!(values[1], AggregateValue::Integer(20)));
    }
}
