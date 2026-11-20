use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ParseError {
    #[error("Packet terlalu pendek: butuh minimal {expected} bytes, dapat {actual} bytes")]
    PacketTooShort { expected: usize, actual: usize },

    #[error("Device ID bukan UTF-8 yang valid")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("Severity tidak valid: {0} (harus 1-10)")]
    InvalidSeverity(u8),
}

/// Struct Zero-Copy dengan lifetime `'a`.
/// Karena hanya berisi integer dan shared reference (`&'a`), tipe ini bisa `Copy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZeroCopyPacket<'a> {
    pub severity: u8,
    pub device_id: &'a str,
    pub payload: &'a [u8],
}

impl ZeroCopyPacket<'_> {
    #[must_use]
    pub fn is_critical(&self) -> bool {
        self.severity >= 7 && !self.payload.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedTelemetryPacket {
    pub device_id: String,
    pub payload: Vec<u8>,
    pub severity: u8,
}

/// Trait standar Rust untuk konversi tipe (menggantikan `to_owned_packet` kustom)
impl From<ZeroCopyPacket<'_>> for OwnedTelemetryPacket {
    fn from(pkt: ZeroCopyPacket<'_>) -> Self {
        Self {
            device_id: pkt.device_id.to_owned(),
            payload: pkt.payload.to_vec(),
            severity: pkt.severity,
        }
    }
}

/// Parser Biner Zero-Copy: `[1B severity][2B id_len][NB device_id][sisa payload]`
pub fn parse_raw_bytes(raw: &[u8]) -> Result<ZeroCopyPacket<'_>, ParseError> {
    // 1. Slice pattern matching: ekstrak 3 byte header + sisa slice dalam satu langkah
    let &[severity, len_hi, len_lo, ref rest @ ..] = raw else {
        return Err(ParseError::PacketTooShort {
            expected: 3,
            actual: raw.len(),
        });
    };

    if !(1..=10).contains(&severity) {
        return Err(ParseError::InvalidSeverity(severity));
    }

    let id_len = usize::from(u16::from_be_bytes([len_hi, len_lo]));

    // 2. Belah slice sisa menjadi (device_id, payload) secara aman tanpa panic
    let (id_slice, payload) = rest
        .split_at_checked(id_len)
        .ok_or(ParseError::PacketTooShort {
            expected: 3 + id_len,
            actual: raw.len(),
        })?;

    let device_id = std::str::from_utf8(id_slice)?;

    Ok(ZeroCopyPacket {
        severity,
        device_id,
        payload,
    })
}

// Konvensi Rust: modul unit test selalu berada di paling bawah file
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_zero_copy() {
        let raw = [8, 0, 4, b't', b'e', b's', b't', 0x01, 0x02];
        let pkt = parse_raw_bytes(&raw).unwrap();

        assert_eq!(
            pkt,
            ZeroCopyPacket {
                severity: 8,
                device_id: "test",
                payload: &[0x01, 0x02],
            }
        );
        assert!(pkt.is_critical());
    }

    #[test]
    fn test_parse_truncated_error() {
        let raw = [8, 0, 10, b't', b'e'];
        let err = parse_raw_bytes(&raw).unwrap_err();

        assert_eq!(
            err,
            ParseError::PacketTooShort {
                expected: 13,
                actual: 5,
            }
        );
    }
}
