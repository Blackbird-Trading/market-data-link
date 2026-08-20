//! Canonical binary feature-BBO message produced by FeaturesModule.

use thiserror::Error;

pub const FEATURE_BBO_MESSAGE_TYPE: u8 = 6;
pub const FEATURE_BBO_WIRE_LEN: usize = 97;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeatureBbo {
    pub feature_id: i32,
    pub market_id: i32,
    pub timestamp_mdp_in: i64,
    pub bid: f64,
    pub bid_volume: f64,
    pub ask: f64,
    pub ask_volume: f64,
    pub event_id: u64,
    pub ts_mono_mdp_out: i64,
    pub mdp_received_ts_ns: u64,
    pub feature_start_ts_ns: u64,
    pub feature_done_ts_ns: u64,
    pub signal_bps: f64,
}

impl FeatureBbo {
    pub fn encode_le(&self) -> [u8; FEATURE_BBO_WIRE_LEN] {
        let mut bytes = [0; FEATURE_BBO_WIRE_LEN];
        bytes[0] = FEATURE_BBO_MESSAGE_TYPE;
        put(&mut bytes, 1, self.feature_id.to_le_bytes());
        put(&mut bytes, 5, self.market_id.to_le_bytes());
        put(&mut bytes, 9, self.timestamp_mdp_in.to_le_bytes());
        put(&mut bytes, 17, self.bid.to_le_bytes());
        put(&mut bytes, 25, self.bid_volume.to_le_bytes());
        put(&mut bytes, 33, self.ask.to_le_bytes());
        put(&mut bytes, 41, self.ask_volume.to_le_bytes());
        put(&mut bytes, 49, self.event_id.to_le_bytes());
        put(&mut bytes, 57, self.ts_mono_mdp_out.to_le_bytes());
        put(&mut bytes, 65, self.mdp_received_ts_ns.to_le_bytes());
        put(&mut bytes, 73, self.feature_start_ts_ns.to_le_bytes());
        put(&mut bytes, 81, self.feature_done_ts_ns.to_le_bytes());
        put(&mut bytes, 89, self.signal_bps.to_le_bytes());
        bytes
    }

    pub fn decode_le(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != FEATURE_BBO_WIRE_LEN {
            return Err(CodecError::InvalidLength {
                expected: FEATURE_BBO_WIRE_LEN,
                actual: bytes.len(),
            });
        }
        if bytes[0] != FEATURE_BBO_MESSAGE_TYPE {
            return Err(CodecError::UnexpectedType(bytes[0]));
        }
        Ok(Self {
            feature_id: read(bytes, 1, i32::from_le_bytes)?,
            market_id: read(bytes, 5, i32::from_le_bytes)?,
            timestamp_mdp_in: read(bytes, 9, i64::from_le_bytes)?,
            bid: read(bytes, 17, f64::from_le_bytes)?,
            bid_volume: read(bytes, 25, f64::from_le_bytes)?,
            ask: read(bytes, 33, f64::from_le_bytes)?,
            ask_volume: read(bytes, 41, f64::from_le_bytes)?,
            event_id: read(bytes, 49, u64::from_le_bytes)?,
            ts_mono_mdp_out: read(bytes, 57, i64::from_le_bytes)?,
            mdp_received_ts_ns: read(bytes, 65, u64::from_le_bytes)?,
            feature_start_ts_ns: read(bytes, 73, u64::from_le_bytes)?,
            feature_done_ts_ns: read(bytes, 81, u64::from_le_bytes)?,
            signal_bps: read(bytes, 89, f64::from_le_bytes)?,
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("unexpected message type {0}")]
    UnexpectedType(u8),
    #[error("invalid wire length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
}

fn put<const N: usize>(bytes: &mut [u8], offset: usize, value: [u8; N]) {
    bytes[offset..offset + N].copy_from_slice(&value);
}

fn read<const N: usize, T>(
    bytes: &[u8],
    offset: usize,
    convert: impl FnOnce([u8; N]) -> T,
) -> Result<T, CodecError> {
    let end = offset + N;
    let actual = bytes.len();
    let value = bytes
        .get(offset..end)
        .ok_or(CodecError::InvalidLength {
            expected: end,
            actual,
        })?
        .try_into()
        .expect("slice length was checked");
    Ok(convert(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_bbo_golden_vector_and_round_trip() {
        let message = FeatureBbo {
            feature_id: 7,
            market_id: 42,
            timestamp_mdp_in: 1,
            bid: 100.0,
            bid_volume: 2.0,
            ask: 101.0,
            ask_volume: 3.0,
            event_id: 9,
            ts_mono_mdp_out: 10,
            mdp_received_ts_ns: 11,
            feature_start_ts_ns: 12,
            feature_done_ts_ns: 13,
            signal_bps: 4.0,
        };
        let encoded = message.encode_le();
        assert_eq!(&encoded[..9], &[6, 7, 0, 0, 0, 42, 0, 0, 0]);
        assert_eq!(FeatureBbo::decode_le(&encoded).unwrap(), message);
    }

    #[test]
    fn feature_bbo_rejects_wrong_marker_and_length() {
        let mut encoded = FeatureBbo {
            feature_id: 1,
            market_id: 2,
            timestamp_mdp_in: 3,
            bid: 4.0,
            bid_volume: 5.0,
            ask: 6.0,
            ask_volume: 7.0,
            event_id: 8,
            ts_mono_mdp_out: 9,
            mdp_received_ts_ns: 10,
            feature_start_ts_ns: 11,
            feature_done_ts_ns: 12,
            signal_bps: 13.0,
        }
        .encode_le();
        encoded[0] = 1;
        assert_eq!(
            FeatureBbo::decode_le(&encoded),
            Err(CodecError::UnexpectedType(1))
        );
        assert_eq!(
            FeatureBbo::decode_le(&encoded[..FEATURE_BBO_WIRE_LEN - 1]),
            Err(CodecError::InvalidLength {
                expected: FEATURE_BBO_WIRE_LEN,
                actual: FEATURE_BBO_WIRE_LEN - 1,
            })
        );
    }
}
