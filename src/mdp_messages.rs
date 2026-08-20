//! Canonical binary market-data messages produced by MDP.

use thiserror::Error;

pub const STATUS_MESSAGE_TYPE: u8 = 0;
pub const BBO_MESSAGE_TYPE: u8 = 1;
pub const TRADE_MESSAGE_TYPE: u8 = 2;
pub const SNAPSHOT_MESSAGE_TYPE: u8 = 3;
pub const DEPTH_UPDATE_MESSAGE_TYPE: u8 = 4;

pub const BBO_WIRE_LEN: usize = 61;
pub const TRADE_WIRE_LEN: usize = 46;
pub const ORDER_BOOK_HEADER_LEN: usize = 55;
pub const ORDER_BOOK_LEVEL_LEN: usize = 32;
pub const STATUS_WIRE_LEN: usize = 31;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bbo {
    pub market_id: i32,
    pub timestamp_mdp_in: i64,
    pub bid: f64,
    pub bid_volume: f64,
    pub ask: f64,
    pub ask_volume: f64,
    pub event_id: u64,
    pub ts_mono_mdp_out: i64,
}

impl Bbo {
    pub fn encode_le(&self) -> [u8; BBO_WIRE_LEN] {
        let mut bytes = [0; BBO_WIRE_LEN];
        bytes[0] = BBO_MESSAGE_TYPE;
        put(&mut bytes, 1, self.market_id.to_le_bytes());
        put(&mut bytes, 5, self.timestamp_mdp_in.to_le_bytes());
        put(&mut bytes, 13, self.bid.to_le_bytes());
        put(&mut bytes, 21, self.bid_volume.to_le_bytes());
        put(&mut bytes, 29, self.ask.to_le_bytes());
        put(&mut bytes, 37, self.ask_volume.to_le_bytes());
        put(&mut bytes, 45, self.event_id.to_le_bytes());
        put(&mut bytes, 53, self.ts_mono_mdp_out.to_le_bytes());
        bytes
    }

    pub fn decode_le(bytes: &[u8]) -> Result<Self, CodecError> {
        require_type_and_len(bytes, BBO_MESSAGE_TYPE, BBO_WIRE_LEN)?;
        Ok(Self {
            market_id: read(bytes, 1, i32::from_le_bytes)?,
            timestamp_mdp_in: read(bytes, 5, i64::from_le_bytes)?,
            bid: read(bytes, 13, f64::from_le_bytes)?,
            bid_volume: read(bytes, 21, f64::from_le_bytes)?,
            ask: read(bytes, 29, f64::from_le_bytes)?,
            ask_volume: read(bytes, 37, f64::from_le_bytes)?,
            event_id: read(bytes, 45, u64::from_le_bytes)?,
            ts_mono_mdp_out: read(bytes, 53, i64::from_le_bytes)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    pub market_id: i32,
    pub timestamp_mdp_in: i64,
    pub price: f64,
    pub quantity: f64,
    pub side: i8,
    pub event_id: u64,
    pub ts_mono_mdp_out: i64,
}

impl Trade {
    pub fn encode_le(&self) -> [u8; TRADE_WIRE_LEN] {
        let mut bytes = [0; TRADE_WIRE_LEN];
        bytes[0] = TRADE_MESSAGE_TYPE;
        put(&mut bytes, 1, self.market_id.to_le_bytes());
        put(&mut bytes, 5, self.timestamp_mdp_in.to_le_bytes());
        put(&mut bytes, 13, self.price.to_le_bytes());
        put(&mut bytes, 21, self.quantity.to_le_bytes());
        bytes[29] = self.side as u8;
        put(&mut bytes, 30, self.event_id.to_le_bytes());
        put(&mut bytes, 38, self.ts_mono_mdp_out.to_le_bytes());
        bytes
    }

    pub fn decode_le(bytes: &[u8]) -> Result<Self, CodecError> {
        require_type_and_len(bytes, TRADE_MESSAGE_TYPE, TRADE_WIRE_LEN)?;
        Ok(Self {
            market_id: read(bytes, 1, i32::from_le_bytes)?,
            timestamp_mdp_in: read(bytes, 5, i64::from_le_bytes)?,
            price: read(bytes, 13, f64::from_le_bytes)?,
            quantity: read(bytes, 21, f64::from_le_bytes)?,
            side: bytes[29] as i8,
            event_id: read(bytes, 30, u64::from_le_bytes)?,
            ts_mono_mdp_out: read(bytes, 38, i64::from_le_bytes)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBook {
    pub message_type: u8,
    pub depth: u16,
    pub market_id: i32,
    pub update_id: i64,
    pub timestamp_mdp_in: i64,
    pub timestamp_matching_engine: i64,
    pub timestamp: i64,
    pub event_id: u64,
    pub timestamp_mdp_out: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Copy)]
pub struct OrderBookView<'a> {
    pub message_type: u8,
    pub depth: u16,
    pub market_id: i32,
    pub update_id: i64,
    pub timestamp_mdp_in: i64,
    pub timestamp_matching_engine: i64,
    pub timestamp: i64,
    pub event_id: u64,
    pub timestamp_mdp_out: i64,
    pub bids: &'a [(f64, f64)],
    pub asks: &'a [(f64, f64)],
}

impl OrderBookView<'_> {
    pub fn encode_le_into<'a>(&self, bytes: &'a mut Vec<u8>) -> Result<&'a [u8], CodecError> {
        if !matches!(
            self.message_type,
            SNAPSHOT_MESSAGE_TYPE | DEPTH_UPDATE_MESSAGE_TYPE
        ) {
            return Err(CodecError::UnexpectedType(self.message_type));
        }
        if self.bids.len() != self.depth as usize || self.asks.len() != self.depth as usize {
            return Err(CodecError::DepthMismatch);
        }
        bytes.resize(
            ORDER_BOOK_HEADER_LEN + self.depth as usize * ORDER_BOOK_LEVEL_LEN,
            0,
        );
        bytes[0] = self.message_type;
        put(bytes, 1, self.depth.to_le_bytes());
        put(bytes, 3, self.market_id.to_le_bytes());
        put(bytes, 7, self.update_id.to_le_bytes());
        put(bytes, 15, self.timestamp_mdp_in.to_le_bytes());
        put(bytes, 23, self.timestamp_matching_engine.to_le_bytes());
        put(bytes, 31, self.timestamp.to_le_bytes());
        put(bytes, 39, self.event_id.to_le_bytes());
        put(bytes, 47, self.timestamp_mdp_out.to_le_bytes());
        let mut offset = ORDER_BOOK_HEADER_LEN;
        for (bid, ask) in self.bids.iter().zip(self.asks) {
            put(bytes, offset, bid.0.to_le_bytes());
            put(bytes, offset + 8, bid.1.to_le_bytes());
            put(bytes, offset + 16, ask.0.to_le_bytes());
            put(bytes, offset + 24, ask.1.to_le_bytes());
            offset += ORDER_BOOK_LEVEL_LEN;
        }
        Ok(bytes)
    }
}

impl OrderBook {
    pub fn encode_le(&self) -> Result<Vec<u8>, CodecError> {
        let mut bytes =
            Vec::with_capacity(ORDER_BOOK_HEADER_LEN + self.depth as usize * ORDER_BOOK_LEVEL_LEN);
        OrderBookView {
            message_type: self.message_type,
            depth: self.depth,
            market_id: self.market_id,
            update_id: self.update_id,
            timestamp_mdp_in: self.timestamp_mdp_in,
            timestamp_matching_engine: self.timestamp_matching_engine,
            timestamp: self.timestamp,
            event_id: self.event_id,
            timestamp_mdp_out: self.timestamp_mdp_out,
            bids: &self.bids,
            asks: &self.asks,
        }
        .encode_le_into(&mut bytes)?;
        Ok(bytes)
    }

    pub fn decode_le(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() < ORDER_BOOK_HEADER_LEN {
            return Err(CodecError::InvalidLength {
                expected: ORDER_BOOK_HEADER_LEN,
                actual: bytes.len(),
            });
        }
        let message_type = bytes[0];
        if !matches!(
            message_type,
            SNAPSHOT_MESSAGE_TYPE | DEPTH_UPDATE_MESSAGE_TYPE
        ) {
            return Err(CodecError::UnexpectedType(message_type));
        }
        let depth = read(bytes, 1, u16::from_le_bytes)?;
        let expected = ORDER_BOOK_HEADER_LEN + depth as usize * ORDER_BOOK_LEVEL_LEN;
        if bytes.len() != expected {
            return Err(CodecError::InvalidLength {
                expected,
                actual: bytes.len(),
            });
        }
        let mut bids = Vec::with_capacity(depth as usize);
        let mut asks = Vec::with_capacity(depth as usize);
        let mut offset = ORDER_BOOK_HEADER_LEN;
        for _ in 0..depth {
            bids.push((
                read(bytes, offset, f64::from_le_bytes)?,
                read(bytes, offset + 8, f64::from_le_bytes)?,
            ));
            asks.push((
                read(bytes, offset + 16, f64::from_le_bytes)?,
                read(bytes, offset + 24, f64::from_le_bytes)?,
            ));
            offset += ORDER_BOOK_LEVEL_LEN;
        }
        Ok(Self {
            message_type,
            depth,
            market_id: read(bytes, 3, i32::from_le_bytes)?,
            update_id: read(bytes, 7, i64::from_le_bytes)?,
            timestamp_mdp_in: read(bytes, 15, i64::from_le_bytes)?,
            timestamp_matching_engine: read(bytes, 23, i64::from_le_bytes)?,
            timestamp: read(bytes, 31, i64::from_le_bytes)?,
            event_id: read(bytes, 39, u64::from_le_bytes)?,
            timestamp_mdp_out: read(bytes, 47, i64::from_le_bytes)?,
            bids,
            asks,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketStatus {
    pub market_id: i32,
    pub timestamp_mdp_in: i64,
    pub state: u16,
    pub event_id: u64,
    pub ts_mono_mdp_out: i64,
}

impl MarketStatus {
    pub fn encode_le(&self) -> [u8; STATUS_WIRE_LEN] {
        let mut bytes = [0; STATUS_WIRE_LEN];
        bytes[0] = STATUS_MESSAGE_TYPE;
        put(&mut bytes, 1, self.market_id.to_le_bytes());
        put(&mut bytes, 5, self.timestamp_mdp_in.to_le_bytes());
        put(&mut bytes, 13, self.state.to_le_bytes());
        put(&mut bytes, 15, self.event_id.to_le_bytes());
        put(&mut bytes, 23, self.ts_mono_mdp_out.to_le_bytes());
        bytes
    }

    pub fn decode_le(bytes: &[u8]) -> Result<Self, CodecError> {
        require_type_and_len(bytes, STATUS_MESSAGE_TYPE, STATUS_WIRE_LEN)?;
        Ok(Self {
            market_id: read(bytes, 1, i32::from_le_bytes)?,
            timestamp_mdp_in: read(bytes, 5, i64::from_le_bytes)?,
            state: read(bytes, 13, u16::from_le_bytes)?,
            event_id: read(bytes, 15, u64::from_le_bytes)?,
            ts_mono_mdp_out: read(bytes, 23, i64::from_le_bytes)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WireMessage {
    Status(MarketStatus),
    Bbo(Bbo),
    Trade(Trade),
    OrderBook(OrderBook),
    Other(Vec<u8>),
}

impl WireMessage {
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        match bytes.first().copied().ok_or(CodecError::Empty)? {
            STATUS_MESSAGE_TYPE => Ok(Self::Status(MarketStatus::decode_le(bytes)?)),
            BBO_MESSAGE_TYPE => Ok(Self::Bbo(Bbo::decode_le(bytes)?)),
            TRADE_MESSAGE_TYPE => Ok(Self::Trade(Trade::decode_le(bytes)?)),
            SNAPSHOT_MESSAGE_TYPE | DEPTH_UPDATE_MESSAGE_TYPE => {
                Ok(Self::OrderBook(OrderBook::decode_le(bytes)?))
            }
            _ => Ok(Self::Other(bytes.to_vec())),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("empty wire message")]
    Empty,
    #[error("unexpected message type {0}")]
    UnexpectedType(u8),
    #[error("invalid wire length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
    #[error("order-book depth does not match level counts")]
    DepthMismatch,
}

fn require_type_and_len(bytes: &[u8], message_type: u8, expected: usize) -> Result<(), CodecError> {
    if bytes.len() != expected {
        return Err(CodecError::InvalidLength {
            expected,
            actual: bytes.len(),
        });
    }
    if bytes[0] != message_type {
        return Err(CodecError::UnexpectedType(bytes[0]));
    }
    Ok(())
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
    fn bbo_golden_vector_and_round_trip() {
        let bbo = Bbo {
            market_id: 42,
            timestamp_mdp_in: 1,
            bid: 100.0,
            bid_volume: 2.0,
            ask: 101.0,
            ask_volume: 3.0,
            event_id: 9,
            ts_mono_mdp_out: 10,
        };
        let encoded = bbo.encode_le();
        assert_eq!(&encoded[..5], &[1, 42, 0, 0, 0]);
        assert_eq!(Bbo::decode_le(&encoded).unwrap(), bbo);
    }

    #[test]
    fn trade_and_status_golden_vectors_round_trip() {
        let trade = Trade {
            market_id: 42,
            timestamp_mdp_in: 1,
            price: 100.0,
            quantity: 2.0,
            side: -1,
            event_id: 9,
            ts_mono_mdp_out: 10,
        };
        let trade_bytes = trade.encode_le();
        assert_eq!(&trade_bytes[..5], &[TRADE_MESSAGE_TYPE, 42, 0, 0, 0]);
        assert_eq!(trade_bytes[29], u8::MAX);
        assert_eq!(Trade::decode_le(&trade_bytes).unwrap(), trade);

        let status = MarketStatus {
            market_id: 7,
            timestamp_mdp_in: 11,
            state: 2,
            event_id: 12,
            ts_mono_mdp_out: 13,
        };
        let status_bytes = status.encode_le();
        assert_eq!(&status_bytes[..5], &[STATUS_MESSAGE_TYPE, 7, 0, 0, 0]);
        assert_eq!(MarketStatus::decode_le(&status_bytes).unwrap(), status);
    }

    #[test]
    fn order_book_round_trip_and_depth_validation() {
        let book = OrderBook {
            message_type: SNAPSHOT_MESSAGE_TYPE,
            depth: 1,
            market_id: 7,
            update_id: 8,
            timestamp_mdp_in: 9,
            timestamp_matching_engine: 10,
            timestamp: 11,
            event_id: 12,
            timestamp_mdp_out: 13,
            bids: vec![(100.0, 2.0)],
            asks: vec![(101.0, 3.0)],
        };
        let encoded = book.encode_le().unwrap();
        assert_eq!(encoded.len(), ORDER_BOOK_HEADER_LEN + ORDER_BOOK_LEVEL_LEN);
        assert_eq!(OrderBook::decode_le(&encoded).unwrap(), book);

        let mut mismatched = book.clone();
        mismatched.asks.clear();
        assert_eq!(mismatched.encode_le(), Err(CodecError::DepthMismatch));

        let truncated = &encoded[..encoded.len() - 1];
        assert_eq!(
            OrderBook::decode_le(truncated),
            Err(CodecError::InvalidLength {
                expected: encoded.len(),
                actual: truncated.len(),
            })
        );
    }

    #[test]
    fn fixed_codecs_reject_wrong_markers_and_lengths() {
        let mut bbo = Bbo {
            market_id: 1,
            timestamp_mdp_in: 2,
            bid: 3.0,
            bid_volume: 4.0,
            ask: 5.0,
            ask_volume: 6.0,
            event_id: 7,
            ts_mono_mdp_out: 8,
        }
        .encode_le();
        bbo[0] = TRADE_MESSAGE_TYPE;
        assert_eq!(
            Bbo::decode_le(&bbo),
            Err(CodecError::UnexpectedType(TRADE_MESSAGE_TYPE))
        );
        assert_eq!(
            Bbo::decode_le(&bbo[..BBO_WIRE_LEN - 1]),
            Err(CodecError::InvalidLength {
                expected: BBO_WIRE_LEN,
                actual: BBO_WIRE_LEN - 1,
            })
        );
    }

    #[test]
    fn errors_are_not_data_messages() {
        assert_eq!(
            WireMessage::decode(&[5]).unwrap(),
            WireMessage::Other(vec![5])
        );
    }
}
