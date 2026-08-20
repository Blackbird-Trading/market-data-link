//! JSON messages sent by market-data clients.
//!
//! Subscription commands intentionally remain uncorrelated for compatibility
//! with the existing MDP and TDP control planes. Transport selection is an
//! optional request/reply setup exchange.

use std::{collections::BTreeSet, error::Error, fmt, net::SocketAddr};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOperation {
    Subscribe,
    Unsubscribe,
    RefetchBbo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionArg {
    pub id: Vec<i32>,
    pub stream: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub noise_filter_bps: Option<f64>,
}

impl SubscriptionArg {
    pub fn new(id: impl IntoIterator<Item = i32>, stream: impl Into<String>) -> Self {
        Self {
            id: id.into_iter().collect(),
            stream: stream.into(),
            noise_filter_bps: None,
        }
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.id.is_empty() {
            return Err(ProtocolError::EmptyIds);
        }
        if self.stream.trim().is_empty() {
            return Err(ProtocolError::EmptyStream);
        }
        if self
            .noise_filter_bps
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(ProtocolError::InvalidNoiseFilter);
        }
        Ok(())
    }

    pub fn keys(&self) -> impl Iterator<Item = SubscriptionKey> + '_ {
        self.id.iter().copied().map(|id| SubscriptionKey {
            id,
            stream: self.stream.clone(),
            noise_filter_bps_bits: self.noise_filter_bps.map(f64::to_bits),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionKey {
    pub id: i32,
    pub stream: String,
    pub noise_filter_bps_bits: Option<u64>,
}

impl SubscriptionKey {
    pub fn new(id: i32, stream: impl Into<String>) -> Self {
        Self {
            id,
            stream: stream.into(),
            noise_filter_bps_bits: None,
        }
    }

    pub fn noise_filter_bps(&self) -> Option<f64> {
        self.noise_filter_bps_bits.map(f64::from_bits)
    }

    pub fn as_arg(&self) -> SubscriptionArg {
        SubscriptionArg {
            id: vec![self.id],
            stream: self.stream.clone(),
            noise_filter_bps: self.noise_filter_bps(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlRequest {
    pub op: ControlOperation,
    pub args: Vec<SubscriptionArg>,
}

impl ControlRequest {
    pub fn subscribe(args: Vec<SubscriptionArg>) -> Self {
        Self {
            op: ControlOperation::Subscribe,
            args,
        }
    }

    pub fn unsubscribe(args: Vec<SubscriptionArg>) -> Self {
        Self {
            op: ControlOperation::Unsubscribe,
            args,
        }
    }

    pub fn refetch_bbo(ids: impl IntoIterator<Item = i32>) -> Self {
        Self {
            op: ControlOperation::RefetchBbo,
            args: vec![SubscriptionArg::new(ids, "bbo")],
        }
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.args.is_empty() {
            return Err(ProtocolError::EmptyArgs);
        }
        for arg in &self.args {
            arg.validate()?;
        }
        if self.op == ControlOperation::RefetchBbo && self.args.iter().any(|arg| arg.stream != "bbo") {
            return Err(ProtocolError::InvalidRefetchStream);
        }
        Ok(())
    }

    pub fn keys(&self) -> BTreeSet<SubscriptionKey> {
        self.args.iter().flat_map(SubscriptionArg::keys).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectTransport {
    pub op: SelectTransportOperation,
    pub transport: TransportSelection,
}

impl SelectTransport {
    pub fn new(transport: TransportSelection) -> Self {
        Self {
            op: SelectTransportOperation::SelectTransport,
            transport,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectTransportOperation {
    SelectTransport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransportSelection {
    Udp { client_port: u16 },
    AeronIpc { stream_id: i32 },
    AeronUdp { client_port: u16, stream_id: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AeronChannel {
    Ipc,
    Udp { host: String, port: u16 },
}

impl AeronChannel {
    pub fn ipc() -> Self {
        Self::Ipc
    }

    pub fn udp(host: impl Into<String>, port: u16) -> Self {
        Self::Udp {
            host: host.into(),
            port,
        }
    }

    pub fn to_channel_string(&self) -> String {
        match self {
            Self::Ipc => "aeron:ipc".to_string(),
            Self::Udp { host, port } => format!("aeron:udp?endpoint={host}:{port}"),
        }
    }
}

/// Legacy MDP/TDP setup message. Its untagged shape is intentionally retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AeronConfigMessage {
    pub aeron_channel: AeronChannel,
    pub stream_id: i32,
}

/// Legacy MDP UDP setup message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UdpConfigMessage {
    pub client_address: SocketAddr,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError {
    EmptyArgs,
    EmptyIds,
    EmptyStream,
    InvalidNoiseFilter,
    InvalidRefetchStream,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyArgs => "control request args cannot be empty",
            Self::EmptyIds => "subscription ids cannot be empty",
            Self::EmptyStream => "subscription stream cannot be empty",
            Self::InvalidNoiseFilter => "noise_filter_bps must be finite and non-negative",
            Self::InvalidRefetchStream => "refetch_bbo only supports the bbo stream",
        })
    }
}

impl Error for ProtocolError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_are_uncorrelated() {
        let subscribe = ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]);
        assert_eq!(
            serde_json::to_string(&subscribe).unwrap(),
            r#"{"op":"subscribe","args":[{"id":[42],"stream":"bbo"}]}"#
        );
        assert_eq!(
            serde_json::to_string(&ControlRequest::unsubscribe(subscribe.args.clone())).unwrap(),
            r#"{"op":"unsubscribe","args":[{"id":[42],"stream":"bbo"}]}"#
        );
        assert_eq!(
            serde_json::to_string(&ControlRequest::refetch_bbo([42])).unwrap(),
            r#"{"op":"refetch_bbo","args":[{"id":[42],"stream":"bbo"}]}"#
        );
    }

    #[test]
    fn transport_selection_has_no_protocol_identity() {
        let value = serde_json::to_value(SelectTransport::new(TransportSelection::Udp { client_port: 4000 })).unwrap();
        assert_eq!(value["op"], "select_transport");
        assert_eq!(value["transport"]["type"], "udp");
        assert!(value.get("control_version").is_none());
    }

    #[test]
    fn legacy_setup_shapes_are_preserved() {
        let aeron = serde_json::to_string(&AeronConfigMessage {
            aeron_channel: AeronChannel::Ipc,
            stream_id: 1002,
        })
        .unwrap();
        assert_eq!(aeron, r#"{"aeron_channel":{"type":"ipc"},"stream_id":1002}"#);

        let udp = serde_json::to_string(&UdpConfigMessage {
            client_address: "127.0.0.1:4000".parse().unwrap(),
        })
        .unwrap();
        assert_eq!(udp, r#"{"client_address":"127.0.0.1:4000"}"#);
    }

    #[test]
    fn invalid_subscriptions_are_rejected() {
        assert_eq!(
            ControlRequest::subscribe(Vec::new()).validate(),
            Err(ProtocolError::EmptyArgs)
        );
        assert_eq!(SubscriptionArg::new([], "bbo").validate(), Err(ProtocolError::EmptyIds));
    }
}
