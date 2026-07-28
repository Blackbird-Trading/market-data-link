use std::{collections::BTreeSet, net::SocketAddr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    /// Optional service-level hint. Shared UDP/Aeron infrastructure validates
    /// and forwards this value but does not apply any noise filtering.
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

/// JSON control request with request/reply correlation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlRequestEnvelope {
    pub request_id: u64,
    #[serde(flatten)]
    pub request: ControlRequest,
}

impl ControlRequestEnvelope {
    pub fn new(request_id: u64, request: ControlRequest) -> Self {
        Self {
            request_id,
            request,
        }
    }
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
        if self.op == ControlOperation::RefetchBbo
            && self.args.iter().any(|arg| arg.stream != "bbo")
        {
            return Err(ProtocolError::InvalidRefetchStream);
        }
        Ok(())
    }

    pub fn keys(&self) -> BTreeSet<SubscriptionKey> {
        self.args.iter().flat_map(SubscriptionArg::keys).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlReply {
    TransportReady { transport: TransportReady },
    Subscribed { args: Vec<SubscriptionArg> },
    Unsubscribed { args: Vec<SubscriptionArg> },
    Refetched { args: Vec<SubscriptionArg> },
    Error { message: String },
}

/// An asynchronous failure affecting a data stream.
///
/// Unlike [`ControlReply::Error`], this is not a response to a control
/// request and therefore has no request ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamError {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    pub severity: u8,
    pub message: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlEvent {
    StreamError {
        #[serde(flatten)]
        error: StreamError,
    },
}

impl ControlEvent {
    pub fn stream_error(error: StreamError) -> Self {
        Self::StreamError { error }
    }

    pub fn into_stream_error(self) -> StreamError {
        match self {
            Self::StreamError { error } => error,
        }
    }
}

/// JSON control reply carrying the request ID supplied by the client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlReplyEnvelope {
    pub request_id: u64,
    #[serde(flatten)]
    pub reply: ControlReply,
}

impl ControlReplyEnvelope {
    pub fn new(request_id: u64, reply: ControlReply) -> Self {
        Self { request_id, reply }
    }
}

impl ControlReply {
    pub fn for_request(request: &ControlRequest) -> Self {
        match request.op {
            ControlOperation::Subscribe => Self::Subscribed {
                args: request.args.clone(),
            },
            ControlOperation::Unsubscribe => Self::Unsubscribed {
                args: request.args.clone(),
            },
            ControlOperation::RefetchBbo => Self::Refetched {
                args: request.args.clone(),
            },
        }
    }

    pub fn error(error: impl ToString) -> Self {
        Self::Error {
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientTransportConfig {
    Udp,
    AeronIpc { stream_id: i32 },
    AeronUdp { stream_id: i32 },
}

impl ClientTransportConfig {
    pub fn is_aeron(&self) -> bool {
        matches!(self, Self::AeronIpc { .. } | Self::AeronUdp { .. })
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

impl TransportSelection {
    pub fn aeron_config(&self, client_ip: std::net::IpAddr) -> Option<AeronConfig> {
        match self {
            Self::AeronIpc { stream_id } => Some(AeronConfig::Ipc {
                stream_id: *stream_id,
            }),
            Self::AeronUdp {
                client_port,
                stream_id,
            } => Some(AeronConfig::Udp {
                endpoint: SocketAddr::new(client_ip, *client_port),
                stream_id: *stream_id,
            }),
            Self::Udp { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransportReady {
    Udp {
        server_address: SocketAddr,
    },
    AeronIpc {
        stream_id: i32,
    },
    AeronUdp {
        endpoint: SocketAddr,
        stream_id: i32,
    },
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AeronConfig {
    Ipc {
        stream_id: i32,
    },
    Udp {
        endpoint: SocketAddr,
        stream_id: i32,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("control request args cannot be empty")]
    EmptyArgs,
    #[error("subscription ids cannot be empty")]
    EmptyIds,
    #[error("subscription stream cannot be empty")]
    EmptyStream,
    #[error("noise_filter_bps must be finite and non-negative")]
    InvalidNoiseFilter,
    #[error("refetch_bbo only supports the bbo stream")]
    InvalidRefetchStream,
}
