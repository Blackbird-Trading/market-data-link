#![doc = include_str!("../README.md")]

pub mod client;
pub mod codec;
pub mod protocol;
pub mod server;
pub mod transport;

/// Compatibility module for the original flat module layout.
///
/// New code can use [`server::aeron_publisher`] to make the server-side
/// ownership explicit.
pub mod aeron_publisher {
    pub use crate::server::aeron_publisher::*;
}

/// Compatibility module for the original flat module layout.
///
/// New code can use [`client::polling`] to make the client role explicit.
pub mod polling {
    pub use crate::client::polling::*;
}

pub use client::{Client, ClientConfig, ClientEvent, PollEvent, PollingClient};
pub use protocol::{
    AeronChannel, AeronConfig, ClientTransportConfig, ControlEvent, ControlOperation, ControlReply,
    ControlReplyEnvelope, ControlRequest, ControlRequestEnvelope, SelectTransport, StreamError,
    SubscriptionArg, SubscriptionKey, TransportReady, TransportSelection,
};
pub use server::aeron_publisher::{
    AeronFrame, AeronPublishReport, AeronPublisher, AeronPublisherStats, AeronRoute,
    JoinCompletion, JoinStatus,
};
pub use server::{
    ClientRouter, PublishReport, Server, ServerConfig, ServerHandler, SessionChannels,
    SessionControl,
};
pub use transport::{AERON_DRIVER_DIR, ControlEndpoint, InboundFrame};
