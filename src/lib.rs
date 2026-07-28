#![doc = include_str!("../README.md")]

#[cfg(feature = "aeron")]
pub mod aeron_publisher;
pub mod client;
pub mod codec;
pub mod polling;
pub mod protocol;
pub mod server;
pub mod transport;

#[cfg(feature = "aeron")]
pub use aeron_publisher::{
    AeronFrame, AeronPublishReport, AeronPublisher, AeronPublisherStats, AeronRoute,
    JoinCompletion, JoinStatus,
};
pub use client::{Client, ClientConfig};
pub use polling::{PollEvent, PollingClient};
pub use protocol::{
    AeronChannel, AeronConfig, ClientTransportConfig, ControlEvent, ControlOperation, ControlReply,
    ControlReplyEnvelope, ControlRequest, ControlRequestEnvelope, SelectTransport, StreamError,
    SubscriptionArg, SubscriptionKey, TransportReady, TransportSelection,
};
pub use server::{
    ClientRouter, PublishReport, Server, ServerConfig, ServerHandler, SessionChannels,
    SessionControl,
};
pub use transport::{AERON_DRIVER_DIR, ControlEndpoint, InboundFrame};
