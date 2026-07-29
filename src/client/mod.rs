//! Asynchronous client API.
//!
//! The client lifecycle is intentionally linear:
//!
//! 1. [`Client::connect`] opens the WebSocket control plane.
//! 2. It prepares and negotiates the selected data transport.
//! 3. [`Client::send_request`] sends a request with a unique ID.
//! 4. The client waits for the reply carrying that same ID.
//! 5. Market data is read from UDP or from [`Client::take_aeron_subscriber`].

use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

pub mod polling;

pub use polling::{PollEvent, PollingClient};

use anyhow::{Context, Result};
use futures_util::{
    FutureExt, SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::{TcpStream, UdpSocket};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::transport::{AeronClient, AeronSubscriber};
use crate::{
    protocol::{
        ClientTransportConfig, ControlEvent, ControlReply, ControlReplyEnvelope, ControlRequest,
        ControlRequestEnvelope, SelectTransport, StreamError, SubscriptionArg, SubscriptionKey,
        TransportReady, TransportSelection,
    },
    transport::{ControlEndpoint, InboundFrame},
};

const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
/// Settings used for every connection and reconnection.
pub struct ClientConfig {
    /// WebSocket endpoint used for reliable control messages.
    pub endpoint: ControlEndpoint,
    /// Data plane to negotiate immediately after the WebSocket opens.
    pub transport: ClientTransportConfig,
    /// Whether this process is allowed to connect to the Aeron media driver.
    pub aeron_enabled: bool,
    /// Idle duration after which [`Client::send_ping_if_due`] sends a ping.
    pub keepalive_interval: Duration,
}

impl ClientConfig {
    /// Creates a client configuration with Aeron disabled and a 30-second
    /// keepalive interval.
    pub fn new(endpoint: ControlEndpoint, transport: ClientTransportConfig) -> Self {
        Self {
            endpoint,
            transport,
            aeron_enabled: false,
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
        }
    }
}

type Sender = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Receiver = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub struct Client {
    config: ClientConfig,
    sender: Sender,
    receiver: Receiver,
    udp_socket: Option<UdpSocket>,
    aeron_subscriber: Option<AeronSubscriber>,
    aeron_udp_port: Option<u16>,
    ref_counts: HashMap<SubscriptionKey, usize>,
    last_message_sent: Instant,
    transport_ready: bool,
    next_request_id: u64,
    pending_events: VecDeque<ClientEvent>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("config", &self.config)
            .field("subscriptions", &self.ref_counts)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Connects the control plane and completes data-plane negotiation.
    ///
    /// A successful return means the server has replied with
    /// `transport_ready`; subscription requests can be sent immediately.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        // Phase 1: prepare any receive endpoint whose details must be included
        // in the transport selection request.
        if config.transport.is_aeron() && !config.aeron_enabled {
            anyhow::bail!("Aeron transport selected while [aeron].enabled is false");
        }
        let (aeron_subscriber, aeron_udp_port) = match &config.transport {
            ClientTransportConfig::AeronUdp { stream_id } => {
                let aeron = AeronClient::connect()
                    .context("failed to connect to the Aeron media driver")?;
                let (subscriber, port) = aeron.ephemeral_udp_subscriber(*stream_id)?;
                (Some(subscriber), Some(port))
            }
            _ => (None, None),
        };

        // Phase 2: open the reliable WebSocket control plane.
        let ControlEndpoint::Tcp(uri) = &config.endpoint;
        let (stream, _) = connect_async(uri)
            .await
            .with_context(|| format!("failed to connect to WebSocket {uri}"))?;
        let (sender, receiver) = stream.split();

        let udp_socket = if matches!(config.transport, ClientTransportConfig::Udp) {
            Some(
                UdpSocket::bind("0.0.0.0:0")
                    .await
                    .context("failed to bind ephemeral UDP client socket")?,
            )
        } else {
            None
        };

        // Phase 3: ask the server to use the prepared data plane and wait for
        // its transport_ready reply.
        let mut client = Self {
            config,
            sender,
            receiver,
            udp_socket,
            aeron_subscriber,
            aeron_udp_port,
            ref_counts: HashMap::new(),
            last_message_sent: Instant::now(),
            transport_ready: false,
            next_request_id: 1,
            pending_events: VecDeque::new(),
        };
        client.negotiate_transport().await?;
        Ok(client)
    }

    /// Returns the immutable settings used by this connection.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Transfers ownership of the negotiated Aeron receiver to the caller.
    ///
    /// This returns `None` for UDP and after the receiver has already been
    /// taken.
    pub fn take_aeron_subscriber(&mut self) -> Option<AeronSubscriber> {
        self.aeron_subscriber.take()
    }

    /// Returns the number of local owners of one confirmed subscription.
    pub fn subscription_count(&self, key: &SubscriptionKey) -> usize {
        self.ref_counts.get(key).copied().unwrap_or_default()
    }

    /// Acquires logical references to one or more subscriptions.
    ///
    /// Only subscriptions whose local count changes from zero to one are sent
    /// to the server. Counts are committed after the server acknowledges the
    /// request.
    pub async fn acquire(&mut self, arg: SubscriptionArg) -> Result<()> {
        arg.validate()?;
        let keys = arg.keys().collect::<Vec<_>>();
        let first = keys
            .iter()
            .filter(|key| self.subscription_count(key) == 0)
            .map(SubscriptionKey::as_arg)
            .collect::<Vec<_>>();
        if !first.is_empty() {
            self.send_request(ControlRequest::subscribe(first)).await?;
        }
        for key in keys {
            *self.ref_counts.entry(key).or_default() += 1;
        }
        Ok(())
    }

    /// Releases logical references to one or more subscriptions.
    ///
    /// Only subscriptions whose local count changes from one to zero are sent
    /// to the server. Counts are committed after the server acknowledges the
    /// request.
    pub async fn release(&mut self, arg: SubscriptionArg) -> Result<()> {
        arg.validate()?;
        let keys = arg.keys().collect::<Vec<_>>();
        let last = keys
            .iter()
            .filter(|key| self.subscription_count(key) == 1)
            .map(SubscriptionKey::as_arg)
            .collect::<Vec<_>>();
        if !last.is_empty() {
            self.send_request(ControlRequest::unsubscribe(last)).await?;
        }
        for key in keys {
            let Some(count) = self.ref_counts.get_mut(&key) else {
                continue;
            };
            *count -= 1;
            if *count == 0 {
                self.ref_counts.remove(&key);
            }
        }
        Ok(())
    }

    /// Requests fresh BBO data for the supplied IDs.
    ///
    /// The reply only acknowledges the request; refreshed market data arrives
    /// later on the negotiated data plane.
    pub async fn refetch_bbo(&mut self, ids: impl IntoIterator<Item = i32>) -> Result<()> {
        self.send_request(ControlRequest::refetch_bbo(ids)).await
    }

    /// Sends one validated request and waits for its correlated reply.
    ///
    /// While waiting, asynchronous stream errors, keepalives, and replies for
    /// other request IDs are buffered for [`Self::poll_control`].
    pub async fn send_request(&mut self, request: ControlRequest) -> Result<()> {
        if !self.transport_ready {
            anyhow::bail!("data transport is not ready");
        }
        request.validate()?;
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        let operation = request.op;
        self.send_text(serde_json::to_string(&ControlRequestEnvelope::new(
            request_id, request,
        ))?)
        .await?;
        self.await_request_reply(request_id, operation).await
    }

    /// Re-sends the current confirmed subscription set.
    pub async fn resubscribe(&mut self) -> Result<()> {
        let args = self
            .ref_counts
            .keys()
            .map(SubscriptionKey::as_arg)
            .collect::<Vec<_>>();
        if !args.is_empty() {
            self.send_request(ControlRequest::subscribe(args)).await?;
        }
        Ok(())
    }

    /// Reopen the configured transports and restore every logical subscription
    /// with one canonical subscribe request.
    pub async fn reconnect(&mut self) -> Result<()> {
        let mut replacement = Self::connect(self.config.clone()).await?;
        replacement.ref_counts = self.ref_counts.clone();
        replacement.resubscribe().await?;
        *self = replacement;
        Ok(())
    }

    /// Sends a WebSocket ping when the configured idle interval has elapsed.
    pub async fn send_ping_if_due(&mut self) -> Result<()> {
        if self.last_message_sent.elapsed() >= self.config.keepalive_interval {
            self.send(Message::Ping(Vec::new().into())).await?;
        }
        Ok(())
    }

    /// Non-blockingly reads one buffered or immediately available control event.
    ///
    /// `Ok(None)` means no event is ready. It does not mean the connection
    /// closed.
    pub async fn poll_control(&mut self) -> Result<Option<ClientEvent>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }
        let next = self.receiver.next().now_or_never();
        let Some(next) = next else {
            return Ok(None);
        };
        let Some(frame) = next else {
            anyhow::bail!("control WebSocket closed");
        };
        match frame? {
            Message::Binary(_) => {
                anyhow::bail!("binary frames are not allowed on the control WebSocket")
            }
            Message::Text(text) => {
                let event = parse_client_event(&text)?;
                Ok(Some(event))
            }
            Message::Ping(payload) => {
                self.send(Message::Pong(payload)).await?;
                Ok(Some(ClientEvent::Keepalive))
            }
            Message::Pong(_) => Ok(Some(ClientEvent::Keepalive)),
            Message::Close(frame) => anyhow::bail!("control WebSocket closed: {frame:?}"),
            _ => Ok(None),
        }
    }

    /// Non-blockingly receives one UDP market-data frame.
    ///
    /// Returns `Ok(None)` when UDP is not selected or no datagram is ready.
    pub fn try_recv_udp(&mut self, buffer: &mut [u8]) -> Result<Option<InboundFrame>> {
        let Some(socket) = &self.udp_socket else {
            return Ok(None);
        };
        match socket.try_recv(buffer) {
            Ok(size) => Ok(Some(InboundFrame {
                bytes: buffer[..size].to_vec(),
                received_at: Instant::now(),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Waits for the next UDP market-data frame.
    ///
    /// Unlike [`Self::try_recv_udp`], this is asynchronous and waits until a
    /// datagram arrives. It returns an error if UDP was not selected.
    pub async fn recv_udp(&mut self, buffer: &mut [u8]) -> Result<InboundFrame> {
        let socket = self
            .udp_socket
            .as_ref()
            .context("the link has no UDP data plane")?;
        let size = socket.recv(buffer).await?;
        Ok(InboundFrame {
            bytes: buffer[..size].to_vec(),
            received_at: Instant::now(),
        })
    }

    /// Returns the local UDP endpoint when UDP is selected.
    pub fn udp_local_addr(&self) -> Result<Option<std::net::SocketAddr>> {
        self.udp_socket
            .as_ref()
            .map(UdpSocket::local_addr)
            .transpose()
            .map_err(Into::into)
    }

    /// Returns the negotiated server UDP endpoint when UDP is selected.
    pub fn udp_peer_addr(&self) -> Result<Option<std::net::SocketAddr>> {
        self.udp_socket
            .as_ref()
            .map(UdpSocket::peer_addr)
            .transpose()
            .map_err(Into::into)
    }

    /// Sends a normal WebSocket close frame.
    pub async fn close(&mut self) -> Result<()> {
        self.send(Message::Close(None)).await
    }

    /// Sends `select_transport` and waits for the server's `transport_ready`.
    ///
    /// Subscription traffic is deliberately unavailable until this completes.
    async fn negotiate_transport(&mut self) -> Result<()> {
        let selection = match &self.config.transport {
            ClientTransportConfig::Udp => TransportSelection::Udp {
                client_port: self
                    .udp_socket
                    .as_ref()
                    .context("UDP socket was not initialized")?
                    .local_addr()?
                    .port(),
            },
            ClientTransportConfig::AeronIpc { stream_id } => TransportSelection::AeronIpc {
                stream_id: *stream_id,
            },
            ClientTransportConfig::AeronUdp { stream_id } => TransportSelection::AeronUdp {
                client_port: self
                    .aeron_udp_port
                    .context("Aeron UDP subscription endpoint was not prepared")?,
                stream_id: *stream_id,
            },
        };
        self.send_text(serde_json::to_string(&SelectTransport::new(selection))?)
            .await?;

        loop {
            let frame = self
                .receiver
                .next()
                .await
                .context("control WebSocket closed during transport negotiation")??;
            match frame {
                Message::Text(text) => match serde_json::from_str::<ControlReply>(&text)? {
                    ControlReply::TransportReady { transport } => {
                        if let TransportReady::Udp { server_address } = transport {
                            self.udp_socket
                                .as_ref()
                                .context("server selected UDP without a client socket")?
                                .connect(server_address)
                                .await?;
                        }
                        self.transport_ready = true;
                        return Ok(());
                    }
                    ControlReply::Error { message } => anyhow::bail!(message),
                    reply => anyhow::bail!(
                        "unexpected control reply during transport negotiation: {reply:?}"
                    ),
                },
                Message::Ping(payload) => self.send(Message::Pong(payload)).await?,
                Message::Close(frame) => {
                    anyhow::bail!("control WebSocket closed during negotiation: {frame:?}")
                }
                _ => {}
            }
        }
    }

    async fn send_text(&mut self, text: String) -> Result<()> {
        self.send(Message::Text(text.into())).await
    }

    /// Receives control messages until the matching request reply arrives.
    ///
    /// The request ID is the correlation boundary. Other valid control
    /// messages are preserved in `pending_events` instead of being lost.
    async fn await_request_reply(
        &mut self,
        request_id: u64,
        operation: crate::protocol::ControlOperation,
    ) -> Result<()> {
        loop {
            let frame = self
                .receiver
                .next()
                .await
                .context("control WebSocket closed while awaiting request acknowledgement")??;
            match frame {
                Message::Binary(_) => {
                    anyhow::bail!("binary frames are not allowed on the control WebSocket")
                }
                Message::Text(text) => {
                    if let Ok(event) = serde_json::from_str::<ControlEvent>(&text) {
                        let error = event.into_stream_error();
                        self.pending_events
                            .push_back(ClientEvent::StreamError(error));
                        continue;
                    }
                    let envelope = serde_json::from_str::<ControlReplyEnvelope>(&text)
                        .context("invalid control WebSocket message")?;
                    if envelope.request_id != request_id {
                        self.pending_events
                            .push_back(ClientEvent::Reply(envelope.reply));
                        continue;
                    }
                    return validate_request_reply(operation, envelope.reply);
                }
                Message::Ping(payload) => {
                    self.send(Message::Pong(payload)).await?;
                    self.pending_events.push_back(ClientEvent::Keepalive);
                }
                Message::Pong(_) => self.pending_events.push_back(ClientEvent::Keepalive),
                Message::Close(frame) => {
                    anyhow::bail!(
                        "control WebSocket closed while awaiting request acknowledgement: {frame:?}"
                    )
                }
                _ => {}
            }
        }
    }

    async fn send(&mut self, message: Message) -> Result<()> {
        self.sender.send(message).await?;
        self.last_message_sent = Instant::now();
        Ok(())
    }
}

fn validate_request_reply(
    operation: crate::protocol::ControlOperation,
    reply: ControlReply,
) -> Result<()> {
    use crate::protocol::ControlOperation;

    match (operation, reply) {
        (ControlOperation::Subscribe, ControlReply::Subscribed { .. })
        | (ControlOperation::Unsubscribe, ControlReply::Unsubscribed { .. })
        | (ControlOperation::RefetchBbo, ControlReply::Refetched { .. }) => Ok(()),
        (_, ControlReply::Error { message }) => anyhow::bail!(message),
        (_, reply) => anyhow::bail!("unexpected control reply for {operation:?}: {reply:?}"),
    }
}

#[derive(Debug)]
/// An asynchronous control-plane event not consumed by a request wait.
pub enum ClientEvent {
    /// A reply for a request other than the one currently being awaited.
    Reply(ControlReply),
    /// A service runtime error, optionally scoped to an ID and stream.
    StreamError(StreamError),
    /// A ping or pong was handled.
    Keepalive,
}

fn parse_client_event(text: &str) -> Result<ClientEvent> {
    if let Ok(event) = serde_json::from_str::<ControlEvent>(text) {
        return Ok(ClientEvent::StreamError(event.into_stream_error()));
    }
    let envelope = serde_json::from_str::<ControlReplyEnvelope>(text)
        .context("invalid control WebSocket message")?;
    Ok(ClientEvent::Reply(envelope.reply))
}
