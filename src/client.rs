use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::{
    FutureExt, SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::{TcpStream, UdpSocket};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    frame_filter::FrameFilter,
    protocol::{
        ClientTransportConfig, ControlReply, ControlRequest, SelectTransport, SubscriptionArg,
        SubscriptionKey, TransportReady, TransportSelection,
    },
    transport::{ControlEndpoint, InboundFrame},
};

const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct LinkClientConfig {
    pub endpoint: ControlEndpoint,
    pub transport: ClientTransportConfig,
    pub aeron_enabled: bool,
    pub keepalive_interval: Duration,
}

impl LinkClientConfig {
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

pub struct LinkClient {
    config: LinkClientConfig,
    sender: Sender,
    receiver: Receiver,
    udp_socket: Option<UdpSocket>,
    ref_counts: HashMap<SubscriptionKey, usize>,
    frame_filter: FrameFilter,
    last_message_sent: Instant,
    transport_ready: bool,
}

impl std::fmt::Debug for LinkClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LinkClient")
            .field("config", &self.config)
            .field("subscriptions", &self.ref_counts)
            .finish_non_exhaustive()
    }
}

impl LinkClient {
    pub async fn connect(config: LinkClientConfig) -> Result<Self> {
        if config.transport.is_aeron() && !config.aeron_enabled {
            anyhow::bail!("Aeron transport selected while [aeron].enabled is false");
        }
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

        let mut client = Self {
            config,
            sender,
            receiver,
            udp_socket,
            ref_counts: HashMap::new(),
            frame_filter: FrameFilter::default(),
            last_message_sent: Instant::now(),
            transport_ready: false,
        };
        client.negotiate_transport().await?;
        Ok(client)
    }

    pub fn config(&self) -> &LinkClientConfig {
        &self.config
    }

    pub fn subscription_count(&self, key: &SubscriptionKey) -> usize {
        self.ref_counts.get(key).copied().unwrap_or_default()
    }

    pub async fn acquire(&mut self, arg: SubscriptionArg) -> Result<()> {
        arg.validate()?;
        let mut first = Vec::new();
        for key in arg.keys() {
            let count = self.ref_counts.entry(key.clone()).or_default();
            if *count == 0 {
                first.push(key.as_arg());
            }
            *count += 1;
        }
        if !first.is_empty() {
            let request = ControlRequest::subscribe(first);
            self.send_request(request.clone()).await?;
            self.frame_filter.apply(&request);
        }
        Ok(())
    }

    pub async fn release(&mut self, arg: SubscriptionArg) -> Result<()> {
        arg.validate()?;
        let mut last = Vec::new();
        for key in arg.keys() {
            let Some(count) = self.ref_counts.get_mut(&key) else {
                continue;
            };
            *count -= 1;
            if *count == 0 {
                self.ref_counts.remove(&key);
                last.push(key.as_arg());
            }
        }
        if !last.is_empty() {
            let request = ControlRequest::unsubscribe(last);
            self.send_request(request.clone()).await?;
            self.frame_filter.apply(&request);
        }
        Ok(())
    }

    pub async fn refetch_bbo(&mut self, ids: impl IntoIterator<Item = i32>) -> Result<()> {
        self.send_request(ControlRequest::refetch_bbo(ids)).await
    }

    pub async fn send_request(&mut self, request: ControlRequest) -> Result<()> {
        if !self.transport_ready {
            anyhow::bail!("data transport is not ready");
        }
        request.validate()?;
        self.send_text(serde_json::to_string(&request)?).await
    }

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
        replacement
            .frame_filter
            .replace_subscriptions(replacement.ref_counts.keys().cloned());
        replacement.resubscribe().await?;
        *self = replacement;
        Ok(())
    }

    pub async fn send_ping_if_due(&mut self) -> Result<()> {
        if self.last_message_sent.elapsed() >= self.config.keepalive_interval {
            self.send(Message::Ping(Vec::new().into())).await?;
        }
        Ok(())
    }

    pub async fn poll_control(&mut self) -> Result<Option<ClientEvent>> {
        loop {
            let next = self.receiver.next().now_or_never();
            let Some(next) = next else {
                return Ok(None);
            };
            let Some(frame) = next else {
                anyhow::bail!("control WebSocket closed");
            };
            match frame? {
                Message::Binary(bytes) => {
                    if !self.frame_filter.accepts(&bytes) {
                        continue;
                    }
                    return Ok(Some(ClientEvent::Data(InboundFrame {
                        bytes: bytes.to_vec(),
                        received_at: Instant::now(),
                    })));
                }
                Message::Text(text) => {
                    let reply = serde_json::from_str::<ControlReply>(&text)
                        .map(ClientEvent::Reply)
                        .unwrap_or_else(|_| ClientEvent::Text(text.to_string()));
                    return Ok(Some(reply));
                }
                Message::Ping(payload) => {
                    self.send(Message::Pong(payload)).await?;
                    return Ok(Some(ClientEvent::Keepalive));
                }
                Message::Pong(_) => return Ok(Some(ClientEvent::Keepalive)),
                Message::Close(frame) => anyhow::bail!("control WebSocket closed: {frame:?}"),
                _ => return Ok(None),
            }
        }
    }

    pub fn try_recv_udp(&mut self, buffer: &mut [u8]) -> Result<Option<InboundFrame>> {
        let Some(socket) = &self.udp_socket else {
            return Ok(None);
        };
        loop {
            match socket.try_recv(buffer) {
                Ok(size) if self.frame_filter.accepts(&buffer[..size]) => {
                    return Ok(Some(InboundFrame {
                        bytes: buffer[..size].to_vec(),
                        received_at: Instant::now(),
                    }));
                }
                Ok(_) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub async fn recv_udp(&mut self, buffer: &mut [u8]) -> Result<InboundFrame> {
        let socket = self
            .udp_socket
            .as_ref()
            .context("the link has no UDP data plane")?;
        loop {
            let size = socket.recv(buffer).await?;
            if self.frame_filter.accepts(&buffer[..size]) {
                return Ok(InboundFrame {
                    bytes: buffer[..size].to_vec(),
                    received_at: Instant::now(),
                });
            }
        }
    }

    pub fn udp_local_addr(&self) -> Result<Option<std::net::SocketAddr>> {
        self.udp_socket
            .as_ref()
            .map(UdpSocket::local_addr)
            .transpose()
            .map_err(Into::into)
    }

    pub fn udp_peer_addr(&self) -> Result<Option<std::net::SocketAddr>> {
        self.udp_socket
            .as_ref()
            .map(UdpSocket::peer_addr)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn close(&mut self) -> Result<()> {
        self.send(Message::Close(None)).await
    }

    async fn negotiate_transport(&mut self) -> Result<()> {
        let selection = match &self.config.transport {
            ClientTransportConfig::WebSocket => TransportSelection::WebSocket,
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
            ClientTransportConfig::AeronUdp {
                endpoint,
                stream_id,
            } => TransportSelection::AeronUdp {
                endpoint: *endpoint,
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

    async fn send(&mut self, message: Message) -> Result<()> {
        self.sender.send(message).await?;
        self.last_message_sent = Instant::now();
        Ok(())
    }
}

#[derive(Debug)]
pub enum ClientEvent {
    Data(InboundFrame),
    Reply(ControlReply),
    Text(String),
    Keepalive,
}
