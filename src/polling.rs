use std::{
    collections::HashSet,
    net::{SocketAddr, TcpStream, UdpSocket},
    time::Duration,
};

use anyhow::{Context, Result};
use tungstenite::{Message, WebSocket};

use crate::protocol::{
    ClientTransportConfig, ControlEvent, ControlReply, ControlReplyEnvelope, ControlRequest,
    ControlRequestEnvelope, SelectTransport, StreamError, TransportReady, TransportSelection,
};
use crate::transport::InboundFrame;
#[cfg(feature = "aeron")]
use crate::transport::{AeronClient, AeronSubscriber};

#[derive(Debug)]
pub enum PollEvent {
    Data(InboundFrame),
    Reply(ControlReplyEnvelope),
    StreamError(StreamError),
    Keepalive,
}

pub struct PollingClient {
    control: WebSocket<TcpStream>,
    udp: Option<UdpSocket>,
    #[cfg(feature = "aeron")]
    aeron_subscriber: Option<AeronSubscriber>,
    next_request_id: u64,
    pending_requests: HashSet<u64>,
}

impl std::fmt::Debug for PollingClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PollingClient")
            .field(
                "udp",
                &self
                    .udp
                    .as_ref()
                    .and_then(|socket| socket.local_addr().ok()),
            )
            .finish_non_exhaustive()
    }
}

impl PollingClient {
    pub fn connect_tcp(
        address: &str,
        timeout: Duration,
        transport: ClientTransportConfig,
    ) -> Result<Self> {
        #[cfg(feature = "aeron")]
        let (aeron_subscriber, aeron_udp_port) = match &transport {
            ClientTransportConfig::AeronUdp { stream_id } => {
                let aeron = AeronClient::connect()
                    .context("failed to connect to the Aeron media driver")?;
                let (subscriber, port) = aeron.ephemeral_udp_subscriber(*stream_id)?;
                (Some(subscriber), Some(port))
            }
            _ => (None, None),
        };
        #[cfg(not(feature = "aeron"))]
        let aeron_udp_port = match &transport {
            ClientTransportConfig::AeronUdp { .. } => {
                anyhow::bail!("Aeron UDP requires the market-data-link `aeron` feature")
            }
            _ => None,
        };
        let socket_target = address
            .strip_prefix("ws://")
            .or_else(|| address.strip_prefix("wss://"))
            .unwrap_or(address);
        let socket_address: SocketAddr = socket_target
            .parse()
            .with_context(|| format!("invalid socket address {address}"))?;
        let stream = TcpStream::connect_timeout(&socket_address, timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let (mut control, _) = tungstenite::client::client(
            if address.starts_with("ws://") || address.starts_with("wss://") {
                address.to_string()
            } else {
                format!("ws://{address}")
            },
            stream,
        )?;

        let udp = match &transport {
            ClientTransportConfig::Udp => {
                let socket = UdpSocket::bind("0.0.0.0:0")?;
                Some(socket)
            }
            _ => None,
        };

        let selection = match &transport {
            ClientTransportConfig::Udp => TransportSelection::Udp {
                client_port: udp
                    .as_ref()
                    .context("UDP socket was not prepared")?
                    .local_addr()?
                    .port(),
            },
            ClientTransportConfig::AeronIpc { stream_id } => TransportSelection::AeronIpc {
                stream_id: *stream_id,
            },
            ClientTransportConfig::AeronUdp { stream_id } => TransportSelection::AeronUdp {
                client_port: aeron_udp_port
                    .context("Aeron UDP subscription endpoint was not prepared")?,
                stream_id: *stream_id,
            },
        };
        control.send(Message::Text(
            serde_json::to_string(&SelectTransport::new(selection))?.into(),
        ))?;
        loop {
            match control.read()? {
                Message::Text(text) => match serde_json::from_str::<ControlReply>(&text)? {
                    ControlReply::TransportReady { transport: ready } => {
                        match (&transport, ready) {
                            (
                                ClientTransportConfig::Udp,
                                TransportReady::Udp { server_address },
                            ) => {
                                udp.as_ref()
                                    .context("UDP socket was not prepared")?
                                    .connect(server_address)?;
                            }
                            (
                                ClientTransportConfig::AeronIpc {
                                    stream_id: requested,
                                },
                                TransportReady::AeronIpc {
                                    stream_id: negotiated,
                                },
                            ) if requested == &negotiated => {}
                            (
                                ClientTransportConfig::AeronUdp {
                                    stream_id: requested,
                                },
                                TransportReady::AeronUdp {
                                    endpoint,
                                    stream_id: negotiated,
                                },
                            ) if requested == &negotiated
                                && aeron_udp_port == Some(endpoint.port()) => {}
                            (requested, negotiated) => {
                                anyhow::bail!(
                                    "server returned incompatible transport during negotiation: \
                                     requested {requested:?}, returned {negotiated:?}"
                                )
                            }
                        }
                        break;
                    }
                    ControlReply::Error { message } => anyhow::bail!(message),
                    reply => {
                        anyhow::bail!("unexpected reply during transport negotiation: {reply:?}")
                    }
                },
                Message::Ping(payload) => control.send(Message::Pong(payload))?,
                Message::Close(frame) => {
                    anyhow::bail!("control WebSocket closed during negotiation: {frame:?}")
                }
                _ => {}
            }
        }
        control.get_mut().set_read_timeout(None)?;
        control.get_mut().set_write_timeout(None)?;
        control.get_mut().set_nonblocking(true)?;
        if let Some(socket) = &udp {
            socket.set_nonblocking(true)?;
        }
        Ok(Self {
            control,
            udp,
            #[cfg(feature = "aeron")]
            aeron_subscriber,
            next_request_id: 1,
            pending_requests: HashSet::new(),
        })
    }

    pub fn send(&mut self, message: Message) -> tungstenite::Result<()> {
        self.control.send(message)
    }

    #[cfg(feature = "aeron")]
    pub fn take_aeron_subscriber(&mut self) -> Option<AeronSubscriber> {
        self.aeron_subscriber.take()
    }

    /// Sends a correlated control request without blocking for its reply.
    ///
    /// The request remains pending until [`Self::poll`] returns its correlated
    /// reply.
    pub fn send_request(&mut self, request: ControlRequest) -> Result<u64> {
        request.validate()?;
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        let text =
            serde_json::to_string(&ControlRequestEnvelope::new(request_id, request.clone()))?;
        self.control.send(Message::Text(text.into()))?;
        self.pending_requests.insert(request_id);
        Ok(request_id)
    }

    pub fn flush(&mut self) -> tungstenite::Result<()> {
        self.control.flush()
    }

    pub fn poll(&mut self) -> Result<Option<PollEvent>> {
        if let Some(socket) = &self.udp {
            let mut bytes = vec![0; 65_535];
            match socket.recv(&mut bytes) {
                Ok(size) => {
                    bytes.truncate(size);
                    return Ok(Some(PollEvent::Data(InboundFrame {
                        bytes,
                        received_at: std::time::Instant::now(),
                    })));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
        }
        loop {
            let message = match self.control.read() {
                Ok(message) => message,
                Err(tungstenite::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error.into()),
            };
            match message {
                Message::Binary(_) => {
                    anyhow::bail!("binary frames are not allowed on the control WebSocket")
                }
                Message::Text(text) => {
                    if let Ok(event) = serde_json::from_str::<ControlEvent>(&text) {
                        let error = event.into_stream_error();
                        return Ok(Some(PollEvent::StreamError(error)));
                    }
                    let envelope = serde_json::from_str::<ControlReplyEnvelope>(&text)
                        .context("invalid control WebSocket message")?;
                    self.pending_requests.remove(&envelope.request_id);
                    return Ok(Some(PollEvent::Reply(envelope)));
                }
                Message::Ping(payload) => {
                    self.control.send(Message::Pong(payload))?;
                    return Ok(Some(PollEvent::Keepalive));
                }
                Message::Pong(_) => return Ok(Some(PollEvent::Keepalive)),
                Message::Close(frame) => {
                    anyhow::bail!("control WebSocket closed: {frame:?}")
                }
                _ => continue,
            }
        }
    }

    pub fn probe(&mut self) -> bool {
        self.send(Message::Ping(Vec::new().into()))
            .and_then(|_| self.flush())
            .is_ok()
    }

    pub fn has_udp(&self) -> bool {
        self.udp.is_some()
    }
}
