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
        aeron_enabled: bool,
    ) -> Result<Self> {
        if transport.is_aeron() && !aeron_enabled {
            anyhow::bail!("Aeron transport selected while [aeron].enabled is false");
        }
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

        let selection = match transport {
            ClientTransportConfig::Udp => TransportSelection::Udp {
                client_port: udp
                    .as_ref()
                    .expect("UDP socket must exist")
                    .local_addr()?
                    .port(),
            },
            ClientTransportConfig::AeronIpc { stream_id } => {
                TransportSelection::AeronIpc { stream_id }
            }
            ClientTransportConfig::AeronUdp {
                endpoint,
                stream_id,
            } => TransportSelection::AeronUdp {
                endpoint,
                stream_id,
            },
        };
        control.send(Message::Text(
            serde_json::to_string(&SelectTransport::new(selection))?.into(),
        ))?;
        loop {
            match control.read()? {
                Message::Text(text) => match serde_json::from_str::<ControlReply>(&text)? {
                    ControlReply::TransportReady { transport } => {
                        if let TransportReady::Udp { server_address } = transport {
                            udp.as_ref()
                                .expect("server returned UDP for a non-UDP client")
                                .connect(server_address)?;
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
            next_request_id: 1,
            pending_requests: HashSet::new(),
        })
    }

    pub fn send(&mut self, message: Message) -> tungstenite::Result<()> {
        self.control.send(message)
    }

    /// Sends a correlated control request without blocking for its reply.
    ///
    /// The request remains pending until [`Self::poll`] returns its correlated
    /// reply.
    pub fn send_request(&mut self, request: ControlRequest) -> Result<u64> {
        request.validate()?;
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        let text = serde_json::to_string(&ControlRequestEnvelope::new(
            Some(request_id),
            request.clone(),
        ))?;
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
                    if let Some(request_id) = envelope.request_id {
                        self.pending_requests.remove(&request_id);
                    }
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
