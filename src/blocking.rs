use std::{
    net::{SocketAddr, TcpStream, UdpSocket},
    time::Duration,
};

use anyhow::{Context, Result};
use tungstenite::{Message, WebSocket};

use crate::FrameFilter;
use crate::protocol::{
    ClientTransportConfig, ControlReply, ControlRequest, SelectTransport, TransportReady,
    TransportSelection,
};

pub struct BlockingLink {
    control: WebSocket<TcpStream>,
    udp: Option<UdpSocket>,
    frame_filter: FrameFilter,
}

impl std::fmt::Debug for BlockingLink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlockingLink")
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

impl BlockingLink {
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
            ClientTransportConfig::WebSocket => TransportSelection::WebSocket,
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
            frame_filter: FrameFilter::default(),
        })
    }

    pub fn send(&mut self, message: Message) -> tungstenite::Result<()> {
        let request = match &message {
            Message::Text(text) => serde_json::from_str::<ControlRequest>(text).ok(),
            _ => None,
        };
        self.control.send(message)?;
        if let Some(request) = request {
            self.frame_filter.apply(&request);
        }
        Ok(())
    }

    pub fn flush(&mut self) -> tungstenite::Result<()> {
        self.control.flush()
    }

    pub fn read(&mut self) -> tungstenite::Result<Message> {
        if let Some(socket) = &self.udp {
            let mut bytes = vec![0; 65_535];
            match socket.recv(&mut bytes) {
                Ok(size) => {
                    bytes.truncate(size);
                    if self.frame_filter.accepts(&bytes) {
                        return Ok(Message::Binary(bytes.into()));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(tungstenite::Error::Io(error)),
            }
        }
        loop {
            let message = self.control.read()?;
            if let Message::Binary(bytes) = &message
                && !self.frame_filter.accepts(bytes)
            {
                continue;
            }
            return Ok(message);
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
