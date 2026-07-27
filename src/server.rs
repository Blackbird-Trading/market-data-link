use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, UdpSocket},
    sync::{OnceCell, RwLock, mpsc},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::protocol::{
    AeronConfig, ControlOperation, ControlReply, ControlRequest, SelectTransport, SubscriptionKey,
    TransportReady, TransportSelection,
};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct LinkServerConfig {
    pub tcp_bind_addr: String,
    pub aeron_enabled: bool,
    pub active_client_log_interval: Duration,
}

impl LinkServerConfig {
    pub fn tcp(tcp_bind_addr: impl Into<String>) -> Self {
        Self {
            tcp_bind_addr: tcp_bind_addr.into(),
            aeron_enabled: false,
            active_client_log_interval: Duration::from_secs(30),
        }
    }
}

pub struct BackendSession {
    pub client_id: u64,
    pub outbound: Option<mpsc::Receiver<Vec<u8>>>,
    pub control: Option<mpsc::Receiver<SessionControl>>,
}

impl BackendSession {
    pub fn control_only(client_id: u64) -> Self {
        Self {
            client_id,
            outbound: None,
            control: None,
        }
    }
}

#[derive(Debug)]
pub enum SessionControl {
    Error(String),
    Close,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishReport {
    pub delivered: usize,
    pub full: usize,
    pub closed: usize,
}

struct RoutedSession {
    subscriptions: HashSet<SubscriptionKey>,
    sender: mpsc::Sender<Vec<u8>>,
}

/// A reusable per-session subscription table and bounded output router.
///
/// Backends remain responsible for domain validation. Once a request succeeds,
/// they apply it here and publish frames by their `(id, stream)` routing key.
#[derive(Clone, Default)]
pub struct SubscriptionRouter {
    sessions: Arc<StdRwLock<HashMap<u64, RoutedSession>>>,
}

impl SubscriptionRouter {
    pub fn connect(&self, client_id: u64, capacity: usize) -> BackendSession {
        let (sender, outbound) = mpsc::channel(capacity);
        self.sessions
            .write()
            .expect("subscription router lock poisoned")
            .insert(
                client_id,
                RoutedSession {
                    subscriptions: HashSet::new(),
                    sender,
                },
            );
        BackendSession {
            client_id,
            outbound: Some(outbound),
            control: None,
        }
    }

    pub fn apply(&self, client_id: u64, request: &ControlRequest) -> Result<()> {
        let mut sessions = self
            .sessions
            .write()
            .expect("subscription router lock poisoned");
        let session = sessions
            .get_mut(&client_id)
            .with_context(|| format!("unknown link client {client_id}"))?;
        match request.op {
            ControlOperation::Subscribe => session.subscriptions.extend(request.keys()),
            ControlOperation::Unsubscribe => {
                for key in request.keys() {
                    session.subscriptions.remove(&key);
                }
            }
            ControlOperation::RefetchBbo => {}
        }
        Ok(())
    }

    pub fn disconnect(&self, client_id: u64) {
        self.sessions
            .write()
            .expect("subscription router lock poisoned")
            .remove(&client_id);
    }

    pub fn publish(&self, key: &SubscriptionKey, bytes: &[u8]) -> PublishReport {
        let sessions = self
            .sessions
            .read()
            .expect("subscription router lock poisoned");
        let mut report = PublishReport::default();
        let mut closed = Vec::new();
        for (client_id, session) in sessions.iter() {
            if !session
                .subscriptions
                .iter()
                .any(|subscription| subscription.id == key.id && subscription.stream == key.stream)
            {
                continue;
            }
            match session.sender.try_send(bytes.to_vec()) {
                Ok(()) => report.delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => report.full += 1,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    report.closed += 1;
                    closed.push(*client_id);
                }
            }
        }
        drop(sessions);
        if !closed.is_empty() {
            let mut sessions = self
                .sessions
                .write()
                .expect("subscription router lock poisoned");
            for client_id in closed {
                sessions.remove(&client_id);
            }
        }
        report
    }

    pub fn session_count(&self) -> usize {
        self.sessions
            .read()
            .expect("subscription router lock poisoned")
            .len()
    }
}

#[async_trait]
pub trait SubscriptionBackend: Clone + Send + Sync + 'static {
    async fn connect(&self, suggested_client_id: u64) -> Result<BackendSession>;
    async fn request(&self, client_id: u64, request: ControlRequest) -> Result<ControlReply>;
    async fn configure_aeron(&self, _client_id: u64, _config: AeronConfig) -> Result<()> {
        anyhow::bail!("Aeron is not supported by this backend")
    }
    async fn disconnect(&self, client_id: u64);
}

pub struct LinkServer<B> {
    config: LinkServerConfig,
    backend: B,
}

impl<B: SubscriptionBackend> LinkServer<B> {
    pub fn new(config: LinkServerConfig, backend: B) -> Self {
        Self { config, backend }
    }

    pub async fn run(self) -> Result<()> {
        let tcp_listener = TcpListener::bind(&self.config.tcp_bind_addr)
            .await
            .with_context(|| {
                format!(
                    "failed to bind WebSocket server at {}",
                    self.config.tcp_bind_addr
                )
            })?;
        info!(address = %tcp_listener.local_addr()?, "link server listening");

        let udp_socket = Arc::new(OnceCell::<Arc<UdpSocket>>::new());
        let active_clients = Arc::new(RwLock::new(BTreeMap::<u64, String>::new()));
        let mut status_interval = tokio::time::interval(self.config.active_client_log_interval);

        loop {
            tokio::select! {
                accepted = tcp_listener.accept() => {
                    let (stream, peer) = accepted?;
                    let local_ip = stream.local_addr()?.ip();
                    self.spawn_session(
                        stream,
                        peer.to_string(),
                        peer.ip(),
                        local_ip,
                        udp_socket.clone(),
                        active_clients.clone(),
                    );
                }
                _ = status_interval.tick() => {
                    let clients = active_clients.read().await;
                    info!(count = clients.len(), clients = ?*clients, "active link clients");
                }
            }
        }
    }

    fn spawn_session<T>(
        &self,
        stream: T,
        peer: String,
        peer_ip: IpAddr,
        local_ip: IpAddr,
        udp_socket: Arc<OnceCell<Arc<UdpSocket>>>,
        active_clients: Arc<RwLock<BTreeMap<u64, String>>>,
    ) where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let backend = self.backend.clone();
        let aeron_enabled = self.config.aeron_enabled;
        tokio::spawn(async move {
            let suggested_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
            let result = run_session(
                stream,
                peer.clone(),
                peer_ip,
                local_ip,
                udp_socket,
                backend.clone(),
                aeron_enabled,
                suggested_id,
                active_clients.clone(),
            )
            .await;
            if let Err(error) = result {
                warn!(%peer, ?error, "link session ended with error");
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session<T, B>(
    stream: T,
    peer: String,
    peer_ip: IpAddr,
    local_ip: IpAddr,
    udp_socket: Arc<OnceCell<Arc<UdpSocket>>>,
    backend: B,
    aeron_enabled: bool,
    suggested_id: u64,
    active_clients: Arc<RwLock<BTreeMap<u64, String>>>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
    B: SubscriptionBackend,
{
    let mut websocket = accept_async(stream).await?;
    let mut session = backend.connect(suggested_id).await?;
    let client_id = session.client_id;
    active_clients.write().await.insert(client_id, peer.clone());
    info!(client_id, %peer, "link client connected");
    let mut udp_target = None;
    let mut external_data_plane = false;
    let mut transport_ready = false;

    let result: Result<()> = async {
        loop {
            tokio::select! {
            frame = websocket.next() => {
                let Some(frame) = frame else { break };
                match frame? {
                    Message::Text(text) => {
                        if let Ok(selection) = serde_json::from_str::<SelectTransport>(&text) {
                            if transport_ready {
                                send_reply(&mut websocket, &ControlReply::error("transport has already been selected")).await?;
                                continue;
                            }
                            let ready = match selection.transport {
                                TransportSelection::WebSocket => {
                                    external_data_plane = false;
                                    TransportReady::WebSocket
                                }
                                TransportSelection::Udp { client_port } => {
                                    if client_port == 0 {
                                        send_reply(&mut websocket, &ControlReply::error("UDP client port must be non-zero")).await?;
                                        continue;
                                    }
                                    let socket = udp_socket
                                        .get_or_try_init(|| async {
                                            Ok::<_, std::io::Error>(Arc::new(
                                                UdpSocket::bind("0.0.0.0:0").await?,
                                            ))
                                        })
                                        .await?;
                                    udp_target = Some(SocketAddr::new(peer_ip, client_port));
                                    external_data_plane = false;
                                    TransportReady::Udp {
                                        server_address: SocketAddr::new(
                                            local_ip,
                                            socket.local_addr()?.port(),
                                        ),
                                    }
                                }
                                selection @ (TransportSelection::AeronIpc { .. }
                                | TransportSelection::AeronUdp { .. }) => {
                                    if !aeron_enabled {
                                        send_reply(&mut websocket, &ControlReply::error(
                                            "Aeron transport selected while [aeron].enabled is false"
                                        )).await?;
                                        continue;
                                    }
                                    let config = selection
                                        .aeron_config()
                                        .expect("Aeron selection must produce an Aeron config");
                                    if let Err(error) = backend.configure_aeron(client_id, config).await {
                                        send_reply(&mut websocket, &ControlReply::error(error)).await?;
                                        continue;
                                    }
                                    external_data_plane = true;
                                    match selection {
                                        TransportSelection::AeronIpc { stream_id } => {
                                            TransportReady::AeronIpc { stream_id }
                                        }
                                        TransportSelection::AeronUdp { endpoint, stream_id } => {
                                            TransportReady::AeronUdp { endpoint, stream_id }
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            };
                            transport_ready = true;
                            send_reply(
                                &mut websocket,
                                &ControlReply::TransportReady { transport: ready },
                            )
                            .await?;
                            continue;
                        }
                        let request = match serde_json::from_str::<ControlRequest>(&text) {
                            Ok(request) => request,
                            Err(error) => {
                                send_reply(&mut websocket, &ControlReply::error(format!("invalid control request: {error}"))).await?;
                                continue;
                            }
                        };
                        if let Err(error) = request.validate() {
                            send_reply(&mut websocket, &ControlReply::error(error)).await?;
                            continue;
                        }
                        if !transport_ready {
                            send_reply(&mut websocket, &ControlReply::error(
                                "select_transport must succeed before subscriptions"
                            )).await?;
                            continue;
                        }
                        let reply = backend
                            .request(client_id, request)
                            .await
                            .unwrap_or_else(ControlReply::error);
                        send_reply(&mut websocket, &reply).await?;
                    }
                    Message::Binary(_) => {
                        send_reply(&mut websocket, &ControlReply::error("binary client messages are unsupported")).await?;
                    }
                    Message::Ping(payload) => websocket.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            data = recv_optional(&mut session.outbound), if session.outbound.is_some() => {
                let Some(data) = data else { break };
                if external_data_plane {
                    continue;
                } else if let (Some(socket), Some(target)) = (udp_socket.get(), udp_target) {
                    socket.send_to(&data, target).await?;
                } else {
                    websocket.send(Message::Binary(data.into())).await?;
                }
            }
            control = recv_optional(&mut session.control), if session.control.is_some() => {
                match control {
                    Some(SessionControl::Error(message)) => {
                        send_reply(&mut websocket, &ControlReply::error(message)).await?;
                    }
                    Some(SessionControl::Close) | None => break,
                }
            }
            }
        }
        Ok(())
    }
    .await;

    active_clients.write().await.remove(&client_id);
    backend.disconnect(client_id).await;
    debug!(client_id, %peer, "link client disconnected");
    result
}

async fn recv_optional<T>(receiver: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

async fn send_reply<T>(websocket: &mut WebSocketStream<T>, reply: &ControlReply) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    websocket
        .send(Message::Text(serde_json::to_string(reply)?.into()))
        .await?;
    Ok(())
}
