//! Server-side session lifecycle and UDP routing.
//!
//! The network flow is centralized here:
//!
//! 1. [`Server::run`] accepts a TCP connection.
//! 2. The connection is upgraded to WebSocket and
//!    [`ServerHandler::open_session`] creates service state.
//! 3. The client selects a data transport.
//! 4. Each correlated request is validated and passed to
//!    [`ServerHandler::handle_request`].
//! 5. The resulting reply is sent with the original request ID.
//! 6. Disconnect always finishes with [`ServerHandler::close_session`].

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub mod aeron_publisher;

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
    AeronConfig, ControlEvent, ControlOperation, ControlReply, ControlReplyEnvelope,
    ControlRequest, ControlRequestEnvelope, SelectTransport, StreamError, SubscriptionKey,
    TransportReady, TransportSelection,
};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
/// Process-wide settings for the link server.
pub struct ServerConfig {
    /// Address on which the WebSocket control listener binds.
    pub tcp_bind_addr: String,
    /// Whether clients may select Aeron as their data plane.
    pub aeron_enabled: bool,
    /// Interval between active-session summaries.
    pub active_client_log_interval: Duration,
}

impl ServerConfig {
    /// Creates a TCP/WebSocket configuration with Aeron disabled.
    pub fn tcp(tcp_bind_addr: impl Into<String>) -> Self {
        Self {
            tcp_bind_addr: tcp_bind_addr.into(),
            aeron_enabled: false,
            active_client_log_interval: Duration::from_secs(30),
        }
    }
}

/// Per-session receivers created by the service handler.
///
/// The server owns and drains these receivers; the handler retains the
/// corresponding senders.
pub struct SessionChannels {
    pub client_id: u64,
    pub udp_outbound: Option<mpsc::Receiver<Vec<u8>>>,
    pub control: Option<mpsc::Receiver<SessionControl>>,
}

impl SessionChannels {
    /// Creates a session that uses only WebSocket control messages.
    pub fn control_only(client_id: u64) -> Self {
        Self {
            client_id,
            udp_outbound: None,
            control: None,
        }
    }
}

#[derive(Debug)]
/// Reliable service-to-client commands sent on the control WebSocket.
pub enum SessionControl {
    StreamError(StreamError),
    Close,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Result of attempting UDP fanout to matching sessions.
pub struct PublishReport {
    pub delivered: usize,
    pub full: usize,
    pub closed: usize,
}

struct RoutedSession {
    subscriptions: HashSet<SubscriptionKey>,
    sender: Option<mpsc::Sender<Vec<u8>>>,
    control: mpsc::Sender<SessionControl>,
}

/// A reusable per-session subscription table and bounded output router.
///
/// Handlers remain responsible for domain validation. Once a request succeeds,
/// they apply it here and publish frames by their `(id, stream)` routing key.
#[derive(Clone, Default)]
pub struct ClientRouter {
    sessions: Arc<StdRwLock<HashMap<u64, RoutedSession>>>,
}

impl ClientRouter {
    /// Registers a client and returns the bounded receivers consumed by
    /// [`Server`].
    pub fn register_client(&self, client_id: u64, capacity: usize) -> SessionChannels {
        let (sender, udp_outbound) = mpsc::channel(capacity);
        let (control_sender, control) = mpsc::channel(capacity);
        self.sessions
            .write()
            .expect("subscription router lock poisoned")
            .insert(
                client_id,
                RoutedSession {
                    subscriptions: HashSet::new(),
                    sender: Some(sender),
                    control: control_sender,
                },
            );
        SessionChannels {
            client_id,
            udp_outbound: Some(udp_outbound),
            control: Some(control),
        }
    }

    /// Applies a successfully handled request to the client's routing table.
    pub fn update_subscriptions(&self, client_id: u64, request: &ControlRequest) -> Result<()> {
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

    /// Removes all routing and control state for a client.
    pub fn remove_client(&self, client_id: u64) {
        self.sessions
            .write()
            .expect("subscription router lock poisoned")
            .remove(&client_id);
    }

    /// Disables UDP delivery while retaining control-event routing and the
    /// client's confirmed subscription set.
    pub fn disable_udp(&self, client_id: u64) {
        if let Some(session) = self
            .sessions
            .write()
            .expect("subscription router lock poisoned")
            .get_mut(&client_id)
        {
            session.sender = None;
        }
    }

    pub fn send_data(&self, key: &SubscriptionKey, bytes: &[u8]) -> PublishReport {
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
            let Some(sender) = &session.sender else {
                continue;
            };
            match sender.try_send(bytes.to_vec()) {
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

    /// Sends a refetched frame to one client without broadcasting it to every
    /// subscriber of the same stream.
    pub fn send_data_to_client(&self, client_id: u64, bytes: &[u8]) -> PublishReport {
        let sessions = self
            .sessions
            .read()
            .expect("subscription router lock poisoned");
        let mut report = PublishReport::default();
        let Some(session) = sessions.get(&client_id) else {
            report.closed = 1;
            return report;
        };
        let Some(sender) = &session.sender else {
            return report;
        };
        match sender.try_send(bytes.to_vec()) {
            Ok(()) => report.delivered = 1,
            Err(mpsc::error::TrySendError::Full(_)) => report.full = 1,
            Err(mpsc::error::TrySendError::Closed(_)) => report.closed = 1,
        }
        report
    }

    /// Sends market status once to each UDP client subscribed to any stream
    /// for the affected market.
    pub fn send_market_status(&self, market_id: i32, bytes: &[u8]) -> PublishReport {
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
                .any(|subscription| subscription.id == market_id)
            {
                continue;
            }
            let Some(sender) = &session.sender else {
                continue;
            };
            match sender.try_send(bytes.to_vec()) {
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

    /// Publishes a reliable control-plane error to matching subscribers.
    ///
    /// An incomplete scope is global. A full control queue removes the
    /// session so the server observes channel closure and disconnects it
    /// instead of silently losing the error.
    pub fn send_error(&self, error: &StreamError) -> PublishReport {
        let sessions = self
            .sessions
            .read()
            .expect("subscription router lock poisoned");
        let mut report = PublishReport::default();
        let mut closed = Vec::new();
        for (client_id, session) in sessions.iter() {
            let matches = match (error.id, error.stream.as_deref()) {
                (Some(id), Some(stream)) => session
                    .subscriptions
                    .iter()
                    .any(|subscription| subscription.id == id && subscription.stream == stream),
                _ => true,
            };
            if !matches {
                continue;
            }
            match session
                .control
                .try_send(SessionControl::StreamError(error.clone()))
            {
                Ok(()) => report.delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    report.full += 1;
                    closed.push(*client_id);
                }
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
}

#[async_trait]
/// Domain adapter called by [`Server`] at the four service lifecycle points.
///
/// Implementations do not need to parse WebSocket messages, correlate request
/// IDs, negotiate UDP, or guarantee disconnect cleanup.
pub trait ServerHandler: Clone + Send + Sync + 'static {
    /// Creates service-side state for a newly upgraded WebSocket session.
    async fn open_session(&self, suggested_client_id: u64) -> Result<SessionChannels>;
    /// Applies request arguments in handler-defined order.
    ///
    /// Handlers that process multi-argument requests sequentially may have
    /// applied earlier arguments when a later one fails. Callers requiring an
    /// exact remote state must reconnect and resubscribe their confirmed set.
    async fn handle_request(&self, client_id: u64, request: ControlRequest)
    -> Result<ControlReply>;
    /// Makes an Aeron publication usable before `transport_ready` is sent.
    async fn select_aeron(&self, _client_id: u64, _config: AeronConfig) -> Result<()> {
        anyhow::bail!("Aeron is not supported by this server handler")
    }
    /// Completes idempotent service cleanup for a disconnected client.
    async fn close_session(&self, client_id: u64) -> Result<()>;
}

/// WebSocket control server parameterized by a service-specific handler.
pub struct Server<H> {
    config: ServerConfig,
    handler: H,
}

impl<H: ServerHandler> Server<H> {
    /// Creates a server. No sockets are opened until [`Self::run`].
    pub fn new(config: ServerConfig, handler: H) -> Self {
        Self { config, handler }
    }

    /// Accepts and supervises link sessions until the listener fails or the
    /// task is cancelled.
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
        let handler = self.handler.clone();
        let aeron_enabled = self.config.aeron_enabled;
        tokio::spawn(async move {
            let suggested_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
            let result = run_session(
                stream,
                peer.clone(),
                peer_ip,
                local_ip,
                udp_socket,
                handler.clone(),
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

struct SessionRuntime {
    client_id: u64,
    peer_ip: IpAddr,
    local_ip: IpAddr,
    udp_target: Option<SocketAddr>,
    transport_ready: bool,
}

#[allow(clippy::too_many_arguments)]
async fn run_session<T, H>(
    stream: T,
    peer: String,
    peer_ip: IpAddr,
    local_ip: IpAddr,
    udp_socket: Arc<OnceCell<Arc<UdpSocket>>>,
    handler: H,
    aeron_enabled: bool,
    suggested_id: u64,
    active_clients: Arc<RwLock<BTreeMap<u64, String>>>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
    H: ServerHandler,
{
    let mut websocket = accept_async(stream).await?;
    let mut session = handler.open_session(suggested_id).await?;
    let client_id = session.client_id;
    active_clients.write().await.insert(client_id, peer.clone());
    info!(client_id, %peer, "link client connected");
    let mut runtime = SessionRuntime {
        client_id,
        peer_ip,
        local_ip,
        udp_target: None,
        transport_ready: false,
    };

    let result: Result<()> = async {
        loop {
            tokio::select! {
            frame = websocket.next() => {
                let Some(frame) = frame else { break };
                match frame? {
                    Message::Text(text) => {
                        handle_text_message(
                            &mut websocket,
                            &text,
                            &mut runtime,
                            &mut session,
                            &udp_socket,
                            &handler,
                            aeron_enabled,
                        )
                        .await?;
                    }
                    Message::Binary(_) => {
                        send_reply(&mut websocket, &ControlReply::error("binary client messages are unsupported")).await?;
                        break;
                    }
                    Message::Ping(payload) => websocket.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            data = recv_optional(&mut session.udp_outbound),
                if session.udp_outbound.is_some() && runtime.udp_target.is_some() => {
                let Some(data) = data else { break };
                if let (Some(socket), Some(target)) = (udp_socket.get(), runtime.udp_target) {
                    socket.send_to(&data, target).await?;
                }
            }
            control = recv_optional(&mut session.control), if session.control.is_some() => {
                match control {
                    Some(SessionControl::StreamError(error)) => {
                        send_event(
                            &mut websocket,
                            &ControlEvent::stream_error(error),
                        ).await?;
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
    let disconnect_result = handler.close_session(client_id).await;
    debug!(client_id, %peer, "link client disconnected");
    match (result, disconnect_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.context("server handler cleanup failed")),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Handles the control-plane flow for one client text frame.
///
/// Transport selection is the only uncorrelated exchange. Every later request
/// is validated, delegated to the service handler, and answered with its
/// original request ID.
async fn handle_text_message<T, H>(
    websocket: &mut WebSocketStream<T>,
    text: &str,
    runtime: &mut SessionRuntime,
    session: &mut SessionChannels,
    udp_socket: &OnceCell<Arc<UdpSocket>>,
    handler: &H,
    aeron_enabled: bool,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
    H: ServerHandler,
{
    if let Ok(selection) = serde_json::from_str::<SelectTransport>(text) {
        let reply = select_transport(
            selection.transport,
            runtime,
            session,
            udp_socket,
            handler,
            aeron_enabled,
        )
        .await?;
        send_reply(websocket, &reply).await?;
        return Ok(());
    }

    let envelope = match serde_json::from_str::<ControlRequestEnvelope>(text) {
        Ok(envelope) => envelope,
        Err(error) => {
            send_reply(
                websocket,
                &ControlReply::error(format!("invalid control request: {error}")),
            )
            .await?;
            return Ok(());
        }
    };
    let request_id = envelope.request_id;
    let request = envelope.request;

    let reply = if let Err(error) = request.validate() {
        ControlReply::error(error)
    } else if !runtime.transport_ready {
        ControlReply::error("select_transport must succeed before subscriptions")
    } else {
        handler
            .handle_request(runtime.client_id, request)
            .await
            .unwrap_or_else(ControlReply::error)
    };
    send_request_reply(websocket, request_id, &reply).await
}

/// Applies the selected data transport and returns the exact reply to send.
///
/// Errors are protocol replies rather than task failures, so a client may
/// correct its selection without reconnecting.
async fn select_transport<H: ServerHandler>(
    selection: TransportSelection,
    runtime: &mut SessionRuntime,
    session: &mut SessionChannels,
    udp_socket: &OnceCell<Arc<UdpSocket>>,
    handler: &H,
    aeron_enabled: bool,
) -> Result<ControlReply> {
    if runtime.transport_ready {
        return Ok(ControlReply::error("transport has already been selected"));
    }

    let prepared = match selection {
        TransportSelection::Udp { client_port } => {
            if client_port == 0 {
                return Ok(ControlReply::error("UDP client port must be non-zero"));
            }
            let socket = udp_socket
                .get_or_try_init(|| async {
                    Ok::<_, std::io::Error>(Arc::new(UdpSocket::bind("0.0.0.0:0").await?))
                })
                .await?;
            runtime.udp_target = Some(SocketAddr::new(runtime.peer_ip, client_port));
            TransportReady::Udp {
                server_address: SocketAddr::new(runtime.local_ip, socket.local_addr()?.port()),
            }
        }
        selection @ (TransportSelection::AeronIpc { .. } | TransportSelection::AeronUdp { .. }) => {
            if !aeron_enabled {
                return Ok(ControlReply::error(
                    "Aeron transport selected while [aeron].enabled is false",
                ));
            }
            if matches!(
                selection,
                TransportSelection::AeronUdp { client_port: 0, .. }
            ) {
                return Ok(ControlReply::error(
                    "Aeron UDP client port must be non-zero",
                ));
            }
            let config = selection
                .aeron_config(runtime.peer_ip)
                .expect("Aeron selection must produce an Aeron config");
            if let Err(error) = handler.select_aeron(runtime.client_id, config).await {
                return Ok(ControlReply::error(error));
            }

            // Aeron is now the sole data plane. Stop polling and buffering the
            // per-client UDP fanout path.
            session.udp_outbound = None;
            match selection {
                TransportSelection::AeronIpc { stream_id } => {
                    TransportReady::AeronIpc { stream_id }
                }
                TransportSelection::AeronUdp {
                    client_port,
                    stream_id,
                } => TransportReady::AeronUdp {
                    endpoint: SocketAddr::new(runtime.peer_ip, client_port),
                    stream_id,
                },
                _ => unreachable!(),
            }
        }
    };

    runtime.transport_ready = true;
    Ok(ControlReply::TransportReady {
        transport: prepared,
    })
}

async fn send_event<T>(websocket: &mut WebSocketStream<T>, event: &ControlEvent) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    websocket
        .send(Message::Text(serde_json::to_string(event)?.into()))
        .await?;
    Ok(())
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

async fn send_request_reply<T>(
    websocket: &mut WebSocketStream<T>,
    request_id: u64,
    reply: &ControlReply,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    websocket
        .send(Message::Text(
            serde_json::to_string(&ControlReplyEnvelope::new(request_id, reply.clone()))?.into(),
        ))
        .await?;
    Ok(())
}
