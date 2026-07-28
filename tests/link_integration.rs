use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener as StdTcpListener},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use market_data_link::{
    AeronConfig, Client, ClientConfig, ClientTransportConfig, ControlReply, ControlRequest,
    PollEvent, PollingClient, Server, ServerConfig, ServerHandler, SessionChannels, SessionControl,
    StreamError, SubscriptionArg, SubscriptionKey,
    client::ClientEvent,
    codec::{Bbo, FeatureBbo},
    transport::ControlEndpoint,
};
use tokio::{
    net::UdpSocket,
    sync::{Mutex, mpsc},
};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Default)]
struct MockHandler {
    requests: Arc<Mutex<Vec<(u64, ControlRequest)>>>,
    aeron: Arc<Mutex<Vec<(u64, AeronConfig)>>>,
    senders: Arc<Mutex<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
    controls: Arc<Mutex<HashMap<u64, mpsc::Sender<SessionControl>>>>,
    disconnected: Arc<Mutex<Vec<u64>>>,
    reject_next: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl ServerHandler for MockHandler {
    async fn open_session(&self, client_id: u64) -> Result<SessionChannels> {
        let (tx, rx) = mpsc::channel(8);
        let (control_tx, control_rx) = mpsc::channel(8);
        self.senders.lock().await.insert(client_id, tx);
        self.controls.lock().await.insert(client_id, control_tx);
        Ok(SessionChannels {
            client_id,
            udp_outbound: Some(rx),
            control: Some(control_rx),
        })
    }

    async fn handle_request(
        &self,
        client_id: u64,
        request: ControlRequest,
    ) -> Result<ControlReply> {
        self.requests
            .lock()
            .await
            .push((client_id, request.clone()));
        if let Some(message) = self.reject_next.lock().await.take() {
            return Ok(ControlReply::error(message));
        }
        Ok(ControlReply::for_request(&request))
    }

    async fn select_aeron(&self, client_id: u64, config: AeronConfig) -> Result<()> {
        self.aeron.lock().await.push((client_id, config));
        Ok(())
    }

    async fn close_session(&self, client_id: u64) -> Result<()> {
        self.senders.lock().await.remove(&client_id);
        self.controls.lock().await.remove(&client_id);
        self.disconnected.lock().await.push(client_id);
        Ok(())
    }
}

fn free_tcp_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn udp_reference_counts_and_binary_delivery() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let server = Server::new(ServerConfig::tcp(address.to_string()), backend.clone());
    let task = tokio::spawn(server.run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = Client::connect(ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    ))
    .await
    .unwrap();
    let arg = SubscriptionArg::new([101], "feature");
    client.acquire(arg.clone()).await.unwrap();
    client.acquire(arg.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(backend.requests.lock().await.len(), 1);

    let sender = backend
        .senders
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    let feature = FeatureBbo {
        feature_id: 101,
        market_id: 42,
        timestamp_mdp_in: 1,
        bid: 10.0,
        bid_volume: 2.0,
        ask: 11.0,
        ask_volume: 3.0,
        event_id: 4,
        ts_mono_mdp_out: 5,
        mdp_received_ts_ns: 6,
        feature_start_ts_ns: 7,
        feature_done_ts_ns: 8,
        signal_bps: 9.0,
    }
    .encode_le()
    .to_vec();
    sender.send(feature.clone()).await.unwrap();

    let mut buffer = [0; 128];
    let received = tokio::time::timeout(Duration::from_secs(2), client.recv_udp(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.bytes, feature);

    client.release(arg.clone()).await.unwrap();
    assert_eq!(backend.requests.lock().await.len(), 1);
    client.release(arg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(backend.requests.lock().await.len(), 2);

    client.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(backend.disconnected.lock().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn udp_client_delivers_payloads_without_subscription_or_codec_filtering() {
    let handler = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), handler.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = Client::connect(ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    ))
    .await
    .unwrap();
    let sender = handler
        .senders
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    let payload = vec![255, 1, 2, 3];
    sender.send(payload.clone()).await.unwrap();

    let mut buffer = [0; 16];
    let received = tokio::time::timeout(Duration::from_secs(2), client.recv_udp(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.bytes, payload);
    task.abort();
}

#[tokio::test]
async fn aeron_configuration_is_negotiated_with_backend() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let mut server_config = ServerConfig::tcp(address.to_string());
    server_config.aeron_enabled = true;
    let task = tokio::spawn(Server::new(server_config, backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let config = AeronConfig {
        aeron_channel: market_data_link::AeronChannel::Ipc,
        stream_id: 2001,
    };
    let mut client_config = ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::AeronIpc { stream_id: 2001 },
    );
    client_config.aeron_enabled = true;
    let _client = Client::connect(client_config).await.unwrap();
    for _ in 0..100 {
        if backend.aeron.lock().await.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(backend.aeron.lock().await[0].1, config);
    let sender = backend
        .senders
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    assert!(
        sender.send(vec![1]).await.is_err(),
        "Aeron negotiation must drop the legacy per-session receiver"
    );
    task.abort();
}

#[tokio::test]
async fn udp_is_negotiated_and_delivers_backend_frames() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = Client::connect(ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    ))
    .await
    .unwrap();
    client
        .acquire(SubscriptionArg::new([42], "bbo"))
        .await
        .unwrap();

    let sender = loop {
        if let Some(sender) = backend.senders.lock().await.values().next().cloned() {
            break sender;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let client_address = client.udp_local_addr().unwrap().unwrap();
    let rogue = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    rogue
        .send_to(
            &Bbo {
                market_id: 42,
                timestamp_mdp_in: 1,
                bid: 1.0,
                bid_volume: 1.0,
                ask: 2.0,
                ask_volume: 1.0,
                event_id: 1,
                ts_mono_mdp_out: 1,
            }
            .encode_le(),
            SocketAddr::new("127.0.0.1".parse().unwrap(), client_address.port()),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut probe = [0; 128];
    assert!(
        client.try_recv_udp(&mut probe).unwrap().is_none(),
        "connected UDP socket must reject datagrams from another source"
    );

    // The transport does not interpret payloads from the negotiated source.
    let undecodable = vec![1, 42, 0, 0, 0];
    sender.send(undecodable.clone()).await.unwrap();
    let bbo = Bbo {
        market_id: 42,
        timestamp_mdp_in: 1,
        bid: 10.0,
        bid_volume: 2.0,
        ask: 11.0,
        ask_volume: 3.0,
        event_id: 4,
        ts_mono_mdp_out: 5,
    }
    .encode_le()
    .to_vec();
    sender.send(bbo.clone()).await.unwrap();

    let mut buffer = [0; 64];
    let first = tokio::time::timeout(Duration::from_secs(2), client.recv_udp(&mut buffer))
        .await
        .expect("UDP frame was not delivered")
        .unwrap()
        .bytes;
    assert_eq!(first, undecodable);
    let received = tokio::time::timeout(Duration::from_secs(2), client.recv_udp(&mut buffer))
        .await
        .expect("second UDP frame was not delivered")
        .unwrap()
        .bytes;
    assert_eq!(received, bbo);
    assert_ne!(client.udp_local_addr().unwrap().unwrap().port(), 0);
    assert_ne!(client.udp_peer_addr().unwrap().unwrap().port(), 0);
    task.abort();
}

#[tokio::test]
async fn acknowledgement_commits_counts_and_rejection_preserves_them() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = Client::connect(ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    ))
    .await
    .unwrap();
    let arg = SubscriptionArg::new([101], "feature");
    let key = SubscriptionKey::new(101, "feature");

    *backend.reject_next.lock().await = Some("subscribe rejected".to_string());
    assert!(client.acquire(arg.clone()).await.is_err());
    assert_eq!(client.subscription_count(&key), 0);

    client.acquire(arg.clone()).await.unwrap();
    assert_eq!(client.subscription_count(&key), 1);

    *backend.reject_next.lock().await = Some("unsubscribe rejected".to_string());
    assert!(client.release(arg.clone()).await.is_err());
    assert_eq!(client.subscription_count(&key), 1);

    client.close().await.unwrap();
    assert!(client.release(arg).await.is_err());
    assert_eq!(client.subscription_count(&key), 1);
    task.abort();
}

#[tokio::test]
async fn acknowledgement_wait_buffers_existing_data_keepalives_and_unsolicited_errors() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut config = ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    );
    config.keepalive_interval = Duration::ZERO;
    let mut client = Client::connect(config).await.unwrap();
    client
        .acquire(SubscriptionArg::new([101], "feature"))
        .await
        .unwrap();

    let feature = FeatureBbo {
        feature_id: 101,
        market_id: 42,
        timestamp_mdp_in: 1,
        bid: 10.0,
        bid_volume: 2.0,
        ask: 11.0,
        ask_volume: 3.0,
        event_id: 4,
        ts_mono_mdp_out: 5,
        mdp_received_ts_ns: 6,
        feature_start_ts_ns: 7,
        feature_done_ts_ns: 8,
        signal_bps: 9.0,
    }
    .encode_le()
    .to_vec();
    backend
        .senders
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .send(feature.clone())
        .await
        .unwrap();
    backend
        .controls
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .send(SessionControl::StreamError(StreamError {
            id: Some(101),
            stream: Some("feature".to_string()),
            severity: 1,
            message: "unsolicited".to_string(),
            timestamp: 1,
        }))
        .await
        .unwrap();
    client.send_ping_if_due().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    client
        .acquire(SubscriptionArg::new([102], "feature"))
        .await
        .unwrap();

    let mut saw_keepalive = false;
    let mut saw_unsolicited = false;
    for _ in 0..10 {
        match client.poll_control().await.unwrap() {
            Some(ClientEvent::Keepalive) => saw_keepalive = true,
            Some(ClientEvent::StreamError(error)) if error.message == "unsolicited" => {
                saw_unsolicited = true
            }
            _ => {}
        }
        if saw_keepalive && saw_unsolicited {
            break;
        }
    }
    let mut buffer = [0; 128];
    assert_eq!(client.recv_udp(&mut buffer).await.unwrap().bytes, feature);
    assert!(saw_keepalive);
    assert!(saw_unsolicited);
    task.abort();
}

#[tokio::test]
async fn udp_reconnect_renegotiates_with_a_new_ephemeral_port() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task = tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = Client::connect(ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    ))
    .await
    .unwrap();
    let first = client.udp_local_addr().unwrap().unwrap();
    client.reconnect().await.unwrap();
    let second = client.udp_local_addr().unwrap().unwrap();

    assert_ne!(first.port(), 0);
    assert_ne!(second.port(), 0);
    assert_ne!(first, second);
    task.abort();
}

#[tokio::test]
async fn subscriptions_are_rejected_before_transport_selection() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    let request = ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]);
    socket
        .send(Message::Text(
            serde_json::to_string(&request).unwrap().into(),
        ))
        .await
        .unwrap();
    let reply = socket.next().await.unwrap().unwrap();
    let Message::Text(reply) = reply else {
        panic!("expected text control reply");
    };
    assert!(matches!(
        serde_json::from_str::<ControlReply>(&reply).unwrap(),
        ControlReply::Error { message }
            if message.contains("select_transport")
    ));
    assert!(backend.requests.lock().await.is_empty());
    task.abort();
}

#[tokio::test]
async fn aeron_is_rejected_when_disabled_on_either_side() {
    let local_error = Client::connect(ClientConfig::new(
        ControlEndpoint::Tcp("ws://127.0.0.1:1".to_string()),
        ClientTransportConfig::AeronIpc { stream_id: 1001 },
    ))
    .await
    .unwrap_err();
    assert!(local_error.to_string().contains("[aeron].enabled is false"));

    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut config = ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::AeronIpc { stream_id: 1001 },
    );
    config.aeron_enabled = true;
    let server_error = Client::connect(config).await.unwrap_err();
    assert!(
        server_error
            .to_string()
            .contains("[aeron].enabled is false")
    );
    assert!(backend.aeron.lock().await.is_empty());
    task.abort();
}

#[tokio::test]
async fn reconnect_restores_each_subscription_exactly_once() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = Client::connect(ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    ))
    .await
    .unwrap();
    let arg = SubscriptionArg::new([10, 11], "bbo");
    client.acquire(arg.clone()).await.unwrap();
    client.acquire(arg).await.unwrap();
    while backend.requests.lock().await.is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    client.reconnect().await.unwrap();
    while backend.requests.lock().await.len() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let requests = backend.requests.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1].1.op,
        market_data_link::ControlOperation::Subscribe
    );
    assert_eq!(requests[1].1.keys().len(), 2);
    drop(requests);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(backend.disconnected.lock().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn keepalive_ping_receives_a_pong() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task = tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut config = ClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    );
    config.keepalive_interval = Duration::ZERO;
    let mut client = Client::connect(config).await.unwrap();
    client.send_ping_if_due().await.unwrap();

    for _ in 0..100 {
        if matches!(
            client.poll_control().await.unwrap(),
            Some(ClientEvent::Keepalive)
        ) {
            task.abort();
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("server did not answer the keepalive ping");
}

#[tokio::test]
async fn polling_client_correlates_requests_and_polls_udp_data() {
    let backend = MockHandler::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(Server::new(ServerConfig::tcp(address.to_string()), backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = tokio::task::spawn_blocking(move || {
        PollingClient::connect_tcp(
            &format!("ws://{address}"),
            Duration::from_secs(2),
            ClientTransportConfig::Udp,
            false,
        )
    })
    .await
    .unwrap()
    .unwrap();
    let request_id = client
        .send_request(ControlRequest::subscribe(vec![SubscriptionArg::new(
            [42],
            "bbo",
        )]))
        .unwrap();
    loop {
        match client.poll().unwrap() {
            Some(PollEvent::Reply(reply)) if reply.request_id == Some(request_id) => break,
            _ => tokio::time::sleep(Duration::from_millis(1)).await,
        }
    }

    let sender = backend
        .senders
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    let data = Bbo {
        market_id: 42,
        timestamp_mdp_in: 1,
        bid: 1.0,
        bid_volume: 1.0,
        ask: 2.0,
        ask_volume: 1.0,
        event_id: 1,
        ts_mono_mdp_out: 1,
    }
    .encode_le()
    .to_vec();
    sender.send(data.clone()).await.unwrap();
    loop {
        match client.poll().unwrap() {
            Some(PollEvent::Data(frame)) => {
                assert_eq!(frame.bytes, data);
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(1)).await,
        }
    }
    task.abort();
}
