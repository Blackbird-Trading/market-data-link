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
    AeronConfig, BackendSession, ClientTransportConfig, ControlReply, ControlRequest, LinkClient,
    LinkClientConfig, LinkServer, LinkServerConfig, SubscriptionArg, SubscriptionBackend,
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
struct MockBackend {
    requests: Arc<Mutex<Vec<(u64, ControlRequest)>>>,
    aeron: Arc<Mutex<Vec<(u64, AeronConfig)>>>,
    senders: Arc<Mutex<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
    disconnected: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl SubscriptionBackend for MockBackend {
    async fn connect(&self, client_id: u64) -> Result<BackendSession> {
        let (tx, rx) = mpsc::channel(8);
        self.senders.lock().await.insert(client_id, tx);
        Ok(BackendSession {
            client_id,
            outbound: Some(rx),
            control: None,
        })
    }

    async fn request(&self, client_id: u64, request: ControlRequest) -> Result<ControlReply> {
        self.requests
            .lock()
            .await
            .push((client_id, request.clone()));
        Ok(ControlReply::for_request(&request))
    }

    async fn configure_aeron(&self, client_id: u64, config: AeronConfig) -> Result<()> {
        self.aeron.lock().await.push((client_id, config));
        Ok(())
    }

    async fn disconnect(&self, client_id: u64) {
        self.senders.lock().await.remove(&client_id);
        self.disconnected.lock().await.push(client_id);
    }
}

fn free_tcp_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn tcp_reference_counts_and_binary_delivery() {
    let backend = MockBackend::default();
    let address = free_tcp_address();
    let server = LinkServer::new(LinkServerConfig::tcp(address.to_string()), backend.clone());
    let task = tokio::spawn(server.run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = LinkClient::connect(LinkClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::WebSocket,
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

    let mut received = None;
    for _ in 0..100 {
        if let Some(ClientEvent::Data(frame)) = client.poll_control().await.unwrap() {
            received = Some(frame.bytes);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(received, Some(feature));

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
async fn aeron_configuration_is_negotiated_with_backend() {
    let backend = MockBackend::default();
    let address = free_tcp_address();
    let mut server_config = LinkServerConfig::tcp(address.to_string());
    server_config.aeron_enabled = true;
    let task = tokio::spawn(LinkServer::new(server_config, backend.clone()).run());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let config = AeronConfig {
        aeron_channel: market_data_link::AeronChannel::Ipc,
        stream_id: 2001,
    };
    let mut client_config = LinkClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::AeronIpc { stream_id: 2001 },
    );
    client_config.aeron_enabled = true;
    let _client = LinkClient::connect(client_config).await.unwrap();
    for _ in 0..100 {
        if backend.aeron.lock().await.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(backend.aeron.lock().await[0].1, config);
    task.abort();
}

#[tokio::test]
async fn udp_is_negotiated_and_delivers_backend_frames() {
    let backend = MockBackend::default();
    let address = free_tcp_address();
    let task = tokio::spawn(
        LinkServer::new(LinkServerConfig::tcp(address.to_string()), backend.clone()).run(),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = LinkClient::connect(LinkClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::Udp,
    ))
    .await
    .unwrap();
    client
        .acquire(SubscriptionArg::new([42], "bbo"))
        .await
        .unwrap();
    // The subscription reply is ordered after the preceding UDP negotiation.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match client.poll_control().await.unwrap() {
                Some(ClientEvent::Reply(ControlReply::Subscribed { .. })) => break,
                Some(ClientEvent::Reply(ControlReply::Error { message })) => {
                    panic!("server rejected UDP negotiation: {message}")
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("subscription was not acknowledged");

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

    // A datagram from the negotiated source with an invalid known wire layout
    // must also be ignored before delivery to the application.
    sender.send(vec![1, 42, 0, 0, 0]).await.unwrap();
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
    let received = tokio::time::timeout(Duration::from_secs(2), client.recv_udp(&mut buffer))
        .await
        .expect("UDP frame was not delivered")
        .unwrap()
        .bytes;
    assert_eq!(received, bbo);
    assert_ne!(client.udp_local_addr().unwrap().unwrap().port(), 0);
    assert_ne!(client.udp_peer_addr().unwrap().unwrap().port(), 0);
    task.abort();
}

#[tokio::test]
async fn udp_reconnect_renegotiates_with_a_new_ephemeral_port() {
    let backend = MockBackend::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(LinkServer::new(LinkServerConfig::tcp(address.to_string()), backend).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = LinkClient::connect(LinkClientConfig::new(
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
    let backend = MockBackend::default();
    let address = free_tcp_address();
    let task = tokio::spawn(
        LinkServer::new(LinkServerConfig::tcp(address.to_string()), backend.clone()).run(),
    );
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
    let local_error = LinkClient::connect(LinkClientConfig::new(
        ControlEndpoint::Tcp("ws://127.0.0.1:1".to_string()),
        ClientTransportConfig::AeronIpc { stream_id: 1001 },
    ))
    .await
    .unwrap_err();
    assert!(local_error.to_string().contains("[aeron].enabled is false"));

    let backend = MockBackend::default();
    let address = free_tcp_address();
    let task = tokio::spawn(
        LinkServer::new(LinkServerConfig::tcp(address.to_string()), backend.clone()).run(),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut config = LinkClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::AeronIpc { stream_id: 1001 },
    );
    config.aeron_enabled = true;
    let server_error = LinkClient::connect(config).await.unwrap_err();
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
    let backend = MockBackend::default();
    let address = free_tcp_address();
    let task = tokio::spawn(
        LinkServer::new(LinkServerConfig::tcp(address.to_string()), backend.clone()).run(),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut client = LinkClient::connect(LinkClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::WebSocket,
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
    let backend = MockBackend::default();
    let address = free_tcp_address();
    let task =
        tokio::spawn(LinkServer::new(LinkServerConfig::tcp(address.to_string()), backend).run());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut config = LinkClientConfig::new(
        ControlEndpoint::Tcp(format!("ws://{address}")),
        ClientTransportConfig::WebSocket,
    );
    config.keepalive_interval = Duration::ZERO;
    let mut client = LinkClient::connect(config).await.unwrap();
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
