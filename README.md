# market-data-link

`market-data-link` is the common connection layer between the Market Data
Platform (MDP), FeatureModule (FM), and Trading Engine (TE).

It gives those services one shared implementation of:

- WebSocket connection and session lifecycle;
- UDP, Aeron IPC, and Aeron UDP negotiation;
- correlated subscription, unsubscription, and BBO-refetch requests;
- reliable asynchronous stream errors;
- per-client UDP and shared-publication Aeron routing;
- reconnect-time subscription restoration;
- binary market-data codecs.

This keeps transport mechanics out of the services. MDP can focus on exchange
subscriptions and market-data production, FM on feature calculation, and TE on
consuming and applying data.

## How it simplifies each service

```text
MDP                           FeatureModule                         TE

exchange feed                MDP/TDP input                         strategy inputs
      │                            │                                     ▲
      ▼                            ▼                                     │
domain handler ──▶ Link Server ──data──▶ Link Client ──▶ feature engine  │
                       ▲                                  │               │
                       │                                  ▼               │
                       │                             Link Server ──data──▶ Link Client
                       │                                  ▲
                  control requests                   control requests
```

| Service role | Uses from this crate | Keeps in the service |
|---|---|---|
| MDP server | `Server`, `ServerHandler`, `ClientRouter`, `AeronPublisher`, codecs | Exchange sessions, symbol resolution, subscription union, refetch policy |
| FM input client | `Client` or `PollingClient`, transport negotiation, codecs | Market-to-feature mapping and input failure policy |
| FM output server | `Server`, `ClientRouter`, `AeronPublisher`, feature codecs | Feature definitions, calculation, dependency state |
| TE client | `PollingClient` or `Client`, transport negotiation, codecs | Contract routing, scheduling, strategy state, reconnect policy if using `PollingClient` |

The important boundary is simple: the library owns **how a request or frame
travels**; MDP, FM, and TE own **what that request or frame means**.

## The actual flow

Every link has a reliable WebSocket control plane and one separately negotiated
data plane.

```text
Client                                      Server
  │                                           │
  ├─ 1. open WebSocket ──────────────────────▶│ open_session
  │                                           │
  ├─ 2. select_transport ────────────────────▶│ prepare UDP/Aeron
  │◀──────────────────── 3. transport_ready ──┤
  │                                           │
  ├─ 4. request { request_id, op, args } ────▶│ validate + handle_request
  │◀──────────── 5. reply { request_id, ... } ┤
  │                                           │
  │◀════════════ 6. market-data frames ═══════╪═ UDP or Aeron
  │                                           │
  │◀──────────── 7. stream_error (optional) ──┤ WebSocket
  │                                           │
  └─ 8. close/disconnect ────────────────────▶│ close_session
```

The ordering is enforced:

1. The client opens the WebSocket.
2. It sends `select_transport`.
3. The server prepares the data plane and replies `transport_ready`.
4. The client may now send subscription requests.
5. Each request has a unique `request_id`.
6. The server validates it, calls the service handler, and sends a reply with
   the same ID.
7. Market data travels only over UDP or Aeron.
8. Runtime stream errors remain reliable by travelling over the WebSocket.
9. Disconnect always invokes service cleanup.

Binary WebSocket frames are protocol errors. The WebSocket is for JSON control
messages only.

## Client: connect, request, receive

### Async client

`Client` is the high-level async API. `connect` does not return until transport
negotiation succeeds.

```rust,ignore
use market_data_link::{
    Client, ClientConfig, ClientEvent, ClientTransportConfig, ControlEndpoint,
    SubscriptionArg,
};

let config = ClientConfig::new(
    ControlEndpoint::Tcp("ws://127.0.0.1:8701".into()),
    ClientTransportConfig::Udp,
);
let mut client = Client::connect(config).await?;

// acquire sends "subscribe" only when the local count changes 0 -> 1.
client
    .acquire(SubscriptionArg::new([42], "bbo"))
    .await?;

let mut buffer = vec![0; 65_535];
let frame = client.recv_udp(&mut buffer).await?;
// Decode frame.bytes with market_data_link::codec types.

if let Some(ClientEvent::StreamError(error)) = client.poll_control().await? {
    // Apply the service's runtime failure policy.
}

// release sends "unsubscribe" only when the count changes 1 -> 0.
client
    .release(SubscriptionArg::new([42], "bbo"))
    .await?;
```

The request flow inside `Client` is:

```text
send_request
  ├─ validate request
  ├─ assign next request_id
  ├─ send JSON envelope
  └─ await_request_reply
       ├─ matching reply       -> validate and return
       ├─ other reply          -> buffer for poll_control
       ├─ stream_error         -> buffer for poll_control
       └─ ping                 -> pong + buffer Keepalive
```

`acquire` and `release` change local reference counts only after a successful
server acknowledgement. `reconnect` opens a replacement session, negotiates a
fresh data plane, and restores each confirmed subscription once.

For Aeron IPC or UDP, set `ClientConfig::aeron_enabled = true`, select the
corresponding `ClientTransportConfig`, then take the receiver with
`Client::take_aeron_subscriber`.

### Non-blocking connector client

`PollingClient` is designed for TE-style synchronous connector loops. Setup is
blocking; steady-state polling is not.

```rust,ignore
use std::time::Duration;
use market_data_link::{
    ClientTransportConfig, ControlRequest, PollEvent, PollingClient,
    SubscriptionArg,
};

let mut client = PollingClient::connect_tcp(
    "127.0.0.1:8701",
    Duration::from_secs(5),
    ClientTransportConfig::Udp,
)?;

let request_id = client.send_request(ControlRequest::subscribe([
    SubscriptionArg::new([42], "bbo"),
]))?;
client.flush()?;

loop {
    match client.poll()? {
        Some(PollEvent::Reply(reply)) if reply.request_id == request_id => {
            // The subscription request completed.
        }
        Some(PollEvent::Data(frame)) => {
            // Decode and route frame.bytes.
        }
        Some(PollEvent::StreamError(error)) => {
            // Apply TE/FM runtime failure policy.
        }
        Some(PollEvent::Keepalive) | None => {}
        Some(PollEvent::Reply(_)) => {
            // Reply for another outstanding request.
        }
    }
}
```

`PollingClient` intentionally does not own subscription reference counts or a
reconnection policy. Those remain in the connector loop.

## Server: accept, handle, reply

Only a service that **exposes a link server** implements `ServerHandler`. In
this system that means MDP and FeatureModule's output side. FeatureModule's
MDP/TDP input side and TE are link clients, so they do not implement this
trait.

`ServerHandler` is the boundary between generic connection handling and
service-owned state. `Server` can accept a connection, parse requests, and send
frames, but it does not own MDP exchange subscriptions, FeatureModule feature
subscriptions, per-client service queues, or either service's
`AeronPublisher`. It therefore calls a small service adapter when that state
must change:

| Hook | Required? | Why the service handles it |
|---|---|---|
| `open_session` | Yes | Register the client in the service and return the data/control receivers that `Server` should drain. The service may accept the suggested ID or return its own. |
| `handle_request` | Yes | Give `subscribe`, `unsubscribe`, and `refetch_bbo` their service-specific meaning. Framing, validation, and request correlation have already been handled. |
| `select_aeron` | Only for Aeron servers | Register the client with the service-owned `AeronPublisher`. The default implementation rejects Aeron as unsupported. |
| `close_session` | Yes | Idempotently remove service subscriptions, queues, and publication routes. A generic no-op could leak service state. |

There are no useful generic `open_session` and `close_session`
implementations for a data-producing service. Opening a session is not just
creating a receiver: the matching sender must be installed in the MDP actor or
FeatureModule output path so produced frames can reach that client. Closing
the session must undo that exact registration and any domain subscriptions it
created. The link crate cannot do either operation because it does not own
those actors or their subscription state.

The same ownership rule applies to Aeron. The publication is used from the
service's producing loop, so that loop owns its `AeronPublisher`.
`select_aeron` must ask the owner to create or join the requested publication;
`Server` cannot maintain a second, independent publisher without disconnecting
transport setup from the data-producing hot path. A server that does not offer
Aeron can use the default rejecting implementation.

These hooks do not move transport logic back into MDP or FeatureModule. They
only let the link server reach state that deliberately remains owned by the
producing service. `ClientRouter` provides reusable session and UDP-routing
state where it fits; FeatureModule uses it directly, while MDP supplies its
existing actor-backed client queues.

For example, an MDP adapter has this shape:

```rust,ignore
use anyhow::Result;
use async_trait::async_trait;
use market_data_link::{
    AeronConfig, ControlReply, ControlRequest, ServerHandler, SessionChannels,
};

#[derive(Clone)]
struct MdpLinkHandler {
    mdp: MdpHandle,
}

#[async_trait]
impl ServerHandler for MdpLinkHandler {
    async fn open_session(&self, suggested_id: u64) -> Result<SessionChannels> {
        // MDP keeps the matching senders in its producer path. Server receives
        // the other ends so it can forward MDP output to this connection.
        self.mdp.open_link_session(suggested_id).await
    }

    async fn handle_request(
        &self,
        client_id: u64,
        request: ControlRequest,
    ) -> Result<ControlReply> {
        // Validate domain facts and apply MDP subscription/refetch state.
        // WebSocket parsing and request correlation are already handled.
        todo!()
    }

    async fn select_aeron(&self, client_id: u64, config: AeronConfig) -> Result<()> {
        // This command runs against the AeronPublisher owned by MDP's
        // producing loop.
        self.mdp.configure_aeron(client_id, config).await
    }

    async fn close_session(&self, client_id: u64) -> Result<()> {
        // Remove the registration created by open_session, including its
        // domain subscriptions and publication routes.
        self.mdp.close_link_session(client_id).await
    }
}
```

Run it with:

```rust,ignore
use market_data_link::{Server, ServerConfig};
use std::time::Duration;

let config = ServerConfig {
    tcp_bind_addr: "0.0.0.0:8701".into(),
    aeron_enabled: true,
    active_client_log_interval: Duration::from_secs(30),
};

Server::new(config, MdpLinkHandler { /* ... */ }).run().await?;
```

`Server` owns listener setup, WebSocket framing, transport ordering, request
validation, correlation, keepalives, UDP sends, reliable stream-error sends,
and ensuring that the close hook runs after disconnect. The handler remains a
small adapter to the service's actor or engine; the service performs the
actual cleanup of the state it owns.

Handlers currently apply multi-argument requests in their own chosen order. If
a later argument fails, earlier arguments may already be active. A client that
requires an exact remote state after such an error should reconnect and restore
its confirmed local set.

## Publishing data

### UDP

`ClientRouter` stores confirmed `(id, stream)` subscriptions and bounded
per-client channels:

```rust,ignore
let channels = router.register_client(client_id, 1_024);
router.update_subscriptions(client_id, &successful_request)?;

let report = router.send_data(id, stream, encoded_frame);
// report.delivered / full / closed describe the fanout attempt.
```

`full` means the frame was dropped for a backpressured session. `closed` means
the receiver disappeared and stale routing state was removed.

### Aeron

The producing service owns one `AeronPublisher` directly in its actor or
feature loop:

```rust,ignore
let mut publisher = AeronPublisher::new(true)?;

// During transport negotiation:
let status = publisher.add_client(client_id, aeron_config)?;

// Once per owner-loop iteration:
for completion in publisher.progress_registration() {
    // Complete the server's select_aeron request.
}

// After a successful subscription request:
publisher.update_subscriptions(client_id, &request)?;

// On the data hot path:
if publisher.has_route(market_id, "bbo") {
    publisher.publish(AeronFrame::new(market_id, "bbo", &encoded));
}
```

Clients sharing the same `(channel, stream_id)` share one publication. The
publisher sends the union of their subscriptions once to that group.
Publication progress and recovery are non-blocking; offered frames are never
queued or retried.

## Protocol at a glance

Subscription request:

```json
{
  "request_id": 1,
  "op": "subscribe",
  "args": [
    {
      "id": [42],
      "stream": "bbo"
    }
  ]
}
```

Correlated success reply:

```json
{
  "request_id": 1,
  "type": "subscribed",
  "args": [
    {
      "id": [42],
      "stream": "bbo"
    }
  ]
}
```

Available operations are `subscribe`, `unsubscribe`, and `refetch_bbo`.
Configuration or request failures return an error reply. Asynchronous runtime
failures use `ControlEvent::StreamError`, including optional ID/stream scope,
severity, message, and producer timestamp.

## Code layout

```text
src/
├── client/
│   ├── mod.rs              async Client: connect -> request -> reply
│   └── polling.rs          non-blocking PollingClient
├── server/
│   ├── mod.rs              accept -> negotiate -> dispatch -> reply -> cleanup
│   └── aeron_publisher.rs  server-side shared Aeron routing
├── protocol.rs             JSON control types and validation
├── codec.rs                binary market-data layouts
└── transport.rs            shared low-level UDP/Aeron primitives
```

The old flat `polling` and `aeron_publisher` module paths remain as compatibility
re-exports. New code should prefer `client::polling` and
`server::aeron_publisher`, or the crate-root type exports.

## Data codecs

`codec` contains the shared little-endian layouts for BBO, public trades, order
books, market status, and feature BBO. Fixed-width messages provide
`encode_le` and `decode_le`. `OrderBookView` can encode into a reusable
caller-owned buffer.

The transport layer delivers raw UDP and Aeron payloads. FM and TE decide when
and how to decode, filter, route, and apply them.

## Errors and backpressure

- Startup, connection, and transport infrastructure failures return `Err`.
- Invalid requests receive a correlated error reply.
- Runtime stream failures use the reliable WebSocket control plane for both
  UDP and Aeron clients.
- A full reliable control queue disconnects the lagging session so errors are
  not silently lost.
- Full UDP queues drop frames and increment `PublishReport::full`.
- Aeron backpressure or unavailable publications drop frames and update
  `AeronPublisherStats`.
- Disconnect always calls `ServerHandler::close_session`; cleanup should be
  idempotent.

## Build and test

```bash
cargo fmt --check
cargo test
```

Aeron uses the media driver at `/dev/shm/aeron`. Tests that exercise only
protocol, codecs, routing, and UDP do not require MDP, FM, or TE to be running.
