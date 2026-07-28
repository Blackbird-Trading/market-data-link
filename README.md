# market-data-link

`market-data-link` is the shared control and data-plane implementation used
between:

- Market Data Platform (MDP) and FeatureModule (FM);
- MDP and Trading Engine (TE);
- FeatureModule and Trading Engine.

The crate owns the common protocol, transport negotiation, client session
state, generic server session handling, wire codecs, frame filtering, and
shared Aeron publication routing. Domain decisions remain in the services.

## Roles and boundaries

```text
MDP                         FeatureModule                         TE
┌─────────────────┐         ┌──────────────────────────┐         ┌──────────────┐
│ Server          │◀control─│ Client (MDP input)       │         │              │
│ MdpHandler      │─data───▶│ market/feature engine    │         │              │
│ Aeron publisher │         │                          │         │              │
└─────────────────┘         │ Server (feature output)  │◀control─│ PollingClient│
                            │ FeatureHandler           │─data───▶│ or client    │
                            │ Aeron publisher          │         │              │
                            └──────────────────────────┘         └──────────────┘
```

FeatureModule has two independent link roles:

1. Its input side is a client of MDP and optional TDP.
2. Its output side is a server for TE clients.

The output-side acknowledgement covers only feature subscription and output
routing state. Required MDP/TDP subscription changes are placed on an internal
queue and handled by the input side separately. An unavailable upstream link
therefore does not become a downstream protocol error. If applying an upstream
change fails, FeatureModule treats that as an input-side runtime failure.

The crate does not contain feature definitions, exchange subscriptions,
contract routing, scheduling, latency accounting, or service configuration
loading.

## Client/server interaction

Every session uses a TCP WebSocket as its reliable control plane. Data always
uses a separately negotiated UDP or Aeron data plane. Binary WebSocket frames
are protocol errors.

### Connection sequence

1. The client opens the control WebSocket.
2. The server calls `ServerHandler::open_session` and assigns the session ID.
3. The client sends `select_transport`.
4. The server prepares the selected data plane.
5. The server returns `transport_ready`.
6. Only after that reply may the client send subscription requests.
7. The server validates each request and delegates it to the handler.
8. The server sends a correlated success or error reply.
9. Data frames are routed over the selected data plane.
10. When the WebSocket closes, the server awaits handler cleanup.

Example subscription request:

```json
{
  "request_id": 1,
  "op": "subscribe",
  "args": [
    {
      "id": [101],
      "stream": "feature"
    }
  ]
}
```

Success reply:

```json
{
  "request_id": 1,
  "type": "subscribed",
  "args": [
    {
      "id": [101],
      "stream": "feature"
    }
  ]
}
```

`request_id` is optional for compatibility with raw clients. `Client`
always supplies it and waits for the matching reply. The operations are
`subscribe`, `unsubscribe`, and `refetch_bbo`.

### Data-plane negotiation

`ClientTransportConfig` selects exactly one transport:

```rust,ignore
pub enum ClientTransportConfig {
    Udp,
    AeronIpc { stream_id: i32 },
    AeronUdp { endpoint: SocketAddr, stream_id: i32 },
}
```

- **UDP:** the client binds `0.0.0.0:0` and sends its assigned port over the
  control connection. The server derives the client IP from the WebSocket,
  lazily creates one shared ephemeral UDP sender, and returns its source
  endpoint. The client connects its UDP socket to that endpoint for kernel
  source filtering. Data is server-to-client only.
- **Aeron IPC:** the client chooses the stream ID; the channel is `aeron:ipc`.
- **Aeron UDP:** the client supplies both endpoint and stream ID.

Aeron uses `/dev/shm/aeron`. Selecting Aeron while process-level Aeron support
is disabled fails locally in the client or returns a negotiation error from the
server.

After Aeron becomes ready, `Server` drops the session's UDP receiver. MDP and
FM remove that client from UDP fanout while retaining its reliable control
events, so the shared Aeron publication is the only data path.

## Client API

### `Client`

`Client` is the high-level asynchronous client. Internally it owns:

- the immutable endpoint and selected transport configuration;
- split WebSocket sender and receiver halves;
- the optional connected UDP socket;
- reference counts keyed by `(id, stream, noise_filter_bps)`;
- monotonically increasing request IDs;
- buffered keepalive, stream-error, and unsolicited reply events encountered
  while waiting for an acknowledgement;
- keepalive and transport-readiness state.

Important return types:

| Method | Return | Meaning |
|---|---|---|
| `connect` | `anyhow::Result<Client>` | The WebSocket is open and transport negotiation completed. |
| `acquire` | `anyhow::Result<()>` | The first logical reference was acknowledged by the server, or no wire request was needed. |
| `release` | `anyhow::Result<()>` | The final logical reference was acknowledged by the server, or no wire request was needed. |
| `send_request` | `anyhow::Result<()>` | A matching operation-specific success reply was received. |
| `refetch_bbo` | `anyhow::Result<()>` | The refetch request was acknowledged; returned market data can arrive later. |
| `poll_control` | `anyhow::Result<Option<ClientEvent>>` | `None` means no control-plane event is currently ready, not disconnect. |
| `try_recv_udp` | `anyhow::Result<Option<InboundFrame>>` | Non-blocking receive from the kernel source-filtered UDP socket. |
| `recv_udp` | `anyhow::Result<InboundFrame>` | Waits for the next UDP datagram. |
| `reconnect` | `anyhow::Result<()>` | A replacement session was negotiated and the confirmed subscription set was resubscribed. |

`ClientEvent` is one of:

- `Reply(ControlReply)`, normally an unsolicited reply because request replies
  are consumed by `send_request`;
- `StreamError(StreamError)` for an asynchronous scoped or global runtime error;
- `Keepalive` for ping/pong activity.

Reference counts are changed only after a successful acknowledgement. A
rejection, malformed reply, closed connection, or send failure returns `Err`
and leaves confirmed local state unchanged.

### `PollingClient`

`PollingClient` is the synchronous-setup, non-blocking steady-state client used
by TE. It owns the control socket, optional UDP socket, correlated pending
request IDs, and no market-data interpretation state.

`poll()` returns UDP data, correlated replies, typed stream errors, or
keepalives; `WouldBlock` is `Ok(None)`. Unlike `Client`, it does not own
subscription reference counts or reconnection policy.

## Server API

### `Server<B>`

`Server<H: ServerHandler>` contains:

- `ServerConfig`, including the TCP bind address and Aeron enablement;
- a cloneable service handler;
- one TCP listener;
- one lazily initialized shared UDP sending socket;
- active-session bookkeeping.

Each accepted session owns:

- its WebSocket control connection;
- the `SessionChannels` returned by the service;
- an optional UDP outbound data receiver;
- an optional service-to-session control receiver;
- negotiated UDP target and transport state.

`Server::run` returns `anyhow::Result<()>`. It normally runs forever.
Listener, WebSocket, serialization, UDP, or session-cleanup failures terminate
the affected session; listener-level failures terminate `run`.

### `ServerHandler`

`ServerHandler` is the service adapter behind `Server`. It does not listen on
sockets or publish data itself. It translates generic link operations into
MDP- or FM-specific commands.

Services implement:

```rust,ignore
#[async_trait]
pub trait ServerHandler {
    async fn open_session(&self, suggested_client_id: u64)
        -> anyhow::Result<SessionChannels>;

    async fn handle_request(&self, client_id: u64, request: ControlRequest)
        -> anyhow::Result<ControlReply>;

    async fn select_aeron(&self, client_id: u64, config: AeronConfig)
        -> anyhow::Result<()>;

    async fn close_session(&self, client_id: u64)
        -> anyhow::Result<()>;
}
```

- `open_session` creates service-side session resources.
- `handle_request` performs domain validation and returns an explicit protocol reply.
  Returning `Err` is converted to `ControlReply::Error`.
- `select_aeron` completes only when the publication is usable. The default
  implementation rejects Aeron.
- `close_session` performs idempotent cleanup and is awaited after the network
  session ends.

Handlers currently apply multi-argument requests sequentially. If a later
argument fails, earlier arguments may already be active. A client that needs an
exact remote state after such an error must reconnect and resubscribe its
confirmed local set.

### `SessionChannels` and `ClientRouter`

`SessionChannels` returns the actual client ID plus optional bounded receivers:

- `udp_outbound` carries encoded per-client UDP frames;
- `control` carries asynchronous `SessionControl::StreamError` or
  `SessionControl::Close`.

It is a passive hand-off value, not another task or session implementation.
`Server` consumes these receivers while the service retains their senders.

`ClientRouter` tracks connected clients, their confirmed subscriptions, and
their bounded UDP and control-event senders. `send_data` and `send_error`
return:

```rust
pub struct PublishReport {
    pub delivered: usize,
    pub full: usize,
    pub closed: usize,
}
```

`full` is backpressure: the frame was dropped for that session. `closed` means
the receiver disappeared and the stale session is removed.

## Aeron publication routing

`AeronPublisher` is single-thread-owned and intended to live directly
inside the MDP actor or FM feature loop.

It is the Aeron equivalent of data fanout, but it does not own WebSocket
sessions or UDP channels. Sharing publications and building subscription
unions are private implementation details.

It provides:

- `add_client` to add a client to `(channel, stream_id)`;
- `progress_registration` to poll asynchronous publication registration;
- `update_subscriptions` to update client subscriptions;
- `remove_client` for membership cleanup;
- `has_route` and `has_market_route` to avoid encoding unrouted frames;
- `publish` for one inline offer per matching publication group;
- `stats` for typed publication outcomes.

Multiple clients can share a publication. The publisher sends the union of
their subscriptions once. Every client attached to that publication receives
that union and lets its service-level routing ignore irrelevant IDs.

`AeronFrame<'a>` borrows already encoded bytes and contains only an
`AeronRoute` plus the payload. Publishing therefore performs no payload clone,
decode, queue handoff, mutex acquisition, threshold check, or retry.

Aeron offer failures are latency-first:

- `NOT_CONNECTED`, `BACK_PRESSURED`, and `ADMIN_ACTION` drop that frame and
  increment typed counters;
- fatal/closed publication results mark the publication unavailable;
- `progress_registration` recreates it asynchronously;
- the dropped frame is never retried.

Pending registration currently clones group keys and retries unavailable
registrations on each `progress_registration` call. This is confined to
registration/recovery; steady-state registration progress exits immediately.

## Codecs and data handling

`codec` contains the common little-endian layouts for:

- BBO;
- public trades;
- order books;
- market status;
- feature BBO.

Fixed-width messages provide `encode_le` and `decode_le`. `OrderBookView`
supports encoding into a caller-owned reusable buffer. `WireMessage::decode`
validates the message discriminator and length.

Clients deliver UDP and Aeron payloads without decoding, subscription
filtering, noise filtering, or error-watermark suppression. FM and TE decode
and route payloads in their existing business paths. The
`noise_filter_bps` request field remains available to service business logic
but is ignored by `AeronPublisher`.

## What each service receives from the crate

### MDP

MDP uses:

- `Server<MdpHandler>` for the control plane and UDP sessions;
- protocol request/reply types in its server handler;
- `AeronPublisher` owned by the MDP actor;
- `AeronFrame`, route-presence checks, and market codecs.

MDP still owns exchange subscriptions, market routing, timestamp guards,
refetch behavior, NATS output, and its busy-spinning actor.

### FeatureModule

The input interface uses:

- `Client` for MDP/TDP control;
- the selected UDP or Aeron receive implementation;
- market codecs and frame validation.

The output interface separately uses:

- `Server<FeatureHandler>`;
- `ClientRouter` for UDP clients and typed control events;
- `AeronPublisher` owned by the feature loop;
- feature BBO and market-status codecs.

FM still owns feature construction, feature subscriber state, market-to-feature
mapping, neutral `1@1` reset output, and its independent queue of desired
upstream subscription changes.

### Trading Engine

TE uses:

- `PollingClient` for its existing connector control loop;
- `ClientTransportConfig` and protocol types;
- shared market/feature codecs;
- low-level Aeron subscription types for its existing busy-spin consumer.

TE still owns contract routing, feature slots, connector workers, scheduling,
latency measurement, and reconnect policy.

## Error-handling summary

- Configuration and connection failures return `Err` during startup.
- Invalid transport selection returns `ControlReply::Error`.
- Subscription validation or handler rejection returns a correlated
  `ControlReply::Error`.
- Asynchronous runtime failures use `ControlEvent::StreamError` and retain
  optional ID/stream scope, severity, message, and producer timestamp.
- Scoped stream errors go only to matching subscribers; incomplete scope is
  global. They travel over the reliable control WebSocket for UDP and Aeron
  clients alike.
- Runtime errors do not suppress later data, including delayed data produced
  before the error.
- FM resets severity `>= 1` dependencies and publishes both a neutral `1@1`
  feature value and a feature-scoped stream error. Severity `0` is logged.
- `Client` converts a matching error reply to `Err` without committing
  local subscription state.
- A full control-event queue disconnects the lagging session so errors are not
  silently lost; reconnect and resubscription restore it.
- Raw UDP per-client backpressure drops frames and is reported through
  `PublishReport`.
- Aeron offer failures drop frames and are reported through
  `AeronPublisherStats`.
- FM and TE own inbound decoding, validation, and business routing.
- Network disconnect always invokes handler cleanup; cleanup errors are
  returned from the session task after the socket is closed.
