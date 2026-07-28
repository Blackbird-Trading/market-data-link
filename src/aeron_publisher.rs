use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, bail};

use crate::{
    protocol::{AeronChannel, AeronConfig, ControlOperation, ControlRequest, SubscriptionKey},
    transport::{AeronClient, AeronPublication, AeronPublicationRegistration},
};

const PUBLICATION_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    channel: AeronChannel,
    stream_id: i32,
}

impl From<AeronConfig> for StreamKey {
    fn from(config: AeronConfig) -> Self {
        Self {
            channel: config.aeron_channel,
            stream_id: config.stream_id,
        }
    }
}

pub trait PublicationOffer {
    fn offer(&self, bytes: &[u8]) -> i64;
}

impl PublicationOffer for AeronPublication {
    fn offer(&self, bytes: &[u8]) -> i64 {
        AeronPublication::offer(self, bytes)
    }
}

pub trait PublicationRegistration {
    type Publication: PublicationOffer;

    fn poll(&self) -> Result<Option<Self::Publication>>;
}

impl PublicationRegistration for AeronPublicationRegistration {
    type Publication = AeronPublication;

    fn poll(&self) -> Result<Option<Self::Publication>> {
        AeronPublicationRegistration::poll(self)
            .map_err(|error| anyhow!("failed to poll Aeron publication registration: {error:?}"))
    }
}

pub trait PublicationFactory {
    type Publication: PublicationOffer;
    type Registration: PublicationRegistration<Publication = Self::Publication>;

    fn begin(&mut self, channel: &AeronChannel, stream_id: i32) -> Result<Self::Registration>;
}

pub struct AeronFactory {
    client: AeronClient,
}

impl AeronFactory {
    fn connect() -> Result<Self> {
        Ok(Self {
            client: AeronClient::connect()
                .map_err(|error| anyhow!("failed to connect to Aeron: {error:?}"))?,
        })
    }
}

impl PublicationFactory for AeronFactory {
    type Publication = AeronPublication;
    type Registration = AeronPublicationRegistration;

    fn begin(&mut self, channel: &AeronChannel, stream_id: i32) -> Result<Self::Registration> {
        self.client
            .begin_publisher(channel, stream_id)
            .map_err(|error| anyhow!("failed to register Aeron publication: {error:?}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStatus {
    Ready,
    Pending,
}

#[derive(Debug, PartialEq, Eq)]
pub struct JoinCompletion {
    pub client_id: u64,
    pub result: std::result::Result<(), String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AeronRoute<'a> {
    Stream { id: i32, stream: &'a str },
    MarketStatus { market_id: i32 },
}

#[derive(Debug, Clone, Copy)]
pub struct AeronFrame<'a> {
    pub route: AeronRoute<'a>,
    pub bytes: &'a [u8],
}

impl<'a> AeronFrame<'a> {
    pub fn new(id: i32, stream: &'a str, bytes: &'a [u8]) -> Self {
        Self {
            route: AeronRoute::Stream { id, stream },
            bytes,
        }
    }

    pub fn market_status(market_id: i32, bytes: &'a [u8]) -> Self {
        Self {
            route: AeronRoute::MarketStatus { market_id },
            bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AeronPublishReport {
    pub matched: usize,
    pub offered: usize,
    pub unavailable: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AeronPublisherStats {
    pub attempted: u64,
    pub offered: u64,
    pub not_connected: u64,
    pub backpressured: u64,
    pub admin_action: u64,
    pub fatal: u64,
    pub registrations: u64,
    pub registration_failures: u64,
}

enum PublicationState<F: PublicationFactory> {
    Registering {
        registration: F::Registration,
        started_at: Instant,
    },
    Ready(F::Publication),
    Unavailable,
}

struct StreamGroup<F: PublicationFactory> {
    members: HashSet<u64>,
    waiters: HashSet<u64>,
    publication: PublicationState<F>,
}

impl<F: PublicationFactory> StreamGroup<F> {
    fn is_ready(&self) -> bool {
        matches!(self.publication, PublicationState::Ready(_))
    }
}

/// Single-thread-owned Aeron publication and subscription-union router.
///
/// The owner calls `publish` directly from its data loop and calls
/// `progress_registration` once per loop iteration to advance non-blocking
/// publication registration.
pub struct AeronPublisher<F: PublicationFactory = AeronFactory> {
    enabled: bool,
    factory: Option<F>,
    groups: HashMap<StreamKey, StreamGroup<F>>,
    client_groups: HashMap<u64, StreamKey>,
    subscriptions: HashMap<u64, HashSet<SubscriptionKey>>,
    routes: HashMap<i32, HashMap<String, Vec<StreamKey>>>,
    market_groups: HashMap<i32, Vec<StreamKey>>,
    stats: AeronPublisherStats,
    work_pending: bool,
}

impl AeronPublisher<AeronFactory> {
    pub fn new(enabled: bool) -> Result<Self> {
        let factory = enabled.then(AeronFactory::connect).transpose()?;
        Ok(Self::from_factory(enabled, factory))
    }
}

impl<F: PublicationFactory> AeronPublisher<F> {
    fn from_factory(enabled: bool, factory: Option<F>) -> Self {
        Self {
            enabled,
            factory,
            groups: HashMap::new(),
            client_groups: HashMap::new(),
            subscriptions: HashMap::new(),
            routes: HashMap::new(),
            market_groups: HashMap::new(),
            stats: AeronPublisherStats::default(),
            work_pending: false,
        }
    }

    #[cfg(test)]
    fn with_factory(factory: F) -> Self {
        Self::from_factory(true, Some(factory))
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn add_client(&mut self, client_id: u64, config: AeronConfig) -> Result<JoinStatus> {
        if !self.enabled {
            bail!("Aeron support is disabled");
        }
        let key = StreamKey::from(config);
        if let Some(previous) = self.client_groups.get(&client_id) {
            if previous != &key {
                bail!("client {client_id} has already selected another Aeron stream");
            }
            return Ok(
                if self.groups.get(&key).is_some_and(StreamGroup::is_ready) {
                    JoinStatus::Ready
                } else {
                    JoinStatus::Pending
                },
            );
        }

        if let Some(group) = self.groups.get_mut(&key) {
            group.members.insert(client_id);
            self.client_groups.insert(client_id, key);
            self.subscriptions.entry(client_id).or_default();
            if group.is_ready() {
                self.rebuild_routes();
                return Ok(JoinStatus::Ready);
            }
            group.waiters.insert(client_id);
            self.rebuild_routes();
            return Ok(JoinStatus::Pending);
        }

        let registration = self
            .factory
            .as_mut()
            .expect("enabled Aeron publisher must have a factory")
            .begin(&key.channel, key.stream_id)?;
        self.groups.insert(
            key.clone(),
            StreamGroup {
                members: HashSet::from([client_id]),
                waiters: HashSet::from([client_id]),
                publication: PublicationState::Registering {
                    registration,
                    started_at: Instant::now(),
                },
            },
        );
        self.client_groups.insert(client_id, key);
        self.subscriptions.entry(client_id).or_default();
        self.work_pending = true;
        self.rebuild_routes();
        Ok(JoinStatus::Pending)
    }

    pub fn progress_registration(&mut self) -> Vec<JoinCompletion> {
        self.do_work_at(Instant::now())
    }

    fn do_work_at(&mut self, now: Instant) -> Vec<JoinCompletion> {
        if !self.work_pending {
            return Vec::new();
        }
        let mut completions = Vec::new();
        // Registration/recovery deliberately favors simple non-blocking
        // progress over allocation-free bookkeeping: pending group keys are
        // cloned and unavailable registrations are retried on each
        // `progress_registration`
        // call. This path is inactive once every publication is ready
        // (`work_pending == false`); steady-state `publish` remains inline and
        // does not allocate, enqueue, hand off, or retry an offered frame.
        let keys = self.groups.keys().cloned().collect::<Vec<_>>();
        let mut failed_initial_groups = Vec::new();
        let mut unavailable = Vec::new();

        for key in keys {
            let Some(group) = self.groups.get_mut(&key) else {
                continue;
            };
            match &group.publication {
                PublicationState::Registering {
                    registration,
                    started_at,
                } => {
                    let failure =
                        if now.duration_since(*started_at) >= PUBLICATION_REGISTRATION_TIMEOUT {
                            Some(anyhow!("Aeron publication registration timed out"))
                        } else {
                            match registration.poll() {
                                Ok(Some(publication)) => {
                                    group.publication = PublicationState::Ready(publication);
                                    self.stats.registrations += 1;
                                    for client_id in group.waiters.drain() {
                                        completions.push(JoinCompletion {
                                            client_id,
                                            result: Ok(()),
                                        });
                                    }
                                    continue;
                                }
                                Ok(None) => continue,
                                Err(error) => Some(error),
                            }
                        };

                    self.stats.registration_failures += 1;
                    let waiters = group.waiters.drain().collect::<Vec<_>>();
                    for client_id in &waiters {
                        group.members.remove(client_id);
                        self.client_groups.remove(client_id);
                        self.subscriptions.remove(client_id);
                        completions.push(JoinCompletion {
                            client_id: *client_id,
                            result: Err(failure
                                .as_ref()
                                .expect("failed registration must have an error")
                                .to_string()),
                        });
                    }
                    if group.members.is_empty() {
                        failed_initial_groups.push(key.clone());
                    } else {
                        group.publication = PublicationState::Unavailable;
                        unavailable.push(key.clone());
                    }
                }
                PublicationState::Unavailable => unavailable.push(key.clone()),
                PublicationState::Ready(_) => {}
            }
        }

        let routes_changed = !failed_initial_groups.is_empty();
        for key in failed_initial_groups {
            self.groups.remove(&key);
        }
        for key in unavailable {
            let registration = self
                .factory
                .as_mut()
                .expect("enabled Aeron publisher must have a factory")
                .begin(&key.channel, key.stream_id);
            match registration {
                Ok(registration) => {
                    if let Some(group) = self.groups.get_mut(&key) {
                        group.publication = PublicationState::Registering {
                            registration,
                            started_at: now,
                        };
                    }
                }
                Err(_) => self.stats.registration_failures += 1,
            }
        }
        self.work_pending = self
            .groups
            .values()
            .any(|group| !matches!(group.publication, PublicationState::Ready(_)));
        if routes_changed {
            self.rebuild_routes();
        }
        completions
    }

    pub fn update_subscriptions(
        &mut self,
        client_id: u64,
        request: &ControlRequest,
    ) -> Result<()> {
        if !self.client_groups.contains_key(&client_id) {
            return Ok(());
        }
        let subscriptions = self.subscriptions.entry(client_id).or_default();
        match request.op {
            ControlOperation::Subscribe => subscriptions.extend(request.keys()),
            ControlOperation::Unsubscribe => {
                let removed = request.keys();
                subscriptions
                    .retain(|existing| !removed.iter().any(|key| same_subscription(existing, key)));
            }
            ControlOperation::RefetchBbo => {}
        }
        self.rebuild_routes();
        Ok(())
    }

    pub fn remove_client(&mut self, client_id: u64) {
        self.subscriptions.remove(&client_id);
        let Some(key) = self.client_groups.remove(&client_id) else {
            return;
        };
        let remove_group = self.groups.get_mut(&key).is_some_and(|group| {
            group.members.remove(&client_id);
            group.waiters.remove(&client_id);
            group.members.is_empty()
        });
        if remove_group {
            self.groups.remove(&key);
        }
        self.work_pending = self
            .groups
            .values()
            .any(|group| !matches!(group.publication, PublicationState::Ready(_)));
        self.rebuild_routes();
    }

    pub fn publish(&mut self, frame: AeronFrame<'_>) -> AeronPublishReport {
        let mut report = AeronPublishReport::default();
        let keys = match frame.route {
            AeronRoute::Stream { id, stream } => self
                .routes
                .get(&id)
                .and_then(|streams| streams.get(stream)),
            AeronRoute::MarketStatus { market_id } => self.market_groups.get(&market_id),
        };
        let Some(keys) = keys else {
            return report;
        };

        let (groups, stats) = (&mut self.groups, &mut self.stats);
        for key in keys {
            let Some(group) = groups.get_mut(key) else {
                continue;
            };
            Self::offer_to_group(group, stats, frame, &mut report);
        }
        if report.failed > 0
            && self
                .groups
                .values()
                .any(|group| matches!(group.publication, PublicationState::Unavailable))
        {
            self.work_pending = true;
        }
        report
    }

    pub fn publish_to_client_group(
        &mut self,
        client_id: u64,
        frame: AeronFrame<'_>,
    ) -> AeronPublishReport {
        let Some(key) = self.client_groups.get(&client_id).cloned() else {
            return AeronPublishReport::default();
        };
        let Some(group) = self.groups.get_mut(&key) else {
            return AeronPublishReport::default();
        };
        let mut report = AeronPublishReport::default();
        Self::offer_to_group(group, &mut self.stats, frame, &mut report);
        if matches!(group.publication, PublicationState::Unavailable) {
            self.work_pending = true;
        }
        report
    }

    pub fn stats(&self) -> AeronPublisherStats {
        self.stats
    }

    /// Returns whether at least one Aeron group subscribes to this route.
    ///
    /// Publication readiness is intentionally ignored so callers can cheaply
    /// avoid encoding entirely unrouted frames.
    pub fn has_route(&self, id: i32, stream: &str) -> bool {
        self.routes
            .get(&id)
            .and_then(|streams| streams.get(stream))
            .is_some_and(|groups| !groups.is_empty())
    }

    /// Returns whether market status for `id` has at least one Aeron target.
    pub fn has_market_route(&self, id: i32) -> bool {
        self.market_groups
            .get(&id)
            .is_some_and(|groups| !groups.is_empty())
    }

    pub fn has_client(&self, client_id: u64) -> bool {
        self.client_groups.contains_key(&client_id)
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    fn offer_to_group(
        group: &mut StreamGroup<F>,
        stats: &mut AeronPublisherStats,
        frame: AeronFrame<'_>,
        report: &mut AeronPublishReport,
    ) {
        report.matched += 1;
        let PublicationState::Ready(publication) = &group.publication else {
            report.unavailable += 1;
            return;
        };
        stats.attempted += 1;
        let result = publication.offer(frame.bytes);
        match result {
            position if position >= 0 => {
                report.offered += 1;
                stats.offered += 1;
            }
            -1 => {
                report.failed += 1;
                stats.not_connected += 1;
            }
            -2 => {
                report.failed += 1;
                stats.backpressured += 1;
            }
            -3 => {
                report.failed += 1;
                stats.admin_action += 1;
            }
            _ => {
                report.failed += 1;
                stats.fatal += 1;
                group.publication = PublicationState::Unavailable;
            }
        }
    }

    fn rebuild_routes(&mut self) {
        self.routes.clear();
        self.market_groups.clear();

        for (key, group) in &self.groups {
            let mut group_routes = HashSet::<(i32, String)>::new();
            for client_id in &group.members {
                let Some(subscriptions) = self.subscriptions.get(client_id) else {
                    continue;
                };
                for subscription in subscriptions {
                    group_routes.insert((subscription.id, subscription.stream.clone()));
                }
            }

            let mut market_ids = HashSet::new();
            for (id, stream) in group_routes {
                market_ids.insert(id);
                self.routes
                    .entry(id)
                    .or_default()
                    .entry(stream)
                    .or_default()
                    .push(key.clone());
            }
            for id in market_ids {
                self.market_groups.entry(id).or_default().push(key.clone());
            }
        }
    }
}

fn same_subscription(left: &SubscriptionKey, right: &SubscriptionKey) -> bool {
    left.id == right.id && left.stream == right.stream
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque, rc::Rc};

    use super::*;
    use crate::protocol::SubscriptionArg;

    #[derive(Default)]
    struct FakeState {
        registrations: Vec<(AeronChannel, i32)>,
        offers: Vec<(i32, Vec<u8>)>,
        offer_results: VecDeque<i64>,
        never_ready: bool,
    }

    #[derive(Clone)]
    struct FakeFactory {
        state: Rc<RefCell<FakeState>>,
    }

    struct FakeRegistration {
        stream_id: i32,
        state: Rc<RefCell<FakeState>>,
    }

    struct FakePublication {
        stream_id: i32,
        state: Rc<RefCell<FakeState>>,
    }

    impl PublicationOffer for FakePublication {
        fn offer(&self, bytes: &[u8]) -> i64 {
            let mut state = self.state.borrow_mut();
            state.offers.push((self.stream_id, bytes.to_vec()));
            state.offer_results.pop_front().unwrap_or(1)
        }
    }

    impl PublicationRegistration for FakeRegistration {
        type Publication = FakePublication;

        fn poll(&self) -> Result<Option<Self::Publication>> {
            if self.state.borrow().never_ready {
                return Ok(None);
            }
            Ok(Some(FakePublication {
                stream_id: self.stream_id,
                state: Rc::clone(&self.state),
            }))
        }
    }

    impl PublicationFactory for FakeFactory {
        type Publication = FakePublication;
        type Registration = FakeRegistration;

        fn begin(&mut self, channel: &AeronChannel, stream_id: i32) -> Result<Self::Registration> {
            self.state
                .borrow_mut()
                .registrations
                .push((channel.clone(), stream_id));
            Ok(FakeRegistration {
                stream_id,
                state: Rc::clone(&self.state),
            })
        }
    }

    fn router() -> (AeronPublisher<FakeFactory>, Rc<RefCell<FakeState>>) {
        let state = Rc::new(RefCell::new(FakeState::default()));
        (
            AeronPublisher::with_factory(FakeFactory {
                state: Rc::clone(&state),
            }),
            state,
        )
    }

    fn join(router: &mut AeronPublisher<FakeFactory>, client_id: u64, stream_id: i32) {
        assert_eq!(
            router
                .add_client(
                    client_id,
                    AeronConfig {
                        aeron_channel: AeronChannel::Ipc,
                        stream_id,
                    },
                )
                .unwrap(),
            JoinStatus::Pending
        );
        assert_eq!(
            router.progress_registration(),
            vec![JoinCompletion {
                client_id,
                result: Ok(()),
            }]
        );
    }

    fn subscribe(
        router: &mut AeronPublisher<FakeFactory>,
        client_id: u64,
        arg: SubscriptionArg,
    ) {
        router
            .update_subscriptions(client_id, &ControlRequest::subscribe(vec![arg]))
            .unwrap();
    }

    #[test]
    fn shared_stream_uses_one_registration_and_one_offer() {
        let (mut router, state) = router();
        join(&mut router, 1, 1001);
        assert_eq!(
            router
                .add_client(
                    2,
                    AeronConfig {
                        aeron_channel: AeronChannel::Ipc,
                        stream_id: 1001,
                    },
                )
                .unwrap(),
            JoinStatus::Ready
        );
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));
        subscribe(&mut router, 2, SubscriptionArg::new([10], "bbo"));

        let report = router.publish(AeronFrame::new(10, "bbo", b"frame"));
        assert_eq!(report.offered, 1);
        assert_eq!(router.group_count(), 1);
        let state = state.borrow();
        assert_eq!(state.registrations.len(), 1);
        assert_eq!(state.offers, vec![(1001, b"frame".to_vec())]);
    }

    #[test]
    fn route_presence_tracks_subscribe_unsubscribe_status_and_disconnect() {
        let (mut router, _) = router();
        assert!(!router.has_route(10, "bbo"));
        assert!(!router.has_market_route(10));

        join(&mut router, 1, 1001);
        assert!(router.has_client(1));
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));
        assert!(router.has_route(10, "bbo"));
        assert!(router.has_market_route(10));
        assert!(!router.has_route(10, "trades"));

        router
            .update_subscriptions(
                1,
                &ControlRequest::unsubscribe(vec![SubscriptionArg::new([10], "bbo")]),
            )
            .unwrap();
        assert!(!router.has_route(10, "bbo"));
        assert!(!router.has_market_route(10));

        subscribe(&mut router, 1, SubscriptionArg::new([10], "trades"));
        router.remove_client(1);
        assert!(!router.has_client(1));
        assert!(!router.has_route(10, "trades"));
        assert!(!router.has_market_route(10));
    }

    #[test]
    fn disabled_router_has_no_routes() {
        let router = AeronPublisher::<FakeFactory>::from_factory(false, None);
        assert!(!router.has_route(10, "bbo"));
        assert!(!router.has_market_route(10));
        assert!(!router.has_client(1));
    }

    #[test]
    fn distinct_streams_get_distinct_publications() {
        let (mut router, state) = router();
        join(&mut router, 1, 1001);
        join(&mut router, 2, 2001);
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));
        subscribe(&mut router, 2, SubscriptionArg::new([10], "bbo"));

        assert_eq!(
            router
                .publish(AeronFrame::new(10, "bbo", b"frame"))
                .offered,
            2
        );
        assert_eq!(state.borrow().registrations.len(), 2);
    }

    #[test]
    fn distinct_udp_endpoints_get_distinct_publications() {
        let (mut router, state) = router();
        for (client_id, port) in [(1, 40101), (2, 40102)] {
            assert_eq!(
                router
                    .add_client(
                        client_id,
                        AeronConfig {
                            aeron_channel: AeronChannel::udp("127.0.0.1", port),
                            stream_id: 1001,
                        },
                    )
                    .unwrap(),
                JoinStatus::Pending
            );
        }
        assert_eq!(router.progress_registration().len(), 2);
        assert_eq!(router.group_count(), 2);
        assert_eq!(state.borrow().registrations.len(), 2);
    }

    #[test]
    fn byte_identical_events_are_not_deduplicated() {
        let (mut router, state) = router();
        join(&mut router, 1, 1001);
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));

        router.publish(AeronFrame::new(10, "bbo", b"same"));
        router.publish(AeronFrame::new(10, "bbo", b"same"));
        assert_eq!(state.borrow().offers.len(), 2);
    }

    #[test]
    fn unrelated_routes_are_not_offered() {
        let (mut router, state) = router();
        join(&mut router, 1, 1001);
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));

        assert_eq!(
            router.publish(AeronFrame::new(11, "bbo", b"other-id")),
            AeronPublishReport::default()
        );
        assert_eq!(
            router.publish(AeronFrame::new(10, "trades", b"other-stream")),
            AeronPublishReport::default()
        );
        assert!(state.borrow().offers.is_empty());
    }

    #[test]
    fn different_noise_thresholds_do_not_filter_shared_publication() {
        let (mut router, state) = router();
        join(&mut router, 1, 1001);
        assert_eq!(
            router
                .add_client(
                    2,
                    AeronConfig {
                        aeron_channel: AeronChannel::Ipc,
                        stream_id: 1001,
                    },
                )
                .unwrap(),
            JoinStatus::Ready
        );
        subscribe(
            &mut router,
            1,
            SubscriptionArg {
                id: vec![10],
                stream: "bbo".to_string(),
                noise_filter_bps: Some(5.0),
            },
        );
        subscribe(
            &mut router,
            2,
            SubscriptionArg {
                id: vec![10],
                stream: "bbo".to_string(),
                noise_filter_bps: Some(1.0),
            },
        );

        let first = AeronFrame::new(10, "bbo", b"first");
        let second = AeronFrame::new(10, "bbo", b"second");
        let third = AeronFrame::new(10, "bbo", b"third");

        assert_eq!(router.publish(first).offered, 1);
        assert_eq!(router.publish(second).offered, 1);
        assert_eq!(router.publish(third).offered, 1);
        assert_eq!(state.borrow().offers.len(), 3);
    }

    #[test]
    fn market_status_routes_by_market_without_filter_state() {
        let (mut router, state) = router();
        join(&mut router, 1, 1001);
        subscribe(
            &mut router,
            1,
            SubscriptionArg {
                id: vec![42],
                stream: "bbo".to_string(),
                noise_filter_bps: Some(10.0),
            },
        );
        let bbo = |bytes: &'static [u8]| AeronFrame::new(42, "bbo", bytes);

        assert_eq!(router.publish(bbo(b"first")).offered, 1);
        assert_eq!(router.publish(bbo(b"second")).offered, 1);
        assert_eq!(
            router
                .publish(AeronFrame::market_status(42, b"status"))
                .offered,
            1
        );
        assert_eq!(state.borrow().offers.len(), 3);
    }

    #[test]
    fn final_disconnect_removes_publication() {
        let (mut router, _) = router();
        join(&mut router, 1, 1001);
        assert_eq!(
            router
                .add_client(
                    2,
                    AeronConfig {
                        aeron_channel: AeronChannel::Ipc,
                        stream_id: 1001,
                    },
                )
                .unwrap(),
            JoinStatus::Ready
        );
        router.remove_client(1);
        assert_eq!(router.group_count(), 1);
        router.remove_client(2);
        assert_eq!(router.group_count(), 0);
    }

    #[test]
    fn backpressure_is_counted_without_retry() {
        let (mut router, state) = router();
        state.borrow_mut().offer_results.push_back(-2);
        join(&mut router, 1, 1001);
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));

        let report = router.publish(AeronFrame::new(10, "bbo", b"frame"));
        assert_eq!(report.failed, 1);
        assert_eq!(router.stats().backpressured, 1);
        assert_eq!(state.borrow().offers.len(), 1);
    }

    #[test]
    fn not_connected_is_counted_without_retry() {
        let (mut router, state) = router();
        state.borrow_mut().offer_results.push_back(-1);
        join(&mut router, 1, 1001);
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));

        let report = router.publish(AeronFrame::new(10, "bbo", b"frame"));
        assert_eq!(report.failed, 1);
        assert_eq!(router.stats().not_connected, 1);
        assert_eq!(state.borrow().offers.len(), 1);
    }

    #[test]
    fn fatal_offer_starts_non_blocking_recreation() {
        let (mut router, state) = router();
        join(&mut router, 1, 1001);
        join(&mut router, 2, 2001);
        subscribe(&mut router, 1, SubscriptionArg::new([10], "bbo"));
        subscribe(&mut router, 2, SubscriptionArg::new([10], "bbo"));
        state.borrow_mut().offer_results.push_back(-4);

        assert_eq!(
            router
                .publish_to_client_group(1, AeronFrame::new(10, "bbo", b"fatal"))
                .failed,
            1
        );
        assert_eq!(router.stats().fatal, 1);
        let while_recovering = router.publish(AeronFrame::new(10, "bbo", b"other-group"));
        assert_eq!(while_recovering.offered, 1);
        assert_eq!(while_recovering.unavailable, 1);
        assert!(router.progress_registration().is_empty());
        assert!(router.progress_registration().is_empty());
        assert_eq!(
            router
                .publish(AeronFrame::new(10, "bbo", b"recovered"))
                .offered,
            2
        );
        assert_eq!(state.borrow().registrations.len(), 3);
    }

    #[test]
    fn registration_timeout_fails_negotiation() {
        let (mut router, state) = router();
        state.borrow_mut().never_ready = true;
        assert_eq!(
            router
                .add_client(
                    7,
                    AeronConfig {
                        aeron_channel: AeronChannel::Ipc,
                        stream_id: 1001,
                    },
                )
                .unwrap(),
            JoinStatus::Pending
        );
        let completions = router.do_work_at(Instant::now() + PUBLICATION_REGISTRATION_TIMEOUT);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].client_id, 7);
        assert!(completions[0].result.is_err());
        assert_eq!(router.group_count(), 0);
    }
}
