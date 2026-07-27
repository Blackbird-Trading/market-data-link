use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread,
};

use anyhow::{Result, anyhow, bail};
use tracing::warn;

use crate::{
    protocol::{AeronChannel, AeronConfig, ControlOperation, ControlRequest, SubscriptionKey},
    transport::{AeronClient, AeronPublisher},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    channel: AeronChannel,
    stream_id: i32,
}

struct StreamGroup {
    members: HashSet<u64>,
}

#[derive(Default)]
struct HubState {
    groups: HashMap<StreamKey, StreamGroup>,
    client_groups: HashMap<u64, StreamKey>,
    subscriptions: HashMap<u64, HashSet<SubscriptionKey>>,
    recent_frames: HashMap<(StreamKey, i32, String), VecDeque<Vec<u8>>>,
}

impl HubState {
    fn join(&mut self, client_id: u64, key: StreamKey) {
        self.groups
            .entry(key.clone())
            .or_insert_with(|| StreamGroup {
                members: HashSet::new(),
            })
            .members
            .insert(client_id);
        self.client_groups.insert(client_id, key);
        self.subscriptions.entry(client_id).or_default();
    }

    fn apply(&mut self, client_id: u64, request: &ControlRequest) {
        if !self.client_groups.contains_key(&client_id) {
            return;
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
    }

    fn matching_groups(&self, key: &SubscriptionKey) -> Vec<StreamKey> {
        self.groups
            .iter()
            .filter_map(|(stream_key, group)| {
                group
                    .members
                    .iter()
                    .any(|client_id| {
                        self.subscriptions
                            .get(client_id)
                            .is_some_and(|subscriptions| {
                                subscriptions.iter().any(|subscription| {
                                    subscription.id == key.id
                                        && (key.stream == "market_status"
                                            || subscription.stream == key.stream)
                                })
                            })
                    })
                    .then_some(stream_key.clone())
            })
            .collect()
    }

    /// Returns `None` when the group has no such subscription and
    /// `Some(None)` when at least one member requests the unfiltered stream.
    #[cfg(test)]
    fn effective_noise_filter(
        &self,
        group_key: &StreamKey,
        id: i32,
        stream: &str,
    ) -> Option<Option<f64>> {
        let group = self.groups.get(group_key)?;
        let mut found = false;
        let mut minimum = None;
        for client_id in &group.members {
            let Some(subscriptions) = self.subscriptions.get(client_id) else {
                continue;
            };
            for subscription in subscriptions
                .iter()
                .filter(|key| key.id == id && key.stream == stream)
            {
                found = true;
                let Some(threshold) = subscription.noise_filter_bps() else {
                    return Some(None);
                };
                minimum = Some(minimum.map_or(threshold, |current: f64| current.min(threshold)));
            }
        }
        found.then_some(minimum)
    }

    fn disconnect(&mut self, client_id: u64) -> Option<StreamKey> {
        self.subscriptions.remove(&client_id);
        let key = self.client_groups.remove(&client_id)?;
        let remove_group = self.groups.get_mut(&key).is_some_and(|group| {
            group.members.remove(&client_id);
            group.members.is_empty()
        });
        if !remove_group {
            return None;
        }
        self.groups.remove(&key);
        self.recent_frames
            .retain(|(stream, _, _), _| stream != &key);
        Some(key)
    }

    fn record_if_new(
        &mut self,
        stream_key: &StreamKey,
        key: &SubscriptionKey,
        bytes: &[u8],
    ) -> bool {
        let frame_key = (stream_key.clone(), key.id, key.stream.clone());
        let recent = self.recent_frames.entry(frame_key).or_default();
        if recent.iter().any(|previous| previous == bytes) {
            return false;
        }
        const RECENT_FRAME_LIMIT: usize = 256;
        if recent.len() == RECENT_FRAME_LIMIT {
            recent.pop_front();
        }
        recent.push_back(bytes.to_vec());
        true
    }
}

/// Shared Aeron broadcast groups keyed by `(channel, stream_id)`.
///
/// A frame is offered once per matching group even when several group members
/// subscribe to the same key. Individual clients retain their own subscription
/// sets and are expected to discard union frames they did not request.
#[derive(Clone)]
pub struct AeronStreamHub {
    enabled: bool,
    state: Arc<Mutex<HubState>>,
    worker: Option<SyncSender<WorkerCommand>>,
}

impl AeronStreamHub {
    pub fn new(enabled: bool) -> Self {
        let worker = enabled.then(|| {
            let (sender, receiver) = sync_channel(4096);
            thread::Builder::new()
                .name("market-data-aeron".to_string())
                .spawn(move || run_worker(receiver))
                .expect("failed to spawn Aeron publisher thread");
            sender
        });
        Self {
            enabled,
            state: Arc::new(Mutex::new(HubState::default())),
            worker,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn join(&self, client_id: u64, config: AeronConfig) -> Result<()> {
        if !self.enabled {
            bail!("Aeron support is disabled");
        }
        let key = StreamKey {
            channel: config.aeron_channel,
            stream_id: config.stream_id,
        };
        let mut state = self.state.lock().expect("Aeron stream hub lock poisoned");

        if let Some(previous) = state.client_groups.get(&client_id) {
            if previous == &key {
                return Ok(());
            }
            bail!("client {client_id} has already selected another Aeron stream");
        }

        if !state.groups.contains_key(&key) {
            let (reply_tx, reply_rx) = std::sync::mpsc::channel();
            self.worker
                .as_ref()
                .expect("enabled Aeron hub must have a worker")
                .send(WorkerCommand::Join {
                    key: key.clone(),
                    reply: reply_tx,
                })
                .map_err(|_| anyhow!("Aeron publisher thread stopped"))?;
            reply_rx
                .recv()
                .map_err(|_| anyhow!("Aeron publisher thread dropped join reply"))??;
        }

        state.join(client_id, key);
        Ok(())
    }

    pub fn apply(&self, client_id: u64, request: &ControlRequest) -> Result<()> {
        let mut state = self.state.lock().expect("Aeron stream hub lock poisoned");
        state.apply(client_id, request);
        Ok(())
    }

    pub fn publish(&self, key: &SubscriptionKey, bytes: &[u8]) -> Result<usize> {
        let mut state = self.state.lock().expect("Aeron stream hub lock poisoned");
        let mut delivered = 0;
        let matching_groups = state.matching_groups(key);
        let mut groups = Vec::new();
        for stream_key in matching_groups {
            if state.record_if_new(&stream_key, key, bytes) {
                groups.push(stream_key);
                delivered += 1;
            }
        }
        drop(state);
        if !groups.is_empty() {
            match self
                .worker
                .as_ref()
                .expect("enabled Aeron hub must have a worker")
                .try_send(WorkerCommand::Publish {
                    groups,
                    bytes: bytes.to_vec(),
                }) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => bail!("Aeron publisher queue is full"),
                Err(TrySendError::Disconnected(_)) => bail!("Aeron publisher thread stopped"),
            }
        }
        Ok(delivered)
    }

    pub fn disconnect(&self, client_id: u64) {
        let mut state = self.state.lock().expect("Aeron stream hub lock poisoned");
        if let Some(key) = state.disconnect(client_id)
            && let Some(worker) = &self.worker
        {
            let _ = worker.try_send(WorkerCommand::Leave { key });
        }
    }

    pub fn group_count(&self) -> usize {
        self.state
            .lock()
            .expect("Aeron stream hub lock poisoned")
            .groups
            .len()
    }
}

enum WorkerCommand {
    Join {
        key: StreamKey,
        reply: std::sync::mpsc::Sender<Result<()>>,
    },
    Leave {
        key: StreamKey,
    },
    Publish {
        groups: Vec<StreamKey>,
        bytes: Vec<u8>,
    },
}

fn run_worker(receiver: Receiver<WorkerCommand>) {
    let mut client = None;
    let mut publishers = HashMap::<StreamKey, AeronPublisher>::new();
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::Join { key, reply } => {
                let result = (|| {
                    if client.is_none() {
                        client =
                            Some(AeronClient::connect().map_err(|error| {
                                anyhow!("failed to connect to Aeron: {error:?}")
                            })?);
                    }
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        publishers.entry(key.clone())
                    {
                        let publisher = client
                            .as_ref()
                            .expect("Aeron client must be initialized")
                            .publisher(&key.channel, key.stream_id)
                            .map_err(|error| {
                                anyhow!("failed to create Aeron publication: {error:?}")
                            })?;
                        entry.insert(publisher);
                    }
                    Ok(())
                })();
                let _ = reply.send(result);
            }
            WorkerCommand::Leave { key } => {
                publishers.remove(&key);
            }
            WorkerCommand::Publish { groups, bytes } => {
                for key in groups {
                    if let Some(publisher) = publishers.get(&key) {
                        let result = publisher.offer(&bytes);
                        if result < 0 {
                            warn!(
                                channel = %key.channel.to_channel_string(),
                                stream_id = key.stream_id,
                                result,
                                "Aeron publication offer failed"
                            );
                        }
                    }
                }
            }
        }
    }
}

fn same_subscription(left: &SubscriptionKey, right: &SubscriptionKey) -> bool {
    left.id == right.id && left.stream == right.stream
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::SubscriptionArg;
    use std::sync::mpsc::channel;

    fn ipc(stream_id: i32) -> StreamKey {
        StreamKey {
            channel: AeronChannel::Ipc,
            stream_id,
        }
    }

    #[test]
    fn multiple_clients_share_one_group_and_publish_the_union_once() {
        let group = ipc(1001);
        let mut state = HubState::default();
        state.join(1, group.clone());
        state.join(2, group.clone());
        state.apply(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([10], "bbo")]),
        );
        state.apply(
            2,
            &ControlRequest::subscribe(vec![
                SubscriptionArg::new([10], "bbo"),
                SubscriptionArg::new([20], "bbo"),
            ]),
        );

        assert_eq!(state.groups.len(), 1);
        assert_eq!(state.groups[&group].members.len(), 2);
        assert_eq!(
            state.matching_groups(&SubscriptionKey::new(10, "bbo")),
            vec![group.clone()]
        );
        assert_eq!(
            state.matching_groups(&SubscriptionKey::new(20, "bbo")),
            vec![group]
        );
    }

    #[test]
    fn shared_group_uses_lowest_noise_threshold() {
        let group = ipc(1001);
        let mut state = HubState::default();
        state.join(1, group.clone());
        state.join(2, group.clone());
        state.apply(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg {
                id: vec![10],
                stream: "bbo".to_string(),
                noise_filter_bps: Some(5.0),
            }]),
        );
        state.apply(
            2,
            &ControlRequest::subscribe(vec![SubscriptionArg {
                id: vec![10],
                stream: "bbo".to_string(),
                noise_filter_bps: Some(1.0),
            }]),
        );

        assert_eq!(
            state.effective_noise_filter(&group, 10, "bbo"),
            Some(Some(1.0))
        );
    }

    #[test]
    fn final_disconnect_removes_the_publication_group() {
        let group = ipc(1001);
        let mut state = HubState::default();
        state.join(1, group.clone());
        state.join(2, group.clone());

        assert_eq!(state.disconnect(1), None);
        assert!(state.groups.contains_key(&group));
        assert_eq!(state.disconnect(2), Some(group.clone()));
        assert!(!state.groups.contains_key(&group));
    }

    #[test]
    fn duplicate_frames_are_suppressed_even_when_arrivals_interleave() {
        let group = ipc(1001);
        let key = SubscriptionKey::new(10, "bbo");
        let mut state = HubState::default();

        assert!(state.record_if_new(&group, &key, b"event-1"));
        assert!(state.record_if_new(&group, &key, b"event-2"));
        assert!(!state.record_if_new(&group, &key, b"event-1"));
    }

    #[test]
    fn market_status_matches_any_stream_subscription_for_the_market() {
        let group = ipc(1001);
        let unrelated_group = ipc(2001);
        let mut state = HubState::default();
        state.join(1, group.clone());
        state.join(2, unrelated_group);
        state.apply(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([42], "trades")]),
        );
        state.apply(
            2,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([99], "bbo")]),
        );

        assert_eq!(
            state.matching_groups(&SubscriptionKey::new(42, "market_status")),
            vec![group]
        );
    }

    #[test]
    fn mock_worker_observes_one_publication_for_shared_stream_members() {
        let (worker_tx, worker_rx) = sync_channel(16);
        let (observed_tx, observed_rx) = channel();
        std::thread::spawn(move || {
            while let Ok(command) = worker_rx.recv() {
                match command {
                    WorkerCommand::Join { key, reply } => {
                        observed_tx.send(("join", key.stream_id)).unwrap();
                        reply.send(Ok(())).unwrap();
                    }
                    WorkerCommand::Publish { groups, .. } => {
                        observed_tx.send(("publish", groups.len() as i32)).unwrap();
                    }
                    WorkerCommand::Leave { key } => {
                        observed_tx.send(("leave", key.stream_id)).unwrap();
                    }
                }
            }
        });
        let hub = AeronStreamHub {
            enabled: true,
            state: Arc::new(Mutex::new(HubState::default())),
            worker: Some(worker_tx),
        };
        let config = AeronConfig {
            aeron_channel: AeronChannel::Ipc,
            stream_id: 1001,
        };

        hub.join(1, config.clone()).unwrap();
        hub.join(2, config).unwrap();
        assert_eq!(observed_rx.recv().unwrap(), ("join", 1001));
        assert!(observed_rx.try_recv().is_err());
        let request = ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]);
        hub.apply(1, &request).unwrap();
        hub.apply(2, &request).unwrap();
        let key = SubscriptionKey::new(42, "bbo");
        assert_eq!(hub.publish(&key, b"one-event").unwrap(), 1);
        assert_eq!(hub.publish(&key, b"one-event").unwrap(), 0);
        assert_eq!(observed_rx.recv().unwrap(), ("publish", 1));
        assert!(observed_rx.try_recv().is_err());

        hub.disconnect(1);
        assert!(observed_rx.try_recv().is_err());
        hub.disconnect(2);
        assert_eq!(observed_rx.recv().unwrap(), ("leave", 1001));
    }
}
