use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    codec::WireMessage,
    protocol::{ControlOperation, ControlRequest, SubscriptionKey},
};

type Route = (i32, String);

#[derive(Debug, Clone, Copy)]
enum FilteredPrice {
    Bbo { bid: f64, ask: f64 },
    Trade(f64),
}

#[derive(Debug, Clone)]
struct NoiseFilter {
    threshold_bps: f64,
    last_published: Option<FilteredPrice>,
    last_published_at: Option<Instant>,
}

impl NoiseFilter {
    const BBO_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    fn new(threshold_bps: f64) -> Self {
        Self {
            threshold_bps,
            last_published: None,
            last_published_at: None,
        }
    }

    fn allows(&mut self, current: FilteredPrice, now: Instant) -> bool {
        let allowed = match (self.last_published, current) {
            (None, _) => true,
            (
                Some(FilteredPrice::Bbo {
                    bid: previous_bid,
                    ask: previous_ask,
                }),
                FilteredPrice::Bbo { bid, ask },
            ) => {
                price_moved_bps(previous_bid, bid, self.threshold_bps)
                    || price_moved_bps(previous_ask, ask, self.threshold_bps)
                    || self
                        .last_published_at
                        .is_some_and(|last| now.duration_since(last) >= Self::BBO_REFRESH_INTERVAL)
            }
            (Some(FilteredPrice::Trade(previous)), FilteredPrice::Trade(price)) => {
                price_moved_bps(previous, price, self.threshold_bps)
            }
            _ => true,
        };

        if allowed {
            self.last_published = Some(current);
            self.last_published_at = Some(now);
        }
        allowed
    }
}

/// Client-side subscription and noise filter for shared data planes.
///
/// WebSocket and raw UDP sessions are already routed per client, but Aeron
/// streams intentionally broadcast the union of all member subscriptions.
/// Applying this filter at every client keeps transport behavior identical.
#[derive(Debug, Clone, Default)]
pub struct FrameFilter {
    subscriptions: HashMap<Route, Option<f64>>,
    noise_filters: HashMap<Route, NoiseFilter>,
}

impl FrameFilter {
    pub fn apply(&mut self, request: &ControlRequest) {
        match request.op {
            ControlOperation::Subscribe => {
                for key in request.keys() {
                    self.insert(key);
                }
            }
            ControlOperation::Unsubscribe => {
                for key in request.keys() {
                    let route = (key.id, key.stream);
                    self.subscriptions.remove(&route);
                    self.noise_filters.remove(&route);
                }
            }
            ControlOperation::RefetchBbo => {}
        }
    }

    pub fn replace_subscriptions(
        &mut self,
        subscriptions: impl IntoIterator<Item = SubscriptionKey>,
    ) {
        self.subscriptions.clear();
        self.noise_filters.clear();
        for key in subscriptions {
            self.insert(key);
        }
    }

    pub fn accepts(&mut self, bytes: &[u8]) -> bool {
        self.accepts_at(bytes, Instant::now())
    }

    fn accepts_at(&mut self, bytes: &[u8], now: Instant) -> bool {
        let Ok(message) = WireMessage::decode(bytes) else {
            return false;
        };
        match message {
            WireMessage::Status(status) => {
                let subscribed = self
                    .subscriptions
                    .keys()
                    .any(|(id, _)| *id == status.market_id);
                if subscribed {
                    self.noise_filters
                        .retain(|(id, _), _| *id != status.market_id);
                    self.rebuild_noise_filters_for_id(status.market_id);
                }
                subscribed
            }
            WireMessage::Bbo(bbo) => self.accept_route(
                (bbo.market_id, "bbo".to_string()),
                Some(FilteredPrice::Bbo {
                    bid: bbo.bid,
                    ask: bbo.ask,
                }),
                now,
            ),
            WireMessage::Trade(trade) => self.accept_route(
                (trade.market_id, "trades".to_string()),
                Some(FilteredPrice::Trade(trade.price)),
                now,
            ),
            WireMessage::OrderBook(order_book) => self.accept_route(
                (
                    order_book.market_id,
                    format!("orderbook.{}", order_book.depth),
                ),
                None,
                now,
            ),
            WireMessage::FeatureBbo(feature) => self.accept_route(
                (feature.feature_id, "feature".to_string()),
                Some(FilteredPrice::Bbo {
                    bid: feature.bid,
                    ask: feature.ask,
                }),
                now,
            ),
            WireMessage::Other(_) => false,
        }
    }

    fn insert(&mut self, key: SubscriptionKey) {
        let threshold = key.noise_filter_bps();
        let route = (key.id, key.stream);
        self.subscriptions.insert(route.clone(), threshold);
        match threshold {
            Some(threshold) => {
                self.noise_filters
                    .insert(route, NoiseFilter::new(threshold));
            }
            None => {
                self.noise_filters.remove(&route);
            }
        }
    }

    fn accept_route(&mut self, route: Route, price: Option<FilteredPrice>, now: Instant) -> bool {
        if !self.subscriptions.contains_key(&route) {
            return false;
        }
        match (self.noise_filters.get_mut(&route), price) {
            (Some(filter), Some(price)) => filter.allows(price, now),
            _ => true,
        }
    }

    fn rebuild_noise_filters_for_id(&mut self, id: i32) {
        for (route, threshold) in &self.subscriptions {
            if route.0 == id
                && let Some(threshold) = threshold
            {
                self.noise_filters
                    .insert(route.clone(), NoiseFilter::new(*threshold));
            }
        }
    }
}

#[inline]
fn price_moved_bps(previous: f64, current: f64, threshold_bps: f64) -> bool {
    if previous == current {
        return threshold_bps == 0.0;
    }
    if !previous.is_finite() || !current.is_finite() || previous == 0.0 {
        return true;
    }
    ((current - previous).abs() / previous.abs()) * 10_000.0 >= threshold_bps
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec::{Bbo, MarketStatus},
        protocol::SubscriptionArg,
    };

    fn bbo(market_id: i32, bid: f64, ask: f64) -> Vec<u8> {
        Bbo {
            market_id,
            timestamp_mdp_in: 1,
            bid,
            bid_volume: 2.0,
            ask,
            ask_volume: 3.0,
            event_id: 4,
            ts_mono_mdp_out: 5,
        }
        .encode_le()
        .to_vec()
    }

    #[test]
    fn shared_stream_frames_are_filtered_per_client_and_noise_threshold() {
        let mut filter = FrameFilter::default();
        filter.apply(&ControlRequest::subscribe(vec![SubscriptionArg {
            id: vec![101],
            stream: "bbo".to_string(),
            noise_filter_bps: Some(10.0),
        }]));

        let start = Instant::now();
        assert!(filter.accepts_at(&bbo(101, 100.0, 101.0), start));
        assert!(!filter.accepts_at(&bbo(101, 100.05, 101.05), start + Duration::from_millis(10)));
        assert!(filter.accepts_at(&bbo(101, 100.2, 101.2), start + Duration::from_millis(20)));
        assert!(!filter.accepts_at(&bbo(202, 100.0, 101.0), start + Duration::from_millis(30)));
        assert!(!filter.accepts(&[1, 101, 0, 0, 0]));
        assert!(!filter.accepts(&[99, 1, 2, 3]));
    }

    #[test]
    fn market_status_is_delivered_by_market_and_resets_noise_state() {
        let mut filter = FrameFilter::default();
        filter.apply(&ControlRequest::subscribe(vec![SubscriptionArg {
            id: vec![101],
            stream: "bbo".to_string(),
            noise_filter_bps: Some(10.0),
        }]));
        assert!(filter.accepts(&bbo(101, 100.0, 101.0)));
        assert!(!filter.accepts(&bbo(101, 100.01, 101.01)));

        let status = MarketStatus {
            market_id: 101,
            timestamp_mdp_in: 2,
            state: 3,
            event_id: 4,
            ts_mono_mdp_out: 5,
        }
        .encode_le();
        assert!(filter.accepts(&status));
        assert!(filter.accepts(&bbo(101, 100.01, 101.01)));
    }
}
