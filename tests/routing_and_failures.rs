use market_data_link::{
    ControlRequest, FrameFilter, SubscriptionArg, SubscriptionKey, SubscriptionRouter,
    codec::{Bbo, FeatureBbo, MarketStatus, WireMessage},
};

#[tokio::test]
async fn router_reports_backpressure_and_closed_channels() {
    let router = SubscriptionRouter::default();
    let mut session = router.connect(1, 1);
    router
        .apply(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]),
        )
        .unwrap();
    let key = SubscriptionKey::new(42, "bbo");

    assert_eq!(router.publish(&key, &[1]).delivered, 1);
    assert_eq!(router.publish(&key, &[2]).full, 1);
    assert_eq!(
        session.outbound.as_mut().unwrap().recv().await,
        Some(vec![1])
    );

    let outbound = session.outbound.take().unwrap();
    drop(outbound);
    assert_eq!(router.publish(&key, &[3]).closed, 1);
    assert_eq!(router.session_count(), 0);
}

#[test]
fn mock_market_status_resets_fm_and_propagates_neutral_feature_to_te() {
    let status = MarketStatus {
        market_id: 42,
        timestamp_mdp_in: 10,
        state: 0x4000,
        event_id: 11,
        ts_mono_mdp_out: 12,
    };
    let mut fm_filter = FrameFilter::default();
    fm_filter.apply(&ControlRequest::subscribe(vec![SubscriptionArg::new(
        [42],
        "bbo",
    )]));
    assert!(fm_filter.accepts(&status.encode_le()));

    let neutral = FeatureBbo::neutral_for_status(101, &status, 13, 14, 15, 16);
    assert_eq!((neutral.bid, neutral.ask), (1.0, 1.0));

    let mut te_filter = FrameFilter::default();
    te_filter.apply(&ControlRequest::subscribe(vec![SubscriptionArg::new(
        [101],
        "feature",
    )]));
    let bytes = neutral.encode_le();
    assert!(te_filter.accepts(&bytes));
    assert!(matches!(
        WireMessage::decode(&bytes).unwrap(),
        WireMessage::FeatureBbo(feature)
            if feature.feature_id == 101
                && feature.market_id == 42
                && feature.bid == 1.0
                && feature.ask == 1.0
    ));
}

#[tokio::test]
async fn disconnect_removes_all_session_subscriptions() {
    let router = SubscriptionRouter::default();
    let _session = router.connect(9, 4);
    router
        .apply(
            9,
            &ControlRequest::subscribe(vec![
                SubscriptionArg::new([1], "bbo"),
                SubscriptionArg::new([101], "feature"),
            ]),
        )
        .unwrap();
    router.disconnect(9);

    assert_eq!(router.session_count(), 0);
    assert_eq!(
        router.publish(&SubscriptionKey::new(1, "bbo"), &[1]),
        Default::default()
    );
}

#[tokio::test]
async fn mock_mdp_to_fm_and_fm_to_te_routes_keep_streams_separate() {
    let router = SubscriptionRouter::default();
    let mut fm = router.connect(1, 4);
    let mut te = router.connect(2, 4);
    router
        .apply(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]),
        )
        .unwrap();
    router
        .apply(
            2,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([101], "feature")]),
        )
        .unwrap();

    let market = Bbo {
        market_id: 42,
        timestamp_mdp_in: 1,
        bid: 10.0,
        bid_volume: 2.0,
        ask: 11.0,
        ask_volume: 3.0,
        event_id: 4,
        ts_mono_mdp_out: 5,
    }
    .encode_le();
    let feature = FeatureBbo {
        feature_id: 101,
        market_id: 42,
        timestamp_mdp_in: 1,
        bid: 10.1,
        bid_volume: 2.0,
        ask: 10.9,
        ask_volume: 3.0,
        event_id: 4,
        ts_mono_mdp_out: 5,
        mdp_received_ts_ns: 6,
        feature_start_ts_ns: 7,
        feature_done_ts_ns: 8,
        signal_bps: 9.0,
    }
    .encode_le();

    assert_eq!(
        router
            .publish(&SubscriptionKey::new(42, "bbo"), &market)
            .delivered,
        1
    );
    assert_eq!(
        router
            .publish(&SubscriptionKey::new(101, "feature"), &feature)
            .delivered,
        1
    );

    let market_received = fm.outbound.as_mut().unwrap().recv().await.unwrap();
    let feature_received = te.outbound.as_mut().unwrap().recv().await.unwrap();
    assert!(matches!(
        WireMessage::decode(&market_received).unwrap(),
        WireMessage::Bbo(value) if value.market_id == 42
    ));
    assert!(matches!(
        WireMessage::decode(&feature_received).unwrap(),
        WireMessage::FeatureBbo(value)
            if value.feature_id == 101 && value.market_id == 42
    ));
    assert!(fm.outbound.as_mut().unwrap().try_recv().is_err());
    assert!(te.outbound.as_mut().unwrap().try_recv().is_err());
}
