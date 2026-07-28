use market_data_link::{
    ClientRouter, ControlRequest, SessionControl, StreamError, SubscriptionArg, SubscriptionKey,
    codec::{Bbo, FeatureBbo, MarketStatus, WireMessage},
};

#[tokio::test]
async fn router_reports_backpressure_and_closed_channels() {
    let router = ClientRouter::default();
    let mut session = router.register_client(1, 1);
    router
        .update_subscriptions(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]),
        )
        .unwrap();
    let key = SubscriptionKey::new(42, "bbo");

    assert_eq!(router.send_data(&key, &[1]).delivered, 1);
    assert_eq!(router.send_data(&key, &[2]).full, 1);
    assert_eq!(
        session.udp_outbound.as_mut().unwrap().recv().await,
        Some(vec![1])
    );

    let outbound = session.udp_outbound.take().unwrap();
    drop(outbound);
    assert_eq!(router.send_data(&key, &[3]).closed, 1);
    assert_eq!(router.session_count(), 0);
}

#[tokio::test]
async fn stream_errors_are_scoped_and_control_backpressure_removes_session() {
    let router = ClientRouter::default();
    let mut matching = router.register_client(1, 1);
    let mut other = router.register_client(2, 1);
    router
        .update_subscriptions(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]),
        )
        .unwrap();
    router
        .update_subscriptions(
            2,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([43], "bbo")]),
        )
        .unwrap();
    let error = StreamError {
        id: Some(42),
        stream: Some("bbo".to_string()),
        severity: 1,
        message: "down".to_string(),
        timestamp: 10,
    };
    assert_eq!(router.send_error(&error).delivered, 1);
    assert!(matches!(
        matching.control.as_mut().unwrap().recv().await,
        Some(SessionControl::StreamError(received)) if received == error
    ));
    assert!(other.control.as_mut().unwrap().try_recv().is_err());

    assert_eq!(router.send_error(&error).delivered, 1);
    let report = router.send_error(&error);
    assert_eq!(report.full, 1);
    assert_eq!(router.session_count(), 1);
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
    assert!(matches!(
        WireMessage::decode(&status.encode_le()).unwrap(),
        WireMessage::Status(decoded) if decoded.market_id == 42
    ));

    let neutral = FeatureBbo::neutral_for_status(101, &status, 13, 14, 15, 16);
    assert_eq!((neutral.bid, neutral.ask), (1.0, 1.0));

    let bytes = neutral.encode_le();
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
    let router = ClientRouter::default();
    let _session = router.register_client(9, 4);
    router
        .update_subscriptions(
            9,
            &ControlRequest::subscribe(vec![
                SubscriptionArg::new([1], "bbo"),
                SubscriptionArg::new([101], "feature"),
            ]),
        )
        .unwrap();
    router.remove_client(9);
    router.remove_client(9);

    assert_eq!(router.session_count(), 0);
    assert_eq!(
        router.send_data(&SubscriptionKey::new(1, "bbo"), &[1]),
        Default::default()
    );
}

#[tokio::test]
async fn mock_mdp_to_fm_and_fm_to_te_routes_keep_streams_separate() {
    let router = ClientRouter::default();
    let mut fm = router.register_client(1, 4);
    let mut te = router.register_client(2, 4);
    router
        .update_subscriptions(
            1,
            &ControlRequest::subscribe(vec![SubscriptionArg::new([42], "bbo")]),
        )
        .unwrap();
    router
        .update_subscriptions(
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
            .send_data(&SubscriptionKey::new(42, "bbo"), &market)
            .delivered,
        1
    );
    assert_eq!(
        router
            .send_data(&SubscriptionKey::new(101, "feature"), &feature)
            .delivered,
        1
    );

    let market_received = fm.udp_outbound.as_mut().unwrap().recv().await.unwrap();
    let feature_received = te.udp_outbound.as_mut().unwrap().recv().await.unwrap();
    assert!(matches!(
        WireMessage::decode(&market_received).unwrap(),
        WireMessage::Bbo(value) if value.market_id == 42
    ));
    assert!(matches!(
        WireMessage::decode(&feature_received).unwrap(),
        WireMessage::FeatureBbo(value)
            if value.feature_id == 101 && value.market_id == 42
    ));
    assert!(fm.udp_outbound.as_mut().unwrap().try_recv().is_err());
    assert!(te.udp_outbound.as_mut().unwrap().try_recv().is_err());
}
