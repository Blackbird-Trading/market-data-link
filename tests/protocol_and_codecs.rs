use market_data_link::{
    ClientTransportConfig, ControlEvent, ControlOperation, ControlReply, ControlRequest,
    StreamError, SubscriptionArg, TransportSelection,
    codec::{
        BBO_WIRE_LEN, Bbo, CodecError, FEATURE_BBO_WIRE_LEN, FeatureBbo, MarketStatus,
        ORDER_BOOK_HEADER_LEN, ORDER_BOOK_LEVEL_LEN, OrderBook, OrderBookView,
        SNAPSHOT_MESSAGE_TYPE, STATUS_WIRE_LEN, Trade, WireMessage,
    },
};

#[test]
fn canonical_feature_subscription_json() {
    let request = ControlRequest::subscribe(vec![SubscriptionArg::new([101], "feature")]);
    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["op"], "subscribe");
    assert_eq!(json["args"][0]["id"], serde_json::json!([101]));
    assert_eq!(json["args"][0]["stream"], "feature");
    assert!(request.validate().is_ok());

    let decoded: ControlRequest = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.op, ControlOperation::Subscribe);
    assert_eq!(decoded, request);
}

#[test]
fn protocol_validation_rejects_empty_and_invalid_values() {
    assert!(
        ControlRequest::subscribe(vec![SubscriptionArg::new([], "bbo")])
            .validate()
            .is_err()
    );
    assert!(
        ControlRequest::subscribe(vec![SubscriptionArg::new([1], "")])
            .validate()
            .is_err()
    );
    let mut arg = SubscriptionArg::new([1], "bbo");
    arg.noise_filter_bps = Some(f64::NAN);
    assert!(ControlRequest::subscribe(vec![arg]).validate().is_err());
}

#[test]
fn client_transport_configuration_is_tagged_and_minimal() {
    assert!(
        serde_json::from_value::<ClientTransportConfig>(serde_json::json!({"type": "websocket"}))
            .is_err()
    );
    assert_eq!(
        serde_json::to_value(ClientTransportConfig::Udp).unwrap(),
        serde_json::json!({"type": "udp"})
    );
    assert_eq!(
        serde_json::to_value(ClientTransportConfig::AeronIpc { stream_id: 2001 }).unwrap(),
        serde_json::json!({"type": "aeron_ipc", "stream_id": 2001})
    );
    assert_eq!(
        serde_json::to_value(ClientTransportConfig::AeronUdp { stream_id: 2002 }).unwrap(),
        serde_json::json!({"type": "aeron_udp", "stream_id": 2002})
    );
    assert_eq!(
        serde_json::to_value(TransportSelection::AeronUdp {
            client_port: 40123,
            stream_id: 2002,
        })
        .unwrap(),
        serde_json::json!({
            "type": "aeron_udp",
            "client_port": 40123,
            "stream_id": 2002
        })
    );
}

#[test]
fn runtime_stream_errors_are_distinct_from_request_errors() {
    let error = StreamError {
        id: Some(42),
        stream: Some("bbo".to_string()),
        severity: 3,
        message: "exchange disconnected".to_string(),
        timestamp: 123,
    };
    let value = serde_json::to_value(ControlEvent::stream_error(error.clone())).unwrap();
    assert_eq!(value["type"], "stream_error");
    assert_eq!(
        serde_json::from_value::<ControlEvent>(value)
            .unwrap()
            .into_stream_error(),
        error
    );
    assert!(
        serde_json::from_value::<ControlReply>(
            serde_json::json!({"type":"stream_error","severity":3,"message":"x","timestamp":1})
        )
        .is_err()
    );
}

#[test]
fn bbo_round_trip_matches_wire_layout() {
    let value = Bbo {
        market_id: 42,
        timestamp_mdp_in: 123,
        bid: 10.0,
        bid_volume: 2.0,
        ask: 11.0,
        ask_volume: 3.0,
        event_id: 99,
        ts_mono_mdp_out: 456,
    };
    let encoded = value.encode_le();
    assert_eq!(encoded.len(), BBO_WIRE_LEN);
    assert_eq!(Bbo::decode_le(&encoded).unwrap(), value);
    assert_eq!(
        WireMessage::decode(&encoded).unwrap(),
        WireMessage::Bbo(value)
    );
}

#[test]
fn trade_round_trip_matches_wire_layout() {
    let value = Trade {
        market_id: 8,
        timestamp_mdp_in: 123,
        price: 10.5,
        quantity: 7.0,
        side: -1,
        event_id: 100,
        ts_mono_mdp_out: 789,
    };
    assert_eq!(Trade::decode_le(&value.encode_le()).unwrap(), value);
}

#[test]
fn order_book_round_trip_and_depth_validation() {
    let value = OrderBook {
        message_type: 3,
        depth: 1,
        market_id: 9,
        update_id: 10,
        timestamp_mdp_in: 11,
        timestamp_matching_engine: 12,
        timestamp: 13,
        event_id: 14,
        timestamp_mdp_out: 15,
        bids: vec![(100.0, 2.0)],
        asks: vec![(101.0, 3.0)],
    };
    let encoded = value.encode_le().unwrap();
    assert_eq!(OrderBook::decode_le(&encoded).unwrap(), value);

    let mut invalid = value;
    invalid.depth = 5;
    assert_eq!(invalid.encode_le().unwrap_err(), CodecError::DepthMismatch);
}

#[test]
fn order_book_view_reuses_caller_owned_encoding_buffer() {
    let bids = [(100.0, 2.0), (99.0, 3.0)];
    let asks = [(101.0, 4.0), (102.0, 5.0)];
    let view = OrderBookView {
        message_type: SNAPSHOT_MESSAGE_TYPE,
        depth: 2,
        market_id: 42,
        update_id: 9,
        timestamp_mdp_in: 10,
        timestamp_matching_engine: 11,
        timestamp: 12,
        event_id: 13,
        timestamp_mdp_out: 14,
        bids: &bids,
        asks: &asks,
    };
    let mut buffer = Vec::with_capacity(ORDER_BOOK_HEADER_LEN + 2 * ORDER_BOOK_LEVEL_LEN);
    view.encode_le_into(&mut buffer).unwrap();
    let allocation = buffer.as_ptr();
    view.encode_le_into(&mut buffer).unwrap();

    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(OrderBook::decode_le(&buffer).unwrap().bids, bids);
}

#[test]
fn feature_bbo_round_trip_matches_te_layout() {
    let value = FeatureBbo {
        feature_id: 101,
        market_id: 42,
        timestamp_mdp_in: 1,
        bid: 2.0,
        bid_volume: 3.0,
        ask: 4.0,
        ask_volume: 5.0,
        event_id: 6,
        ts_mono_mdp_out: 7,
        mdp_received_ts_ns: 8,
        feature_start_ts_ns: 9,
        feature_done_ts_ns: 10,
        signal_bps: 11.0,
    };
    let encoded = value.encode_le();
    assert_eq!(encoded.len(), FEATURE_BBO_WIRE_LEN);
    assert_eq!(FeatureBbo::decode_le(&encoded).unwrap(), value);
    assert_eq!(
        WireMessage::decode(&encoded).unwrap(),
        WireMessage::FeatureBbo(value)
    );
}

#[test]
fn malformed_fixed_width_messages_are_rejected() {
    assert!(matches!(
        Bbo::decode_le(&[1; 12]),
        Err(CodecError::InvalidLength { .. })
    ));
    assert!(WireMessage::decode(&[]).is_err());
}

#[test]
fn market_status_round_trip_preserves_reset_event_context() {
    let value = MarketStatus {
        market_id: 42,
        timestamp_mdp_in: 1_700_000_000_000_000,
        state: 0x4000,
        event_id: 77,
        ts_mono_mdp_out: 99,
    };
    let encoded = value.encode_le();
    assert_eq!(encoded.len(), STATUS_WIRE_LEN);
    assert_eq!(MarketStatus::decode_le(&encoded).unwrap(), value);
    assert_eq!(
        WireMessage::decode(&encoded).unwrap(),
        WireMessage::Status(value)
    );

    let neutral = FeatureBbo::neutral_for_status(101, &value, 100, 101, 102, 103);
    assert_eq!(neutral.feature_id, 101);
    assert_eq!(neutral.market_id, 42);
    assert_eq!((neutral.bid, neutral.ask), (1.0, 1.0));
    assert_eq!(neutral.signal_bps, 0.0);
    assert_eq!(neutral.event_id, value.event_id);
}
