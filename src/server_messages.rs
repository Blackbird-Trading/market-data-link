//! JSON messages sent by market-data servers over the mandatory control plane.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlReply {
    TransportReady { transport: TransportReady },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransportReady {
    Udp { server_port: u16 },
    AeronIpc { stream_id: i32 },
    AeronUdp { stream_id: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamError {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    pub severity: u8,
    pub message: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlEvent {
    StreamError {
        #[serde(flatten)]
        error: StreamError,
    },
}

impl ControlEvent {
    pub fn stream_error(error: StreamError) -> Self {
        Self::StreamError { error }
    }

    pub fn into_stream_error(self) -> StreamError {
        match self {
            Self::StreamError { error } => error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_ready_is_uncorrelated_and_has_no_protocol_identity() {
        let reply = ControlReply::TransportReady {
            transport: TransportReady::AeronIpc { stream_id: 42 },
        };
        let value = serde_json::to_value(&reply).unwrap();
        assert_eq!(value["type"], "transport_ready");
        assert_eq!(value["transport"]["type"], "aeron_ipc");
        assert!(value.get("request_id").is_none());
        assert!(value.get("protocol").is_none());
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"type":"transport_ready","transport":{"type":"aeron_ipc","stream_id":42}}"#
        );
    }

    #[test]
    fn udp_transport_ready_advertises_only_the_source_port() {
        let reply = ControlReply::TransportReady {
            transport: TransportReady::Udp { server_port: 52781 },
        };
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"type":"transport_ready","transport":{"type":"udp","server_port":52781}}"#
        );

        let reply = ControlReply::TransportReady {
            transport: TransportReady::AeronUdp { stream_id: 42 },
        };
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"type":"transport_ready","transport":{"type":"aeron_udp","stream_id":42}}"#
        );
    }

    #[test]
    fn stream_error_is_a_json_text_control_event() {
        let event = ControlEvent::stream_error(StreamError {
            id: Some(7),
            stream: Some("bbo".into()),
            severity: 1,
            message: "feed disconnected".into(),
            timestamp: 123,
        });
        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["type"], "stream_error");
        assert_eq!(value["id"], 7);
        assert_eq!(value["message"], "feed disconnected");
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"type":"stream_error","id":7,"stream":"bbo","severity":1,"message":"feed disconnected","timestamp":123}"#
        );
    }
}
