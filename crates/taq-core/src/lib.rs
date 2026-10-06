use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Versión del esquema serializado para evolución compatible.
    #[serde(default = "default_schema_version")]
    pub schema_version: u16,
    /// Hash determinista de source + id nativo; hace idempotente la ingesta.
    pub id: String,
    pub source: String,
    pub kind: String,
    pub entity_ids: Vec<String>,
    pub occurred_at_ms: i64,
    /// Indicates whether the occurrence timestamp came from the source or a capture-time fallback.
    #[serde(default)]
    pub occurred_at_quality: TimestampQuality,
    pub ingested_at_ms: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampQuality {
    Source,
    CaptureTime,
    #[default]
    Unknown,
}

fn default_schema_version() -> u16 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    Burst,
    Anomaly,
    Forecast,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub id: String,
    pub entity_id: String,
    pub kind: SignalKind,
    pub score: f32,
    pub horizon_s: Option<u32>,
    /// Ids de los eventos que originaron la señal; permite auditar la predicción.
    pub evidence: Vec<String>,
    pub model_version: String,
    pub emitted_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub id: String,
    pub signal_id: String,
    pub target: String,
    pub payload: Value,
    /// Evita acciones duplicadas ante reintentos o señales repetidas.
    pub dedup_key: String,
}

#[cfg(test)]
mod tests {
    use super::{Action, Event, Signal, SignalKind, TimestampQuality};
    use serde_json::json;

    #[test]
    fn event_round_trips_with_json_payload_and_timestamps() {
        let event = Event {
            schema_version: 2,
            id: "stable-id".into(),
            source: "synthetic".into(),
            kind: "mention".into(),
            entity_ids: vec!["entity-1".into()],
            occurred_at_ms: 1_700_000_000_000,
            occurred_at_quality: TimestampQuality::Source,
            ingested_at_ms: 1_700_000_000_100,
            payload: json!({"text": "TaQ", "score": 1}),
        };

        let encoded = serde_json::to_string(&event).expect("event serializes");
        let decoded: Event = serde_json::from_str(&encoded).expect("event deserializes");
        let value = serde_json::to_value(decoded).expect("event converts to value");

        assert_eq!(value["id"], "stable-id");
        assert_eq!(value["entity_ids"][0], "entity-1");
        assert_eq!(value["payload"]["text"], "TaQ");
    }

    #[test]
    fn event_without_schema_version_defaults_to_version_one() {
        let encoded = r#"{"id":"legacy","source":"test","kind":"mention","entity_ids":[],"occurred_at_ms":1,"ingested_at_ms":2,"payload":{}}"#;
        let event: Event = serde_json::from_str(encoded).expect("legacy event deserializes");

        assert_eq!(event.schema_version, 1);
        assert_eq!(event.occurred_at_quality, TimestampQuality::Unknown);
    }

    #[test]
    fn timestamp_quality_serializes_as_a_stable_string() {
        assert_eq!(
            serde_json::to_string(&TimestampQuality::CaptureTime).unwrap(),
            "\"capture_time\""
        );
    }

    #[test]
    fn signal_kind_uses_snake_case_and_signal_round_trips() {
        let signal = Signal {
            id: "signal-1".into(),
            entity_id: "entity-1".into(),
            kind: SignalKind::Burst,
            score: 0.95,
            horizon_s: Some(900),
            evidence: vec!["event-1".into()],
            model_version: "baseline-v1".into(),
            emitted_at_ms: 1_700_000_001_000,
        };

        let encoded = serde_json::to_string(&signal).expect("signal serializes");
        assert!(encoded.contains("\"kind\":\"burst\""));
        let decoded: Signal = serde_json::from_str(&encoded).expect("signal deserializes");
        let value = serde_json::to_value(decoded).expect("signal converts to value");
        assert_eq!(value["evidence"][0], "event-1");
        assert_eq!(value["horizon_s"], 900);
    }

    #[test]
    fn action_round_trips_payload_and_deduplication_key() {
        let action = Action {
            id: "action-1".into(),
            signal_id: "signal-1".into(),
            target: "discord".into(),
            payload: json!({"message": "alert"}),
            dedup_key: "discord:signal-1".into(),
        };

        let value = serde_json::to_value(action).expect("action serializes");
        let decoded: Action = serde_json::from_value(value.clone()).expect("action deserializes");
        assert_eq!(value["dedup_key"], "discord:signal-1");
        assert_eq!(decoded.payload["message"], "alert");
    }
}
