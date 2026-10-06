use serde::{Deserialize, Serialize};
use serde_json::Value;
use taq_core::{Event, TimestampQuality};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeEvent {
    pub native_id: String,
    pub kind: String,
    pub entity_ids: Vec<String>,
    pub occurred_at_ms: i64,
    #[serde(default)]
    pub occurred_at_quality: TimestampQuality,
    pub payload: Value,
}

pub fn normalize_event(source: &str, native: NativeEvent, ingested_at_ms: i64) -> Event {
    let id = blake3::hash(format!("{source}|{}", native.native_id).as_bytes())
        .to_hex()
        .to_string();
    Event {
        schema_version: 2,
        id,
        source: source.to_owned(),
        kind: native.kind,
        entity_ids: native.entity_ids,
        occurred_at_ms: native.occurred_at_ms,
        occurred_at_quality: native.occurred_at_quality,
        ingested_at_ms,
        payload: native.payload,
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeEvent, normalize_event};
    use serde_json::json;
    use taq_core::TimestampQuality;

    #[test]
    fn normalization_uses_stable_source_and_native_id_hash() {
        let event = NativeEvent {
            native_id: "item-42".into(),
            kind: "mention".into(),
            entity_ids: vec!["taq".into()],
            occurred_at_ms: 1_700_000_000_000,
            occurred_at_quality: TimestampQuality::Source,
            payload: json!({"text": "TaQ"}),
        };

        let first = normalize_event("feed", event.clone(), 1_700_000_000_100);
        let retry = normalize_event("feed", event.clone(), 1_700_000_000_200);
        let other_source = normalize_event("other", event, 1_700_000_000_100);

        assert_eq!(first.id, retry.id);
        assert_ne!(first.id, other_source.id);
        assert_eq!(first.ingested_at_ms, 1_700_000_000_100);
        assert_eq!(first.schema_version, 2);
        assert_eq!(first.occurred_at_quality, TimestampQuality::Source);
    }
}
