use std::future::Future;

use anyhow::{Context, Result, anyhow, bail};
use taq_core::Event;
use taq_ingest::{DEAD_LETTER_SCHEMA_VERSION, DeadLetter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    Validated { event_id: String },
    Published { event_id: String },
}

/// Validate one DLQ envelope, publish its original event, then commit its source offset.
/// The callback boundary makes the ACK-before-commit order testable without a live broker.
pub async fn replay_dead_letter<P, PFut, C>(
    payload: &[u8],
    dry_run: bool,
    publish: P,
    commit: C,
) -> Result<ReplayOutcome>
where
    P: FnOnce(Event) -> PFut,
    PFut: Future<Output = Result<(), String>>,
    C: FnOnce() -> Result<(), String>,
{
    let letter: DeadLetter =
        serde_json::from_slice(payload).context("DLQ envelope JSON inválido")?;
    if letter.schema_version != DEAD_LETTER_SCHEMA_VERSION {
        bail!(
            "versión DLQ no soportada: {}; versión admitida: {}",
            letter.schema_version,
            DEAD_LETTER_SCHEMA_VERSION
        );
    }
    let event: Event = letter.event;
    if !matches!(event.schema_version, 1 | 2) {
        bail!("versión de evento no soportada: {}", event.schema_version);
    }
    let event_id = event.id.clone();
    if dry_run {
        return Ok(ReplayOutcome::Validated { event_id });
    }

    publish(event)
        .await
        .map_err(|error| anyhow!("falló la republicación a events.raw: {error}"))?;
    commit().map_err(|error| anyhow!("falló el commit del offset DLQ: {error}"))?;
    Ok(ReplayOutcome::Published { event_id })
}

#[cfg(test)]
mod tests {
    use super::{ReplayOutcome, replay_dead_letter};
    use anyhow::Result;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn dead_letter(version: Option<u16>) -> Vec<u8> {
        let mut value = json!({
            "source": "rss",
            "native_id": "native-1",
            "failed_at_ms": 300,
            "reason": "broker timeout",
            "event": {
                "schema_version": 2,
                "id": "stable-event-id",
                "source": "rss",
                "kind": "rss_item",
                "entity_ids": [],
                "occurred_at_ms": 100,
                "occurred_at_quality": "source",
                "ingested_at_ms": 200,
                "payload": {"headline": "original"}
            }
        });
        if let Some(version) = version {
            value["schema_version"] = json!(version);
        }
        serde_json::to_vec(&value).expect("test DLQ JSON serializes")
    }

    #[tokio::test]
    async fn legacy_envelope_republishes_event_then_commits() -> Result<()> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let publish_calls = Arc::clone(&calls);
        let commit_calls = Arc::clone(&calls);

        let outcome = replay_dead_letter(
            &dead_letter(None),
            false,
            move |event| {
                let calls = Arc::clone(&publish_calls);
                async move {
                    assert_eq!(event.id, "stable-event-id");
                    assert_eq!(event.ingested_at_ms, 200);
                    calls.lock().expect("calls lock").push("publish_ack");
                    Ok(())
                }
            },
            move || {
                commit_calls.lock().expect("calls lock").push("commit");
                Ok(())
            },
        )
        .await?;

        assert_eq!(
            outcome,
            ReplayOutcome::Published {
                event_id: "stable-event-id".into()
            }
        );
        assert_eq!(
            *calls.lock().expect("calls lock"),
            ["publish_ack", "commit"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn publish_failure_does_not_commit_dlq_offset() {
        let commits = Arc::new(Mutex::new(0));
        let commit_count = Arc::clone(&commits);
        let result = replay_dead_letter(
            &dead_letter(Some(1)),
            false,
            |_| async { Err("broker unavailable".to_string()) },
            move || {
                *commit_count.lock().expect("commits lock") += 1;
                Ok(())
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(*commits.lock().expect("commits lock"), 0);
    }

    #[tokio::test]
    async fn dry_run_validates_but_neither_publishes_nor_commits() -> Result<()> {
        let calls = Arc::new(Mutex::new(0));
        let publish_calls = Arc::clone(&calls);
        let commit_calls = Arc::clone(&calls);
        let outcome = replay_dead_letter(
            &dead_letter(None),
            true,
            move |_| {
                let calls = Arc::clone(&publish_calls);
                async move {
                    *calls.lock().expect("calls lock") += 1;
                    Ok(())
                }
            },
            move || {
                *commit_calls.lock().expect("calls lock") += 1;
                Ok(())
            },
        )
        .await?;

        assert_eq!(
            outcome,
            ReplayOutcome::Validated {
                event_id: "stable-event-id".into()
            }
        );
        assert_eq!(*calls.lock().expect("calls lock"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_schema_and_malformed_json_are_not_committed() {
        let mut incomplete: serde_json::Value =
            serde_json::from_slice(&dead_letter(Some(1))).expect("test DLQ is valid JSON");
        incomplete["event"]
            .as_object_mut()
            .unwrap()
            .remove("payload");
        let incomplete = serde_json::to_vec(&incomplete).expect("incomplete test DLQ serializes");
        for payload in [dead_letter(Some(99)), b"{bad json".to_vec(), incomplete] {
            let commits = Arc::new(Mutex::new(0));
            let commit_count = Arc::clone(&commits);
            let result = replay_dead_letter(
                &payload,
                false,
                |_| async { Ok(()) },
                move || {
                    *commit_count.lock().expect("commits lock") += 1;
                    Ok(())
                },
            )
            .await;

            assert!(result.is_err());
            assert_eq!(*commits.lock().expect("commits lock"), 0);
        }
    }

    #[tokio::test]
    async fn unsupported_event_schema_is_not_committed() {
        let mut payload: serde_json::Value =
            serde_json::from_slice(&dead_letter(Some(1))).expect("test DLQ is valid JSON");
        payload["event"]["schema_version"] = json!(99);
        let encoded = serde_json::to_vec(&payload).expect("test DLQ serializes");
        let commits = Arc::new(Mutex::new(0));
        let commit_count = Arc::clone(&commits);
        let result = replay_dead_letter(
            &encoded,
            false,
            |_| async { Ok(()) },
            move || {
                *commit_count.lock().expect("commits lock") += 1;
                Ok(())
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(*commits.lock().expect("commits lock"), 0);
    }
}
