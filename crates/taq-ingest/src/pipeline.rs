use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};
use serde::{Deserialize, Serialize};
use taq_core::Event;
use taq_observability::{IngestOutcome, global_delivery_metrics};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::{Deduplicator, DeduplicatorConfig, SourceCursorStore, SourceMessage, normalize_event};

pub const DEAD_LETTER_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PipelineStats {
    pub received: u64,
    pub published: u64,
    pub dead_lettered: u64,
    pub duplicate_skipped: u64,
    pub failed: u64,
}

impl PipelineStats {
    fn received(&mut self) {
        self.received = self.received.saturating_add(1);
    }

    fn outcome(&mut self, outcome: IngestOutcome) {
        let counter = match outcome {
            IngestOutcome::Published => &mut self.published,
            IngestOutcome::DeadLettered => &mut self.dead_lettered,
            IngestOutcome::DuplicateSkipped => &mut self.duplicate_skipped,
            IngestOutcome::Failed => &mut self.failed,
        };
        *counter = counter.saturating_add(1);
    }

    pub fn is_balanced(&self) -> bool {
        self.received
            == self
                .published
                .saturating_add(self.dead_lettered)
                .saturating_add(self.duplicate_skipped)
                .saturating_add(self.failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    #[serde(default = "default_dead_letter_schema_version")]
    pub schema_version: u16,
    pub source: String,
    pub native_id: String,
    pub failed_at_ms: i64,
    pub reason: String,
    pub event: Event,
}

fn default_dead_letter_schema_version() -> u16 {
    DEAD_LETTER_SCHEMA_VERSION
}

#[derive(Debug, Clone)]
pub struct KafkaPipelineConfig {
    pub brokers: String,
    pub raw_topic: String,
    pub dead_letter_topic: String,
    pub dedup_capacity: usize,
    pub dedup_retention: Duration,
}

pub struct KafkaPipeline {
    producer: FutureProducer,
    config: KafkaPipelineConfig,
    deduplicator: Deduplicator,
    cursor_store: Option<Arc<dyn SourceCursorStore>>,
}

impl KafkaPipeline {
    pub fn new(config: KafkaPipelineConfig) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "10000")
            .create()
            .context("no se pudo crear el productor Kafka de ingesta")?;
        let deduplicator = Deduplicator::new(DeduplicatorConfig {
            capacity: config.dedup_capacity,
            retention: config.dedup_retention,
        });
        Ok(Self {
            producer,
            config,
            deduplicator,
            cursor_store: None,
        })
    }

    pub fn with_cursor_store(mut self, store: Arc<dyn SourceCursorStore>) -> Self {
        self.cursor_store = Some(store);
        self
    }

    async fn persist_source_cursor(
        &self,
        source: &str,
        cursor: Option<&serde_json::Value>,
    ) -> Result<()> {
        let Some((store, cursor)) = self.cursor_store.as_ref().zip(cursor) else {
            return Ok(());
        };
        store
            .save(source, cursor)
            .await
            .map_err(|error| anyhow::anyhow!(error))
            .with_context(|| format!("no se pudo guardar el cursor durable de {source}"))
    }

    async fn acknowledge_source_event(
        &self,
        source: &str,
        native_id: &str,
        cursor: Option<&serde_json::Value>,
    ) -> Result<()> {
        let Some(store) = self.cursor_store.as_ref() else {
            return Ok(());
        };
        store
            .acknowledge(source, native_id, cursor)
            .await
            .map_err(|error| anyhow::anyhow!(error))
            .with_context(|| format!("no se pudo confirmar el evento de fuente {source}"))
    }

    async fn send_event(&self, event: &Event) -> std::result::Result<(), String> {
        let payload = serde_json::to_vec(event).map_err(|error| error.to_string())?;
        let record = FutureRecord::to(&self.config.raw_topic)
            .key(&event.id)
            .payload(&payload);
        self.producer
            .send(record, Duration::ZERO)
            .await
            .map(|_| ())
            .map_err(|(error, _)| error.to_string())
    }

    async fn send_dead_letter(&self, item: &DeadLetter) -> Result<()> {
        let payload = serde_json::to_vec(item).context("no se pudo serializar el dead-letter")?;
        let record = FutureRecord::to(&self.config.dead_letter_topic)
            .key(&item.event.id)
            .payload(&payload);
        self.producer
            .send(record, Duration::ZERO)
            .await
            .map(|_| ())
            .map_err(|(error, _)| error)
            .context("no se pudo publicar el dead-letter")
    }

    async fn send_source_rejection(
        &self,
        source: &str,
        native_id: &str,
        reason: &str,
        payload_bytes: usize,
        payload_hash: &str,
        rejected_at_ms: i64,
    ) -> Result<()> {
        let item = serde_json::json!({
            "schema_version":1,
            "source":source,
            "native_id":native_id,
            "reason":reason,
            "payload_bytes":payload_bytes,
            "payload_hash":payload_hash,
            "rejected_at_ms":rejected_at_ms,
        });
        let payload =
            serde_json::to_vec(&item).context("no se pudo serializar rechazo de fuente")?;
        let key = blake3::hash(format!("{source}|{native_id}").as_bytes())
            .to_hex()
            .to_string();
        let record = FutureRecord::to("events.source_dead_letter")
            .key(&key)
            .payload(&payload);
        self.producer
            .send(record, Duration::ZERO)
            .await
            .map(|_| ())
            .map_err(|(error, _)| error)
            .context("no se pudo publicar rechazo de fuente")
    }

    async fn flush(&self) -> Result<()> {
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || producer.flush(Duration::from_secs(10)))
            .await
            .context("falló la tarea de flush del productor Kafka")?
            .context("no se pudieron vaciar los mensajes pendientes de Kafka")
    }
}

pub async fn run_pipeline(
    mut input: mpsc::Receiver<(String, SourceMessage)>,
    mut pipeline: KafkaPipeline,
) -> Result<PipelineStats> {
    let mut stats = PipelineStats::default();
    let metrics = global_delivery_metrics();
    while let Some((source, source_message)) = input.recv().await {
        let (native, cursor) = match source_message {
            SourceMessage::Event { event, cursor } => (event, cursor),
            SourceMessage::Checkpoint(cursor) => {
                pipeline
                    .persist_source_cursor(&source, Some(&cursor))
                    .await?;
                continue;
            }
            SourceMessage::Rejected {
                native_id,
                reason,
                payload_bytes,
                payload_hash,
                cursor,
            } => {
                stats.received();
                metrics.record_ingest_received();
                let rejected_at_ms = now_ms()?;
                if let Err(error) = pipeline
                    .send_source_rejection(
                        &source,
                        &native_id,
                        &reason,
                        payload_bytes,
                        &payload_hash,
                        rejected_at_ms,
                    )
                    .await
                {
                    stats.outcome(IngestOutcome::Failed);
                    metrics.record_ingest_outcome_for_source(&source, IngestOutcome::Failed);
                    log_pipeline_failure(
                        &stats,
                        &format!("rechazo de fuente sin DLQ durable: {error:#}"),
                    );
                    return Err(error);
                }
                stats.outcome(IngestOutcome::DeadLettered);
                metrics.record_ingest_outcome_for_source(&source, IngestOutcome::DeadLettered);
                pipeline
                    .acknowledge_source_event(&source, &native_id, cursor.as_ref())
                    .await?;
                continue;
            }
        };
        stats.received();
        metrics.record_ingest_received();
        let native_id = native.native_id.clone();
        let ingested_at_ms = match now_ms() {
            Ok(timestamp) => timestamp,
            Err(error) => {
                stats.outcome(IngestOutcome::Failed);
                metrics.record_ingest_outcome_for_source(&source, IngestOutcome::Failed);
                log_pipeline_failure(&stats, &error.to_string());
                return Err(error);
            }
        };
        let event = normalize_event(&source, native, ingested_at_ms);
        if !pipeline
            .deduplicator
            .check_and_insert(&event.id, Instant::now())
        {
            stats.outcome(IngestOutcome::DuplicateSkipped);
            metrics.record_ingest_outcome_for_source(&source, IngestOutcome::DuplicateSkipped);
            info!(source, event_id = %event.id, "evento duplicado omitido");
            pipeline
                .acknowledge_source_event(&source, &native_id, cursor.as_ref())
                .await?;
            continue;
        }

        match pipeline.send_event(&event).await {
            Ok(()) => {
                stats.outcome(IngestOutcome::Published);
                metrics.record_ingest_outcome_for_source(&source, IngestOutcome::Published);
            }
            Err(reason) => {
                error!(source, event_id = %event.id, %reason, "falló la publicación del evento");
                let failed_at_ms = match now_ms() {
                    Ok(timestamp) => timestamp,
                    Err(error) => {
                        stats.outcome(IngestOutcome::Failed);
                        metrics.record_ingest_outcome_for_source(&source, IngestOutcome::Failed);
                        log_pipeline_failure(&stats, &error.to_string());
                        return Err(error);
                    }
                };
                let dead_letter = DeadLetter {
                    schema_version: DEAD_LETTER_SCHEMA_VERSION,
                    source: source.clone(),
                    native_id: native_id.clone(),
                    failed_at_ms,
                    reason,
                    event,
                };
                if let Err(error) = pipeline.send_dead_letter(&dead_letter).await {
                    stats.outcome(IngestOutcome::Failed);
                    metrics.record_ingest_outcome_for_source(&source, IngestOutcome::Failed);
                    log_pipeline_failure(
                        &stats,
                        &format!("publicación fallida y dead-letter no disponible: {error:#}"),
                    );
                    bail!("publicación fallida y dead-letter no disponible: {error:#}");
                }
                stats.outcome(IngestOutcome::DeadLettered);
                metrics.record_ingest_outcome_for_source(&source, IngestOutcome::DeadLettered);
            }
        }
        pipeline
            .acknowledge_source_event(&source, &native_id, cursor.as_ref())
            .await?;
    }
    if !stats.is_balanced() {
        log_pipeline_failure(&stats, "invariante de contabilización incumplida");
        bail!("invariante de contabilización incumplida al drenar el pipeline");
    }
    pipeline.flush().await.inspect_err(|error| {
        log_pipeline_failure(&stats, &error.to_string());
    })?;
    Ok(stats)
}

fn log_pipeline_failure(stats: &PipelineStats, reason: &str) {
    error!(
        reason,
        received = stats.received,
        published = stats.published,
        dead_lettered = stats.dead_lettered,
        duplicate_skipped = stats.duplicate_skipped,
        failed = stats.failed,
        accounted = stats.is_balanced(),
        "pipeline terminó con error; resumen de entrega"
    );
}

fn now_ms() -> Result<i64> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    i64::try_from(millis).context("timestamp excede el rango de i64")
}

#[cfg(test)]
mod tests {
    use super::{DeadLetter, PipelineStats};
    use serde_json::json;
    use taq_core::Event;
    use taq_observability::IngestOutcome;

    fn sample_event() -> Event {
        serde_json::from_value(json!({
            "schema_version": 2,
            "id": "event-1",
            "source": "rss",
            "kind": "rss_item",
            "entity_ids": [],
            "occurred_at_ms": 100,
            "occurred_at_quality": "source",
            "ingested_at_ms": 200,
            "payload": {}
        }))
        .expect("sample event matches the event contract")
    }

    #[test]
    fn dead_letter_serializes_with_current_schema_version() {
        let item = DeadLetter {
            schema_version: 1,
            source: "rss".into(),
            native_id: "native-1".into(),
            failed_at_ms: 200,
            reason: "broker timeout".into(),
            event: sample_event(),
        };

        let encoded = serde_json::to_value(item).expect("DLQ serializes");

        assert_eq!(encoded["schema_version"], 1);
    }

    #[test]
    fn dead_letter_without_version_deserializes_as_v1() {
        let legacy = json!({
            "source": "rss",
            "native_id": "native-1",
            "failed_at_ms": 200,
            "reason": "broker timeout",
            "event": {
                "schema_version": 2,
                "id": "event-1",
                "source": "rss",
                "kind": "rss_item",
                "entity_ids": [],
                "occurred_at_ms": 100,
                "occurred_at_quality": "source",
                "ingested_at_ms": 200,
                "payload": {}
            }
        });

        let item: DeadLetter = serde_json::from_value(legacy).expect("legacy DLQ decodes");

        assert_eq!(item.schema_version, 1);
        assert_eq!(item.event.id, "event-1");
    }

    #[test]
    fn pipeline_stats_balance_each_received_event_into_one_outcome() {
        let mut stats = PipelineStats::default();
        for outcome in [
            IngestOutcome::Published,
            IngestOutcome::DeadLettered,
            IngestOutcome::DuplicateSkipped,
            IngestOutcome::Failed,
        ] {
            stats.received();
            stats.outcome(outcome);
        }

        assert_eq!(stats.received, 4);
        assert_eq!(stats.published, 1);
        assert_eq!(stats.dead_lettered, 1);
        assert_eq!(stats.duplicate_skipped, 1);
        assert_eq!(stats.failed, 1);
        assert!(stats.is_balanced());
    }

    #[test]
    fn pipeline_stats_detect_unaccounted_events() {
        let mut stats = PipelineStats::default();
        stats.received();

        assert!(!stats.is_balanced());
    }
}
