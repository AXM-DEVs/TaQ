use std::{env, future::Future, sync::OnceLock, time::Duration};

use anyhow::{Context, Result};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Meter},
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};

pub struct Telemetry {
    provider: SdkMeterProvider,
}

impl Telemetry {
    pub fn init(service_name: &str) -> Result<Self> {
        let endpoint =
            env::var("TAQ_OTEL_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4317".into());
        let exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .context("no se pudo crear el exportador de métricas OTLP")?;
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter)
            .with_resource(
                Resource::builder()
                    .with_service_name(service_name.to_owned())
                    .build(),
            )
            .build();
        global::set_meter_provider(provider.clone());
        Ok(Self { provider })
    }

    pub async fn shutdown(self) -> Result<()> {
        let provider = self.provider;
        tokio::time::timeout(
            Duration::from_secs(8),
            tokio::task::spawn_blocking(move || provider.shutdown()),
        )
        .await
        .context("flush OTLP excedió ocho segundos")?
        .context("falló la tarea de cierre de métricas OTLP")?
        .context("no se pudieron vaciar las métricas OTLP")
    }
}

pub async fn wait_for_shutdown_signal() -> Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).context("no se pudo registrar SIGTERM")?;
        select_shutdown_signal(tokio::signal::ctrl_c(), async move {
            let _ = terminate.recv().await;
        })
        .await
    }
    #[cfg(not(unix))]
    {
        select_shutdown_signal(tokio::signal::ctrl_c(), std::future::pending()).await
    }
}

async fn select_shutdown_signal<C, T>(ctrl_c: C, sigterm: T) -> Result<&'static str>
where
    C: Future<Output = std::io::Result<()>>,
    T: Future<Output = ()>,
{
    tokio::select! {
        result = ctrl_c => result.map(|()| "Ctrl+C").context("falló el listener de Ctrl+C"),
        () = sigterm => Ok("SIGTERM"),
    }
}

#[derive(Clone)]
pub struct DeliveryMetrics {
    ingest_received: Counter<u64>,
    ingest_outcomes: Counter<u64>,
    ingest_source_outcomes: Counter<u64>,
    source_retries: Counter<u64>,
    source_retry_by_source: Counter<u64>,
    ingest_queue_depth: opentelemetry::metrics::Histogram<u64>,
    replay_reads: Counter<u64>,
    replay_events: Counter<u64>,
    replay_publish_acks: Counter<u64>,
    context_events: Counter<u64>,
    context_findings: Counter<u64>,
    context_failures: Counter<u64>,
    context_lag_records: opentelemetry::metrics::Histogram<u64>,
    api_requests: Counter<u64>,
    api_rate_limited: Counter<u64>,
}

impl DeliveryMetrics {
    pub fn new(meter: Meter) -> Self {
        Self {
            ingest_received: meter.u64_counter("ingest_events_received").build(),
            ingest_outcomes: meter.u64_counter("ingest_events").build(),
            ingest_source_outcomes: meter.u64_counter("ingest_events_by_source").build(),
            source_retries: meter.u64_counter("ingest_source_retries").build(),
            source_retry_by_source: meter.u64_counter("ingest_source_retries_by_source").build(),
            ingest_queue_depth: meter.u64_histogram("ingest_queue_depth").build(),
            replay_reads: meter.u64_counter("dlq_replay_reads").build(),
            replay_events: meter.u64_counter("dlq_replay_events").build(),
            replay_publish_acks: meter.u64_counter("dlq_replay_publish_acks").build(),
            context_events: meter.u64_counter("context_events_consumed").build(),
            context_findings: meter.u64_counter("context_findings_created").build(),
            context_failures: meter.u64_counter("context_processing_failures").build(),
            context_lag_records: meter.u64_histogram("context_kafka_lag_records").build(),
            api_requests: meter.u64_counter("api_requests").build(),
            api_rate_limited: meter.u64_counter("api_rate_limited").build(),
        }
    }

    pub fn record_ingest_received(&self) {
        self.ingest_received.add(1, &[]);
    }

    pub fn record_ingest_outcome(&self, outcome: IngestOutcome) {
        self.ingest_outcomes
            .add(1, &[KeyValue::new("outcome", outcome.label())]);
    }

    pub fn record_ingest_outcome_for_source(&self, source: &str, outcome: IngestOutcome) {
        self.record_ingest_outcome(outcome);
        self.ingest_source_outcomes.add(
            1,
            &[
                KeyValue::new("source", source.to_owned()),
                KeyValue::new("outcome", outcome.label()),
            ],
        );
    }

    pub fn record_source_retry(&self) {
        self.source_retries.add(1, &[]);
    }

    pub fn record_source_retry_for(&self, source: &str) {
        self.record_source_retry();
        self.source_retry_by_source
            .add(1, &[KeyValue::new("source", source.to_owned())]);
    }

    pub fn record_ingest_queue_depth(&self, stage: &str, source: Option<&str>, depth: usize) {
        let mut attributes = vec![KeyValue::new("stage", stage.to_owned())];
        if let Some(source) = source {
            attributes.push(KeyValue::new("source", source.to_owned()));
        }
        self.ingest_queue_depth.record(depth as u64, &attributes);
    }

    pub fn record_replay_read(&self) {
        self.replay_reads.add(1, &[]);
    }

    pub fn record_replay_outcome(&self, outcome: ReplayMetricOutcome) {
        self.replay_events
            .add(1, &[KeyValue::new("outcome", outcome.label())]);
    }

    pub fn record_replay_publish_ack(&self) {
        self.replay_publish_acks.add(1, &[]);
    }

    pub fn record_context_event(&self, findings: u64, lag_records: Option<u64>) {
        self.context_events.add(1, &[]);
        self.context_findings.add(findings, &[]);
        if let Some(lag_records) = lag_records {
            self.context_lag_records.record(lag_records, &[]);
        }
    }

    pub fn record_context_failure(&self) {
        self.context_failures.add(1, &[]);
    }

    pub fn record_api_request(&self) {
        self.api_requests.add(1, &[]);
    }

    pub fn record_api_rate_limited(&self) {
        self.api_rate_limited.add(1, &[]);
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IngestOutcome {
    Published,
    DeadLettered,
    DuplicateSkipped,
    Failed,
}

impl IngestOutcome {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::DeadLettered => "dead_lettered",
            Self::DuplicateSkipped => "duplicate_skipped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ReplayMetricOutcome {
    Validated,
    Committed,
    Failed,
}

impl ReplayMetricOutcome {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Validated => "validated",
            Self::Committed => "committed",
            Self::Failed => "failed",
        }
    }
}

static GLOBAL_DELIVERY_METRICS: OnceLock<DeliveryMetrics> = OnceLock::new();

pub fn global_delivery_metrics() -> &'static DeliveryMetrics {
    GLOBAL_DELIVERY_METRICS.get_or_init(|| DeliveryMetrics::new(global::meter("taq")))
}

#[cfg(test)]
mod tests {
    use super::{DeliveryMetrics, IngestOutcome, ReplayMetricOutcome, select_shutdown_signal};
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    #[test]
    fn counters_export_terminal_outcomes_without_event_identifiers() {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let metrics = DeliveryMetrics::new(provider.meter("taq-test"));

        metrics.record_ingest_received();
        metrics.record_ingest_outcome(IngestOutcome::Published);
        metrics.record_source_retry();
        metrics.record_replay_read();
        metrics.record_replay_publish_ack();
        metrics.record_replay_outcome(ReplayMetricOutcome::Committed);
        provider.force_flush().expect("test metrics export");

        let collected = format!(
            "{:?}",
            exporter.get_finished_metrics().expect("metrics collected")
        );
        assert!(collected.contains("ingest_events_received"));
        assert!(collected.contains("ingest_events"));
        assert!(collected.contains("ingest_source_retries"));
        assert!(collected.contains("dlq_replay_events"));
        assert!(collected.contains("dlq_replay_publish_acks"));
        assert!(collected.contains("published"));
        assert!(collected.contains("committed"));
        assert!(!collected.contains("private-event-id"));
        provider.shutdown().expect("test provider shuts down");
    }

    #[test]
    fn ingest_outcomes_are_a_bounded_set_of_labels() {
        let labels = [
            IngestOutcome::Published,
            IngestOutcome::DeadLettered,
            IngestOutcome::DuplicateSkipped,
            IngestOutcome::Failed,
        ]
        .map(IngestOutcome::label);

        assert_eq!(
            labels,
            ["published", "dead_lettered", "duplicate_skipped", "failed"]
        );
    }

    #[tokio::test]
    async fn shutdown_signal_selector_handles_sigterm_and_ctrl_c() {
        use std::{future::pending, io};

        let ctrl_c = select_shutdown_signal(async { Ok::<(), io::Error>(()) }, pending());
        assert_eq!(ctrl_c.await.expect("Ctrl+C selected"), "Ctrl+C");

        let sigterm = select_shutdown_signal(pending(), async {}).await;
        assert_eq!(sigterm.expect("SIGTERM selected"), "SIGTERM");
    }
}
