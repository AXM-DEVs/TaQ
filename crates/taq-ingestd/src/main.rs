mod config;

use anyhow::{Context, Result, anyhow, bail};
use std::time::Duration;
use taq_connector_bluesky::BlueskySource;
use taq_connector_rss::RssSource;
use taq_connector_wikimedia::WikimediaSource;
use taq_ingest::{
    KafkaPipeline, KafkaPipelineConfig, RetryPolicy, Source, SourceError, SourceMessage,
    bounded_source_channel, run_pipeline, run_source_with_backoff,
};
use taq_observability::wait_for_shutdown_signal;
use taq_observability::{Telemetry, global_delivery_metrics};
use taq_state::PgCursorStore;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let telemetry = Telemetry::init("taq-ingestd")?;
    let result = run().await;
    let shutdown = telemetry.shutdown().await;
    match (result, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => {
            warn!(%error, "falló el flush de métricas OTLP durante un apagado normal");
            Ok(())
        }
        (Err(error), Err(shutdown_error)) => {
            warn!(%shutdown_error, "no se pudieron vaciar métricas OTLP durante el apagado");
            Err(error)
        }
    }
}

async fn run() -> Result<()> {
    info!("Iniciando taq-ingestd...");

    let settings = config::Settings::from_env()?;
    let config = KafkaPipelineConfig {
        brokers: settings.kafka_broker.clone(),
        raw_topic: "events.raw".to_string(),
        dead_letter_topic: "events.dead_letter".to_string(),
        dedup_capacity: 100_000,
        dedup_retention: Duration::from_secs(3600),
    };

    let cursor_store = std::sync::Arc::new(PgCursorStore::connect(&settings.database_url).await?);
    let pipeline = KafkaPipeline::new(config)?.with_cursor_store(cursor_store.clone());
    let (main_tx, main_rx) = mpsc::channel(settings.source_queue_capacity.saturating_mul(3));
    let retry_policy = RetryPolicy::new(settings.retry_initial_delay, settings.retry_max_delay);

    // Pipeline task
    let mut pipeline_task = tokio::spawn(run_pipeline(main_rx, pipeline));

    // --- Wikimedia ---
    let (wiki_tx, wiki_rx) = bounded_source_channel(settings.source_queue_capacity)?;
    let wikimedia = WikimediaSource::with_http_settings(
        settings.wikimedia_url.clone(),
        &settings.user_agent,
        settings.connect_timeout,
    )?
    .with_cursor_store(cursor_store.clone());
    let wiki_name = wikimedia.name().to_string();
    let main_tx_wiki = main_tx.clone();
    let wiki_retry_policy = retry_policy;

    let mut wiki_source_task = tokio::spawn(async move {
        run_source_with_backoff(&wikimedia, wiki_tx, wiki_retry_policy).await
    });
    let wiki_fwd_task = tokio::spawn(forward_source_events(
        wiki_rx,
        main_tx_wiki,
        wiki_name,
        settings.wikimedia_events_per_second,
    ));

    // --- Bluesky ---
    let (bsky_tx, bsky_rx) = bounded_source_channel(settings.source_queue_capacity)?;
    let bluesky = BlueskySource::with_connection_settings(
        settings.bluesky_url.clone(),
        settings.user_agent.clone(),
        settings.connect_timeout,
    )
    .with_cursor_store(cursor_store.clone());
    let bsky_name = bluesky.name().to_string();
    let main_tx_bsky = main_tx.clone();
    let bsky_retry_policy = retry_policy;

    let mut bsky_source_task =
        tokio::spawn(
            async move { run_source_with_backoff(&bluesky, bsky_tx, bsky_retry_policy).await },
        );
    let bsky_fwd_task = tokio::spawn(forward_source_events(
        bsky_rx,
        main_tx_bsky,
        bsky_name,
        settings.bluesky_events_per_second,
    ));

    // --- RSS ---
    let (rss_tx, rss_rx) = bounded_source_channel(settings.source_queue_capacity)?;
    let rss = RssSource::with_http_settings(
        settings.rss_url.clone(),
        settings.rss_poll_interval,
        &settings.user_agent,
        settings.connect_timeout,
        settings.request_timeout,
    )?
    .with_cursor_store(cursor_store.clone())
    .with_retry_policy(retry_policy);
    let rss_name = rss.name().to_string();
    let main_tx_rss = main_tx.clone();
    let rss_retry_policy = retry_policy;

    let mut rss_source_task =
        tokio::spawn(async move { run_source_with_backoff(&rss, rss_tx, rss_retry_policy).await });
    let rss_fwd_task = tokio::spawn(forward_source_events(
        rss_rx,
        main_tx_rss,
        rss_name,
        settings.rss_events_per_second,
    ));

    // Any unexpected critical task exit fails the process so the runtime
    // supervisor can restart it instead of leaving a half-alive daemon.
    tokio::select! {
        signal_result = wait_for_shutdown_signal() => match signal_result {
            Ok(signal_name) => info!(signal_name, "recibida señal de apagado, terminando ingestd..."),
            Err(err) => return Err(err).context("falló el listener de señales de apagado"),
        },
        result = &mut pipeline_task => match result {
            Ok(Ok(stats)) => bail!("el pipeline terminó antes de la señal de apagado (recibidos={}, publicados={}, DLQ={}, duplicados={}, fallidos={})", stats.received, stats.published, stats.dead_lettered, stats.duplicate_skipped, stats.failed),
            Ok(Err(err)) => return Err(err).context("falló el pipeline de Kafka"),
            Err(err) => return Err(anyhow!(err).context("terminó la tarea del pipeline")),
        },
        result = &mut wiki_source_task => return Err(source_task_exit("wikimedia", result)),
        result = &mut bsky_source_task => return Err(source_task_exit("bluesky", result)),
        result = &mut rss_source_task => return Err(source_task_exit("rss", result)),
    }

    // Stop producers first. Dropping their output senders lets each forwarder
    // drain its bounded channel and then exit naturally.
    wiki_source_task.abort();
    bsky_source_task.abort();
    rss_source_task.abort();

    let _ = wiki_source_task.await;
    let _ = bsky_source_task.await;
    let _ = rss_source_task.await;

    tokio::time::timeout(Duration::from_secs(60), async {
        let wiki_fwd_result = wiki_fwd_task.await;
        let bsky_fwd_result = bsky_fwd_task.await;
        let rss_fwd_result = rss_fwd_task.await;

        // Closing the final sender lets the pipeline drain all queued events
        // before it flushes Kafka and returns.
        drop(main_tx);
        let stats = pipeline_task
            .await
            .context("falló la tarea del pipeline durante el apagado")??;
        info!(
            received = stats.received,
            published = stats.published,
            dead_lettered = stats.dead_lettered,
            duplicate_skipped = stats.duplicate_skipped,
            failed = stats.failed,
            accounted = stats.is_balanced(),
            "pipeline drenado; resumen de entrega"
        );

        wiki_fwd_result.context("falló el reenviador de Wikimedia")??;
        bsky_fwd_result.context("falló el reenviador de Bluesky")??;
        rss_fwd_result.context("falló el reenviador RSS")??;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("el apagado ordenado excedió el límite de 60 segundos")??;

    Ok(())
}

async fn forward_source_events(
    mut input: mpsc::Receiver<SourceMessage>,
    output: mpsc::Sender<(String, SourceMessage)>,
    source: String,
    events_per_second: u32,
) -> Result<()> {
    let interval = Duration::from_secs_f64(1.0 / f64::from(events_per_second));
    let mut next_send = tokio::time::Instant::now();
    while let Some(event) = input.recv().await {
        let metrics = global_delivery_metrics();
        metrics.record_ingest_queue_depth(
            "connector",
            Some(&source),
            input.max_capacity().saturating_sub(input.capacity()),
        );
        metrics.record_ingest_queue_depth(
            "pipeline",
            Some(&source),
            output.max_capacity().saturating_sub(output.capacity()),
        );
        if matches!(
            &event,
            SourceMessage::Event { .. } | SourceMessage::Rejected { .. }
        ) {
            tokio::time::sleep_until(next_send).await;
            next_send = tokio::time::Instant::now() + interval;
        }
        output
            .send((source.clone(), event))
            .await
            .map_err(|_| anyhow!("el pipeline cerró el canal de la fuente {source}"))?;
    }
    Ok(())
}

fn source_task_exit(
    source: &str,
    result: std::result::Result<std::result::Result<(), SourceError>, tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(Ok(())) => anyhow!("el conector {source} terminó inesperadamente"),
        Ok(Err(error)) => anyhow!("terminó el conector {source}: {error}"),
        Err(error) => anyhow!("falló la tarea del conector {source}: {error}"),
    }
}
