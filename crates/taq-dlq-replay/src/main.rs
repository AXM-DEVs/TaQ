use std::{env, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
};
use taq_dlq_replay::{ReplayOutcome, replay_dead_letter};
use taq_kafka::KafkaSecurityConfig;
use taq_observability::{
    ReplayMetricOutcome, Telemetry, global_delivery_metrics, wait_for_shutdown_signal,
};
use tokio::time::timeout;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let telemetry = Telemetry::init("taq-dlq-replay")?;
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
    let brokers = env::var("TAQ_KAFKA_BROKER").unwrap_or_else(|_| "localhost:19092".into());
    let group_id =
        env::var("TAQ_DLQ_CONSUMER_GROUP").unwrap_or_else(|_| "taq-dlq-replay-v1".into());
    let limit = positive_env("TAQ_DLQ_REPLAY_LIMIT", 1000)?;
    let idle_secs = positive_env("TAQ_DLQ_REPLAY_IDLE_SECS", 30)?;
    let kafka_security = KafkaSecurityConfig::from_env()?;
    let dry_run = match env::var("TAQ_DLQ_REPLAY_DRY_RUN") {
        Ok(value) => parse_dry_run_value(Some(&value))?,
        Err(env::VarError::NotPresent) => parse_dry_run_value(None)?,
        Err(error) => return Err(error).context("no se pudo leer TAQ_DLQ_REPLAY_DRY_RUN"),
    };

    let mut consumer_config = ClientConfig::new();
    kafka_security.apply(&mut consumer_config)?;
    let consumer: StreamConsumer = consumer_config
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("no se pudo crear el consumidor Kafka de replay")?;
    consumer
        .subscribe(&["events.dead_letter"])
        .context("no se pudo suscribir a events.dead_letter")?;

    let mut producer_config = ClientConfig::new();
    kafka_security.apply(&mut producer_config)?;
    let producer: FutureProducer = producer_config
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .context("no se pudo crear el productor Kafka de replay")?;

    info!(%brokers, %group_id, limit, dry_run, "inicia replay de DLQ");
    let mut stream = consumer.stream();
    let shutdown_signal = wait_for_shutdown_signal();
    tokio::pin!(shutdown_signal);
    let metrics = global_delivery_metrics();
    let mut handled = 0_u64;
    loop {
        if handled >= limit {
            break;
        }
        let message = tokio::select! {
            signal_result = &mut shutdown_signal => {
                let signal_name = signal_result?;
                info!(signal_name, handled, "señal recibida; se termina el replay después del mensaje confirmado actual");
                break;
            }
            next_result = timeout(Duration::from_secs(idle_secs), stream.next()) => match next_result {
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(error))) => return Err(error).context("falló la lectura de events.dead_letter"),
                Ok(None) => break,
                Err(_) => {
                    info!(handled, "fin de replay por inactividad");
                    break;
                }
            }
        };
        metrics.record_replay_read();
        let payload = match message.payload() {
            Some(payload) => payload,
            None => {
                metrics.record_replay_outcome(ReplayMetricOutcome::Failed);
                bail!("mensaje DLQ sin payload; offset queda sin confirmar");
            }
        };
        let producer = producer.clone();
        let replay_result = replay_dead_letter(
            payload,
            dry_run,
            move |event| async move {
                let encoded = serde_json::to_vec(&event).map_err(|error| error.to_string())?;
                let record = FutureRecord::to("events.raw")
                    .key(&event.id)
                    .payload(&encoded);
                producer
                    .send(record, Duration::ZERO)
                    .await
                    .map(|_| ())
                    .map_err(|(error, _)| error.to_string())?;
                metrics.record_replay_publish_ack();
                Ok(())
            },
            || {
                consumer
                    .commit_message(&message, CommitMode::Sync)
                    .map_err(|error| error.to_string())
            },
        )
        .await;
        let outcome = match replay_result {
            Ok(outcome) => outcome,
            Err(error) => {
                metrics.record_replay_outcome(ReplayMetricOutcome::Failed);
                return Err(error).with_context(|| {
                    format!(
                        "replay detenido; topic={} partition={} offset={} queda sin confirmar",
                        message.topic(),
                        message.partition(),
                        message.offset()
                    )
                });
            }
        };

        match outcome {
            ReplayOutcome::Validated { event_id } => {
                metrics.record_replay_outcome(ReplayMetricOutcome::Validated);
                info!(%event_id, offset = message.offset(), "evento validado; dry-run no publicó ni confirmó")
            }
            ReplayOutcome::Published { event_id } => {
                metrics.record_replay_outcome(ReplayMetricOutcome::Committed);
                info!(%event_id, offset = message.offset(), "evento republicado y offset confirmado")
            }
        }
        handled += 1;
    }

    if dry_run {
        info!(handled, "dry-run terminado; offsets sin confirmar")
    } else {
        info!(handled, "replay terminado")
    }
    if handled == 0 {
        info!("sin mensajes DLQ para procesar");
    }
    let flush_producer = producer.clone();
    tokio::task::spawn_blocking(move || flush_producer.flush(Duration::from_secs(10)))
        .await
        .context("falló la tarea de flush del productor de replay")?
        .context("no se pudieron vaciar las publicaciones pendientes del replay")?;
    Ok(())
}

fn positive_env(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse::<u64>()
                .with_context(|| format!("{name} debe ser un entero positivo"))?;
            if parsed == 0 {
                bail!("{name} debe ser mayor que cero");
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("no se pudo leer {name}")),
    }
}

fn parse_dry_run_value(value: Option<&str>) -> Result<bool> {
    match value {
        None => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(_) => bail!("TAQ_DLQ_REPLAY_DRY_RUN debe ser 'true' o 'false'"),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_dry_run_value;

    #[test]
    fn dry_run_setting_accepts_only_explicit_boolean_values() {
        assert!(parse_dry_run_value(None).expect("default is safe"));
        assert!(parse_dry_run_value(Some("TRUE")).expect("true accepted"));
        assert!(!parse_dry_run_value(Some("false")).expect("false accepted"));
        assert!(parse_dry_run_value(Some("maybe")).is_err());
    }
}
