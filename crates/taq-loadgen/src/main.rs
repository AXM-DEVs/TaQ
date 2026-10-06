use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use futures_util::{
    FutureExt,
    stream::{FuturesUnordered, StreamExt},
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};
use serde_json::json;
use taq_core::{Event, TimestampQuality};
use tokio::time::sleep;
use tracing::{error, info};

const MAX_IN_FLIGHT: usize = 1024;
const MAX_EVENT_CACHE: usize = 4096;

#[derive(Debug, Clone)]
struct EventSample {
    native_id: String,
    payload: Vec<u8>,
}

fn remember_event(cache: &mut VecDeque<EventSample>, event: EventSample, capacity: usize) {
    if capacity == 0 {
        return;
    }
    if cache.len() == capacity {
        cache.pop_front();
    }
    cache.push_back(event);
}

struct Config {
    rate: u64,
    duration_s: u64,
    entities: u64,
    duplicate_ratio: f64,
}

fn parse_args() -> Result<Config> {
    let mut args = std::env::args().skip(1);
    let mut rate = None;
    let mut duration_s = None;
    let mut entities = 100_u64;
    let mut duplicate_ratio = 0.0_f64;

    while let Some(flag) = args.next() {
        let value = args
            .next()
            .with_context(|| format!("falta valor para {flag}"))?;
        match flag.as_str() {
            "--rate" => {
                rate = Some(
                    value
                        .parse()
                        .context("--rate debe ser un entero positivo")?,
                )
            }
            "--duration" => {
                duration_s = Some(
                    value
                        .parse()
                        .context("--duration debe ser un entero positivo")?,
                )
            }
            "--entities" => {
                entities = value
                    .parse()
                    .context("--entities debe ser un entero positivo")?
            }
            "--duplicate-ratio" => {
                duplicate_ratio = value
                    .parse()
                    .context("--duplicate-ratio debe estar entre 0 y 1")?
            }
            _ => bail!("opción desconocida: {flag}"),
        }
    }

    let config = Config {
        rate: rate.context("falta --rate")?,
        duration_s: duration_s.context("falta --duration")?,
        entities,
        duplicate_ratio,
    };
    if config.rate == 0 || config.duration_s == 0 || config.entities == 0 {
        bail!("--rate, --duration y --entities deben ser positivos");
    }
    if !(0.0..=1.0).contains(&config.duplicate_ratio) {
        bail!("--duplicate-ratio debe estar entre 0 y 1");
    }
    Ok(config)
}

fn now_ms() -> Result<i64> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    i64::try_from(millis).context("timestamp excede el rango de i64")
}

fn event_id(source: &str, native_id: &str) -> String {
    blake3::hash(format!("{source}|{native_id}").as_bytes())
        .to_hex()
        .to_string()
}

fn account_result(
    (event, is_unique, delivery): (EventSample, bool, std::result::Result<(), String>),
    event_cache: &mut VecDeque<EventSample>,
    unique_sent: &mut u64,
    sent: &mut u64,
    errors: &mut u64,
) {
    match delivery {
        Ok(()) => {
            *sent += 1;
            if is_unique {
                *unique_sent += 1;
                remember_event(event_cache, event, MAX_EVENT_CACHE);
            }
        }
        Err(message) => {
            *errors += 1;
            error!(%message, "falló el envío del evento");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = parse_args()?;
    let broker = std::env::var("TAQ_KAFKA_BROKER").unwrap_or_else(|_| "localhost:19092".into());
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "10000")
        .set("queue.buffering.max.messages", "1000000")
        .create()
        .context("no se pudo crear el productor Kafka")?;

    let deadline = Instant::now() + Duration::from_secs(config.duration_s);
    let started = Instant::now();
    let total = config.rate.saturating_mul(config.duration_s);
    let mut event_cache: VecDeque<EventSample> = VecDeque::with_capacity(MAX_EVENT_CACHE);
    let mut pending = FuturesUnordered::new();
    let mut rng = StdRng::seed_from_u64(42);
    let mut sent = 0_u64;
    let mut unique_sent = 0_u64;
    let mut errors = 0_u64;
    let mut last_report_sent = 0_u64;
    let mut last_report_errors = 0_u64;
    let mut last_report = Instant::now();
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

    for sequence in 0..total {
        let scheduled = started + Duration::from_secs_f64(sequence as f64 / config.rate as f64);
        let now = Instant::now();
        if scheduled > now {
            sleep(scheduled - now).await;
        }

        let (event, is_unique) = if !event_cache.is_empty()
            && rng.random::<f64>() < config.duplicate_ratio
        {
            (
                event_cache[rng.random_range(0..event_cache.len())].clone(),
                false,
            )
        } else {
            let native_id = format!("synthetic-{run_id}-{sequence}");
            let timestamp = now_ms()?;
            let event = Event {
                schema_version: 2,
                id: event_id("loadgen", &native_id),
                source: "loadgen".into(),
                kind: "mention".into(),
                entity_ids: vec![format!("entity-{}", sequence % config.entities)],
                occurred_at_ms: timestamp,
                occurred_at_quality: TimestampQuality::Source,
                ingested_at_ms: timestamp,
                payload: json!({"native_id": native_id, "sequence": sequence, "synthetic": true}),
            };
            let payload = serde_json::to_vec(&event)?;
            (EventSample { native_id, payload }, true)
        };
        let producer = producer.clone();
        pending.push(async move {
            let id = event_id("loadgen", &event.native_id);
            let record = FutureRecord::to("events.raw")
                .key(&id)
                .payload(&event.payload);
            let result = producer
                .send(record, Duration::from_secs(1))
                .await
                .map(|_| ())
                .map_err(|(err, _)| err.to_string());
            (event, is_unique, result)
        });

        if pending.len() >= MAX_IN_FLIGHT {
            let result = pending.next().await.context("futuros de envío vacíos")?;
            account_result(
                result,
                &mut event_cache,
                &mut unique_sent,
                &mut sent,
                &mut errors,
            );
        }
        while let Some(result) = pending.next().now_or_never().flatten() {
            account_result(
                result,
                &mut event_cache,
                &mut unique_sent,
                &mut sent,
                &mut errors,
            );
        }
        if last_report.elapsed() >= Duration::from_secs(1) {
            let elapsed = last_report.elapsed().as_secs_f64().max(0.001);
            info!(
                events_per_second = ((sent - last_report_sent) as f64 / elapsed) as u64,
                errors = errors - last_report_errors,
                "avance de carga"
            );
            last_report_sent = sent;
            last_report_errors = errors;
            last_report = Instant::now();
        }
    }

    while let Some(result) = pending.next().await {
        account_result(
            result,
            &mut event_cache,
            &mut unique_sent,
            &mut sent,
            &mut errors,
        );
    }
    producer
        .flush(Duration::from_secs(10))
        .context("falló el vaciado del productor")?;
    if Instant::now() < deadline {
        sleep(deadline - Instant::now()).await;
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "total_enviados={sent} unique_enviados={unique_sent} errores={errors} tasa_real={:.0} ev/s",
        sent as f64 / elapsed
    );
    if errors > 0 {
        bail!("el generador terminó con {errors} errores de envío");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{EventSample, remember_event};
    use std::collections::VecDeque;

    #[test]
    fn event_sample_cache_discards_oldest_entry_at_capacity() {
        let mut cache = VecDeque::new();
        remember_event(
            &mut cache,
            EventSample {
                native_id: "event-1".into(),
                payload: vec![1],
            },
            2,
        );
        remember_event(
            &mut cache,
            EventSample {
                native_id: "event-2".into(),
                payload: vec![2],
            },
            2,
        );
        remember_event(
            &mut cache,
            EventSample {
                native_id: "event-3".into(),
                payload: vec![3],
            },
            2,
        );

        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.front().map(|event| event.native_id.as_str()),
            Some("event-2")
        );
        assert_eq!(
            cache.back().map(|event| event.native_id.as_str()),
            Some("event-3")
        );
    }
}
