use std::{
    collections::HashSet,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};
use serde_json::json;
use taq_core::Event;
use tokio::{task::JoinSet, time::sleep};
use tracing::{error, info};

const MAX_IN_FLIGHT: usize = 1024;

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
    result: std::result::Result<(String, std::result::Result<(), String>), tokio::task::JoinError>,
    unique_ids: &mut HashSet<String>,
    sent: &mut u64,
    errors: &mut u64,
) -> Result<()> {
    let (id, delivery) = result.context("tarea de envío terminó inesperadamente")?;
    match delivery {
        Ok(()) => {
            *sent += 1;
            unique_ids.insert(id);
        }
        Err(message) => {
            *errors += 1;
            error!(%message, "falló el envío del evento");
        }
    }
    Ok(())
}

async fn collect_one(
    pending: &mut JoinSet<(String, std::result::Result<(), String>)>,
    unique_ids: &mut HashSet<String>,
    sent: &mut u64,
    errors: &mut u64,
) -> Result<()> {
    let Some(result) = pending.join_next().await else {
        return Ok(());
    };
    account_result(result, unique_ids, sent, errors)
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
    let mut events: Vec<(String, Vec<u8>)> = Vec::new();
    let mut unique_ids = HashSet::new();
    let mut pending = JoinSet::new();
    let mut rng = StdRng::seed_from_u64(42);
    let mut sent = 0_u64;
    let mut errors = 0_u64;
    let mut last_report_sent = 0_u64;
    let mut last_report_errors = 0_u64;
    let mut last_report = Instant::now();

    for sequence in 0..total {
        let scheduled = started + Duration::from_secs_f64(sequence as f64 / config.rate as f64);
        let now = Instant::now();
        if scheduled > now {
            sleep(scheduled - now).await;
        }

        let (native_id, payload) = if !events.is_empty()
            && rng.random::<f64>() < config.duplicate_ratio
        {
            events[rng.random_range(0..events.len())].clone()
        } else {
            let native_id = format!("synthetic-{sequence}");
            let timestamp = now_ms()?;
            let event = Event {
                id: event_id("loadgen", &native_id),
                source: "loadgen".into(),
                kind: "mention".into(),
                entity_ids: vec![format!("entity-{}", sequence % config.entities)],
                occurred_at_ms: timestamp,
                ingested_at_ms: timestamp,
                payload: json!({"native_id": native_id, "sequence": sequence, "synthetic": true}),
            };
            let payload = serde_json::to_vec(&event)?;
            events.push((native_id.clone(), payload.clone()));
            (native_id, payload)
        };
        let id = event_id("loadgen", &native_id);
        let producer = producer.clone();
        pending.spawn(async move {
            let record = FutureRecord::to("events.raw").key(&id).payload(&payload);
            let result = producer
                .send(record, Duration::from_secs(0))
                .await
                .map(|_| ())
                .map_err(|(err, _)| err.to_string());
            (id, result)
        });

        if pending.len() >= MAX_IN_FLIGHT {
            collect_one(&mut pending, &mut unique_ids, &mut sent, &mut errors).await?;
        }
        while let Some(result) = pending.try_join_next() {
            account_result(result, &mut unique_ids, &mut sent, &mut errors)?;
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

    while !pending.is_empty() {
        collect_one(&mut pending, &mut unique_ids, &mut sent, &mut errors).await?;
    }
    producer
        .flush(Duration::from_secs(10))
        .context("falló el vaciado del productor")?;
    if Instant::now() < deadline {
        sleep(deadline - Instant::now()).await;
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "total_enviados={sent} únicos_enviados={} errores={errors} tasa_real={:.0} ev/s",
        unique_ids.len(),
        sent as f64 / elapsed
    );
    if errors > 0 {
        bail!("el generador terminó con {errors} errores de envío");
    }
    Ok(())
}
