use anyhow::{Context, Result, bail};
use std::{env, time::Duration};

pub struct Settings {
    pub kafka_broker: String,
    pub database_url: String,
    pub wikimedia_url: String,
    pub bluesky_url: String,
    pub rss_url: String,
    pub rss_poll_interval: Duration,
    pub user_agent: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub source_queue_capacity: usize,
    pub retry_initial_delay: Duration,
    pub retry_max_delay: Duration,
    pub wikimedia_events_per_second: u32,
    pub bluesky_events_per_second: u32,
    pub rss_events_per_second: u32,
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let retry_initial_secs = positive_u64("TAQ_RETRY_INITIAL_SECS", 1)?;
        let retry_max_secs = positive_u64("TAQ_RETRY_MAX_SECS", 60)?;
        if retry_max_secs < retry_initial_secs {
            bail!("TAQ_RETRY_MAX_SECS debe ser mayor o igual a TAQ_RETRY_INITIAL_SECS");
        }

        let settings = Self {
            kafka_broker: string("TAQ_KAFKA_BROKER", "localhost:19092"),
            database_url: string(
                "TAQ_DATABASE_URL",
                "postgres://taq:taq_dev@localhost:5432/taq",
            ),
            wikimedia_url: string(
                "TAQ_WIKIMEDIA_URL",
                "https://stream.wikimedia.org/v2/stream/recentchange",
            ),
            bluesky_url: string(
                "TAQ_BLUESKY_JETSTREAM_URL",
                "wss://jetstream1.us-east.bsky.network/subscribe",
            ),
            rss_url: string("TAQ_RSS_FEED_URL", "https://feeds.bbci.co.uk/news/rss.xml"),
            rss_poll_interval: Duration::from_secs(positive_u64("TAQ_RSS_POLL_INTERVAL_SECS", 60)?),
            user_agent: string("TAQ_SOURCE_USER_AGENT", "TaQ/0.1"),
            connect_timeout: Duration::from_secs(positive_u64(
                "TAQ_SOURCE_CONNECT_TIMEOUT_SECS",
                10,
            )?),
            request_timeout: Duration::from_secs(positive_u64(
                "TAQ_SOURCE_REQUEST_TIMEOUT_SECS",
                30,
            )?),
            source_queue_capacity: positive_usize("TAQ_SOURCE_QUEUE_CAPACITY", 1_000)?,
            retry_initial_delay: Duration::from_secs(retry_initial_secs),
            retry_max_delay: Duration::from_secs(retry_max_secs),
            wikimedia_events_per_second: positive_u32("TAQ_WIKIMEDIA_EVENTS_PER_SEC", 5)?,
            bluesky_events_per_second: positive_u32("TAQ_BLUESKY_EVENTS_PER_SEC", 5)?,
            rss_events_per_second: positive_u32("TAQ_RSS_EVENTS_PER_SEC", 5)?,
        };

        for (name, endpoint) in [
            ("TAQ_WIKIMEDIA_URL", settings.wikimedia_url.as_str()),
            ("TAQ_RSS_FEED_URL", settings.rss_url.as_str()),
        ] {
            let parsed = reqwest::Url::parse(endpoint)
                .with_context(|| format!("{name} debe ser una URL absoluta válida"))?;
            if !matches!(parsed.scheme(), "http" | "https") {
                bail!("{name} debe usar HTTP o HTTPS");
            }
        }
        let bluesky = reqwest::Url::parse(&settings.bluesky_url)
            .context("TAQ_BLUESKY_JETSTREAM_URL debe ser una URL absoluta válida")?;
        if !matches!(bluesky.scheme(), "ws" | "wss") {
            bail!("TAQ_BLUESKY_JETSTREAM_URL debe usar WS o WSS");
        }
        if settings.user_agent.trim().is_empty() {
            bail!("TAQ_SOURCE_USER_AGENT no puede estar vacío");
        }
        if settings.user_agent.len() > 256
            || settings.user_agent.contains('\r')
            || settings.user_agent.contains('\n')
        {
            bail!("TAQ_SOURCE_USER_AGENT excede 256 bytes o contiene saltos de línea");
        }
        if settings.source_queue_capacity > 50_000 {
            bail!("TAQ_SOURCE_QUEUE_CAPACITY no puede exceder 50000");
        }
        if settings.connect_timeout > Duration::from_secs(300)
            || settings.request_timeout > Duration::from_secs(300)
            || settings.rss_poll_interval > Duration::from_secs(86_400)
            || settings.retry_max_delay > Duration::from_secs(86_400)
        {
            bail!("los timeouts y el intervalo/retry RSS exceden el máximo operativo configurado");
        }
        for (name, limit) in [
            (
                "TAQ_WIKIMEDIA_EVENTS_PER_SEC",
                settings.wikimedia_events_per_second,
            ),
            (
                "TAQ_BLUESKY_EVENTS_PER_SEC",
                settings.bluesky_events_per_second,
            ),
            ("TAQ_RSS_EVENTS_PER_SEC", settings.rss_events_per_second),
        ] {
            if limit > 1_000 {
                bail!("{name} no puede exceder 1000 eventos/s");
            }
        }
        if settings.kafka_broker.trim().is_empty() {
            bail!("TAQ_KAFKA_BROKER no puede estar vacío");
        }
        let database_url = reqwest::Url::parse(&settings.database_url)
            .context("TAQ_DATABASE_URL debe ser una URL válida")?;
        if database_url.scheme() != "postgres" && database_url.scheme() != "postgresql" {
            bail!("TAQ_DATABASE_URL debe usar postgres o postgresql");
        }

        Ok(settings)
    }
}

fn string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn positive_u64(key: &str, default: u64) -> Result<u64> {
    let value = env::var(key)
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{key} debe ser un entero positivo"))?
        .unwrap_or(default);
    if value == 0 {
        bail!("{key} debe ser mayor que cero");
    }
    Ok(value)
}

fn positive_usize(key: &str, default: usize) -> Result<usize> {
    let value = env::var(key)
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()
        .with_context(|| format!("{key} debe ser un entero positivo"))?
        .unwrap_or(default);
    if value == 0 {
        bail!("{key} debe ser mayor que cero");
    }
    Ok(value)
}

fn positive_u32(key: &str, default: u32) -> Result<u32> {
    let value = env::var(key)
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()
        .with_context(|| format!("{key} debe ser un entero positivo"))?
        .unwrap_or(default);
    if value == 0 {
        bail!("{key} debe ser mayor que cero");
    }
    Ok(value)
}
