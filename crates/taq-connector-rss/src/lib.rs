use reqwest::Client;
use rss::Channel;
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use taq_ingest::{
    NativeEvent, RetryPolicy, Source, SourceCursorStore, SourceError, SourceMessage,
    TimestampQuality, is_permanent_http_status,
};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn};

const RSS_SEEN_ID_LIMIT: usize = 10_000;
const RSS_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone, Default)]
struct FeedValidators {
    etag: Option<String>,
    last_modified: Option<String>,
}

pub struct RssSource {
    url: String,
    poll_interval: Duration,
    client: Client,
    validators: Arc<RwLock<FeedValidators>>,
    cursor_store: Option<Arc<dyn SourceCursorStore>>,
    retry_policy: RetryPolicy,
}

impl RssSource {
    pub fn new(url: impl Into<String>, poll_interval: Duration) -> Self {
        Self {
            url: url.into(),
            poll_interval,
            client: Client::new(),
            validators: Arc::new(RwLock::new(FeedValidators::default())),
            cursor_store: None,
            retry_policy: RetryPolicy::default(),
        }
    }

    pub fn with_http_settings(
        url: impl Into<String>,
        poll_interval: Duration,
        user_agent: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        let client = Client::builder()
            .user_agent(user_agent)
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()?;
        Ok(Self {
            url: url.into(),
            poll_interval,
            client,
            validators: Arc::new(RwLock::new(FeedValidators::default())),
            cursor_store: None,
            retry_policy: RetryPolicy::default(),
        })
    }

    pub fn with_cursor_store(mut self, store: Arc<dyn SourceCursorStore>) -> Self {
        self.cursor_store = Some(store);
        self
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }
}

impl Source for RssSource {
    fn name(&self) -> &'static str {
        "rss"
    }

    fn run(
        &self,
        output: mpsc::Sender<SourceMessage>,
    ) -> impl Future<Output = Result<(), SourceError>> + Send {
        let url = self.url.clone();
        let poll_interval = self.poll_interval;
        let client = self.client.clone();
        let validators = self.validators.clone();
        let cursor_store = self.cursor_store.clone();
        let retry_policy = self.retry_policy;
        async move {
            info!("Iniciando polling de feed RSS configurado");
            let mut retry_attempt = 0_u32;

            loop {
                match poll_feed(&client, &url, &output, &validators, cursor_store.as_deref()).await
                {
                    Ok(()) => {
                        retry_attempt = 0;
                        tokio::time::sleep(poll_interval).await;
                    }
                    Err(error @ (SourceError::QueueClosed | SourceError::Permanent { .. })) => {
                        return Err(error);
                    }
                    Err(error) => {
                        let delay = retry_policy.delay_for_attempt(retry_attempt);
                        warn!(
                            source = "rss",
                            %error,
                            retry_attempt = retry_attempt + 1,
                            retry_in_ms = delay.as_millis(),
                            "falló la consulta RSS; se reintentará"
                        );
                        tokio::time::sleep(delay).await;
                        retry_attempt = retry_attempt.saturating_add(1);
                    }
                }
            }
        }
    }
}

async fn poll_feed(
    client: &Client,
    url: &str,
    output: &mpsc::Sender<SourceMessage>,
    validators: &RwLock<FeedValidators>,
    cursor_store: Option<&dyn SourceCursorStore>,
) -> Result<(), SourceError> {
    let stored_cursor = match cursor_store {
        Some(store) => store
            .load("rss")
            .await
            .map_err(|message| SourceError::Connector {
                source_name: "rss".to_string(),
                message: format!("no se pudo cargar el cursor RSS: {message}"),
            })?,
        None => None,
    }
    .filter(|cursor| cursor.get("feed_url").and_then(serde_json::Value::as_str) == Some(url));
    let current = match stored_cursor.as_ref() {
        Some(cursor) => feed_validators_from_cursor(cursor),
        None => validators.read().await.clone(),
    };
    let mut seen_ids: Vec<String> = stored_cursor
        .as_ref()
        .and_then(|cursor| cursor.get("seen_ids"))
        .and_then(serde_json::Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let mut request = client.get(url);
    if let Some(etag) = current.etag.as_deref() {
        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    if let Some(last_modified) = current.last_modified.as_deref() {
        request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
    }
    let response = request
        .send()
        .await
        .map_err(|error| SourceError::Connector {
            source_name: "rss".to_string(),
            message: if error.is_timeout() {
                "timeout HTTP consultando feed RSS".to_string()
            } else if error.is_connect() {
                "falló la conexión HTTP del feed RSS".to_string()
            } else {
                "error HTTP consultando feed RSS".to_string()
            },
        })?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(());
    }
    if !status.is_success() {
        let message = format!("Respuesta HTTP no exitosa: {status}");
        if is_permanent_http_status(status.as_u16()) {
            return Err(SourceError::Permanent {
                source_name: "rss".to_string(),
                message,
            });
        }
        return Err(SourceError::Connector {
            source_name: "rss".to_string(),
            message,
        });
    }

    let response_validators = FeedValidators {
        etag: response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        last_modified: response
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    };

    if response
        .content_length()
        .is_some_and(|length| length > RSS_MAX_RESPONSE_BYTES as u64)
    {
        return Err(SourceError::Permanent {
            source_name: "rss".to_string(),
            message: format!("el feed excede el límite de {RSS_MAX_RESPONSE_BYTES} bytes"),
        });
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|error| SourceError::Connector {
            source_name: "rss".to_string(),
            message: if error.is_timeout() {
                "timeout leyendo cuerpo del feed RSS".to_string()
            } else {
                "falló la lectura del cuerpo del feed RSS".to_string()
            },
        })?;
    if bytes.len() > RSS_MAX_RESPONSE_BYTES {
        return Err(SourceError::Permanent {
            source_name: "rss".to_string(),
            message: format!("el feed excede el límite de {RSS_MAX_RESPONSE_BYTES} bytes"),
        });
    }
    let channel = match Channel::read_from(&bytes[..]) {
        Ok(channel) => channel,
        Err(_) => {
            let payload_hash = blake3::hash(&bytes).to_hex().to_string();
            let native_id = format!("invalid-feed:{payload_hash}");
            let cursor = cursor_store.map(|_| feed_cursor(url, &response_validators, &[]));
            output
                .send(SourceMessage::Rejected {
                    native_id,
                    reason: "invalid_rss_xml".to_string(),
                    payload_bytes: bytes.len(),
                    payload_hash,
                    cursor,
                })
                .await
                .map_err(|_| SourceError::QueueClosed)?;
            return Ok(());
        }
    };

    for item in channel.items() {
        let native_id = rss_native_id(
            item.guid().map(|guid| guid.value()),
            item.link(),
            item.title(),
            item.pub_date(),
            item.description(),
        );
        let Some(native_id) = native_id else {
            continue;
        };
        let persisted_seen = match cursor_store {
            Some(store) => store.is_seen("rss", &native_id).await.map_err(|message| {
                SourceError::Connector {
                    source_name: "rss".to_string(),
                    message: format!("no se pudo consultar identidad RSS: {message}"),
                }
            })?,
            None => false,
        };
        if persisted_seen || seen_ids.iter().any(|seen| seen == &native_id) {
            continue;
        }
        seen_ids.push(native_id.clone());
        trim_seen_ids(&mut seen_ids);

        let capture_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let (occurred_at_ms, occurred_at_quality) =
            parse_occurred_at(item.pub_date(), capture_time_ms);

        let mut entity_ids = Vec::new();
        if let Some(author) = item.author() {
            entity_ids.push(format!("author:{}", author));
        }
        for category in item.categories() {
            entity_ids.push(format!("category:{}", category.name()));
        }

        let payload = json!({
            "title": item.title(),
            "description": item.description(),
            "link": item.link(),
            "pub_date": item.pub_date(),
        });
        let event = NativeEvent {
            native_id,
            kind: "rss_item".to_string(),
            entity_ids,
            occurred_at_ms,
            occurred_at_quality,
            payload,
        };

        let cursor = cursor_store.map(|_| feed_cursor(url, &current, &seen_ids));
        output
            .send(SourceMessage::Event { event, cursor })
            .await
            .map_err(|_| SourceError::QueueClosed)?;
    }

    if cursor_store.is_some() {
        let checkpoint = feed_cursor(url, &response_validators, &seen_ids);
        output
            .send(SourceMessage::Checkpoint(checkpoint))
            .await
            .map_err(|_| SourceError::QueueClosed)?;
    }
    *validators.write().await = response_validators;

    Ok(())
}

fn feed_validators_from_cursor(cursor: &serde_json::Value) -> FeedValidators {
    FeedValidators {
        etag: cursor
            .get("etag")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        last_modified: cursor
            .get("last_modified")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
    }
}

fn feed_cursor(url: &str, validators: &FeedValidators, seen_ids: &[String]) -> serde_json::Value {
    json!({
        "feed_url": url,
        "etag": validators.etag,
        "last_modified": validators.last_modified,
        "seen_ids": seen_ids,
    })
}

fn trim_seen_ids(seen_ids: &mut Vec<String>) {
    if seen_ids.len() > RSS_SEEN_ID_LIMIT {
        seen_ids.drain(..seen_ids.len() - RSS_SEEN_ID_LIMIT);
    }
}

fn parse_occurred_at(pub_date: Option<&str>, capture_time_ms: i64) -> (i64, TimestampQuality) {
    let timestamp = pub_date
        .and_then(|date| {
            chrono::DateTime::parse_from_rfc2822(date)
                .or_else(|_| chrono::DateTime::parse_from_rfc3339(date))
                .ok()
        })
        .map(|date| date.timestamp_millis())
        .filter(|timestamp| *timestamp <= capture_time_ms);

    match timestamp {
        Some(timestamp) => (timestamp, TimestampQuality::Source),
        None => (capture_time_ms, TimestampQuality::CaptureTime),
    }
}

fn rss_native_id(
    guid: Option<&str>,
    link: Option<&str>,
    title: Option<&str>,
    pub_date: Option<&str>,
    description: Option<&str>,
) -> Option<String> {
    if let Some(guid) = guid.filter(|value| !value.trim().is_empty()) {
        return Some(guid.to_owned());
    }
    if let Some(link) = link.filter(|value| !value.trim().is_empty()) {
        return Some(link.to_owned());
    }
    if [title, pub_date, description]
        .into_iter()
        .all(|value| value.is_none_or(|value| value.trim().is_empty()))
    {
        return None;
    }

    let content = serde_json::to_vec(&(title, pub_date, description)).ok()?;
    Some(format!("content:{}", blake3::hash(&content).to_hex()))
}

#[cfg(test)]
mod tests {
    use super::{parse_occurred_at, rss_native_id};
    use taq_core::TimestampQuality;

    #[test]
    fn parses_rfc2822_publication_time_as_source_time() {
        let (timestamp, quality) =
            parse_occurred_at(Some("Wed, 02 Oct 2002 13:00:00 GMT"), 1_700_000_000_000);

        assert_eq!(timestamp, 1_033_563_600_000);
        assert_eq!(quality, TimestampQuality::Source);
    }

    #[test]
    fn parses_rfc3339_publication_time_with_offset_as_utc_epoch() {
        let (timestamp, quality) =
            parse_occurred_at(Some("2002-10-02T14:00:00+01:00"), 1_700_000_000_000);

        assert_eq!(timestamp, 1_033_563_600_000);
        assert_eq!(quality, TimestampQuality::Source);
    }

    #[test]
    fn falls_back_for_missing_invalid_timezone_free_or_future_dates() {
        let fallback = 1_700_000_000_000;
        for date in [
            None,
            Some("not a date"),
            Some("Wed, 02 Oct 2002 13:00:00"),
            Some("Wed, 02 Oct 2099 13:00:00 GMT"),
        ] {
            let (timestamp, quality) = parse_occurred_at(date, fallback);
            assert_eq!(timestamp, fallback, "date {date:?}");
            assert_eq!(quality, TimestampQuality::CaptureTime, "date {date:?}");
        }
    }

    #[test]
    fn rss_id_prefers_guid_then_link_then_stable_content_hash() {
        let guid = rss_native_id(
            Some("guid-1"),
            Some("https://example.com/item"),
            Some("title"),
            Some("date"),
            Some("description"),
        )
        .expect("guid identifies item");
        let link = rss_native_id(None, Some("https://example.com/item"), None, None, None)
            .expect("link identifies item");
        let content = rss_native_id(None, None, Some("title"), Some("date"), Some("description"))
            .expect("content identifies item");
        let same_content =
            rss_native_id(None, None, Some("title"), Some("date"), Some("description"))
                .expect("content hash is repeatable");
        let other_content = rss_native_id(None, None, Some("title"), Some("date"), Some("other"))
            .expect("different content identifies another item");

        assert_eq!(guid, "guid-1");
        assert_eq!(link, "https://example.com/item");
        assert!(content.starts_with("content:"));
        assert_eq!(content, same_content);
        assert_ne!(content, other_content);
        assert!(rss_native_id(None, None, None, None, None).is_none());
    }
}
