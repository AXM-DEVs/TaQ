use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::Value;
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};
use taq_ingest::{
    NativeEvent, Source, SourceCursorStore, SourceError, SourceMessage, TimestampQuality,
    is_permanent_http_status,
};
use tokio::sync::mpsc;
use tracing::{error, info};

const WIKIMEDIA_SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

pub struct WikimediaSource {
    client: Client,
    url: String,
    cursor_store: Option<std::sync::Arc<dyn SourceCursorStore>>,
}

impl WikimediaSource {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            url: WIKIMEDIA_SSE_URL.to_owned(),
            cursor_store: None,
        }
    }

    pub fn with_http_settings(
        url: impl Into<String>,
        user_agent: &str,
        connect_timeout: std::time::Duration,
    ) -> Result<Self, reqwest::Error> {
        let client = Client::builder()
            .user_agent(user_agent)
            .connect_timeout(connect_timeout)
            .build()?;
        Ok(Self {
            client,
            url: url.into(),
            cursor_store: None,
        })
    }

    pub fn with_cursor_store(mut self, store: std::sync::Arc<dyn SourceCursorStore>) -> Self {
        self.cursor_store = Some(store);
        self
    }
}

impl Default for WikimediaSource {
    fn default() -> Self {
        Self::new()
    }
}

impl Source for WikimediaSource {
    fn name(&self) -> &'static str {
        "wikimedia"
    }

    fn run(
        &self,
        output: mpsc::Sender<SourceMessage>,
    ) -> impl Future<Output = Result<(), SourceError>> + Send {
        let client = self.client.clone();
        let url = self.url.clone();
        let name = self.name();
        let cursor_store = self.cursor_store.clone();

        async move {
            info!("Iniciando conexión a Wikimedia SSE");
            let last_cursor = match cursor_store.as_deref() {
                Some(store) => {
                    store
                        .load(name)
                        .await
                        .map_err(|message| SourceError::Connector {
                            source_name: name.to_string(),
                            message: format!("no se pudo cargar el cursor SSE: {message}"),
                        })?
                }
                None => None,
            };
            let mut request = client.get(url);
            if let Some(cursor) = last_cursor.as_ref().and_then(Value::as_str) {
                request = request.header("Last-Event-ID", cursor);
            }
            let response = request.send().await.map_err(|e| SourceError::Connector {
                source_name: name.to_string(),
                message: if e.is_timeout() {
                    "timeout al conectar al stream SSE".to_string()
                } else {
                    "falló la conexión HTTP al stream SSE".to_string()
                },
            })?;

            if !response.status().is_success() {
                let status = response.status();
                let message = format!("Status no exitoso: {status}");
                if is_permanent_http_status(status.as_u16()) {
                    return Err(SourceError::Permanent {
                        source_name: name.to_string(),
                        message,
                    });
                }
                return Err(SourceError::Connector {
                    source_name: name.to_string(),
                    message,
                });
            }

            let mut stream = response.bytes_stream().eventsource();

            while let Some(event_res) = stream.next().await {
                match event_res {
                    Ok(event) => {
                        if event.event != "message" {
                            continue;
                        }

                        let payload: Value = match serde_json::from_str(&event.data) {
                            Ok(val) => val,
                            Err(_) => {
                                let payload_hash =
                                    blake3::hash(event.data.as_bytes()).to_hex().to_string();
                                let native_id = if event.id.is_empty() {
                                    format!("invalid-sse-hash:{payload_hash}")
                                } else {
                                    format!("invalid-sse:{}", event.id)
                                };
                                let already_seen=match cursor_store.as_deref() {
                                    Some(store)=>store.is_seen(name,&native_id).await.map_err(|message|SourceError::Connector {
                                        source_name:name.to_string(),message:format!("no se pudo consultar identidad SSE inválida: {message}"),
                                    })?,
                                    None=>false,
                                };
                                if !already_seen {
                                    output
                                        .send(SourceMessage::Rejected {
                                            native_id,
                                            reason: "invalid_wikimedia_json".to_string(),
                                            payload_bytes: event.data.len(),
                                            payload_hash,
                                            cursor: if event.id.is_empty() {
                                                None
                                            } else {
                                                cursor_store
                                                    .as_ref()
                                                    .map(|_| Value::String(event.id.clone()))
                                            },
                                        })
                                        .await
                                        .map_err(|_| SourceError::QueueClosed)?;
                                }
                                continue;
                            }
                        };

                        // Wikimedia publishes synthetic canary events into public
                        // streams. They are operational probes, not source content.
                        if payload["meta"]["domain"].as_str() == Some("canary") {
                            if !event.id.is_empty() && cursor_store.is_some() {
                                output
                                    .send(SourceMessage::Checkpoint(Value::String(event.id)))
                                    .await
                                    .map_err(|_| SourceError::QueueClosed)?;
                            }
                            continue;
                        }

                        let native_id = payload["meta"]["id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();

                        if native_id.is_empty() {
                            if !event.id.is_empty() {
                                let payload_hash =
                                    blake3::hash(event.data.as_bytes()).to_hex().to_string();
                                output
                                    .send(SourceMessage::Rejected {
                                        native_id: format!("missing-native-id:{}", event.id),
                                        reason: "wikimedia_missing_native_id".to_string(),
                                        payload_bytes: event.data.len(),
                                        payload_hash,
                                        cursor: cursor_store
                                            .as_ref()
                                            .map(|_| Value::String(event.id.clone())),
                                    })
                                    .await
                                    .map_err(|_| SourceError::QueueClosed)?;
                            }
                            continue;
                        }

                        let kind = payload["type"].as_str().unwrap_or("unknown").to_string();
                        let source_time = payload["timestamp"]
                            .as_i64()
                            .and_then(|seconds| seconds.checked_mul(1000));
                        let (occurred_at_ms, occurred_at_quality) = match source_time {
                            Some(timestamp) => (timestamp, TimestampQuality::Source),
                            None => (
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis() as i64,
                                TimestampQuality::CaptureTime,
                            ),
                        };

                        let mut entity_ids = Vec::new();
                        if let Some(wiki) = payload["wiki"].as_str() {
                            entity_ids.push(format!("wiki:{}", wiki));
                        }
                        if let Some(user) = payload["user"].as_str() {
                            entity_ids.push(format!("user:{}", user));
                        }
                        if let Some(bot) = payload["bot"].as_bool()
                            && bot
                        {
                            entity_ids.push("bot:true".to_string());
                        }

                        let cursor =
                            (!event.id.is_empty()).then(|| Value::String(event.id.clone()));
                        if let Some(store) = cursor_store.as_deref() {
                            let seen =
                                store.is_seen(name, &native_id).await.map_err(|message| {
                                    SourceError::Connector {
                                        source_name: name.to_string(),
                                        message: format!(
                                            "no se pudo consultar identidad SSE: {message}"
                                        ),
                                    }
                                })?;
                            if seen {
                                if let Some(cursor) = cursor.clone() {
                                    output
                                        .send(SourceMessage::Checkpoint(cursor))
                                        .await
                                        .map_err(|_| SourceError::QueueClosed)?;
                                }
                                continue;
                            }
                        }
                        let native_event = NativeEvent {
                            native_id,
                            kind,
                            entity_ids,
                            occurred_at_ms,
                            occurred_at_quality,
                            payload,
                        };

                        if output
                            .send(SourceMessage::Event {
                                event: native_event,
                                cursor,
                            })
                            .await
                            .is_err()
                        {
                            return Err(SourceError::QueueClosed);
                        }
                    }
                    Err(_) => {
                        error!("falló la lectura del stream SSE");
                        return Err(SourceError::Connector {
                            source_name: name.to_string(),
                            message: "falló la lectura del stream SSE".to_string(),
                        });
                    }
                }
            }

            Ok(())
        }
    }
}
