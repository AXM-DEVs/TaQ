use futures_util::StreamExt;
use serde_json::Value;
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};
use taq_ingest::{
    NativeEvent, Source, SourceCursorStore, SourceError, SourceMessage, TimestampQuality,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tracing::{error, info};

const BLUESKY_JETSTREAM_URL: &str = "wss://jetstream.us-east.bsky.network/subscribe";

pub struct BlueskySource {
    url: String,
    user_agent: String,
    connect_timeout: std::time::Duration,
    cursor_store: Option<std::sync::Arc<dyn SourceCursorStore>>,
}

impl BlueskySource {
    pub fn new() -> Self {
        Self {
            url: BLUESKY_JETSTREAM_URL.to_owned(),
            user_agent: "TaQ/0.1".to_owned(),
            connect_timeout: std::time::Duration::from_secs(10),
            cursor_store: None,
        }
    }

    pub fn with_connection_settings(
        url: impl Into<String>,
        user_agent: impl Into<String>,
        connect_timeout: std::time::Duration,
    ) -> Self {
        Self {
            url: url.into(),
            user_agent: user_agent.into(),
            connect_timeout,
            cursor_store: None,
        }
    }

    pub fn with_cursor_store(mut self, store: std::sync::Arc<dyn SourceCursorStore>) -> Self {
        self.cursor_store = Some(store);
        self
    }
}

impl Default for BlueskySource {
    fn default() -> Self {
        Self::new()
    }
}

impl Source for BlueskySource {
    fn name(&self) -> &'static str {
        "bluesky"
    }

    fn run(
        &self,
        output: mpsc::Sender<SourceMessage>,
    ) -> impl Future<Output = Result<(), SourceError>> + Send {
        let name = self.name();
        let url = self.url.clone();
        let user_agent = self.user_agent.clone();
        let connect_timeout = self.connect_timeout;
        let cursor_store = self.cursor_store.clone();

        async move {
            info!("Iniciando conexión a Bluesky Jetstream (WSS)");
            let mut stream_url = url::Url::parse(&url).map_err(|error| SourceError::Permanent {
                source_name: name.to_owned(),
                message: format!("URL de Jetstream inválida: {error}"),
            })?;
            let last_cursor = match cursor_store.as_deref() {
                Some(store) => {
                    store
                        .load(name)
                        .await
                        .map_err(|message| SourceError::Connector {
                            source_name: name.to_string(),
                            message: format!("no se pudo cargar el cursor Jetstream: {message}"),
                        })?
                }
                None => None,
            };
            let resume_cursor = last_cursor.as_ref().and_then(|cursor| {
                cursor
                    .get("cursor")
                    .and_then(Value::as_u64)
                    .filter(|seq| *seq < 1_000_000_000_000_000)
                    .map(|seq| seq.to_string())
                    .or_else(|| {
                        cursor
                            .get("time_us")
                            .and_then(Value::as_i64)
                            .map(|time_us| time_us.saturating_sub(5_000_000).max(0).to_string())
                    })
            });
            if let Some(resume_cursor) = resume_cursor {
                let mut query = stream_url
                    .query_pairs()
                    .filter(|(key, _)| key != "cursor")
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect::<Vec<_>>();
                query.push(("cursor".to_owned(), resume_cursor));
                stream_url.query_pairs_mut().clear().extend_pairs(query);
            }
            let mut request =
                stream_url
                    .into_client_request()
                    .map_err(|error| SourceError::Permanent {
                        source_name: name.to_owned(),
                        message: format!("no se pudo preparar la conexión Jetstream: {error}"),
                    })?;
            let user_agent_header = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(
                &user_agent,
            )
            .map_err(|error| SourceError::Permanent {
                source_name: name.to_owned(),
                message: format!("User-Agent inválido: {error}"),
            })?;
            request.headers_mut().insert(
                tokio_tungstenite::tungstenite::http::header::USER_AGENT,
                user_agent_header,
            );
            let (mut ws_stream, _) =
                tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(request))
                    .await
                    .map_err(|_| SourceError::Connector {
                        source_name: name.to_string(),
                        message: "timeout al conectar con Jetstream".to_string(),
                    })?
                    .map_err(|_error| SourceError::Connector {
                        source_name: name.to_string(),
                        message: "falló la conexión WebSocket a Jetstream".to_string(),
                    })?;

            while let Some(msg_res) = ws_stream.next().await {
                match msg_res {
                    Ok(msg) => {
                        if !msg.is_text() {
                            continue;
                        }

                        let data = msg.into_text().unwrap_or_default();
                        let payload: Value = match serde_json::from_str(&data) {
                            Ok(val) => val,
                            Err(_) => {
                                let payload_hash =
                                    blake3::hash(data.as_bytes()).to_hex().to_string();
                                let native_id = format!("invalid-jetstream:{payload_hash}");
                                let already_seen=match cursor_store.as_deref() {
                                    Some(store)=>store.is_seen(name,&native_id).await.map_err(|message|SourceError::Connector {
                                        source_name:name.to_string(),message:format!("no se pudo consultar identidad Jetstream inválida: {message}"),
                                    })?,
                                    None=>false,
                                };
                                if !already_seen {
                                    output
                                        .send(SourceMessage::Rejected {
                                            native_id,
                                            reason: "invalid_jetstream_json".to_string(),
                                            payload_bytes: data.len(),
                                            payload_hash,
                                            cursor: None,
                                        })
                                        .await
                                        .map_err(|_| SourceError::QueueClosed)?;
                                }
                                continue;
                            }
                        };

                        // V2's compatibility endpoint keeps the V1 JSON envelope
                        // and adds a per-event sequence cursor.
                        let kind_name = payload["kind"].as_str().unwrap_or("unknown");
                        let kind = if kind_name == "commit" {
                            format!(
                                "commit:{}",
                                payload["commit"]["operation"].as_str().unwrap_or("unknown")
                            )
                        } else {
                            kind_name.to_owned()
                        };

                        // Jetstream time_us is microseconds since epoch
                        let time_us = payload["time_us"].as_i64();
                        let cursor = cursor_from_payload(&payload);
                        let source_time = time_us.map(|us| us / 1000);
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

                        let Some(native_id) = native_id_from_payload(&payload) else {
                            let payload_hash = blake3::hash(data.as_bytes()).to_hex().to_string();
                            let native_id = payload["cursor"]
                                .as_u64()
                                .map(|seq| format!("invalid-native-identity:{seq}"))
                                .unwrap_or_else(|| {
                                    format!("invalid-native-identity:{payload_hash}")
                                });
                            let already_seen = match cursor_store.as_deref() {
                                Some(store) => store.is_seen(name, &native_id).await.map_err(|message| SourceError::Connector {
                                    source_name: name.to_string(),
                                    message: format!("no se pudo consultar identidad Jetstream incompleta: {message}"),
                                })?,
                                None => false,
                            };
                            if !already_seen {
                                output
                                    .send(SourceMessage::Rejected {
                                        native_id,
                                        reason: "missing_jetstream_native_identity".to_string(),
                                        payload_bytes: data.len(),
                                        payload_hash,
                                        cursor,
                                    })
                                    .await
                                    .map_err(|_| SourceError::QueueClosed)?;
                            }
                            continue;
                        };

                        if let Some(store) = cursor_store.as_deref() {
                            let seen =
                                store.is_seen(name, &native_id).await.map_err(|message| {
                                    SourceError::Connector {
                                        source_name: name.to_string(),
                                        message: format!(
                                            "no se pudo consultar identidad Jetstream: {message}"
                                        ),
                                    }
                                })?;
                            if seen {
                                if let Some(cursor) = cursor {
                                    output
                                        .send(SourceMessage::Checkpoint(cursor))
                                        .await
                                        .map_err(|_| SourceError::QueueClosed)?;
                                }
                                continue;
                            }
                        }

                        let mut entity_ids = Vec::new();
                        if let Some(did) = payload["did"].as_str() {
                            entity_ids.push(format!("did:{}", did));
                        }
                        if let Some(commit) = payload.get("commit")
                            && let Some(collection) = commit["collection"].as_str()
                        {
                            entity_ids.push(format!("collection:{}", collection));
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
                        error!("falló la lectura del stream Jetstream");
                        return Err(SourceError::Connector {
                            source_name: name.to_string(),
                            message: "falló la lectura del stream Jetstream".to_string(),
                        });
                    }
                }
            }

            Ok(())
        }
    }
}

fn native_id_from_payload(payload: &Value) -> Option<String> {
    let did = payload["did"].as_str()?.trim();
    if did.is_empty() {
        return None;
    }
    match payload["kind"].as_str()? {
        "commit" => {
            let commit = payload.get("commit")?;
            let collection = commit["collection"].as_str()?.trim();
            let rkey = commit["rkey"].as_str()?.trim();
            let operation = commit["operation"].as_str()?.trim();
            let rev = commit["rev"].as_str()?.trim();
            if collection.is_empty() || rkey.is_empty() || operation.is_empty() || rev.is_empty() {
                return None;
            }
            Some(format!("{did}|{collection}|{rkey}|{operation}|{rev}"))
        }
        "identity" => Some(format!(
            "{did}|identity|{}",
            payload["identity"]["seq"].as_i64()?
        )),
        "account" => Some(format!(
            "{did}|account|{}",
            payload["account"]["seq"].as_i64()?
        )),
        _ => None,
    }
}

fn cursor_from_payload(payload: &Value) -> Option<Value> {
    let mut cursor = serde_json::Map::new();
    if let Some(seq) = payload.get("cursor").and_then(Value::as_u64) {
        cursor.insert("cursor".to_owned(), Value::from(seq));
    }
    if let Some(time_us) = payload.get("time_us").and_then(Value::as_i64) {
        cursor.insert("time_us".to_owned(), Value::from(time_us));
    }
    (!cursor.is_empty()).then_some(Value::Object(cursor))
}

#[cfg(test)]
mod tests {
    use super::native_id_from_payload;
    use serde_json::json;

    #[test]
    fn bluesky_id_includes_repository_collection_record_and_operation() {
        let create = json!({
            "kind": "commit",
            "did": "did:plc:alice",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "same-key",
                "rev": "3l123"
            }
        });
        let other_repository = json!({
            "kind": "commit",
            "did": "did:plc:bob",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "same-key",
                "rev": "3l123"
            }
        });
        let other_collection = json!({
            "kind": "commit",
            "did": "did:plc:alice",
            "commit": {
                "operation": "create",
                "collection": "app.bsky.graph.follow",
                "rkey": "same-key",
                "rev": "3l123"
            }
        });
        let update = json!({
            "kind": "commit",
            "did": "did:plc:alice",
            "commit": {
                "operation": "update",
                "collection": "app.bsky.feed.post",
                "rkey": "same-key",
                "rev": "3l124"
            }
        });

        let create_id = native_id_from_payload(&create).expect("complete create identity");
        assert_eq!(
            create_id,
            "did:plc:alice|app.bsky.feed.post|same-key|create|3l123"
        );
        assert_ne!(
            create_id,
            native_id_from_payload(&other_repository).unwrap()
        );
        assert_ne!(
            create_id,
            native_id_from_payload(&other_collection).unwrap()
        );
        assert_ne!(create_id, native_id_from_payload(&update).unwrap());
        assert!(native_id_from_payload(&json!({"did":"did:plc:alice"})).is_none());
    }
}
