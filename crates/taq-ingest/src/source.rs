use rand::random_range;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

use crate::NativeEvent;
use taq_observability::global_delivery_metrics;

#[derive(Debug, Clone)]
pub enum SourceMessage {
    Event {
        event: NativeEvent,
        cursor: Option<Value>,
    },
    Checkpoint(Value),
    Rejected {
        native_id: String,
        reason: String,
        payload_bytes: usize,
        payload_hash: String,
        cursor: Option<Value>,
    },
}

pub trait SourceCursorStore: Send + Sync {
    fn load<'a>(
        &'a self,
        source: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, String>> + Send + 'a>>;

    fn save<'a>(
        &'a self,
        source: &'a str,
        cursor: &'a Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

    fn is_seen<'a>(
        &'a self,
        source: &'a str,
        native_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

    fn acknowledge<'a>(
        &'a self,
        source: &'a str,
        native_id: &'a str,
        cursor: Option<&'a Value>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

/// HTTP 4xx responses are configuration/data errors except for statuses whose
/// semantics explicitly invite a retry.
pub fn is_permanent_http_status(status: u16) -> bool {
    (400..500).contains(&status) && !matches!(status, 408 | 425 | 429)
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("conector {source_name} falló: {message}")]
    Connector {
        source_name: String,
        message: String,
    },
    #[error("conector {source_name} requiere intervención: {message}")]
    Permanent {
        source_name: String,
        message: String,
    },
    #[error("cola de ingesta cerrada")]
    QueueClosed,
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    initial_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    pub fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        let max_delay = max_delay.max(Duration::from_millis(1));
        Self {
            initial_delay: initial_delay.max(Duration::from_millis(1)).min(max_delay),
            max_delay,
        }
    }

    pub fn delay_for_attempt(self, attempt: u32) -> Duration {
        let multiplier = 1_u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX);
        let base = self
            .initial_delay
            .saturating_mul(multiplier)
            .min(self.max_delay);
        let jitter_percent = random_range(80_u64..=120_u64);
        base.mul_f64(jitter_percent as f64 / 100.0)
            .min(self.max_delay)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(60))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("la capacidad de la cola de ingesta debe ser mayor que cero")]
pub struct InvalidChannelCapacity;

pub fn bounded_source_channel(
    capacity: usize,
) -> Result<(mpsc::Sender<SourceMessage>, mpsc::Receiver<SourceMessage>), InvalidChannelCapacity> {
    if capacity == 0 {
        return Err(InvalidChannelCapacity);
    }
    Ok(mpsc::channel(capacity))
}

pub trait Source: Send + Sync {
    fn name(&self) -> &'static str;

    fn run(
        &self,
        output: mpsc::Sender<SourceMessage>,
    ) -> impl Future<Output = Result<(), SourceError>> + Send;
}

/// Runs a source continuously, retrying connector failures with capped
/// exponential backoff. A closed downstream queue is terminal.
pub async fn run_source_with_backoff<S: Source>(
    source: &S,
    output: mpsc::Sender<SourceMessage>,
    retry_policy: RetryPolicy,
) -> Result<(), SourceError> {
    let mut attempt = 0;
    loop {
        let started_at = tokio::time::Instant::now();
        let result = source.run(output.clone()).await;
        if started_at.elapsed() >= Duration::from_secs(30) {
            attempt = 0;
        }

        match result {
            Ok(()) => warn!(source = source.name(), "la fuente terminó; se reintentará"),
            Err(error @ (SourceError::QueueClosed | SourceError::Permanent { .. })) => {
                return Err(error);
            }
            Err(error) => warn!(
                source = source.name(),
                %error,
                retry_attempt = attempt + 1,
                "falló el conector; se reintentará"
            ),
        }

        let delay = retry_policy.delay_for_attempt(attempt);
        global_delivery_metrics().record_source_retry_for(source.name());
        warn!(
            source = source.name(),
            retry_in_ms = delay.as_millis(),
            "esperando antes de reintentar"
        );
        tokio::time::sleep(delay).await;
        attempt = attempt.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        InvalidChannelCapacity, RetryPolicy, Source, SourceError, bounded_source_channel,
        is_permanent_http_status, run_source_with_backoff,
    };
    use crate::{NativeEvent, SourceMessage, TimestampQuality};
    use serde_json::json;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc;

    struct FlakySource {
        attempts: AtomicUsize,
    }

    impl Source for FlakySource {
        fn name(&self) -> &'static str {
            "flaky"
        }

        fn run(
            &self,
            output: mpsc::Sender<SourceMessage>,
        ) -> impl Future<Output = Result<(), SourceError>> + Send {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    return Err(SourceError::Connector {
                        source_name: "flaky".into(),
                        message: "temporary outage".into(),
                    });
                }

                output
                    .send(SourceMessage::Event {
                        event: NativeEvent {
                            native_id: "event-1".into(),
                            kind: "mention".into(),
                            entity_ids: Vec::new(),
                            occurred_at_ms: 1,
                            occurred_at_quality: TimestampQuality::Source,
                            payload: json!({}),
                        },
                        cursor: None,
                    })
                    .await
                    .map_err(|_| SourceError::QueueClosed)?;
                Ok(())
            }
        }
    }

    struct PermanentFailure(AtomicUsize);

    impl Source for PermanentFailure {
        fn name(&self) -> &'static str {
            "invalid-config"
        }

        fn run(
            &self,
            _output: mpsc::Sender<SourceMessage>,
        ) -> impl Future<Output = Result<(), SourceError>> + Send {
            self.0.fetch_add(1, Ordering::SeqCst);
            async {
                Err(SourceError::Permanent {
                    source_name: "invalid-config".into(),
                    message: "invalid configuration".into(),
                })
            }
        }
    }

    #[tokio::test]
    async fn bounded_channel_applies_backpressure_at_capacity() {
        let (sender, mut receiver) = bounded_source_channel(1).expect("channel is valid");
        let event = NativeEvent {
            native_id: "event-1".into(),
            kind: "mention".into(),
            entity_ids: Vec::new(),
            occurred_at_ms: 1,
            occurred_at_quality: TimestampQuality::Source,
            payload: json!({}),
        };

        sender
            .try_send(SourceMessage::Event {
                event: event.clone(),
                cursor: None,
            })
            .expect("first item fits");
        assert!(
            sender
                .try_send(SourceMessage::Event {
                    event,
                    cursor: None,
                })
                .is_err()
        );
        receiver.recv().await.expect("queued event is available");
    }

    #[test]
    fn zero_capacity_is_rejected() {
        let result = bounded_source_channel(0);
        assert!(matches!(result, Err(InvalidChannelCapacity)));
    }

    #[tokio::test]
    async fn transient_source_error_is_retried_and_queue_close_stops_retrying() {
        let source = FlakySource {
            attempts: AtomicUsize::new(0),
        };
        let (sender, receiver) = bounded_source_channel(1).expect("channel is valid");
        let retry_task = tokio::spawn(async move {
            run_source_with_backoff(&source, sender, RetryPolicy::default()).await
        });
        let mut receiver = receiver;

        let message = receiver.recv().await.expect("retry emits an event");
        assert!(
            matches!(message, SourceMessage::Event { event, .. } if event.native_id == "event-1")
        );
        drop(receiver);

        assert!(matches!(
            retry_task.await.unwrap(),
            Err(SourceError::QueueClosed)
        ));
    }

    #[tokio::test]
    async fn permanent_source_error_is_not_retried() {
        let source = PermanentFailure(AtomicUsize::new(0));
        let (sender, _receiver) = bounded_source_channel(1).expect("channel is valid");

        let result = run_source_with_backoff(&source, sender, RetryPolicy::default()).await;

        assert!(matches!(result, Err(SourceError::Permanent { .. })));
        assert_eq!(source.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retry_delay_applies_jitter_and_respects_the_cap() {
        let policy = RetryPolicy::new(Duration::from_secs(10), Duration::from_secs(12));

        for _ in 0..100 {
            let first = policy.delay_for_attempt(0);
            assert!((Duration::from_secs(8)..=Duration::from_secs(12)).contains(&first));
            assert!(policy.delay_for_attempt(20) <= Duration::from_secs(12));
        }
    }

    #[test]
    fn client_errors_are_terminal_except_transient_http_codes() {
        for status in [400, 401, 403, 404, 422] {
            assert!(is_permanent_http_status(status));
        }
        for status in [408, 425, 429, 500, 503] {
            assert!(!is_permanent_http_status(status));
        }
    }
}
