mod dedup;
mod event;
mod pipeline;
mod source;

pub use taq_core::TimestampQuality;

pub use dedup::{Deduplicator, DeduplicatorConfig};
pub use event::{NativeEvent, normalize_event};
pub use pipeline::{
    DEAD_LETTER_SCHEMA_VERSION, DeadLetter, KafkaPipeline, KafkaPipelineConfig, PipelineStats,
    run_pipeline,
};
pub use source::{
    InvalidChannelCapacity, RetryPolicy, Source, SourceCursorStore, SourceError, SourceMessage,
    bounded_source_channel, is_permanent_http_status, run_source_with_backoff,
};
