CREATE DATABASE IF NOT EXISTS taq;

CREATE TABLE IF NOT EXISTS taq.events_raw
(
    schema_version UInt16,
    id String,
    source String,
    kind String,
    entity_ids Array(String),
    occurred_at_ms Int64,
    occurred_at_quality String,
    ingested_at_ms Int64,
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'events.raw',
    kafka_group_name = 'taq-clickhouse-events-raw',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 6,
    kafka_poll_timeout_ms = 100,
    kafka_flush_interval_ms = 500,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'dead_letter_queue',
    input_format_defaults_for_omitted_fields = 1,
    input_format_json_read_objects_as_strings = 1;

CREATE TABLE IF NOT EXISTS taq.events
(
    schema_version UInt16 DEFAULT 1,
    id String,
    source String,
    kind String,
    entity_ids Array(String),
    occurred_at_ms Int64,
    occurred_at_quality String DEFAULT 'unknown',
    ingested_at_ms Int64,
    broker_appended_at DateTime64(3),
    payload String,
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at_ms)
PARTITION BY cityHash64(id) % 64
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS taq.events_raw_to_events
TO taq.events
AS
SELECT
    if(schema_version = 0, 1, schema_version) AS schema_version,
    id,
    source,
    kind,
    entity_ids,
    occurred_at_ms,
    if(occurred_at_quality = '', 'unknown', occurred_at_quality) AS occurred_at_quality,
    ingested_at_ms,
    ifNull(_timestamp_ms, fromUnixTimestamp64Milli(ingested_at_ms)) AS broker_appended_at,
    payload
FROM taq.events_raw;
