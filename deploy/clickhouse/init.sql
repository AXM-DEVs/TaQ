CREATE DATABASE IF NOT EXISTS taq;

CREATE TABLE IF NOT EXISTS taq.events_raw
(
    id String,
    source String,
    kind String,
    entity_ids Array(String),
    occurred_at_ms Int64,
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
    input_format_json_read_objects_as_strings = 1;

CREATE TABLE IF NOT EXISTS taq.events
(
    id String,
    source String,
    kind String,
    entity_ids Array(String),
    occurred_at_ms Int64,
    ingested_at_ms Int64,
    payload String,
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(ingested_at_ms))
ORDER BY (ingested_at_ms, id);

CREATE MATERIALIZED VIEW IF NOT EXISTS taq.events_raw_to_events
TO taq.events
AS
SELECT
    id,
    source,
    kind,
    entity_ids,
    occurred_at_ms,
    ingested_at_ms,
    payload
FROM taq.events_raw;
