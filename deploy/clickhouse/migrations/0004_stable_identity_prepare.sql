-- Phase 1: stop ingestd and contextd, then detach the Kafka consumer and build
-- a replacement deduplicated on stable event identity, independent of retries.
DETACH TABLE taq.events_raw;
DROP VIEW taq.events_raw_to_events;

CREATE TABLE taq.events_identity_safe
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

INSERT INTO taq.events_identity_safe
SELECT * FROM taq.events;

-- These must be equal. Also inspect sampled payloads, timestamps, and
-- provenance. Do not activate if an identity collision changed the latest row.
SELECT
    (SELECT uniqExact(id) FROM taq.events) AS distinct_source_ids,
    (SELECT count() FROM taq.events_identity_safe FINAL) AS replacement_rows;
