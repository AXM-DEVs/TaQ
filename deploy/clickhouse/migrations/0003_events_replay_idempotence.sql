-- PHASE 1/2: prepare and verify. Run manually; this intentionally leaves the
-- Kafka table detached until the operator has checked the final SELECT.
-- Do not run this migration automatically on an existing volume.
-- See README.md in this directory for prerequisites, activation, and rollback.

DETACH TABLE taq.events_raw;
DROP VIEW taq.events_raw_to_events;

CREATE TABLE taq.events_replay_safe
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
ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(ingested_at_ms))
ORDER BY (ingested_at_ms, id);

INSERT INTO taq.events_replay_safe
SELECT * FROM taq.events;

-- Expected equality: source_keys = replacement_rows. Do not continue unless
-- this holds and sample payloads/timestamps match the old table.
SELECT
    (SELECT uniqExact(tuple(ingested_at_ms, id)) FROM taq.events) AS source_keys,
    (SELECT count() FROM taq.events_replay_safe FINAL) AS replacement_rows;
