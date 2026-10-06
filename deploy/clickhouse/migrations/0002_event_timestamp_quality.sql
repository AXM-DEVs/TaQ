-- Apply after stopping taq-ingestd and before deploying schema_version 2 events.
-- The Kafka engine does not support column defaults. Legacy JSON messages omit
-- this field, so the materialized view maps the empty string to "unknown".
ALTER TABLE taq.events_raw
    ADD COLUMN IF NOT EXISTS occurred_at_quality String;

ALTER TABLE taq.events_raw
    MODIFY SETTING input_format_defaults_for_omitted_fields = 1;

ALTER TABLE taq.events
    ADD COLUMN IF NOT EXISTS occurred_at_quality String DEFAULT 'unknown';

DROP VIEW IF EXISTS taq.events_raw_to_events;

CREATE MATERIALIZED VIEW taq.events_raw_to_events
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
