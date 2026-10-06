-- Roll back only before resuming the Kafka consumer. Once new rows have been
-- written to events, retain the identity-safe table and repair forward.
DROP TABLE IF EXISTS taq.events_identity_safe;

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

ATTACH TABLE taq.events_raw;
