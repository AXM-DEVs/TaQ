-- PHASE 2/2: run only after phase 1 validation succeeds.
-- The old MergeTree is retained as taq.events_pre_f2 for rollback/audit.
RENAME TABLE
    taq.events TO taq.events_pre_f2,
    taq.events_replay_safe TO taq.events;

ATTACH TABLE taq.events_raw;

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
