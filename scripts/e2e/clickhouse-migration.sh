#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose --env-file .env.example -f deploy/docker-compose.yml)
clickhouse() {
  "${compose[@]}" exec -T clickhouse clickhouse-client \
    --user=default --password=taq_dev "$@"
}

# Convert this disposable, fresh ReplacingMergeTree table into the legacy
# MergeTree layout so the real staged migration is exercised in CI.
clickhouse --multiquery --query "RENAME TABLE taq.events TO taq.events_replacing_seed; CREATE TABLE taq.events AS taq.events_replacing_seed ENGINE = MergeTree PARTITION BY toDate(fromUnixTimestamp64Milli(ingested_at_ms)) ORDER BY (ingested_at_ms, id);"

clickhouse --query "INSERT INTO taq.events (schema_version, id, source, kind, entity_ids, occurred_at_ms, occurred_at_quality, ingested_at_ms, broker_appended_at, payload) VALUES (2, 'f2-migration-duplicate', 'f2_migration', 'migration_test', [], 1791028800000, 'source', 1791028800000, toDateTime64('2026-10-03 12:00:00', 3), '{}'), (2, 'f2-migration-duplicate', 'f2_migration', 'migration_test', [], 1791028800000, 'source', 1791028800000, toDateTime64('2026-10-03 12:00:00', 3), '{}')"

clickhouse --multiquery --queries-file=/docker-migrations/0003_events_replay_idempotence.sql
source_keys_match=$(clickhouse --format=TSV --query "SELECT toUInt8((SELECT uniqExact(tuple(ingested_at_ms, id)) FROM taq.events) = (SELECT count() FROM taq.events_replay_safe FINAL))")
fixture_rows=$(clickhouse --format=TSV --query "SELECT count() FROM taq.events_replay_safe FINAL WHERE id = 'f2-migration-duplicate'")
test "$source_keys_match" = "1"
test "$fixture_rows" = "1"

clickhouse --multiquery --queries-file=/docker-migrations/0003_events_replay_idempotence_activate.sql
engine=$(clickhouse --format=TSV --query "SELECT engine FROM system.tables WHERE database = 'taq' AND name = 'events'")
fixture_rows=$(clickhouse --format=TSV --query "SELECT count() FROM taq.events FINAL WHERE id = 'f2-migration-duplicate'")
test "$engine" = "ReplacingMergeTree"
test "$fixture_rows" = "1"

echo "migration_engine=$engine duplicate_logical_rows=$fixture_rows"
