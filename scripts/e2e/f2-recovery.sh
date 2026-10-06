#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose --env-file .env.example -f deploy/docker-compose.yml)
run_id="f2-$(date +%s%N)"
signal_event_count=40
replay_bin="target/release/taq-dlq-replay"
cargo build --release -p taq-dlq-replay
if [[ -x "${replay_bin}.exe" ]]; then
  replay_bin="${replay_bin}.exe"
fi
test -x "$replay_bin"

for sequence in $(seq 1 "$signal_event_count"); do
  event_id="${run_id}-signal-${sequence}"
  ingested_at_ms="$(date +%s%3N)"
  printf '{"schema_version":1,"source":"f2_signal_e2e","native_id":"%s","failed_at_ms":%s,"reason":"signal fixture","event":{"schema_version":2,"id":"%s","source":"f2_signal_e2e","kind":"recovery_test","entity_ids":[],"occurred_at_ms":%s,"occurred_at_quality":"source","ingested_at_ms":%s,"payload":{"sequence":%s}}}\n' \
    "$event_id" "$ingested_at_ms" "$event_id" "$ingested_at_ms" "$ingested_at_ms" "$sequence"
  sleep 0.25
done |
  "${compose[@]}" exec -T redpanda rpk topic produce events.dead_letter \
    --brokers redpanda:9092 --compression none --key "${run_id}-signal-batch" &
produce_pid=$!

signal_log=$(mktemp)
TAQ_KAFKA_BROKER=localhost:19092 \
TAQ_OTEL_ENDPOINT=http://127.0.0.1:4317 \
TAQ_DLQ_CONSUMER_GROUP="${run_id}-signal" \
TAQ_DLQ_REPLAY_DRY_RUN=false \
TAQ_DLQ_REPLAY_LIMIT=100 \
TAQ_DLQ_REPLAY_IDLE_SECS=30 \
  "$replay_bin" >"$signal_log" 2>&1 &
replay_pid=$!
for attempt in $(seq 1 600); do
  if grep -q "evento republicado" "$signal_log"; then
    break
  fi
  if ! kill -0 "$replay_pid" 2>/dev/null; then
    cat "$signal_log"
    exit 1
  fi
  sleep 0.05
done
if ! grep -q "evento republicado" "$signal_log"; then
  cat "$signal_log" >&2
  exit 1
fi
kill -TERM "$replay_pid"
wait "$replay_pid"
grep -q "SIGTERM" "$signal_log"
rm -f "$signal_log"
wait "$produce_pid"

restart_log=$(mktemp)
TAQ_KAFKA_BROKER=localhost:19092 \
TAQ_OTEL_ENDPOINT=http://127.0.0.1:4317 \
TAQ_DLQ_CONSUMER_GROUP="${run_id}-signal" \
TAQ_DLQ_REPLAY_DRY_RUN=false \
TAQ_DLQ_REPLAY_LIMIT=100 \
TAQ_DLQ_REPLAY_IDLE_SECS=1 \
  "$replay_bin" >"$restart_log" 2>&1
restart_replays=$(grep -c "evento republicado y offset confirmado" "$restart_log" || true)
cat "$restart_log"
rm -f "$restart_log"
test "$restart_replays" -gt 0

signal_rows=0
for attempt in $(seq 1 60); do
  signal_rows=$("${compose[@]}" exec -T clickhouse clickhouse-client \
    --user=default --password=taq_dev --format=TSV \
    --query="SELECT count() FROM taq.events FINAL WHERE source = 'f2_signal_e2e'")
  if [[ "$signal_rows" == "$signal_event_count" ]]; then
    break
  fi
  sleep 1
done
test "$signal_rows" = "$signal_event_count"

event_id="${run_id}-event"
ingested_at_ms="$(date +%s%3N)"
event=$(cat <<JSON
{"schema_version":2,"id":"${event_id}","source":"f2_e2e","kind":"recovery_test","entity_ids":[],"occurred_at_ms":${ingested_at_ms},"occurred_at_quality":"source","ingested_at_ms":${ingested_at_ms},"payload":{"run_id":"${run_id}"}}
JSON
)
dead_letter=$(cat <<JSON
{"schema_version":1,"source":"f2_e2e","native_id":"${event_id}","failed_at_ms":${ingested_at_ms},"reason":"e2e replay fixture","event":${event}}
JSON
)

printf '%s\n' "$dead_letter" |
  "${compose[@]}" exec -T redpanda rpk topic produce events.dead_letter \
    --brokers redpanda:9092 --compression none --key "$event_id"

for pass in 1 2; do
  TAQ_KAFKA_BROKER=localhost:19092 \
  TAQ_OTEL_ENDPOINT=http://127.0.0.1:4317 \
  TAQ_DLQ_CONSUMER_GROUP="${run_id}-pass-${pass}" \
  TAQ_DLQ_REPLAY_DRY_RUN=false \
  TAQ_DLQ_REPLAY_LIMIT=1 \
    "$replay_bin"
done

query_clickhouse() {
  "${compose[@]}" exec -T clickhouse clickhouse-client \
    --user=default --password=taq_dev --format=TSV --query="$1"
}

count=0
for attempt in $(seq 1 60); do
  count=$(query_clickhouse "SELECT count() FROM taq.events FINAL WHERE id = '${event_id}'")
  if [[ "$count" == "1" ]]; then
    break
  fi
  sleep 1
done
test "$count" = "1"

printf '%s\n' "malformed-f2-${run_id}" |
  "${compose[@]}" exec -T redpanda rpk topic produce events.raw \
    --brokers redpanda:9092 --compression none --key "${run_id}-malformed"

dead_letter_count=0
for attempt in $(seq 1 60); do
  query_clickhouse "SYSTEM FLUSH LOGS" >/dev/null
  dead_letter_count=$(query_clickhouse "SELECT count() FROM system.dead_letter_queue WHERE database = 'taq' AND table = 'events_raw' AND kafka_topic_name = 'events.raw' AND raw_message LIKE '%malformed-f2-${run_id}%'")
  if [[ "$dead_letter_count" -gt 0 ]]; then
    break
  fi
  sleep 1
done
test "$dead_letter_count" -gt 0

"${compose[@]}" stop redpanda
if TAQ_KAFKA_BROKER=localhost:19092 \
  cargo run --release -p taq-loadgen -- --rate 1 --duration 1 --entities 1 --duplicate-ratio 0; then
  echo "el productor no reportó fallo durante la interrupción de Redpanda" >&2
  exit 1
else
  echo "fallo de publicación visible durante la interrupción de Redpanda"
fi

"${compose[@]}" start redpanda
healthy=false
for attempt in $(seq 1 30); do
  if "${compose[@]}" exec -T redpanda rpk cluster health 2>&1 | grep -E -q 'Healthy:.+true'; then
    healthy=true
    break
  fi
  sleep 1
done
test "$healthy" = "true"
TAQ_KAFKA_BROKER=localhost:19092 \
  cargo run --release -p taq-loadgen -- --rate 5 --duration 1 --entities 1 --duplicate-ratio 0

echo "sigterm_replay_logical_rows=$signal_rows replay_logical_rows=$count clickhouse_parse_errors=$dead_letter_count"
