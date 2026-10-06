# F2 Reliable Delivery Implementation Plan

> **For agentic workers:** use `superpowers:executing-plans` to implement this plan task by task. Steps use checkbox syntax.

**Goal:** Close F2 with recoverable DLQ delivery, replay-safe ClickHouse projection, observable outcomes, and automated recovery/shutdown coverage.

**Architecture:** Keep Redpanda as the durable event log and add an independent Rust replay consumer. Commit DLQ offsets manually only after raw-topic delivery succeeds. Make the ClickHouse logical read path collapse exact replays with ReplacingMergeTree and `FINAL`, preserving original event identity/time; add operational visibility and failure tests.

**Tech Stack:** Rust 2024, Tokio, rdkafka, Serde, Redpanda, ClickHouse, Docker Compose, GitHub Actions, OpenTelemetry Collector/Prometheus/Grafana.

**Spec:** `docs/superpowers/specs/2026-10-03-f2-entrega-y-recuperacion.md`

## Global Constraints

- At-least-once delivery; do not claim end-to-end exactly-once.
- Replay preserves the event ID and `ingested_at_ms` and confirms DLQ offsets only after producer acknowledgement.
- The ClickHouse idempotency guarantee covers copies with the same event ID and original `ingested_at_ms` in the same partition.
- Do not add event IDs, entity IDs, or payloads as metric labels.
- Keep failure-fast behavior if both raw and DLQ publication fail; no local spool in F2.
- Do not delete or recreate user data volumes during migration or tests.
- Preserve pre-existing working-tree changes; do not stage or commit the shared working tree.

## Review Focus

- Kafka rebalance or process crash between raw delivery acknowledgement and DLQ offset commit must cause a safe duplicate, not lost work.
- Malformed DLQ JSON or an unsupported envelope version must not be silently committed as recovered.
- Same ID with a new `ingested_at_ms` may cross partitions; do not hide this limitation or claim global ID deduplication.
- Migration must stop ClickHouse Kafka consumption before changing the materialized view, and must preserve retained Kafka data for resume/rollback.
- SIGTERM during backpressure or broker outage must have a bounded, visible outcome and must not report a clean drain prematurely.

---

### Task 1: Replay consumer contract and CLI

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/taq-ingest/Cargo.toml`
- Modify: `crates/taq-ingest/src/pipeline.rs`
- Modify: `crates/taq-ingest/src/lib.rs`
- Create: `crates/taq-dlq-replay/Cargo.toml`
- Create: `crates/taq-dlq-replay/src/main.rs`
- Test: `crates/taq-ingest/src/pipeline.rs` unit tests and replay crate tests

**Interfaces:**
- Consumes: `taq_ingest::DeadLetter`, `DEAD_LETTER_SCHEMA_VERSION`, and `taq_core::Event`.
- Produces: `taq-dlq-replay` executable; configuration via `TAQ_KAFKA_BROKER`, `TAQ_DLQ_CONSUMER_GROUP`, `TAQ_DLQ_REPLAY_LIMIT`, `TAQ_DLQ_REPLAY_DRY_RUN`; replay decodes only supported envelopes and republishes serialized `event` under Kafka key `event.id`.

- [ ] Add parser tests for legacy missing `schema_version`, current v1, malformed JSON, missing event fields, and unsupported version.
- [ ] Run focused tests and confirm new expected cases fail before implementation.
- [ ] Add `StreamConsumer` with auto commit/store disabled and `FutureProducer`; process one message at a time, await raw-topic ACK, then commit that DLQ message. Dry-run validates and counts without publishing or committing.
- [ ] Stop at the configured per-run limit; use bounded consumer polling and terminate visibly on poison-message/version errors or publish/commit errors, leaving that message uncommitted.
- [ ] Add a fakeable replay operation boundary so tests can assert ordering: produce acknowledgement precedes commit; produce failure never commits.
- [ ] Run `cargo test -p taq-ingest -p taq-dlq-replay`, `cargo fmt --all -- --check`, and `cargo clippy -p taq-ingest -p taq-dlq-replay --all-targets -- -D warnings`.
- [ ] Document invocation and at-least-once crash window in `README.md` and ADR 0003 or a new F2 ADR.

### Task 2: Idempotent ClickHouse projection and migration

**Files:**
- Modify: `deploy/clickhouse/init.sql`
- Create: `deploy/clickhouse/migrations/0003_events_replay_idempotence.sql`
- Modify: `deploy/clickhouse/migrations/README.md` (create if absent)
- Modify: `.github/workflows/ci.yml`
- Modify: `justfile`

**Interfaces:**
- Consumes: `taq.events` schema, raw Kafka table, materialized view, and replay-preserved event fields.
- Produces: fresh-install DDL using `ReplacingMergeTree` with replacement key aligned to `(ingested_at_ms, id)` and the existing daily partition; operational migration instructions and a verification query; deduplicated reads use `taq.events FINAL` where exact logical counts are asserted.

- [ ] Add an ephemeral Redpanda/ClickHouse integration scenario that proves repeated replay is collapsed by the serving read path; keep assertions at the behavior boundary rather than matching DDL source text.
- [ ] Validate baseline Compose/schema assumptions and inspect every `taq.events` query before migration.
- [ ] Change fresh-install DDL to the chosen ReplacingMergeTree form; do not add a version field unless tests prove it is needed for identical replay copies.
- [ ] Write migration that detaches/stops `taq.events_raw` before dropping/recreating the MV, creates a replacement table, backfills existing rows, switches MV/reads, then reattaches consumption; include explicit checks, rollback sequence, and retention-window warning.
- [ ] Ensure migration never runs automatically against an existing volume and never runs `down -v` outside ephemeral CI cleanup.
- [ ] Run `scripts/e2e/clickhouse-migration.sh` against ephemeral ClickHouse, proving two legacy duplicates migrate to one logical row and the Kafka view resumes, then run `scripts/e2e/f2-recovery.sh` and `docker compose ... config --quiet`.

### Task 3: Application metrics and ClickHouse rejection visibility

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/taq-ingest/Cargo.toml`
- Modify: `crates/taq-ingest/src/pipeline.rs`
- Modify: `crates/taq-ingestd/Cargo.toml`
- Modify: `crates/taq-ingestd/src/main.rs`
- Modify: `crates/taq-dlq-replay/Cargo.toml`
- Modify: `crates/taq-dlq-replay/src/main.rs`
- Modify: `deploy/otel/collector.yaml`
- Modify: `deploy/prometheus/prometheus.yml`
- Modify: `deploy/grafana/dashboards/taq-overview.json`
- Create: `docs/operations/f2-delivery.md`

**Interfaces:**
- Consumes: pipeline outcomes and replay outcomes; Collector OTLP receiver and Prometheus exporter at port 8889.
- Produces: bounded-label counters for ingest/replay outcomes and retries/failures, plus documented `system.dead_letter_queue` queries distinctly named from application topic DLQ.

- [ ] Test metric names, counter increments, bounded label values, and that identifiers/payload are never labels.
- [ ] Select compatible OpenTelemetry Rust crate versions from the locked toolchain and wire OTLP export through the existing Collector; include bounded shutdown timeout for exporter flush.
- [ ] Instrument received/published/application-DLQ/duplicates/failures and replay read/published/failed/committed outcomes at their terminal transitions.
- [ ] Extend dashboard with delivery outcomes and document ClickHouse `system.dead_letter_queue` inspection/retention semantics for the pinned ClickHouse image.
- [ ] Validate Prometheus/Collector/Compose configuration and focused Rust tests.

### Task 4: Portable shutdown, recovery E2E, and CI

**Files:**
- Modify: `crates/taq-ingestd/src/main.rs`
- Modify: `crates/taq-dlq-replay/src/main.rs`
- Modify: `.github/workflows/ci.yml`
- Create: `scripts/e2e/f2-recovery.sh` (or PowerShell equivalent plus Linux runner script if platform parity needs it)
- Modify: `README.md`
- Modify: `docs/roadmap/plan-implementacion-motor.md`

**Interfaces:**
- Consumes: task 1 replay behavior, task 2 projection semantics, task 3 metrics.
- Produces: bounded Ctrl+C/SIGTERM shutdown; disposable Redpanda/ClickHouse E2E in CI for at-least-once recovery, duplicate projection, parse DLQ visibility, and supervisor-visible nonzero failure.

- [ ] Add signal tests/helpers proving producer stops before forwarder drain and pipeline flush; assert timeout exits as failure.
- [ ] Add integration harness scenarios: raw ACK/commit crash window, repeat replay with `FINAL` uniqueness, unsupported DLQ message retained, ClickHouse malformed record visible in system DLQ, and recovery after broker restart.
- [ ] Test pipeline task failure/panic leads to process nonzero and ensure CI captures output/exit status.
- [ ] Run scenarios on ephemeral Docker Compose services in CI with unconditional cleanup and no external volumes.
- [ ] Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, Compose config, then CI-equivalent Docker E2E where Docker is available.
- [ ] Update roadmap acceptance and project context with measured results only; attempt `graphify update .` after code changes and report if executable remains inaccessible.

## Execution notes

- `graphify query` was attempted first as required but the executable path is denied by the current sandbox and no `graphify` command is on PATH; direct repository sources and current roadmap/ADR were inspected as fallback.
- The worktree already contains extensive local changes and `.git` is read-only in this environment. Preserve the current checkout and do not stage/commit changes; validate with targeted diffs and tests.
- Docker engine availability must be checked before claiming E2E verification. If unavailable, leave E2E CI implemented and report local E2E as unverified.

## Self-review

- Spec coverage: replay and offset semantics (Task 1); ClickHouse migration/read semantics (Task 2); Rust and ClickHouse error visibility (Task 3); shutdown, recovery, CI, and roadmap acceptance (Task 4).
- No placeholders: tasks identify paths, commands, behaviors, and outputs; flag/config details are bounded to the spec and existing project conventions.
- Interface consistency: Task 1 exports the existing `DeadLetter` model and standalone binary; later tasks consume that exact envelope and preserved event fields.
- Review Focus tests: crash/rebalance and poison message (Tasks 1/4), cross-partition limitation and migration pause (Task 2), SIGTERM/broker outage (Task 4).
- No commit step is included because the managed workspace marks `.git` read-only and unrelated user changes are already present; no Git mutation is necessary to deliver reviewable implementation.
