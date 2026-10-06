# Graph Report - TaQ  (2026-10-06)

## Corpus Check
- 92 files · ~159,528 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 11 file(s) not represented in the graph (top: (none) 6, .Dockerfile 2, .example 1)

## Summary
- 950 nodes · 1504 edges · 79 communities (53 shown, 26 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 9 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `7be81d46`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- taq-state/src/lib.rs
- pipeline.rs
- What You Must Do When Invoked
- BlueskySource
- graphify reference: extra exports and benchmark
- README.md
- graphify reference: query, path, explain
- source.rs
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- AGENTS.md
- extraction-spec.md
- taq-ingest
- taq-api/src/main.rs
- taq-dlq-replay/src/lib.rs
- taq-core/src/lib.rs
- Plan de implementación del motor TaQ
- Contexto del proyecto TaQ
- init.sql
- poll_feed
- taq-observability/src/lib.rs
- 0002_product_domain.sql
- Review Focus
- F2 — Entrega y recuperación confiables
- Modelo inicial de amenazas y privacidad
- F2 Reliable Delivery Implementation Plan
- Runbook de despliegue y recuperación (borrador F11)
- ADR 0001: Arquitectura y stack de la Fase 0
- Paquete de evidencia de release TaQ
- TaQ
- ADR 0005: Medir latencia desde el append de Redpanda
- ADR 0006: Observabilidad local
- ADR 0007: Entrega y replay confiables (F2)
- wait-compose.sh
- Migraciones ClickHouse
- postgres/init.sql
- 0001_source_cursors.sql
- ADR 0002: Deduplicación acotada en el proceso de ingesta
- ADR 0003: Arquitectura de conectores de la Fase 1
- ADR 0004: Versionado explícito del esquema de eventos
- ADR 0008: cursores durables y primer flujo de producto (F3–F9)
- Operación de entrega y recuperación (F2)
- component
- clickhouse-migration.sh
- f2-recovery.sh
- 0003_events_replay_idempotence.sql
- 0004_stable_identity_prepare.sql
- public.taq_rule_dataset_samples
- component
- component
- component
- component
- component
- component
- component
- component
- component
- component
- Validación local F12 — 2026-10-04
- taq-loadgen/src/main.rs
- taq-ingestd/src/main.rs
- taq-connector-bluesky/src/lib.rs
- taq-connector-rss/src/lib.rs

## God Nodes (most connected - your core abstractions)
1. `ApiError` - 28 edges
2. `AppState` - 23 edges
3. `internal_error()` - 22 edges
4. `Plan de implementación del motor TaQ` - 22 edges
5. `require_scope()` - 20 edges
6. `Auth` - 19 edges
7. `run_pipeline()` - 18 edges
8. `DeliveryMetrics` - 18 edges
9. `register_dataset()` - 17 edges
10. `SourceMessage` - 17 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `create_pg_pool()`  [INFERRED]
  crates/taq-api/src/main.rs → crates/taq-state/src/lib.rs
- `poll_feed()` --calls--> `is_permanent_http_status()`  [INFERRED]
  crates/taq-connector-rss/src/lib.rs → crates/taq-ingest/src/source.rs
- `run()` --calls--> `replay_dead_letter()`  [INFERRED]
  crates/taq-dlq-replay/src/main.rs → crates/taq-dlq-replay/src/lib.rs
- `run()` --calls--> `wait_for_shutdown_signal()`  [INFERRED]
  crates/taq-dlq-replay/src/main.rs → crates/taq-observability/src/lib.rs
- `run_pipeline()` --calls--> `normalize_event()`  [INFERRED]
  crates/taq-ingest/src/pipeline.rs → crates/taq-ingest/src/event.rs

## Import Cycles
- None detected.

## Communities (79 total, 26 thin omitted)

### Community 0 - "taq-state/src/lib.rs"
Cohesion: 0.14
Nodes (5): accepts_valid_pool_limits(), create_pg_pool(), defaults_to_bounded_pool_and_acquire_timeout(), PgCursorStore, PgPoolConfig

### Community 1 - "pipeline.rs"
Cohesion: 0.12
Nodes (12): DEAD_LETTER_SCHEMA_VERSION, dead_letter_serializes_with_current_schema_version(), DeadLetter, KafkaPipeline, KafkaPipelineConfig, log_pipeline_failure(), now_ms(), pipeline_stats_balance_each_received_event_into_one_outcome() (+4 more)

### Community 2 - "What You Must Do When Invoked"
Cohesion: 0.08
Nodes (24): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Part A - Structural extraction for code files (+16 more)

### Community 3 - "BlueskySource"
Cohesion: 0.20
Nodes (4): bluesky_id_includes_repository_collection_record_and_operation(), BlueskySource, cursor_from_payload(), native_id_from_payload()

### Community 4 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 6 - "graphify reference: query, path, explain"
Cohesion: 0.33
Nodes (5): For /graphify explain, For /graphify path, graphify reference: query, path, explain, Step 0 — Constrained query expansion (REQUIRED before traversal), Step 1 — Traversal

### Community 7 - "source.rs"
Cohesion: 0.07
Nodes (23): WikimediaSource, bounded_channel_applies_backpressure_at_capacity(), bounded_source_channel(), FlakySource, InvalidChannelCapacity, is_permanent_http_status(), permanent_source_error_is_not_retried(), PermanentFailure (+15 more)

### Community 8 - "graphify reference: add a URL and watch a folder"
Cohesion: 0.50
Nodes (3): For /graphify add, For --watch, graphify reference: add a URL and watch a folder

### Community 9 - "graphify reference: commit hook and native CLAUDE.md integration"
Cohesion: 0.50
Nodes (3): For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration

### Community 10 - "graphify reference: incremental update and cluster-only"
Cohesion: 0.50
Nodes (3): For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only

### Community 15 - "taq-ingest"
Cohesion: 0.38
Nodes (11): taq-api, taq-connector-bluesky, taq-connector-rss, taq-connector-wikimedia, taq-core, taq-dlq-replay, taq-ingest, taq-ingestd (+3 more)

### Community 16 - "taq-api/src/main.rs"
Cohesion: 0.08
Nodes (68): ActionFilter, ActionPolicy, ApiError, AppState, Auth, authenticate(), calculate_rule_metrics(), ContextMatch (+60 more)

### Community 17 - "taq-dlq-replay/src/lib.rs"
Cohesion: 0.24
Nodes (10): dead_letter(), dry_run_validates_but_neither_publishes_nor_commits(), legacy_envelope_republishes_event_then_commits(), publish_failure_does_not_commit_dlq_offset(), replay_dead_letter(), ReplayOutcome, Published, Validated (+2 more)

### Community 18 - "taq-core/src/lib.rs"
Cohesion: 0.10
Nodes (14): Action, Event, Signal, SignalKind, Anomaly, Burst, Forecast, TimestampQuality (+6 more)

### Community 19 - "Plan de implementación del motor TaQ"
Cohesion: 0.07
Nodes (26): Alcance objetivo y exclusiones, Avance de implementación tras F2 (pendiente de pruebas finales), Comandos comprobados, Decisiones del dueño del producto, Dependencias y puertas, Diagnóstico ejecutivo, Evidencia adicional (2026-10-06), F0 — Verificación del repositorio (completada) (+18 more)

### Community 20 - "Contexto del proyecto TaQ"
Cohesion: 0.13
Nodes (15): Arquitectura y recorrido del evento, Arrancar y trabajar localmente, Avance F2 (2026-10-03), Añadir una fuente, Cambiar el evento o almacenamiento, Contexto del proyecto TaQ, Contratos principales, Cómo extender el proyecto (+7 more)

### Community 23 - "poll_feed"
Cohesion: 0.17
Nodes (6): feed_cursor(), feed_validators_from_cursor(), FeedValidators, poll_feed(), RssSource, trim_seen_ids()

### Community 27 - "taq-observability/src/lib.rs"
Cohesion: 0.06
Nodes (16): counters_export_terminal_outcomes_without_event_identifiers(), DeliveryMetrics, GLOBAL_DELIVERY_METRICS, IngestOutcome, DeadLettered, DuplicateSkipped, Failed, Published (+8 more)

### Community 28 - "0002_product_domain.sql"
Cohesion: 0.18
Nodes (22): idx_taq_api_keys_tenant, idx_taq_entity_alias_lookup, idx_taq_findings_feed, public.taq_action_outbox, public.taq_action_policies, public.taq_actions, public.taq_api_keys, public.taq_api_usage (+14 more)

### Community 29 - "Review Focus"
Cohesion: 0.13
Nodes (14): Execution Notes, Global Constraints, Review Focus, TaQ: F3 a F12 Implementation Plan, Task 10: Consolidated final validation, Task 1: F3 source configuration and durable cursors, Task 2: F4 route-real E2E and readiness, Task 3: F5 tenant-scoped watchlists and context (+6 more)

### Community 30 - "F2 — Entrega y recuperación confiables"
Cohesion: 0.13
Nodes (14): 1. Replay de DLQ con entrega al menos una vez, 2. Proyección idempotente en ClickHouse, 3. Observabilidad y salud, 4. Apagado y supervisión, 5. Pruebas y CI, Auto-revisión, Contexto, Decisiones pendientes para el plan (+6 more)

### Community 31 - "Modelo inicial de amenazas y privacidad"
Cohesion: 0.20
Nodes (8): Bloqueos previos a activar una fuente en producción, Fuentes: límites técnicos y condiciones pendientes, Referencias técnicas y de términos, Activos y límites de confianza, Amenazas y controles presentes, Modelo inicial de amenazas y privacidad, No-Go público actual, Riesgos y tareas de cierre

### Community 32 - "F2 Reliable Delivery Implementation Plan"
Cohesion: 0.20
Nodes (9): Execution notes, F2 Reliable Delivery Implementation Plan, Global Constraints, Review Focus, Self-review, Task 1: Replay consumer contract and CLI, Task 2: Idempotent ClickHouse projection and migration, Task 3: Application metrics and ClickHouse rejection visibility (+1 more)

### Community 33 - "Runbook de despliegue y recuperación (borrador F11)"
Cohesion: 0.29
Nodes (6): Antes de desplegar, Arranque y salud, Backups y restauración, Cierre pendiente para F11, Incidentes y reversión, Runbook de despliegue y recuperación (borrador F11)

### Community 34 - "ADR 0001: Arquitectura y stack de la Fase 0"
Cohesion: 0.33
Nodes (6): ADR 0001: Arquitectura y stack de la Fase 0, Consecuencias, Contexto, Decisión, Deduplicación, Imágenes verificadas

### Community 35 - "Paquete de evidencia de release TaQ"
Cohesion: 0.33
Nodes (5): Decisión, Identificación, Paquete de evidencia de release TaQ, Puertas, Resultados medidos

### Community 36 - "TaQ"
Cohesion: 0.33
Nodes (6): API y producto en desarrollo (F3–F9), Arranque, Estructura, Ingesta (Fase 1 en curso), Requisitos, TaQ

### Community 37 - "ADR 0005: Medir latencia desde el append de Redpanda"
Cohesion: 0.40
Nodes (4): ADR 0005: Medir latencia desde el append de Redpanda, Consecuencias, Contexto, Decisión

### Community 38 - "ADR 0006: Observabilidad local"
Cohesion: 0.40
Nodes (4): ADR 0006: Observabilidad local, Consecuencias, Contexto, Decisión

### Community 39 - "ADR 0007: Entrega y replay confiables (F2)"
Cohesion: 0.40
Nodes (4): ADR 0007: Entrega y replay confiables (F2), Consecuencias, Contexto, Decisión

### Community 41 - "Migraciones ClickHouse"
Cohesion: 0.50
Nodes (3): 0003 — proyección tolerante a replay, 0004 — identidad estable para retry entre reinicios, Migraciones ClickHouse

### Community 42 - "postgres/init.sql"
Cohesion: 0.67
Nodes (3): idx_taq_source_events_seen_seen_at, public.taq_source_cursors, public.taq_source_events_seen

### Community 43 - "0001_source_cursors.sql"
Cohesion: 0.67
Nodes (3): idx_taq_source_events_seen_seen_at, public.taq_source_cursors, public.taq_source_events_seen

### Community 44 - "ADR 0002: Deduplicación acotada en el proceso de ingesta"
Cohesion: 0.50
Nodes (4): ADR 0002: Deduplicación acotada en el proceso de ingesta, Consecuencias, Contexto, Decisión

### Community 45 - "ADR 0003: Arquitectura de conectores de la Fase 1"
Cohesion: 0.50
Nodes (4): ADR 0003: Arquitectura de conectores de la Fase 1, Consecuencias, Contexto, Decisión

### Community 46 - "ADR 0004: Versionado explícito del esquema de eventos"
Cohesion: 0.50
Nodes (4): ADR 0004: Versionado explícito del esquema de eventos, Consecuencias, Contexto, Decisión

### Community 47 - "ADR 0008: cursores durables y primer flujo de producto (F3–F9)"
Cohesion: 0.50
Nodes (3): ADR 0008: cursores durables y primer flujo de producto (F3–F9), Consecuencias y límites, Decisión

### Community 48 - "Operación de entrega y recuperación (F2)"
Cohesion: 0.50
Nodes (3): Métricas de aplicaciones, Operación de entrega y recuperación (F2), Rechazos del consumidor de ClickHouse

### Community 49 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 63 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 64 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 65 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 66 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 67 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 68 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 69 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 70 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 71 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 72 - "component"
Cohesion: 0.10
Nodes (19): bomFormat, bom-ref, components, licenses, name, purl, scope, type (+11 more)

### Community 73 - "Validación local F12 — 2026-10-04"
Cohesion: 0.25
Nodes (7): Addendum de Docker operativo — 2026-10-06, Addendum de recuperación Docker — 2026-10-05 (continuación), Addendum Docker — 2026-10-05, Decisión de release, Gates ejecutados, Gates no ejecutados, Validación local F12 — 2026-10-04

### Community 74 - "taq-loadgen/src/main.rs"
Cohesion: 0.06
Nodes (26): main(), parse_dry_run_value(), positive_env(), run(), accepts_event_after_retention_expires(), dedup(), Deduplicator, DeduplicatorConfig (+18 more)

### Community 76 - "taq-ingestd/src/main.rs"
Cohesion: 0.16
Nodes (4): forward_source_events(), main(), run(), source_task_exit()

### Community 78 - "taq-connector-rss/src/lib.rs"
Cohesion: 0.25
Nodes (8): falls_back_for_missing_invalid_timezone_free_or_future_dates(), parse_occurred_at(), parses_rfc2822_publication_time_as_source_time(), parses_rfc3339_publication_time_with_offset_as_utc_epoch(), rss_id_prefers_guid_then_link_then_stable_content_hash(), RSS_MAX_RESPONSE_BYTES, rss_native_id(), RSS_SEEN_ID_LIMIT

## Knowledge Gaps
- **388 isolated node(s):** `ContextMatch`, `RULE_DATASET_FORMAT_VERSION`, `BLUESKY_JETSTREAM_URL`, `RSS_SEEN_ID_LIMIT`, `RSS_MAX_RESPONSE_BYTES` (+383 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 515 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **26 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `SourceCursorStore` connect `source.rs` to `taq-state/src/lib.rs`, `pipeline.rs`, `BlueskySource`, `poll_feed`?**
  _High betweenness centrality (0.018) - this node is a cross-community bridge._
- **Why does `SourceMessage` connect `source.rs` to `pipeline.rs`, `BlueskySource`, `taq-ingestd/src/main.rs`, `taq-core/src/lib.rs`, `poll_feed`?**
  _High betweenness centrality (0.011) - this node is a cross-community bridge._
- **What connects `ContextMatch`, `RULE_DATASET_FORMAT_VERSION`, `BLUESKY_JETSTREAM_URL` to the rest of the system?**
  _388 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `taq-state/src/lib.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.1422924901185771 - nodes in this community are weakly interconnected._
- **Should `pipeline.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.11827956989247312 - nodes in this community are weakly interconnected._
- **Should `What You Must Do When Invoked` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._
- **Should `source.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.06545879602571596 - nodes in this community are weakly interconnected._