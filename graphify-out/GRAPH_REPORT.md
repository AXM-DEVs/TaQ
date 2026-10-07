# Graph Report - TaQ  (2026-10-06)

## Corpus Check
- 180 files · ~398,555 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 27 file(s) not represented in the graph (top: (none) 9, .toml 5, .woff2 5)

## Summary
- 2936 nodes · 5352 edges · 191 communities (163 shown, 28 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 146 edges (avg confidence: 0.87)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `4145ad3c`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- taq-state/src/lib.rs
- pipeline.rs
- taq-kafka/src/lib.rs
- SourceCursorStore
- captureElementToBlob
- README.md
- Enfoque de revisión
- source.rs
- AGENTS.md
- taq-ingest
- taq-api/src/main.rs
- taq-dlq-replay/src/lib.rs
- taq-core/src/lib.rs
- Plan de implementación del motor TaQ
- Contexto del proyecto TaQ
- init.sql
- taq-connector-rss/src/lib.rs
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
- live-browser.js
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
- taq-connector-bluesky/src/lib.rs
- connectSSE
- modern-screenshot.umd.js
- el
- syncPageChatFocus
- run-hyperframes.mjs
- setLiveState
- gsap.min.js
- renderDesignVisual
- GSAP Plugins
- MotionPathPlugin.min.js
- Multi-Phase Camera
- impeccable/SKILL.md
- initGlobalBar
- ScrollTrigger.min.js
- live.md
- Responsive Design
- mountSvelteComponentVariant
- actOnAgentTarget
- Editorial Forest — Frame (video / frame layer)
- Dynamic Content Sequencing
- onboard.md
- Dynamic Content Sequencing
- Dynamic Content Sequencing
- Operate mode depth (and Read notes)
- The Toolkit
- y
- Coordinate Target Zoom
- t
- createLiveBrowserSessionState
- Design Engineering
- GSAP Core
- createLiveBrowserDomHelpers
- GSAP ScrollTrigger
- s
- ce
- animate.md
- Handle `generate`
- showToast
- Dynamic Content Sequencing
- Center-Outward Expansion
- Dynamic Content Sequencing
- Dynamic Content Sequencing
- Generate Report
- onAnnotDown
- Dc
- doctor.md
- New visual work
- optimize.md
- dedup.rs
- Impeccable Asset Producer
- Scan mode (approach C: auto-extract, then confirm descriptive language)
- critique.md
- Simplify the Design
- Hardening Dimensions
- Frame worker — core contract (shared by the narrative video workflows)
- GSAP Timeline
- WikimediaSource
- Product
- clarify.md
- Nielsen's 10 Heuristics
- Generate Combined Critique Report
- document.md
- polish.md
- quieter.md
- taq-ingestd/src/main.rs
- CORE DIRECTIVE: AWWWARDS-LEVEL DESIGN ENGINEERING
- jf
- STORYBOARD.md
- Init flow
- Tween
- L
- oc
- hyperframes.json
- Common Cognitive Load Violations
- iOS platform
- Shape
- Android platform
- colorize.md
- Persona-Based Design Testing
- Extract Flow
- Component Building Principles
- anyhow
- na
- HyperFrames Composition Project
- HyperFrames Composition Project
- Dynamic Content Sequencing
- Generate Report
- Cognitive Load Assessment
- Impeccable Finish Reviewer
- Impeccable Manual Edit Applier
- live-browser-ignores.js
- Diagnostic Scan
- Visualize: Direction Comps & Asset Production
- fb
- The Animation Decision Framework
- clip-path for Animation
- Performance Rules
- Gesture and Drag Interactions
- Ra
- Adaptation Strategies
- Impeccable Documenter
- Region map
- The Sonner Principles (Building Loved Components)
- site/README.md
- la
- BRIEF.md
- TaQ — Video promocional
- Heuristics Scoring Guide
- Operate
- Persuade and Experience
- Read
- Command guidance
- Core Philosophy
- Debugging Animations
- Relación con el TaQ anterior
- asset-descriptions.md
- index.md

## God Nodes (most connected - your core abstractions)
1. `connectSSE()` - 34 edges
2. `setLiveState()` - 33 edges
3. `resumeSession()` - 33 edges
4. `showToast()` - 31 edges
5. `initGlobalBar()` - 30 edges
6. `el()` - 29 edges
7. `ApiError` - 28 edges
8. `handleKeyDown()` - 27 edges
9. `cleanup()` - 27 edges
10. `buildInsertConfigureRow()` - 26 edges

## Surprising Connections (you probably didn't know these)
- `srcset with Width Descriptors` --references--> `w()`  [INFERRED]
  .agents/skills/impeccable/reference/adapt.md → site/vendor/gsap.min.js
- `Framer Motion hardware acceleration caveat` --references--> `y()`  [INFERRED]
  .codex/skills/emil-design-eng/SKILL.md → site/vendor/gsap.min.js
- `ScrollToPlugin` --references--> `y()`  [INFERRED]
  .codex/skills/gsap-plugins/SKILL.md → site/vendor/gsap.min.js
- `Critical Constraints` --references--> `y()`  [INFERRED]
  taq-video/.hyperframes/frame-packets/06-finding.md → site/vendor/gsap.min.js
- `How It Works` --references--> `y()`  [INFERRED]
  taq-video/.hyperframes/frame-packets/06-finding.md → site/vendor/gsap.min.js

## Import Cycles
- None detected.

## Communities (191 total, 28 thin omitted)

### Community 0 - "taq-state/src/lib.rs"
Cohesion: 0.09
Nodes (10): validate_public_runtime_config(), accepts_valid_pool_limits(), create_pg_pool(), defaults_to_bounded_pool_and_acquire_timeout(), pg_tls_config(), PgCursorStore, PgPoolConfig, PgTlsMode (+2 more)

### Community 1 - "pipeline.rs"
Cohesion: 0.12
Nodes (12): DEAD_LETTER_SCHEMA_VERSION, dead_letter_serializes_with_current_schema_version(), DeadLetter, KafkaPipeline, KafkaPipelineConfig, log_pipeline_failure(), now_ms(), pipeline_stats_balance_each_received_event_into_one_outcome() (+4 more)

### Community 2 - "taq-kafka/src/lib.rs"
Cohesion: 0.07
Nodes (24): CA_ENV, CERT_ENV, config(), identifies_obvious_development_credentials_for_public_guard(), KafkaSecurityConfig, KEY_ENV, loads_sasl_password_from_file_and_preserves_non_newline_whitespace(), MECHANISM_ENV (+16 more)

### Community 4 - "captureElementToBlob"
Cohesion: 0.12
Nodes (20): averageRgb01(), captureChromeNodes(), captureElementFromRenderedAncestor(), captureElementToBlob(), compileShader(), cssColorToRgb01(), dominantRgb01(), findBackdropAncestor() (+12 more)

### Community 6 - "Enfoque de revisión"
Cohesion: 0.12
Nodes (14): Estado, Ledger: transporte seguro, Pre-flight y conflictos de tareas, Rulings, Archivos y límites, Enfoque de revisión, Restricciones globales, Tarea 1: Configuración TLS de PostgreSQL (+6 more)

### Community 7 - "source.rs"
Cohesion: 0.10
Nodes (20): bounded_channel_applies_backpressure_at_capacity(), bounded_source_channel(), FlakySource, InvalidChannelCapacity, permanent_source_error_is_not_retried(), PermanentFailure, retry_delay_applies_jitter_and_respects_the_cap(), RetryPolicy (+12 more)

### Community 15 - "taq-ingest"
Cohesion: 0.39
Nodes (12): taq-api, taq-connector-bluesky, taq-connector-rss, taq-connector-wikimedia, taq-core, taq-dlq-replay, taq-ingest, taq-ingestd (+4 more)

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
Nodes (28): Alcance objetivo y exclusiones, Avance de implementación tras F2 (pendiente de pruebas finales), Comandos comprobados, Decisiones del dueño del producto, Dependencias y puertas, Diagnóstico ejecutivo, Evidencia adicional (2026-10-06), F0 — Verificación del repositorio (completada) (+20 more)

### Community 20 - "Contexto del proyecto TaQ"
Cohesion: 0.12
Nodes (17): Arquitectura y recorrido del evento, Arrancar y trabajar localmente, Avance F2 (2026-10-03), Añadir una fuente, Cambiar el evento o almacenamiento, Contexto del proyecto TaQ, Continuación de F10 — transporte seguro (2026-10-06), Contratos principales (+9 more)

### Community 23 - "taq-connector-rss/src/lib.rs"
Cohesion: 0.12
Nodes (14): falls_back_for_missing_invalid_timezone_free_or_future_dates(), feed_cursor(), feed_validators_from_cursor(), FeedValidators, parse_occurred_at(), parses_rfc2822_publication_time_as_source_time(), parses_rfc3339_publication_time_with_offset_as_utc_epoch(), poll_feed() (+6 more)

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
Cohesion: 0.22
Nodes (8): Antes de desplegar, Arranque y salud, Backups y restauración, Cierre pendiente para F11, Incidentes y reversión, Rotación de CA, certificados y credenciales, Runbook de despliegue y recuperación (borrador F11), Transporte cifrado del API

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

### Community 61 - "live-browser.js"
Cohesion: 0.03
Nodes (112): addManualContextText(), agentHasWorkInFlight(), applyGlobalBarLabelState(), applyLiveBarPreference(), applyParamDefaults(), applyParamValue(), applyPlaceholderSizingStyles(), bindEditBadgeProxy() (+104 more)

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
Cohesion: 0.20
Nodes (11): account_result(), Config, event_id(), event_sample_cache_discards_oldest_entry_at_capacity(), EventSample, main(), MAX_EVENT_CACHE, MAX_IN_FLIGHT (+3 more)

### Community 76 - "taq-connector-bluesky/src/lib.rs"
Cohesion: 0.17
Nodes (5): bluesky_id_includes_repository_collection_record_and_operation(), BLUESKY_JETSTREAM_URL, cursor_from_payload(), native_id_from_payload(), WIKIMEDIA_SSE_URL

### Community 77 - "connectSSE"
Cohesion: 0.07
Nodes (69): applySavedSessionMeta(), checkpointPayload(), clampVariantIndex(), clearSession(), commitAcceptedVariantToDom(), completeParameterGenerationIfReady(), completeParameterPublication(), completeSourceInjection() (+61 more)

### Community 78 - "modern-screenshot.umd.js"
Cohesion: 0.08
Nodes (57): ae(), be(), bt(), Ce(), s(), Ct(), de(), dt() (+49 more)

### Community 79 - "el"
Cohesion: 0.07
Nodes (56): actionLabel(), applyConfigureBarChrome(), bindConfigureCountPillTooltip(), bindConfigureInlineControlHover(), bindConfigureModifierPillHover(), buildConfigureActionControl(), buildConfigureCountControl(), buildConfigureRow() (+48 more)

### Community 80 - "syncPageChatFocus"
Cohesion: 0.09
Nodes (49): armPageChatForTyping(), attachSteerFocusDebug(), attachSteerFocusGuard(), buildSteerProcessingDots(), clearSteerAwaitTimer(), clearSteerFocusRecoverTimer(), collapsePageChat(), expandPageChat() (+41 more)

### Community 81 - "run-hyperframes.mjs"
Cohesion: 0.04
Nodes (40): ffmpeg-static, @ffprobe-installer/win32-x64, @fontsource/jetbrains-mono, @fontsource/manrope, gsap, hyperframes, devDependencies, ffmpeg-static (+32 more)

### Community 83 - "setLiveState"
Cohesion: 0.08
Nodes (69): abortSvelteComponentInjection(), applyEditing(), beginNewLiveConfiguration(), buildInsertPlaceholderSnapshotFromDom(), buildLocatorForLeaf(), buildPickedAnchorSnapshot(), cancelEditing(), cancelEditingToPicking() (+61 more)

### Community 84 - "gsap.min.js"
Cohesion: 0.07
Nodes (18): ia(), ja(), Lc(), Md(), Nc(), Nd(), oa(), Od() (+10 more)

### Community 85 - "renderDesignVisual"
Cohesion: 0.08
Nodes (39): buildCollapsible(), buildColorModels(), buildDesignHeader(), buildListHtml(), buildRadiiModels(), buildTypographyModels(), cssSafe(), designEmptyMessage() (+31 more)

### Community 86 - "GSAP Plugins"
Cohesion: 0.05
Nodes (36): Best practices, CustomBounce, CustomEase, CustomWiggle, Development, Do Not, DOM / UI, Draggable (+28 more)

### Community 87 - "MotionPathPlugin.min.js"
Cohesion: 0.10
Nodes (29): arcToSegment(), ba(), ca(), cacheRawPathMeasurements(), convertToPath(), D(), getGlobalMatrix(), getPositionOnPath() (+21 more)

### Community 88 - "Multi-Phase Camera"
Cohesion: 0.06
Nodes (30): Assigned storyboard block, Critical Constraints, Critical Constraints, Critical Constraints, Extreme range — 4–12× outward (workspace reveal), Frame 4 — Contexto, Frame packet: 04-context, How It Works (+22 more)

### Community 89 - "impeccable/SKILL.md"
Cohesion: 0.10
Nodes (20): Assess Adaptation Challenge, Implement & Verify, Recommended Actions, Craft (deprecated alias), Apply, Live-mode signature params, Set the spatial thesis, Two isolated assessments (+12 more)

### Community 90 - "initGlobalBar"
Cohesion: 0.09
Nodes (37): agentStatusText(), barPaletteForTheme(), brandMarkSvg(), buildParamsPanel(), cursorForInsertAxis(), designPanelCss(), detectPageTheme(), ensureAgentPollTooltip() (+29 more)

### Community 91 - "ScrollTrigger.min.js"
Cohesion: 0.07
Nodes (4): ic(), Ka(), La(), rb()

### Community 92 - "live.md"
Cohesion: 0.07
Nodes (26): Step 1: Parse the request, Step 2: Reuse the page, then start, Step 3: Generate, Step 4: Accept and close, Cleanup, Exit, First-time setup, Handle `accept` (+18 more)

### Community 93 - "Responsive Design"
Cohesion: 0.08
Nodes (25): Assess Adaptation Challenge, Breakpoints: Content-Driven, Content Adaptation, Desktop Adaptation (Mobile → Desktop), Detect Input Method, Not Just Screen Size, Email Adaptation (Web → Email), Implement Adaptations, Layout Adaptation Patterns (+17 more)

### Community 94 - "mountSvelteComponentVariant"
Cohesion: 0.10
Nodes (33): acceptedDomAlreadyClean(), applyOriginalAttrsToSvelteAnchor(), clearHandledWrapperReloadStamp(), commitAcceptedSvelteComponentToDom(), componentModuleCandidates(), deferredRecoverySuperseded(), describeMountFailure(), detectDevServerBase() (+25 more)

### Community 95 - "actOnAgentTarget"
Cohesion: 0.11
Nodes (42): actOnAgentTarget(), agentTargetBusyReason(), agentTargetOverlayGone(), agentTargetTaken(), claimAgentTarget(), claimAndActOnAgentTarget(), clearStoredManualApplyState(), declineAgentTargetBusy() (+34 more)

### Community 96 - "Editorial Forest — Frame (video / frame layer)"
Cohesion: 0.08
Nodes (25): 1 · Cover (identity · move: oversized serif · green · left), 2 · Topic Tiles (catalog · move: rotating-fill grid · cream · the dense frame), 3 · KPI Stat (data · move: oversized figure · green), 4 · Statement (quote · move: display serif · cream · left), 5 · Step Framework (process · move: 8px step tiles · cream/green), 6 · Chart (data · move: bars + meta · green), Approved Real Entities, Aspect-Ratio Behavior (+17 more)

### Community 97 - "Dynamic Content Sequencing"
Cohesion: 0.08
Nodes (23): Accelerating cadence (geometric hold decay), Assigned storyboard block, Critical Constraints, Critical Constraints, Dynamic Content Sequencing, Frame 1 — La pregunta, Frame packet: 01-hook, How It Works (+15 more)

### Community 98 - "onboard.md"
Cohesion: 0.09
Nodes (22): Assess Onboarding Needs, Context Over Ceremony, Contextual Help, Design Onboarding Experiences, Documentation & Help, Empty State Design, Feature Discovery & Adoption, Guided Tours & Walkthroughs (+14 more)

### Community 99 - "Dynamic Content Sequencing"
Cohesion: 0.09
Nodes (22): Accelerating cadence (geometric hold decay), Assigned storyboard block, Critical Constraints, Critical Constraints, Dynamic Content Sequencing, Frame 2 — TaQ, Frame packet: 02-promise, How It Works (+14 more)

### Community 100 - "Dynamic Content Sequencing"
Cohesion: 0.09
Nodes (22): Accelerating cadence (geometric hold decay), Assigned storyboard block, Critical Constraints, Critical Constraints, Dynamic Content Sequencing, Frame 3 — Señales públicas, Frame packet: 03-sources, grid-card-assemble — Grid / Card Assemble (+14 more)

### Community 101 - "Operate mode depth (and Read notes)"
Cohesion: 0.10
Nodes (18): Craft floor, Refuse, Verify, Constraints, Failure modes, Flow, $impeccable hooks, Routing (+10 more)

### Community 102 - "The Toolkit"
Cohesion: 0.10
Nodes (20): Animate complex properties, Assess What "Extraordinary" Means Here, For data-heavy interfaces, For functional UI, For performance-critical UI, For visual/marketing surfaces, Implement with Discipline, Interact with the device (+12 more)

### Community 103 - "y"
Cohesion: 0.10
Nodes (21): Review Checklist, Do Not, Official GSAP best practices, Transforms and CSS properties, y(), Center-Outward Expansion, Critical Constraints, How It Works (+13 more)

### Community 104 - "Coordinate Target Zoom"
Cohesion: 0.10
Nodes (20): Assigned storyboard block, Coordinate Target Zoom, Critical Constraints, Critical Constraints, Frame 7 — Aprobación humana, Frame packet: 07-approval, Getting the offset, How It Works (+12 more)

### Community 105 - "t"
Cohesion: 0.13
Nodes (19): Gc(), Hc(), t(), Critical Constraints, Discrete Text Sequence, How It Works, Recipe, See also (+11 more)

### Community 106 - "createLiveBrowserSessionState"
Cohesion: 0.22
Nodes (15): createLiveBrowserSessionState(), clearHandled(), clearScrollY(), clearSession(), isHandled(), loadSession(), markHandled(), nextCheckpointRevision() (+7 more)

### Community 107 - "Design Engineering"
Cohesion: 0.11
Nodes (17): 3D transforms for depth, Accessibility, CSS Transform Mastery, Design Engineering, Initial Response, Interruptibility advantage, prefers-reduced-motion, Review Format (Required) (+9 more)

### Community 108 - "GSAP Core"
Cohesion: 0.11
Nodes (17): Accessibility and responsive (gsap.matchMedia()), Common vars, Core Tween Methods, Custom: use CustomEase (plugin), Defaults, Easing, Function-based values, GSAP Core (+9 more)

### Community 109 - "createLiveBrowserDomHelpers"
Cohesion: 0.16
Nodes (11): createLiveBrowserDomHelpers(), cssId(), liveUiRoot(), makeFrozenAnchor(), own(), pickable(), rectIsUsableAnchor(), uiAppend() (+3 more)

### Community 110 - "GSAP ScrollTrigger"
Cohesion: 0.12
Nodes (14): Basic Trigger, Do Not, GSAP ScrollTrigger, Horizontal scroll (containerAnimation), Key config options, Learn More, Markers (Development), Official GSAP best practices (+6 more)

### Community 111 - "s"
Cohesion: 0.18
Nodes (17): _a(), Ao(), cb(), cc(), gb(), hb(), ib(), jb() (+9 more)

### Community 112 - "ce"
Cohesion: 0.17
Nodes (17): Ae(), ce(), $d(), ee(), ga(), ha(), he(), ka() (+9 more)

### Community 113 - "animate.md"
Cohesion: 0.12
Nodes (14): Accessibility and control, Choose material by meaning, Find the job, Implement to the runtime, Set the motion thesis, Timing and easing, Verify, Visitor mode (+6 more)

### Community 114 - "Handle `generate`"
Cohesion: 0.12
Nodes (16): 1. Read the screenshot (if present), 2. Wrap the element, 3. Load the action's reference, 4. Plan three variants: identity first, then mode, then axes, 5. Apply the freeform prompt (if present), 6. Deliver variants, 7. Parameters (composition-sized, 0-4 per variant), 8. Signal done (+8 more)

### Community 115 - "showToast"
Cohesion: 0.15
Nodes (18): abandonForeignSession(), abandonSupersededGo(), copyToClipboard(), discardOrphanedSession(), dismissToast(), injectVariantsFromSource(), isJsxSourceFile(), markSessionHandled() (+10 more)

### Community 118 - "Dynamic Content Sequencing"
Cohesion: 0.12
Nodes (15): Accelerating cadence (geometric hold decay), Assigned storyboard block, Critical Constraints, Dynamic Content Sequencing, Frame 5 — Reglas explicables, Frame packet: 05-rules, How It Works, kinetic-type-beats — Kinetic-Type Beats (+7 more)

### Community 119 - "Center-Outward Expansion"
Cohesion: 0.12
Nodes (15): Assigned storyboard block, Center-Outward Expansion, Critical Constraints, Frame 6 — Hallazgo revisable, Frame packet: 06-finding, grid-card-assemble — Grid / Card Assemble, How It Works, Project inputs (+7 more)

### Community 120 - "Dynamic Content Sequencing"
Cohesion: 0.12
Nodes (15): Accelerating cadence (geometric hold decay), Assigned storyboard block, comparison-split — Comparison Split-Cards, Critical Constraints, Dynamic Content Sequencing, Frame 8 — Cambios gobernados, Frame packet: 08-governance, How It Works (+7 more)

### Community 121 - "Dynamic Content Sequencing"
Cohesion: 0.12
Nodes (15): Accelerating cadence (geometric hold decay), Assigned storyboard block, Critical Constraints, Dynamic Content Sequencing, Frame 9 — Estado real, Frame packet: 09-status, How It Works, Project inputs (+7 more)

### Community 122 - "Generate Report"
Cohesion: 0.13
Nodes (14): 1. Accessibility (A11y), 2. Performance, 3. Theming, 4. Responsive Design, 5. Implementation Integrity (CRITICAL), Audit Health Score, Detailed Findings by Severity, Diagnostic Scan (+6 more)

### Community 123 - "onAnnotDown"
Cohesion: 0.15
Nodes (21): applyPlaceholderDimensions(), beginEditPin(), buildAnnotationsForCapture(), buildPinElement(), cancelEditingPin(), clampPlaceholderSize(), finalizeEditingPin(), initAnnotOverlay() (+13 more)

### Community 124 - "Dc"
Cohesion: 0.17
Nodes (14): C(), Dc(), Mq(), Nq(), Tq(), Ia(), Ja(), pb() (+6 more)

### Community 125 - "doctor.md"
Cohesion: 0.13
Nodes (12): Before you finish, Scope is sovereign, The amplification, The skeleton test, Why it reads flat, Monorepo notes, Opting out of the boot check, Step 1: Run the pass (+4 more)

### Community 126 - "New visual work"
Cohesion: 0.14
Nodes (14): 1. Decide what is already true, 2. Ask what will change the work, 3. Choose the right amount of invention, 4. Commit the world, 5. Record the decision, 6. Build with full commitment, 7. Inspect and finish, Both paths (+6 more)

### Community 127 - "optimize.md"
Cohesion: 0.14
Nodes (13): Animation Performance, Assess Performance Issues, Core Web Vitals Optimization, Cumulative Layout Shift (CLS < 0.1), Interaction to Next Paint (INP < 200ms), Largest Contentful Paint (LCP < 2.5s), Loading Performance, Network Optimization (+5 more)

### Community 128 - "dedup.rs"
Cohesion: 0.22
Nodes (6): accepts_event_after_retention_expires(), dedup(), Deduplicator, DeduplicatorConfig, evicts_oldest_when_capacity_is_reached(), rejects_exact_duplicate_within_retention()

### Community 129 - "Impeccable Asset Producer"
Cohesion: 0.15
Nodes (11): Act on the receipt, Assemble and review, Plan and asset review, Plan, capture, serve, Core Rule, Decision Comps, Impeccable Asset Producer, Input Contract (+3 more)

### Community 130 - "Scan mode (approach C: auto-extract, then confirm descriptive language)"
Cohesion: 0.15
Nodes (13): Component translation rules, Narrative mapping, Scan mode (approach C: auto-extract, then confirm descriptive language), Schema, Step 1: Find the design assets, Step 2: Auto-extract what can be auto-extracted, Step 2b: Stage the frontmatter, Step 3: Ask the user for qualitative language (+5 more)

### Community 131 - "critique.md"
Cohesion: 0.17
Nodes (11): Action Summary, Ask the User, Assessment A: Design Review, Assessment B: Detector + Browser Evidence, Assessment Orchestration, Deliver the Report, Hard Invariants, Persist the Snapshot (+3 more)

### Community 132 - "Simplify the Design"
Cohesion: 0.17
Nodes (11): Assess Current State, Code Simplification, Content Simplification, Document Removed Complexity, Information Architecture, Interaction Simplification, Layout Simplification, Plan Simplification (+3 more)

### Community 133 - "Hardening Dimensions"
Cohesion: 0.17
Nodes (11): Accessibility Resilience, Assess Hardening Needs, Edge Cases & Boundary Conditions, Error Handling, Hardening Dimensions, Input Validation & Sanitization, Internationalization (i18n), Performance Resilience (+3 more)

### Community 135 - "Frame worker — core contract (shared by the narrative video workflows)"
Cohesion: 0.20
Nodes (11): check(), Cross-frame handoffs, Frame constraints, Frame worker — core contract (shared by the narrative video workflows), Frame worker — product-launch delta, Placing candidates (product-launch constraint), Self-check before finishing (you do NOT run the CLI), When a confirmed sketch exists (+3 more)

### Community 136 - "GSAP Timeline"
Cohesion: 0.17
Nodes (11): Controlling Playback, Creating a Timeline, Do Not, GSAP Timeline, Labels, Nesting Timelines, Official GSAP Best practices, Position Parameter (+3 more)

### Community 138 - "Product"
Cohesion: 0.17
Nodes (11): Brand Commitments, Capabilities and Constraints, Evidence on Hand, Operating Context, Platform, Positioning, Product, Product Principles (+3 more)

### Community 139 - "clarify.md"
Cohesion: 0.18
Nodes (10): Actions and navigation, Audit the language, Errors and permissions, Forms, Help and instructional text, Loading, empty, and success states, Rewrite by function, Set the message hierarchy (+2 more)

### Community 140 - "Nielsen's 10 Heuristics"
Cohesion: 0.18
Nodes (11): 10. Help and Documentation, 1. Visibility of System Status, 2. Match Between System and Real World, 3. User Control and Freedom, 4. Consistency and Standards, 5. Error Prevention, 6. Recognition Rather Than Recall, 7. Flexibility and Efficiency of Use (+3 more)

### Community 141 - "Generate Combined Critique Report"
Cohesion: 0.18
Nodes (11): Design Health Score, Design Specificity Verdict, Generate Combined Critique Report, Minor Observations, Overall Impression, Persona Red Flags, Priority Issues, Questions to Consider (+3 more)

### Community 142 - "document.md"
Cohesion: 0.18
Nodes (10): Pitfalls, Seed mode, Step 1: Route through new-work's workshop, Step 2: Write seed DESIGN.md, Step 3: Confirm, Style guidelines, The frontmatter: token schema, The markdown body: eight sections (canonical order) (+2 more)

### Community 143 - "polish.md"
Cohesion: 0.18
Nodes (10): 1. Establish the system, 2. Gather the evidence, 3. Triage, 4. Polish the whole path, 5. Verify and finish, Color, imagery, and icons, Content and code, Flow and hierarchy (+2 more)

### Community 144 - "quieter.md"
Cohesion: 0.18
Nodes (10): Assess Current State, Color Refinement, Composition Refinement, Motion Reduction, Plan Refinement, Refine the Design, Simplification, Verify Quality (+2 more)

### Community 145 - "taq-ingestd/src/main.rs"
Cohesion: 0.09
Nodes (8): main(), parse_dry_run_value(), positive_env(), run(), forward_source_events(), main(), run(), source_task_exit()

### Community 146 - "CORE DIRECTIVE: AWWWARDS-LEVEL DESIGN ENGINEERING"
Cohesion: 0.18
Nodes (10): 1. PYTHON-DRIVEN TRUE RANDOMIZATION (BREAKING THE LOOP), 2. AIDA STRUCTURE & SPACING, 3. HERO ARCHITECTURE & THE 2-LINE IRON RULE, 4. THE GAPLESS BENTO GRID, 5. ADVANCED GSAP MOTION & HOVER PHYSICS, 6. COMPONENT ARSENAL & CREATIVITY, 7. CONTENT, ASSETS & STRICT BANS, 8. MANDATORY PRE-FLIGHT <design_plan> (+2 more)

### Community 147 - "jf"
Cohesion: 0.27
Nodes (11): jf(), lf(), mf(), N(), nf(), O(), of(), tf() (+3 more)

### Community 148 - "STORYBOARD.md"
Cohesion: 0.50
Nodes (3): Diseño, Escenas, Reglas de honestidad

### Community 149 - "Init flow"
Cohesion: 0.20
Nodes (10): Completion gate, Init flow, Step 1: Load current state, Step 2: Explore the project, Step 3: Interview for product truth, Step 4: Write PRODUCT.md, Step 5: Record workflow defaults, Step 6: Wrap up or resume (+2 more)

### Community 150 - "Tween"
Cohesion: 0.27
Nodes (10): _assertThisInitialized(), ic(), ta(), Timeline(), Tween(), w(), x(), xa() (+2 more)

### Community 151 - "L"
Cohesion: 0.24
Nodes (10): A(), B(), F(), G(), fd(), L(), r(), rc() (+2 more)

### Community 152 - "oc"
Cohesion: 0.20
Nodes (10): Db(), J(), nb(), oc(), qc(), Tb(), Ua(), Va() (+2 more)

### Community 153 - "hyperframes.json"
Cohesion: 0.20
Nodes (9): authoringSkill, media, autoProxy, paths, assets, blocks, components, registry (+1 more)

### Community 154 - "Common Cognitive Load Violations"
Cohesion: 0.22
Nodes (9): 1. The Wall of Options, 2. The Memory Bridge, 3. The Hidden Navigation, 4. The Jargon Barrier, 5. The Visual Noise Floor, 6. The Inconsistent Pattern, 7. The Multi-Task Demand, 8. The Context Switch (+1 more)

### Community 155 - "iOS platform"
Cohesion: 0.22
Nodes (9): Color & materials, Components & controls, iOS platform, Layout & structure, Motion, The iOS slop test, Touch targets, Typography (+1 more)

### Community 156 - "Shape"
Cohesion: 0.22
Nodes (8): Cadence, Confirm and stop, Phase 1: Discovery interview, Phase 2: Resolve the design direction, Phase 3: Write the brief, Round 1: purpose, people, and outcome, Round 2: material, behavior, and boundaries, Shape

### Community 157 - "Android platform"
Cohesion: 0.25
Nodes (8): Android platform, Color & theming, Components & motion, Layout & structure, The Android slop test, Touch targets, Typography, Verifying the build

### Community 158 - "colorize.md"
Cohesion: 0.25
Nodes (7): Apply at system scale, Audit before choosing, Choose a strategy, Contrast and perception, Live-mode signature params, Verify, Visitor mode

### Community 159 - "Persona-Based Design Testing"
Cohesion: 0.25
Nodes (8): 1. Impatient Power User: "Alex", 2. Confused First-Timer: "Jordan", 3. Accessibility-Dependent User: "Sam", 4. Deliberate Stress Tester: "Riley", 5. Distracted Mobile User: "Casey", Persona-Based Design Testing, Project-Specific Personas, Selecting Personas

### Community 160 - "Extract Flow"
Cohesion: 0.25
Nodes (7): Extract Flow, Step 1: Discover the Design System, Step 2: Identify Patterns, Step 3: Plan Extraction, Step 4: Extract & Enrich, Step 5: Migrate, Step 6: Document

### Community 161 - "Component Building Principles"
Cohesion: 0.25
Nodes (8): Animate enter states with @starting-style, Buttons must feel responsive, Component Building Principles, Make popovers origin-aware, Never animate from scale(0), Tooltips: skip delay on subsequent hovers, Use blur to mask imperfect transitions, Use CSS transitions over keyframes for interruptible UI

### Community 162 - "anyhow"
Cohesion: 0.39
Nodes (5): positive_u32(), positive_u64(), positive_usize(), Settings, string()

### Community 163 - "na"
Cohesion: 0.25
Nodes (6): Aa(), Ca(), na(), Vb(), wb(), Xb()

### Community 164 - "HyperFrames Composition Project"
Cohesion: 0.25
Nodes (7): Commands, Documentation, HyperFrames Composition Project, Key Rules, Linting — ALWAYS RUN AFTER CHANGES, Project Structure, Skills — USE THESE FIRST

### Community 165 - "HyperFrames Composition Project"
Cohesion: 0.25
Nodes (7): Commands, Documentation, HyperFrames Composition Project, Key Rules, Linting — ALWAYS RUN AFTER CHANGES, Project Structure, Skills — USE THESE FIRST

### Community 166 - "Dynamic Content Sequencing"
Cohesion: 0.25
Nodes (8): Accelerating cadence (geometric hold decay), Critical Constraints, Dynamic Content Sequencing, How It Works, Recipe, See also, Values, Variations

### Community 167 - "Generate Report"
Cohesion: 0.29
Nodes (7): Audit Health Score, Detailed Findings by Severity, Executive Summary, Generate Report, Patterns & Systemic Issues, Platform Conformance Verdict, Positive Findings

### Community 168 - "Cognitive Load Assessment"
Cohesion: 0.29
Nodes (7): Cognitive Load Assessment, Cognitive Load Checklist, Extraneous Load: Bad Design, Germane Load: Learning Effort, Intrinsic Load: The Task Itself, The Working Memory Rule, Three Types of Cognitive Load

### Community 169 - "Impeccable Finish Reviewer"
Cohesion: 0.29
Nodes (6): Checks, in order, Disposition, Impeccable Finish Reviewer, Input Contract, Output Contract, Verdict Pass

### Community 170 - "Impeccable Manual Edit Applier"
Cohesion: 0.29
Nodes (6): Checks, Entry Atomicity, Impeccable Manual Edit Applier, Input Contract, Output Contract, Workflow

### Community 171 - "live-browser-ignores.js"
Cohesion: 0.52
Nodes (6): globToRegex(), matchesScope(), normalizeIgnoreRule(), normalizeIgnoreValue(), pageCandidates(), resolveDetectIgnores()

### Community 172 - "Diagnostic Scan"
Cohesion: 0.33
Nodes (6): 1. Accessibility (VoiceOver / TalkBack), 2. Performance, 3. Appearance & Theming, 4. Platform Conformance (CRITICAL), 5. Adaptivity, Diagnostic Scan

### Community 173 - "Visualize: Direction Comps & Asset Production"
Cohesion: 0.33
Nodes (5): After approval: the comp becomes a spec, Generate three compositional options, One approval point, Plates and provenance, Visualize: Direction Comps & Asset Production

### Community 175 - "fb"
Cohesion: 0.40
Nodes (6): Et(), Context(), Db(), Eb(), fb(), Gw()

### Community 176 - "The Animation Decision Framework"
Cohesion: 0.33
Nodes (6): 1. Should this animate at all?, 2. What is the purpose?, 3. What easing should it use?, 4. How fast should it be?, Perceived performance, The Animation Decision Framework

### Community 177 - "clip-path for Animation"
Cohesion: 0.33
Nodes (6): clip-path for Animation, Comparison sliders, Hold-to-delete pattern, Image reveals on scroll, Tabs with perfect color transitions, The inset shape

### Community 178 - "Performance Rules"
Cohesion: 0.33
Nodes (6): CSS animations beat JS under load, CSS variables are inheritable, Framer Motion hardware acceleration caveat, Only animate transform and opacity, Performance Rules, Use WAAPI for programmatic CSS animations

### Community 179 - "Gesture and Drag Interactions"
Cohesion: 0.33
Nodes (6): Damping at boundaries, Friction instead of hard stops, Gesture and Drag Interactions, Momentum-based dismissal, Multi-touch protection, Pointer capture for drag

### Community 181 - "Ra"
Cohesion: 0.53
Nodes (6): Eb(), Ma(), Na(), Oa(), Ra(), z()

### Community 182 - "Adaptation Strategies"
Cohesion: 0.40
Nodes (5): Adaptation Strategies, Orientation & foldables, Phone → Tablet (iPad / large screens), Platform → platform (iOS ↔ Android), Web → native (porting a website or web app)

### Community 183 - "Impeccable Documenter"
Cohesion: 0.40
Nodes (4): Impeccable Documenter, Input Contract, Output Contract, Workflow

### Community 184 - "Region map"
Cohesion: 0.40
Nodes (4): Containment, Painted material, Region map, What varies independently

### Community 186 - "The Sonner Principles (Building Loved Components)"
Cohesion: 0.40
Nodes (5): Asymmetric enter/exit timing, Cohesion matters, Review your work the next day, The opacity + height combination, The Sonner Principles (Building Loved Components)

### Community 187 - "site/README.md"
Cohesion: 0.40
Nodes (3): Landing de TaQ, Vista previa local, GSAP vendor files

### Community 188 - "la"
Cohesion: 0.60
Nodes (5): Animation(), Da(), la(), Ua(), Va()

### Community 189 - "BRIEF.md"
Cohesion: 0.40
Nodes (4): Assets, Customizations, Intent, Notes

### Community 190 - "TaQ — Video promocional"
Cohesion: 0.17
Nodes (11): Assets, Capturar fotogramas, Colores y tipografía, Editar, Estructura, Lint / Validación, Previsualizar, Renderizar MP4 (+3 more)

### Community 191 - "Heuristics Scoring Guide"
Cohesion: 0.50
Nodes (4): Heuristics Scoring Guide, Issue Severity (P0–P3), Reference Material, Score Summary

### Community 192 - "Operate"
Cohesion: 0.50
Nodes (3): Comps, Directions, Operate

### Community 193 - "Persuade and Experience"
Cohesion: 0.50
Nodes (3): Comps, Directions, Persuade and Experience

### Community 194 - "Read"
Cohesion: 0.50
Nodes (3): Comps, Directions, Read

### Community 195 - "Command guidance"
Cohesion: 0.50
Nodes (3): Command guidance, No-argument routing: the context-aware menu, Workflow questions

### Community 196 - "Core Philosophy"
Cohesion: 0.50
Nodes (4): Beauty is leverage, Core Philosophy, Taste is trained, not innate, Unseen details compound

### Community 197 - "Debugging Animations"
Cohesion: 0.50
Nodes (4): Debugging Animations, Frame-by-frame inspection, Slow motion testing, Test on real devices

### Community 198 - "Relación con el TaQ anterior"
Cohesion: 0.50
Nodes (4): Decisión de producto, Qué conviene conservar como referencia, Qué no portar directamente, Relación con el TaQ anterior

## Knowledge Gaps
- **1162 isolated node(s):** `ContextMatch`, `RULE_DATASET_FORMAT_VERSION`, `BLUESKY_JETSTREAM_URL`, `RSS_SEEN_ID_LIMIT`, `RSS_MAX_RESPONSE_BYTES` (+1157 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 1389 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **28 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `y()` connect `y` to `Frame worker — core contract (shared by the narrative video workflows)`, `modern-screenshot.umd.js`, `s`, `Performance Rules`, `gsap.min.js`, `Center-Outward Expansion`, `GSAP Plugins`, `Tween`?**
  _High betweenness centrality (0.165) - this node is a cross-community bridge._
- **Are the 7 inferred relationships involving `initGlobalBar()` (e.g. with `hideAgentPollTooltip()` and `onDetectMessage()`) actually correct?**
  _`initGlobalBar()` has 7 INFERRED edges - model-reasoned connections that need verification._
- **What connects `ContextMatch`, `RULE_DATASET_FORMAT_VERSION`, `BLUESKY_JETSTREAM_URL` to the rest of the system?**
  _1162 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `taq-state/src/lib.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.0946969696969697 - nodes in this community are weakly interconnected._
- **Why does `check()` connect `Frame worker — core contract (shared by the narrative video workflows)` to `setLiveState`?**
  _High betweenness centrality (0.133) - this node is a cross-community bridge._
- **Should `pipeline.rs` be split into smaller, more focused modules?**
  _Cohesion score 0.11827956989247312 - nodes in this community are weakly interconnected._
- **Why does `hasTextRows()` connect `setLiveState` to `live-browser.js`, `Frame worker — core contract (shared by the narrative video workflows)`?**
  _High betweenness centrality (0.133) - this node is a cross-community bridge._