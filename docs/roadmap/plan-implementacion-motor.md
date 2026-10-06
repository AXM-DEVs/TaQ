# Plan de implementación del motor TaQ

## Diagnóstico ejecutivo

El estado comprobado de TaQ es una canalización de ingesta y proyección: fuentes públicas → `NativeEvent` → normalización → deduplicación exacta en memoria → Redpanda → proyección de ClickHouse. `taq-loadgen` valida un camino sintético separado. El repositorio contiene contratos para `Signal` y `Action`, pero no implementa contextualización, inferencia, coordinación ni aprendizaje. Por tanto, aún no cumple la promesa completa de un motor de inteligencia contextual.

Este plan define tres puertas de salida:

- **L0 — Fase 1 verificable:** ingestión real robusta, observable y reproducible de fuentes a ClickHouse.
- **L1 — piloto privado:** usuarios conocidos operan un flujo de lista vigilada a hallazgo explicable y acción controlada.
- **L2 — público controlado:** las cinco etapas mínimas existen, están evaluadas y pueden operarse con controles de seguridad, privacidad y recuperación.

Si en L2 falta alguna de las cinco etapas, TaQ se presenta como **monitoreo OSINT**, no como motor completo. No se fija calendario ni se prometen SLAs o 50.000 eventos/s antes de medirlos.

## F0 — Verificación del repositorio (completada)

La verificación se hizo en el árbol de trabajo existente. Hay numerosos cambios locales modificados y archivos nuevos en varias áreas —incluidos los crates de Fase 1, infraestructura, ADR y `graphify-out`—; se conservaron sin modificación durante esta revisión. No existe `docs/roadmap/` ni un plan equivalente previo.

Graphify está instalado como skill y `graphify-out/graph.json` existe. Se intentó `graphify query`; el lanzador `uv` falla en Windows con `uv trampoline failed to canonicalize script path`. Se inspeccionaron directamente el contexto, README, ADR, CI, Compose y fuentes; el grafo queda pendiente de actualización.

### Hipótesis y veredictos

| ID | Veredicto y evidencia |
| --- | --- |
| H1 | **Confirmada.** Existen conectores Wikimedia, Bluesky y RSS. No hay crates GDELT ni CT Logs en el workspace (`crates/taq-connector-*`). |
| H2 | **Confirmada.** `taq-ingest/src/dedup.rs` implementa dedupe exacta en memoria con capacidad y retención; ADR 0002 dice que Redpanda sigue siendo durable y que los consumidores toleran repetición. |
| H3 | **Confirmada.** RSS asigna `occurred_at_ms` con el reloj de lectura y conserva `pub_date` en payload (`crates/taq-connector-rss/src/lib.rs:58,76`). |
| H4 | **Confirmada.** Bluesky usa `commit.rkey` como `native_id` (`crates/taq-connector-bluesky/src/lib.rs:75-81`); hay que validar su unicidad con DID, colección y tipo de operación. |
| H5 | **Parcialmente resuelta.** Hay retry exponencial con jitter, cap de 60 segundos y clasificación de HTTP 4xx terminales salvo 408/425/429. Aún no hay límites de intentos ni métricas de lag/reconexión por fuente. RSS reintenta dentro de su polling. |
| H6 | **Parcialmente resuelta.** `taq-ingestd` cancela productores, espera reenviadores/pipeline, aplica timeout de apagado de 60 segundos y flush Kafka de hasta 10 segundos. No hay persistencia de cursores; SIGTERM en Windows/Linux aún no está cubierto con la misma integración portable que Ctrl+C. |
| H7 | **Parcialmente resuelta.** Si fallan la publicación principal y DLQ, `run_pipeline` retorna error, que el daemon convierte en salida no-cero. No existe spool local y el reinicio depende del supervisor externo. El contrato DLQ incluye `schema_version: 1`, y `PipelineStats` resume recibidos = publicados + DLQ + duplicados + fallidos en logs de apagado/error. |
| H8 | **Confirmada.** Compose prepara PostgreSQL/pgvector y Redis; los crates Rust no los usan en el flujo actual (ADR 0001 y fuentes del workspace). |
| H9 | **Confirmada.** Hay configuración de Collector, Prometheus y Grafana; los procesos Rust no exportan métricas OTLP. |
| H10 | **Confirmada.** El job `e2e` de `.github/workflows/ci.yml` publica 500 eventos de `taq-loadgen` directamente y valida ClickHouse. No atraviesa `taq-ingestd` ni conectores. |
| H11 | **Confirmada.** El objetivo de 50.000 eventos/s no está demostrado; el ADR 0001 lo trata como objetivo arquitectónico. |
| H12 | **Confirmada para desarrollo.** Compose/`.env.example` contienen credenciales de ejemplo (`taq_dev`); los puertos locales se enlazan a loopback. No existe despliegue productivo verificado. |

### Comandos comprobados

Pasaron en este árbol de trabajo:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
docker compose --env-file .env.example -f deploy/docker-compose.yml config --quiet
```

`cargo test --workspace` ejecutó 11 tests unitarios (4 de `taq-core`, 6 de `taq-ingest` y 1 de `taq-loadgen`); los crates de conectores y `taq-ingestd` reportaron cero tests. Compose imprimió advertencias de acceso a `C:\Users\axmce\.docker\config.json`, pero la validación terminó con código 0. También se ejecutó `cargo check --workspace` en la revisión previa y pasó.

El job E2E existente ejecuta `cargo run --release -p taq-loadgen -- --rate 100 --duration 5 --entities 20 --duplicate-ratio 0.01`, luego consulta `taq.events` y comprueba conteo, IDs únicos y p99 menor a 2.000 ms. **No se ejecutó aquí** porque levanta una pila Docker; su script se inspeccionó en `.github/workflows/ci.yml`.

### Progreso tras aprobación

F1 está implementada en el árbol de trabajo para contratos Rust y conectores: `schema_version` 2 agrega calidad del timestamp; RSS usa `pub_date` válido y estable, luego marca fallback a captura; su identidad usa GUID, link o hash del contenido; Bluesky incluye DID, colección, rkey y operación. `deploy/clickhouse/migrations/0002_event_timestamp_quality.sql` y el esquema de instalación nueva están listos. Format, Clippy, `cargo test --workspace` y `cargo check --workspace` pasan; pruebas unitarias cubren RFC 2822/RFC 3339, zona horaria/fallback/futuro, identidad RSS/Bluesky y compatibilidad del contrato.

F1 sigue pendiente de aplicar y comprobar la migración en un ClickHouse desechable, y de un E2E con mensajes nuevos y antiguos. Ningún volumen del usuario se modificó. G1 no se considera alcanzado.

F2 implementa entrega explícita y recuperación. `taq-dlq-replay` valida el contrato versionado, usa dry-run por defecto y solo confirma el offset después del ACK de publicación; el contrato es at-least-once. `ReplacingMergeTree` más `FINAL` permite lecturas lógicas sin duplicar eventos reintentados con la misma clave `(ingested_at_ms, id)`. La migración 0003 separa preparación, validación y activación, conserva la tabla previa y define rollback antes de reanudar consumo. ClickHouse `system.dead_letter_queue` queda habilitada para fallos de parseo y se documenta aparte de la DLQ de aplicación. ADR 0007 registra estas decisiones.

CI añade E2E para migración, replay con grupos independientes, SIGTERM/reinicio, mensajes malformados y caída/recuperación del broker. En Docker Engine 29.8.1 local, el stack base alcanzó readiness y la migración 0003 pasó sobre ClickHouse 26.9.2.8 (`ReplacingMergeTree`, una fila lógica para el fixture duplicado). El replay no quedó validado: el binario nativo Windows no pudo conectar con el puerto de Redpanda publicado por Docker Desktop. La compilación del perfil `app` también quedó bloqueada por errores de escritura de BuildKit en Debian. Docker Desktop ahora presenta errores EXT4/JBD2 y el daemon sigue inaccesible tras reinicio; se preservaron el VHDX y los volúmenes. Detalle en `docs/operations/validation/f12-local-2026-10-04.md`.

La DLQ mantiene `schema_version: 1`; mensajes anteriores sin versión se leen como v1. F2 no implica que TaQ sea producto público listo: todavía requiere ejecución CI verde con Docker, supervisión/reinicio en el despliegue, alertas/SLO, revisión de seguridad y privacidad, validación de la migración sobre una copia representativa y definición de retención/capacidad operativa.

## Alcance objetivo y exclusiones

El objetivo L2 de este plan es un servicio de OSINT defensivo por invitación, para una cohorte acotada, con lista vigilada versionada, hallazgos trazables, acciones con aprobación y feedback que propone cambios versionados revisados por una persona.

Quedan fuera salvo decisión posterior: aprendizaje en línea autónomo, efectos externos sin aprobación, otros verticales, facturación self-serve, SSO empresarial, multi-región, modelos generativos en la ruta de decisión y compromiso contractual de 50.000 eventos/s. GDELT y CT Logs se agregan solo si el caso de uso y los términos justifican su prioridad.

## F1 — Contrato de evento, identidad y tiempo

**Objetivo:** hacer reproducibles la identidad, el tiempo y la procedencia antes de persistir más estado derivado.

**Entregables:** definir ID determinista con fuente e ID nativo completo; distinguir hora del hecho, hora de captura/ingesta y calidad del timestamp; resolver la clave Bluesky con DID + colección + rkey y operaciones de create/update/delete; usar `pub_date` RSS en UTC y marcar fallback ante fecha ausente, inválida o futura; definir ID RSS cuando falte GUID; incluir procedencia/hash cuando corresponda. Si cambia el contrato JSON, aumentar `schema_version` y planear migración expand/contract de ClickHouse.

**Áreas:** `taq-core`, `taq-ingest`, conectores RSS/Bluesky, `deploy/clickhouse/init.sql`, ADR 0003/0004 y posiblemente ADR 0002.

**Decisiones:** formato y canonicalización del ID; tolerancia a timestamps tardíos; política de fallback; tratamiento del histórico sintético; semántica de delete/update.

**Riesgos y mitigaciones:** cambio de claves causa colisiones o rompe dedupe/histórico; cubrir con fixtures, pruebas de contrato y migración compatible antes de cambiar consumidores.

**Aceptación:** fixtures para GUID/fecha ausentes, zona horaria, fecha futura, mismo rkey en repositorios/colecciones y operaciones distintas; ID y timestamp estables al reprocesar; migración probada hacia delante y con procedimiento de rollback.

## F2 — Entrega, recuperación y apagado confiables

**Objetivo:** que cada evento aceptado termine publicado, en DLQ o contabilizado; que fallos críticos no dejen un demonio aparentemente sano pero detenido.

**Entregables:** documentar entrega at-least-once y consumo idempotente; retry con jitter, límites y clasificación transitoria/permanente; supervisor para pipeline y conectores con estado de salud; DLQ versionada y herramienta de replay; política explícita para caída simultánea de broker y DLQ (evaluar spool acotado en disco frente a fallo rápido y reinicio); apagado con timeout que detenga productores, drene colas y espere publicaciones pendientes; visibilidad de mensajes rechazados por ClickHouse.

**Áreas:** `taq-ingest/src/pipeline.rs`, `taq-ingest/src/source.rs`, `taq-ingestd/src/main.rs`, DDL/Compose y ADR 0002/0003.

**Decisiones:** dedupe persistente o en memoria (según instancias); spool o fallo explícito; capacidad y política de disco; errores que terminan una fuente vs todo el proceso.

**Riesgos y mitigaciones:** spool agotado y reinicios en bucle; límites configurables, backoff/jitter, alarma y estado no-ready.

**Aceptación:** pruebas de broker/DLQ caídos, broker lento, SIGTERM bajo carga, restart a mitad de envío, pipeline panicking; invariante auditable: recibidos = publicados + DLQ + descartes explícitamente contados. Ninguna tarea crítica termina sin reinicio o salida no-cero visible.

## F3 — Fuentes configurables, cursores y valor del vertical

**Objetivo:** operar fuentes sin recompilar, dentro de sus límites y con un comportamiento conocido tras desconexión.

**Entregables:** configuración validada al inicio para endpoint, intervalo, límites de tasa, timeouts, retry y User-Agent; cursores/replay por fuente según soporte real; RSS con ETag/Last-Modified e IDs vistos si el feed lo permite; confirmar soporte de `Last-Event-ID` en Wikimedia y cursor Jetstream en documentación vigente; errores de parseo con motivo medible/DLQ; matriz de términos, atribución, uso comercial, redistribución y límites; medición del volumen y utilidad por fuente.

**Áreas:** conectores, `taq-ingestd`, módulo de configuración, almacén de cursores (Postgres/Redis solo tras justificarlo), documentación de fuentes.

**Decisiones:** store de cursores; priorizar CT Logs si lookalikes son centrales; GDELT después; mantener Wikimedia/Bluesky solo si aportan señal demostrable.

**Riesgos y mitigaciones:** 429/bloqueos y huecos tras cortes; rate limits, backoff, identificación de cliente, lag por fuente y replay comprobado.

**Aceptación:** servidores simulados devuelven 429, 5xx, cortes, retraso y payload inválido; reinicio recupera dentro de la ventana garantizada sin duplicación no idempotente; matriz de términos revisada antes de activar uso comercial.

## F4 — E2E real y observabilidad (G1 / L0)

**Objetivo:** probar `taq-ingestd → Redpanda → ClickHouse`, no únicamente el generador de Fase 0.

**Entregables:** E2E determinista con fuentes simuladas; conteos, ID, timestamps, dedupe y DLQ verificados; métricas Prometheus/OTel desde Rust para eventos, errores, DLQ, profundidad de cola, lag y latencias; health/readiness, logs estructurados, dashboard mínimo y alertas; medir carga inicial y documentar hardware.

**Áreas:** CI, Compose de pruebas, crates Rust, configuraciones Prometheus/Grafana/Collector.

**Decisiones:** qué corre por PR y qué nocturno; objetivos L0 de latencia y volumen (no inventar umbrales, fijarlos con datos).

**Riesgos y mitigaciones:** E2E intermitente; esperar por condiciones y usar fuentes simuladas reproducibles.

**Aceptación G1:** E2E falla si se detiene el binario o se altera un campo; métricas visibles; checklist L0 completo; fallos críticos de F2 probados en CI.

**No-Go L0:** pérdida silenciosa, IDs/tiempos inestables, reinicio del pipeline no observable o ausencia de un recorrido real verificable.

## F5 — Contextualización y entidades vigiladas

**Objetivo:** vincular eventos a marcas, dominios y organizaciones vigiladas con evidencia reproducible.

**Entregables:** modelo tipado de entidad, alias, relaciones y versión; resolución exacta/normalizada antes de considerar fuzzy; normalización IDNA/punycode para dominios; lista versionada; enrichment asíncrono con caché y presupuesto; eventos contextualizados en flujo/tabla versionados con `context_version`; replay reproducible al cambiar listas.

**Áreas:** nueva etapa del pipeline, PostgreSQL/migraciones, posiblemente Redis y ClickHouse.

**Decisiones:** entidades de L1; pgvector solo si hay requisito medido; histórico y reproceso; tenant único en piloto o multi-tenant futuro (decidir antes de congelar esquema).

**Riesgos y mitigaciones:** homónimos y coste de enriquecimiento; alias negativos, gold set, caché, límites y medición.

**Aceptación:** gold set etiquetado evento-entidad; medir precisión/recall contra umbral fijado por producto; reproceso idempotente y migraciones comprobadas.

## F6 — Inferencia explicable y hallazgos

**Objetivo:** producir hallazgos útiles que se puedan justificar y evaluar.

**Entregables:** comenzar con reglas versionadas; `Finding` con tipo, severidad, score, entidades, eventos/URLs/hashes de evidencia, regla/versión y explicación; umbrales por lista, agrupación y supresión de repetidos; estados nuevo/confirmado/falso positivo/descartado; arnés offline y gold set congelado. ML solo si métricas muestran que reglas no alcanzan.

**Áreas:** nueva etapa/servicio de inferencia, almacenamiento de hallazgos, formato de reglas.

**Decisiones:** casos centrales (menciones, suplantación, lookalikes, noticias); coste tolerable de falsos positivos/negativos; unidad de precisión.

**Riesgos y mitigaciones:** fatiga de alertas y evidencia no reproducible; agrupación, umbrales calibrados, snapshot/hash y trazabilidad.

**Aceptación:** evaluación reproducible de precisión/recall por regla y falsos positivos por volumen; cada hallazgo traza a evidencia; regresiones bloqueadas según umbrales que apruebe el dueño.

## F7 — Coordinación y acciones auditables

**Objetivo:** convertir hallazgos en acciones controladas sin efectos externos no autorizados.

**Entregables:** catálogo de señal→acción; políticas versionadas; estado pendiente/aprobada/rechazada/expirada; acciones externas requieren aprobación humana; claves idempotentes, outbox, reintentos seguros y auditoría; modo sombra y kill switch.

**Áreas:** servicio de acciones, PostgreSQL/outbox/auditoría e integraciones seleccionadas.

**Decisiones:** canales del piloto, aprobadores y notificaciones que pueden automatizarse.

**Riesgos y mitigaciones:** duplicados o acción equivocada; modo sombra, aprobación, idempotencia y auditoría.

**Aceptación:** caída durante una acción se recupera sin duplicarla; permisos de aprobación probados; auditoría reconstruye cada decisión; piloto en sombra antes de ejecutar.

## F8 — Feedback y aprendizaje gobernado

**Objetivo:** que el feedback mejore reglas/umbrales sin degradar silenciosamente resultados.

**Entregables:** feedback con autor/motivo y versión de finding; etiquetas/snapshots inmutables; candidato→evaluación offline (gold + holdout temporal)→aprobación→sombra/canary→rollback; versionado de reglas/datasets/modelos; muestreo de eventos no alertados para recall; monitoreo de drift.

**Áreas:** almacenamiento de feedback, evaluación, registro de versiones y API/CLI.

**Decisiones:** L2 aprende con reglas versionadas revisadas, no online; tolerancia a regresión; aislamiento de feedback por cliente.

**Riesgos y mitigaciones:** feedback sesgado/malicioso y sobreajuste; autoría, revisión, holdout rotativo y límites de promoción.

**Aceptación:** cambio candidato bloqueado automáticamente si incumple guardrails; rollback ensayado; ciclo completo de promoción/rechazo con evidencia.

## F9 — Interfaz de producto e identidad de usuarios

**Objetivo:** permitir operar el piloto sin tocar infraestructura.

**Entregables:** API HTTP JSON/OpenAPI y CLI sobre API; endpoints para listas, findings/evidencia, acciones/aprobaciones, feedback y webhooks; API keys con hash/scopes al inicio, RBAC mínimo, cuotas, rate limits, paginación, versionado e idempotency keys. UI solo si el piloto demuestra que hace falta.

**Áreas:** servicio/API nuevo, CLI, gateway/TLS, CI contract tests.

**Decisiones:** managed service, API de datos, autoalojado o consultoría con herramienta; multi-tenant solo si el modelo comercial lo requiere.

**Riesgos y mitigaciones:** fuga entre tenants y abuso; aislamiento lógico, pruebas negativas, cuotas y auditoría.

**Aceptación L1:** usuario del piloto completa lista→hallazgo→acción→feedback mediante API/CLI; autorización por rol; aislamiento por tenant probado si aplica.

## F10 — Seguridad, privacidad y uso permitido

**Objetivo:** tratar datos públicos con controles compatibles con el uso y jurisdicción previstos.

**Entregables:** clasificación/minimización de datos personales (autores, handles, DID, texto); política de retención por clase y borrado propagado a topics/almacenes/backups; TLS y cifrado en reposo; secretos externos al repo/Compose y rotación; mínimo privilegio/red segmentada; procedencia por evento; matriz de términos aplicada; threat model, escaneo de dependencias/secretos, SBOM y revisión legal.

**Áreas:** contratos/esquemas, CI, Compose/IaC, almacenes y controles de acceso.

**Decisiones:** entidad responsable, jurisdicción, base legal, retención, responsable de incidentes y si datos personales se procesan deliberadamente; validar con asesoría legal competente.

**Riesgos y mitigaciones:** “público” no significa libre de obligaciones; inventario de copias y prueba integral de borrado/restauración.

**Aceptación:** revisión de seguridad/privacidad con evidencias y excepciones aprobadas; secretos de desarrollo bloqueados en despliegue público; borrado verificado de punta a punta.

## F11 — Operación de producción

**Objetivo:** operar cambios y fallos sin intervención heroica.

**Entregables:** plataforma distinta a Compose de desarrollo; redundancia apropiada en broker/almacenes/ingesta; migraciones expand/contract; backups automáticos y restauración ensayada; métricas/logs/trazas, SLIs y alertas con destinatario; runbooks y simulacros; artefactos inmutables, upgrades escalonados y rollback.

**Áreas:** IaC/despliegue, CI/CD, Redpanda, ClickHouse, Postgres/Redis según uso, observabilidad.

**Decisiones:** gestionado vs autoalojado; región; RTO/RPO, disponibilidad y guardia fijados por el dueño; topología de instancias y dedupe compartida.

**Riesgos y mitigaciones:** complejidad/costo y estado compartido; adoptar solo la redundancia requerida por objetivos y medir costos antes del piloto.

**Aceptación:** restore en entorno limpio, caída de nodo, upgrade y rollback dentro de RTO/RPO aprobados; alertas llegan a persona responsable.

## F12 — Calificación para lanzamiento (G2/G3)

**Objetivo:** decidir el alcance de piloto o lanzamiento público con evidencia reproducible.

**Entregables:** E2E integral; fallos de broker/almacén/red/disco y fuentes; carga sostenida y benchmark reproducible (hardware/versión declarados); evaluación de precisión con gold set y muestreo ciego; piloto con cohorte/duración aprobadas; revisión de seguridad, restauración, rollback y checklist legal.

**Áreas:** staging, arnés de carga/caos, reportes archivados y responsables Go/No-Go.

**Decisiones:** volumen/duración, cohorte, umbrales de precisión, SLIs, presupuesto y revisión externa.

**Riesgos y mitigaciones:** datos sintéticos no representativos y resultados no reproducibles; usar muestras reales permitidas, scripts versionados y hardware documentado.

**Aceptación G2:** piloto privado cumple criterios fijados y no tiene fallos críticos abiertos. **Aceptación G3:** informe de cada criterio medido; cada etapa del motor presente para la promesa comercial. No publicar capacidad o SLA no medido.

**No-Go automático G3:** pérdida silenciosa, fuga entre cuentas, credenciales de desarrollo, restauración no probada o acción externa sin aprobación.

## Dependencias y puertas

```text
F0 (hecha) → F1 → F2 → F3 → F4/G1
                                  ↓
                     F5 → F6 → F7 → F8
                                  ↓
                             F9 + F10
                                  ↓
                           F11 → F12
```

F9/F10 pueden empezar en diseño antes de terminar F8, pero su esquema y controles de tenant dependen de D1/D2. F5 no debe congelar persistencia multi-tenant hasta decidir D2. F1 precede a almacenamiento de contexto y hallazgos porque cambios posteriores en ID/tiempo requieren migrar datos.

- **G1:** L0 pasa E2E real, pruebas de recuperación y observabilidad de Fase 1.
- **G2:** L1 pasa evaluación de findings, seguridad/privacidad básica, API operable, acciones aprobadas y restauración ensayada.
- **G3:** L2 pasa todas las etapas mínimas, operación y evaluación; promesa comercial coincide exactamente con evidencia.

## Decisiones del dueño del producto

1. **D1 — Qué se vende:** servicio gestionado (recomendado para piloto), API de datos, autoalojado o consultoría con herramienta.
2. **D2 — Multi-tenant:** tenant único en L1; añadir aislamiento multi-tenant antes de congelar esquema si se prevén varios clientes L2.
3. **D3 — Promesa:** “motor” solo con las cinco etapas; si no, limitar mensaje a monitoreo OSINT.
4. **D4 — Dedupe:** event_id determinista e idempotencia en consumidores; dedupe persistente/compartida depende de operar varias instancias y puede requerir revisar ADR 0002.
5. **D5 — Umbrales:** disponibilidad, latencia, precisión/recall, retención, RTO/RPO y volumen; medir antes de comprometer.
6. **D6 — Fuentes:** CT Logs primero si dominios lookalike son el caso central; GDELT después. Priorizar fuentes por valor medido y términos.
7. **D7 — Acciones:** efectos externos siempre con aprobación humana en L2 inicial.
8. **D8 — Operación:** managed para servicios con estado si presupuesto/ubicación de datos lo permite.
9. **D9 — 50.000 ev/s:** objetivo experimental, no requisito/claim hasta benchmark reproducible.
10. **D10 — Piloto/legal:** entidad responsable, jurisdicción, quién participa, tamaño y duración.

Supuestos iniciales que deben confirmarse: una persona desarrolla/opera; un vertical y una región; piloto privado con usuarios conocidos; fuentes públicas gratuitas; reglas antes que ML; conservar Redpanda/ClickHouse/Postgres/Redis salvo que F0/F1 justifiquen ADR nuevo. No se fijan números sin volumen real, presupuesto y perfil de clientes.

## Secuencia de validación disponible

Desde PowerShell, comandos ya definidos por repo:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check --workspace
docker compose --env-file .env.example -f deploy/docker-compose.yml config --quiet
```

Con entorno local disponible, `.env` creado desde `.env.example`:

```powershell
just up
just loadgen 5000 30
just verify
```

El último bloque valida Fase 0 sintética, no la ruta completa de Fase 1. El E2E actual en GitHub Actions puede ejecutarse revisando el job `e2e` de `.github/workflows/ci.yml`; solo después de añadir E2E con `taq-ingestd` se considera evidencia de L0. No ejecutar `docker compose down -v` salvo en un entorno desechable: borra los volúmenes.

## Inconsistencias y revisiones documentales

- `CONTEXTO_PROYECTO.md` y README deben seguir distinguiendo carga sintética Fase 0 de conectores Fase 1.
- ADR 0002/0003 reconocen dedupe volátil y fallo explícito si broker+DLQ fallan; la implementación todavía carece del supervisor descrito como necesario.
- ADR 0003 determina ID `source + native_id`; los IDs incompletos de RSS/Bluesky requieren decidir si se revisa ADR y `schema_version`.
- ADR 0005 mide append Redpanda→ClickHouse; no equivale a latencia desde ocurrencia externa.
- Infra observability está desplegada en Compose, pero no hay exportación desde Rust.
- “E2E” en CI actualmente valida loadgen; renombrar o ampliar para evitar afirmar que cubre ingestd.
- Redis/Postgres/pgvector están provisionados, no forman parte del flujo hasta F5/F3.
- 50.000 ev/s no es rendimiento medido.
- Credenciales Compose son solo de ejemplo/desarrollo; despliegue público debe rechazarlas.

## Orden recomendado

Implementar primero **F1 → F2 → F3 → F4**. Así se congela identidad/tiempo, se evita construir evidencia sobre claves inestables y se obtiene un Go de L0 sobre la ruta real. Después iniciar F5 y F6 para el valor principal del vertical. Mantener F7/F8 en modo controlado y cerrar decisiones D1–D3/D10 antes de diseño de producto público. Ninguna fase posterior debe considerarse lista solo por compilar: sus criterios Done exigen evidencia de comportamiento y operación.

## Avance de implementación tras F2 (pendiente de pruebas finales)

El usuario autorizó avanzar por F3–F12 y agrupar la suite de tests/validación al final. El código y documentación que siguen son **implementación pendiente de validar**, no evidencia de aceptación:

- **F3 — en implementación:** endpoints y límites configurables/validados; RSS con validators/seen IDs; Wikimedia SSE y Bluesky Jetstream con cursor; PostgreSQL guarda cursores e identidades vistas, que solo avanzan tras ACK Kafka. Términos fuente documentados; Wikimedia canaries se descartan y su cursor se checkpointa; Bluesky migra al endpoint v2 con cursor secuencial y lookback predeterminado de 36 h; BBC queda como ejemplo local y requiere permiso comercial. Falta probar cortes/429/restart/ventana real y verificar el comportamiento de cursor con fixtures.
- **F4 — parcial:** `taq-api` sirve live/readiness y consume `events.raw`; hay métricas OTLP de procesamiento, resultados y reintentos por fuente, profundidad p95 de colas, hallazgos, fallos y lag muestreado, con paneles iniciales de Grafana. Falta ejecutar y confirmar visibilidad en runtime, E2E `taq-ingestd → Redpanda → ClickHouse` con fuentes fixture, dashboards/alertas y baseline medido.
- **F5 — parcial:** migración multi-tenant, versiones de watchlist, alias exactos, normalización IDNA y proyección contextualizada idempotente. Falta gold set, métricas reales de matching, enrichment acotado y borrado.
- **F6 — parcial:** reglas versionadas, matching determinista y hallazgos explicables con IDs/URL/hash. F8 añade un runner que ejecuta el matcher sobre snapshots inmutables y calcula métricas para gold/holdout; aún no se compiló ni probó y no existe gold set aprobado, por lo que no declarar precisión/recall como evidencia.
- **F7 — parcial:** políticas versionadas, solicitud/aprobación/expiración, outbox/auditoría transaccional y separación del solicitante/aprobador. El dispatcher interno omite acciones no vigentes al reclamarlas y expira/aparta del outbox las aprobaciones vencidas; no hay executor externo. Falta validar en runtime carreras, recuperación, kill switch y modo sombra; no se ejecutan efectos externos.
- **F8 — parcial, runner compilado y con pruebas unitarias:** feedback conserva autor, snapshot y versiones. `rules:evaluate` toma únicamente versión/dataset; TaQ verifica BLAKE3, tenant, entidad y versión de watchlist, ejecuta el matching sobre eventos/aliases del snapshot y guarda métricas de gold y temporal holdout con la versión de guardrails, sin contadores del cliente. El hash ya es independiente del orden y la respuesta informa las diferencias holdout menos gold. Tres pruebas unitarias cubren hash normalizado, métricas/confusión con alias negativo y dominio/subdominio. No se probó registro/evaluación contra PostgreSQL ni existe gold set aprobado; tampoco hay muestreo automatizado de no-alertas, gate de drift, canary o rollback probado.
- **F9 — parcial:** `taq-api`, OpenAPI y `taqctl` cubren rutas iniciales; claves guardan hash/scopes/principal/expiración/revocación/cuota; queries filtran por tenant. Falta prueba negativa de aislamiento, TLS real, webhooks y completar idempotencia/paginación.
- **F10 — parcial:** hash de claves, metadata minimizada en poison messages, binding loopback, threat model y términos fuente. `cargo audit` local pasó sobre 291 dependencias tras actualizar `tokio-tungstenite` 0.23.1, eliminando cuatro advisories de `rustls-webpki` 0.102.8. SBOM CycloneDX JSON 1.5 generado para 11 paquetes con SHA-256 en `sbom/f12-local/`; el generador advirtió que `UNLICENSED` no es una expresión SPDX válida. Gitleaks 8.30.1 quedó instalado el 6-oct y su binario se verificó; el escaneo y CI siguen pendientes. Falta retención/borrado propagado, TLS/cifrado real, secret manager, mínimo privilegio, firma/procedencia SBOM, threat review externa y resolución legal/privacy/licencias.
- **F11 — parcial:** Dockerfiles multi-stage, runbook y regla Prometheus local para dependencia sin scrape durante cinco minutos. El stack base inicia y reporta healthy localmente; la alerta se carga en Prometheus, pero no se validó firing/entrega y no existe Alertmanager, canal o responsable. La API comparte un pool PostgreSQL configurable (predeterminado 16 conexiones, límite 64) con timeout de espera y creación; tareas de fondo reintentan tras errores. Se corrigió el vencimiento de outbox reclamados y la atomicidad de la transición terminal fallida junto con auditoría. Falta medir concurrencia/capacidad y completar pruebas con fallos de broker en runtime. También faltan imágenes inmutables, despliegue de producción, restore ensayado, SLI medidos y failure drills.
- **F12 — validación local parcial:** 40 pruebas del workspace pasan en Windows con `cargo test --workspace --offline`; también pasan formato, Clippy (`-D warnings`), `cargo check --workspace --locked`, Compose config y `cargo audit`. Stack Compose base y migración ClickHouse 0003 pasan localmente; el 6-oct se recuperó Docker y pasó el readiness del stack normal con volúmenes conservados. El build/arranque del perfil de aplicación sigue sin validar por bibliotecas truncadas en la imagen Debian de Docker Desktop; los volúmenes no se tocaron. Replay Windows e integración CI Linux siguen pendientes. El reporte local y addenda están en `docs/operations/validation/f12-local-2026-10-04.md`. Gitleaks y ejecución CI siguen pendientes; el pack release conserva `PENDIENTE` y `NO-GO`.

La migración ClickHouse 0004 usa el ID estable como clave de proyección para deduplicar restart/replay aunque cambie `ingested_at_ms`; está escrita, no aplicada en un volumen. F3/F4 no satisfacen L0/G1 hasta validar la ventana/cambio de cursor Bluesky, probar migraciones, CI y E2E. F10/F11 y decisiones legales/operativas abiertas bloquean G2/G3. La validación final se ejecutará después de terminar la implementación restante.

### Evidencia adicional (2026-10-06)

- Tras quitar las aplicaciones solicitadas, C: tiene 19,59 GiB libres. La cifra anterior de 5,81 GB ya no aplica.
- Docker Engine 29.8.1 responde. Los siete servicios base aparecen activos; ClickHouse, PostgreSQL, Redis y Redpanda están healthy. `docker compose config --quiet` y el perfil app configuran con `--env-file .env.example`; `.env` no contiene las versiones de imagen requeridas por Compose.
- Se intentó construir `taq-ingestd` con el perfil app. El build de `rdkafka-sys` falló porque la imagen `rust:1.98.1-bookworm` presenta bibliotecas del sistema vacías/truncadas (`libcurl.so.4.8.0`, `libnghttp2.so.14`). Una reinstalación de paquete dentro de un contenedor temporal corrigió libcurl pero expuso otra biblioteca dañada. No se tocaron volúmenes ni se ejecutó prune/reset; requiere reparar/verificar el almacenamiento de Docker Desktop antes de reintentar builds.
- `cargo check --offline -p taq-api -p taq-state`, `cargo test --workspace --offline` (40 pruebas), `cargo fmt --all -- --check` y `git diff --check` pasan en Windows.
- Graphify query/update se ejecutaron con acceso elevado; el grafo quedó actualizado tras los cambios de F11.


### F11/F12 continuación 2026-10-06

- En Windows pasan compilación de 	aq-api/	aq-state, las 40 pruebas del workspace, Clippy con -D warnings, rustfmt y git diff --check. El pool está implementado; no se ha hecho prueba de concurrencia o capacity/load.
- 	aq-loadgen publicó 20 eventos a Redpanda con cero errores; ClickHouse recibió 20 filas, 15 IDs únicos. El volumen usa MergeTree; la migración ClickHouse no se aplicó a los datos persistentes.
- Las migraciones PostgreSQL 0001–0004 pasan en una instancia de prueba temporal. El PostgreSQL local persistente no tiene aún 	aq_api_keys. No se aplicaron: auto-review bloqueó la 0003 por el UPDATE que invalida evaluaciones históricas; falta autorización del usuario para tocar ese volumen y decidir respaldo/efecto sobre evaluaciones.
- El smoke HTTP de la API en la instancia temporal quedó sin respuesta: TCP escucha, pero /health/live agotó el timeout y el loop de expiración reportó timeout al crear conexión. F11 sigue parcial hasta diagnosticar este comportamiento, validar workers y probar endpoints/readiness.
- Docker Compose mantiene las dependencias base saludables. Build app no repetido: persiste el fallo de bibliotecas truncadas observado en la imagen Debian. No hacer reset/prune.


