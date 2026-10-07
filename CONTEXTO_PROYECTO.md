# Contexto del proyecto TaQ

Guía para entender la intención, arquitectura y estado actual de TaQ antes de modificarlo. Complementa el [README](README.md): aquí se explica cómo se conectan las piezas y qué límites tiene la implementación actual.

**Última revisión del contenido:** 2026-10-06. F1–F12 tienen implementación parcial o evidencia local desigual; el producto todavía no ha pasado la calificación final para uso público. Distingue siempre código existente de comportamiento validado y de criterios de lanzamiento.

## Qué es TaQ

TaQ es un motor modular de inteligencia contextual. El flujo de producto previsto es **ingerir → contextualizar → inferir → coordinar → aprender**. El primer vertical es OSINT defensivo sobre una lista vigilada de marcas, dominios y organizaciones, pero los contratos centrales buscan seguir independientes de ese vertical.

F0–F2 cubren el prototipo de ingesta y recuperación. La continuación actual implementa F3–F9: cursores de fuentes, contexto multi-tenant, reglas explicables, hallazgos, aprobación/outbox, feedback gobernado y API/CLI. Esta implementación aún no ha pasado las pruebas finales ni E2E; no marcar fases completas por la existencia de código o SQL.

## Relación con el TaQ anterior

El repositorio [AXM-DEVs/taq-engine](https://github.com/AXM-DEVs/taq-engine) es un prototipo anterior e independiente: Python/FastAPI, investigaciones OSINT, playbooks, conectores, puntuación y consultas. Se revisó el 2026-10-06 para rescatar requisitos e ideas. No es la implementación base ni una fuente de comportamiento verificado para el motor Rust actual.

### Qué conviene conservar como referencia

- **Catálogo de fuentes OSINT:** DNS, WHOIS, GeoIP, VirusTotal, AbuseIPDB y Shodan pueden orientar la evaluación de conectores futuros. Antes de implementar cada uno hay que revisar disponibilidad, términos de uso, límites, atribución, privacidad, secretos y si su señal sirve al caso defensivo. Empezar con fuentes públicas y acotadas; no asumir que todo conector anterior es gratuito o apto para producción.
- **Flujos declarativos de análisis:** la estructura conceptual de playbook (pasos, transformaciones, condiciones y notificaciones) puede servir para diseñar una capa de orquestación más adelante. El motor actual ya tiene reglas, hallazgos y acciones con aprobación/outbox; todavía no tiene executor externo de playbooks.
- **Explicabilidad de señales:** antigüedad, confiabilidad de la fuente, corroboración/correlación y reputación son dimensiones candidatas para enriquecer evidencia y priorización. Son hipótesis de diseño, no pesos calibrados ni un modelo validado. Priorizar reglas versionadas, evidencia trazable y evaluación contra gold set antes de combinar factores en una puntuación.
- **Casos de uso e interacción:** investigaciones, entidades y exploración de relaciones pueden ayudar a identificar necesidades de API o interfaz. Adaptar los casos al flujo actual de listas vigiladas → contexto → reglas → hallazgos → feedback, sin replicar el producto anterior automáticamente.

### Qué no portar directamente

No copiar directamente el motor de investigaciones, autenticación/API keys, persistencia, executor de playbooks, webhooks, límites por tier ni traducción de lenguaje natural a SQL/Cypher. En la revisión estática del prototipo anterior se encontraron desajustes de argumentos en la creación de investigaciones, estado de investigaciones en memoria, paginación aplicada dos veces, manejo de fallos de pasos que podía dejar continuar una ejecución y notificación webhook sin validación suficiente de destinos ni estados HTTP. Su puntuación usa pesos heurísticos sin evidencia de calibración. Estos componentes requieren rediseño y revisión de seguridad si alguna necesidad futura justifica reemplazarlos por versiones acordes a los contratos Rust.

No se ejecutaron sus dependencias, pruebas ni servicios; las conclusiones anteriores son una revisión de código y documentación, no certificación funcional. Su README presenta planes y afirmaciones de producto que no deben trasladarse al contexto actual como capacidades logradas. Cualquier reutilización futura debe mantener atribución/licencia y pasar por una revisión legal y de dependencias; el repositorio anterior declara MIT, pero esto no sustituye la revisión de cada archivo y dependencia.

### Decisión de producto

TaQ actual sigue siendo un motor gratuito de inteligencia contextual, con un primer vertical de OSINT defensivo. No se adopta el modelo premium/SaaS que describe el prototipo anterior. No se importará código del repo anterior por ahora. Las ideas rescatadas entran al backlog como candidatos, subordinados a cerrar la confiabilidad, seguridad y validación del flujo central existente.

## Arquitectura y recorrido del evento

```text
Wikimedia SSE  ─┐
Bluesky WS ─────┼─> NativeEvent ─> canales Tokio acotados ─> multiplexor ─┐
RSS polling ────┘                                                       │
                                                                       v
                                                       normalización + ID estable
                                                       deduplicación temporal
                                                                       │
                                                        Redpanda events.raw
                                                                       │
                                            ClickHouse Kafka Engine + vista materializada
                                                                       v
                                                          taq.events (MergeTree)
```

`taq-ingestd` arranca un único `KafkaPipeline` por proceso. Cada conector produce eventos o checkpoints por canal acotado. El evento se publica primero a Kafka; después del ACK se persiste la identidad/cursor en PostgreSQL. `taq-api` consume `events.raw` con commit manual, enlaza con watchlists/versiones por tenant y evalúa reglas activas en la misma transacción que guarda contexto y hallazgos.

Al fallar la publicación principal, el pipeline intenta publicar un `DeadLetter` a `events.dead_letter`. Si tampoco puede publicar allí, devuelve un error. `taq-ingestd` observa las tareas críticas y termina con código no-cero ante un fallo inesperado; el reinicio corresponde al supervisor externo. No hay spool local y las fuentes sin cursor/replay pueden perder eventos durante una caída completa del broker.

El envelope JSON del dead-letter lleva `schema_version: 1`; los mensajes previos sin ese campo se leen como versión 1. Hay que incrementar la versión si se cambia de forma incompatible y proporcionar compatibilidad para los mensajes que permanezcan en el topic.

`run_pipeline` lleva contadores de eventos recibidos, publicados, enviados a DLQ, omitidos como duplicados y fallidos. El daemon escribe el resumen al apagarse; ante errores terminales, el pipeline registra los mismos contadores junto al estado del invariante. Son contadores del proceso y sus logs, no métricas Prometheus/OTLP ni almacenamiento durable.

El generador `taq-loadgen` es un camino separado: publica eventos `Event` ya normalizados directamente en `events.raw`. Se usa para validar el recorrido Fase 0 y no pasa por `taq-ingestd` ni por la deduplicación de Fase 1.

## Workspace Rust

| Crate | Responsabilidad |
| --- | --- |
| `crates/taq-core` | Contratos compartidos `Event`, `Signal`, `SignalKind` y `Action`. |
| `crates/taq-loadgen` | Productor sintético para pruebas de carga de Kafka. |
| `crates/taq-ingest` | `Source`, `NativeEvent`, normalización, deduplicación y publicación Kafka. |
| `crates/taq-connector-wikimedia` | Cliente SSE para cambios recientes de Wikimedia. |
| `crates/taq-connector-bluesky` | Cliente WebSocket para Bluesky Jetstream. |
| `crates/taq-connector-rss` | Polling y parseo de feeds RSS. |
| `crates/taq-ingestd` | Proceso que configura el pipeline y conecta las fuentes. |

Las dependencias compartidas y miembros del workspace están en el `Cargo.toml` raíz. La versión Rust está fijada en `rust-toolchain.toml` (1.98.1, con `rustfmt` y `clippy`).

### Contratos principales

- `NativeEvent` representa los datos que entrega un conector: `native_id`, `kind`, `entity_ids`, `occurred_at_ms`, `occurred_at_quality` y `payload`.
- `Event` es el formato común publicado: incluye `schema_version`, `id`, `source`, los campos del evento, `ingested_at_ms` y `payload`. La versión 2 agrega `occurred_at_quality`: `source`, `capture_time` o `unknown` para mensajes históricos.
- `normalize_event` calcula `Event.id` como blake3 de `source + "|" + native_id`. El ID no cambia entre reintentos de la misma fuente, pero sí cambia si cambia la fuente.
- Los eventos normalizados nuevos usan `schema_version` `2`; los mensajes antiguos sin ese campo se deserializan con versión `1`, y sin calidad de timestamp como `unknown`.
- `Signal` y `Action` son contratos preparados para etapas posteriores. Que estén definidos no significa que exista lógica que los produzca o ejecute.

### Fuentes existentes y sus límites

- **Wikimedia:** SSE en `https://stream.wikimedia.org/v2/stream/recentchange`. El identificador nativo sale de `meta.id`; `timestamp` se convierte de segundos a milisegundos.
- **Bluesky:** WebSocket a Jetstream. El ID nativo incluye DID, colección, `rkey` y operación; el tiempo `time_us` se convierte a milisegundos y se marca como hora de fuente. La URL de conexión no tiene aún filtros configurables.
- **RSS:** consulta por defecto el feed BBC `https://feeds.bbci.co.uk/news/rss.xml` cada 60 segundos. `TAQ_RSS_FEED_URL` y `TAQ_RSS_POLL_INTERVAL_SECS` permiten cambiar el feed y el intervalo; los valores inválidos del intervalo usan el valor predeterminado. `occurred_at_ms` usa `pub_date` RFC 2822/RFC 3339 si es válido y no futuro; en otro caso usa la hora de captura y marca `capture_time`. El ID prefiere GUID, luego link y finalmente hash del contenido disponible.
- **GDELT y CT Logs:** no tienen conectores implementados.

Wikimedia reanuda SSE con Last-Event-ID; Bluesky usa el host público Jetstream v2, persiste el cursor secuencial más time_us y reanuda por cursor dentro de su ventana predeterminada de 36 h; cursores históricos V1 usan fallback temporal solo durante la transición; RSS conserva ETag/Last-Modified e IDs vistos. PostgreSQL conserva IDs vistos 30 días. Los cursores avanzan solo después del ACK Kafka. La entrega sigue siendo al menos una vez: los replays que llegan se deduplican por ID estable en ClickHouse y claves idempotentes en el consumidor. La migración ClickHouse 0004 está escrita pero pendiente de ejecutar sobre un volumen existente.

## Infraestructura local y datos

`deploy/docker-compose.yml` levanta:

- **Redpanda:** API Kafka; topics `events.raw` y `events.dead_letter`, seis particiones y retención local de siete días.
- **ClickHouse:** Kafka Engine lee `events.raw`; una vista materializada persiste los eventos en `taq.events` (`ReplacingMergeTree` para tolerar replays con la misma clave). Las consultas que requieran deduplicación inmediata deben usar `FINAL`. La columna `broker_appended_at` proviene del timestamp de append de Redpanda.
- **PostgreSQL/pgvector:** usado para cursores, tenants/API keys, watchlists/versiones, contexto, reglas/evaluaciones, datasets/samples inmutables, hallazgos, acciones/outbox, feedback y auditoría (migraciones 0001–0004). Redis sigue preparado, todavía fuera del flujo actual.
- **OpenTelemetry Collector, Prometheus y Grafana:** `taq-ingestd` y `taq-dlq-replay` exportan métricas OTLP de resultados de entrega/replay; Prometheus las recolecta y Grafana muestra paneles básicos.

Los puertos de desarrollo se enlazan a `127.0.0.1`. Las versiones y credenciales locales de ejemplo están en `.env.example`; copia ese archivo a `.env` para usar Compose. No uses credenciales de ejemplo fuera del entorno local.

El esquema inicial de ClickHouse se ejecuta al crear el volumen por primera vez. Los cambios de esquema no migran automáticamente volúmenes existentes. `docker compose ... down -v` elimina los datos persistidos; revisa el README antes de usarlo.

La latencia verificada por `just verify` mide desde el append en Redpanda hasta la inserción en ClickHouse. No representa el tiempo total desde que ocurrió un hecho externo: ese tiempo requeriría comparar con `occurred_at_ms` y considerar sincronización de reloj.

## Arrancar y trabajar localmente

Desde PowerShell, en la raíz del repositorio:

```powershell
Copy-Item .env.example .env
just up
```

Validar el recorrido sintético:

```powershell
just loadgen 5000 30
just verify
```

Arrancar los conectores externos requiere Kafka local disponible:

```powershell
cargo run -p taq-ingestd
```

`TAQ_KAFKA_BROKER` cambia el broker; por defecto es `localhost:19092`. `TAQ_RSS_FEED_URL` y `TAQ_RSS_POLL_INTERVAL_SECS` configuran RSS. Los nombres de topics, la capacidad de canales y los valores de deduplicación siguen fijos en código.

Comprobaciones definidas por el proyecto:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check --workspace
```

`just lint` ejecuta formato y Clippy; `just test` ejecuta las pruebas del workspace. CI ejecuta esas validaciones y la configuración de Compose; su job E2E levanta Docker, envía carga sintética y valida ClickHouse.

## Decisiones que guían los cambios

Lee los ADR antes de alterar contratos o topología:

- [ADR 0001](docs/adr/0001-arquitectura-y-stack.md): stack y recorrido Fase 0; Redpanda es el log de entrada y las bases son proyecciones.
- [ADR 0002](docs/adr/0002-deduplicacion-en-ingesta.md): deduplicación exacta en memoria, limitada por capacidad y retención.
- [ADR 0003](docs/adr/0003-arquitectura-de-conectores.md): conectores desacoplados por canales acotados y un solo pipeline por proceso.
- [ADR 0004](docs/adr/0004-versionado-de-eventos.md): evolución explícita mediante `schema_version`.
- [ADR 0005](docs/adr/0005-medicion-de-latencia-ingesta.md): medir append de Redpanda → ClickHouse usando `LogAppendTime`.
- [ADR 0006](docs/adr/0006-observabilidad-local.md): Prometheus, Grafana y Collector para observabilidad local.
- [ADR 0007](docs/adr/0007-entrega-y-replay-f2.md): replay at-least-once, idempotencia de la proyección y fallo visible ante pérdida del broker.

La deduplicación evita falsos positivos dentro de la ventana retenida, pero no es durable: un reinicio o una expulsión por capacidad permite que vuelva a entrar un ID. Los consumidores deben ser idempotentes. No cambies a una estrategia probabilística ni persistente sin revisar ADR 0002.

## Cómo extender el proyecto

### Añadir una fuente

1. Crea un crate `taq-connector-<fuente>` y añádelo a `workspace.members`.
2. Implementa `taq_ingest::Source`; convierte el formato externo en `NativeEvent` y usa un canal acotado.
3. Usa un ID nativo estable y una marca de tiempo con unidades documentadas. Conserva datos de origen útiles en `payload`.
4. Añade el crate como dependencia de `taq-ingestd`, crea source/receiver y reenvía `(source.name(), event)` al canal principal. Mantén una sola instancia de `KafkaPipeline`.
5. Decide y documenta reconexión, cursores/replay, límites de tasa, errores y apagado. Respeta los términos y límites de la fuente pública.
6. Añade pruebas para parseo/normalización y ajusta documentación/ADR si cambia una decisión.

### Cambiar el evento o almacenamiento

Un cambio incompatible al JSON debe actualizar `schema_version`, lectores/proyecciones y plan de migración del volumen de ClickHouse. `events.raw` es la entrada reproducible; Redis, PostgreSQL y ClickHouse no deben asumirse como fuentes autoritativas sin modificar la decisión arquitectónica.

## Estado conocido y próximos huecos

- F3 está parcialmente implementada: fuentes configurables, cursores RSS/Wikimedia/Bluesky y persistencia PostgreSQL. Bluesky ahora usa Jetstream v2 con cursor por evento y replay en la ventana predeterminada de 36 h; falta probar reinicios, inclusividad, clamping y el cambio de cursor desde una base existente. Wikimedia filtra canaries sintéticos y conserva el checkpoint; su historial publicado es variable (7–31 días), no un SLA. No se han validado cortes, replay, 429 ni reinicios.
- F4 está parcial: health/readiness y métricas iniciales; CI valida principalmente taq-loadgen directo hacia ClickHouse, no el recorrido completo de taq-ingestd con fixture.
- F5–F7 tienen una primera implementación de contexto multi-tenant, reglas/findings y aprobación/outbox; no se han medido contra gold sets y no hay executor externo. El dispatcher interno ahora exige que la acción siga aprobada y vigente al reclamarla; el vencimiento expira y audita solicitudes/aprobaciones pendientes de despacho, y los mensajes incluyen `expires_at` para validación por cualquier consumidor futuro. Falta validar carreras, reinicio/lease y el kill switch en runtime.
- F8 conserva feedback y guardrails. `rules:evaluate` ejecuta el matcher determinista de TaQ sobre snapshots de eventos/aliases versionados por watchlist y almacenados por migración 0004; calcula TP/FP/FN/TN, precisión/recall y falsos positivos por mil para gold y temporal holdout. El hash normaliza orden de muestras/aliases y la respuesta informa diferencias holdout-gold; tres pruebas unitarias cubren hash, métricas/alias negativos y dominio. La implementación compila; falta probar el API contra PostgreSQL, aprobar gold set, automatizar muestreo de no-alertas y definir gate/canary/rollback. Métricas antiguas se marcan no confiables con migración 0003; promociones requieren evidencia `taq_runner_v1`.
- F9 dispone de API/CLI autenticadas, scopes, cuota, paginación en findings/acciones/reglas y límite de 1 MiB en cuerpos protegidos. Falta probar aislamiento negativo y completar webhooks/TLS de despliegue.
- F10–F12 siguen bloqueando lanzamiento público. F12 local anterior: 37 tests pasan; pasan formato, Clippy, check con lock, Compose config y `cargo audit` (291 dependencias, sin vulnerabilidades después de actualizar tokio-tungstenite). CycloneDX generó 11 SBOM con SHA-256 en `sbom/f12-local/`, pero marcó `UNLICENSED` como expresión SPDX inválida. El 6-oct Docker Engine 29.8.1 conecta y los siete servicios base Compose están activos; ClickHouse, PostgreSQL, Redis y Redpanda están healthy. `docker compose config` pasa con `.env.example`. Tras desinstalar las aplicaciones solicitadas hay 19,59 GiB libres en C:. El build de `taq-ingestd` con perfil app se intentó de nuevo y falla en `rdkafka-sys`: el layer de `rust:1.98.1-bookworm` contiene bibliotecas de sistema vacías/truncadas (`libcurl.so.4.8.0`, `libnghttp2.so.14`). Un contenedor temporal confirmó el daño y reinstalar paquetes solo lo desplazó a otra biblioteca. Se preservaron imágenes y volúmenes del stack; no se hizo prune/reset. `cargo test --workspace --offline` tampoco finalizó en Windows: quedó compilando `rdkafka-sys`, CMake seleccionó Visual Studio 17 y no se localizaron `cl`/`msbuild` en PATH. Después se reparó el acceso Cargo usando permisos de workspace y `cargo test --workspace --offline` completó: 40 pruebas pasan, 0 fallan. También `cargo check --offline -p taq-api -p taq-state` pasa. Graphify query/update se ejecutaron con acceso elevado y el grafo actualizado. Replay Windows, CI Linux y E2E replay/API/PostgreSQL siguen sin evidencia. El build de la imagen Docker continúa bloqueado por bibliotecas truncadas en una capa Debian. Permanecen pendientes borrado/retención propagados, secret manager, TLS/cifrado real, decisiones legales/licencias, medir capacidad del pool, despliegue/restore ensayado y evidencia release completa. Prometheus carga alerta local por dependencia sin scrape durante 5 min, sin Alertmanager, receptor ni responsable.
- La meta de 50.000 eventos por segundo no está medida. No declarar L0/L1/L2 aprobados hasta ejecutar la validación final y adjuntar evidencia.

### Avance F2 (2026-10-03)

- `taq-dlq-replay` valida versiones y mensajes, opera por defecto en dry-run, publica el evento original y confirma el offset DLQ solo después del ACK del productor. La semántica es at-least-once; repeticiones conservan `id` y `ingested_at_ms`.
- Las instalaciones nuevas usan `ReplacingMergeTree`. La migración 0003 está dividida en preparación, verificación y activación; conserva la tabla anterior para auditoría y documenta reversión previa a reanudar consumo.
- ClickHouse `system.dead_letter_queue` queda habilitada para inspeccionar errores de parseo; es distinta de `events.dead_letter` de la aplicación.
- CI añade escenarios de migración, replay duplicado, SIGTERM/reinicio, error de parseo y broker fuera/en recuperación. Los scripts pasan `bash -n`; el runtime E2E aún está pendiente. Docker Engine se recuperó el 6-oct y la pila Compose base normal volvió a pasar readiness; se usará Git Bash en Windows, porque el `bash` predeterminado abre WSL/Kali sin integración Docker.
- Validación local vigente: `cargo test --workspace --offline`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --offline -- -D warnings`, Compose config y `git diff --check`. Las limitaciones locales de red/Docker/Graphify se registran en el plan F2.

## Hábitos de trabajo en este repositorio

- `AGENTS.md` indica consultar Graphify primero para preguntas de arquitectura cuando exista el grafo. Si la CLI no está disponible, anótalo y valida los detalles en las fuentes.
- Tras cambiar código, intenta ejecutar `graphify update .` para mantener el grafo; no sustituyas ni borres sus archivos manualmente.
- Conserva los cambios locales preexistentes. Antes de editar, inspecciona `git status` y `git diff`; no asumas que la rama está limpia.
- Tras una modificación, ejecuta las comprobaciones relevantes y distingue claramente los comandos que pasaron de los que no pudieron ejecutarse.


### Evidencia de continuación (2026-10-06, F11)

- Pool deadpool-postgres implementado en 	aq-state, compartido por API y checkpoints; límites predeterminados 16/conexiones, máximo 64 y timeouts de adquisición y creación. API/ingesta leen configuración desde TAQ_PG_POOL_MAX_SIZE y TAQ_PG_POOL_ACQUIRE_TIMEOUT_SECS, que Compose propaga.
- Workers de contexto, expiración y dispatch se reinician con espera de 2 s después de error. La transición terminal de acción/outbox/auditoría se ejecuta en una transacción; la expiración también resuelve filas outbox reclamadas y vencidas.
- Verificación Rust en Windows: cargo check --offline -p taq-api -p taq-state; cargo test --workspace --offline (40 passed, 0 failed); cargo clippy --workspace --all-targets --offline -- -D warnings; cargo fmt --all -- --check; git diff --check; Compose app config y Graphify update pasan.
- Smoke de integración aditivo con el stack actual: 	aq-loadgen envió 20 eventos (0 errores); ClickHouse observó 20 filas y 15 IDs únicos. 	aq.events local sigue en MergeTree, no ReplacingMergeTree; no se modificó ni migró ese volumen.
- El volumen PostgreSQL normal está healthy pero carece de 	aq_api_keys: sus migraciones 0002–0004 no corrieron al crearse el volumen antiguo. Auto-review rechazó aplicarlas porque 0003 puede invalidar evaluaciones históricas existentes. No se alteró el volumen. En una instancia PostgreSQL desechable con tmpfs, migraciones 0001–0004 corrieron correctamente; el smoke HTTP de 	aq-api no devolvió respuesta aun cuando el puerto aceptó TCP y el worker de expiración registró timeout al crear conexión. Investigar esta interacción de arranque/worker/pool antes de afirmar API funcional.
- Se verificó Compose config --quiet, los siete servicios base están activos; base healthchecks principales pasan. La construcción de la imagen Rust en Docker sigue pendiente por el problema previo de bibliotecas truncadas en el layer base. No se ejecutó prune/reset.
- En la evidencia original F11, el guard público fallaba porque PostgreSQL aún usaba `NoTls` y los clientes Kafka no tenían TLS/SASL. La continuación de transporte más reciente está abajo: esos clientes ya comparten configuración cifrada, pero la exposición sigue fail-closed hasta F12 y los gates de privacidad/operación. La afirmación TLS upstream nunca prueba la topología real ni reemplaza la revisión del despliegue.
- En la continuación del 2026-10-06, `docker compose --profile app ps -a` muestra los servicios detenidos; Redpanda y ClickHouse finalizaron con código 137 y no hay contenedor `taq-api`. No existe una reproducción actual del timeout HTTP. La llamada síncrona `fetch_metadata` del consumidor es una hipótesis de bloqueo del runtime, no causa confirmada; medir con API y dependencias activas antes de cambiarla.

### Continuación de F10 — transporte seguro (2026-10-06)

- Tarea 1 implementada, todavía sin verificación de build ni pruebas: `taq-state` configura PostgreSQL con `TAQ_PG_TLS_MODE=disable` solo cuando se declara explícitamente para desarrollo, o `verify-full` con Rustls, hostname verificado, WebPKI y CA PEM opcional. API y `PgCursorStore` consumen el mismo modo. Se añadieron pruebas unitarias y fixture de CA; se reservan para F12.
- Tarea 2 implementada, sin build ni pruebas: nuevo crate `taq-kafka` valida `PLAINTEXT`, `SSL` y `SASL_SSL`, mecanismos SASL permitidos, credenciales, CA y pareja mTLS; configura verificación de hostname y redacta identidad/contraseña en `Debug`. La contraseña admite un archivo secreto montado para evitar exponerla como variable de entorno. `rdkafka/ssl` y OpenSSL se añadieron a dependencias/imágenes; `Cargo.lock` se actualizó con `cargo fetch`.
- Tarea 3 integró la política en los siete productores/consumidores del API, ingesta, replay y loadgen; cada configuración se aplica antes de crear el cliente. Se usó revisión estática y `git diff --check`; no hubo build ni pruebas.
- Tarea 4 parcialmente implementada: Compose declara los perfiles locales, pasa opciones TLS/SASL, monta `./secrets` como solo lectura en `/run/taq-secrets` e ignora el directorio en Git. El guard público valida PostgreSQL `verify-full`, Kafka `SASL_SSL`, credenciales no-placeholder y broker/bind antes de DB/listener, y permanece bloqueado hasta completar F10/F11/F12. Sigue pendiente verificar el despliegue real, provisionar secretos/CA, rotación y cobertura de topic-init/health checks seguros. No marcar F10/F12 como aprobadas ni exponer el servicio públicamente.
- El launcher de `graphify.exe` aún falla con `uv trampoline failed to canonicalize script path`; se actualizó la herramienta local `graphifyy` a 0.9.79 y se ejecutaron query/update por su módulo Python directo. El grafo se regeneró (2936 nodos/5352 relaciones); Graphify guardó backup, no se manipularon a mano los artefactos. No se ejecutaron build, tests, Clippy, Docker ni pruebas E2E.


