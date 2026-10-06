# TaQ

Motor de inteligencia contextual defensiva. F0–F2 cubren la ingesta y recuperación iniciales; F3–F9 se están implementando con cursores durables, contexto multi-tenant, reglas, hallazgos, aprobaciones, feedback y API/CLI. El estado actual aún espera la validación final y no es apto para oferta pública.

## Requisitos

- Rust 1.98.1 con `rustfmt` y `clippy` (lo instala `rustup` según `rust-toolchain.toml`).
- Docker Desktop con Docker Compose v2.
- [`just`](https://just.systems/).

## Arranque

En PowerShell, desde la raíz del repositorio:

```powershell
Copy-Item .env.example .env
just up
just loadgen 5000 30
just verify
just test
just lint
```

Prometheus queda en `http://localhost:9090` y Grafana en `http://localhost:3000` (credenciales configuradas en `.env`). El dashboard `TaQ / Overview` se provisiona al iniciar. Redpanda y ClickHouse exponen métricas a Prometheus; el Collector recibe métricas OTLP en `localhost:4317` (gRPC) y `localhost:4318` (HTTP). `taq-ingestd` y `taq-dlq-replay` exportan resultados y reintentos al Collector; define `TAQ_OTEL_ENDPOINT` si ejecutas los procesos desde otra ubicación.

`just up` espera a que Redpanda, Redis, ClickHouse y PostgreSQL estén saludables. También crea `events.raw` con seis particiones, retención de siete días y clave de partición igual al id del mensaje. Los puertos de desarrollo se enlazan solo a `127.0.0.1`. El generador usa `localhost:19092`; puede cambiarse con `TAQ_KAFKA_BROKER`.

Para enviar duplicados en la comprobación manual:

```powershell
$env:RUST_LOG = 'info'
cargo run --release -p taq-loadgen -- --rate 5000 --duration 30 --entities 100 --duplicate-ratio 0.01
just verify
```

El generador informa el total confirmado, los ids únicos confirmados, errores y tasa real. `just verify` muestra las filas lógicas con `FINAL`, los ids únicos y p99 desde el timestamp de append de Redpanda hasta la inserción en ClickHouse; así evita restar el reloj del host productor al del contenedor. ClickHouse consume el topic en segundo plano y puede tardar unos instantes en alcanzar al productor.

`just down` detiene los servicios y conserva los volúmenes. Para descartar también los datos, ejecuta `docker compose --env-file .env -f deploy/docker-compose.yml down -v`.

El SQL inicial de ClickHouse solo corre al crear el volumen por primera vez. Para volúmenes existentes, detén `taq-ingestd` y aplica `deploy/clickhouse/migrations/0002_event_timestamp_quality.sql` antes de desplegar eventos de esquema 2:

```powershell
docker compose --env-file .env -f deploy/docker-compose.yml exec -T clickhouse clickhouse-client --multiquery --queries-file=/docker-migrations/0002_event_timestamp_quality.sql
```

No borres los volúmenes para actualizar el esquema si necesitas conservar datos. La configuración del topic también actualiza los topics que ya existan.

Para inspeccionar y reproducir eventos del topic de aplicación `events.dead_letter`, primero valida sin publicar ni confirmar offsets y luego habilita el replay explícitamente:

```powershell
$env:TAQ_KAFKA_BROKER = 'localhost:19092'
$env:TAQ_DLQ_REPLAY_DRY_RUN = 'true'
$env:TAQ_DLQ_REPLAY_LIMIT = '100'
cargo run --release -p taq-dlq-replay

$env:TAQ_DLQ_REPLAY_DRY_RUN = 'false'
cargo run --release -p taq-dlq-replay
```

El programa procesa hasta 1.000 mensajes por ejecución por defecto, termina tras 30 segundos sin mensajes y usa el grupo `taq-dlq-replay-v1`. Cambia `TAQ_DLQ_CONSUMER_GROUP` para una campaña nueva. Conserva los logs y revisa los contadores antes y después; el replay es al menos una vez y puede reenviar un evento si el proceso cae tras el ACK del broker y antes del commit DLQ. Las repeticiones exactas se deduplican en lecturas `FINAL` de ClickHouse si mantienen su `ingested_at_ms` original. Ver [operación de F2](docs/operations/f2-delivery.md) y [migraciones ClickHouse](deploy/clickhouse/migrations/README.md).

Graphify queda integrado en Codex para navegar el código y sus relaciones. Usa `$graphify` para generar o actualizar el grafo del proyecto.

## Ingesta (Fase 1 en curso)

`crates/taq-ingest` define `Source` y `NativeEvent`, normaliza ids con blake3 y provee una cola Tokio acotada, deduplicación exacta temporal y publicación a `events.raw`. El esquema de evento 2 incluye la calidad del timestamp de ocurrencia: `source`, `capture_time` o `unknown` para mensajes antiguos. Compose crea también `events.dead_letter`. La política actual no pierde precisión por falsos positivos: al expirar o expulsar ids, o al reiniciar el proceso, un duplicado puede volver a publicarse. El DLQ comparte Redpanda; si el broker entero está caído, el pipeline falla explícitamente. Los resultados y retries se exportan a OTLP con etiquetas de cardinalidad fija.

`taq-ingestd` multiplexa Wikimedia SSE, Bluesky Jetstream por WebSocket y un feed RSS configurable hacia un único pipeline Kafka. El RSS usa `pub_date` cuando es válido y no futuro; si no, marca la hora de captura como fallback. Los IDs RSS prefieren GUID, luego link y finalmente hash estable del contenido disponible. Bluesky usa DID, colección, rkey y operación para distinguir registros y cambios. GDELT y CT Logs todavía no tienen conectores. Revisa [ADR 0002](docs/adr/0002-deduplicacion-en-ingesta.md), [ADR 0003](docs/adr/0003-arquitectura-de-conectores.md) y [ADR 0004](docs/adr/0004-versionado-de-eventos.md) antes de extender esta base.

Para arrancar el demonio con Kafka local disponible, ejecuta `cargo run -p taq-ingestd`. Por defecto se conecta a `localhost:19092`; el broker se puede cambiar con `TAQ_KAFKA_BROKER`.

## API y producto en desarrollo (F3–F9)

Con PostgreSQL, Redpanda y OTLP disponibles, inicia la ingesta y el API con `docker compose --env-file .env -f deploy/docker-compose.yml --profile app up -d --build taq-ingestd taq-api`. Compose publica el API solo en loopback. El API consume `events.raw`, crea contexto/hallazgos para reglas activas y expone `/health/live` y `/health/ready`. Requiere migraciones PostgreSQL 0001–0004; las nuevas instalaciones Compose montan la inicialización completa.

`taq-api` y `taq-ingestd` crean sus pools PostgreSQL con la misma configuración. `TAQ_PG_POOL_MAX_SIZE` admite de 1 a 64 conexiones por proceso (predeterminado 16) y `TAQ_PG_POOL_ACQUIRE_TIMEOUT_SECS` limita la espera de adquisición a 1–60 segundos (predeterminado 5). Ambos procesos fallan al iniciar si los valores no son enteros dentro de esos límites.

Consulta [guía de producto](docs/operations/product-api.md), [OpenAPI](docs/api/openapi.yaml), [modelo de amenazas](docs/security/threat-model.md) y [runbook](docs/operations/production-runbook.md). `taqctl` es el cliente CLI de la API. Las acciones aprobadas solo escriben un outbox; no ejecutan efectos externos en esta versión.

**No-GO público actual:** están pendientes CI E2E real, ejecución de migraciones en servicios, mediciones de calidad/carga, borrado/retención, TLS/cifrado, gestor de secretos, análisis de dependencias/SBOM, revisión legal de fuentes y restauración ensayada. Usa [la plantilla de evidencia](docs/operations/release-evidence-template.md) para cerrar puertas con datos medidos. BBC continúa como ejemplo local no autorizado para uso comercial.

## Estructura

- `crates/taq-core`: contratos `Event`, `SignalKind`, `Signal` y `Action`.
- `crates/taq-loadgen`: generador de carga para `events.raw`.
- `crates/taq-ingest`: contratos y pipeline común de ingesta (en curso).
- `crates/taq-connector-wikimedia`: cliente SSE de cambios recientes de Wikimedia.
- `crates/taq-connector-bluesky`: cliente WebSocket de Bluesky Jetstream.
- `crates/taq-connector-rss`: polling y parseo de feeds RSS.
- `crates/taq-ingestd`: demonio que multiplexa los conectores hacia Kafka.
- `crates/taq-dlq-replay`: ejecutable de validación/replay manual de `events.dead_letter`.
- `crates/taq-observability`: exportador OTLP, cierre acotado y métricas de entrega.
- `deploy/`: servicios y esquema analítico de desarrollo.
- `docs/adr/`: decisiones arquitectónicas registradas.
