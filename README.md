# TaQ

Motor de inteligencia contextual. Esta carpeta contiene únicamente los fundamentos de la Fase 0: contratos compartidos, generador sintético y servicios locales para recorrer un evento desde Redpanda hasta ClickHouse.

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

`just up` espera a que Redpanda, Redis, ClickHouse y PostgreSQL estén saludables. También crea `events.raw` con seis particiones, retención de siete días y clave de partición igual al id del mensaje. El generador usa `localhost:19092`; puede cambiarse con `TAQ_KAFKA_BROKER`.

Para enviar duplicados en la comprobación manual:

```powershell
$env:RUST_LOG = 'info'
cargo run --release -p taq-loadgen -- --rate 5000 --duration 30 --entities 100 --duplicate-ratio 0.01
just verify
```

El generador informa el total confirmado, los ids únicos confirmados, errores y tasa real. `just verify` muestra el total almacenado, `uniqExact(id)` y p99 de la latencia entre `ingested_at_ms` y la inserción en ClickHouse. Compara `unique_ids` con `uniqExact(id)`; ClickHouse consume el topic en segundo plano y puede tardar unos instantes en alcanzar al productor.

`just down` detiene los servicios y conserva los volúmenes. Para descartar también los datos, ejecuta `docker compose --env-file .env -f deploy/docker-compose.yml down -v`.

Graphify queda integrado en Codex para navegar el código y sus relaciones. Usa `$graphify` para generar o actualizar el grafo del proyecto.

## Estructura

- `crates/taq-core`: contratos `Event`, `SignalKind`, `Signal` y `Action`.
- `crates/taq-loadgen`: generador de carga para `events.raw`.
- `deploy/`: servicios y esquema analítico de desarrollo.
- `docs/adr/`: decisiones arquitectónicas registradas.
