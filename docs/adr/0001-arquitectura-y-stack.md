# ADR 0001: Arquitectura y stack de la Fase 0

- Estado: aceptado
- Fecha: 2026-09-30

## Contexto

TaQ procesa eventos heterogéneos en el lazo ingestar → contextualizar → inferir → coordinar → aprender. El vertical inicial es OSINT defensivo sobre una watchlist de marcas, dominios y organizaciones con datos públicos. El núcleo debe seguir independiente del vertical. La arquitectura posterior apunta a 50.000 eventos por segundo sostenidos en un nodo de 8 vCPU.

La Fase 0 debe demostrar un evento sintético desde su publicación hasta su persistencia analítica. Redis y PostgreSQL deben iniciar sanos, aunque todavía no reciben estado del motor.

## Decisión

Se crea un workspace modular de Cargo con contratos compartidos en `taq-core` y un generador Kafka en `taq-loadgen`. Rust estable y tokio son la base del core. Redpanda expone la API Kafka con el topic `events.raw`, seis particiones, siete días de retención en desarrollo y la clave del mensaje igual a `Event.id`.

ClickHouse consume ese topic mediante Kafka Engine y una vista materializada que persiste los eventos en MergeTree, particionado por el día de `ingested_at_ms`. Redis y PostgreSQL con pgvector se mantienen en Compose con healthchecks. Los cuatro servicios usan volúmenes nombrados. Docker Compose ejecuta también la creación del topic al iniciar.

`Event.id` se calcula como blake3 de `source + "|" + id nativo`. El generador conserva ids nativos para poder reenviar duplicados controlados. Los servicios de modelo, API y dashboard quedan fuera de esta fase.

## Imágenes verificadas

A 2026-09-30, se fijan tags estables publicados para Redpanda `v26.2.3`, ClickHouse `26.9.2.8`, Redis `8.10.2` y pgvector `0.8.6-pg18` (PostgreSQL 18). Se usan versiones explícitas en `.env.example`, no `latest`.

## Decisión abierta: deduplicación

La ingestión debe ser idempotente mediante ids deterministas. Queda abierta la elección entre deduplicar aguas arriba, antes de persistir, o usar `ReplacingMergeTree` y aceptar su semántica de consolidación eventual. La Fase 0 no deduplica: ClickHouse conserva cada mensaje consumido, incluidos los duplicados. La métrica `uniqExact(id)` permite medir ids únicos sin alterar el log.

## Consecuencias

- El log de Redpanda es el registro reproducible de entrada; Redis y ClickHouse son estado/proyecciones derivadas.
- La partición actual por id reparte la carga entre seis particiones. Cuando el contexto por entidad requiera localidad, una fase posterior debe re-particionar por entidad y diseñar el replay.
- El motor Kafka de ClickHouse simplifica la demostración extremo a extremo; la persistencia duradera del historial queda en MergeTree.
- El rendimiento medido del generador sirve para validar el recorrido local, no sustituye un benchmark de producción de 50.000 eventos/s.
