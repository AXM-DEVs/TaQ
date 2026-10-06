# ADR 0005: Medir latencia desde el append de Redpanda

- Estado: aceptado
- Fecha: 2026-10-01

## Contexto

`ingested_at_ms` lo escribe el productor y `inserted_at` lo asigna ClickHouse. Restarlos mezcla el reloj del host con el del contenedor, por lo que una diferencia de reloj puede inflar o volver negativa la latencia medida.

## Decisión

`events.raw` usa `message.timestamp.type=LogAppendTime`. La vista de ClickHouse persiste su columna virtual `_timestamp_ms` como `broker_appended_at`; `just verify` calcula el p99 entre ese timestamp y `inserted_at`. `ingested_at_ms` sigue siendo parte del evento y conserva su semántica original.

## Consecuencias

- El p99 mide broker-append → ClickHouse, no el tiempo total desde la observación de origen.
- Redpanda y ClickHouse deben compartir una base de tiempo sincronizada; en desarrollo ambos corren dentro de Docker.
- La configuración de tópico se aplica también a tópicos existentes al iniciar Compose.

