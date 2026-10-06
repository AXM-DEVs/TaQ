# Operación de entrega y recuperación (F2)

## Métricas de aplicaciones

`taq-ingestd` y `taq-dlq-replay` exportan métricas OTLP gRPC a `TAQ_OTEL_ENDPOINT`, por defecto `http://127.0.0.1:4317`. En desarrollo, inicia el Collector con `just up` y ejecuta ambos programas desde el host. El Collector las expone a Prometheus en `http://localhost:9090` mediante el target `otel-collector`.

Series esperadas:

- `taq_ingest_events_received_total`: eventos tomados por el pipeline.
- `taq_ingest_events_total{outcome=published|dead_lettered|duplicate_skipped|failed}`: resultados terminales por evento.
- `taq_ingest_source_retries_total`: reintentos de conectores.
- `taq_dlq_replay_reads_total`: mensajes DLQ leídos.
- `taq_dlq_replay_publish_acks_total`: republicaciones confirmadas por Redpanda.
- `taq_dlq_replay_events_total{outcome=validated|committed|failed}`: resultado terminal del replay.

Las etiquetas son categorías fijas. Ningún ID, entidad o payload se usa como etiqueta. `committed` solo se incrementa después del ACK de `events.raw` y del commit del offset DLQ. Un contador `publish_acks` mayor que `committed` indica que un ACK llegó antes de una caída/commit fallido; el mensaje puede volver a reproducirse.

El dashboard `TaQ / Overview` presenta resultados de ingesta, replay y retries. Un proceso sin actividad no genera una serie de counter hasta que la observe; revisa también `up{job="otel-collector"}` para distinguir falta de exportación.

Ambos ejecutables atienden Ctrl+C; `taq-ingestd` también atiende SIGTERM en Unix. Replay deja terminar el mensaje actual y confirma solo tras el ACK, luego vacía el productor con un límite de 10 segundos. El daemon detiene fuentes, drena colas y termina el pipeline en hasta 60 segundos. Un límite excedido debe quedar como fallo y el supervisor debe reiniciar/reportar el proceso; la política del supervisor no está incluida en Compose.

## Rechazos del consumidor de ClickHouse

`events.dead_letter` es el DLQ de la aplicación. Los mensajes que el Kafka Engine de ClickHouse no puede parsear se guardan por separado en `system.dead_letter_queue`. El XML de ClickHouse activa el buffer de esta tabla y la configuración `kafka_handle_error_mode='dead_letter_queue'` de `taq.events_raw` los enruta ahí. ClickHouse crea la tabla solo cuando existe esta configuración y no elimina sus filas automáticamente; el cuerpo de cada mensaje queda almacenado en `raw_message`.

Consulta errores recientes después de vaciar los buffers:

```sql
SYSTEM FLUSH LOGS;

SELECT
    event_time,
    kafka_topic_name,
    kafka_partition,
    kafka_offset,
    error,
    raw_message
FROM system.dead_letter_queue
WHERE database = 'taq' AND table = 'events_raw'
ORDER BY event_time DESC
LIMIT 100;
```

Busca el offset correspondiente en `events.raw`, identifica si el productor serializó una versión/campo incompatible y corrige el consumidor antes de reanudar. No borres el registro antes de preservar el error y los datos necesarios para investigar. Como la cola no tiene expiración automática, revisa su crecimiento; una vez investigados, se puede aplicar la retención operativa de 30 días a los errores de esta tabla con:

```sql
ALTER TABLE system.dead_letter_queue
DELETE WHERE database = 'taq'
    AND table = 'events_raw'
    AND event_date < today() - 30;
```

La mutación es asíncrona; confirma su finalización en `system.mutations` antes de considerar liberado el espacio. Ajusta el periodo al requisito de retención del despliegue antes de producción.

La tabla del sistema contiene mensajes crudos y errores; limita los permisos de consulta y respeta la retención de datos definida por el operador. Este registro de parseo no se copia automáticamente a `events.dead_letter` ni incrementa los contadores de DLQ de `taq-ingestd`.
