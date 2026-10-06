# ADR 0007: Entrega y replay confiables (F2)

- Estado: Aceptado
- Fecha: 2026-10-03

## Contexto

La ingesta puede fallar después de aceptar eventos y antes de que todos los destinos confirmen su escritura. La DLQ necesita una recuperación operable, y reintentar una publicación puede producir copias físicas en ClickHouse. Un daemon que queda vivo después de terminar una tarea crítica también oculta la pérdida de servicio.

## Decisión

- El replay de `events.dead_letter` es at-least-once. `taq-dlq-replay` valida el sobre y el evento, publica el evento original a `events.raw` y confirma el offset DLQ únicamente después del ACK del productor. El valor predeterminado es `dry-run`; publicar requiere opt-in explícito.
- La proyección ClickHouse usa `ReplacingMergeTree` ordenado por `(ingested_at_ms, id)` y conserva la partición diaria. Replays del mismo evento preservan ambas claves. Consultas que requieren deduplicación inmediata usan `FINAL`; merges no son una garantía de unicidad física inmediata.
- La migración de volúmenes existentes se ejecuta manualmente por fases: detener/detachar consumidor, copiar y comparar, luego activar. La tabla histórica se conserva como `events_pre_f2`; rollback solo antes de reanudar consumo.
- Los errores de parsing del Kafka Engine se inspeccionan en `system.dead_letter_queue`; esta tabla no sustituye al topic `events.dead_letter` de la aplicación.
- Si broker y DLQ no aceptan escritura, el proceso falla con error visible para que un supervisor externo lo reinicie. No se introduce spool local en esta fase.
- El daemon drena canales y hace flush limitado durante el apagado; métricas OTLP de resultados usan etiquetas acotadas sin IDs ni payloads.

## Consecuencias

- Puede haber duplicados físicos hasta que ClickHouse compacte; lecturas sensibles a unicidad deben usar `FINAL` o agregación equivalente.
- Una interrupción entre el ACK de raw y el commit DLQ puede republicar el evento, comportamiento esperado por at-least-once.
- El despliegue debe configurar reinicio y alertas. Esta decisión no constituye un SLO ni certifica aptitud para servicio público.
- La prueba completa requiere Docker y se ejecuta en CI; los unit tests no prueban la integración real con Redpanda y ClickHouse.
