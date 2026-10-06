# ADR 0002: Deduplicación acotada en el proceso de ingesta

- Estado: aceptado para la Fase 1
- Fecha: 2026-09-30

## Contexto

Los conectores pueden reenviar eventos cuando reconectan o repiten una consulta. `Event.id` ya es determinista por `source` e identificador nativo, pero Kafka conserva cada publicación y no deduplica por clave. Un Bloom filter puede descartar eventos válidos por falsos positivos; un conjunto sin límite crecería indefinidamente.

## Decisión

La Fase 1 usa un mapa exacto en memoria, acotado por capacidad y retención configurable. Los ids presentes se omiten; los expirados o expulsados por capacidad vuelven a admitirse. El orden FIFO y la expiración se gestionan con una cola para evitar recorrer el mapa completo por cada evento. No se usa Bloom filter.

## Consecuencias

- No hay falsos positivos de deduplicación dentro de la ventana retenida.
- Un reinicio o una expulsión por capacidad puede permitir que un duplicado vuelva al log; el contrato es entrega al menos una vez y deduplicación temporal best-effort.
- La política no pretende cumplir por sí sola la meta de 50.000 eventos/s. Antes de dimensionar producción debe medirse memoria y rendimiento; un almacén persistente requeriría otro ADR.
- La fuente durable sigue siendo Redpanda. Los consumidores deben tolerar la repetición de `Event.id`.

