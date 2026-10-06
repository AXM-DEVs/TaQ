# ADR 0008: cursores durables y primer flujo de producto (F3–F9)

- Estado: Propuesto hasta validación final de implementación
- Fecha: 2026-10-03

## Decisión

- Guardar cursores opacos e identidades nativas vistas en PostgreSQL. Persistirlos después del ACK Kafka; en restart, respetar semántica de replay inclusivo y deduplicar por identidad estable.
- Mantener entrega al menos una vez. Aceptar duplicados posibles entre el ACK Kafka y el commit del cursor; hacer la proyección y el consumidor downstream idempotentes por event ID.
- Usar un esquema multi-tenant desde el primer esquema derivado. Claves API almacenan hashes BLAKE3, scopes, principal, expiración, revocación y cuota distribuida por minuto.
- Hacer matching exacto/canónico y dominio IDNA antes de cualquier fuzzy match. Reproducir contexto con versión de watchlist; almacenar referencias, URLs públicas y hash de evidencia.
- Ejecutar reglas solo si una versión está explícitamente `active`. Promocionar candidate → shadow → active solo con gold y holdout temporal aprobados contra la versión vigente de guardrails. No auto-promover.
- Las acciones necesitan política versionada, estado pending y aprobación de otro principal. Aprobación y outbox comparten una transacción; esta versión no ejecuta efectos externos.
- No retener payload original en la DLQ de parseo del contextualizador; guardar motivo/offset/tamaño/hash. No equivale a una solución general de privacidad.

## Consecuencias y límites

- PostgreSQL pasa a ser dependencia crítica de arranque para checkpoints y el control plane.
- La vista ClickHouse nueva usa `ReplacingMergeTree(ingested_at_ms)`, `ORDER BY id`, con bucket particionado por hash de ID para que un reintento entre meses no duplique por partición. Volúmenes existentes requieren migración manual 0004 con ventana y rollback antes de reanudar.
- Los cursores y dedupe no constituyen exactly-once distribuido. La limpieza de `source_events_seen` es 30 días; la proyección keyed by id sigue deduplicando reintentos antiguos.
- Las mediciones presentadas por la ruta de evaluación dependen de un principal `rules:evaluate` confiable y un manifest aprobado. La carga/verificación reproducible de datasets no se ha automatizado; bloquear promoción pública hasta completarla.
- El outbox no tiene executor ni kill switch operativo; no declarar F7 completo.
- La retención de datos analíticos, borrado propagado, TLS interno, cifrado, secreto externo, SBOM, términos por uso real, backup/restore y E2E están pendientes de evidencias.
