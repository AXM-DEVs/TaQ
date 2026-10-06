# Paquete de evidencia de release TaQ

Copiar este archivo por release y sustituir cada `PENDIENTE` por evidencia reproducible. Un campo sin artefacto no es una aprobación.

## Identificación

- Commit / tag inmutable: PENDIENTE
- Fecha UTC / responsable de release: PENDIENTE
- Configuración, migraciones y versiones de servicio: PENDIENTE
- Cohorte/tenants autorizados y ventana: PENDIENTE

## Puertas

| Puerta | Evidencia necesaria | Estado |
| --- | --- | --- |
| Contratos e identidad (F1) | fixtures, migración compatible, IDs/tiempos reproducibles | PENDIENTE |
| Entrega y recuperación (F2) | replay, DLQ, broker caído/lento, apagado y restart | PENDIENTE |
| Fuentes y uso permitido (F3) | cursores/replay por fuente, límites y aprobación de términos | PENDIENTE |
| Ruta real L0 (F4) | CI `ingestd → Redpanda → ClickHouse`; métricas/alertas visibles | PENDIENTE |
| Contexto (F5) | gold set versionado, precision/recall y replay contextual | PENDIENTE |
| Hallazgos (F6) | evaluación reproducible por regla/volumen, evidencia verificable | PENDIENTE |
| Acciones (F7) | aprobación separada, outbox durable, idempotencia, kill switch y sombra | PENDIENTE |
| Feedback (F8) | gold + temporal holdout, promoción revisada, rollback y drift | PENDIENTE |
| API/CLI/tenants (F9) | OpenAPI/CLI y negativas de autorización/aislamiento | PENDIENTE |
| Seguridad/privacidad (F10) | threat review, retención/borrado, TLS, secretos, SBOM y legal | PENDIENTE |
| Operación (F11) | restore, rollback, SLI/alertas, runbooks y on-call | PENDIENTE |
| Release (F12) | carga sostenida medida, costos, excepciones y Go/No-Go | PENDIENTE |

## Resultados medidos

- Ruta, dataset y hardware: PENDIENTE
- Volumen sostenido y duración: PENDIENTE
- p50 / p95 / p99 end-to-end: PENDIENTE
- Errores, pérdida, duplicación lógica y lag máximo: PENDIENTE
- Precision / recall / falsos positivos por volumen: PENDIENTE
- Backup y restauración: PENDIENTE
- Hallazgos, defectos y excepciones aprobadas: PENDIENTE
- SBOM CycloneDX del commit/tag, SHA-256 y revisión de advisories/licencias: PENDIENTE

## Decisión

- Decisión: **NO-GO hasta evidencia**
- Aprobadores y fecha: PENDIENTE
- Límites de la cohorte/uso autorizados: PENDIENTE
- Condiciones que detienen el rollout: PENDIENTE
