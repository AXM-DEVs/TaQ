# ADR 0003: Arquitectura de conectores de la Fase 1

- Estado: aceptado para la Fase 1
- Fecha: 2026-09-30

## Contexto

Wikimedia SSE, Bluesky WebSocket, RSS/GDELT y CT Logs entregan formatos y ritmos distintos. La etapa de contexto todavía no existe; los conectores deben producir el contrato común `Event` sin incorporar reglas de inferencia.

## Decisión

Los conectores implementan `Source` y emiten `NativeEvent` a través de un `tokio::sync::mpsc` acotado. La presión del canal suspende al productor del conector, en vez de acumular eventos sin límite. La normalización calcula `Event.id` como blake3 de `source + "|" + native_id` y conserva el payload público original como JSON.

Un único pipeline por proceso realiza deduplicación temporal y publica en `events.raw`, usando `Event.id` como clave Kafka. Un fallo de publicación se envía a `events.dead_letter`; si el dead-letter también falla, el pipeline termina con error para que el supervisor pueda reiniciar el flujo. Ambos topics se crean al levantar Compose, con seis particiones y siete días de retención.

Los fallos de red/servidor y los HTTP 408, 425 y 429 son transitorios y se reintentan con backoff exponencial, jitter y límite. Los demás HTTP 4xx se consideran terminales para evitar bucles ante configuración o solicitudes inválidas. Si una tarea crítica termina, `taq-ingestd` finaliza con error y el supervisor de despliegue debe reiniciarlo y reportar su estado; el proceso no intenta ocultar el fallo recreando una tarea sin límites. Ante caída conjunta del broker y DLQ se falla rápido: no se usa spool local en esta fase, así que las fuentes sin cursor/replay pueden perder eventos durante la interrupción.

Al recibir Ctrl+C, el daemon cancela las fuentes, deja drenar los reenviadores y la cola principal, espera hasta 60 segundos y solicita flush de mensajes pendientes del productor Kafka (hasta 10 segundos). Si vence el límite, el proceso falla de forma visible.

Los mensajes de `events.dead_letter` usan un envelope con `schema_version: 1`. La ausencia del campo se interpreta como versión 1 para leer mensajes emitidos antes de introducir esta propiedad. Los cambios futuros incompatibles deben incrementar la versión y mantener un lector compatible o migrar los mensajes antes del replay.

## Consecuencias

- Los conectores quedan desacoplados del productor Kafka y comparten backpressure y serialización.
- El dead-letter usa el mismo broker y no puede proteger contra una caída total de Redpanda. Ese caso falla explícitamente; se requiere supervisión y replay desde la fuente o un mecanismo durable adicional.
- El canal acotado limita memoria, pero la política concreta de reconexión, cursores y replay se implementa por conector.
- Solo se procesan fuentes públicas; cada adaptador debe respetar límites y términos publicados por su servicio.

