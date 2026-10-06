# F2 — Entrega y recuperación confiables

**Estado:** propuesta para revisión del usuario  
**Fecha:** 2026-10-03  
**Alcance:** cerrar F2 de la Fase 1 descrita en `docs/roadmap/plan-implementacion-motor.md`.

## Contexto

F2 ya cuenta con reintentos HTTP con jitter y clasificación, supervisión que deja una salida no-cero ante fallos críticos, apagado con drenaje/flush acotado, contabilidad básica en `PipelineStats` y envelope DLQ versión 1 con lectura compatible. Faltan replay de la DLQ, visibilidad operativa de resultados, pruebas de fallos y una ruta de almacenamiento que tolere re-publicaciones. La infraestructura local usa Redpanda (`events.raw`, `events.dead_letter`, retención configurada de siete días) y ClickHouse Kafka Engine con una vista materializada hacia `taq.events` (`MergeTree`).

## Objetivos

1. Recuperar mensajes reparables de `events.dead_letter` sin perder el evento normalizado original ni ocultar fallos de publicación.
2. Hacer que repetir la recuperación produzca una sola fila lógica en la proyección de ClickHouse para el mismo evento y la misma marca original de ingesta.
3. Hacer visibles los resultados de ingesta y replay, incluidos rechazos de parseo del consumidor ClickHouse.
4. Probar fallos y recuperación bajo carga, incluido el apagado y el reinicio.
5. Mantener F2 acotada a entrega/proyección. No afirmar que esto completa el motor contextual descrito para etapas posteriores.

## Fuera de alcance

- Spool local cuando Redpanda y la DLQ están simultáneamente inaccesibles. La decisión existente sigue siendo terminar con error visible y depender de reinicio/replay del supervisor o de la fuente.
- Garantía exactly-once de extremo a extremo.
- Deduplicación de dos eventos distintos de origen que compartan `id` pero tengan distintas marcas `ingested_at_ms`, ni corrección de duplicados históricos que queden en particiones diferentes.
- Cursores de fuentes, replay directo desde cada fuente, cambios en conectores y contextualización/inferencia/acciones.
- SLA público, objetivo de rendimiento no medido, despliegue de producción o configuración de un supervisor externo concreto.

## Diseño

### 1. Replay de DLQ con entrega al menos una vez

Agregar un ejecutable independiente `taq-dlq-replay`. Consume `events.dead_letter` mediante un grupo de consumidor configurable y vuelve a publicar en `events.raw` el `Event` normalizado del envelope, conservando exactamente `id`, `schema_version`, tiempos, payload y clave de mensaje disponibles en ese contrato.

El replay confirma manualmente cada offset DLQ solo después de recibir confirmación satisfactoria del productor Kafka para la publicación a `events.raw`. Errores de parseo, versiones no soportadas o fallos de publicación no se confirman como si hubieran sido recuperados; el proceso informa el fallo y termina de forma no-cero o conserva el offset para reintento conforme al modo operativo que se concrete en el plan.

La semántica es **al menos una vez**: si el proceso cae después de que Redpanda acepte el evento y antes de confirmar el offset de DLQ, se puede publicar otra copia. El comando debe limitar el trabajo por ejecución (límite de mensajes o duración), admitir inspección sin publicar y permitir un grupo/offset controlado. El detalle de flags y configuración se fijará en el plan de implementación.

### 2. Proyección idempotente en ClickHouse

Cambiar `taq.events` a `ReplacingMergeTree` con una clave de orden que incluya `(ingested_at_ms, id)`. Replay publica el evento almacenado sin regenerar `ingested_at_ms`; por ello, las copias del mismo evento quedan en la misma partición diaria y comparten la clave de reemplazo. Elegir y documentar una columna de versión estable y monotónica para las copias/reintentos, o prescindir de ella si la estrategia probada no la necesita. Las consultas que requieran deduplicación inmediata leerán con `FINAL`; la compactación asíncrona por sí sola no es garantía inmediata.

La garantía de reemplazo se limita a filas con los mismos valores de partición y orden. La reingesta de un mismo ID con nueva marca `ingested_at_ms` puede quedar en otra fecha/partición y no se deduplica con este diseño. El plan debe decidir si esa limitación se acepta explícitamente para F2 o si la evidencia del contrato exige otra clave/tabla de estado antes de implementar.

El cambio debe incluir migración versionada para volúmenes existentes y procedimiento de rollback. La secuencia debe evitar que el consumidor Kafka y la vista materializada escriban concurrentemente durante el cambio: pausar el consumo ClickHouse, detener o desacoplar la vista, crear/cargar/verificar la tabla destino, cambiar la vista/lectura y reanudar. Redpanda conserva el log durante la ventana solo dentro de su retención vigente; no truncar ni recrear topics. No usar `down -v` como migración. Consultas y verificaciones existentes sobre `taq.events` deben adaptarse a la semántica lógica elegida.

### 3. Observabilidad y salud

Instrumentar `taq-ingestd` y `taq-dlq-replay` con métricas Prometheus/OTLP, siguiendo la infraestructura existente. Como mínimo: eventos recibidos, publicados, enviados a DLQ, descartados/duplicados contabilizados, intentos/reintentos y fallos de publicación; para replay, leídos, republicados, fallidos y offsets confirmados. No incluir IDs de evento, payload, entidades ni etiquetas de cardinalidad no acotada.

Hacer visible `system.dead_letter_queue` de ClickHouse, que recoge errores del Kafka Engine cuando `kafka_handle_error_mode='dead_letter_queue'`. Documentar consultas operativas y exponer contadores/alertas adecuados para errores y crecimiento/antigüedad de mensajes cuando la versión/configuración de ClickHouse permita medirlos. Distinguir esta cola interna de ClickHouse del topic de aplicación `events.dead_letter`; son dos rutas de error distintas.

La salud debe distinguir proceso vivo de pipeline operativo. Ante una falla crítica sin recuperación, conservar la salida no-cero y exponer estado/error accionable en logs y métricas. La política de restart/backoff pertenece al despliegue y debe documentarse como requisito operacional, no simularse dentro de un daemon que reinicia tareas sin límite.

### 4. Apagado y supervisión

Conservar el drenaje acotado existente y completar señal de terminación portable en plataformas soportadas, incluidos Ctrl+C y SIGTERM en Unix. El apagado detiene primero las fuentes, espera reenvíos y canal principal, y después espera el flush del productor hasta los límites configurados. Si se exceden, termina de forma visible con fallo, no como apagado limpio. El plan debe fijar la interacción exacta entre timeout de apagado, flush y código de salida, sin extender indefinidamente la terminación.

### 5. Pruebas y CI

Ampliar pruebas deterministas unitarias/contrato para versión DLQ, offsets manuales y clasificación de fallos; pruebas de integración con Redpanda y ClickHouse para el flujo real. Incluir al menos:

- Redpanda temporalmente inaccesible y recuperación/reinicio observable.
- Productor lento, backpressure y límites de retry.
- Replay confirmado solo tras ACK del productor.
- Caída simulada entre ACK y commit de offset, seguida de replay repetido; ClickHouse presenta una fila lógica.
- Replay de envelope antiguo sin `schema_version` y rechazo explícito de una versión incompatible.
- JSON/evento que ClickHouse no puede parsear y comprobación en `system.dead_letter_queue`.
- SIGTERM/Ctrl+C bajo carga, drenaje dentro del timeout y timeout excedido con fallo visible.
- Pipeline/conector que termina o panic: daemon sale no-cero y el job verifica esa señal.

CI debe ejecutar las pruebas reproducibles y publicar diagnósticos útiles cuando fallen. Las pruebas de infraestructura no deben depender de una instancia persistente del desarrollador; deben crear su entorno efímero y limpiarlo sin borrar volúmenes externos. Los tests solo demuestran el entorno/escenario cubierto, no disponibilidad de producción.

## Invariantes y aceptación de F2

- Cada evento aceptado queda publicado, en DLQ, duplicado/descartado bajo política contada, o produce un fallo explícito; el resumen debe reconciliar las categorías sin contar dos veces.
- El replay no confirma un mensaje antes del ACK de la republicación.
- Repetir un mismo evento normalizado con su `ingested_at_ms` original da una sola fila lógica en consultas que usan el camino deduplicado documentado.
- Los errores del DLQ de aplicación y los errores de parseo de ClickHouse se distinguen y pueden investigarse.
- Todo fallo crítico termina en recuperación acotada visible o salida no-cero supervisable.
- Apagado completo o timeout quedan diferenciados en logs/métricas/código de salida.
- Las pruebas de fallos principales corren en CI con sus dependencias efímeras.
- `cargo fmt`, Clippy, pruebas del workspace, Compose config y `graphify update .` pasan; el E2E Redpanda/ClickHouse se ejecuta en un runner con Docker disponible. Si falta Docker local, se informa sin darlo por validado.

## Riesgos y mitigaciones

- **Duplicados por ventana ACK/commit:** inevitables con at-least-once; ClickHouse absorbe repeticiones del mismo evento/partición/clave y las pruebas demuestran la ruta.
- **Offset atascado por poison message:** registrar versión/causa sin volcar payload sensible; definir en el plan una política explícita de aislamiento/rechazo manual sin saltarse silenciosamente mensajes.
- **FINAL costoso:** limitarlo a rutas de lectura que requieren estado lógico inmediato, medir E2E y documentar el costo antes de ampliar volumen.
- **Migración con pérdida/duplicación:** pausa controlada del consumidor, retención suficiente, conteos/hash de verificación, copia de seguridad lógica cuando aplique y rollback ensayado en ClickHouse desechable.
- **Cardinalidad de métricas:** usar etiquetas acotadas (fuente, resultado, etapa); jamás IDs ni entidades.
- **Desfase entre semántica y contabilidad:** definir estados terminales mutuamente excluyentes y comprobar la ecuación con fallos inyectados.

## Decisiones pendientes para el plan

1. Confirmar con prueba qué campo usar como versión de `ReplacingMergeTree` (si se usa) ante replays idénticos y ordenar copias de forma determinista.
2. Definir el tratamiento explícito de poison messages del replay (fallo y reintento, o mecanismo autorizado para aislar/avanzar).
3. Definir cómo expresar readiness/liveness y restart/backoff en el despliegue disponible.
4. Auditar todas las consultas, scripts, dashboards y checks que leen `taq.events` para elegir dónde se requiere `FINAL`.
5. Precisar y ensayar la migración y rollback con un ClickHouse efímero antes de tocar cualquier volumen local.

## Auto-revisión

- Se diferencia at-least-once de exactly-once y se documenta el intervalo de posible repetición.
- La deduplicación se restringe a igualdad de clave y partición; no se promete dedupe global por ID.
- Se distinguen `events.dead_letter` y `system.dead_letter_queue`.
- La migración no supone que el DDL de inicio se aplique a volúmenes existentes y evita borrar volúmenes.
- Se evita dar por cerrada F2 solo por los tests unitarios: hacen falta pruebas de broker y proyección reales en CI.
- La especificación no altera fuentes, contextualización ni el alcance de Fase 2 del producto.
