# Runbook de despliegue y recuperación (borrador F11)

Este runbook define las tareas antes del piloto; no certifica un despliegue productivo. Usa volúmenes/servicios administrados con persistencia, TLS, alertas y credenciales inyectadas desde un gestor de secretos. Los Dockerfiles generan binarios Rust mínimos y ejecutan como UID 10001; el pipeline final debe publicar imágenes por digest y SBOM.

## Antes de desplegar

1. Identificar el digest de cada imagen, commit/tag, migración y configuración aprobados.
2. Provisionar PostgreSQL, Redpanda/Kafka y ClickHouse de forma privada, con TLS y usuarios de mínimo privilegio; verificar límites y retención de cada servicio.
3. Crear el tenant, claves con scopes mínimos y principales distintos para operar y aprobar; las claves solo se muestran al crearlas.
4. Aplicar migraciones PostgreSQL 0001–0004 en orden con backup previo; 0003 marca como no válidas para promoción las evaluaciones antiguas sin procedencia reproducible y 0004 crea almacenamiento de muestras inmutables. En volúmenes ClickHouse existentes, detener productores/consumidores y seguir por completo el procedimiento de 0003 y 0004; conservar las tablas antiguas.
5. Confirmar términos/licencias y la lista de fuentes autorizadas; BBC sigue siendo solo valor de ejemplo.
6. Verificar endpoint de métricas, disponibilidad de dashboards, alertas del runbook y contacto responsable de incidentes.

## Transporte cifrado del API

Para `TAQ_API_PUBLICLY_ROUTABLE=true`, configurar `TAQ_PG_TLS_MODE=verify-full` y `TAQ_KAFKA_SECURITY_PROTOCOL=SASL_SSL`, con CA confiable, mecanismo SASL permitido y credenciales completas. PostgreSQL y librdkafka validan el hostname del certificado. Nunca usar `disable` o `PLAINTEXT` en un endpoint expuesto. Los Docker Compose de TaQ solo declaran el perfil local; montan `./secrets` del host como `/run/taq-secrets` de solo lectura para archivos CA/certificado/llave. Compose admite `TAQ_KAFKA_SASL_PASSWORD_FILE=/run/taq-secrets/kafka-sasl-password` para que la contraseña se lea del archivo y no pase como variable de entorno; TaQ elimina solo CR/LF finales y jamás la imprime. Para producción, inyectar secretos desde el gestor del operador con permisos mínimos, rotación documentada y sin agregarlos al repositorio. Si un orchestrator obliga a usar env vars, limitar el acceso al runtime y al host que puede inspeccionarlas.

### Rotación de CA, certificados y credenciales

1. Emitir la CA/certificado de reemplazo con SAN DNS que coincidan con el hostname usado por la conexión; comprobar fechas, EKU de servidor y cadena con la infraestructura de certificados del operador.
2. Publicar un bundle temporal con CA antigua y nueva. Actualizar el archivo montado y reiniciar gradualmente API e ingesta para que ambos procesos lo carguen; no registrar el contenido del certificado privado ni secretos durante la operación.
3. Cambiar el certificado de PostgreSQL o Kafka y confirmar health/readiness, errores TLS y consumo/publicación desde el entorno controlado. Si falla, restaurar el certificado anterior manteniendo el bundle dual.
4. Tras completar el cambio en todos los endpoints y comprobar que ya no se presenta la CA antigua, retirar esa CA del bundle y reiniciar los procesos para retirar confianza residual.
5. Rotar PostgreSQL con una segunda cuenta de servicio de mínimo privilegio y Kafka con un principal SCRAM nuevo: inyectar las credenciales nuevas desde el gestor, reiniciar consumidores/productores, confirmar operación y después revocar la credencial anterior. Nunca pasar secretos en argumentos de proceso, comandos guardados o logs.

La secuencia requiere coordinación del operador de PostgreSQL/Kafka y una ventana de solapamiento; practicarla antes de declarar disponibilidad productiva.

## Arranque y salud

- Orden: PostgreSQL/migraciones → Redpanda y topics → ClickHouse y migraciones → `taq-ingestd` → `taq-api`/consumer.
- `/health/live` comprueba el proceso; `/health/ready` exige PostgreSQL y un consumidor conectado a `events.raw`.
- Comprobar eventos ingeridos y proyectados, DLQ de app y contexto, retraso por partición, retries, tasa de errores, OTLP y cuota API. Prometheus incluye `TaQDependencyScrapeUnavailable` para targets de Redpanda, ClickHouse y OTel Collector caídos durante cinco minutos; revisar `/alerts` y documentar el incidente.
- No reanudar tráfico si la nueva proyección no mantiene una fila lógica por `id`, o si la migración no conserva muestras.

## Backups y restauración

Antes de cada migración, capturar snapshots consistentes de PostgreSQL, ClickHouse y configuración/versiones de topics; registrar retención y fecha. El restore se ensaya en un entorno aislado y debe comprobar claves/IDs lógicos, tenants, findings, auditoría, outbox y offsets. No conectar el restore a fuentes externas hasta elegir/reconciliar el offset del consumer group.

**Pendiente:** definir RPO/RTO y período de backup con el operador, automatizar snapshots cifrados y practicar el restore. No inventar garantías de recuperación hasta completar esa práctica.

## Incidentes y reversión

- Broker o DLQ indisponibles: `taq-ingestd` termina con error no cero; el supervisor reinicia con backoff. Confirmar retención raw antes de reanudar.
- PostgreSQL indisponible: API no está ready; el contexto no confirma offsets antes de transacción. Recuperar DB y verificar lag/replay antes de reducir el backlog.
- Aumento de hallazgos erróneos: mover regla a `retired`, conservar feedback/evaluaciones, congelar promociones y revisar el holdout.
- Error de acción: no hay executor externo en esta entrega; confirmar solo el estado del outbox. El dispatcher interno omite aprobaciones expiradas e incluye `expires_at`; cualquier consumidor futuro debe revalidarlo y deduplicar antes de ejecutar. No volver a publicar fuera de TaQ desde esta API.
- Esquema ClickHouse: rollback de las fases de preparación solo antes de reanudar el consumidor. Tras aceptar datos nuevos, corregir hacia delante y reconciliar.
- Exposición/uso no autorizado: revocar las API keys afectadas, restringir red, preservar logs, detener ingestión de la fuente y activar el proceso de incidentes del operador.

## Cierre pendiente para F11

Definir SLI/umbrales con mediciones reales; configurar Alertmanager y un canal de notificación con responsables; confirmar que la regla de disponibilidad dispara en runtime; retención y borrado; restore ensayado; despliegue TLS comprobado; estrategia de escalado/particionado; executor/outbox si se habilitan acciones; y simulacros de fallo. Las reglas actuales son locales y no notifican por sí mismas. Hasta entonces esta configuración es apta para desarrollo/piloto controlado, no para servicio público.
