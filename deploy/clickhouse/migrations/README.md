# Migraciones ClickHouse

Las migraciones de esta carpeta son manuales para volúmenes existentes. No elimines los volúmenes para aplicar cambios.

## 0003 — proyección tolerante a replay

La instalación nueva ya recibe `ReplacingMergeTree` desde `init.sql`. Para datos existentes se requiere una ventana de mantenimiento de ClickHouse, espacio libre suficiente para una copia lógica de `taq.events` y una migración completa en menos de los siete días de retención configurados para `events.raw`. Si la cola raw puede vencer antes de reanudar el consumidor, detén el despliegue y planifica una ventana segura antes de iniciar.

Si el volumen todavía no tiene `occurred_at_quality`, aplica primero la migración 0002 documentada en el README raíz. Verifica antes que no se haya creado `taq.events_pre_f2` ni `taq.events_replay_safe` de una ejecución anterior. Mantén disponibles los productores: Kafka retiene los eventos raw mientras ClickHouse está detenido, dentro de la retención del topic.

Desde PowerShell, prepara la tabla nueva y deja pausado el consumidor de ClickHouse:

```powershell
docker compose --env-file .env -f deploy/docker-compose.yml exec -T clickhouse clickhouse-client --user="$env:CLICKHOUSE_USER" --password="$env:CLICKHOUSE_PASSWORD" --multiquery --queries-file=/docker-migrations/0003_events_replay_idempotence.sql
```

La consulta final de la fase de preparación muestra `source_keys` y `replacement_rows`. Deben ser iguales. También compara una muestra de IDs, payloads y tiempos de `taq.events` con `taq.events_replay_safe FINAL`. No continúes ante diferencias; conserva el consumidor detenido y revisa el error. Para volver antes de la activación, ejecuta `0003_events_replay_idempotence_rollback.sql`.

Activa la tabla únicamente tras esa verificación:

```powershell
docker compose --env-file .env -f deploy/docker-compose.yml exec -T clickhouse clickhouse-client --user="$env:CLICKHOUSE_USER" --password="$env:CLICKHOUSE_PASSWORD" --multiquery --queries-file=/docker-migrations/0003_events_replay_idempotence_activate.sql
```

Comprueba que `taq.events_raw` está adjunta, que la vista materializada escribe en `taq.events`, que el retraso de `events.raw` baja y que `SELECT count() FROM taq.events FINAL` concuerda con `uniqExact(tuple(ingested_at_ms, id))` del histórico original. El histórico MergeTree permanece en `taq.events_pre_f2` para auditoría. No lo borres como parte de esta migración.

La reversión SQL incluida solo es segura antes de reanudar el consumidor. Después de activar y consumir filas nuevas, conserva `ReplacingMergeTree` y corrige hacia delante; una reversión requiere un plan de reconciliación que copie también los datos nuevos.

Tras 0003, `ReplacingMergeTree` elimina copias de una misma clave durante merges asíncronos; las lecturas que necesitan unicidad usan `FINAL`. Esa clave intermedia es `(ingested_at_ms, id)` y **no** evita un duplicado de la fuente reintentado con otra hora de ingesta. La migración 0004 cambia a identidad estable `id` para cubrir ese caso.

## 0004 — identidad estable para retry entre reinicios

Las instalaciones nuevas ya usan `ReplacingMergeTree(ingested_at_ms)`, `ORDER BY id` y 64 buckets por `cityHash64(id)`. Esto deduplica un evento aunque la nueva entrega reciba otro `ingested_at_ms`, siempre que el ID normalizado siga estable. No particionar por fecha: ClickHouse no reemplaza filas entre particiones.

En volúmenes que ya aplicaron 0003, detén `taq-ingestd` y el consumer `taq-api`; prepara con `0004_stable_identity_prepare.sql`. Compara `distinct_source_ids` y `replacement_rows`, además de payload, ID, hora, URL y hash de una muestra representativa. Solo después ejecuta `0004_stable_identity_activate.sql`. La tabla previa queda como `taq.events_pre_f3` y debe conservarse hasta validar backups, consultas y el nuevo consumo. Antes de activar, `0004_stable_identity_rollback.sql` permite volver a adjuntar el consumidor antiguo; tras escribir filas nuevas, reparar hacia delante y reconciliar.

La nueva organización por hash facilita reemplazo y reparación por ID, pero no da retención temporal automática. Definir TTL/borrado del dato con el operador y jurisdicción antes del piloto público.
