# API de producto (F5–F9)

`taq-api` sirve la API v1 y ejecuta el consumidor contextualizador de `events.raw`. Requiere las migraciones PostgreSQL 0001_source_cursors.sql, 0002_product_domain.sql, 0003_rule_evaluation_provenance.sql y 0004_rule_dataset_samples.sql; Compose las instala al crear un volumen nuevo. Un volumen existente necesita una copia de seguridad y aplicación manual de cada migración en orden. La 0003 invalida para promoción los resultados históricos basados en contadores declarados por el cliente, conservándolos como evidencia marcada. La 0004 marca los manifests anteriores como `metadata_only_v0`; registra un `dataset_version` nuevo para incorporar las muestras verificables. Para el modo local integrado, inicia `docker compose --env-file .env -f deploy/docker-compose.yml --profile app up -d --build taq-ingestd taq-api`; en ese perfil el puerto del API se mapea solo a `127.0.0.1`.

Para trabajar localmente, inicia Compose y luego el servicio:

```powershell
$env:TAQ_PG_TLS_MODE = 'disable'
$env:TAQ_KAFKA_SECURITY_PROTOCOL = 'PLAINTEXT'
$env:TAQ_DATABASE_URL = 'postgres://taq:taq_dev@127.0.0.1:5432/taq'
cargo run -p taq-api
```

La shell no carga `.env` automáticamente; conserva estas variables para los comandos de provisionamiento siguientes. El API se enlaza a `127.0.0.1:8080` por defecto. Crea un tenant y una clave limitada, en vez de asignar todas las capacidades a un mismo principal:

```powershell
cargo run -p taq-api -- --provision-tenant "Piloto AXM" "watchlist-writer" "watchlist:read,watchlist:write"
cargo run -p taq-api -- --create-key '<tenant_uuid>' "reviewer" "findings:read,findings:review,feedback:write,actions:approve"
```

Cada clave aparece una vez por stdout; la base almacena solo su hash BLAKE3. Guarda la clave en un gestor de secretos, no en el repositorio ni en el historial compartido. Revócala por UUID de clave con `DELETE /v1/api-keys/{key_id}` y un principal con `api_keys:admin`. El middleware verifica tenant, expiración, estado, scope y cuota distribuida por minuto en PostgreSQL.

`docs/api/openapi.yaml` describe las rutas. Listados de hallazgos, acciones y reglas aceptan `limit` (máximo 100) y `offset`; los cuerpos JSON protegidos se limitan a 1 MiB. `taqctl` usa el mismo API; `TAQ_API_URL` y `TAQ_API_KEY` vienen del entorno:

```powershell
$env:TAQ_API_KEY = '<clave>'
cargo run -p taq-api --bin taqctl -- watchlist list
cargo run -p taq-api --bin taqctl -- findings list
```

`taq-api` consume `events.raw`, hace coincidencia canónica/exacta y por dominio IDNA, guarda el contexto versionado y crea hallazgos para reglas `active`. Para que aparezcan hallazgos, un operador debe registrar una versión de regla y recorrer las puertas de evaluación antes de activarla. Registra un dataset enviando hasta 1.000 muestras completas a `POST /v1/rule-datasets`. Cada muestra contiene un evento TaQ, entidad vigilada existente en el tenant, versión de watchlist, snapshot de aliases normalizados, etiqueta (`alertable`/`not_alertable`) y split (`gold`/`temporal_holdout`); cada split debe tener ambas etiquetas. El manifest ordena las muestras y aliases antes de calcular BLAKE3, así que el hash no depende del orden de entrada. TaQ guarda las muestras, rechaza reutilizar el mismo `dataset_version` con otro contenido y devuelve 409 si el mismo manifest ya tiene otra versión. La evaluación solicita solo versión de regla y dataset: TaQ comprueba hash/conteo, vuelve a ejecutar el matching determinista contra cada snapshot y calcula TP/FP/FN/TN, precisión, recall y falsos positivos por mil. Evalúa ambos splits, persiste resultados con el hash del dataset y versión de guardrails, y devuelve `temporal_drift` como diferencia holdout menos gold para esas métricas. Esa diferencia es descriptiva; los guardrails se aplican por separado a cada split y no existe aún política de deriva independiente. No se aceptan métricas del cliente. Requiere guardrails explícitos antes de evaluar; una promoción solo procede con evidencia `taq_runner_v1` que pasa los criterios actuales en ambos splits. La regla solo crea hallazgos; ninguna integración externa se ejecuta.

Una acción solicitada se mantiene `pending`. Un principal distinto puede aprobarla; la transacción cambia el estado, crea el outbox y registra la auditoría. La expiración afecta tanto a solicitudes pendientes como a aprobaciones todavía no despachadas; el dispatcher solo reclama aprobaciones vigentes y las vencidas quedan auditadas y fuera de la cola. `TAQ_ACTION_DISPATCH_ENABLED=false` mantiene el dispatcher interno apagado por defecto. Si se habilita, el dispatcher publica mensajes aprobados a `actions.approved`, con retries y clave idempotente por action ID. **No hay executor externo en esta entrega**: el topic queda dentro de TaQ y no envía correo ni ejecuta una integración. Cualquier consumidor futuro debe comprobar `expires_at`, la autorización vigente y la idempotencia antes de ejecutar un efecto.

El modo públicamente enrutable valida la configuración antes de conectarse o abrir el listener. Si `TAQ_API_PUBLICLY_ROUTABLE=true`, exige `TAQ_DATABASE_URL`, `TAQ_KAFKA_BROKER` y `TAQ_API_BIND` explícitos; rechaza credenciales PostgreSQL/SASL de desarrollo, brokers loopback conocidos, bind loopback/puerto 0, PostgreSQL distinto de `verify-full`, Kafka distinto de `SASL_SSL` y ausencia de `TAQ_API_TLS_TERMINATED_UPSTREAM=true`. PostgreSQL valida cadena y hostname con Rustls; los clientes Kafka comparten CA/SASL/mTLS y verifican hostname. Después de esa validación, el arranque público sigue bloqueado hasta completar la calificación F12 y los gates F10/F11. La pila Compose incluida permanece en modo local con `disable`/`PLAINTEXT`, puerto API ligado a loopback y secretos/certificados locales bajo `./secrets` (gitignored), montados como solo lectura en `/run/taq-secrets`. F10/F11/F12 incluyen además la revisión de red/proxy, privacidad, retención/borrado, secretos, restore y CI. La bandera upstream es una afirmación del operador, no prueba la topología ni impide acceso directo.

## Límites conocidos

- El consumidor trabaja con offsets manuales. Confirma después del commit PostgreSQL por tenant; reintentos son idempotentes por clave del finding y evento contextualizado.
- Payloads de eventos con JSON inválido se envían al topic `events.context_dead_letter` como metadatos, tamaño y hash. El payload original no se copia a ese topic.
- La ventana de replay de Redpanda es siete días en desarrollo. Si una interrupción supera esa retención, hace falta una fuente con replay más largo o se pierde el histórico del topic.
- Las métricas API/contexto y lag son métricas del proceso; CI debe comprobar que OTLP llega a Prometheus antes de declarar F4 listo.
- Las cuotas se aplican por clave y minuto, no por usuario humano autenticado. La identidad auditada es el `principal` inmutable de la clave.
