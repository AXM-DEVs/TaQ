# Transporte Seguro para TaQ — Plan de Implementación

> **Para agentes de implementación:** ejecutar este plan tarea por tarea con `superpowers:subagent-driven-development` y revisión del cambio completo al final. Las pruebas descritas se acumulan para la validación final porque el usuario pidió dejar la suite para el final.

**Goal:** Permitir que TaQ se conecte con TLS verificado y autenticación a PostgreSQL y Kafka en despliegues expuestos, conservando el modo local Compose aislado y sin cifrar como opción explícita de desarrollo.

**Architecture:** Centralizar la política de seguridad del cliente Kafka en un crate compartido, y construir un `MakeRustlsConnect` compartido para el pool deadpool PostgreSQL. El perfil local declara `PLAINTEXT` y `disable`; un perfil de despliegue debe configurar PostgreSQL `verify-full` y Kafka `SASL_SSL`, CA confiable y credenciales. El guard de `taq-api` permanece cerrado hasta que ambos clientes consuman y validen esas opciones.

**Tech Stack:** Rust 2024, Tokio, deadpool-postgres 0.14, tokio-postgres 0.7, rustls 0.23, tokio-postgres-rustls, rdkafka 0.39/librdkafka, Docker Compose.

**Spec:** `docs/security/threat-model.md`, `docs/operations/product-api.md`, `docs/operations/production-runbook.md`, `docs/roadmap/plan-implementacion-motor.md`.

## Restricciones globales

- No registrar contraseñas, contenido de DSN, claves privadas ni secretos SASL.
- La validación de certificados PostgreSQL debe verificar CA y hostname (`verify-full`); no aceptar modos que solo cifran sin validar identidad del servidor.
- La salida local de Compose debe seguir operando con sus listeners loopback/bridge de desarrollo y credenciales de ejemplo.
- `TAQ_API_PUBLICLY_ROUTABLE=true` no puede permitir startup hasta que PostgreSQL y todos los clientes Kafka aplicables usen los valores validados.
- No ejecutar suites de prueba ni pruebas E2E antes del hito F12 final solicitado por el usuario; anotar los casos concretos y correrlos juntos ahí.
- Las migraciones y los volúmenes persistentes no se modifican para implementar transporte.

## Archivos y límites

- `crates/taq-state/src/lib.rs`: configuración TLS PostgreSQL y construcción del pool compartido API/ingesta.
- `crates/taq-kafka/` (crate nuevo): parser/validador de política y función única para aplicar TLS/SASL a `rdkafka::ClientConfig`.
- `Cargo.toml` y `Cargo.lock`: dependencias, miembros del workspace y features TLS de rdkafka.
- `crates/taq-api/src/main.rs`, `crates/taq-ingest/src/pipeline.rs`, `crates/taq-dlq-replay/src/main.rs`, `crates/taq-loadgen/src/main.rs`: usar helpers compartidos en cada productor/consumidor.
- `deploy/docker-compose.yml`, `.env.example`, `deploy/taq-*.Dockerfile`: conservar configuración local y agregar soporte de build/runtime para TLS.
- `docs/operations/product-api.md`, `docs/operations/production-runbook.md`, `docs/security/threat-model.md`, `CONTEXTO_PROYECTO.md`: instrucciones y límites reales.
- `crates/taq-state/src/lib.rs`, crate Kafka nuevo y constructores de clientes: añadir los casos unitarios descritos abajo; ejecutarlos solo al final.

## Enfoque de revisión

1. DSN PostgreSQL con `verify-full` pero sin CA pública/confiable: rechazar handshake; no bajar a `NoTls`.
2. Kafka `SASL_SSL` sin CA, mecanismo permitido, usuario o contraseña: rechazar al cargar config sin mostrar el secreto.
3. Un Kafka bootstrap CSV con una entrada loopback de desarrollo: el modo público no debe aceptarlo.
4. El perfil local Compose sin TLS: debe declarar modo de desarrollo explícito y seguir permitiendo sus URLs actuales.
5. Cambiar `TAQ_API_PUBLICLY_ROUTABLE=true` dejando cualquier productor/consumidor del API en `PLAINTEXT`: startup debe fallar antes de escuchar.

---

### Tarea 1: Configuración TLS de PostgreSQL

**Archivos:**
- Modificar: `Cargo.toml`, `Cargo.lock`, `crates/taq-state/Cargo.toml`, `crates/taq-state/src/lib.rs`.
- Pruebas a añadir: módulo de configuración PostgreSQL en `crates/taq-state/src/lib.rs`.

**Interfaces:**
- `PgTlsMode::from_value(Option<&str>) -> anyhow::Result<PgTlsMode>` acepta `disable` solo para la modalidad local y `verify-full` para conexiones verificadas; rechaza cualquier otra cadena.
- `create_pg_pool(database_url: &str, config: PgPoolConfig, tls_mode: PgTlsMode, ca_file: Option<&Path>) -> anyhow::Result<PgPool>` aplica la política antes de crear el pool. `tokio-postgres 0.7` no tiene `SslMode::VerifyFull`: para el modo TaQ `verify-full`, fuerza `SslMode::Require` y usa `MakeRustlsConnect` con verificación Rustls predeterminada de cadena y hostname; `Require` por sí solo no verifica identidad.
- Todos los consumidores de `create_pg_pool` reciben el modo/CA desde variables compartidas `TAQ_PG_TLS_MODE` y `TAQ_PG_TLS_CA_FILE`.

- [x] Añadir un test que acepte `disable` y `verify-full`, y rechace `require`, `prefer`, valor desconocido y vacío explícito. (Escrito; ejecución diferida a F12.)
- [x] Añadir tests de lectura de CA: PEM vacío/inválido falla; certificado válido se agrega a una raíz confiable. (Escrito; ejecución diferida a F12.)
- [x] Añadir dependencia `tokio-postgres-rustls` compatible con `tokio-postgres` 0.7 y `rustls` 0.23, con proveedor criptográfico y raíces explícitos; no desactivar validación de hostname.
- [x] Construir `RootCertStore` desde raíces de WebPKI y, cuando `TAQ_PG_TLS_CA_FILE` esté presente, añadir sus certificados PEM como raíces adicionales; retornar errores con ruta/contexto pero sin imprimir el DSN.
- [x] Establecer `SslMode::Disable` + `NoTls` solo en el modo local explícito; para `verify-full`, forzar `SslMode::Require` e instalar el adapter Rustls con verificación predeterminada de CA y hostname en el manager de deadpool.
- [x] Usar `TAQ_PG_TLS_MODE=disable` de manera predeterminada únicamente para desarrollo; el guard público exige `verify-full`. (Compose local lo declara explícitamente.)
- [ ] Al final de F12: comprobar autenticación de servidor válida, hostname correcto, CA incorrecta y fallo cuando el servidor no ofrece TLS.

### Tarea 2: Política compartida de seguridad Kafka

**Archivos:**
- Crear: `crates/taq-kafka/Cargo.toml`, `crates/taq-kafka/src/lib.rs`.
- Modificar: `Cargo.toml`, `Cargo.lock` y manifests de crates que creen clientes `rdkafka`.
- Pruebas a añadir: `crates/taq-kafka/src/lib.rs`.

**Interfaces:**
- `KafkaSecurityConfig::from_env() -> anyhow::Result<KafkaSecurityConfig>` lee `TAQ_KAFKA_SECURITY_PROTOCOL`, `TAQ_KAFKA_SASL_MECHANISM`, `TAQ_KAFKA_SASL_USERNAME`, `TAQ_KAFKA_SASL_PASSWORD` (o `TAQ_KAFKA_SASL_PASSWORD_FILE`), `TAQ_KAFKA_TLS_CA_FILE`, `TAQ_KAFKA_TLS_CERT_FILE` y `TAQ_KAFKA_TLS_KEY_FILE`.
- `KafkaSecurityConfig::apply(&self, client: &mut ClientConfig) -> anyhow::Result<()>` solo aplica opciones; el caller sigue agregando `bootstrap.servers`, timeouts y config específica del cliente.
- Protocolos admitidos: `PLAINTEXT` local; `SSL` con CA; `SASL_SSL` con SASL permitido y credenciales completas. Mecanismos permitidos: `SCRAM-SHA-256`, `SCRAM-SHA-512` y `PLAIN` solo sobre `SASL_SSL`.

- [x] Añadir tests para combinaciones válidas/invalidas y confirmar que no se imprime ni serializa la contraseña en `Debug`/errores. (Escritos; ejecución diferida a F12.)
- [x] Crear el crate y habilitar SSL de rdkafka con su feature Cargo; añadir dependencias de sistema requeridas a la etapa builder Debian. (OpenSSL agregado a builders/runtime Debian.)
- [x] Validar archivos requeridos, protocolo y mecanismo antes de instanciar clientes; errores solo nombran la variable/configuración que falta.
- [x] Producir una `ClientConfig` saneada que use CA verificada y, si el broker requiere mTLS, certificado y llave; nunca configurar `ssl.endpoint.identification.algorithm=none`.
- [ ] Al final de F12: probar conexión con CA/certificado correcto y rechazo de CA incorrecta, SASL inválido y credenciales ausentes.

### Tarea 3: Aplicar la política Kafka a todos los caminos

**Archivos:**
- Modificar: `crates/taq-api/src/main.rs`, `crates/taq-ingest/src/pipeline.rs`, `crates/taq-dlq-replay/src/main.rs`, `crates/taq-loadgen/src/main.rs` y sus `Cargo.toml`.
- Revisar: `crates/taq-ingestd/src/config.rs` y ejecución de administración/topic-init de Compose.

**Interfaces:** cada cliente aplica `KafkaSecurityConfig` antes de `.create()`; todos leen el mismo perfil/env, sin construir protocolos distintos por binario.

- [x] Añadir tests de constructor/config que comprueben cada productor/consumidor sin conectar a broker y que el log de errores nunca contenga secretos. (Pruebas de política escritas; ejecución diferida a F12.)
- [x] Sustituir `ClientConfig::new()` aislados para API consumer, API DLQ producer, action producer, ingest producer, replay consumer/producer y loadgen producer por la función compartida.
- [ ] Asegurar que topic-init y health checks del despliegue protegido usan el mismo protocolo y secretos cuando se separen del Compose local.
- [ ] Buscar cada `ClientConfig::new()` restante y documentar si es un uso de desarrollo/fixture que intencionalmente no consume eventos reales.
- [ ] Al final de F12: confirmar todos los sitios y validar publicación, consumo, DLQ y replay usando broker con TLS/SASL.

### Tarea 4: Guard de exposición y entornos

**Archivos:**
- Modificar: `crates/taq-api/src/main.rs`, `deploy/docker-compose.yml`, `.env.example`, manifests Docker aplicables.
- Pruebas a añadir: validadores de config API/Kafka/PostgreSQL.

- [x] El perfil Compose local declara `TAQ_PG_TLS_MODE=disable` y `TAQ_KAFKA_SECURITY_PROTOCOL=PLAINTEXT`; no publicar puertos fuera de loopback por defecto.
- [x] El modo público exige PostgreSQL `verify-full`, Kafka `SASL_SSL`, CA confiable y credenciales SASL; la política Kafka validada se comparte con los clientes runtime.
- [x] Mantener error fail-closed hasta completar Tareas 1–3 y la calificación F10/F11/F12; el guard valida la configuración segura y todavía rechaza el arranque público.
- [x] Rechazar fallback de credenciales locales/de ejemplo, brokers loopback, bind loopback/puerto 0 y afirmación upstream TLS ausente. (Escrito; ramas negativas se verifican en F12.)
- [ ] Al final de F12: cubrir cada rama y hacer arranque negativo sin DB para comprobar que el guard falla antes de conectarse, y arranque positivo frente a fixture seguro.

### Tarea 5: Operación y documentación

**Archivos:**
- Modificar: `docs/operations/product-api.md`, `docs/operations/production-runbook.md`, `docs/security/threat-model.md`, `.env.example`, `CONTEXTO_PROYECTO.md`, `docs/roadmap/plan-implementacion-motor.md`.

- [x] Documentar provisión de CA/secretos por archivos o gestor, permisos y no usar valores de desarrollo en despliegues. (La rotación operativa sigue pendiente.)
- [x] Documentar compatibilidad de los certificados de PostgreSQL y broker con hostname/CA y un procedimiento de rotación sin loguear secretos. (El procedimiento sigue requiriendo ensayo con operadores.)
- [x] Conservar el perfil Compose local como desarrollo; no presentar `disable`/`PLAINTEXT` como aceptables para un endpoint público.
- [ ] Actualizar F10/NO-GO con evidencia de TLS/ACL y dejar como pendientes legales, retención/borrado, mínimo privilegio y threat review si no están cerrados.

### Tarea 6: Calificación final de transporte en F12

**Archivos:**
- Añadir fixture y pipeline según `docs/roadmap/plan-implementacion-motor.md` y `.github/workflows/ci.yml`.
- Generar evidencia en `docs/operations/validation/`.

- [ ] Ejecutar tests unitarios de configuración, formato, Clippy y build en Windows/Linux.
- [ ] Ejecutar E2E PostgreSQL con TLS validado: CA válida + hostname válida conecta; CA incorrecta, hostname incorrecto y TLS no disponible fallan.
- [ ] Ejecutar E2E Kafka con TLS/SASL: produce/consume/DLQ/replay; certificados y credenciales inválidas fallan sin filtrar secretos.
- [ ] Ejecutar Compose dev para verificar que plaintext local sigue funcionando, y Compose secure/CI para el camino público.
- [ ] Actualizar el pack de release, amenazas y roadmap con comandos/logs sanitizados y NO-GO/GO explícito; no declarar TaQ público listo si quedan gates de privacidad, borrado, restore, CI o operación sin evidencia.
