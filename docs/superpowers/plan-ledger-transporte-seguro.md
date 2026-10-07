# Ledger: transporte seguro

Plan: [2026-10-06-transporte-seguro.md](./plans/2026-10-06-transporte-seguro.md)

## Pre-flight y conflictos de tareas

| Tareas/ámbito | Archivos/interfaz compartidos | Resultado de revisión | Decisión |
| --- | --- | --- | --- |
| Tarea 1 ↔ Tarea 2 | `Cargo.toml`, `Cargo.lock` | Ambas añaden dependencias distintas al workspace. | Ejecutar secuencialmente; resolver lockfile antes de continuar. |
| Tarea 1 ↔ Tarea 4 | `crates/taq-api/src/main.rs`, variables PostgreSQL | Tarea 1 configura el pool; Tarea 4 exige los modos seguros. | Tarea 4 queda después de cablear TLS. Guard actual falla cerrado mientras tanto. |
| Tarea 2 ↔ Tarea 3 | `Cargo.toml`; crate nuevo consumido por los clientes | Tarea 3 depende de la interfaz `KafkaSecurityConfig` de Tarea 2. | Secuencial: crate/config primero, integración luego. |
| Tarea 3 ↔ Tarea 4 | `crates/taq-api/src/main.rs`, Compose/variables | Guard depende de que todos los constructores consuman la política compartida. | Tarea 4 después de revisar cada `ClientConfig::new()`. |
| Tarea 1 | `taq-state` + dependencias TLS | El pool deadpool guarda `Connect` type-erased; puede tener adapter Rustls y `PgConfig` decide `Disable` o `VerifyFull`. | Usar un único pool type y TLS connector verificador; nunca volver a NoTls en modo verify-full. |
| Tarea 2 | rdkafka SSL/SASL | Se habilitó `ssl` y `cargo fetch` resolvió `openssl-sys` en el lockfile. SCRAM/PLAIN están restringidos por política y aún requieren build/handshake de F12. | Mantener el guard público cerrado hasta evidencia de build y handshake, sin inferir soporte del crate por feature. |

## Rulings

- Ruling: mantener perfil local plaintext explícito y no modificar volúmenes — la instalación local actual usa servicios de desarrollo — coste si falla: el usuario debe migrar `.env` para marcar perfil.
- Ruling: PostgreSQL público solo `verify-full`, no `require` — cifrar sin verificar hostname/CA deja al cliente vulnerable a intermediario — coste si falla: algunas CAs privadas requieren configurarlas explícitamente.
- Ruling: mapear el modo TaQ `verify-full` a `SslMode::Require` más `MakeRustlsConnect` con su verificador predeterminado; `tokio-postgres 0.7.18` no define `SslMode::VerifyFull`, y `Require` aislado no verifica identidad — coste si la verificación predeterminada no enlazara el hostname esperado: se debe detener la implementación antes de abrir el guard público.
- Ruling: `TAQ_PG_TLS_CA_FILE` añade CA privadas a las raíces WebPKI del sistema — permite CAs públicas y privadas en el mismo despliegue — coste: los roots públicos siguen siendo confiables; operadores que requieran exclusividad necesitan una decisión/configuración futura explícita.
- Ruling: tests y pruebas de red se ejecutarán agrupados en F12 — el usuario dejó la suite al final — coste si falla: errores de integración se descubren tarde, así que el guard permanece bloqueado hasta ese hito.
- Ruling: conservar la API pública bloqueada mientras falta TLS/SASL — el código actual usa `NoTls` y librdkafka sin SSL — coste si falla: no habrá despliegue público antes de completar e integrar todos los clientes.

## Estado

- Tarea 1 implementada, pendiente de verificación F12: pool PostgreSQL admite `disable` explícito y `verify-full` mediante Rustls, raíces WebPKI y CA PEM adicional; API y cursor store leen el mismo modo. Se agregó fixture ISRG Root X1 y casos unitarios, aún sin ejecutar.
- Se actualizó `Cargo.lock` con `cargo fetch`; las dependencias TLS ya están resueltas en lockfile. No se ha compilado ni probado el código.
- El `SslMode` fue corregido para importar desde `tokio_postgres::config`; confirmado contra el código local de `tokio-postgres 0.7.18`.
- Compose declara explícitamente `TAQ_PG_TLS_MODE=disable` en servicios locales. El montaje de `TAQ_PG_TLS_CA_FILE` y perfil seguro quedan pendientes de Tarea 4.
- Tarea 2 implementada, pendiente de validación F12: `taq-kafka` valida `PLAINTEXT`, `SSL` y `SASL_SSL`, CA, SASL y pareja opcional mTLS; aplica verificación de hostname (`https`) y redacta credenciales en `Debug`/errores. Contraseña SASL acepta variable o archivo secreto (nunca ambos); Compose pasa la ruta al mount read-only. Vacíos opcionales se normalizan como ausentes para Compose; valores no-Unicode fallan con error sanitizado. OpenSSL quedó en Cargo.lock e imágenes Debian. Pruebas escritas, no ejecutadas.
- Tarea 3 implementada, pendiente de validación F12: los siete clientes Kafka de API, ingest, replay y loadgen aplican la configuración común antes de crearse; el único `KafkaPipeline::new` pasa la política validada desde ingestd. Los comandos actuales de topic-init/health siguen perteneciendo al Compose local PLAINTEXT.
- Tarea 4 parcialmente implementada: Compose declara `disable`/`PLAINTEXT`, restringe API a loopback por defecto y pasa envs TLS/SASL; `./secrets` se monta read-only en `/run/taq-secrets` y está ignorado por Git. Guard público valida PostgreSQL `verify-full`, Kafka `SASL_SSL`, credenciales no-placeholder, broker/bind explícitos y la afirmación proxy TLS; aun con config válida permanece bloqueado hasta calificar F10/F11/F12. Los valores externos/topología todavía requieren configuración/validación F12.
- Límite abierto del guard: el parser detecta localhost, direcciones IP loopback (también formatos IPv4 no canónicos y mapped IPv6) y rechaza hosts inválidos, pero no resuelve dominios DNS arbitrarios que pudieran apuntar a loopback/alias local. El modo público sigue bloqueado; decidir allowlist/validación de DNS antes de levantar ese bloqueo.
- No ejecutar pruebas/build hasta F12. Al cerrar cada tarea, actualizar este ledger y la sección de estado del plan sin marcar validación como completada.
- El launcher Graphify falla con `uv trampoline failed to canonicalize script path`; se actualizó `graphifyy` a 0.9.79 y query/update se ejecutaron por Python de su entorno. El grafo quedó actualizado a 2936 nodos/5352 relaciones y `graphify-out/` fue generado por la herramienta. `rustfmt` tampoco fue posible por error de acceso del entorno; revisión estática y `git diff --check` sí se hicieron, falta formato/build/test final.
- No ejecutar pruebas hasta el hito F12 final.
