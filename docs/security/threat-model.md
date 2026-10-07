# Modelo inicial de amenazas y privacidad

Estado: controles parciales para piloto local. Esta nota no sustituye una revisión legal, evaluación de impacto ni auditoría de seguridad.

## Activos y límites de confianza

| Activo | Ubicación | Datos tratados |
| --- | --- | --- |
| Evento raw | Redpanda `events.raw` | Fuente, identidad estable, entidades, payload público, horas |
| Proyección analítica | ClickHouse `taq.events` | Evento normalizado, URL pública y payload |
| Checkpoint de fuente | PostgreSQL | Cursor de reanudación, ETags y hasta 10.000 IDs RSS por feed |
| Mapa de clientes | PostgreSQL | Tenant, entidades vigiladas, alias y versiones |
| Hallazgo/feedback/acción | PostgreSQL | Evidencia/hash/URL, decisión, autor de clave y auditoría |
| Credenciales | Entorno/gestor del operador | URLs DB, broker y claves opacas de API |

## Amenazas y controles presentes

- **Lectura cruzada entre clientes:** las rutas filtran por el tenant ligado a la API key; las claves guardan hash, tienen scopes, expiración, revocación y cuotas. Las claves de solicitud y aprobación deben pertenecer a principales distintos.
- **Replay y duplicación:** cursor solo avanza después del ACK Kafka; cursor/identidad de fuente persisten en PostgreSQL; consumer commit ocurre después de contextualizar transaccionalmente. ClickHouse usa id estable como clave de reemplazo.
- **Abuso de la API:** límite de cuerpo JSON de Axum, validación de entradas, claves con cuotas por minuto y binding local por defecto. El modo públicamente enrutable valida DB, broker y bind, y falla cerrado mientras Kafka no consuma la política TLS/SASL común. PostgreSQL ya tiene `verify-full` mediante Rustls con verificación de certificado y hostname; todavía falta calificarlo en integración. Esto evita exponer por error una configuración sin transporte protegido; no verifica la topología real del proxy.
- **Efecto externo accidental:** las rutas de aprobación solo registran outbox; no existe executor que haga escrituras externas. La política de kill switch queda pendiente junto con ese executor.
- **Trazabilidad:** hallazgos conservan evento/URL/hash; feedback guarda snapshot; cambios de watchlist, regla, política, acción y revisión se auditan.
- **Poison message:** JSON inválido se envía a DLQ de contexto con hash, longitud y offset, sin copiar su payload.

## Riesgos y tareas de cierre

1. El hecho de que la fuente sea pública no autoriza automáticamente almacenar, perfilar, redistribuir o comercializar personas, texto o medios. Confirmar finalidades, jurisdicción, bases legales, licencias, atribución y contratos por cada feed antes de un piloto con clientes.
2. La retención de eventos ClickHouse no está limitada por TTL y los backups tampoco tienen ciclo de borrado. Definir plazo por clase, ejecución de borrado propagado y restauración/borrado de backups antes de uso público.
3. Las contraseñas de ejemplo de `.env.example` solo sirven para desarrollo. Producción necesita gestor externo de secretos, rotación, roles PostgreSQL/Kafka/ClickHouse de mínimo privilegio, TLS entre servicios y cifrado en reposo.
4. El cursor `seen` dura 30 días; eso evita repeticiones recientes, pero no reemplaza una retención legal ni un plan de deduplicación histórica.
5. CI configura escaneo de dependencias Rust (`cargo audit`) y secretos (Gitleaks) en push/PR y semanalmente. También genera un SBOM CycloneDX JSON 1.5 y suma SHA-256 a un artefacto CI de 30 días; aún falta ejecutar el pipeline, revisar resultados y vincular el SBOM al artefacto de release. No hay firma/procedencia criptográfica del SBOM, threat-model review externa ni reporte de incidentes ensayado todavía.
6. `TAQ_API_TLS_TERMINATED_UPSTREAM=true` es una afirmación del operador, no una prueba automática de TLS. Verificar la red y la imposibilidad de acceso directo al proceso. El modo público permanece bloqueado hasta integrar y validar PostgreSQL `verify-full` y TLS/SASL Kafka, además de gestor de secretos, rotación, mínimo privilegio y revisión del despliegue.

## No-Go público actual

No ofrecer acceso público/producción hasta cerrar por evidencia la revisión legal/privacidad, borrado/retención, TLS y secretos, escaneo/SBOM, recuperación/restore, límites medidos, CI E2E y operación de acciones. La lista de términos por fuente está en [source-terms.md](../operations/source-terms.md).
