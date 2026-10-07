# Handoff de TaQ para reanudar el 23 de octubre de 2026

## Objetivo

Retomar el cierre técnico sin perder el estado actual y sin confundir implementación con aceptación. TaQ es gratuito para su uso previsto, pero sigue en **NO-GO para publicación abierta** hasta cerrar privacidad, operación y calificación con evidencia.

## Punto de partida

- Repositorio: `AXM-DEVs/TaQ`; rama de trabajo `codex/taq-postgres-tls`.
- Último commit remoto observado: `4c938d3 docs: refresh Graphify for secure transport`.
- Transporte PostgreSQL TLS y Kafka TLS/SASL ya está implementado en código, pero la revisión del 6 de octubre fue estática: **no se compiló ni se ejecutaron pruebas de esos cambios**.
- La protección de arranque público sigue cerrada a propósito. No quitar el fail-closed ni anunciar disponibilidad pública.
- El cambio local previo en `.vscode/codementor-learning.json` pertenece al entorno de aprendizaje; conservarlo y no incluirlo automáticamente en commits.
- Las pruebas quedan acumuladas para F12 por instrucción del propietario. Esta pausa no cambia esa decisión.

## Orden de reanudación

### 1. Recuperar contexto y checkout

```powershell
git status --short --branch
git log -5 --oneline --decorate
git fetch origin
```

Confirmar que se continúa en `codex/taq-postgres-tls`, inspeccionar los cambios locales y conservar `.vscode/codementor-learning.json`. No hacer reset, limpieza de Docker ni migración de volúmenes persistentes.

### 2. Cerrar implementación y documentación F10/F11 antes de calificar

- Revisar `docs/superpowers/plans/2026-10-06-transporte-seguro.md` y completar lo que no requiera decisiones del propietario: topic-init/health checks para perfiles TLS/SASL, configuración documentada de CA y secretos montados, rotación y rechazo de fallbacks inseguros.
- Revisar el broker y PostgreSQL reales del despliegue elegido. La variable que afirma TLS en el proxy no prueba que el proxy esté configurado; registrar la topología comprobable y mantener el guard cerrado ante dudas.
- Diagnosticar el build de imágenes desde una base limpia y verificada. El último bloqueo fue bibliotecas vacías/truncadas dentro de la imagen Debian; no asumir que aumentar el espacio lo resuelve ni reparar el VHDX mediante reset.
- Hacer explícita la migración de volúmenes existentes. PostgreSQL persistente carecía de migraciones 0002–0004 y ClickHouse conserva una tabla `MergeTree`; respaldar, revisar impacto de migración 0003/0004 y preservar filas antes de cualquier cambio.
- Diagnosticar el timeout observado en `/health/live` con dependencias activas y logs sanitizados antes de cambiar el pool o los workers.

### 3. Resolver decisiones de operación y privacidad con AXM

No inventar las decisiones que fijan obligaciones o garantías. Antes de abrir F12, registrar:

- responsable del servicio y jurisdicción aplicable;
- clases de datos admitidas, términos de cada fuente y retención por clase;
- alcance y mecanismo de borrado propagado, incluidas copias y backups;
- despliegue/región, destinatario de alertas y respuesta a incidentes;
- objetivos RPO/RTO y disponibilidad deseada;
- licencia y declaración SPDX de TaQ (los crates aparecen como `UNLICENSED` y el SBOM lo reportó como valor inválido).

Si alguna decisión sigue abierta, documentarla como bloqueo de release. El uso gratuito no elimina estos gates.

### 4. Preparar F12 y ejecutar una sola calificación ordenada

Cuando implementación, configuración y decisiones estén listas, ejecutar y registrar la matriz en `docs/operations/validation/`:

- formato, `cargo check`, Clippy y `cargo test` del workspace con lockfile;
- autenticación TLS PostgreSQL: CA/hostname válidos e inválidos, servidor sin TLS;
- Kafka: SSL/SASL_SSL, CA incorrecta, SASL inválido/ausente, mTLS si se habilita, publicación/consumo/DLQ/replay;
- guard público: caminos positivos y negativos, incluyendo dependencias fuera de servicio antes de abrir el socket;
- Gitleaks, SBOM válido y escaneo de dependencias; CI Linux limpio;
- Compose e imágenes del perfil de aplicación, migraciones sobre base desechable y luego respaldo/restauración controlados de datos persistentes;
- E2E desde fuente fixture hasta API/almacenamiento, reinicios, broker/DB/red/disco fallidos y replay;
- benchmark reproducible con hardware, versiones, duración y resultado declarados; no publicar claims no medidos;
- restore en entorno limpio, rollback, métricas/alertas con receptor y responsable;
- revisión de aislamiento multi-tenant, gold set, precisión/recall, licencia y términos de fuentes.

Mantener datos/credenciales reales fuera de logs y reportes. Separar explícitamente **PASÓ**, **FALLÓ**, **NO EJECUTADO** y **BLOQUEADO**. Un resultado unitario no sustituye E2E ni restore.

### 5. Decidir Go/No-Go

Mantener **NO-GO** ante cualquiera de estos casos: prueba pendiente o fallida en el camino crítico; borrado/retención sin evidencia; licencia/legal sin resolver; secretos de desarrollo aceptados; transporte cifrado no verificado; migraciones o restore sin ensayo; alerta sin destinatario; fuga entre tenants; fallos silenciosos; artefactos sin procedencia verificable. Solo cambiar el estado público al presentar el pack de evidencia y registrar el responsable que acepta cada riesgo residual.

## Referencias de trabajo

- Plan de transporte: `docs/superpowers/plans/2026-10-06-transporte-seguro.md`.
- Roadmap funcional: `docs/roadmap/plan-implementacion-motor.md`.
- Contexto de arquitectura y situación detallada: `CONTEXTO_PROYECTO.md`.
- Runbook F11: `docs/operations/production-runbook.md`.
- Validación F12 histórica: `docs/operations/validation/f12-local-2026-10-04.md`.
