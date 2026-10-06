# ADR 0004: Versionado explícito del esquema de eventos

- Estado: aceptado
- Fecha: 2026-10-01

## Contexto

Los eventos se conservan en Redpanda como fuente inmutable y se proyectan a ClickHouse. Cambiar el contrato después de acumular datos requiere lectores compatibles y una migración de las proyecciones.

## Decisión

`Event` incluye `schema_version` como entero sin signo de 16 bits. La versión inicial es `1`; al deserializar mensajes históricos que no incluyan el campo, Rust y ClickHouse aplican el valor predeterminado `1`. La versión `2` añade `occurred_at_quality`, que distingue timestamps obtenidos de la fuente (`source`), sustituidos por hora de captura (`capture_time`) y heredados sin información (`unknown`). Los lectores aceptan el campo ausente como `unknown`; ClickHouse recibe la columna con ese valor por defecto. Un cambio incompatible incrementará la versión y deberá desplegar primero lectores compatibles.

## Consecuencias

- Los consumidores pueden seleccionar la lógica de decodificación por versión.
- El valor por defecto conserva compatibilidad con eventos ya escritos antes de este cambio.
- ClickHouse solo aplica el `DEFAULT` en tablas nuevas; los volúmenes existentes requieren migración explícita o reinicialización de desarrollo.
- La migración `deploy/clickhouse/migrations/0002_event_timestamp_quality.sql` modifica el Kafka Engine y recrea la vista materializada. Aplicarla con el demonio detenido antes de desplegar productores de versión 2.

