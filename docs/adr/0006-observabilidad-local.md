# ADR 0006: Observabilidad local

- Estado: Aceptado
- Fecha: 2026-10-01

## Contexto

La plataforma necesita exponer métricas de infraestructura desde el entorno de desarrollo y dejar disponible un receptor estándar para instrumentación futura. La Fase 0 no instrumenta todavía los procesos Rust.

## Decisión

Se agregan Prometheus para recolectar métricas, Grafana para consultarlas y OpenTelemetry Collector como receptor OTLP y puente Prometheus. Redpanda publica `/public_metrics`; ClickHouse expone `/metrics` en el puerto 9363. Grafana aprovisiona una fuente Prometheus y un dashboard inicial. Los puertos publicados quedan enlazados a localhost y los datos usan volúmenes Docker.

Las versiones se fijan en `.env.example` y pueden cambiarse explícitamente en `.env`. El Collector queda preparado para métricas OTLP; la instrumentación de las aplicaciones se hará cuando los procesos que las emitan formen parte del alcance.

## Consecuencias

- Prometheus: `http://localhost:9090`; Grafana: `http://localhost:3000`.
- Credenciales locales de Grafana se configuran con `GRAFANA_ADMIN_USER` y `GRAFANA_ADMIN_PASSWORD`.
- La configuración se valida en CI mediante `docker compose config`; el arranque efectivo requiere Docker.
