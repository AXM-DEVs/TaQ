# Fuentes: límites técnicos y condiciones pendientes

Revisión documental realizada el 2026-10-04. Este inventario sirve para operar el prototipo; no es dictamen legal ni autoriza uso comercial. Confirmar de nuevo los términos de cada endpoint y del contenido concreto antes de habilitar un cliente o publicar TaQ.

| Fuente configurada | Capacidades técnicas observadas | Condición operativa / lanzamiento |
| --- | --- | --- |
| Wikimedia EventStreams | SSE; admite Last-Event-ID y publica una ventana de historial variable (aprox. 7–31 días). Emite canaries sintéticos que TaQ descarta y checkpointa. | Preservar procedencia/enlace al artículo. Texto y archivos multimedia pueden tener licencias distintas; atribuir, indicar cambios y cumplir la licencia de cada material. No asumir que todo el payload se puede redistribuir igual. |
| Bluesky Jetstream v2 | WebSocket; TaQ usa el host v2 y el wire JSON compatible /subscribe con cursor secuencial por evento. El lookback por defecto publicado es 36 horas; cursores antiguos de la base se retoman temporalmente durante la transición. | Minimizar datos personales y conservar procedencia. Revisar términos, privacidad y derechos de autor para la función y jurisdicción antes de uso comercial. Los cortes que excedan la ventana disponible no se recuperan con garantía. |
| RSS BBC (valor local de ejemplo) | Polling con ETag/Last-Modified cuando el feed los entrega; 304 Not Modified evita descargar cambios ausentes. | Solo fixture de desarrollo por defecto. Los términos BBC indican que el uso empresarial de sus RSS requiere permiso y puede tener coste. No activar BBC para un servicio empresarial/comercial sin autorización/licencia. |

## Referencias técnicas y de términos

- Wikimedia EventStreams explica SSE, Last-Event-ID, el historial disponible y que los clientes descarten canaries: https://wikitech.wikimedia.org/wiki/EventStreams.
- Wikimedia Foundation Terms of Use explica licencias y atribución, con posibles requisitos específicos por contenido: https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use.
- Jetstream oficial documenta el wire compatible /subscribe, cursor secuencial, traducción de timestamps y lookback por defecto de 36 horas: https://github.com/bluesky-social/jetstream/blob/main/docs/README.md.
- La documentación oficial del cliente Bluesky diferencia los hosts legacy v1 y el servicio v2: https://github.com/bluesky-social/bsky/blob/main/packages/jetstream/README.md.
- Términos de servicio de Bluesky: https://bsky.social/about/support/tos.
- Términos BBC, sección Metadata and RSS feeds: https://downloads.bbc.co.uk/usingthebbc/bbc_terms_of_use_31March2022english.pdf.
- RFC 9110, sección 13: semántica de If-None-Match, If-Modified-Since y 304: https://datatracker.ietf.org/doc/html/rfc9110#section-13.

La ventana de historial de Wikimedia varía por stream y no es una garantía contractual. TaQ no monitoriza actualmente la ausencia de canaries. El host Jetstream v2 y su lookback son operados por terceros; TaQ mantiene idempotencia para replays y no debe prometer recuperación después de la ventana disponible.

## Bloqueos previos a activar una fuente en producción

1. Identificar la entidad que opera TaQ, jurisdicción, base de tratamiento, finalidad y clientes.
2. Documentar para cada dato guardado su procedencia/licencia, atribución, retención, redistribución y borrado.
3. Obtener autorización comercial de BBC o sustituir el feed de ejemplo por una fuente autorizada para el uso previsto.
4. Definir revisión periódica de términos, rate limits y avisos de cambios; desactivar la fuente si sus condiciones no se pueden cumplir.
