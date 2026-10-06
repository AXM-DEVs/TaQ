# Landing de TaQ

Landing estática en español para presentar el propósito, el flujo, el estado y los principios del proyecto. No forma parte del servicio ejecutable ni implica que exista un despliegue público.

## Vista previa local

Abre `index.html` directamente en un navegador. Para servirlo en local, desde la raíz del repositorio:

```powershell
python -m http.server 8080 --directory site
```

Después abre `http://localhost:8080`. La tipografía DM Sans y Manrope se solicita a Google Fonts; si no hay conexión, se usan fuentes de respaldo del sistema. El diagrama y el ícono de TaQ son SVG locales. GSAP, ScrollTrigger y MotionPathPlugin se sirven desde `vendor/`; su procedencia y licencia están en [vendor/README.md](vendor/README.md).

El movimiento principal sigue una señal desde una fuente hasta el hallazgo en el diagrama. Al entrar en pantalla, los pasos de la cadena y el estado se revelan en grupos breves. El pulso se pausa al salir de pantalla o al cambiar de pestaña. Con `prefers-reduced-motion: reduce`, no se inicia ninguna animación; sin JavaScript, todo el contenido sigue visible.

El texto diferencia las capacidades presentes en el código de los hitos aún no validados. La landing no anuncia una fecha de publicación, un servicio productivo, métricas, licencia ni una infraestructura de alojamiento.
