# TaQ — Video promocional

Video promocional de TaQ construido con [HyperFrames](https://hyperframes.dev). 1920×1080, 68 s, 7 escenas.

## Previsualizar

```bash
npm run preview
```

Abre el navegador en `http://localhost:3000` (o el puerto que indique).

## Renderizar MP4

```bash
npm run render
```

Genera `out/taq-promo.mp4`.

## Lint / Validación

```bash
npm run lint          # errores de composición
npm run check         # lint + runtime + layout + contraste
```

## Capturar fotogramas

```bash
npx hyperframes snapshot --at 3,10,20,32,45,54,63
```

## Editar

### Textos y tiempos

Cada escena es un archivo HTML independiente en `compositions/frames/`:

| Escena | Archivo | Duración |
|--------|---------|----------|
| 01 Gancho | `01-hook.html` | 6 s |
| 02 Fuentes | `02-sources.html` | 9 s |
| 03 Contexto | `03-context.html` | 10 s |
| 04 Flujo | `04-pipeline.html` | 15 s |
| 05 Hallazgo | `05-finding.html` | 10 s |
| 06 Estado | `06-status.html` | 9 s |
| 07 Cierre | `07-closing.html` | 9 s |

Los textos están directamente en el HTML de cada escena. Edítalos y vuelve a renderizar.

### Tiempos globales

El archivo raíz `index.html` define el orden y timing de cada escena con `data-start` y `data-duration`.

### Colores y tipografía

- Fondo oscuro: `#102b33` / `#17363b`
- Teal: `#176f68`
- Papel: `#e8eee6` / `#fbfcf8`
- Ámbar (acento): `#e3b45d`
- Display: Manrope · Chrome: JetBrains Mono

### Assets

- `assets/taq-mark.svg` — marca TaQ
- `assets/fonts/` — fuentes locales (woff2)

## Estructura

```
taq-video/
├── index.html                    # composición raíz (timeline)
├── compositions/frames/          # una escena por archivo
│   ├── 01-hook.html ... 07-closing.html
├── assets/
│   ├── taq-mark.svg
│   └── fonts/
├── out/                          # salida renderizada
├── STORYBOARD.md                 # guion y tiempos
└── package.json                  # scripts npm
```
