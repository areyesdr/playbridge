# PlayBridge — Spotify → YouTube Music

App web Flask para migrar y mantener sincronizadas tus playlists.
Sync **incremental** (no duplica), scheduler automático, registro de canciones
no encontradas, instalable como **PWA** en Android. Deploy 100% gratis.

## Deploy gratis en Render + Supabase ($0/mes)

### 1. Base de datos — Supabase (PostgreSQL gratis, 500 MB)
1. [supabase.com](https://supabase.com) → **New project** (guarda la contraseña de la DB)
2. **Settings → Database → Connection string** → modo *Transaction pooler*
3. Copia el URI `postgresql://...` — será tu `DATABASE_URL`

> Las credenciales (token Spotify, headers YT) se guardan **en la DB**,
> así sobreviven a los redeploys de Render aunque el filesystem sea efímero.

### 2. Web — Render (plan free)
1. Sube este repo a GitHub
2. [render.com](https://render.com) → **New → Blueprint** → conecta el repo
   (el `render.yaml` configura todo) — o **New → Web Service** manual:
   - Build: `pip install -r requirements.txt`
   - Start: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120`
3. Variables de entorno:

| Variable | Valor |
|---|---|
| `DATABASE_URL` | connection string de Supabase |
| `SECRET_KEY` | `python3 -c "import secrets; print(secrets.token_hex(32))"` |
| `SPOTIFY_CLIENT_ID` | de developer.spotify.com |
| `SPOTIFY_CLIENT_SECRET` | de developer.spotify.com |
| `SPOTIFY_REDIRECT_URI` | `https://TU-APP.onrender.com/callback` |
| `YT_CLIENT_ID` | Client ID OAuth de Google (opcional) |
| `YT_CLIENT_SECRET` | Client Secret OAuth de Google (opcional) |

### 3. Spotify
1. [developer.spotify.com/dashboard](https://developer.spotify.com/dashboard) → **Create app**
2. Redirect URI **exacta**: `https://TU-APP.onrender.com/callback`
3. En la app desplegada: **⚙ Config** → pega Client ID/Secret → botón **Spotify** → autoriza

### 4. YouTube Music (OAuth Google — recomendado)
La forma más sencilla: **Iniciar sesión con Google** directamente desde la app.

1. **⚙ Config** → añade `YT_CLIENT_ID` y `YT_CLIENT_SECRET` (ver abajo)
2. Botón **YT Music** → **Iniciar sesión con Google**
3. Se abrirá un código y un link; ingrésalo en [google.com/device](https://google.com/device)
4. Listo — el token se renueva automáticamente

**Para obtener las credenciales Google:**
1. [console.cloud.google.com](https://console.cloud.google.com) → **APIs & Services → Credentials**
2. **CREATE CREDENTIALS → OAuth client ID → TV and Limited Input device**
3. Habilita **YouTube Data API v3** en "Enabled APIs & Services"
4. Copia el Client ID y Client Secret en **⚙ Config**

**Alternativa avanzada** (si no quieres usar OAuth):
`music.youtube.com` → F12 → Red → filtra `/browse` → click derecho → *Copiar headers de solicitud* → pegar en la opción avanzada del diálogo.

### 5. Evitar que se duerma (opcional)
El plan free de Render duerme tras 15 min sin tráfico. Solución gratis:
[uptimerobot.com](https://uptimerobot.com) → monitor HTTP a
`https://TU-APP.onrender.com/healthz` cada 10 minutos.

> ⚠️ Si se duerme igual no pasa nada: la primera visita tarda ~40 s en despertar
> y el estado está intacto en Supabase.

## Instalar como app en Android (PWA)
1. Abre la URL de Render en Chrome
2. Menú ⋮ → **Añadir a pantalla de inicio** / **Instalar app**
3. Se instala con ícono propio y abre a pantalla completa

## Uso local (PC / Termux) — sin configurar nada
Sin `DATABASE_URL` usa SQLite local automáticamente:

```bash
bash setup.sh
source venv/bin/activate && python3 app.py     # http://localhost:5000
```

Modo demo (probar la UI sin credenciales): `DEMO=1 python3 app.py`

### Arranque automático en Debian (opcional)
```bash
sudo cp -r . /opt/playbridge
sudo cp sync.service /etc/systemd/system/playbridge.service
sudo systemctl enable --now playbridge
```

## Uso

| Acción | Cómo |
|---|---|
| Cargar playlists | `↻ Refrescar de Spotify` |
| Migrar | marcar checkboxes → `Sincronizar seleccionadas` (sin selección = todas) |
| Re-sincronizar | mismo botón: solo procesa canciones **nuevas** |
| Ver no encontradas | enlace rojo `N no encontradas` en cada playlist |
| Sync automática | activar switch + intervalo en horas (persiste entre reinicios) |

## Estructura
```
app.py            servidor Flask + OAuth callback + API REST + healthz
sync_engine.py    motor: PostgreSQL/SQLite, worker en thread, scheduler, búsqueda YT
render.yaml       blueprint de Render (plan free)
templates/        index.html (vista única)
static/           style.css · app.js · manifest.json · sw.js · íconos PWA
```
