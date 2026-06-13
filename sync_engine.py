"""
sync_engine.py — Motor de PlayBridge (Spotify → YouTube Music)
Multi-usuario por sesión: cada visitante conecta su propia cuenta sin login.
Estado en PostgreSQL (Supabase/Render) o SQLite local si no hay DATABASE_URL.
Sync incremental, scheduler por usuario, credenciales persistidas en DB
(sobreviven redeploys: el filesystem de Render es efímero).
"""
import os
import re
import time
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheHandler
from ytmusicapi import YTMusic

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "sync.db")
SCOPE = "playlist-read-private playlist-read-collaborative user-library-read"

DEMO = os.getenv("DEMO", "0") == "1"
DATABASE_URL = os.getenv("DATABASE_URL", "")  # postgresql://user:pass@host:5432/db
USE_PG = bool(DATABASE_URL)
SCHEMA_VERSION = "2"  # v2: tablas con columna uid (multi-usuario)

if USE_PG:
    import psycopg2
    import psycopg2.extras
    from psycopg2.pool import ThreadedConnectionPool

# Pool de conexiones (2–10 conexiones, thread-safe)
_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            2, 10, DATABASE_URL,
            # keepalives TCP: evita que el pooler de Supabase corte conexiones idle
            keepalives=1, keepalives_idle=30,
            keepalives_interval=10, keepalives_count=3,
        )
    return _pool


def _checkout():
    """Saca una conexión VIVA del pool (pre-ping).
    El pooler de Supabase cierra conexiones inactivas; sin esto el pool
    entrega conexiones muertas → 'SSL SYSCALL error: EOF detected'."""
    pool = _get_pool()
    for _ in range(3):
        conn = pool.getconn()
        try:
            with conn.cursor() as ping:
                ping.execute("SELECT 1")
            conn.rollback()  # cierra la transacción del ping
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            pool.putconn(conn, close=True)  # descartar y reintentar con una nueva
    return pool.getconn()


class _SQLiteCursor:
    """Adapta sqlite3 a la interfaz psycopg2 usada aquí: %s y filas tipo dict."""
    def __init__(self, cur):
        self.cur = cur

    def execute(self, query, params=()):
        self.cur.execute(query.replace("%s", "?"), params)
        return self

    def fetchone(self):
        row = self.cur.fetchone()
        return dict(row) if row is not None else None

    def fetchall(self):
        return [dict(r) for r in self.cur.fetchall()]


@contextmanager
def db():
    """Context manager: cursor tipo dict con auto-commit/rollback.
    PostgreSQL si hay DATABASE_URL (deploy), SQLite local si no (PC/Termux/demo)."""
    if USE_PG:
        pool = _get_pool()
        conn = _checkout()
        try:
            conn.autocommit = False
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass  # conexión ya cerrada: no enmascarar el error original
            raise
        finally:
            pool.putconn(conn, close=bool(conn.closed))
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield _SQLiteCursor(conn.cursor())
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def init_db():
    with db() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
    # migración v1 → v2: las tablas viejas no tenían uid; se recrean
    if setting_get("schema_version") != SCHEMA_VERSION:
        with db() as c:
            c.execute("DROP TABLE IF EXISTS playlists")
            c.execute("DROP TABLE IF EXISTS tracks")
        setting_set("schema_version", SCHEMA_VERSION)
    with db() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS playlists(
            uid TEXT,
            sp_id TEXT,
            name TEXT,
            yt_id TEXT,
            total INTEGER DEFAULT 0,
            synced INTEGER DEFAULT 0,
            missing INTEGER DEFAULT 0,
            last_sync TEXT,
            PRIMARY KEY (uid, sp_id)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS tracks(
            uid TEXT,
            sp_track_id TEXT,
            sp_playlist_id TEXT,
            name TEXT,
            artists TEXT,
            yt_video_id TEXT,
            status TEXT DEFAULT 'pending',
            PRIMARY KEY (uid, sp_track_id, sp_playlist_id)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS playlist_tracks_cache(
            uid TEXT,
            sp_playlist_id TEXT,
            sp_track_id TEXT,
            name TEXT,
            artists TEXT,
            position INTEGER,
            PRIMARY KEY (uid, sp_playlist_id, sp_track_id)
        )""")


def setting_get(key, default=None):
    with db() as c:
        c.execute("SELECT value FROM settings WHERE key=%s", (key,))
        row = c.fetchone()
        return row["value"] if row else default


def setting_set(key, value):
    with db() as c:
        c.execute("""INSERT INTO settings(key,value) VALUES(%s,%s)
                     ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value""",
                  (key, str(value)))


class DBCacheHandler(CacheHandler):
    """Token OAuth de Spotify por usuario en la tabla settings."""
    def __init__(self, uid):
        self.uid = uid

    def get_cached_token(self):
        raw = setting_get(f"sp_token:{self.uid}")
        return json.loads(raw) if raw else None

    def save_token_to_cache(self, token_info):
        setting_set(f"sp_token:{self.uid}", json.dumps(token_info))


def _default_state():
    return {
        "running": False, "playlist": None, "current": None,
        "done": 0, "total": 0, "found": 0, "missing": 0,
        "log": [], "spotify_ok": DEMO, "yt_ok": DEMO,
        "spotify_state": "off", "spotify_user": None,
        "yt_state": "off", "yt_method": None, "yt_user": None,
        "scheduler": {"enabled": False, "hours": 24, "last_run": None},
    }


class YTAuthError(Exception):
    """La sesión de YT Music no es válida (token/headers expirados)."""


# ---------------------------------------------------------------- Engine
class SyncEngine:
    def __init__(self):
        init_db()
        self.lock = threading.Lock()
        self.states = {}      # uid -> estado en memoria (requiere 1 worker gunicorn)
        self.yt_clients = {}  # uid -> YTMusic
        threading.Thread(target=self._scheduler_loop, daemon=True).start()

    # ------------------------------------------------ estado por usuario
    def st(self, uid):
        with self.lock:
            if uid not in self.states:
                self.states[uid] = _default_state()
                fresh = True
            else:
                fresh = False
        if fresh:
            sch = self.states[uid]["scheduler"]
            sch["enabled"] = setting_get(f"sched_enabled:{uid}", "0") == "1"
            sch["hours"] = int(setting_get(f"sched_hours:{uid}", "24"))
            sch["last_run"] = setting_get(f"sched_last_run:{uid}")
            # el registro vive en memoria del proceso: si el worker se
            # reinicia (redeploy/sleep de Render), se restaura desde la DB
            try:
                self.states[uid]["log"] = json.loads(setting_get(f"log:{uid}") or "[]")
            except Exception:
                pass
            if DEMO:
                self._seed_demo(uid)
        return self.states[uid]

    def log(self, uid, msg, level="info"):
        line = {"t": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": msg}
        s = self.st(uid)
        with self.lock:
            s["log"].append(line)
            s["log"] = s["log"][-300:]
            setting_set(f"log:{uid}", json.dumps(s["log"]))

    def clear_log(self, uid):
        s = self.st(uid)
        with self.lock:
            s["log"] = []
            setting_set(f"log:{uid}", "[]")

    def snapshot(self, uid):
        """Estado para el dashboard: lee el último resultado conocido en DB,
        sin llamar a Spotify/YT Music. La validación real contra esas APIs
        ocurre solo en el momento de uso (ver verify_spotify/verify_yt)."""
        s = self.st(uid)

        # ---- Spotify: último estado conocido ----
        if DEMO:
            s["spotify_ok"] = True
            s["spotify_state"] = "ok"
            s["spotify_user"] = "Demo User"
        elif setting_get(f"sp_token:{uid}"):
            s["spotify_ok"] = setting_get(f"sp_ok:{uid}") == "1"
            s["spotify_user"] = setting_get(f"sp_user:{uid}")
            s["spotify_state"] = "ok" if s["spotify_ok"] else "expired"
        else:
            s["spotify_ok"] = False
            s["spotify_state"] = "off"
            s["spotify_user"] = None

        # ---- YT Music: último estado conocido ----
        if DEMO:
            s["yt_ok"] = True
            s["yt_state"] = "ok"
            s["yt_method"] = "oauth"
            s["yt_user"] = "Demo User"
        else:
            method = self._yt_auth_method(uid)
            if method:
                s["yt_ok"] = setting_get(f"yt_ok:{uid}") == "1"
                s["yt_state"] = "ok" if s["yt_ok"] else "expired"
                s["yt_method"] = method
                s["yt_user"] = setting_get(f"yt_user:{uid}")
            else:
                s["yt_ok"] = False
                s["yt_state"] = "off"
                s["yt_method"] = None
                s["yt_user"] = None

        with self.lock:
            return json.loads(json.dumps(s))

    # ------------------------------------------------ verificación on-demand
    def verify_spotify(self, uid):
        """Revalida el token de Spotify contra la API en el momento de uso
        (antes de listar playlists o sincronizar). Cachea el resultado ~120s
        y respeta Retry-After si Spotify devuelve 429."""
        if DEMO:
            return True
        if not setting_get(f"sp_token:{uid}"):
            return False
        now = time.time()
        retry_until = setting_get(f"sp_retry_after:{uid}")
        if retry_until and now < float(retry_until):
            return setting_get(f"sp_ok:{uid}") == "1"
        last_check = setting_get(f"sp_check:{uid}")
        if last_check and now - float(last_check) <= 120:
            return setting_get(f"sp_ok:{uid}") == "1"
        ok = False
        try:
            sp = self.sp(uid)
            me = sp.current_user() if sp else None
            if me:
                ok = True
                setting_set(f"sp_user:{uid}", me.get("display_name") or me.get("id"))
        except spotipy.SpotifyException as e:
            if e.http_status == 429:
                wait = float(e.headers.get("Retry-After", 60) or 60)
                setting_set(f"sp_retry_after:{uid}", str(now + wait))
        except Exception:
            pass
        setting_set(f"sp_check:{uid}", str(now))
        setting_set(f"sp_ok:{uid}", "1" if ok else "0")
        return ok

    def verify_yt(self, uid):
        """Revalida la sesión de YT Music en el momento de uso, con caché
        de ~120s para no repetir la llamada en clics seguidos."""
        if DEMO:
            return True
        if not self._yt_auth_method(uid):
            return False
        now = time.time()
        last_check = setting_get(f"yt_check:{uid}")
        if last_check and now - float(last_check) <= 120:
            return setting_get(f"yt_ok:{uid}") == "1"
        setting_set(f"yt_check:{uid}", str(now))
        return self.connect_yt(uid)

    # ------------------------------------------------ auth Spotify
    def oauth(self, uid):
        return SpotifyOAuth(
            client_id=setting_get("sp_client_id") or os.getenv("SPOTIFY_CLIENT_ID", ""),
            client_secret=setting_get("sp_client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET", ""),
            redirect_uri=setting_get("sp_redirect") or os.getenv("SPOTIFY_REDIRECT_URI",
                                     "http://localhost:5000/callback"),
            scope=SCOPE, cache_handler=DBCacheHandler(uid), open_browser=False,
        )

    def sp(self, uid):
        """Cliente Spotify del usuario, reconstruido desde el token en DB.
        Funciona en cualquier worker/reinicio sin estado en memoria."""
        if DEMO:
            return None
        auth = self.oauth(uid)
        if not auth.cache_handler.get_cached_token():
            return None
        return spotipy.Spotify(auth_manager=auth)

    def connect_spotify(self, uid):
        if DEMO:
            return True
        now = time.time()
        try:
            client = self.sp(uid)
            if client is None:
                return False
            me = client.current_user()  # valida token
            setting_set(f"sp_user:{uid}", me.get("display_name") or me.get("id"))
            setting_set(f"sp_ok:{uid}", "1")
            setting_set(f"sp_check:{uid}", str(now))
            self.st(uid)["spotify_ok"] = True
            return True
        except Exception as e:
            self.log(uid, f"Spotify: {self._friendly_error(e, 'Spotify')}", "error")
            setting_set(f"sp_ok:{uid}", "0")
            setting_set(f"sp_check:{uid}", str(now))
            self.st(uid)["spotify_ok"] = False
            return False

    # ------------------------------------------------ auth YT Music
    def _yt_path(self, uid):
        return os.path.join(BASE_DIR, f"browser_{uid}.json")

    def _yt_auth_method(self, uid):
        """'oauth' (login con Google), 'headers' (navegador) o None si no hay auth."""
        raw = setting_get(f"yt_auth:{uid}")
        if raw is None:
            path = self._yt_path(uid)
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        raw = f.read()
                except Exception:
                    return None
        if not raw:
            return None
        try:
            return "oauth" if "refresh_token" in json.loads(raw) else "headers"
        except Exception:
            return None

    def yt(self, uid):
        if uid in self.yt_clients:
            return self.yt_clients[uid]
        path = self._yt_path(uid)
        if not os.path.exists(path):
            saved = setting_get(f"yt_auth:{uid}")  # restaurar tras redeploy
            if not saved:
                return None
            with open(path, "w") as f:
                f.write(saved)
        # el archivo puede ser headers de navegador o token OAuth de Google
        try:
            with open(path) as f:
                is_oauth = "refresh_token" in json.load(f)
        except Exception:
            is_oauth = False
        if is_oauth:
            client = YTMusic(path, oauth_credentials=self.yt_oauth_creds())
        else:
            client = YTMusic(path)
        self.yt_clients[uid] = client
        return client

    # ------------------------------------------------ OAuth Google (YT Music)
    def yt_oauth_creds(self):
        """Credenciales del cliente OAuth de Google (tipo 'TV y dispositivos
        de entrada limitada' con YouTube Data API v3 habilitada)."""
        from ytmusicapi.auth.oauth import OAuthCredentials
        cid = setting_get("yt_client_id") or os.getenv("YT_CLIENT_ID", "")
        sec = setting_get("yt_client_secret") or os.getenv("YT_CLIENT_SECRET", "")
        if not (cid and sec):
            return None
        return OAuthCredentials(client_id=cid, client_secret=sec)

    def yt_oauth_start(self, uid):
        """Device flow: devuelve código y URL para que el usuario autorice."""
        creds = self.yt_oauth_creds()
        code = creds.get_code()
        setting_set(f"yt_device:{uid}", code["device_code"])
        return {"url": f"{code['verification_url']}?user_code={code['user_code']}",
                "code": code["user_code"]}

    def yt_oauth_poll(self, uid):
        """Consulta si el usuario ya autorizó; al confirmarse guarda el token."""
        creds = self.yt_oauth_creds()
        device = setting_get(f"yt_device:{uid}")
        if not (creds and device):
            return {"ok": False, "error": "Flujo no iniciado"}
        raw = creds.token_from_code(device)
        if "access_token" not in raw:
            return {"ok": False, "pending": True}
        token = {
            "scope": raw["scope"], "token_type": raw["token_type"],
            "access_token": raw["access_token"],
            "refresh_token": raw["refresh_token"],
            "expires_at": int(time.time()) + raw["expires_in"],
            "expires_in": raw.get("refresh_token_expires_in", raw["expires_in"]),
        }
        with open(self._yt_path(uid), "w") as f:
            json.dump(token, f)
        setting_set(f"yt_auth:{uid}", json.dumps(token))
        setting_set(f"yt_device:{uid}", "")
        self.yt_clients.pop(uid, None)
        if self.connect_yt(uid):
            return {"ok": True}
        # autorización de Google completada, pero la validación falló:
        # devolver el motivo en vez de "Flujo no iniciado" en el siguiente poll
        last_err = ""
        for entry in reversed(self.st(uid)["log"]):
            if entry["level"] == "error" and "YT Music" in entry["msg"]:
                last_err = entry["msg"]
                break
        return {"ok": False, "error": last_err or "No se pudo validar la cuenta de YT Music"}

    def connect_yt(self, uid):
        if DEMO:
            return True
        try:
            client = self.yt(uid)
            if client is None:
                return False
            info = client.get_account_info()  # valida la sesión
            name = info.get("accountName")
            if name:
                setting_set(f"yt_user:{uid}", name)
            self.st(uid)["yt_ok"] = True
            setting_set(f"yt_ok:{uid}", "1")
            return True
        except Exception as e:
            if not self._is_definite_auth_error(e):
                # error transitorio/desconocido (ej. 400 "invalid argument"
                # intermitente vía OAuth): el token recién obtenido suele ser
                # válido igual, así que no se bloquea la conexión por esto.
                self.log(uid, "YT Music: verificación de cuenta falló con un error "
                               "transitorio, pero el inicio de sesión se guardó. "
                               "Si la sincronización falla después, reconecta.", "warn")
                self.st(uid)["yt_ok"] = True
                setting_set(f"yt_ok:{uid}", "1")
                return True
            self.log(uid, f"YT Music: {self._friendly_error(e, 'YouTube Music')}", "error")
            self.st(uid)["yt_ok"] = False
            setting_set(f"yt_ok:{uid}", "0")
            self.yt_clients.pop(uid, None)
            return False

    def setup_yt_headers(self, uid, headers_raw):
        """Crea el auth de YT Music desde headers pegados por el usuario.
        Tolera el formato de Chrome 'Copiar headers de solicitud' (HTTP/2
        incluido), headers planos, o formato aplanado en una línea."""
        import ytmusicapi
        path = self._yt_path(uid)

        # ── limpiar: quitar pseudo-headers HTTP/2 (:authority, :method…), ──
        #    normalizar líneas, quedarse solo con lo esencial
        raw = headers_raw.strip()
        # si está aplanado en una línea, insertar saltos de línea
        if "\n" not in raw:
            raw = re.sub(r"\s+(?=[A-Za-z0-9-]+:\s)", "\n", raw)
        # filtrar pseudo-headers y líneas vacías
        lines = [l for l in raw.split("\n") if l.strip() and not l.startswith(":")]
        # normalizar el authorization: Chrome copia varios SAPISIDHASH/PASH;
        # ytmusicapi espera solo "SAPISIDHASH <hash>" (se regenera en cada req)
        clean = []
        for l in lines:
            if l.lower().startswith("authorization"):
                m = re.search(r"(SAPISIDHASH\s+\S+)", l)
                if m:
                    clean.append(f"authorization: {m.group(1)}")
                # si no encuentra SAPISIDHASH, descarta la línea
            else:
                clean.append(l)
        raw = "\n".join(clean)

        try:
            ytmusicapi.setup(filepath=path, headers_raw=raw)
        except Exception:
            # ── rescate manual: extraer cookie, authuser, user-agent ──
            # buscar la cookie más larga que contenga SAPISID
            m_cookie = re.search(
                r"((?:[\w.~\-]+=[^;\s]+;\s*){3,}[\w.~\-]+=[^;\s]+)", headers_raw)
            if not (m_cookie and "SAPISID" in m_cookie.group(1)):
                raise
            m_user = re.search(r"x-goog-authuser\D*(\d+)", headers_raw, re.I)
            m_ua = re.search(r"(Mozilla/5\.0[^\n]*?Safari/[\d.]+)", headers_raw)
            m_auth = re.search(r"(SAPISIDHASH\s+\S+)", headers_raw)
            cfg = {
                "accept": "*/*",
                "accept-encoding": "gzip, deflate",
                "accept-language": "es-419,es;q=0.9",
                "authorization": m_auth.group(1) if m_auth else "SAPISIDHASH 0",
                "content-type": "application/json",
                "cookie": m_cookie.group(1).strip(),
                "origin": "https://music.youtube.com",
                "user-agent": m_ua.group(1) if m_ua else
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
                "x-goog-authuser": m_user.group(1) if m_user else "0",
                "x-origin": "https://music.youtube.com",
            }
            with open(path, "w") as f:
                json.dump(cfg, f)

        with open(path) as f:
            setting_set(f"yt_auth:{uid}", f.read())
        self.yt_clients.pop(uid, None)  # forzar recarga con headers nuevos
        ok = self.connect_yt(uid)
        if not ok:
            # leer el último error que connect_yt() dejó en el log
            last_err = ""
            for entry in reversed(self.st(uid)["log"]):
                if entry["level"] == "error" and "YT Music" in entry["msg"]:
                    last_err = entry["msg"]
                    break
            raise Exception(
                f"YT Music rechazó la autenticación.\n{last_err}\n\n"
                "Posibles causas:\n"
                "• Cookie expirada — recarga music.youtube.com y copia headers FRESCOS\n"
                "• La cuenta no tiene YouTube Music disponible\n"
                "• Intenta con otro navegador o perfil de Chrome"
            )
        return True

    # ------------------------------------------------ datos Spotify
    def fetch_playlists(self, uid):
        if DEMO:
            return self._demo_playlists(uid)
        sp = self.sp(uid)
        results = sp.current_user_playlists(limit=50)
        items = results["items"]
        while results["next"]:
            results = sp.next(results)
            items.extend(results["items"])
        # algunas playlists (p.ej. generadas por Spotify) vienen sin "tracks"
        return [{"sp_id": p["id"], "name": p["name"],
                 "total": (p.get("tracks") or {}).get("total", 0)}
                for p in items if p and p.get("id")]

    def fetch_tracks(self, uid, sp_playlist_id):
        if DEMO:
            return self._demo_tracks(sp_playlist_id)
        sp = self.sp(uid)
        if sp is None:
            self.log(uid, "  ⚠ Token de Spotify no disponible. Reconecta Spotify.", "warn")
            return []
        results = sp.playlist_tracks(sp_playlist_id, limit=100)
        items = results["items"]
        while results["next"]:
            results = sp.next(results)
            items.extend(results["items"])
        out = []
        skip_no_track, skip_no_id, skip_local, skip_no_artists = 0, 0, 0, 0
        for it in items:
            # algunas respuestas de la API devuelven el track bajo "item"
            # en vez de "track" (visto en cuentas/playlists recientes)
            t = it.get("track") or it.get("item")
            if not t:
                skip_no_track += 1
                continue
            if t.get("is_local") or it.get("is_local"):
                skip_local += 1
                continue
            if not t.get("id"):
                skip_no_id += 1
                continue
            artists = t.get("artists")
            if not artists:
                # episodios de podcast u otros items sin artistas: no migrables
                skip_no_artists += 1
                continue
            out.append({
                "sp_track_id": t["id"],
                "name": t["name"],
                "artists": ", ".join(a["name"] for a in artists),
            })
        if out:
            self.log(uid, f"  {len(out)} canciones obtenidas de Spotify ({len(items)} ítems totales)", "info")
        elif items:
            detail = (f"sin objeto track: {skip_no_track}, locales: {skip_local}, "
                      f"sin id: {skip_no_id}, sin artistas: {skip_no_artists}")
            self.log(uid, f"  ⚠ 0 canciones migrables de {len(items)} ítems ({detail})", "warn")
            if skip_no_track == len(items):
                sample = json.dumps(items[0])[:400]
                self.log(uid, f"  🔍 muestra del primer ítem: {sample}", "warn")
        else:
            self.log(uid, "  ⚠ La playlist está vacía en Spotify", "warn")
        return out

    def refresh_playlists(self, uid):
        """Trae playlists de Spotify y las refleja en DB: agrega/actualiza las
        vigentes y elimina (con su progreso) las que ya no existen en Spotify."""
        pls = self.fetch_playlists(uid)
        current_ids = {p["sp_id"] for p in pls}
        with db() as c:
            for p in pls:
                c.execute("""INSERT INTO playlists(uid,sp_id,name,total) VALUES(%s,%s,%s,%s)
                             ON CONFLICT(uid,sp_id) DO UPDATE SET name=EXCLUDED.name,
                             total=EXCLUDED.total""",
                          (uid, p["sp_id"], p["name"], p["total"]))
            c.execute("SELECT sp_id, name FROM playlists WHERE uid=%s", (uid,))
            removed = [r for r in c.fetchall() if r["sp_id"] not in current_ids]
            for r in removed:
                c.execute("DELETE FROM tracks WHERE uid=%s AND sp_playlist_id=%s",
                          (uid, r["sp_id"]))
                c.execute("DELETE FROM playlist_tracks_cache WHERE uid=%s AND sp_playlist_id=%s",
                          (uid, r["sp_id"]))
                c.execute("DELETE FROM playlists WHERE uid=%s AND sp_id=%s",
                          (uid, r["sp_id"]))
        for r in removed:
            self.log(uid, f"  🗑 Playlist eliminada en Spotify: {r['name']}", "info")
        return self.playlists_view(uid)

    def playlists_view(self, uid):
        with db() as c:
            c.execute("SELECT * FROM playlists WHERE uid=%s ORDER BY name", (uid,))
            return [dict(r) for r in c.fetchall()]

    def missing_tracks(self, uid, sp_playlist_id):
        with db() as c:
            c.execute("""SELECT name, artists, sp_track_id FROM tracks
                         WHERE uid=%s AND sp_playlist_id=%s AND status='missing'
                         ORDER BY artists""", (uid, sp_playlist_id))
            return [dict(r) for r in c.fetchall()]

    def playlist_tracklist(self, uid, sp_playlist_id, refresh=False):
        """Tracklist de una playlist para el acordeón del listado.
        Se cachea en DB tras la primera consulta a Spotify (playlist_tracks_cache)
        para no repetir llamadas a la API en cada apertura."""
        if DEMO:
            return self._demo_tracks(sp_playlist_id)
        if not refresh:
            with db() as c:
                c.execute("""SELECT sp_track_id, name, artists FROM playlist_tracks_cache
                             WHERE uid=%s AND sp_playlist_id=%s
                             ORDER BY position""", (uid, sp_playlist_id))
                rows = [dict(r) for r in c.fetchall()]
                if rows:
                    return rows
        sp = self.sp(uid)
        if sp is None:
            raise RuntimeError("Conecta Spotify primero (botón Spotify)")
        results = sp.playlist_tracks(sp_playlist_id, limit=100)
        items = results["items"]
        while results["next"]:
            results = sp.next(results)
            items.extend(results["items"])
        out = []
        for it in items:
            t = it.get("track") or it.get("item")
            if not t or t.get("is_local") or it.get("is_local") or not t.get("id"):
                continue
            artists = t.get("artists")
            if not artists:
                continue
            out.append({
                "sp_track_id": t["id"],
                "name": t["name"],
                "artists": ", ".join(a["name"] for a in artists),
            })
        with db() as c:
            c.execute("""DELETE FROM playlist_tracks_cache
                         WHERE uid=%s AND sp_playlist_id=%s""", (uid, sp_playlist_id))
            for pos, t in enumerate(out):
                c.execute("""INSERT INTO playlist_tracks_cache
                             (uid, sp_playlist_id, sp_track_id, name, artists, position)
                             VALUES(%s,%s,%s,%s,%s,%s)""",
                          (uid, sp_playlist_id, t["sp_track_id"], t["name"], t["artists"], pos))
        return out

    def track_album(self, uid, sp_track_id):
        """Álbum de un track + su tracklist completo, para el acordeón
        de reproducción en el panel de canciones faltantes."""
        sp = self.sp(uid)
        if not sp:
            return None
        track = sp.track(sp_track_id)
        album = track["album"]
        items = sp.album_tracks(album["id"])["items"]
        return {
            "id": album["id"],
            "name": album["name"],
            "image": (album["images"][-1]["url"] if album.get("images") else None),
            "tracks": [{
                "sp_track_id": t["id"],
                "name": t["name"],
                "track_number": t["track_number"],
                "artists": ", ".join(a["name"] for a in t["artists"]),
            } for t in items],
        }

    def reset_playlist(self, uid, sp_playlist_id):
        """Olvida el progreso de una playlist (y su yt_id) para forzar una
        resincronización completa desde cero."""
        with db() as c:
            c.execute("DELETE FROM tracks WHERE uid=%s AND sp_playlist_id=%s",
                      (uid, sp_playlist_id))
            c.execute("""UPDATE playlists SET synced=0, missing=0, yt_id=NULL,
                         last_sync=NULL WHERE uid=%s AND sp_id=%s""",
                      (uid, sp_playlist_id))

    # ------------------------------------------------ sync core
    def start_sync(self, uid, playlist_ids):
        s = self.st(uid)
        with self.lock:
            if s["running"]:
                return False
            s["running"] = True
        threading.Thread(target=self._sync_worker, args=(uid, playlist_ids),
                         daemon=True).start()
        return True

    def _sync_worker(self, uid, playlist_ids):
        s = self.st(uid)
        try:
            if playlist_ids == "all":
                playlist_ids = [p["sp_id"] for p in self.playlists_view(uid)]
            for pid in playlist_ids:
                self._sync_one(uid, pid)
            self.log(uid, "Sincronización completada", "ok")
        except YTAuthError as e:
            self.log(uid, f"⚠ {e}. Reconecta YouTube Music y vuelve a sincronizar.", "error")
            setting_set(f"yt_check:{uid}", "0")  # forzar re-validación inmediata del estado
            setting_set(f"yt_ok:{uid}", "0")
            with self.lock:
                s["yt_ok"] = False
        except Exception as e:
            self.log(uid, f"Error fatal: {e}", "error")
        finally:
            with self.lock:
                s["running"] = False
                s["current"] = None
                s["playlist"] = None

    def _sync_one(self, uid, sp_playlist_id):
        s = self.st(uid)
        with db() as c:
            c.execute("SELECT * FROM playlists WHERE uid=%s AND sp_id=%s",
                      (uid, sp_playlist_id))
            pl = c.fetchone()
        if not pl:
            return
        name = pl["name"]
        self.log(uid, f"▶ Playlist: {name}")
        tracks = self.fetch_tracks(uid, sp_playlist_id)

        # incremental: solo pendientes / nuevas
        with db() as c:
            c.execute("""SELECT sp_track_id FROM tracks
                         WHERE uid=%s AND sp_playlist_id=%s AND status='synced'""",
                      (uid, sp_playlist_id))
            done_ids = {r["sp_track_id"] for r in c.fetchall()}
        todo = [t for t in tracks if t["sp_track_id"] not in done_ids]

        with self.lock:
            s.update(playlist=name, total=len(todo), done=0, found=0, missing=0)

        if not todo:
            if tracks:
                self.log(uid, f"  Sin cambios ({len(tracks)} ya sincronizadas)")
            self._update_counts(uid, sp_playlist_id, len(tracks))
            return

        self.log(uid, f"  {len(tracks)} canciones totales · {len(done_ids)} ya sincronizadas "
                      f"· {len(todo)} pendientes")

        yt_id = pl["yt_id"] or self._create_yt_playlist(uid, name)
        batch = []  # tracks con videoId encontrado, pendientes de añadir a YT

        def flush_batch():
            if not batch:
                return
            ok_list, reasons = self._add_to_yt(uid, yt_id, [b["vid"] for b in batch])
            with db() as c:
                for b, ok in zip(batch, ok_list):
                    status = "synced" if ok else "missing"
                    c.execute("""INSERT INTO tracks(uid,sp_track_id,sp_playlist_id,name,
                                 artists,yt_video_id,status) VALUES(%s,%s,%s,%s,%s,%s,%s)
                                 ON CONFLICT(uid,sp_track_id,sp_playlist_id) DO UPDATE SET
                                 yt_video_id=EXCLUDED.yt_video_id, status=EXCLUDED.status""",
                              (uid, b["t"]["sp_track_id"], sp_playlist_id, b["t"]["name"],
                               b["t"]["artists"], b["vid"] if ok else None, status))
            with self.lock:
                for ok in ok_list:
                    if ok:
                        s["found"] += 1
                    else:
                        s["missing"] += 1
                    s["done"] += 1
            for b, ok, reason in zip(batch, ok_list, reasons):
                if ok:
                    self.log(uid, f"  ✓ {b['label']}", "ok")
                else:
                    extra = f" — {reason}" if reason else ""
                    self.log(uid, f"  ✗ no se pudo añadir a YT Music: {b['label']}{extra}", "warn")
            batch.clear()

        for t in todo:
            label = f"{t['artists']} — {t['name']}"
            with self.lock:
                s["current"] = label
            vid = self._search_yt(uid, t)
            if vid:
                batch.append({"t": t, "vid": vid, "label": label})
            else:
                with db() as c:
                    c.execute("""INSERT INTO tracks(uid,sp_track_id,sp_playlist_id,name,
                                 artists,yt_video_id,status) VALUES(%s,%s,%s,%s,%s,%s,%s)
                                 ON CONFLICT(uid,sp_track_id,sp_playlist_id) DO UPDATE SET
                                 yt_video_id=EXCLUDED.yt_video_id, status=EXCLUDED.status""",
                              (uid, t["sp_track_id"], sp_playlist_id, t["name"],
                               t["artists"], None, "missing"))
                with self.lock:
                    s["missing"] += 1
                    s["done"] += 1
                self.log(uid, f"  ✗ No encontrada: {label}", "warn")
            # lote de 25 para no saturar la API
            if len(batch) >= 25:
                flush_batch()
            if not DEMO:
                time.sleep(0.25)

        flush_batch()
        self._update_counts(uid, sp_playlist_id, len(tracks), yt_id)

    def _update_counts(self, uid, sp_playlist_id, total, yt_id=None):
        with db() as c:
            c.execute("""SELECT COUNT(*) n FROM tracks
                         WHERE uid=%s AND sp_playlist_id=%s AND status='synced'""",
                      (uid, sp_playlist_id))
            synced = c.fetchone()["n"]
            c.execute("""SELECT COUNT(*) n FROM tracks
                         WHERE uid=%s AND sp_playlist_id=%s AND status='missing'""",
                      (uid, sp_playlist_id))
            missing = c.fetchone()["n"]
            c.execute("""UPDATE playlists SET total=%s, synced=%s, missing=%s,
                         last_sync=%s, yt_id=COALESCE(%s, yt_id)
                         WHERE uid=%s AND sp_id=%s""",
                      (total, synced, missing,
                       datetime.now().isoformat(timespec="seconds"),
                       yt_id, uid, sp_playlist_id))

    def _create_yt_playlist(self, uid, name):
        if DEMO:
            return f"DEMO_{name}"
        client = self.yt(uid)
        if client is None:
            raise YTAuthError("YT Music no está conectado")
        try:
            return client.create_playlist(name, description="Sincronizada desde Spotify")
        except Exception as e:
            if self._is_auth_error(e):
                raise YTAuthError(f"sesión de YT Music expirada al crear la playlist: {e}")
            raise

    @staticmethod
    def _is_auth_error(e):
        msg = str(e)
        return any(k in msg for k in
                    ("401", "403", "Unauthorized", "UNAUTHENTICATED", "credentials",
                     "invalid argument", "Invalid argument"))

    @staticmethod
    def _is_definite_auth_error(e):
        """Errores que indican sin ambigüedad credenciales inválidas/expiradas
        (a diferencia del 400 "invalid argument" intermitente de Google)."""
        msg = str(e)
        return any(k in msg for k in
                    ("401", "403", "Unauthorized", "UNAUTHENTICATED", "credentials"))

    @staticmethod
    def _friendly_error(e, service="Spotify"):
        """Traduce errores técnicos de las APIs (OAuth, red, rate-limit, etc.)
        a un mensaje que el usuario pueda entender y sobre el que pueda actuar."""
        msg = str(e)
        low = msg.lower()
        if "invalid_client" in low or "failed to get client" in low:
            return (f"Credenciales de {service} inválidas o no configuradas. "
                    f"Revísalas en ⚙ Configuración.")
        if any(k in low for k in ("invalid_grant", "invalid_token", "revoked", "token expired")):
            return f"La autorización de {service} expiró o fue revocada. Reconecta tu cuenta."
        if "429" in msg or any(k in low for k in ("rate/request limit", "too many requests", "rate limit")):
            return f"{service} está limitando las solicitudes por exceso de tráfico. Espera unos minutos y vuelve a intentar."
        if any(k in msg for k in ("401", "403")) or any(k in low for k in ("unauthorized", "unauthenticated", "forbidden")):
            return f"La sesión de {service} no es válida o no tiene permisos suficientes. Reconéctala."
        if any(k in low for k in ("connection", "timeout", "max retries", "failed to establish",
                                   "name resolution", "temporarily unavailable", "network")):
            return f"No se pudo conectar con {service}. Verifica tu conexión a internet e intenta de nuevo."
        if any(k in msg for k in ("500", "502", "503")) or "server error" in low:
            return f"{service} está teniendo problemas en sus servidores. Intenta de nuevo en unos minutos."
        return f"No se pudo verificar la conexión con {service}. Si el problema persiste, reconecta tu cuenta."

    def _search_yt(self, uid, track):
        if DEMO:
            time.sleep(0.08)
            return None if hash(track["sp_track_id"]) % 9 == 0 else "demo_vid"
        client = self.yt(uid)
        if client is None:
            raise YTAuthError("YT Music no está conectado")
        try:
            q = f"{track['artists']} {track['name']}"
            res = client.search(q, filter="songs", limit=3)
            for r in res:
                if r.get("videoId"):
                    return r["videoId"]
            # algunas pistas no están en el catálogo "songs" pero sí como
            # video/lyric video: reintentar con un filtro más amplio
            res = client.search(q, filter="videos", limit=3)
            for r in res:
                if r.get("videoId"):
                    return r["videoId"]
        except Exception as e:
            if self._is_auth_error(e):
                raise YTAuthError(f"sesión de YT Music expirada: {e}")
            self.log(uid, f"  búsqueda falló: {e}", "error")
        return None

    def _add_to_yt(self, uid, yt_id, video_ids):
        """Añade video_ids a la playlist YT. Devuelve (ok, reasons): dos
        listas paralelas a video_ids — ok[i] indica si ese video quedó
        realmente en la playlist, y reasons[i] el motivo del rechazo
        (None si se añadió bien)."""
        if DEMO:
            return [True] * len(video_ids), [None] * len(video_ids)
        try:
            resp = self.yt(uid).add_playlist_items(yt_id, video_ids, duplicates=False)
        except Exception as e:
            if self._is_auth_error(e):
                raise YTAuthError(f"sesión de YT Music expirada al añadir canciones: {e}")
            self.log(uid, f"  ⚠ Lote de {len(video_ids)} falló ({e}), reintentando individual…", "warn")
            return self._add_individual_retry(uid, yt_id, video_ids)

        # YT Music a veces no ejecuta la acción y en su lugar devuelve un
        # diálogo de confirmación ("One or more of the tracks are already in
        # your playlist", STATUS_FAILED) en vez de aplicar dedupeOption. Lo
        # reenviamos con la opción "Skip duplicates" (DEDUPE_OPTION_DROP_DUPLICATE),
        # que añade lo nuevo y omite lo que ya estaba — el resultado que
        # duplicates=False debía dar desde un principio.
        if isinstance(resp, dict) and resp.get("status") == "STATUS_FAILED":
            retry_body = self._extract_dedupe_skip(resp)
            if retry_body:
                resp = self.yt(uid)._send_request("browse/edit_playlist", retry_body)
                if isinstance(resp, dict) and "SUCCEEDED" in resp.get("status", ""):
                    self.log(uid, f"  + {len(video_ids)} añadidas/ya presentes en la playlist de YT Music", "ok")
                    return [True] * len(video_ids), [None] * len(video_ids)

        if isinstance(resp, dict) and "SUCCEEDED" in resp.get("status", ""):
            results = resp.get("playlistEditResults", [])
            ok, reasons = [], []
            for i in range(len(video_ids)):
                r = results[i] if i < len(results) else None
                ok.append(bool(r))
                reasons.append(None if r else "YT Music rechazó este video (no disponible o restringido)")
            n_failed = ok.count(False)
            if n_failed:
                self.log(uid, f"  ⚠ {len(video_ids) - n_failed}/{len(video_ids)} añadidas "
                              f"({n_failed} rechazadas por YT Music)", "warn")
            else:
                self.log(uid, f"  + {len(video_ids)} añadidas a la playlist de YT Music", "ok")
            return ok, reasons

        self.log(uid, f"  ⚠ Respuesta inesperada al añadir lote ({resp}), reintentando individual…", "warn")
        return self._add_individual_retry(uid, yt_id, video_ids)

    @staticmethod
    def _extract_dedupe_skip(resp):
        """Si resp es el diálogo de confirmación de duplicados de YT Music,
        devuelve el cuerpo de la petición equivalente a su botón
        "Skip duplicates" (omite los videos ya presentes y añade el resto).
        None si resp no tiene esa forma."""
        try:
            dialog = resp["actions"][0]["confirmDialogEndpoint"]["content"]["confirmDialogRenderer"]
            cmd = dialog["cancelButton"]["buttonRenderer"]["command"]["playlistEditEndpoint"]
            return {"playlistId": cmd["playlistId"], "actions": cmd["actions"]}
        except (KeyError, IndexError, TypeError):
            return None

    def _add_individual_retry(self, uid, yt_id, video_ids):
        """Reintenta añadir video_ids uno por uno. Devuelve (ok, reasons)
        igual que _add_to_yt, con el mensaje de error real de cada fallo."""
        ok, reasons = [], []
        for vid in video_ids:
            try:
                self.yt(uid).add_playlist_items(yt_id, [vid], duplicates=False)
                ok.append(True)
                reasons.append(None)
            except Exception as e:
                if self._is_auth_error(e):
                    raise YTAuthError(f"sesión de YT Music expirada al añadir canciones: {e}")
                ok.append(False)
                reasons.append(str(e))
        n_failed = ok.count(False)
        if n_failed:
            self.log(uid, f"  ⚠ {len(video_ids) - n_failed}/{len(video_ids)} añadidas tras reintento individual", "warn")
        return ok, reasons

    # ------------------------------------------------ scheduler (por usuario)
    def set_scheduler(self, uid, enabled, hours):
        setting_set(f"sched_enabled:{uid}", "1" if enabled else "0")
        setting_set(f"sched_hours:{uid}", int(hours))
        s = self.st(uid)
        with self.lock:
            s["scheduler"]["enabled"] = enabled
            s["scheduler"]["hours"] = int(hours)

    def _scheduler_loop(self):
        while True:
            time.sleep(60)
            try:
                with db() as c:
                    c.execute("""SELECT key FROM settings
                                 WHERE key LIKE 'sched_enabled:%%' AND value='1'""")
                    uids = [r["key"].split(":", 1)[1] for r in c.fetchall()]
            except Exception:
                continue
            for uid in uids:
                s = self.st(uid)
                if s["running"]:
                    continue
                hours = int(setting_get(f"sched_hours:{uid}", "24"))
                last = setting_get(f"sched_last_run:{uid}")
                if last:
                    elapsed = (datetime.now() - datetime.fromisoformat(last)).total_seconds()
                    if elapsed < hours * 3600:
                        continue
                if not (self.verify_spotify(uid) and self.verify_yt(uid)):
                    continue
                self.log(uid, "⏰ Sync automática programada", "info")
                now = datetime.now().isoformat(timespec="seconds")
                setting_set(f"sched_last_run:{uid}", now)
                with self.lock:
                    s["scheduler"]["last_run"] = now
                self.start_sync(uid, "all")

    # ------------------------------------------------ demo
    def _seed_demo(self, uid):
        demo = [("dm1", "Synthwave Nights", 14), ("dm2", "Made in Abyss OST", 9),
                ("dm3", "Coding Focus", 22), ("dm4", "Latin Classics", 11)]
        with db() as c:
            for pid, name, total in demo:
                c.execute("""INSERT INTO playlists(uid,sp_id,name,total) VALUES(%s,%s,%s,%s)
                             ON CONFLICT(uid,sp_id) DO NOTHING""", (uid, pid, name, total))

    def _demo_playlists(self, uid):
        return [{"sp_id": r["sp_id"], "name": r["name"], "total": r["total"]}
                for r in self.playlists_view(uid)]

    def _demo_tracks(self, pid):
        import random
        random.seed(pid)
        n = {"dm1": 14, "dm2": 9, "dm3": 22, "dm4": 11}.get(pid, 10)
        artists = ["The Midnight", "Kevin Penkin", "Carpenter Brut", "Gustavo Cerati",
                   "FM-84", "Perturbator", "Tycho", "Soda Stereo"]
        return [{"sp_track_id": f"{pid}_t{i}",
                 "name": f"Track {i+1}",
                 "artists": random.choice(artists)} for i in range(n)]
