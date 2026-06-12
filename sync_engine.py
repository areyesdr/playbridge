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
        "scheduler": {"enabled": False, "hours": 24, "last_run": None},
    }


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
            if DEMO:
                self._seed_demo(uid)
        return self.states[uid]

    def log(self, uid, msg, level="info"):
        line = {"t": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": msg}
        s = self.st(uid)
        with self.lock:
            s["log"].append(line)
            s["log"] = s["log"][-300:]

    def snapshot(self, uid):
        s = self.st(uid)
        # flags perezosos: validar cada ~30s que el token siga vivo
        now = time.time()
        if s["spotify_ok"]:
            last_check = setting_get(f"sp_check:{uid}")
            if not last_check or now - float(last_check) > 30:
                try:
                    sp = self.sp(uid)
                    if sp:
                        sp.current_user()
                        s["spotify_ok"] = True
                    else:
                        s["spotify_ok"] = False
                except Exception:
                    s["spotify_ok"] = False
                setting_set(f"sp_check:{uid}", str(now))
        else:
            s["spotify_ok"] = DEMO or bool(setting_get(f"sp_token:{uid}"))
        if not s["yt_ok"]:
            s["yt_ok"] = DEMO or setting_get(f"yt_ok:{uid}") == "1"
        with self.lock:
            return json.loads(json.dumps(s))

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
        try:
            client = self.sp(uid)
            if client is None:
                return False
            client.current_user()  # valida token
            self.st(uid)["spotify_ok"] = True
            return True
        except Exception as e:
            self.log(uid, f"Spotify: {e}", "error")
            self.st(uid)["spotify_ok"] = False
            return False

    # ------------------------------------------------ auth YT Music
    def _yt_path(self, uid):
        return os.path.join(BASE_DIR, f"browser_{uid}.json")

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
        return {"ok": self.connect_yt(uid)}

    def connect_yt(self, uid):
        if DEMO:
            return True
        try:
            client = self.yt(uid)
            if client is None:
                return False
            client.get_library_playlists(limit=1)  # valida headers
            self.st(uid)["yt_ok"] = True
            setting_set(f"yt_ok:{uid}", "1")
            return True
        except Exception as e:
            self.log(uid, f"YT Music: {e}", "error")
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
        for it in items:
            t = it.get("track")
            if not t or not t.get("id"):
                continue
            out.append({
                "sp_track_id": t["id"],
                "name": t["name"],
                "artists": ", ".join(a["name"] for a in t["artists"]),
            })
        self.log(uid, f"  {len(out)} canciones obtenidas de Spotify", "info")
        return out

    def refresh_playlists(self, uid):
        """Trae playlists de Spotify y las refleja en DB (sin borrar estado)."""
        pls = self.fetch_playlists(uid)
        with db() as c:
            for p in pls:
                c.execute("""INSERT INTO playlists(uid,sp_id,name,total) VALUES(%s,%s,%s,%s)
                             ON CONFLICT(uid,sp_id) DO UPDATE SET name=EXCLUDED.name,
                             total=EXCLUDED.total""",
                          (uid, p["sp_id"], p["name"], p["total"]))
        return self.playlists_view(uid)

    def playlists_view(self, uid):
        with db() as c:
            c.execute("SELECT * FROM playlists WHERE uid=%s ORDER BY name", (uid,))
            return [dict(r) for r in c.fetchall()]

    def missing_tracks(self, uid, sp_playlist_id):
        with db() as c:
            c.execute("""SELECT name, artists FROM tracks
                         WHERE uid=%s AND sp_playlist_id=%s AND status='missing'
                         ORDER BY artists""", (uid, sp_playlist_id))
            return [dict(r) for r in c.fetchall()]

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
            self.log(uid, f"  Sin cambios ({len(tracks)} ya sincronizadas)")
            self._update_counts(uid, sp_playlist_id, len(tracks))
            return

        yt_id = pl["yt_id"] or self._create_yt_playlist(uid, name)
        added_ids = []

        for t in todo:
            label = f"{t['artists']} — {t['name']}"
            with self.lock:
                s["current"] = label
            vid = self._search_yt(uid, t)
            if vid:
                added_ids.append(vid)
                status = "synced"
                with self.lock:
                    s["found"] += 1
                self.log(uid, f"  ✓ {label}", "ok")
            else:
                vid = None
                status = "missing"
                with self.lock:
                    s["missing"] += 1
                self.log(uid, f"  ✗ No encontrada: {label}", "warn")
            with db() as c:
                c.execute("""INSERT INTO tracks(uid,sp_track_id,sp_playlist_id,name,
                             artists,yt_video_id,status) VALUES(%s,%s,%s,%s,%s,%s,%s)
                             ON CONFLICT(uid,sp_track_id,sp_playlist_id) DO UPDATE SET
                             yt_video_id=EXCLUDED.yt_video_id, status=EXCLUDED.status""",
                          (uid, t["sp_track_id"], sp_playlist_id, t["name"],
                           t["artists"], vid, status))
            with self.lock:
                s["done"] += 1
            # lote de 25 para no saturar la API
            if len(added_ids) >= 25:
                self._add_to_yt(uid, yt_id, added_ids)
                added_ids = []
            if not DEMO:
                time.sleep(0.25)

        if added_ids:
            self._add_to_yt(uid, yt_id, added_ids)
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
        return self.yt(uid).create_playlist(name, description="Sincronizada desde Spotify")

    def _search_yt(self, uid, track):
        if DEMO:
            time.sleep(0.08)
            return None if hash(track["sp_track_id"]) % 9 == 0 else "demo_vid"
        try:
            q = f"{track['artists']} {track['name']}"
            res = self.yt(uid).search(q, filter="songs", limit=3)
            for r in res:
                if r.get("videoId"):
                    return r["videoId"]
        except Exception as e:
            self.log(uid, f"  búsqueda falló: {e}", "error")
        return None

    def _add_to_yt(self, uid, yt_id, video_ids):
        if DEMO:
            return
        try:
            self.yt(uid).add_playlist_items(yt_id, video_ids, duplicates=False)
        except Exception as e:
            self.log(uid, f"  error añadiendo lote: {e}", "error")

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
                if not (setting_get(f"sp_token:{uid}") and
                        setting_get(f"yt_ok:{uid}") == "1"):
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
