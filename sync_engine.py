"""
sync_engine.py — Motor de sincronización PlayBridge (Spotify → YouTube Music)
Estado en PostgreSQL (Supabase/Render) o SQLite local si no hay DATABASE_URL.
Sync incremental, scheduler, credenciales persistidas en DB (sobreviven redeploys).
"""
import os
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
YT_AUTH = os.path.join(BASE_DIR, "browser.json")
DB_PATH = os.path.join(BASE_DIR, "sync.db")
SCOPE = "playlist-read-private playlist-read-collaborative user-library-read"

DEMO = os.getenv("DEMO", "0") == "1"
DATABASE_URL = os.getenv("DATABASE_URL", "")  # postgresql://user:pass@host:5432/db
USE_PG = bool(DATABASE_URL)

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
        CREATE TABLE IF NOT EXISTS playlists(
            sp_id TEXT PRIMARY KEY,
            name TEXT,
            yt_id TEXT,
            total INTEGER DEFAULT 0,
            synced INTEGER DEFAULT 0,
            missing INTEGER DEFAULT 0,
            last_sync TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS tracks(
            sp_track_id TEXT,
            sp_playlist_id TEXT,
            name TEXT,
            artists TEXT,
            yt_video_id TEXT,
            status TEXT DEFAULT 'pending',
            PRIMARY KEY (sp_track_id, sp_playlist_id)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
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
    """Token OAuth de Spotify en la tabla settings — sobrevive redeploys
    (el filesystem de Render es efímero: un archivo .spotify_cache se perdería)."""
    def get_cached_token(self):
        raw = setting_get("sp_token")
        return json.loads(raw) if raw else None

    def save_token_to_cache(self, token_info):
        setting_set("sp_token", json.dumps(token_info))


# ---------------------------------------------------------------- Engine
class SyncEngine:
    def __init__(self):
        init_db()
        self.sp = None
        self.yt = None
        self.lock = threading.Lock()
        self.state = {
            "running": False,
            "playlist": None,
            "current": None,
            "done": 0,
            "total": 0,
            "found": 0,
            "missing": 0,
            "log": [],
            "spotify_ok": False,
            "yt_ok": False,
            "scheduler": {"enabled": setting_get("sched_enabled", "0") == "1",
                          "hours": int(setting_get("sched_hours", "24")),
                          "last_run": setting_get("sched_last_run")},
        }
        self._sched_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._sched_thread.start()
        if DEMO:
            self.state["spotify_ok"] = True
            self.state["yt_ok"] = True
            self._seed_demo()

    # ------------------------------------------------ logging / estado
    def log(self, msg, level="info"):
        line = {"t": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": msg}
        with self.lock:
            self.state["log"].append(line)
            self.state["log"] = self.state["log"][-300:]

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self.state))

    # ------------------------------------------------ auth
    def oauth(self):
        return SpotifyOAuth(
            client_id=setting_get("sp_client_id") or os.getenv("SPOTIFY_CLIENT_ID", ""),
            client_secret=setting_get("sp_client_secret") or os.getenv("SPOTIFY_CLIENT_SECRET", ""),
            redirect_uri=setting_get("sp_redirect") or os.getenv("SPOTIFY_REDIRECT_URI",
                                     "http://localhost:5000/callback"),
            scope=SCOPE, cache_handler=DBCacheHandler(), open_browser=False,
        )

    def connect_spotify(self):
        if DEMO:
            return True
        try:
            auth = self.oauth()
            token = auth.get_cached_token()
            if not token:
                return False
            self.sp = spotipy.Spotify(auth_manager=auth)
            self.sp.current_user()  # valida token
            with self.lock:
                self.state["spotify_ok"] = True
            return True
        except Exception as e:
            self.log(f"Spotify: {e}", "error")
            with self.lock:
                self.state["spotify_ok"] = False
            return False

    def connect_yt(self):
        if DEMO:
            return True
        try:
            if not os.path.exists(YT_AUTH):
                # restaurar desde DB tras un redeploy (filesystem efímero en Render)
                saved = setting_get("yt_auth")
                if not saved:
                    return False
                with open(YT_AUTH, "w") as f:
                    f.write(saved)
            self.yt = YTMusic(YT_AUTH)
            self.yt.get_library_playlists(limit=1)  # valida headers
            with self.lock:
                self.state["yt_ok"] = True
            return True
        except Exception as e:
            self.log(f"YT Music: {e}", "error")
            with self.lock:
                self.state["yt_ok"] = False
            return False

    def setup_yt_headers(self, headers_raw):
        """Crea browser.json desde headers pegados por el usuario y lo respalda en DB."""
        import ytmusicapi
        ytmusicapi.setup(filepath=YT_AUTH, headers_raw=headers_raw)
        with open(YT_AUTH) as f:
            setting_set("yt_auth", f.read())
        return self.connect_yt()

    # ------------------------------------------------ datos Spotify
    def fetch_playlists(self):
        if DEMO:
            return self._demo_playlists()
        results = self.sp.current_user_playlists(limit=50)
        items = results["items"]
        while results["next"]:
            results = self.sp.next(results)
            items.extend(results["items"])
        return [{"sp_id": p["id"], "name": p["name"], "total": p["tracks"]["total"]}
                for p in items]

    def fetch_tracks(self, sp_playlist_id):
        if DEMO:
            return self._demo_tracks(sp_playlist_id)
        results = self.sp.playlist_tracks(sp_playlist_id, limit=100)
        items = results["items"]
        while results["next"]:
            results = self.sp.next(results)
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
        return out

    def refresh_playlists(self):
        """Trae playlists de Spotify y las refleja en DB (sin borrar estado)."""
        pls = self.fetch_playlists()
        with db() as c:
            for p in pls:
                c.execute("""INSERT INTO playlists(sp_id,name,total) VALUES(%s,%s,%s)
                             ON CONFLICT(sp_id) DO UPDATE SET name=EXCLUDED.name,
                             total=EXCLUDED.total""",
                          (p["sp_id"], p["name"], p["total"]))
        return self.playlists_view()

    def playlists_view(self):
        with db() as c:
            c.execute("SELECT * FROM playlists ORDER BY name")
            return [dict(r) for r in c.fetchall()]

    def missing_tracks(self, sp_playlist_id):
        with db() as c:
            c.execute("""SELECT name, artists FROM tracks
                         WHERE sp_playlist_id=%s AND status='missing'
                         ORDER BY artists""", (sp_playlist_id,))
            return [dict(r) for r in c.fetchall()]

    # ------------------------------------------------ sync core
    def start_sync(self, playlist_ids):
        with self.lock:
            if self.state["running"]:
                return False
            self.state["running"] = True
        threading.Thread(target=self._sync_worker, args=(playlist_ids,), daemon=True).start()
        return True

    def _sync_worker(self, playlist_ids):
        try:
            if playlist_ids == "all":
                playlist_ids = [p["sp_id"] for p in self.playlists_view()]
            for pid in playlist_ids:
                self._sync_one(pid)
            self.log("Sincronización completada", "ok")
        except Exception as e:
            self.log(f"Error fatal: {e}", "error")
        finally:
            with self.lock:
                self.state["running"] = False
                self.state["current"] = None
                self.state["playlist"] = None

    def _sync_one(self, sp_playlist_id):
        with db() as c:
            c.execute("SELECT * FROM playlists WHERE sp_id=%s", (sp_playlist_id,))
            pl = c.fetchone()
        if not pl:
            return
        name = pl["name"]
        self.log(f"▶ Playlist: {name}")
        tracks = self.fetch_tracks(sp_playlist_id)

        # incremental: solo pendientes / nuevas
        with db() as c:
            c.execute("SELECT sp_track_id FROM tracks WHERE sp_playlist_id=%s AND status='synced'",
                      (sp_playlist_id,))
            done_ids = {r["sp_track_id"] for r in c.fetchall()}
        todo = [t for t in tracks if t["sp_track_id"] not in done_ids]

        with self.lock:
            self.state.update(playlist=name, total=len(todo), done=0,
                              found=0, missing=0)

        if not todo:
            self.log(f"  Sin cambios ({len(tracks)} ya sincronizadas)")
            self._update_counts(sp_playlist_id, len(tracks))
            return

        yt_id = pl["yt_id"] or self._create_yt_playlist(name)
        added_ids = []

        for t in todo:
            label = f"{t['artists']} — {t['name']}"
            with self.lock:
                self.state["current"] = label
            vid = self._search_yt(t)
            if vid:
                added_ids.append(vid)
                status = "synced"
                with self.lock:
                    self.state["found"] += 1
                self.log(f"  ✓ {label}", "ok")
            else:
                vid = None
                status = "missing"
                with self.lock:
                    self.state["missing"] += 1
                self.log(f"  ✗ No encontrada: {label}", "warn")
            with db() as c:
                c.execute("""INSERT INTO tracks(sp_track_id,sp_playlist_id,name,artists,
                             yt_video_id,status) VALUES(%s,%s,%s,%s,%s,%s)
                             ON CONFLICT(sp_track_id,sp_playlist_id) DO UPDATE SET
                             yt_video_id=EXCLUDED.yt_video_id, status=EXCLUDED.status""",
                          (t["sp_track_id"], sp_playlist_id, t["name"], t["artists"],
                           vid, status))
            with self.lock:
                self.state["done"] += 1
            # lote de 25 para no saturar la API
            if len(added_ids) >= 25:
                self._add_to_yt(yt_id, added_ids)
                added_ids = []
            if not DEMO:
                time.sleep(0.25)

        if added_ids:
            self._add_to_yt(yt_id, added_ids)
        self._update_counts(sp_playlist_id, len(tracks), yt_id)

    def _update_counts(self, sp_playlist_id, total, yt_id=None):
        with db() as c:
            c.execute("SELECT COUNT(*) n FROM tracks WHERE sp_playlist_id=%s AND status='synced'",
                      (sp_playlist_id,))
            synced = c.fetchone()["n"]
            c.execute("SELECT COUNT(*) n FROM tracks WHERE sp_playlist_id=%s AND status='missing'",
                      (sp_playlist_id,))
            missing = c.fetchone()["n"]
            c.execute("""UPDATE playlists SET total=%s, synced=%s, missing=%s,
                         last_sync=%s, yt_id=COALESCE(%s, yt_id) WHERE sp_id=%s""",
                      (total, synced, missing, datetime.now().isoformat(timespec="seconds"),
                       yt_id, sp_playlist_id))

    def _create_yt_playlist(self, name):
        if DEMO:
            return f"DEMO_{name}"
        return self.yt.create_playlist(name, description="Sincronizada desde Spotify")

    def _search_yt(self, track):
        if DEMO:
            time.sleep(0.08)
            return None if hash(track["sp_track_id"]) % 9 == 0 else "demo_vid"
        try:
            q = f"{track['artists']} {track['name']}"
            res = self.yt.search(q, filter="songs", limit=3)
            for r in res:
                if r.get("videoId"):
                    return r["videoId"]
        except Exception as e:
            self.log(f"  búsqueda falló: {e}", "error")
        return None

    def _add_to_yt(self, yt_id, video_ids):
        if DEMO:
            return
        try:
            self.yt.add_playlist_items(yt_id, video_ids, duplicates=False)
        except Exception as e:
            self.log(f"  error añadiendo lote: {e}", "error")

    # ------------------------------------------------ scheduler
    def set_scheduler(self, enabled, hours):
        setting_set("sched_enabled", "1" if enabled else "0")
        setting_set("sched_hours", int(hours))
        with self.lock:
            self.state["scheduler"]["enabled"] = enabled
            self.state["scheduler"]["hours"] = int(hours)

    def _scheduler_loop(self):
        while True:
            time.sleep(60)
            with self.lock:
                sch = self.state["scheduler"]
                running = self.state["running"]
            if not sch["enabled"] or running:
                continue
            last = setting_get("sched_last_run")
            elapsed_ok = True
            if last:
                elapsed = (datetime.now() - datetime.fromisoformat(last)).total_seconds()
                elapsed_ok = elapsed >= sch["hours"] * 3600
            if elapsed_ok and self.state["spotify_ok"] and self.state["yt_ok"]:
                self.log("⏰ Sync automática programada", "info")
                setting_set("sched_last_run", datetime.now().isoformat(timespec="seconds"))
                with self.lock:
                    self.state["scheduler"]["last_run"] = setting_get("sched_last_run")
                self.start_sync("all")

    # ------------------------------------------------ demo
    def _seed_demo(self):
        demo = [("dm1", "Synthwave Nights", 14), ("dm2", "Made in Abyss OST", 9),
                ("dm3", "Coding Focus", 22), ("dm4", "Latin Classics", 11)]
        with db() as c:
            for pid, name, total in demo:
                c.execute("""INSERT INTO playlists(sp_id,name,total) VALUES(%s,%s,%s)
                             ON CONFLICT(sp_id) DO NOTHING""", (pid, name, total))

    def _demo_playlists(self):
        return [{"sp_id": r["sp_id"], "name": r["name"], "total": r["total"]}
                for r in self.playlists_view()]

    def _demo_tracks(self, pid):
        import random
        random.seed(pid)
        n = {"dm1": 14, "dm2": 9, "dm3": 22, "dm4": 11}.get(pid, 10)
        artists = ["The Midnight", "Kevin Penkin", "Carpenter Brut", "Gustavo Cerati",
                   "FM-84", "Perturbator", "Tycho", "Soda Stereo"]
        return [{"sp_track_id": f"{pid}_t{i}",
                 "name": f"Track {i+1}",
                 "artists": random.choice(artists)} for i in range(n)]
