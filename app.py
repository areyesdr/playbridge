"""
app.py — PlayBridge: sincronizador Spotify → YouTube Music
Multi-usuario: cada visitante conecta su propia cuenta vía cookie de sesión
anónima (sin registro). Corre en Render (gunicorn --workers 1 --threads 8),
Debian (python3 app.py) o Termux, e instalable como PWA en Android.
"""
import os
import secrets
from uuid import uuid4
from datetime import timedelta

from flask import (Flask, render_template, request, jsonify, redirect,
                   send_from_directory, session)
from sync_engine import SyncEngine, setting_get, setting_set, DEMO

app = Flask(__name__)
engine = SyncEngine()

# SECRET_KEY estable: si no viene por env, se genera una vez y persiste en DB
# (si cambiara en cada arranque, las sesiones — y la identidad de cada
# usuario — se perderían en cada reinicio)
_sk = os.getenv("SECRET_KEY") or setting_get("secret_key")
if not _sk:
    _sk = secrets.token_hex(32)
    setting_set("secret_key", _sk)
app.secret_key = _sk
app.permanent_session_lifetime = timedelta(days=365)

# Render sirve detrás de proxy — confiar en headers X-Forwarded
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)


@app.after_request
def no_cache(response):
    """Prevenir caché del navegador en todas las respuestas."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def uid():
    """Identidad anónima por navegador: cookie de sesión de larga duración."""
    if "uid" not in session:
        session["uid"] = uuid4().hex
        session.permanent = True
    return session["uid"]


# ---------------------------------------------------------------- vistas
@app.route("/")
def index():
    return render_template("index.html", demo=DEMO)


@app.route("/healthz")
def healthz():
    """Healthcheck para Render (no toca la DB)."""
    return jsonify({"ok": True})


@app.route("/sw.js")
def service_worker():
    # servido desde la raíz para que el scope de la PWA cubra toda la app
    return send_from_directory(app.static_folder, "sw.js",
                               mimetype="application/javascript")


# ---------------------------------------------------------------- OAuth Spotify
@app.route("/spotify/login")
def spotify_login():
    return redirect(engine.oauth(uid()).get_authorize_url())


@app.route("/callback")
def spotify_callback():
    code = request.args.get("code")
    if code:
        u = uid()
        engine.oauth(u).get_access_token(code, as_dict=False)
        engine.connect_spotify(u)
    return redirect("/")


# ---------------------------------------------------------------- API
@app.route("/api/status")
def api_status():
    return jsonify(engine.snapshot(uid()))


@app.route("/api/log/clear", methods=["POST"])
def api_log_clear():
    engine.clear_log(uid())
    return jsonify({"ok": True})


@app.route("/api/playlists")
def api_playlists():
    u = uid()
    refresh = request.args.get("refresh") == "1"
    try:
        if refresh:
            if not (DEMO or engine.verify_spotify(u)):
                return jsonify({"error": "Conecta Spotify primero (botón Spotify)"}), 400
            return jsonify(engine.refresh_playlists(u))
        return jsonify(engine.playlists_view(u))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sync", methods=["POST"])
def api_sync():
    u = uid()
    data = request.get_json(force=True)
    ids = data.get("playlist_ids", "all")
    if not DEMO:
        if not engine.verify_spotify(u):
            return jsonify({"error": "Conecta Spotify primero (botón Spotify)"}), 400
        if not engine.verify_yt(u):
            return jsonify({"error": "Conecta YouTube Music primero"}), 400
    started = engine.start_sync(u, ids)
    return jsonify({"started": started})


@app.route("/api/missing/<pl_id>")
def api_missing(pl_id):
    return jsonify(engine.missing_tracks(uid(), pl_id))


@app.route("/api/playlist/<pl_id>/tracks")
def api_playlist_tracks(pl_id):
    u = uid()
    refresh = request.args.get("refresh") == "1"
    try:
        return jsonify(engine.playlist_tracklist(u, pl_id, refresh=refresh))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/track/<track_id>/album")
def api_track_album(track_id):
    u = uid()
    if DEMO or not engine.verify_spotify(u):
        return jsonify({"error": "Conecta Spotify primero (botón Spotify)"}), 400
    try:
        album = engine.track_album(u, track_id)
        return jsonify(album) if album else (jsonify({"error": "No encontrado"}), 404)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/resync/<pl_id>", methods=["POST"])
def api_resync(pl_id):
    u = uid()
    if not DEMO:
        if not engine.verify_spotify(u):
            return jsonify({"error": "Conecta Spotify primero (botón Spotify)"}), 400
        if not engine.verify_yt(u):
            return jsonify({"error": "Conecta YouTube Music primero"}), 400
    engine.reset_playlist(u, pl_id)
    started = engine.start_sync(u, [pl_id])
    return jsonify({"started": started})


@app.route("/api/scheduler", methods=["POST"])
def api_scheduler():
    data = request.get_json(force=True)
    engine.set_scheduler(uid(), bool(data.get("enabled")), int(data.get("hours", 24)))
    return jsonify({"ok": True})


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "POST":
        data = request.get_json(force=True)
        for k in ("sp_client_id", "sp_client_secret", "sp_redirect",
                  "yt_client_id", "yt_client_secret"):
            if k in data and data[k]:
                setting_set(k, data[k].strip())
        return jsonify({"ok": True})
    return jsonify({
        "sp_client_id": setting_get("sp_client_id", ""),
        "sp_redirect": setting_get("sp_redirect",
                                   os.getenv("SPOTIFY_REDIRECT_URI",
                                             "http://localhost:5000/callback")),
        "has_secret": bool(setting_get("sp_client_secret")),
        "yt_client_id": setting_get("yt_client_id",
                                    os.getenv("YT_CLIENT_ID", "")),
        "has_yt_secret": bool(setting_get("yt_client_secret") or
                              os.getenv("YT_CLIENT_SECRET")),
    })


@app.route("/api/yt/oauth/start", methods=["POST"])
def api_yt_oauth_start():
    if not engine.yt_oauth_creds():
        return jsonify({"error": "Google OAuth no configurado. El administrador debe definir "
                                 "YT_CLIENT_ID y YT_CLIENT_SECRET en ⚙ Config o variables de entorno. "
                                 "Mientras tanto usa la opción de headers del navegador."}), 400
    try:
        return jsonify(engine.yt_oauth_start(uid()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/yt/oauth/poll", methods=["POST"])
def api_yt_oauth_poll():
    try:
        return jsonify(engine.yt_oauth_poll(uid()))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/yt/setup", methods=["POST"])
def api_yt_setup():
    headers_raw = request.get_json(force=True).get("headers", "")
    try:
        ok = engine.setup_yt_headers(uid(), headers_raw)
        return jsonify({"ok": ok})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/reconnect", methods=["POST"])
def api_reconnect():
    u = uid()
    return jsonify({
        "spotify": engine.connect_spotify(u),
        "yt": engine.connect_yt(u),
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    # host 0.0.0.0 → accesible desde Android en la misma red: http://IP_PC:5000
    app.run(host="0.0.0.0", port=port, debug=False)
