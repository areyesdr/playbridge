"""
app.py — PlayBridge: sincronizador Spotify → YouTube Music
Corre en Render (gunicorn), Debian (python3 app.py) o Termux,
y es instalable como PWA en Android.
"""
import os
from flask import Flask, render_template, request, jsonify, redirect, send_from_directory
from sync_engine import SyncEngine, setting_get, setting_set, DEMO

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(32))

# Render sirve detrás de proxy — confiar en headers X-Forwarded
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

engine = SyncEngine()
engine.connect_spotify()
engine.connect_yt()


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
    return redirect(engine.oauth().get_authorize_url())


@app.route("/callback")
def spotify_callback():
    code = request.args.get("code")
    if code:
        engine.oauth().get_access_token(code, as_dict=False)
        engine.connect_spotify()
    return redirect("/")


# ---------------------------------------------------------------- API
@app.route("/api/status")
def api_status():
    return jsonify(engine.snapshot())


@app.route("/api/playlists")
def api_playlists():
    refresh = request.args.get("refresh") == "1"
    try:
        if refresh and (engine.state["spotify_ok"] or DEMO):
            return jsonify(engine.refresh_playlists())
        return jsonify(engine.playlists_view())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sync", methods=["POST"])
def api_sync():
    data = request.get_json(force=True)
    ids = data.get("playlist_ids", "all")
    if not DEMO and not (engine.state["spotify_ok"] and engine.state["yt_ok"]):
        return jsonify({"error": "Conecta Spotify y YouTube Music primero"}), 400
    started = engine.start_sync(ids)
    return jsonify({"started": started})


@app.route("/api/missing/<pl_id>")
def api_missing(pl_id):
    return jsonify(engine.missing_tracks(pl_id))


@app.route("/api/scheduler", methods=["POST"])
def api_scheduler():
    data = request.get_json(force=True)
    engine.set_scheduler(bool(data.get("enabled")), int(data.get("hours", 24)))
    return jsonify({"ok": True})


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "POST":
        data = request.get_json(force=True)
        for k in ("sp_client_id", "sp_client_secret", "sp_redirect"):
            if k in data and data[k]:
                setting_set(k, data[k].strip())
        return jsonify({"ok": True})
    return jsonify({
        "sp_client_id": setting_get("sp_client_id", ""),
        "sp_redirect": setting_get("sp_redirect", "http://localhost:5000/callback"),
        "has_secret": bool(setting_get("sp_client_secret")),
    })


@app.route("/api/yt/setup", methods=["POST"])
def api_yt_setup():
    headers_raw = request.get_json(force=True).get("headers", "")
    try:
        ok = engine.setup_yt_headers(headers_raw)
        return jsonify({"ok": ok})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/reconnect", methods=["POST"])
def api_reconnect():
    return jsonify({
        "spotify": engine.connect_spotify(),
        "yt": engine.connect_yt(),
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    # host 0.0.0.0 → accesible desde Android en la misma red: http://IP_PC:5000
    app.run(host="0.0.0.0", port=port, debug=False)
