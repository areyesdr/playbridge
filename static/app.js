/* PlayBridge — frontend */
const $ = (id) => document.getElementById(id);
let lastLogLen = 0;
const LOG_KEY = "playbridge_log";

/* ── tabs ─────────────────────────────── */
function switchTab(name) {
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
  document.querySelectorAll(".tab-content").forEach((c) => c.classList.toggle("active", c.id === "tab-" + name));
}

/* ── log persistido en localStorage ───── */
function logRender(entries) {
  const html = entries.length
    ? entries.map((l) => `<div class="${l.level}"><span class="t">${l.t}</span>${esc(l.msg)}</div>`).join("")
    : '<div class="empty muted">Esperando actividad…</div>';
  $("log").innerHTML = html;
  $("log").scrollTop = $("log").scrollHeight;
}

function logSave(entries) {
  try { localStorage.setItem(LOG_KEY, JSON.stringify(entries.slice(-500))); } catch (e) {}
}

function clearLog() {
  lastLogLen = 0;
  if (confirm("¿Borrar todo el registro?")) {
    try { localStorage.removeItem(LOG_KEY); } catch (e) {}
    logRender([]);
  }
}

// restaurar log al cargar la página
try {
  const saved = localStorage.getItem(LOG_KEY);
  if (saved) logRender(JSON.parse(saved));
} catch (e) {}

/* PWA: registrar service worker (scope raíz) */
if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/sw.js").catch(() => {});
}

/* ───────────────────────── toasts */
function toast(msg, type = "info", ms = 3800) {
  let wrap = $("toasts");
  if (!wrap) {
    wrap = document.createElement("div");
    wrap.id = "toasts";
    document.body.appendChild(wrap);
  }
  const t = document.createElement("div");
  t.className = "toast " + type;
  t.textContent = msg;
  wrap.appendChild(t);
  requestAnimationFrame(() => t.classList.add("show"));
  setTimeout(() => {
    t.classList.remove("show");
    setTimeout(() => t.remove(), 300);
  }, ms);
}

/* ───────────────────────── estado (polling cada 1.5s) */
async function poll() {
  try {
    const s = await (await fetch("/api/status")).json();

    $("chip-sp").classList.toggle("ok", s.spotify_ok);
    $("chip-yt").classList.toggle("ok", s.yt_ok);

    const pct = s.total ? Math.round((s.done / s.total) * 100) : 0;
    $("beam-fill").style.width = (s.running ? Math.max(pct, 3) : pct) + "%";
    $("beam").classList.toggle("active", s.running);
    $("ticker").textContent = s.running
      ? "▸ " + (s.current || "preparando…")
      : "— inactivo —";
    $("beam-playlist").textContent = s.playlist || "";
    $("beam-count").textContent = s.total ? `${s.done}/${s.total}` : "";
    $("stat-found").textContent = `${s.found} ✓`;
    $("stat-missing").textContent = `${s.missing} ✗`;
    $("btn-sync").disabled = s.running;

    // log incremental + persistente
    if (s.log.length !== lastLogLen) {
      logRender(s.log);
      logSave(s.log);
      lastLogLen = s.log.length;
      if (!s.running) loadPlaylists(false); // refrescar contadores al terminar
    }

    // scheduler
    $("sched-on").checked = s.scheduler.enabled;
    if (document.activeElement !== $("sched-hours"))
      $("sched-hours").value = s.scheduler.hours;
    $("sched-last").textContent = s.scheduler.last_run
      ? "última: " + s.scheduler.last_run.replace("T", " ") : "";
  } catch (e) { /* servidor reiniciando */ }
  setTimeout(poll, 1500);
}

/* ───────────────────────── playlists */
async function loadPlaylists(refresh) {
  const res = await fetch("/api/playlists" + (refresh ? "?refresh=1" : ""));
  const data = await res.json();
  if (data.error) { toast(data.error, "error"); return; }
  const t = $("pl-table");
  if (refresh) toast(`${data.length} playlists cargadas de Spotify`, "ok");
  if (!data.length) { t.innerHTML = `<div class="empty">Sin playlists. Conecta Spotify y refresca.</div>`; return; }
  t.innerHTML = data.map((p) => {
    const pct = p.total ? Math.round((p.synced / p.total) * 100) : 0;
    return `<div class="row">
      <input type="checkbox" class="pl-check" value="${p.sp_id}">
      <span class="name">${esc(p.name)}</span>
      <span class="bar-mini"><i style="width:${pct}%"></i></span>
      <span class="meta">${p.synced}/${p.total} · ${pct}%</span>
      ${p.missing ? `<button class="miss-link" onclick="showMissing('${p.sp_id}','${esc(p.name)}')">${p.missing} no encontradas</button>` : ""}
      <span class="meta">${p.last_sync ? "⏱ " + p.last_sync.replace("T", " ") : ""}</span>
    </div>`;
  }).join("");
}

async function syncSelected() {
  const ids = [...document.querySelectorAll(".pl-check:checked")].map((c) => c.value);
  const body = { playlist_ids: ids.length ? ids : "all" };
  if (!ids.length && !confirm("¿Sincronizar TODAS las playlists?")) return;
  const res = await (await fetch("/api/sync", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })).json();
  if (res.error) toast(res.error, "error");
  else if (res.started) toast("Sincronización iniciada", "ok");
  else toast("Ya hay una sincronización en curso", "warn");
}

async function showMissing(id, name) {
  const list = await (await fetch("/api/missing/" + id)).json();
  $("missing-title").textContent = `No encontradas — ${name}`;
  $("missing-list").innerHTML = list
    .map((t) => `<div class="warn">✗ ${esc(t.artists)} — ${esc(t.name)}</div>`).join("")
    || `<div class="ok">Nada pendiente ✓</div>`;
  openPanel("missing");
}

/* ───────────────────────── conexiones / config */
function connectSpotify() {
  // siempre permite (re)autorizar: si ya estaba autorizada, Spotify
  // redirige de vuelta al instante sin pedir nada
  window.location = "/spotify/login";
}

/* ───────────────────────── OAuth Google para YT Music (device flow) */
let ytPollTimer = null;

async function startYtOauth() {
  const res = await (await fetch("/api/yt/oauth/start", { method: "POST" })).json();
  if (res.error) { toast(res.error, "error", 7000); return; }
  $("yt-oauth-code").textContent = res.code;
  $("yt-oauth-link").href = res.url;
  $("yt-oauth-box").style.display = "block";
  $("yt-oauth-status").textContent = "esperando autorización…";
  clearInterval(ytPollTimer);
  ytPollTimer = setInterval(async () => {
    try {
      const r = await (await fetch("/api/yt/oauth/poll", { method: "POST" })).json();
      if (r.ok) {
        clearInterval(ytPollTimer);
        closePanel("yt");
        toast("YouTube Music conectado ✓", "ok");
      } else if (r.error && !r.pending) {
        clearInterval(ytPollTimer);
        $("yt-oauth-status").textContent = "error: " + r.error;
        toast(r.error, "error", 6000);
      }
    } catch (e) { /* red intermitente: seguir intentando */ }
  }, 4000);
}

async function saveYtHeaders() {
  const res = await (await fetch("/api/yt/setup", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ headers: $("yt-headers").value }),
  })).json();
  if (res.ok) { closePanel("yt"); toast("YouTube Music conectado ✓", "ok"); }
  else toast("Headers inválidos: " + (res.error || "revisa el formato"), "error", 6000);
}

async function saveConfig() {
  await fetch("/api/config", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sp_client_id: $("cfg-id").value,
      sp_client_secret: $("cfg-secret").value,
      sp_redirect: $("cfg-redirect").value,
      yt_client_id: $("cfg-yt-id").value,
      yt_client_secret: $("cfg-yt-secret").value,
    }),
  });
  closePanel("cfg");
  toast("Credenciales guardadas", "ok");
}

async function loadConfig() {
  const c = await (await fetch("/api/config")).json();
  $("cfg-id").value = c.sp_client_id;
  $("cfg-redirect").value = c.sp_redirect;
  $("cfg-yt-id").value = c.yt_client_id || "";
}

async function saveScheduler() {
  await fetch("/api/scheduler", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      enabled: $("sched-on").checked,
      hours: parseInt($("sched-hours").value || "24", 10),
    }),
  });
}

/* ───────────────────────── helpers */
function openPanel(n) { if (n === "cfg") loadConfig(); $("panel-" + n).showModal(); }
function closePanel(n) {
  if (n === "yt") clearInterval(ytPollTimer);
  $("panel-" + n).close();
}
function esc(s) {
  return String(s).replace(/[&<>"']/g, (m) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
}

loadPlaylists(false);
poll();
