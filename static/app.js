/* PlayBridge — frontend */
const $ = (id) => document.getElementById(id);
let lastLogLen = 0;
let allLogEntries = [];
let logFilter = "all";
let lastYtWasOk = false;
const LOG_KEY = "playbridge_log";

/* ── tabs ─────────────────────────────── */
function switchTab(name) {
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
  document.querySelectorAll(".tab-content").forEach((c) => c.classList.toggle("active", c.id === "tab-" + name));
}

/* ── log persistido en localStorage ───── */
function setLogFilter(lvl) {
  logFilter = lvl;
  document.querySelectorAll("#log-filters .lf").forEach((b) =>
    b.classList.toggle("active", b.dataset.lvl === lvl));
  logRender(allLogEntries);
}

function logRender(entries) {
  allLogEntries = entries;
  const filtered = logFilter === "all" ? entries : entries.filter((l) => l.level === logFilter);
  const html = filtered.length
    ? filtered.map((l) => `<div class="${l.level}"><span class="t">${l.t}</span>${esc(l.msg)}</div>`).join("")
    : '<div class="empty muted">Sin actividad para este filtro…</div>';
  $("log").innerHTML = html;
  $("log").scrollTop = $("log").scrollHeight;
  $("log-meta").textContent = entries.length
    ? `${filtered.length} de ${entries.length} líneas`
    : "Esperando actividad…";
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

function copyLog() {
  const text = allLogEntries.map((l) => `[${l.t}] ${l.msg}`).join("\n");
  if (!text) { toast("No hay registro para copiar", "warn"); return; }
  navigator.clipboard?.writeText(text)
    .then(() => toast("Registro copiado ✓", "ok"))
    .catch(() => toast("No se pudo copiar", "error"));
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

/* ───────────────────────── cuentas (pills de 3 estados) */
let lastSpState = null, lastYtState = null;

function setPill(kind, state, sub) {
  const pill = $("pill-" + kind);
  pill.classList.remove("state-off", "state-ok", "state-expired");
  pill.classList.add("state-" + state);
  const subEl = $("pill-" + kind + "-sub");
  if (state === "ok") subEl.textContent = sub ? "· " + sub : "· conectado";
  else if (state === "expired") subEl.textContent = "· reconectar";
  else subEl.textContent = "";
}

function onPillClick(kind) {
  if (kind === "sp") {
    if (lastSpState === "ok") openPanel("sp");
    else connectSpotify();
  } else {
    openPanel("yt");
  }
}

function updateOnboard(spState, ytState) {
  const spOk = spState === "ok", ytOk = ytState === "ok";
  $("ob-sp").classList.toggle("done", spOk);
  $("ob-yt").classList.toggle("done", ytOk);
  $("ob-sp").querySelector(".ob-num").textContent = spOk ? "✓" : "1";
  $("ob-yt").querySelector(".ob-num").textContent = ytOk ? "✓" : "2";
  $("onboard").style.display = (spOk && ytOk) ? "none" : "flex";
}

/* ───────────────────────── estado (polling cada 1.5s) */
async function poll() {
  try {
    const s = await (await fetch("/api/status")).json();

    setPill("sp", s.spotify_state, s.spotify_user);
    setPill("yt", s.yt_state, s.yt_user || (s.yt_method === "oauth" ? "Google" : s.yt_method === "headers" ? "Navegador" : null));
    updateOnboard(s.spotify_state, s.yt_state);
    lastSpState = s.spotify_state;
    lastYtState = s.yt_state;
    $("sp-account-info").textContent = s.spotify_user
      ? `Conectado como ${s.spotify_user}` : "Sin información de cuenta.";

    if (s.yt_state === "expired" && lastYtWasOk) {
      toast("La sesión de YT Music expiró. Reconéctala.", "warn", 6000);
    }
    lastYtWasOk = s.yt_state === "ok";

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
let allPlaylists = [];

async function loadPlaylists(refresh) {
  const res = await fetch("/api/playlists" + (refresh ? "?refresh=1" : ""));
  const data = await res.json();
  if (data.error) { toast(data.error, "error"); return; }
  if (refresh) toast(`${data.length} playlists cargadas de Spotify`, "ok");
  allPlaylists = data;
  renderPlaylists();
}

function renderPlaylists() {
  const t = $("pl-table");
  const filter = $("pl-filter").value.trim().toLowerCase();
  const data = filter
    ? allPlaylists.filter((p) => p.name.toLowerCase().includes(filter))
    : allPlaylists;

  if (!allPlaylists.length) {
    $("pl-table-head").style.display = "none";
    t.innerHTML = `<div class="empty" id="pl-empty">Conecta Spotify y pulsa «Refrescar» para cargar tus playlists.</div>`;
    return;
  }
  $("pl-table-head").style.display = "flex";
  $("pl-count").textContent = filter ? `${data.length} de ${allPlaylists.length}` : `${allPlaylists.length} playlists`;

  if (!data.length) {
    t.innerHTML = `<div class="empty">Sin resultados para «${esc(filter)}».</div>`;
    return;
  }
  t.innerHTML = data.map((p) => {
    const pct = p.total ? Math.round((p.synced / p.total) * 100) : 0;
    const synced = p.synced >= p.total && p.total > 0;
    return `<div class="row">
      <input type="checkbox" class="pl-check" value="${p.sp_id}">
      <span class="name">${esc(p.name)}</span>
      <span class="bar-mini"><i style="width:${pct}%"></i></span>
      <span class="meta">${p.synced}/${p.total} · ${pct}%</span>
      ${synced ? '<span class="badge ok">✓ sincronizada</span>' : ""}
      ${p.missing ? `<button class="miss-link" onclick="showMissing('${p.sp_id}','${esc(p.name)}')">${p.missing} no encontradas</button>` : ""}
      <span class="meta">${p.last_sync ? "⏱ " + p.last_sync.replace("T", " ") : ""}</span>
    </div>`;
  }).join("");
  syncSelectAllState();
}

function toggleAll() {
  const checked = $("pl-select-all").checked;
  document.querySelectorAll(".pl-check").forEach((c) => { c.checked = checked; });
}

function syncSelectAllState() {
  document.querySelectorAll(".pl-check").forEach((c) =>
    c.addEventListener("change", () => {
      const all = [...document.querySelectorAll(".pl-check")];
      $("pl-select-all").checked = all.length > 0 && all.every((x) => x.checked);
    }));
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
        toast(r.error, "error", 10000);
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
