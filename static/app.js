/* PlayBridge — frontend */
const $ = (id) => document.getElementById(id);
let lastLogLen = 0;
let allLogEntries = [];
let logFilter = "all";
let lastYtWasOk = false;
const LOG_KEY = "playbridge_log";

const state = {
  selected: new Set(),
  sp_state: "off",
  yt_state: "off",
  running: false,
};

/* ── tabs ───────────────────────────── */
let unseenLogs = 0;
document.querySelectorAll(".nav-tab").forEach((tab) => {
  tab.onclick = () => {
    document.querySelectorAll(".nav-tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".view").forEach((v) => v.classList.remove("active"));
    tab.classList.add("active");
    $("view-" + tab.dataset.tab).classList.add("active");
    if (tab.dataset.tab === "logs") {
      unseenLogs = 0;
      $("log-badge").classList.remove("show");
    }
  };
});

/* ── log persistido en localStorage ───── */
const LEVEL_LABEL = { info: "INFO", ok: "OK", warn: "WARN", error: "ERROR" };

function setLogFilter(lvl) {
  logFilter = lvl;
  document.querySelectorAll(".logs-toolbar .chip[data-lvl]").forEach((b) =>
    b.classList.toggle("active", b.dataset.lvl === lvl));
  logRender(allLogEntries);
}

function logRender(entries) {
  allLogEntries = entries;
  const filtered = logFilter === "all" ? entries : entries.filter((l) => l.level === logFilter);
  const html = filtered.length
    ? filtered.map((l) => `<div class="log-line l-${l.level}"><span class="ts">${esc(l.t)}</span><span class="lvl">${LEVEL_LABEL[l.level] || "INFO"}</span><span class="msg">${esc(l.msg)}</span></div>`).join("")
    : '<div class="log-empty">Sin actividad para este filtro…</div>';
  $("log").innerHTML = html;
  $("log").scrollTop = $("log").scrollHeight;
}

function logSave(entries) {
  try { localStorage.setItem(LOG_KEY, JSON.stringify(entries.slice(-500))); } catch (e) {}
}

async function clearLog() {
  if (!confirm("¿Borrar todo el registro?")) return;
  try { localStorage.removeItem(LOG_KEY); } catch (e) {}
  logRender([]);
  lastLogLen = 0;
  try {
    await fetch("/api/log/clear", { method: "POST" });
  } catch (e) {}
  toast("Registro limpiado.", "info");
}

function copyLog() {
  const text = allLogEntries.map((l) => `[${l.t}] ${LEVEL_LABEL[l.level] || "INFO"} ${l.msg}`).join("\n");
  if (!text) { toast("No hay registro para copiar.", "warn"); return; }
  navigator.clipboard?.writeText(text)
    .then(() => toast("Registro copiado al portapapeles.", "ok"))
    .catch(() => toast("No se pudo copiar.", "error"));
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
const TOAST_META = {
  ok: { ico: "✓", title: "Completado", life: 4500 },
  error: { ico: "✕", title: "Error", life: 8000 },
  warn: { ico: "!", title: "Aviso", life: 6000 },
  info: { ico: "i", title: "Información", life: 4000 },
};
function toast(msg, type = "info", life) {
  const m = TOAST_META[type] || TOAST_META.info;
  const ms = life || m.life;
  const el = document.createElement("div");
  el.className = `toast t-${type}`;
  el.setAttribute("role", "status");
  el.innerHTML = `
    <div class="toast-head">
      <span class="ico">${m.ico}</span>
      <span>${m.title}</span>
      <button class="t-close" aria-label="Cerrar notificación">×</button>
    </div>
    <div class="toast-body">${msg}</div>
    <div class="toast-life"><i style="animation-duration:${ms}ms"></i></div>`;
  const zone = $("toasts");
  zone.prepend(el);
  while (zone.children.length > 5) zone.lastChild.remove();
  const kill = () => { el.classList.add("hiding"); setTimeout(() => el.remove(), 200); };
  el.querySelector(".t-close").onclick = kill;
  const t = setTimeout(kill, ms);
  el.addEventListener("mouseenter", () => {
    clearTimeout(t);
    el.querySelector(".toast-life i").style.animationPlayState = "paused";
  }, { once: true });
}

/* ───────────────────────── cuentas (pills de 3 estados) */
let lastSpState = null, lastYtState = null;

function setPill(kind, st, sub) {
  const pill = $("pill-" + kind);
  pill.dataset.state = st;
  const subEl = $("pill-" + kind + "-sub");
  if (st === "ok") subEl.textContent = sub || "conectado";
  else if (st === "expired") subEl.textContent = "reconectar";
  else subEl.textContent = "sin conectar";

  // pane header
  if (kind === "sp") {
    $("sp-refresh").style.display = st === "ok" ? "" : "none";
    const btn = $("sp-login-btn");
    btn.querySelector(".btn-label").textContent = st === "ok" ? "Cuenta" : "Conectar";
    btn.className = "btn btn-sm " + (st === "ok" ? "btn-ghost" : "btn-sp");
    $("sp-sub").textContent = st === "ok"
      ? (sub ? `Conectado como ${sub}` : "Conectado")
      : st === "expired" ? "Sesión expirada — reconecta tu cuenta" : "Conecta tu cuenta para listar tus playlists";
  } else {
    const btn = $("yt-login-btn");
    btn.querySelector(".btn-label").textContent = st === "ok" ? "Cuenta" : "Conectar";
    btn.className = "btn btn-sm " + (st === "ok" ? "btn-ghost" : "btn-yt");
    $("yt-sub").textContent = st === "ok"
      ? (sub ? `Conectado como ${sub}` : "Conectado")
      : st === "expired" ? "Sesión expirada — reconecta tu cuenta" : "Conecta tu cuenta para recibir las playlists";
  }
}

function onPillClick(kind) {
  if (kind === "sp") {
    if (lastSpState === "ok") openPanel("sp");
    else connectSpotify();
  } else {
    openPanel("yt");
  }
}

/* ───────────────────────── estado (polling cada 1.5s, 30s si la pestaña está oculta) */
async function poll() {
  try {
    const s = await (await fetch("/api/status?t=" + Date.now())).json();

    setPill("sp", s.spotify_state, s.spotify_user);
    setPill("yt", s.yt_state, s.yt_user || (s.yt_method === "oauth" ? "Google" : s.yt_method === "headers" ? "Navegador" : null));
    lastSpState = s.spotify_state;
    lastYtState = s.yt_state;
    state.sp_state = s.spotify_state;
    state.yt_state = s.yt_state;
    state.running = s.running;
    $("sp-account-info").textContent = s.spotify_user
      ? `Conectado como ${s.spotify_user}` : "Sin información de cuenta.";

    if (s.yt_state === "expired" && lastYtWasOk) {
      toast("La sesión de YT Music expiró. Reconéctala desde el panel «YouTube Music».", "warn", 6000);
    }
    lastYtWasOk = s.yt_state === "ok";

    const pct = s.total ? Math.round((s.done / s.total) * 100) : 0;
    $("prog-fill").style.width = (s.running ? Math.max(pct, 3) : pct) + "%";
    $("prog-pct").textContent = pct + "%";
    $("prog-wrap").classList.toggle("show", s.running || s.total > 0);
    $("bridge").classList.toggle("flowing", s.running);
    $("prog-now").textContent = s.running
      ? `▸ ${s.playlist ? s.playlist + " — " : ""}${s.current || "preparando…"}`
      : (s.total ? "Última migración completada" : "—");
    $("prog-found").textContent = `${s.found} ✓`;
    $("prog-missing").textContent = `${s.missing} ✗`;
    $("btn-sync").classList.toggle("loading", s.running);
    updateSelection();

    // log incremental + persistente
    if (s.log.length !== lastLogLen) {
      logRender(s.log);
      logSave(s.log);
      if (s.log.length > lastLogLen) {
        const added = s.log.slice(lastLogLen);
        if (!$("view-logs").classList.contains("active") && added.some((l) => l.level === "error" || l.level === "warn")) {
          unseenLogs++;
          const b = $("log-badge");
          b.textContent = unseenLogs;
          b.classList.add("show");
        }
      }
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
  pollTimer = setTimeout(poll, document.hidden ? 30000 : 1500);
}

let pollTimer = null;
// pestaña en segundo plano: bajar frecuencia para no acumular llamadas
// a las APIs de Spotify/YT Music mientras nadie la está viendo
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) {
    clearTimeout(pollTimer);
    poll();
  }
});

/* ───────────────────────── playlists */
let allPlaylists = [];

const COVER_COLORS = ["#1DB954", "#FF4D4D", "#5AA7FF", "#F5B83D", "#7C5CFF", "#FF7A45", "#34D399", "#FF4D9D"];
function coverColor(name) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return COVER_COLORS[h % COVER_COLORS.length];
}

async function loadPlaylists(refresh) {
  const res = await fetch("/api/playlists" + (refresh ? "?refresh=1" : ""));
  const data = await res.json();
  if (data.error) { toast(esc(data.error), "error"); return; }
  if (refresh) toast(`Se cargaron <b>${data.length} playlists</b> desde Spotify.`, "ok");
  allPlaylists = data;
  renderPlaylists();
}

function renderPlaylists() {
  const filterEl = $("pl-filter");
  const filter = filterEl ? filterEl.value.trim().toLowerCase() : "";
  const data = filter
    ? allPlaylists.filter((p) => p.name.toLowerCase().includes(filter))
    : allPlaylists;

  if (!allPlaylists.length) {
    $("pl-toolbar").style.display = "none";
    $("pl-list").innerHTML = `<div class="empty" id="sp-empty"><div class="big">♫</div>
      <p>${state.sp_state === "ok"
        ? "Sin playlists todavía. Pulsa <b>↻ Recargar</b> para cargarlas desde Spotify."
        : "Sin conexión a Spotify. Pulsa <b>Conectar</b> y autoriza el acceso para cargar tus playlists."}</p></div>`;
    renderYtPane();
    return;
  }

  $("pl-toolbar").style.display = "flex";
  $("pl-count").textContent = filter ? `${data.length} / ${allPlaylists.length}` : `${allPlaylists.length} playlists`;

  $("pl-list").innerHTML = data.length
    ? data.map((p) => {
        const selected = state.selected.has(p.sp_id);
        return `<div class="pl-row ${selected ? "selected" : ""}" data-id="${p.sp_id}" role="checkbox" aria-checked="${selected}" tabindex="0">
          <span class="cbx">✓</span>
          <div class="pl-cover" style="background:${coverColor(p.name)}">${esc(p.name[0] || "?").toUpperCase()}</div>
          <div class="pl-info">
            <div class="pl-name">${esc(p.name)}</div>
            <div class="pl-meta">${p.total} pistas${p.last_sync ? ` · ⏱ ${p.last_sync.replace("T", " ")}` : ""}</div>
          </div>
          <span class="mini-state"><span class="spinner"></span> migrando…</span>
        </div>`;
      }).join("")
    : `<div class="empty"><p>Sin resultados para «${esc(filter)}».</p></div>`;

  $("pl-list").querySelectorAll(".pl-row").forEach((row) => {
    const toggle = () => {
      if (state.running) { toast("La selección está bloqueada mientras dura una migración.", "warn"); return; }
      const id = row.dataset.id;
      state.selected.has(id) ? state.selected.delete(id) : state.selected.add(id);
      row.classList.toggle("selected", state.selected.has(id));
      row.setAttribute("aria-checked", state.selected.has(id));
      updateSelection();
    };
    row.onclick = toggle;
    row.onkeydown = (e) => { if (e.key === " " || e.key === "Enter") { e.preventDefault(); toggle(); } };
  });

  // marcar fila en curso como "migrando"
  if (state.running) {
    $("pl-list").querySelectorAll(".pl-row").forEach((row) => {
      const p = allPlaylists.find((x) => x.sp_id === row.dataset.id);
      row.classList.toggle("migrating", !!(p && $("prog-now").textContent.includes(p.name)));
    });
  }

  const selectAll = $("pl-select-all");
  selectAll.onchange = () => {
    if (state.running) { selectAll.checked = !selectAll.checked; toast("La selección está bloqueada mientras dura una migración.", "warn"); return; }
    if (selectAll.checked) data.forEach((p) => state.selected.add(p.sp_id));
    else data.forEach((p) => state.selected.delete(p.sp_id));
    renderPlaylists();
  };

  updateSelection();
  renderYtPane();
}

function renderYtPane() {
  const body = $("yt-body");
  if (state.yt_state === "off") {
    body.innerHTML = `<div class="empty"><div class="big">▶</div>
      <p>Sin conexión a YouTube Music. El progreso de cada playlist migrada aparecerá aquí.</p></div>`;
    return;
  }
  const migrated = allPlaylists.filter((p) => p.synced > 0 || p.yt_id);
  if (!migrated.length) {
    body.innerHTML = `<div class="empty"><div class="big">⇣</div>
      <p>Conectado y listo para recibir. Selecciona playlists en el panel de Spotify y pulsa <b>Migrar seleccionadas</b>.</p></div>`;
    return;
  }
  body.innerHTML = migrated.map((p) => {
    const pct = p.total ? Math.round((p.synced / p.total) * 100) : 0;
    const done = p.total > 0 && p.synced >= p.total;
    return `<div class="pl-row" style="cursor:default">
      <div class="pl-cover" style="background:${coverColor(p.name)}">${esc(p.name[0] || "?").toUpperCase()}</div>
      <div class="pl-info">
        <div class="pl-name">${esc(p.name)} ${done ? '<span class="ok-tag">✓ migrada</span>' : ""}</div>
        <div class="pl-meta">
          <span class="pl-bar"><i style="width:${pct}%"></i></span>
          ${p.synced}/${p.total} · ${pct}%
          ${p.last_sync ? ` · ⏱ ${p.last_sync.replace("T", " ")}` : ""}
        </div>
      </div>
      <div class="pl-actions">
        ${done ? `<button class="badge ok" data-action="resync" data-id="${p.sp_id}" data-name="${esc(p.name)}" title="Forzar resincronización completa">↻ resincronizar</button>` : ""}
        ${p.missing ? `<button class="badge miss" data-action="missing" data-id="${p.sp_id}" data-name="${esc(p.name)}">${p.missing} no encontradas</button>` : ""}
      </div>
    </div>`;
  }).join("");

  body.querySelectorAll("[data-action]").forEach((btn) => {
    btn.onclick = (e) => {
      e.stopPropagation();
      const { id, name } = btn.dataset;
      if (btn.dataset.action === "resync") resyncPlaylist(id, name);
      else showMissing(id, name);
    };
  });
}

function toggleAll() {
  $("pl-select-all").onchange();
}

/* ───────────────────────── selección */
function updateSelection() {
  const n = state.selected.size;
  const total = allPlaylists.length;
  const sel = allPlaylists.filter((p) => state.selected.has(p.sp_id));
  const tracks = sel.reduce((a, p) => a + p.total, 0);
  $("sel-summary").innerHTML = n
    ? `<strong>${n}</strong> playlist${n > 1 ? "s" : ""} seleccionada${n > 1 ? "s" : ""} · <strong>${tracks}</strong> pistas en total`
    : `<strong>0</strong> playlists seleccionadas`;
  const sa = $("pl-select-all");
  if (sa) {
    const visible = [...document.querySelectorAll(".pl-list .pl-row")].map((r) => r.dataset.id);
    const selVisible = visible.filter((id) => state.selected.has(id));
    sa.checked = visible.length > 0 && selVisible.length === visible.length;
    sa.indeterminate = selVisible.length > 0 && selVisible.length < visible.length;
  }
  $("btn-sync").disabled = n === 0 || state.running || state.sp_state !== "ok" || state.yt_state !== "ok";
}

/* clic sobre el botón deshabilitado → explicar por qué */
$("migrate-wrap").addEventListener("click", () => {
  const btn = $("btn-sync");
  if (!btn.disabled || state.running) return;
  if (state.sp_state !== "ok") toast("Primero conecta tu cuenta de <b>Spotify</b> (origen) para poder migrar.", "warn");
  else if (state.yt_state !== "ok") toast("Conecta tu cuenta de <b>YouTube Music</b> (destino) antes de iniciar la migración.", "warn");
  else if (state.selected.size === 0) toast("No hay ninguna playlist seleccionada. Marca al menos una en el panel de Spotify.", "warn");
});

/* ───────────────────────── sincronización */
async function syncSelected() {
  const ids = [...state.selected];
  if (!ids.length) return;
  const body = { playlist_ids: ids };
  const res = await (await fetch("/api/sync", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })).json();
  if (res.error) toast(esc(res.error), "error");
  else if (res.started) toast(`Migración iniciada: <b>${ids.length}</b> playlist${ids.length > 1 ? "s" : ""}. Sigue el detalle en <b>Registro</b>.`, "ok");
  else toast("Ya hay una migración en curso.", "warn");
}

async function resyncPlaylist(id, name) {
  if (!confirm(`¿Forzar resincronización completa de «${name}»?\n\nSe borrará el progreso guardado y se volverá a crear/llenar la playlist en YT Music desde cero.`)) return;
  const res = await (await fetch("/api/resync/" + id, { method: "POST" })).json();
  if (res.error) toast(esc(res.error), "error");
  else if (res.started) toast("Resincronización iniciada.", "ok");
  else toast("Ya hay una migración en curso.", "warn");
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
  const btn = $("btn-yt-oauth");
  btn.classList.add("loading"); btn.disabled = true;
  let res;
  try {
    res = await (await fetch("/api/yt/oauth/start", { method: "POST" })).json();
  } finally {
    btn.classList.remove("loading"); btn.disabled = false;
  }
  if (res.error) { toast(esc(res.error), "error", 9000); return; }
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
        toast("YouTube Music conectado.", "ok");
      } else if (r.error && !r.pending) {
        clearInterval(ytPollTimer);
        $("yt-oauth-status").textContent = "error: " + r.error;
        toast(esc(r.error), "error", 10000);
      }
    } catch (e) { /* red intermitente: seguir intentando */ }
  }, 4000);
}

async function saveYtHeaders() {
  const res = await (await fetch("/api/yt/setup", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ headers: $("yt-headers").value }),
  })).json();
  if (res.ok) { closePanel("yt"); toast("YouTube Music conectado.", "ok"); }
  else toast("Headers inválidos: " + esc(res.error || "revisa el formato"), "error", 6000);
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
  toast("Credenciales guardadas.", "ok");
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
