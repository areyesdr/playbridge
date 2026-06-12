/* PlayBridge — service worker mínimo para instalación PWA.
   Cachea solo los estáticos; la API siempre va a la red. */
const CACHE = "playbridge-v1";
const STATIC = ["/static/style.css", "/static/app.js", "/static/manifest.json",
                "/static/icon-192.png", "/static/icon-512.png"];

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(STATIC)));
  self.skipWaiting();
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (url.pathname.startsWith("/static/")) {
    e.respondWith(
      caches.match(e.request).then((hit) => hit || fetch(e.request))
    );
  }
  // /api/*, /spotify/*, etc. pasan directo a la red
});
