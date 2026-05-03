const CACHE_NAME = "pet-gato-ponto-premium-v1";
const APP_SHELL = [
  "/ponto",
  "/static/css/style.css",
  "/static/js/app.js",
  "/static/manifest.json",
  "/static/icon-192.png",
  "/static/icon-512.png",
  "/static/icon-180.png"
];
self.addEventListener("install", event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL)));
  self.skipWaiting();
});
self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))))
  );
  self.clients.claim();
});
self.addEventListener("fetch", event => {
  if (event.request.method !== "GET") return;
  event.respondWith(
    fetch(event.request).then(response => {
      const clone = response.clone();
      caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
      return response;
    }).catch(() => caches.match(event.request).then(res => res || caches.match("/ponto")))
  );
});
