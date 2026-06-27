const CACHE_NAME = 'kineticcam-v1';
const ASSETS = ['/', '/manifest.json', '/icons/icon-192.png', '/icons/icon-512.jpeg'];
self.addEventListener('install', event => {
  self.skipWaiting();
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS).catch(() => {})));
});
self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(keys.map(k => k !== CACHE_NAME && caches.delete(k)))));
  self.clients.claim();
});
self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET' || !req.url.startsWith(self.location.origin)) return;
  const url = new URL(req.url);
  if (url.pathname.startsWith('/socket.io') || url.pathname.startsWith('/api')) return;
  event.respondWith(fetch(req).then(res => {
    const copy = res.clone();
    caches.open(CACHE_NAME).then(cache => cache.put(req, copy)).catch(() => {});
    return res;
  }).catch(() => caches.match(req).then(cached => cached || caches.match('/'))));
});
