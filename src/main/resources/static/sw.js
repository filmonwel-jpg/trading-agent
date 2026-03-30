const CACHE_NAME = 'trading-stack-status-v2';
const APP_SHELL = [
  '/mobile-status.html',
  '/manifest.webmanifest',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
  '/icons/icon-maskable-512.png',
  '/icons/apple-touch-icon.png'
];

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL)));
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))))
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const request = event.request;
  const url = new URL(request.url);

  if (request.method !== 'GET' || url.origin !== self.location.origin) {
    return;
  }

  if (url.pathname === '/api/stack/overview') {
    event.respondWith(networkFirst(request));
    return;
  }

  if (APP_SHELL.includes(url.pathname) || url.pathname === '/' || url.pathname === '/index.html') {
    event.respondWith(cacheFirst(request));
    return;
  }

  event.respondWith(staleWhileRevalidate(request));
});

async function cacheFirst(request) {
  const cached = await caches.match(request);
  return cached || fetch(request);
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    const cache = await caches.open(CACHE_NAME);
    cache.put(request, response.clone());
    return response;
  } catch (error) {
    const cached = await caches.match(request);
    if (cached) {
      return cached;
    }
    return new Response(JSON.stringify({ offline: true, message: 'Offline and no cached status is available yet.' }), {
      headers: { 'Content-Type': 'application/json' },
      status: 503
    });
  }
}

async function staleWhileRevalidate(request) {
  const cached = await caches.match(request);
  const networkPromise = fetch(request)
    .then(async response => {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
      return response;
    })
    .catch(() => null);
  return cached || networkPromise || new Response('Offline', { status: 503 });
}


