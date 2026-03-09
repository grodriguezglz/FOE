// HEB Price Tracker — Service Worker
// Caches the app shell for fast loads; always fetches fresh data from the server.

const CACHE_NAME = 'heb-tracker-v1';

// Static assets to cache on install (app shell)
const PRECACHE = [
  '/',
  '/search',
  '/categories',
  '/static/manifest.json',
];

// ── Install: pre-cache app shell ───────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(PRECACHE))
  );
  self.skipWaiting();
});

// ── Activate: remove old caches ────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

// ── Fetch: network first, fall back to cache ───────────────────
// This ensures price data is always fresh from the server.
self.addEventListener('fetch', event => {
  // Only handle GET requests
  if (event.request.method !== 'GET') return;

  // Skip non-HTTP requests (e.g. chrome-extension://)
  if (!event.request.url.startsWith('http')) return;

  // Skip CDN requests — let them go straight to network
  const url = new URL(event.request.url);
  if (url.hostname !== self.location.hostname) return;

  event.respondWith(
    fetch(event.request)
      .then(response => {
        // Cache successful responses for offline fallback
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => {
        // Network failed — try cache
        return caches.match(event.request).then(cached => {
          if (cached) return cached;
          // Nothing in cache either — return offline page if it's a navigation
          if (event.request.mode === 'navigate') {
            return caches.match('/');
          }
        });
      })
  );
});
