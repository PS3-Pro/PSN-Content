self.addEventListener('install', function(event) {
    console.log('PWA: Service Worker installed.');
    self.skipWaiting();
});

self.addEventListener('fetch', function(event) {
});