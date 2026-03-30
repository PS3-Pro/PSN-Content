self.addEventListener('install', function(event) {
    self.skipWaiting();
    console.log("PWA: Service Worker installed.");
});

self.addEventListener('fetch', function(event) {
    event.respondWith(
        fetch(event.request).catch(function() {
            return new Response("PWA Offline Mode");
        })
    );
});