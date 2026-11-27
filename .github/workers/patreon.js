const ENDPOINTS_URL = 'https://raw.githubusercontent.com/PS3-Pro/PSN-Content/refs/heads/main/resources/server/endpoints.json';
let cachedOrigin = '', cachedUntil = 0;
function parsePsnServerEndpoints(data) {
  if (!Array.isArray(data?.servers) || !data.servers.length || data.servers.length > 16) throw new Error('Invalid server list');
  return [...new Set(data.servers.map(value => {
    if (typeof value !== 'string') throw new Error('Invalid server URL');
    const url = new URL(value);
    if (url.protocol !== 'https:' || !/^[a-z0-9-]+\.onrender\.com$/.test(url.hostname) || url.username || url.password || url.port || url.pathname !== '/' || url.search || url.hash) throw new Error('Invalid server origin');
    return url.origin;
  }))];
}
async function getPatreonOrigin() {
  if (cachedOrigin && Date.now() < cachedUntil) return cachedOrigin;
  const response = await fetch(ENDPOINTS_URL, {redirect:'manual',headers:{'Cache-Control':'no-cache','Pragma':'no-cache'},signal:AbortSignal.timeout(10000),cf:{cacheEverything:false,cacheTtl:0}});
  if (!response.ok) throw new Error(`Configuration HTTP ${response.status}`);
  const text = await response.text();
  if (text.length > 16384) throw new Error('Invalid configuration size');
  cachedOrigin = parsePsnServerEndpoints(JSON.parse(text))[0];
  cachedUntil = Date.now() + 15000;
  return cachedOrigin;
}

export default {
  async fetch(request) {
    const incoming = new URL(request.url);
    const callback = incoming.pathname === '/api/patreon/callback';
    const webhook = incoming.pathname === '/api/patreon/webhook';
    const noCache = { 'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer' };
    if (!callback && !webhook) return new Response('Not found', { status: 404, headers: noCache });
    const method = callback ? 'GET' : 'POST';
    if (request.method !== method) return new Response('Method not allowed', { status: 405, headers: { ...noCache, Allow: method } });
    let stage = 'configuration';
    try {
      const origin = new URL(await getPatreonOrigin());
      stage = 'upstream';
      const target = new URL(incoming.pathname + incoming.search, origin);
      const headers = new Headers({'Cache-Control':'no-cache, no-store','Pragma':'no-cache'});
      if (webhook) {
        for (const name of ['Content-Type', 'X-Patreon-Signature', 'X-Patreon-Event']) {
          const value = request.headers.get(name);
          if (value !== null) headers.set(name, value);
        }
      }
      const response = await fetch(target, {
        method, headers,
        body: webhook ? request.body : undefined,
        redirect: 'manual',
        cf: {cacheEverything:false,cacheTtl:0}
      });
      if (response.status >= 300 && response.status < 400) return new Response('Unexpected upstream redirect', {status:502,headers:noCache});
      const outgoing = new Response(response.body, response);
      for (const [name, value] of Object.entries(noCache)) outgoing.headers.set(name, value);
      return outgoing;
    } catch {
      console.warn('[PATREON PROXY] failed:', stage);
      return new Response(`Service temporarily unavailable (${stage})`, { status: 503, headers: {...noCache, 'X-Patreon-Proxy-Stage':stage} });
    }
  }
};
