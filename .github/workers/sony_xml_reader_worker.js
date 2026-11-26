const MAX_XML_BYTES = 256 * 1024;
const MAX_PIECES = 128;
const MAX_REDIRECTS = 3;
const FETCH_TIMEOUT_MS = 12000;
const CACHE_TTL_SECONDS = 600;
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
  'X-Content-Type-Options': 'nosniff'
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status, headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }
  });
}

function allowedSource(raw) {
  let url;
  try { url = new URL(raw); } catch { throw new Error('Invalid source URL'); }
  const host = url.hostname.toLowerCase();
  if ((url.protocol !== 'http:' && url.protocol !== 'https:') ||
      (host !== 'dl.playstation.net' && !host.endsWith('.dl.playstation.net')) ||
      url.username || url.password || url.port ||
      !/\.pkg\.xml$/i.test(url.pathname) ||
      !url.pathname.startsWith('/cdn/')) {
    throw new Error('Only official Sony /cdn/*.pkg.xml URLs are allowed');
  }
  url.hash = '';
  return url;
}

function decodeEntities(value) {
  return value.replace(/&(amp|lt|gt|quot|apos|#x[0-9a-f]+|#[0-9]+);/gi, (_, code) => {
    const named = { amp:'&', lt:'<', gt:'>', quot:'"', apos:"'" };
    const lowered = code.toLowerCase();
    if (named[lowered] !== undefined) return named[lowered];
    const num = lowered.startsWith('#x') ? parseInt(lowered.slice(2), 16) : parseInt(lowered.slice(1), 10);
    return Number.isInteger(num) && num > 0 && num <= 0x10FFFF && !(num >= 0xD800 && num <= 0xDFFF) ? String.fromCodePoint(num) : '';
  });
}

function tagValue(xml, name) {
  const match = xml.match(new RegExp('<' + name + '\\s*>\\s*([^<]*?)\\s*<\\/' + name + '\\s*>', 'i'));
  return match ? decodeEntities(match[1].trim()) : '';
}

function attributes(markup) {
  const out = Object.create(null);
  const re = /([a-z_][a-z0-9_-]*)\s*=\s*(?:"([^"]*)"|'([^']*)')/gi;
  for (const match of markup.matchAll(re)) out[match[1].toLowerCase()] = decodeEntities(match[2] ?? match[3]);
  return out;
}

function parseHfs(xml) {
  if (!/<hfs_manifest\b[^>]*>/i.test(xml) || !/<\/hfs_manifest\s*>/i.test(xml))
    throw new Error('Not an HFS manifest');
  if (/<!DOCTYPE|<!ENTITY/i.test(xml)) throw new Error('External XML entities are not allowed');

  const fileName = tagValue(xml, 'file_name');
  const fileSize = tagValue(xml, 'file_size');
  const countRaw = tagValue(xml, 'number_of_split_files');
  const count = Number(countRaw);
  if (!fileName || !/\.pkg$/i.test(fileName) || !/^\d+$/.test(fileSize) ||
      !Number.isInteger(count) || count < 1 || count > MAX_PIECES) {
    throw new Error('Missing or invalid HFS metadata');
  }
  const pieces = [];
  const seen = new Set();
  for (const match of xml.matchAll(/<pieces\b([^>]*?)\/?\s*>/gi)) {
    const p = attributes(match[1]);
    const i = Number(p.index);
    if (!/^\d+$/.test(p.index || '') || !Number.isInteger(i) || i < 0 || i >= count || seen.has(i) ||
        !/^\d+$/.test(p.file_size || '')) throw new Error('Invalid piece index or size');
    let part;
    try { part = new URL(p.url); } catch { throw new Error('Invalid piece URL'); }
    if (!['http:', 'https:'].includes(part.protocol) || part.username || part.password || !/\.pkg$/i.test(part.pathname))
      throw new Error('Invalid piece download URL');
    if (p.hash_value && !/^[0-9a-f]{40}$/i.test(p.hash_value)) throw new Error('Invalid SHA-1 in manifest');
    seen.add(i);
    pieces.push({ index:i, url:part.toString(), file_size:p.file_size, hash_value:p.hash_value || null });
  }
  pieces.sort((a,b) => a.index - b.index);
  if (pieces.length !== count || pieces.some((p,i) => p.index !== i))
    throw new Error('Incomplete manifest: expected ' + count + ' pieces, found ' + pieces.length);
  const sum = pieces.reduce((acc, p) => acc + BigInt(p.file_size), 0n);
  if (sum !== BigInt(fileSize)) throw new Error('Piece sizes do not add up to file_size');
  return { file_name:fileName, file_size:fileSize, number_of_split_files:count, pieces };
}

async function readLimited(response) {
  if (!response.body) throw new Error('Empty upstream response');
  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal:true });
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_XML_BYTES) throw new Error('Manifest exceeds the 256 KiB limit');
      chunks.push(decoder.decode(value, { stream:true }));
    }
    chunks.push(decoder.decode());
    return chunks.join('');
  } finally { reader.releaseLock(); }
}

async function loadFromSony(source) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), FETCH_TIMEOUT_MS);
  try {
    let current = source;
    for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
      const response = await fetch(current.toString(), {
        method:'GET', redirect:'manual', signal:ac.signal,
        headers: { 'Accept':'application/xml, text/xml, */*' }
      });
      if ([301,302,303,307,308].includes(response.status)) {
        const next = response.headers.get('Location');
        if (!next || hop === MAX_REDIRECTS) throw new Error('Too many Sony CDN redirects');
        try { current = allowedSource(new URL(next, current).toString()); }
        catch { const e = new Error('Sony CDN redirect is not allowed'); e.upstreamStatus = 502; throw e; }
        continue;
      }
      if (!response.ok) {
        const e = new Error('Sony CDN returned HTTP ' + response.status);
        e.upstreamStatus = response.status;
        throw e;
      }
      const length = Number(response.headers.get('content-length'));
      if (Number.isFinite(length) && length > MAX_XML_BYTES) throw new Error('Manifest exceeds the 256 KiB limit');
      return await readLimited(response);
    }
  } finally { clearTimeout(timer); }
}

export default {
  async fetch(request) {
    if (request.method === 'OPTIONS') return new Response(null, { status:204, headers:CORS });
    if (!['GET', 'HEAD'].includes(request.method)) return json({ok:false,error:'Method not allowed'}, 405);
    const req = new URL(request.url);
    if (req.pathname === '/health') return json({ ok:true, service:'Sony HFS XML reader', version:1 });
    if (req.pathname !== '/manifest') return json({ok:false,error:'Use /manifest?url=<Sony .pkg.xml URL>'},404);
    const raw = req.searchParams.get('url');
    if (!raw) return json({ok:false,error:'Missing url query parameter'},400);
    let source;
    try { source = allowedSource(raw); }
    catch (e) { return json({ok:false,error:e.message},400); }
    const format = req.searchParams.get('format') === 'json' ? 'json' : 'xml';
    const key = new Request(new URL('/manifest?url=' + encodeURIComponent(source.toString()) + '&format=' + format, req.origin).toString());
    const cache = typeof caches !== 'undefined' ? caches.default : null;
    if (cache) {
      try { const saved = await cache.match(key); if (saved) return request.method === 'HEAD' ? new Response(null,{status:200,headers:saved.headers}) : saved; }
      catch (_) { /* Cache is optional. */ }
    }
    try {
      const xml = await loadFromSony(source);
      const parsed = parseHfs(xml);
      const headers = {
        ...CORS,
        'Content-Type': format === 'json' ? 'application/json; charset=utf-8' : 'application/xml; charset=utf-8',
        'Cache-Control':'public, max-age=' + CACHE_TTL_SECONDS,
        'X-PSN-Manifest-Parts':String(parsed.number_of_split_files)
      };
      const body = format === 'json' ? JSON.stringify({ ok:true, source:source.toString(), ...parsed }) : xml;
      const result = new Response(body,{status:200,headers});
      if (cache) { try { await cache.put(key, result.clone()); } catch (_) {} }
      return request.method === 'HEAD' ? new Response(null,{status:200,headers}) : result;
    } catch (error) {
      const status = error.name === 'AbortError' ? 504 : error.upstreamStatus === 404 ? 404 : error.upstreamStatus ? 502 : /manifest|piece|metadata|XML|SHA-1/i.test(error.message) ? 422 : 502;
      return json({ ok:false, error:error.name === 'AbortError' ? 'Sony CDN request timed out' : error.message },status);
    }
  }
};
