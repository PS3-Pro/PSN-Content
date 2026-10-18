const REPO = 'PS3-Pro/PSN-Content';
const BRANCH = 'main';
const BASE = `https://raw.githubusercontent.com/${REPO}/${BRANCH}/resources/database/content/`;
const EXTERNAL_API = `https://api.github.com/repos/${REPO}/contents/resources/database/content/external?ref=${BRANCH}`;
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Cache-Control': 'public, max-age=60'
};

function reply(body, status = 200, type = 'text/plain; charset=utf-8', extra = {}) {
  return new Response(status === 204 ? null : body, { status, headers: { ...CORS, 'Content-Type': type, ...extra } });
}
function escapeXml(value) {
  return String(value).replace(/[<>&"']/g, c => ({
    '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;'
  })[c]);
}
function errorResponse(message, status, detail, debug) {
  if (debug) return reply(JSON.stringify({ ok: false, message, ...detail }, null, 2), status, 'application/json; charset=utf-8');
  return reply(message, status);
}

async function readText(url) {
  const response = await fetch(url, { cf: { cacheEverything: true, cacheTtl: 300 } });
  if (!response.ok) return { status: response.status, text: null };
  return { status: response.status, text: await response.text() };
}

function findTitleId(text, titleId, category) {
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim()) continue;
    const parts = line.split('\t');
    if (String(parts[0] || '').trim().toUpperCase() !== titleId) continue;

    const raw = String(parts[3] || '').trim();
    if (!raw || raw.toUpperCase() === 'MISSING') continue;
    const urls = raw.split('|').map(s => s.trim()).filter(Boolean);
    if (!urls.length || urls.some(u => !/^https?:\/\//i.test(u))) continue;

    let size = 0;
    if (category === 'updates') {
      size = Number.parseInt(String(parts[8] || '').trim(), 10) || 0;
    } else {
      for (let j = parts.length - 1; j >= 3; j--) {
        if (/^\s*\d+\s*$/.test(parts[j] || '')) {
          size = Number.parseInt(parts[j].trim(), 10);
          break;
        }
      }
    }
    const each = Math.floor(size / urls.length);
    const pieces = urls.map((url, index) => ({ url, size: each + (index === urls.length - 1 ? size % urls.length : 0) }));
    return { pieces, totalSize: size };
  }
  return null;
}

async function externalFiles(ctx, issues) {
  const cache = typeof caches !== 'undefined' ? caches.default : null;
  const cacheKey = new Request(EXTERNAL_API);
  try {
    const cached = cache && await cache.match(cacheKey);
    if (cached) return await cached.json();
    const response = await fetch(EXTERNAL_API, {
      headers: { 'Accept': 'application/vnd.github+json', 'User-Agent': 'PSN-Content-XML-Worker' }
    });
    if (!response.ok) {
      issues.push({ source: 'GitHub API external', status: response.status, remaining: response.headers.get('x-ratelimit-remaining') });
      return null;
    }
    const entries = await response.json();
    if (!Array.isArray(entries)) {
      issues.push({ source: 'GitHub API external', error: 'Unexpected response format' });
      return null;
    }
    const urls = entries.filter(x => x.type === 'file' && /\.tsv$/i.test(x.name || '') && /^https:\/\//i.test(x.download_url || ''))
      .map(x => x.download_url);
    if (cache) {
      const serialized = new Response(JSON.stringify(urls), {
        headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=1800' }
      });
      if (ctx?.waitUntil) ctx.waitUntil(cache.put(cacheKey, serialized));
      else await cache.put(cacheKey, serialized);
    }
    return urls;
  } catch (e) {
    issues.push({ source: 'GitHub API external', error: String(e.message || e) });
    return null;
  }
}

async function locate(urls, titleId, category, issues, checked) {
  for (let i = 0; i < urls.length; i += 4) {
    const batch = urls.slice(i, i + 4);
    const results = await Promise.all(batch.map(async url => {
      try {
        const { status, text } = await readText(url);
        checked.push({ url, status });
        if (status === 404) return null;
        if (status !== 200 || text === null) {
          issues.push({ source: url, status });
          return null;
        }
        return findTitleId(text, titleId, category);
      } catch (e) {
        checked.push({ url, status: 'request error' });
        issues.push({ source: url, error: String(e.message || e) });
        return null;
      }
    }));
    const winner = results.findIndex(Boolean);
    if (winner !== -1) return { ...results[winner], source: batch[winner] };
  }
  return null;
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return reply('', 204);
    if (request.method !== 'GET' && request.method !== 'HEAD') return reply('Method not allowed.', 405);

    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    const debug = url.searchParams.has('debug');
    const titleId = String(segments[1] || '').trim().toUpperCase();
    const category = String(segments[2] || '').replace(/\.xml$/i, '').trim().toLowerCase();
    if (segments[0] !== 'id' || !/^[A-Z0-9_-]{4,24}$/.test(titleId) || !/^[a-z0-9_-]+$/.test(category)) {
      return errorResponse('Use /id/TITLEID/games.xml', 400, { titleId, category }, debug);
    }

    const fileName = category === 'updates' ? 'game_updates.tsv' : `${category}.tsv`;
    const baseFiles = [
      `${BASE}official/${fileName}`,
      `${BASE}official/pending/${fileName}`,
      `${BASE}unofficial/${fileName}`
    ];
    const checked = [], issues = [];
    let found = await locate(baseFiles, titleId, category, issues, checked);
    if (!found && category === 'games') {
      const extra = await externalFiles(ctx, issues);
      if (extra) found = await locate(extra, titleId, category, issues, checked);
    }
    const detail = { titleId, category, checked, issues };
    if (!found) {
      const lookupIncomplete = issues.length > 0;
      const message = lookupIncomplete
        ? `Could not fully search the TSV database for ${titleId}. Check GitHub sources/API.`
        : `Title ID ${titleId} not found in ${fileName}.`;
      return errorResponse(message, lookupIncomplete ? 503 : 404, detail, debug);
    }

    if (debug) return reply(JSON.stringify({ ok: true, source: found.source, pieces: found.pieces, totalSize: found.totalSize, ...detail }, null, 2), 200, 'application/json; charset=utf-8');
    const piecesXml = found.pieces.map((piece, index) =>
      `<pieces file_size="${piece.size}" hash_value="0000000000000000000000000000000000000000" index="${index}" url="${escapeXml(piece.url)}"/>`
    ).join('\n');
    const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<hfs_manifest>\n<file_name>${escapeXml(titleId)}.pkg</file_name>\n<file_size>${found.totalSize}</file_size>\n<number_of_split_files>${found.pieces.length}</number_of_split_files>\n${piecesXml}\n</hfs_manifest>`;
    return reply(xml, 200, 'application/xml; charset=utf-8', { 'X-PSN-Source': 'TSV', 'Cache-Control': 'public, max-age=300' });
  }
};
