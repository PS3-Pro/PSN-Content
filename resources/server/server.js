const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");
const { Pool, Client } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const server = http.createServer(app);


const ADMIN_USERS = ["Luan Teles", "Goku Cheats", "JumpSuit"];

const DEFAULT_AVATAR = "https://raw.githubusercontent.com/PS3-Pro/PSN-Content/master/resources/interface/modern/images/avatars/default.png";

const MAX_CHAT_HISTORY = 1000; 
const CHAT_SYNC_CHANGE_LOG_MAX = Math.max(1000, Math.min(20000, parseInt(process.env.CHAT_SYNC_CHANGE_LOG_MAX || "5000", 10) || 5000));
const CHAT_SYNC_MAX_DELTA = Math.max(100, Math.min(5000, parseInt(process.env.CHAT_SYNC_MAX_DELTA || "1500", 10) || 1500));

const SERVER_STARTED_AT = Date.now();
const INSTANCE_ID = process.env.RENDER_INSTANCE_ID || process.env.RAILWAY_REPLICA_ID || process.env.HOSTNAME || `instance-${Math.random().toString(36).slice(2, 10)}`;
const PRESENCE_TTL_SECONDS = 90;
const PRESENCE_HEARTBEAT_MS = 25000;
const CHAT_SYNC_INTERVAL_MS = 3000;
const KEEP_ALIVE_INTERVAL_MS = Math.max(60000, parseInt(process.env.KEEP_ALIVE_INTERVAL_MS || "600000", 10) || 600000);
const KEEP_ALIVE_TIMEOUT_MS = Math.max(1000, parseInt(process.env.KEEP_ALIVE_TIMEOUT_MS || "10000", 10) || 10000);
const KEEP_ALIVE_URLS = [
  "https://psn-content-kjir.onrender.com",
];
const PROFILE_SYNC_INTERVAL_MS = Math.max(10000, parseInt(process.env.PROFILE_SYNC_INTERVAL_MS || "15000", 10) || 15000);
const ENABLE_PROFILE_PERIODIC_SYNC = process.env.ENABLE_PROFILE_PERIODIC_SYNC === "1";
const POST_AUTH_CHAT_HISTORY_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_CHAT_HISTORY_DELAY_MS || "180", 10) || 180);
const POST_AUTH_ADMIN_STATE_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_ADMIN_STATE_DELAY_MS || "550", 10) || 550);
const POST_AUTH_ONLINE_LIST_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_ONLINE_LIST_DELAY_MS || "1400", 10) || 1400);
const POST_AUTH_PROFILE_SYNC_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_PROFILE_SYNC_DELAY_MS ?? "0", 10) || 0);
const USER_CACHE_REFRESH_INTERVAL_MS = 30000;
const PASSWORD_RESET_WINDOW_MS = 10 * 60 * 1000;
const USER_CACHE_WARMUP_INTERVAL_MS = 120000;
const DEFAULT_MAINTENANCE_MESSAGE = "The service is under maintenance. Please try again soon.";
const VALID_USER_ROLES = new Set(["user", "trusted", "mod", "admin"]);
const VALID_PROFILE_COUNTRY_CODES = new Set(['AD','AE','AF','AG','AI','AL','AM','AO','AQ','AR','AS','AT','AU','AW','AX','AZ','BA','BB','BD','BE','BF','BG','BH','BI','BJ','BL','BM','BN','BO','BQ','BR','BS','BT','BV','BW','BY','BZ','CA','CC','CD','CF','CG','CH','CI','CK','CL','CM','CN','CO','CR','CU','CV','CW','CX','CY','CZ','DE','DJ','DK','DM','DO','DZ','EC','EE','EG','EH','ER','ES','ET','FI','FJ','FK','FM','FO','FR','GA','GB','GD','GE','GF','GG','GH','GI','GL','GM','GN','GP','GQ','GR','GS','GT','GU','GW','GY','HK','HM','HN','HR','HT','HU','ID','IE','IL','IM','IN','IO','IQ','IR','IS','IT','JE','JM','JO','JP','KE','KG','KH','KI','KM','KN','KP','KR','KW','KY','KZ','LA','LB','LC','LI','LK','LR','LS','LT','LU','LV','LY','MA','MC','MD','ME','MF','MG','MH','MK','ML','MM','MN','MO','MP','MQ','MR','MS','MT','MU','MV','MW','MX','MY','MZ','NA','NC','NE','NF','NG','NI','NL','NO','NP','NR','NU','NZ','OM','PA','PE','PF','PG','PH','PK','PL','PM','PN','PR','PS','PT','PW','PY','QA','RE','RO','RS','RU','RW','SA','SB','SC','SD','SE','SG','SH','SI','SJ','SK','SL','SM','SN','SO','SR','SS','ST','SV','SX','SY','SZ','TC','TD','TF','TG','TH','TJ','TK','TL','TM','TN','TO','TR','TT','TV','TW','TZ','UA','UG','UM','US','UY','UZ','VA','VC','VE','VG','VI','VN','VU','WF','WS','YE','YT','ZA','ZM','ZW']);
const ADMIN_STATE_KEYS = {
  maintenance: "maintenance",
  chatControls: "chat_controls",
  pinnedAnnouncement: "pinned_announcement"
};

const PG_POOL_MAX = Math.max(1, Math.min(5, parseInt(process.env.PG_POOL_MAX || process.env.DB_POOL_MAX || "5", 10) || 5));
const PG_CONNECTION_TIMEOUT_MS = Math.max(3000, parseInt(process.env.PG_CONNECTION_TIMEOUT_MS || "10000", 10) || 10000);
const PG_IDLE_TIMEOUT_MS = Math.max(30000, parseInt(process.env.PG_IDLE_TIMEOUT_MS || "120000", 10) || 120000);
const PG_QUERY_TIMEOUT_MS = Math.max(5000, parseInt(process.env.PG_QUERY_TIMEOUT_MS || "25000", 10) || 25000);
const PG_STATEMENT_TIMEOUT_MS = Math.max(5000, parseInt(process.env.PG_STATEMENT_TIMEOUT_MS || "20000", 10) || 20000);
const PG_MAX_USES = Math.max(0, parseInt(process.env.PG_MAX_USES || "0", 10) || 0);
const ONLINE_LIST_CACHE_MS = Math.max(250, parseInt(process.env.ONLINE_LIST_CACHE_MS || "1200", 10) || 1200);
const ONLINE_LIST_UNCHANGED_SKIP_ENABLED = process.env.ONLINE_LIST_SKIP_UNCHANGED !== "0";
const FRIEND_ACTIVITY_MAX_PER_USER = 200;
const USER_NOTIFICATION_MAX_PER_USER = 200;

const pgConnectionOptions = {
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  application_name: String(`psn-db-${INSTANCE_ID}`).slice(0, 63),
  connectionTimeoutMillis: PG_CONNECTION_TIMEOUT_MS,
  statement_timeout: PG_STATEMENT_TIMEOUT_MS,
  query_timeout: PG_QUERY_TIMEOUT_MS,
  keepAlive: true,
  keepAliveInitialDelayMillis: 5000
};

const poolOptions = {
  ...pgConnectionOptions,
  max: PG_POOL_MAX,
  min: 0,
  idleTimeoutMillis: PG_IDLE_TIMEOUT_MS,
  allowExitOnIdle: false
};
if (PG_MAX_USES > 0) poolOptions.maxUses = PG_MAX_USES;
const pool = new Pool(poolOptions);
let profileSyncNotifyClient = null;
let profileSyncReconnectTimer = null;

let onlineListCache = null;
let onlineListCacheAt = 0;
let lastBroadcastOnlineListSignature = "";

function invalidateOnlineListCache(reason = "") {
  onlineListCache = null;
  onlineListCacheAt = 0;
  if (reason && process.env.DEBUG_ONLINE_CACHE === "1") {
    console.log(`[ONLINE CACHE] invalidated: ${reason}`);
  }
}

function stableStringifySmall(value) {
  if (!value) return "";
  if (typeof value !== "object") return String(value);
  try { return JSON.stringify(value); } catch (err) { return String(value); }
}

function getPs3StatusPresenceSignature(status) {
  if (!status || typeof status !== "object" || Array.isArray(status)) return stableStringifySmall(status);
  const stable = { ...status };
  delete stable.playTime;
  delete stable.playTimeUpdatedAt;
  return stableStringifySmall(stable);
}

function buildOnlineListSignature(list = []) {
  if (!Array.isArray(list) || !list.length) return "empty";
  return list.map(user => {
    const online = user && user.online === true;
    const lastSeenToken = online ? "" : String(user && user.lastSeen || "");
    return [
      user && user.name || "",
      online ? "1" : "0",
      online ? (user && user.id || "") : "",
      lastSeenToken,
      user && user.profileUpdatedAt || 0,
      user && user.avatar || "",
      user && user.role || "",
      getUserCountryCode(user),
      user && user.banned ? "1" : "0",
      getPs3StatusPresenceSignature(user && user.ps3Status)
    ].join(":");
  }).join("|");
}

function getOnlineCountFromList(list = []) {
  return Array.isArray(list) ? list.reduce((count, user) => count + (user && user.online === true ? 1 : 0), 0) : 0;
}

function hasAdminSockets() {
  for (const client of io.sockets.sockets.values()) {
    if (client && client.connected && client.isAdmin === true) return true;
  }
  return false;
}

pool.on('error', (err) => {
  console.error('[DB POOL IDLE ERROR]:', err && err.message ? err.message : err);
});

function isPgConnectionLimitError(err) {
  return !!(err && (err.code === '53300' || /remaining connection slots|too many clients/i.test(String(err.message || ''))));
}

function isPgTransientConnectionError(err) {
  if (!err) return false;
  const code = String(err.code || '').toUpperCase();
  const message = String(err.message || err).toLowerCase();
  if (['08000','08001','08003','08004','08006','08007','08P01','57P01','57P02','57P03','53300'].includes(code)) return true;
  if (code === '57014' && /statement timeout|query timeout|canceling statement/.test(message)) return true;
  return /query read timeout|query timeout|read timeout|connection terminated|connection timeout|timeout exceeded when trying to connect|connection reset|econnreset|etimedout|ehostunreach|enetunreach|socket hang up|broken pipe|server closed the connection|terminating connection|the database system is starting up|too many clients|remaining connection slots/.test(message);
}

function waitMs(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

async function queryDbWithRetry(text, params = [], options = {}) {
  const attempts = Math.max(1, Math.min(4, Number(options.attempts) || 2));
  const label = String(options.label || 'DB QUERY');
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await pool.query(text, params);
    } catch (err) {
      lastError = err;
      if (!isPgTransientConnectionError(err) || attempt >= attempts) throw err;
      const delayMs = attempt === 1 ? 180 : 650;
      console.warn(`[${label}] Temporary PostgreSQL connection failure. Retrying ${attempt + 1}/${attempts} in ${delayMs}ms.`);
      await waitMs(delayMs);
    }
  }
  throw lastError;
}

function scheduleProfileSyncReconnect(delayMs = 5000) {
  if (profileSyncReconnectTimer) return;
  profileSyncReconnectTimer = setTimeout(() => {
    profileSyncReconnectTimer = null;
    initProfileSyncNotifications().catch(e => console.error('[PROFILE LISTEN RECONNECT ERROR]:', e));
  }, delayMs);
}

function runNonOverlappingTask(taskName, taskFn) {
  let running = false;
  return async () => {
    if (running) return;
    running = true;
    try {
      await taskFn();
    } catch (err) {
      console.error(`[${taskName} ERROR]:`, err);
    } finally {
      running = false;
    }
  };
}

let keepAliveInterval = null;

function getKeepAlivePingUrl(rawUrl) {
  const value = String(rawUrl || "").trim();
  if (!value) return null;

  try {
    const url = new URL(value);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return null;
    url.hash = '';
    url.search = '';
    const pathname = url.pathname.replace(/\/+$/, '');
    url.pathname = pathname.endsWith('/ping') ? pathname : `${pathname}/ping`;
    return url;
  } catch (err) {
    console.error(`[KEEP ALIVE] Invalid URL: ${value}`);
    return null;
  }
}

function pingKeepAliveServer(rawUrl, redirectsLeft = 2) {
  const url = rawUrl instanceof URL ? rawUrl : getKeepAlivePingUrl(rawUrl);
  if (!url) return Promise.resolve({ ok: false, url: String(rawUrl || ''), error: 'Invalid URL' });

  return new Promise(resolve => {
    const client = url.protocol === 'https:' ? https : http;
    const req = client.get(url, {
      headers: {
        'User-Agent': 'PSN-Server-KeepAlive/1.0',
        'Cache-Control': 'no-cache'
      }
    }, res => {
      const status = Number(res.statusCode || 0);
      const location = res.headers.location;
      res.resume();

      if (status >= 300 && status < 400 && location && redirectsLeft > 0) {
        try {
          const redirectedUrl = new URL(location, url);
          pingKeepAliveServer(redirectedUrl, redirectsLeft - 1).then(resolve);
        } catch (err) {
          resolve({ ok: false, url: url.href, status, error: err.message });
        }
        return;
      }

      resolve({
        ok: status >= 200 && status < 400,
        url: url.href,
        status
      });
    });

    req.setTimeout(KEEP_ALIVE_TIMEOUT_MS, () => {
      req.destroy(new Error(`Timeout after ${KEEP_ALIVE_TIMEOUT_MS}ms`));
    });

    req.on('error', err => {
      resolve({ ok: false, url: url.href, error: err.message });
    });
  });
}

function startKeepAlivePings() {
  const urls = [...new Set(KEEP_ALIVE_URLS.map(value => String(value || '').trim()).filter(Boolean))];
  if (!urls.length) {
    console.log('[KEEP ALIVE] Disabled. Add server URLs to KEEP_ALIVE_URLS to enable it.');
    return;
  }
  if (keepAliveInterval) return;

  const runKeepAlive = runNonOverlappingTask('KEEP ALIVE', async () => {
    const results = await Promise.all(urls.map(url => pingKeepAliveServer(url)));
    const failed = results.filter(result => !result.ok);

    if (failed.length) {
      failed.forEach(result => {
        console.error(`[KEEP ALIVE] Failed ${result.url}: ${result.status || result.error || 'Unknown error'}`);
      });
    }

    if (process.env.DEBUG_KEEP_ALIVE === '1' || failed.length) {
      console.log(`[KEEP ALIVE] ${results.length - failed.length}/${results.length} server(s) reachable.`);
    }
  });

  console.log(`[KEEP ALIVE] Enabled for ${urls.length} server(s), every ${Math.round(KEEP_ALIVE_INTERVAL_MS / 60000)} minute(s).`);
  setTimeout(runKeepAlive, 5000);
  keepAliveInterval = setInterval(runKeepAlive, KEEP_ALIVE_INTERVAL_MS);
  if (typeof keepAliveInterval.unref === 'function') keepAliveInterval.unref();
}

app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

function setSiteVisitsCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Vary', 'Origin');
}

app.options('/api/site-visits', (req, res) => {
  setSiteVisitsCors(res);
  res.status(204).end();
});

app.get('/api/site-visits', async (req, res) => {
  setSiteVisitsCors(res);
  try {
    const result = await queryDbWithRetry(
      "SELECT value FROM site_stats WHERE stat_key = 'database_visits' LIMIT 1",
      [],
      { attempts: 2, label: 'SITE VISITS READ' }
    );
    const total = Number(result.rows[0] && result.rows[0].value || 0);
    return res.json({ ok: true, total: Number.isFinite(total) && total >= 0 ? total : 0 });
  } catch (err) {
    console.error('[SITE VISITS READ ERROR]:', err && err.message ? err.message : err);
    return res.status(503).json({ ok: false, error: 'Visit counter unavailable.' });
  }
});

app.post('/api/site-visits', async (req, res) => {
  setSiteVisitsCors(res);
  try {
    const result = await queryDbWithRetry(
      `INSERT INTO site_stats (stat_key, value, updated_at)
       VALUES ('database_visits', 1, NOW())
       ON CONFLICT (stat_key)
       DO UPDATE SET value = site_stats.value + 1, updated_at = NOW()
       RETURNING value`,
      [],
      { attempts: 3, label: 'SITE VISITS INCREMENT' }
    );
    const total = Number(result.rows[0] && result.rows[0].value || 0);
    return res.json({ ok: true, total: Number.isFinite(total) && total >= 0 ? total : 0 });
  } catch (err) {
    console.error('[SITE VISITS INCREMENT ERROR]:', err && err.message ? err.message : err);
    return res.status(503).json({ ok: false, error: 'Visit counter unavailable.' });
  }
});


const DEFAULT_IGDB_CLIENT_ID = String(process.env.IGDB_CLIENT_ID || process.env.TWITCH_CLIENT_ID || '').trim();
const DEFAULT_IGDB_CLIENT_SECRET = String(process.env.IGDB_CLIENT_SECRET || process.env.TWITCH_CLIENT_SECRET || '').trim();
const METADATA_PROXY_TIMEOUT_MS = Math.max(2500, parseInt(process.env.METADATA_PROXY_TIMEOUT_MS || '8000', 10) || 8000);
const METADATA_PROXY_CACHE_MS = Math.max(60000, parseInt(process.env.METADATA_PROXY_CACHE_MS || '1800000', 10) || 1800000);
const METADATA_PROXY_CACHE_MAX = Math.max(50, Math.min(1000, parseInt(process.env.METADATA_PROXY_CACHE_MAX || '300', 10) || 300));
const metadataProxyCache = new Map();
let igdbAccessToken = '';
let igdbAccessTokenExpiresAt = 0;
let igdbAccessTokenPromise = null;
let igdbAccessTokenClientId = '';

function setMetadataCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-IGDB-Client-ID, X-IGDB-Client-Secret');
  res.setHeader('Vary', 'Origin');
}

function getMetadataProxyCache(key) {
  const entry = metadataProxyCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.at > METADATA_PROXY_CACHE_MS) {
    metadataProxyCache.delete(key);
    return null;
  }
  return entry.value;
}

function setMetadataProxyCache(key, value) {
  metadataProxyCache.set(key, { at: Date.now(), value });
  while (metadataProxyCache.size > METADATA_PROXY_CACHE_MAX) {
    const oldestKey = metadataProxyCache.keys().next().value;
    if (oldestKey === undefined) break;
    metadataProxyCache.delete(oldestKey);
  }
}

function requestMetadataJson(rawUrl, options = {}) {
  return new Promise((resolve, reject) => {
    let url;
    try { url = rawUrl instanceof URL ? rawUrl : new URL(rawUrl); }
    catch (err) { reject(err); return; }

    const body = options.body === undefined || options.body === null ? null : Buffer.from(String(options.body), 'utf8');
    const headers = { ...(options.headers || {}) };
    if (body && !headers['Content-Length'] && !headers['content-length']) headers['Content-Length'] = String(body.length);
    const requestOptions = {
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port || undefined,
      path: `${url.pathname}${url.search}`,
      method: String(options.method || (body ? 'POST' : 'GET')).toUpperCase(),
      headers
    };

    const client = url.protocol === 'http:' ? http : https;
    const req = client.request(requestOptions, response => {
      const chunks = [];
      let bytes = 0;
      response.on('data', chunk => {
        bytes += chunk.length;
        if (bytes <= 4 * 1024 * 1024) chunks.push(chunk);
      });
      response.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let data = null;
        if (text) {
          try { data = JSON.parse(text); }
          catch (err) { data = null; }
        }
        resolve({
          ok: Number(response.statusCode || 0) >= 200 && Number(response.statusCode || 0) < 300,
          status: Number(response.statusCode || 0),
          data,
          text
        });
      });
    });

    req.setTimeout(Math.max(1000, Number(options.timeoutMs || METADATA_PROXY_TIMEOUT_MS)), () => {
      req.destroy(new Error('Metadata upstream timeout.'));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function getIgdbAccessToken(clientId = DEFAULT_IGDB_CLIENT_ID, clientSecret = DEFAULT_IGDB_CLIENT_SECRET, forceRefresh = false) {
  clientId = String(clientId || '').trim();
  clientSecret = String(clientSecret || '').trim();
  if (!clientId || !clientSecret) {
    const err = new Error('IGDB credentials are not configured.');
    err.code = 'IGDB_NOT_CONFIGURED';
    throw err;
  }
  const sameClient = igdbAccessTokenClientId === clientId;
  if (!forceRefresh && sameClient && igdbAccessToken && Date.now() < igdbAccessTokenExpiresAt - 60000) return igdbAccessToken;
  if (!forceRefresh && sameClient && igdbAccessTokenPromise) return igdbAccessTokenPromise;
  if (!sameClient) {
    igdbAccessToken = '';
    igdbAccessTokenExpiresAt = 0;
    igdbAccessTokenPromise = null;
    igdbAccessTokenClientId = clientId;
  }

  igdbAccessTokenPromise = (async () => {
    const tokenUrl = new URL('https://id.twitch.tv/oauth2/token');
    tokenUrl.searchParams.set('client_id', clientId);
    tokenUrl.searchParams.set('client_secret', clientSecret);
    tokenUrl.searchParams.set('grant_type', 'client_credentials');
    const response = await requestMetadataJson(tokenUrl, { method: 'POST' });
    const token = String(response.data && response.data.access_token || '').trim();
    if (!response.ok || !token) {
      const err = new Error(`IGDB token request failed (HTTP ${response.status || 0}).`);
      err.status = response.status || 502;
      throw err;
    }
    const expiresIn = Math.max(300, Number(response.data && response.data.expires_in || 3600));
    igdbAccessToken = token;
    igdbAccessTokenExpiresAt = Date.now() + expiresIn * 1000;
    return token;
  })().finally(() => { igdbAccessTokenPromise = null; });

  return igdbAccessTokenPromise;
}

function escapeIgdbSearchText(value) {
  return String(value || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/[\r\n]+/g, ' ').trim().slice(0, 180);
}

function igdbImageUrl(imageId, size = '720p') {
  const id = String(imageId || '').trim();
  return id ? `https://images.igdb.com/igdb/image/upload/t_${size}/${id}.jpg` : '';
}

function normalizeIgdbGame(game = {}) {
  const companies = Array.isArray(game.involved_companies) ? game.involved_companies : [];
  const companyNames = items => items.map(item => String(item && item.company && item.company.name || '').trim()).filter(Boolean);
  const developers = companyNames(companies.filter(item => item && item.developer === true));
  const publishers = companyNames(companies.filter(item => item && item.publisher === true));
  const released = Number(game.first_release_date) > 0 ? new Date(Number(game.first_release_date) * 1000).toISOString().slice(0, 10) : '';
  return {
    id: game.id,
    name: String(game.name || '').trim(),
    released,
    description_raw: String(game.summary || '').trim(),
    metacritic: Number.isFinite(Number(game.aggregated_rating)) ? Math.round(Number(game.aggregated_rating)) : null,
    developers: developers.map(name => ({ name })),
    publishers: publishers.map(name => ({ name })),
    genres: (Array.isArray(game.genres) ? game.genres : []).map(item => ({ name: String(item && item.name || '').trim() })).filter(item => item.name),
    tags: (Array.isArray(game.game_modes) ? game.game_modes : []).map(item => ({ name: String(item && item.name || '').trim() })).filter(item => item.name),
    platforms: (Array.isArray(game.platforms) ? game.platforms : []).map(item => String(item && item.name || '').trim()).filter(Boolean),
    screenshots: (Array.isArray(game.screenshots) ? game.screenshots : []).map(item => igdbImageUrl(item && item.image_id, '720p')).filter(Boolean).slice(0, 8),
    cover: igdbImageUrl(game.cover && game.cover.image_id, 'cover_big_2x'),
    versionParent: game.version_parent || null,
    versionTitle: String(game.version_title || '').trim(),
    source: 'igdb'
  };
}

async function queryIgdbGames(searchText, clientId = DEFAULT_IGDB_CLIENT_ID, clientSecret = DEFAULT_IGDB_CLIENT_SECRET, retryAuth = true) {
  const token = await getIgdbAccessToken(clientId, clientSecret, false);
  const safeSearch = escapeIgdbSearchText(searchText);
  if (!safeSearch) return [];
  const body = `search "${safeSearch}"; fields name,summary,first_release_date,aggregated_rating,genres.name,involved_companies.company.name,involved_companies.developer,involved_companies.publisher,platforms.name,game_modes.name,screenshots.image_id,cover.image_id,version_parent,version_title; limit 10;`;
  const response = await requestMetadataJson('https://api.igdb.com/v4/games', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'text/plain',
      'Client-ID': clientId,
      'Authorization': `Bearer ${token}`
    },
    body
  });
  if (response.status === 401 && retryAuth) {
    igdbAccessToken = '';
    igdbAccessTokenExpiresAt = 0;
    await getIgdbAccessToken(clientId, clientSecret, true);
    return queryIgdbGames(searchText, clientId, clientSecret, false);
  }
  if (!response.ok || !Array.isArray(response.data)) {
    const err = new Error(`IGDB search failed (HTTP ${response.status || 0}).`);
    err.status = response.status || 502;
    throw err;
  }
  return response.data.map(normalizeIgdbGame).filter(item => item.name);
}

function normalizeSteamDetails(appId, data = {}) {
  const categories = (Array.isArray(data.categories) ? data.categories : []).map(item => String(item && item.description || '').trim()).filter(Boolean);
  return {
    id: String(appId || ''),
    name: String(data.name || '').trim(),
    released: String(data.release_date && data.release_date.date || '').trim(),
    description_raw: String(data.short_description || data.detailed_description || '').replace(/<br\s*\/?\s*>/gi, '\n').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim(),
    metacritic: Number.isFinite(Number(data.metacritic && data.metacritic.score)) ? Number(data.metacritic.score) : null,
    developers: (Array.isArray(data.developers) ? data.developers : []).map(name => ({ name: String(name || '').trim() })).filter(item => item.name),
    publishers: (Array.isArray(data.publishers) ? data.publishers : []).map(name => ({ name: String(name || '').trim() })).filter(item => item.name),
    genres: (Array.isArray(data.genres) ? data.genres : []).map(item => ({ name: String(item && item.description || '').trim() })).filter(item => item.name),
    tags: categories.map(name => ({ name })),
    platforms: Object.keys(data.platforms || {}).filter(key => data.platforms[key] === true),
    screenshots: (Array.isArray(data.screenshots) ? data.screenshots : []).map(item => String(item && (item.path_full || item.path_thumbnail) || '').trim()).filter(Boolean).slice(0, 8),
    cover: String(data.header_image || '').trim(),
    source: 'steam'
  };
}

app.options('/api/metadata/igdb', (req, res) => { setMetadataCors(res); res.status(204).end(); });
app.options('/api/metadata/steam/search', (req, res) => { setMetadataCors(res); res.status(204).end(); });
app.options('/api/metadata/steam/details', (req, res) => { setMetadataCors(res); res.status(204).end(); });

app.get('/api/metadata/igdb', async (req, res) => {
  setMetadataCors(res);
  const query = String(req.query && req.query.q || '').trim().slice(0, 180);
  const clientId = String(req.get('x-igdb-client-id') || (req.query && req.query.client_id) || DEFAULT_IGDB_CLIENT_ID || '').trim().slice(0, 160);
  const clientSecret = String(req.get('x-igdb-client-secret') || (req.query && req.query.client_secret) || DEFAULT_IGDB_CLIENT_SECRET || '').trim().slice(0, 300);
  if (!query) return res.status(400).json({ ok: false, provider: 'igdb', error: 'Missing q.' });
  if (!clientId || !clientSecret) return res.status(503).json({ ok: false, provider: 'igdb', configured: false, error: 'IGDB credentials are not configured.' });
  const cacheKey = `igdb:${query.toLowerCase()}`;
  const cached = getMetadataProxyCache(cacheKey);
  if (cached) return res.json({ ...cached, cached: true });
  try {
    const results = await queryIgdbGames(query, clientId, clientSecret);
    const payload = { ok: true, provider: 'igdb', configured: true, results };
    setMetadataProxyCache(cacheKey, payload);
    return res.json(payload);
  } catch (err) {
    const status = Number(err && err.status || 502);
    console.warn(`[METADATA IGDB] ${query}: ${err && err.message ? err.message : err}`);
    return res.status(status === 429 ? 429 : 502).json({ ok: false, provider: 'igdb', configured: true, upstreamStatus: status, error: err && err.message ? err.message : 'IGDB request failed.' });
  }
});

app.get('/api/metadata/steam/search', async (req, res) => {
  setMetadataCors(res);
  const query = String(req.query && req.query.q || '').trim().slice(0, 180);
  if (!query) return res.status(400).json({ ok: false, provider: 'steam', error: 'Missing q.' });
  const cacheKey = `steam-search:${query.toLowerCase()}`;
  const cached = getMetadataProxyCache(cacheKey);
  if (cached) return res.json({ ...cached, cached: true });
  try {
    const url = new URL('https://store.steampowered.com/api/storesearch/');
    url.searchParams.set('term', query);
    url.searchParams.set('l', 'english');
    url.searchParams.set('cc', 'US');
    const response = await requestMetadataJson(url, { headers: { 'User-Agent': 'PSN-Content-Metadata/1.0' } });
    if (!response.ok || !response.data || !Array.isArray(response.data.items)) {
      return res.status(response.status === 429 ? 429 : 502).json({ ok: false, provider: 'steam', upstreamStatus: response.status || 0, error: `Steam search failed (HTTP ${response.status || 0}).` });
    }
    const results = response.data.items.slice(0, 10).map(item => ({
      id: String(item && (item.id || item.appid) || ''),
      name: String(item && item.name || '').trim(),
      released: String(item && (item.released || item.release_date || '') || '').trim(),
      platforms: item && item.platforms ? item.platforms : null,
      source: 'steam'
    })).filter(item => item.id && item.name);
    const payload = { ok: true, provider: 'steam', results };
    setMetadataProxyCache(cacheKey, payload);
    return res.json(payload);
  } catch (err) {
    console.warn(`[METADATA STEAM SEARCH] ${query}: ${err && err.message ? err.message : err}`);
    return res.status(502).json({ ok: false, provider: 'steam', error: err && err.message ? err.message : 'Steam search failed.' });
  }
});

app.get('/api/metadata/steam/details', async (req, res) => {
  setMetadataCors(res);
  const appId = String(req.query && req.query.appid || '').trim();
  if (!/^\d{1,12}$/.test(appId)) return res.status(400).json({ ok: false, provider: 'steam', error: 'Invalid appid.' });
  const cacheKey = `steam-details:${appId}`;
  const cached = getMetadataProxyCache(cacheKey);
  if (cached) return res.json({ ...cached, cached: true });
  try {
    const url = new URL('https://store.steampowered.com/api/appdetails');
    url.searchParams.set('appids', appId);
    url.searchParams.set('l', 'english');
    url.searchParams.set('cc', 'US');
    const response = await requestMetadataJson(url, { headers: { 'User-Agent': 'PSN-Content-Metadata/1.0' } });
    const entry = response.data && response.data[appId];
    if (!response.ok || !entry || entry.success !== true || !entry.data || entry.data.type !== 'game') {
      return res.status(response.status === 429 ? 429 : 404).json({ ok: false, provider: 'steam', upstreamStatus: response.status || 0, error: 'Steam game details unavailable.' });
    }
    const payload = { ok: true, provider: 'steam', details: normalizeSteamDetails(appId, entry.data) };
    setMetadataProxyCache(cacheKey, payload);
    return res.json(payload);
  } catch (err) {
    console.warn(`[METADATA STEAM DETAILS] ${appId}: ${err && err.message ? err.message : err}`);
    return res.status(502).json({ ok: false, provider: 'steam', error: err && err.message ? err.message : 'Steam details failed.' });
  }
});

let userDatabase = {};
let userCacheMeta = {};
let userCacheLastFullRefresh = 0;
let userCacheRefreshInFlight = null;
const userProfileWriteInFlight = new Set();
const fullUserCacheNames = new Set();
let profileHydrationQueues = [Promise.resolve(), Promise.resolve()];
let profileHydrationNextLane = 0;
let chatHistoryEmitQueue = Promise.resolve();
const USER_HEAVY_CACHE_KEYS = ['downloadsData', 'libraryData', 'wishlistData', 'favoritesData', 'trophiesData', 'friendsData'];
let trendingCache = null;
let trendingCacheAt = 0;
let trendingBuildInFlight = null;
let globalTrophyStatsCache = null;
let globalTrophyStatsCacheAt = 0;
let globalTrophyStatsBuildInFlight = null;
let lastMemoryPressureLogAt = 0;
let profileHydrationQueued = 0;
let profileHydrationActive = 0;
let profileSyncActiveSockets = 0;
const MEMORY_TRACE_ENABLED = String(process.env.MEMORY_TRACE || 'false').trim().toLowerCase() === 'true';

function getSocketWriteBufferLength(socket) {
  try {
    const conn = socket && socket.client && socket.client.conn;
    return conn && Array.isArray(conn.writeBuffer) ? conn.writeBuffer.length : 0;
  } catch (err) {
    return 0;
  }
}

async function waitForSocketWriteBufferDrain(socket, options = {}) {
  const maxPending = Math.max(0, Number(options.maxPending ?? 1));
  const timeoutMs = Math.max(500, Number(options.timeoutMs || 12000));
  const pollMs = Math.max(10, Number(options.pollMs || 25));
  const startedAt = Date.now();
  while (socket && socket.connected) {
    const pending = getSocketWriteBufferLength(socket);
    if (pending <= maxPending) return { drained: true, pending, elapsedMs: Date.now() - startedAt };
    if (Date.now() - startedAt >= timeoutMs) return { drained: false, pending, elapsedMs: Date.now() - startedAt };
    await waitMs(pollMs);
  }
  return { drained: false, pending: getSocketWriteBufferLength(socket), elapsedMs: Date.now() - startedAt, disconnected: true };
}

function getTotalSocketWriteBufferLength() {
  let total = 0;
  try {
    io.sockets.sockets.forEach(client => { total += getSocketWriteBufferLength(client); });
  } catch (err) {}
  return total;
}

function formatApproxBytes(bytes = 0) {
  const value = Math.max(0, Number(bytes) || 0);
  if (value >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)} MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${Math.round(value)} B`;
}

function estimateValueBytes(value, options = {}) {
  const maxNodes = Math.max(1000, Number(options.maxNodes || 100000));
  const maxBytes = Math.max(1024 * 1024, Number(options.maxBytes || 128 * 1024 * 1024));
  const seen = new WeakSet();
  const stack = [value];
  let nodes = 0;
  let bytes = 0;
  while (stack.length && nodes < maxNodes && bytes < maxBytes) {
    const current = stack.pop();
    nodes += 1;
    if (current === null || current === undefined) { bytes += 4; continue; }
    const type = typeof current;
    if (type === 'string') { bytes += current.length * 2; continue; }
    if (type === 'number') { bytes += 8; continue; }
    if (type === 'boolean') { bytes += 4; continue; }
    if (type !== 'object') { bytes += 8; continue; }
    if (seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      bytes += current.length * 8;
      for (let i = 0; i < current.length; i += 1) stack.push(current[i]);
      continue;
    }
    const keys = Object.keys(current);
    bytes += keys.length * 8;
    for (const key of keys) {
      bytes += key.length * 2;
      stack.push(current[key]);
    }
  }
  return { bytes, truncated: stack.length > 0 || nodes >= maxNodes || bytes >= maxBytes, nodes };
}

function getPayloadItemCount(value) {
  if (Array.isArray(value)) return value.length;
  if (value && typeof value === 'object') return Object.keys(value).length;
  return value === null || value === undefined ? 0 : 1;
}

function getMemoryDiagnosticSnapshot() {
  const mem = process.memoryUsage();
  return {
    heapMb: mem.heapUsed / 1024 / 1024,
    heapTotalMb: mem.heapTotal / 1024 / 1024,
    rssMb: mem.rss / 1024 / 1024,
    sockets: io.sockets.sockets.size,
    writeBufferPackets: getTotalSocketWriteBufferLength(),
    profileHydrationQueued,
    profileHydrationActive,
    profileSyncActiveSockets
  };
}

function logMemoryTrace(reason, details = '') {
  if (!MEMORY_TRACE_ENABLED) return;
  const snap = getMemoryDiagnosticSnapshot();
  const suffix = details ? ` ${details}` : '';
  console.log(`[MEMORY TRACE] ${reason} heap=${snap.heapMb.toFixed(1)}/${snap.heapTotalMb.toFixed(1)}MB rss=${snap.rssMb.toFixed(1)}MB sockets=${snap.sockets} writeBuffer=${snap.writeBufferPackets} hydration=${snap.profileHydrationActive}/${snap.profileHydrationQueued} profileSync=${snap.profileSyncActiveSockets}${suffix}`);
}

function logMemoryPressureIfNeeded(reason = 'periodic') {
  const mem = process.memoryUsage();
  const heapMb = mem.heapUsed / 1024 / 1024;
  if (heapMb < 120 && reason === 'periodic') return;
  const now = Date.now();
  if (reason === 'periodic' && now - lastMemoryPressureLogAt < 30000) return;
  lastMemoryPressureLogAt = now;
  const snap = getMemoryDiagnosticSnapshot();
  console.log(`[MEMORY] ${reason} heap ${heapMb.toFixed(1)} MB / ${(mem.heapTotal / 1024 / 1024).toFixed(1)} MB, rss ${(mem.rss / 1024 / 1024).toFixed(1)} MB, sockets ${snap.sockets}, writeBuffer ${snap.writeBufferPackets}, hydration ${snap.profileHydrationActive}/${snap.profileHydrationQueued}, profile-sync ${snap.profileSyncActiveSockets}, compact users ${Object.keys(userDatabase).length}, full-cache ${fullUserCacheNames.size}, trend cache ${trendingCache ? 'yes' : 'no'}, content-count cache ${contentDownloadCountCache ? contentDownloadCountCache.size : 0}`);
}
let trendingRefreshTimer = null;
let trophyStatsRefreshTimer = null;
const TRENDING_CACHE_MS = Math.max(10000, parseInt(process.env.TRENDING_CACHE_MS || '300000', 10) || 300000);
const TROPHY_STATS_CACHE_MS = Math.max(10000, parseInt(process.env.TROPHY_STATS_CACHE_MS || '30000', 10) || 30000);
let messageHistory = [];
let lastChatDbId = 0;
let chatSyncState = { epoch: '', revision: 0 };
let pinnedMessages = [];

let adminState = {
  maintenance: { enabled: false, message: DEFAULT_MAINTENANCE_MESSAGE, by: "", at: null },
  chatControls: { locked: false, slowSeconds: 0, by: "", at: null },
  pinnedAnnouncement: null
};
let moderationLog = [];
let serverLog = [];
let adminReports = [];
let lastKnownOnlineList = [];
let adminStateLastRefreshAt = 0;
let adminStateRefreshInFlight = null;
let adminStateConnectionLimitWarnedAt = 0;

async function refreshAdminStateFromDb() {
  try {
    const stateRes = await queryDbWithRetry('SELECT state_key, data FROM admin_state', [], { attempts: 2, label: 'ADMIN STATE READ' });
    stateRes.rows.forEach(row => {
      if (row.state_key === ADMIN_STATE_KEYS.maintenance) {
        adminState.maintenance = normalizeMaintenanceState(row.data || {});
      } else if (row.state_key === ADMIN_STATE_KEYS.chatControls) {
        adminState.chatControls = normalizeChatControls(row.data || {});
      } else if (row.state_key === ADMIN_STATE_KEYS.pinnedAnnouncement) {
        adminState.pinnedAnnouncement = row.data && row.data.text ? row.data : null;
      }
    });
    adminStateLastRefreshAt = Date.now();
  } catch (err) {
    if (isPgConnectionLimitError(err)) {
      const now = Date.now();
      if (now - adminStateConnectionLimitWarnedAt > 30000) {
        adminStateConnectionLimitWarnedAt = now;
        console.error('[ADMIN STATE REFRESH ERROR]: PostgreSQL connection limit reached; using cached admin state.');
      }
    } else {
      console.error('[ADMIN STATE REFRESH ERROR]:', err);
    }
  }
  return adminState;
}

async function refreshAdminStateThrottled(maxAgeMs = 3000) {
  if (adminStateRefreshInFlight) return adminStateRefreshInFlight;
  if (adminStateLastRefreshAt && Date.now() - adminStateLastRefreshAt < maxAgeMs) return adminState;

  adminStateRefreshInFlight = refreshAdminStateFromDb()
    .finally(() => { adminStateRefreshInFlight = null; });
  return adminStateRefreshInFlight;
}

async function refreshModerationLogFromDb() {
  try {
    const modLogRes = await pool.query('SELECT entry FROM moderation_log ORDER BY created_at DESC LIMIT 100');
    moderationLog = modLogRes.rows.map(r => r.entry);
  } catch (err) {
    console.error('[ADMIN LOG REFRESH ERROR]:', err);
  }
  return moderationLog;
}

async function refreshServerLogFromDb() {
  try {
    const logRes = await pool.query('SELECT entry FROM server_log ORDER BY created_at DESC LIMIT 120');
    serverLog = logRes.rows.map(r => r.entry);
  } catch (err) {
    console.error('[SERVER LOG REFRESH ERROR]:', err);
  }
  return serverLog;
}


async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      name TEXT PRIMARY KEY,
      data JSONB
    );
    CREATE TABLE IF NOT EXISTS chat (
      id SERIAL PRIMARY KEY,
      message JSONB
    );
    CREATE TABLE IF NOT EXISTS chat_sync_state (
      id SMALLINT PRIMARY KEY,
      epoch TEXT NOT NULL,
      revision BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS chat_changes (
      revision BIGINT PRIMARY KEY,
      epoch TEXT NOT NULL,
      change_type TEXT NOT NULL,
      message_id TEXT,
      message JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_chat_changes_epoch_revision ON chat_changes(epoch, revision);
    CREATE TABLE IF NOT EXISTS chat_seen_events (
      id BIGSERIAL PRIMARY KEY,
      message_id TEXT NOT NULL,
      sender TEXT NOT NULL,
      reader TEXT NOT NULL,
      seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(message_id, reader)
    );
    CREATE INDEX IF NOT EXISTS idx_chat_seen_events_sender_id ON chat_seen_events(sender, id);
    CREATE INDEX IF NOT EXISTS idx_chat_seen_events_reader_id ON chat_seen_events(reader, id);
    CREATE TABLE IF NOT EXISTS pinned_messages (
      id SERIAL PRIMARY KEY,
      message_id TEXT UNIQUE,
      data JSONB
    );
    CREATE TABLE IF NOT EXISTS admin_state (
      state_key TEXT PRIMARY KEY,
      data JSONB
    );
    CREATE TABLE IF NOT EXISTS moderation_log (
      id SERIAL PRIMARY KEY,
      entry JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS server_log (
      id SERIAL PRIMARY KEY,
      entry JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS reports (
      id TEXT PRIMARY KEY,
      data JSONB,
      resolved BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS deleted_accounts (
      name TEXT PRIMARY KEY,
      data JSONB,
      deleted_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS presence_sessions (
      socket_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      instance_id TEXT,
      connected_at TIMESTAMPTZ DEFAULT NOW(),
      last_seen TIMESTAMPTZ DEFAULT NOW(),
      data JSONB DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_presence_sessions_name ON presence_sessions(name);
    CREATE INDEX IF NOT EXISTS idx_presence_sessions_last_seen ON presence_sessions(last_seen);
    CREATE TABLE IF NOT EXISTS chat_backups (
      id SERIAL PRIMARY KEY,
      by_user TEXT,
      reason TEXT,
      messages JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS site_stats (
      stat_key TEXT PRIMARY KEY,
      value BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS friend_activity (
      id BIGSERIAL PRIMARY KEY,
      actor_name TEXT NOT NULL,
      event_type TEXT NOT NULL,
      data JSONB NOT NULL DEFAULT '{}'::jsonb,
      dedupe_key TEXT UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_friend_activity_actor_id ON friend_activity(actor_name, id DESC);
    CREATE INDEX IF NOT EXISTS idx_friend_activity_created_at ON friend_activity(created_at DESC);
    CREATE TABLE IF NOT EXISTS friend_activity_read_state (
      user_name TEXT PRIMARY KEY,
      last_read_id BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS user_notifications (
      id BIGSERIAL PRIMARY KEY,
      user_name TEXT NOT NULL,
      event_type TEXT NOT NULL,
      data JSONB NOT NULL DEFAULT '{}'::jsonb,
      dedupe_key TEXT UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_user_notifications_user_id ON user_notifications(user_name, id DESC);
    CREATE TABLE IF NOT EXISTS user_notification_read_state (
      user_name TEXT PRIMARY KEY,
      last_read_id BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await queryDbWithRetry(
    `DELETE FROM friend_activity
     WHERE id IN (
       SELECT id FROM (
         SELECT id, ROW_NUMBER() OVER (PARTITION BY actor_name ORDER BY id DESC) AS row_num
         FROM friend_activity
       ) ranked
       WHERE row_num > ${FRIEND_ACTIVITY_MAX_PER_USER}
     )`,
    [],
    { attempts: 2, label: 'FRIEND ACTIVITY RETENTION' }
  );

  await queryDbWithRetry(
    `DELETE FROM user_notifications
     WHERE id IN (
       SELECT id FROM (
         SELECT id, ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY id DESC) AS row_num
         FROM user_notifications
       ) ranked
       WHERE row_num > ${USER_NOTIFICATION_MAX_PER_USER}
     )`,
    [],
    { attempts: 2, label: 'USER NOTIFICATION RETENTION' }
  );

  await queryDbWithRetry(
    'INSERT INTO chat_sync_state (id, epoch, revision) VALUES (1, $1, 0) ON CONFLICT (id) DO NOTHING',
    [`${Date.now()}-${INSTANCE_ID}-${Math.random().toString(36).slice(2, 8)}`],
    { attempts: 2, label: 'CHAT SYNC STATE INIT' }
  );
  await refreshChatSyncStateFromDb();

  await pool.query(`DELETE FROM presence_sessions WHERE instance_id = $1 OR last_seen < NOW() - INTERVAL '${PRESENCE_TTL_SECONDS} seconds'`, [INSTANCE_ID]);
  await refreshAllUsersCacheFromDb({ preserveOnline: false });

  await refreshChatHistoryFromDb();
  
  const pinnedRes = await pool.query('SELECT data FROM pinned_messages ORDER BY id ASC');
  pinnedMessages = pinnedRes.rows.map(r => r.data);

  await refreshAdminStateFromDb();

  const reportsRes = await queryDbWithRetry('SELECT data FROM reports WHERE resolved = false ORDER BY created_at DESC LIMIT 100', [], { attempts: 2, label: 'REPORTS READ' });
  adminReports = reportsRes.rows.map(r => r.data);

  const modLogRes = await pool.query('SELECT entry FROM moderation_log ORDER BY created_at DESC LIMIT 100');
  moderationLog = modLogRes.rows.map(r => r.entry);

  const serverLogRes = await pool.query('SELECT entry FROM server_log ORDER BY created_at DESC LIMIT 120');
  serverLog = serverLogRes.rows.map(r => r.entry);

  console.log(`[DB] Database initialized. ${messageHistory.length} messages, ${pinnedMessages.length} pins, ${Object.keys(userDatabase).length} users loaded.`);
}

function getSanitizedOnlineList() {
  return Object.entries(userDatabase).filter(([, u]) => u && u.online === true).map(([username, u]) => ({
    id: u.id,
    name: username,
    avatar: u.avatar || DEFAULT_AVATAR,
    isAdmin: isUserAdmin(username, u),
    role: getUserRole(username, u),
    banned: isUserBanned(u),
    banReason: u.banReason || "",
    level: u.level || 1,
    joined: u.joined || '2026',
    countryCode: getUserCountryCode(u),
    online: u.online,
    lastSeen: u.lastSeen,
    ps3Status: u.ps3Status || null,
    downloads: u.downloads || (Array.isArray(u.downloadsData) ? u.downloadsData.length : 0),
    wishlist: u.wishlist || (Array.isArray(u.wishlistData) ? u.wishlistData.length : 0),
    favorites: u.favorites || (Array.isArray(u.favoritesData) ? u.favoritesData.length : 0),
    trophies: u.trophies || 0,
    library: u.library || (Array.isArray(u.libraryData) ? u.libraryData.length : 0)
  }));
}

async function getSanitizedOnlineListFromDb(options = {}) {
  if (!Object.keys(userDatabase).length) await ensureUserCacheReady();
  if (options.force !== true && Array.isArray(onlineListCache) && Date.now() - onlineListCacheAt < ONLINE_LIST_CACHE_MS) {
    return onlineListCache;
  }
  const list = getSanitizedOnlineList();
  onlineListCache = list;
  onlineListCacheAt = Date.now();
  return list;
}
async function calculateGlobalTrophyStatsFromDb() {
  const stats = {};

  const trophyRes = await queryDbWithRetry(`
    WITH total_users AS (
      SELECT COUNT(*)::numeric AS total FROM users
    ), unlocked_trophies AS (
      SELECT
        trophy.key AS trophy_id,
        COUNT(*)::numeric AS unlocked_count
      FROM users u
      CROSS JOIN LATERAL jsonb_each(
        CASE
          WHEN jsonb_typeof(u.data->'trophiesData') = 'object' THEN u.data->'trophiesData'
          ELSE '{}'::jsonb
        END
      ) AS trophy(key, value)
      WHERE LOWER(COALESCE(trophy.value->>'unlocked', 'false')) IN ('true', '1', 'yes')
      GROUP BY trophy.key
    )
    SELECT
      unlocked_trophies.trophy_id,
      CASE
        WHEN total_users.total > 0 THEN (unlocked_trophies.unlocked_count / total_users.total) * 100
        ELSE 0
      END AS percentage
    FROM unlocked_trophies, total_users
  `, [], { attempts: 2, label: 'TROPHY AGGREGATE' });

  trophyRes.rows.forEach(row => {
    if (row.trophy_id) stats[row.trophy_id] = Number(row.percentage) || 0;
  });

  return stats;
}


function normalizeText(value, fallback = "") {
  return String(value === undefined || value === null ? fallback : value).trim();
}


function normalizeDownloadHistoryNameServer(value) {
  return normalizeText(value, '').toLowerCase().replace(/&amp;/g, '&').replace(/[^a-z0-9]+/g, '');
}

function normalizeDownloadHistoryCategoryServer(value) {
  const raw = normalizeText(value, 'games').toLowerCase().replace(/[\s-]+/g, '_');
  const aliases = {
    game: 'games', app: 'apps', demo: 'demos', dlc: 'dlcs', update: 'updates',
    avatar: 'avatars', theme: 'themes', homebrew: 'homebrew_games', port: 'ports',
    prototype: 'prototypes', emulator: 'emulators', launcher: 'launchers', tool: 'tools',
    dev_tool: 'dev_tools', manager: 'backup_manager'
  };
  return aliases[raw] || raw || 'games';
}

function getDownloadHistoryItemCountServer(item = {}) {
  const count = Number(item.downloadCount);
  return Number.isFinite(count) && count > 0 ? Math.floor(count) : 1;
}

function isSameDownloadHistoryItemServer(first = {}, second = {}) {
  const firstCategory = normalizeDownloadHistoryCategoryServer(first.category || first.rawCategory || 'games');
  const secondCategory = normalizeDownloadHistoryCategoryServer(second.category || second.rawCategory || 'games');
  if (firstCategory !== secondCategory) return false;

  const firstTitleId = normalizeText(first.titleId || first.id, '').toUpperCase();
  const secondTitleId = normalizeText(second.titleId || second.id, '').toUpperCase();
  const firstContentId = normalizeText(first.contentId || first.contentID, '').toUpperCase();
  const secondContentId = normalizeText(second.contentId || second.contentID, '').toUpperCase();
  const firstName = normalizeDownloadHistoryNameServer(first.cleanName || first.name || first.title || first.rawName);
  const secondName = normalizeDownloadHistoryNameServer(second.cleanName || second.name || second.title || second.rawName);

  if (firstTitleId && secondTitleId && firstTitleId !== secondTitleId) return false;
  if (firstContentId && secondContentId) return firstContentId === secondContentId;

  if (firstTitleId && secondTitleId) {
    if (firstName && secondName) return firstName === secondName;
    return !firstContentId && !secondContentId;
  }

  if (firstContentId || secondContentId) return false;
  return !!(firstName && secondName && firstName === secondName);
}

function normalizeDownloadHistoryRecordsServer(history = []) {
  const source = Array.isArray(history) ? history : [];
  const grouped = [];
  let changed = !Array.isArray(history);

  source.forEach(rawItem => {
    if (!rawItem || typeof rawItem !== 'object' || Array.isArray(rawItem)) {
      changed = true;
      return;
    }

    const item = { ...rawItem };
    const rawItemCount = Number(item.downloadCount);
    const itemCount = getDownloadHistoryItemCountServer(item);
    if (Number.isFinite(rawItemCount) && rawItemCount > 0 && rawItemCount !== itemCount) changed = true;
    item.downloadCount = itemCount;
    const normalizedCategory = normalizeDownloadHistoryCategoryServer(item.category || item.rawCategory || 'games');
    if (String(item.category || '') !== normalizedCategory) changed = true;
    item.category = normalizedCategory;

    const existingIndex = grouped.findIndex(existing => isSameDownloadHistoryItemServer(existing, item));
    if (existingIndex < 0) {
      grouped.push(item);
      return;
    }

    changed = true;
    const existing = grouped[existingIndex];
    existing.downloadCount = getDownloadHistoryItemCountServer(existing) + itemCount;

    ['titleId', 'contentId', 'cleanName', 'name', 'cTag', 'console', 'rawCategory', 'cover'].forEach(key => {
      if (!existing[key] && item[key]) existing[key] = item[key];
    });
    if (item.noBingCover === true) existing.noBingCover = true;

    const existingSize = Number(existing.sizeBytes) || 0;
    const itemSize = Number(item.sizeBytes) || 0;
    if (itemSize > existingSize) {
      existing.sizeBytes = itemSize;
      if (item.sizeText) existing.sizeText = item.sizeText;
    } else if (!existing.sizeText && item.sizeText) {
      existing.sizeText = item.sizeText;
    }
  });

  if (grouped.length !== source.length) changed = true;
  return { history: grouped, changed };
}


function normalizeLibraryIdentityTextServer(value) {
  return normalizeText(value, '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&amp;/g, '&')
    .replace(/[^a-z0-9]+/g, '');
}

function normalizeLibraryGamePathServer(value) {
  return normalizeText(value, '')
    .replace(/^https?:\/\/[^/]+/i, '')
    .replace(/\\/g, '/')
    .replace(/\/{2,}/g, '/')
    .toLowerCase();
}

function getLibraryGameTitleIdServer(game = {}) {
  const candidates = [game.titleId, game.id, game.path, game.title];
  for (const candidate of candidates) {
    const match = normalizeText(candidate, '').toUpperCase().match(/[A-Z]{4}\d{5}/);
    if (match) return match[0];
  }
  return '';
}

function getLibraryLastPlayedTimestampServer(game = {}) {
  const explicit = normalizeTimestampValue(game && game.lastPlayedAt);
  if (explicit) return explicit;

  const text = normalizeText(game && game.lastPlayed, '');
  if (!text) return 0;

  const match = text.match(/^([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})\s+-\s+(\d{1,2}):(\d{2})$/);
  if (match) {
    const monthIndex = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].indexOf(match[1].toLowerCase());
    if (monthIndex >= 0) {
      const parsed = new Date(
        Number(match[3]),
        monthIndex,
        Number(match[2]),
        Number(match[4]),
        Number(match[5]),
        0,
        0
      ).getTime();
      if (Number.isFinite(parsed) && parsed > 0) return parsed;
    }
  }

  const fallback = Date.parse(text.replace(/\s+-\s+/, ' '));
  return Number.isFinite(fallback) && fallback > 0 ? fallback : 0;
}

function isSameLibraryGameServer(first = {}, second = {}) {
  const firstTitleId = getLibraryGameTitleIdServer(first);
  const secondTitleId = getLibraryGameTitleIdServer(second);
  if (firstTitleId && secondTitleId) return firstTitleId === secondTitleId;

  const firstPath = normalizeLibraryGamePathServer(first.path);
  const secondPath = normalizeLibraryGamePathServer(second.path);
  if (firstPath && secondPath && firstPath === secondPath) return true;

  const firstTitle = normalizeLibraryIdentityTextServer(first.title || first.name);
  const secondTitle = normalizeLibraryIdentityTextServer(second.title || second.name);
  return !!(firstTitle && secondTitle && firstTitle === secondTitle);
}

function mergeLibraryLastPlayedRecordServer(primary = {}, fallback = {}) {
  const merged = { ...primary };
  const ownTimestamp = getLibraryLastPlayedTimestampServer(merged);

  if (!fallback || typeof fallback !== 'object' || Array.isArray(fallback) || !isSameLibraryGameServer(merged, fallback)) {
    if (ownTimestamp && !normalizeTimestampValue(merged.lastPlayedAt)) merged.lastPlayedAt = ownTimestamp;
    return merged;
  }

  if (!merged.titleId && fallback.titleId) merged.titleId = fallback.titleId;
  if (!merged.id && fallback.id) merged.id = fallback.id;

  const fallbackTimestamp = getLibraryLastPlayedTimestampServer(fallback);
  const shouldUseFallback = fallbackTimestamp > ownTimestamp ||
    (!merged.lastPlayed && !!fallback.lastPlayed) ||
    (!ownTimestamp && fallbackTimestamp > 0);

  if (shouldUseFallback) {
    if (fallback.lastPlayed) merged.lastPlayed = fallback.lastPlayed;
    if (fallbackTimestamp) merged.lastPlayedAt = fallbackTimestamp;
  } else if (ownTimestamp && !normalizeTimestampValue(merged.lastPlayedAt)) {
    merged.lastPlayedAt = ownTimestamp;
  }

  return merged;
}

function mergeLibraryRecordsServer(primaryList = [], fallbackList = []) {
  const primary = Array.isArray(primaryList) ? primaryList : [];
  const fallback = Array.isArray(fallbackList) ? fallbackList : [];

  return primary
    .filter(item => item && typeof item === 'object' && !Array.isArray(item))
    .map(item => {
      const previous = fallback.find(candidate =>
        candidate && typeof candidate === 'object' && !Array.isArray(candidate) &&
        isSameLibraryGameServer(item, candidate)
      );
      return mergeLibraryLastPlayedRecordServer(item, previous || {});
    });
}

function normalizeProfileCountryCodeServer(value) {
  const code = normalizeText(value, "").toUpperCase();
  return VALID_PROFILE_COUNTRY_CODES.has(code) ? code : "";
}

function getUserCountryCode(user = {}) {
  const settings = user && user.settingsData && typeof user.settingsData === "object" && !Array.isArray(user.settingsData)
    ? user.settingsData
    : {};
  return normalizeProfileCountryCodeServer(
    user && (user.countryCode || user.country_code || user.country) ||
    settings.countryCode || settings.country_code || settings.country
  );
}

function getAdminCountryStats() {
  const counts = new Map();
  let total = 0;
  let unknown = 0;

  Object.values(userDatabase || {}).forEach(user => {
    total++;
    const code = getUserCountryCode(user || {});
    if (!code) {
      unknown++;
      return;
    }
    counts.set(code, (counts.get(code) || 0) + 1);
  });

  const countries = Array.from(counts.entries())
    .map(([code, count]) => ({ code, count }))
    .sort((a, b) => b.count - a.count || a.code.localeCompare(b.code));

  return {
    total,
    known: Math.max(0, total - unknown),
    unknown,
    countries
  };
}

function hasProfileCountryPayload(payload = {}) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const settings = payload.settingsData && typeof payload.settingsData === "object" && !Array.isArray(payload.settingsData)
    ? payload.settingsData
    : {};
  return ["countryCode", "country_code", "country"].some(key => Object.prototype.hasOwnProperty.call(payload, key)) ||
    ["countryCode", "country_code", "country"].some(key => Object.prototype.hasOwnProperty.call(settings, key));
}

function normalizeIncomingProfileCountry(payload = {}, settings = null) {
  const hasCountry = hasProfileCountryPayload({ ...payload, settingsData: settings || payload.settingsData });
  if (!hasCountry) return { hasCountry: false, countryCode: "" };

  const countryCode = normalizeProfileCountryCodeServer(
    payload.countryCode || payload.country_code || payload.country ||
    (settings && (settings.countryCode || settings.country_code || settings.country))
  );

  [payload, settings].forEach(target => {
    if (!target || typeof target !== "object" || Array.isArray(target)) return;
    delete target.country_code;
    delete target.country;
    if (countryCode) target.countryCode = countryCode;
    else delete target.countryCode;
  });

  return { hasCountry: true, countryCode };
}

function normalizePs3PlayTimeServer(value) {
  const text = normalizeText(value, '');
  const match = text.match(/^(\d{1,4}):([0-5]\d):([0-5]\d)$/);
  if (!match) return '';
  return `${match[1].padStart(2, '0')}:${match[2]}:${match[3]}`;
}

function normalizeTimeValue(value, fallback = "00:00") {
  const text = normalizeText(value, fallback);
  const match = text.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return fallback;
  const hour = Math.max(0, Math.min(23, parseInt(match[1], 10) || 0));
  const minute = Math.max(0, Math.min(59, parseInt(match[2], 10) || 0));
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function parseTimeToMinutes(value) {
  const safe = normalizeTimeValue(value, "00:00");
  const [hour, minute] = safe.split(':').map(n => parseInt(n, 10) || 0);
  return hour * 60 + minute;
}

function normalizeMaintenanceSchedule(data = {}) {
  const rawDays = Array.isArray(data.days) ? data.days : [];
  const days = [...new Set(rawDays.map(day => parseInt(day, 10)).filter(day => day >= 0 && day <= 6))].sort((a, b) => a - b);
  return {
    enabled: !!data.enabled,
    days,
    startTime: normalizeTimeValue(data.startTime, "02:00"),
    endTime: normalizeTimeValue(data.endTime, "03:00"),
    timezone: normalizeText(data.timezone, "America/Sao_Paulo") || "America/Sao_Paulo"
  };
}

function getZonedNowParts(date = new Date(), timezone = "America/Sao_Paulo") {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone || "America/Sao_Paulo",
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23'
  }).formatToParts(date);
  const get = type => (parts.find(part => part.type === type) || {}).value || '';
  const dayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  const hour = parseInt(get('hour'), 10) || 0;
  const minute = parseInt(get('minute'), 10) || 0;
  const second = parseInt(get('second'), 10) || 0;
  return {
    day: dayMap[get('weekday')] ?? 0,
    minutes: hour * 60 + minute,
    seconds: second
  };
}

function getMaintenanceScheduleStatus(schedule = {}, now = new Date()) {
  const normalized = normalizeMaintenanceSchedule(schedule);
  if (!normalized.enabled || !normalized.days.length) {
    return { active: false, activeUntil: null };
  }

  const start = parseTimeToMinutes(normalized.startTime);
  const end = parseTimeToMinutes(normalized.endTime);
  if (start === end) return { active: false, activeUntil: null };

  const current = getZonedNowParts(now, normalized.timezone);
  const previousDay = (current.day + 6) % 7;
  let active = false;
  let minutesLeft = 0;

  if (start < end) {
    active = normalized.days.includes(current.day) && current.minutes >= start && current.minutes < end;
    minutesLeft = active ? end - current.minutes : 0;
  } else {
    const activeFromToday = normalized.days.includes(current.day) && current.minutes >= start;
    const activeFromYesterday = normalized.days.includes(previousDay) && current.minutes < end;
    active = activeFromToday || activeFromYesterday;
    minutesLeft = activeFromToday ? (1440 - current.minutes + end) : (activeFromYesterday ? end - current.minutes : 0);
  }

  if (!active) return { active: false, activeUntil: null };
  const msLeft = Math.max(0, (minutesLeft * 60 - current.seconds) * 1000);
  return { active: true, activeUntil: new Date(now.getTime() + msLeft).toISOString() };
}

function getRawUserRole(userData = null) {
  return normalizeText(userData && userData.role, "").toLowerCase();
}

function getUserRole(name, userData = null) {
  const fallbackRole = ADMIN_USERS.includes(name) ? "admin" : "user";
  const rawRole = normalizeText(userData && userData.role, fallbackRole).toLowerCase();
  if (rawRole === "moderator") return "mod";
  if (rawRole === "banned") return fallbackRole;
  return VALID_USER_ROLES.has(rawRole) ? rawRole : fallbackRole;
}

function isUserAdmin(name, userData = null) {
  return ADMIN_USERS.includes(name) || getUserRole(name, userData) === "admin";
}

function isUserModerator(name, userData = null) {
  return getUserRole(name, userData) === "mod";
}

function canModerateSocket(socket) {
  if (!socket || !socket.userName) return false;
  if (socket.isAdmin === true) return true;
  const user = userDatabase[socket.userName] || null;
  return isUserModerator(socket.userName, user);
}

function canModerateTarget(socket, targetName = "") {
  if (!canModerateSocket(socket)) return false;
  if (socket.isAdmin === true) return true;
  const targetUser = targetName ? (userDatabase[targetName] || null) : null;
  const targetRole = targetName ? getUserRole(targetName, targetUser) : "user";
  // Moderators can moderate regular/trusted users, including banned accounts, but not admins or other mods.
  return !["admin", "mod"].includes(targetRole);
}

function getActorRole(socket) {
  if (!socket || !socket.userName) return "user";
  return socket.isAdmin === true ? "admin" : getUserRole(socket.userName, userDatabase[socket.userName] || null);
}

function isUserBanned(userData = null) {
  return !!(userData && (userData.banned === true || getRawUserRole(userData) === "banned"));
}

const RECENTLY_VISITED_MAX_ITEMS = 10;

function normalizeRecentlyVisitedRecordsServer(records = []) {
  const seen = new Set();
  const normalized = [];
  for (const raw of Array.isArray(records) ? records : []) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
    const id = normalizeText(raw.id || raw.titleId, '').toUpperCase().slice(0, 32);
    const contentId = normalizeText(raw.contentId, '').toUpperCase().slice(0, 160);
    const cleanName = normalizeText(raw.cleanName || raw.name, '').slice(0, 180);
    const cTag = normalizeText(raw.cTag, '').toLowerCase().slice(0, 16);
    const region = normalizeText(raw.region, '').toUpperCase().slice(0, 16);
    if (!cleanName || (!contentId && !id)) continue;
    const key = contentId && contentId !== 'MISSING' ? `content:${contentId}` : `title:${id}|${region}`;
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push({ id, contentId, cleanName, cTag, region, category: 'games' });
    if (normalized.length >= RECENTLY_VISITED_MAX_ITEMS) break;
  }
  return normalized;
}

function normalizeUserRecord(name, userData = {}) {
  const legacyBannedRole = getRawUserRole(userData) === "banned";
  const normalized = {
    ...userData,
    name,
    role: getUserRole(name, userData),
    banned: userData.banned === true || legacyBannedRole
  };

  const countryCode = getUserCountryCode(userData);
  if (countryCode) {
    normalized.countryCode = countryCode;
    normalized.settingsData = normalized.settingsData && typeof normalized.settingsData === "object" && !Array.isArray(normalized.settingsData)
      ? { ...normalized.settingsData, countryCode }
      : { countryCode };
  }

  if (!normalized.banned) {
    delete normalized.banReason;
    delete normalized.bannedBy;
    delete normalized.bannedAt;
  }

  if (Array.isArray(normalized.downloadsData)) {
    normalized.downloadsData = normalizeDownloadHistoryRecordsServer(normalized.downloadsData).history;
    normalized.downloads = normalized.downloadsData.length;
  }

  if (Array.isArray(normalized.libraryData)) {
    normalized.libraryData = mergeLibraryRecordsServer(normalized.libraryData, []);
    normalized.library = normalized.libraryData.length;
  }

  if (Array.isArray(normalized.recentlyVisitedData)) {
    normalized.recentlyVisitedData = normalizeRecentlyVisitedRecordsServer(normalized.recentlyVisitedData);
    normalized.recentlyVisited = normalized.recentlyVisitedData.length;
    normalized.recentlyVisitedUpdatedAt = normalizeTimestampValue(normalized.recentlyVisitedUpdatedAt);
  }

  return normalized;
}



function hasObjectPayload(value) {
  return !!(value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length > 0);
}





function countUnlockedTrophiesPayload(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return 0;
  return Object.values(value).reduce((count, trophy) => {
    if (!trophy || typeof trophy !== 'object') return count;
    const unlocked = String(trophy.unlocked || '').toLowerCase();
    return count + (unlocked === 'true' || unlocked === '1' || unlocked === 'yes' ? 1 : 0);
  }, 0);
}


function shouldAcceptIncomingTrophies(currentUser = {}, incomingUser = {}) {
  if (!incomingUser || !hasObjectPayload(incomingUser.trophiesData)) return false;
  const currentUnlocked = countUnlockedTrophiesPayload(currentUser.trophiesData);
  const incomingUnlocked = countUnlockedTrophiesPayload(incomingUser.trophiesData);
  if (incomingUnlocked > currentUnlocked) return true;
  if (incomingUnlocked === currentUnlocked) {
    const currentSize = hasObjectPayload(currentUser.trophiesData) ? Object.keys(currentUser.trophiesData).length : 0;
    const incomingSize = hasObjectPayload(incomingUser.trophiesData) ? Object.keys(incomingUser.trophiesData).length : 0;
    return incomingSize >= currentSize;
  }
  return false;
}

function mergeIncomingTrophiesPreservingUnlockState(currentTrophies = {}, incomingTrophies = {}) {
  const current = hasObjectPayload(currentTrophies) ? currentTrophies : {};
  const incoming = hasObjectPayload(incomingTrophies) ? incomingTrophies : {};
  const merged = { ...incoming };

  Object.entries(current).forEach(([id, currentState]) => {
    if (!currentState || typeof currentState !== 'object' || Array.isArray(currentState)) return;
    const currentUnlocked = String(currentState.unlocked || '').toLowerCase();
    if (!['true', '1', 'yes'].includes(currentUnlocked)) return;

    const incomingState = merged[id] && typeof merged[id] === 'object' && !Array.isArray(merged[id]) ? merged[id] : {};
    merged[id] = {
      ...currentState,
      ...incomingState,
      unlocked: true,
      isNew: currentState.isNew === true
    };

    if (currentState.unlockDate) merged[id].unlockDate = currentState.unlockDate;
  });

  return merged;
}

const PROFILE_ARRAY_SYNC_KEYS = {
  downloadsData: { versionKey: 'downloadsUpdatedAt', countKey: 'downloads' },
  wishlistData: { versionKey: 'wishlistUpdatedAt', countKey: 'wishlist' },
  favoritesData: { versionKey: 'favoritesUpdatedAt', countKey: 'favorites' },
  libraryData: { versionKey: 'libraryUpdatedAt', countKey: 'library' },
  friendsData: { versionKey: 'friendsUpdatedAt', countKey: 'friends' },
  recentlyVisitedData: { versionKey: 'recentlyVisitedUpdatedAt', countKey: 'recentlyVisited' }
};

const PROFILE_REPLAY_SECTION_DATA_KEYS = {
  trophies: 'trophiesData',
  downloads: 'downloadsData',
  wishlist: 'wishlistData',
  favorites: 'favoritesData',
  library: 'libraryData',
  friends: 'friendsData',
  recentlyvisited: 'recentlyVisitedData'
};

function normalizeTimestampValue(value) {
  const timestamp = Number(value || 0);
  return Number.isFinite(timestamp) && timestamp > 0 ? timestamp : 0;
}

const VALID_PROFILE_CARD_STYLES = new Set(["default", "neon", "galaxy", "sunset", "ghost", "royal", "matrix", "lovely", "wave", "scan", "nightgrid", "glass", "nebula", "spotlight"]);
const PROFILE_CARD_STYLE_UPDATED_AT_KEY = "profileCardStyleUpdatedAt";
const PROFILE_CARD_STYLE_TIMESTAMP_ALIASES = ["profileCardStyleUpdatedAt", "profileCardEffectUpdatedAt", "profileCardThemeUpdatedAt", "bannerUpdatedAt"];
const PROFILE_SETTINGS_UPDATED_AT_KEY = "settingsUpdatedAt";
const PROFILE_SETTINGS_TIMESTAMP_ALIASES = ["settingsUpdatedAt", "settingsSyncedAt", "settingsVersion"];
const PROFILE_THEME_COLOR_UPDATED_AT_KEY = "themeColorUpdatedAt";
const PROFILE_THEME_COLOR_TIMESTAMP_ALIASES = ["themeColorUpdatedAt", "themeUpdatedAt", "themeColorSyncedAt"];
const PROFILE_BANNER_SETTING_KEYS = new Set(["profileCardStyle", "profileCardEffect", "profileCardTheme", ...PROFILE_CARD_STYLE_TIMESTAMP_ALIASES]);
const PROFILE_SETTINGS_META_KEYS = new Set([...PROFILE_SETTINGS_TIMESTAMP_ALIASES, ...PROFILE_CARD_STYLE_TIMESTAMP_ALIASES, ...PROFILE_THEME_COLOR_TIMESTAMP_ALIASES]);

function normalizeProfileCardStyleServer(value, fallback = "default") {
  const style = normalizeText(value, fallback).toLowerCase();
  if (style === "xmb") return "lovely";
  if (style === "inferno") return "spotlight";
  return VALID_PROFILE_CARD_STYLES.has(style) ? style : fallback;
}

function hasProfileBannerStylePayload(settings = {}) {
  return !!(settings && typeof settings === "object" && (
    Object.prototype.hasOwnProperty.call(settings, "profileCardStyle") ||
    Object.prototype.hasOwnProperty.call(settings, "profileCardEffect") ||
    Object.prototype.hasOwnProperty.call(settings, "profileCardTheme")
  ));
}

function getProfileBannerUpdatedAt(settings = {}, fallback = 0) {
  if (settings && typeof settings === "object") {
    for (const key of PROFILE_CARD_STYLE_TIMESTAMP_ALIASES) {
      const timestamp = normalizeTimestampValue(settings[key]);
      if (timestamp) return timestamp;
    }
  }
  return normalizeTimestampValue(fallback);
}

function getUserProfileCardStyle(user = {}) {
  const settings = user && user.settingsData && typeof user.settingsData === "object" ? user.settingsData : {};
  return normalizeProfileCardStyleServer(settings.profileCardStyle || settings.profileCardEffect || settings.profileCardTheme || user.profileCardStyle || user.profileCardEffect || "default");
}

function getUserProfileCardStyleUpdatedAt(user = {}) {
  const settings = user && user.settingsData && typeof user.settingsData === "object" ? user.settingsData : {};
  return getProfileBannerUpdatedAt(settings, user.profileCardStyleUpdatedAt || user.profileCardEffectUpdatedAt || user.profileUpdatedAt);
}

function getPublicProfileSettings(user = {}) {
  const profileCardStyle = getUserProfileCardStyle(user);
  const profileCardStyleUpdatedAt = getUserProfileCardStyleUpdatedAt(user) || normalizeTimestampValue(user.profileUpdatedAt) || Date.now();
  const themeColor = normalizeThemeColorServer(user.themeColor || (user.settingsData && user.settingsData.themeColor) || "#0070cc");
  const themeColorUpdatedAt = getUserThemeColorUpdatedAt(user) || 0;
  return {
    profileCardStyle,
    profileCardEffect: profileCardStyle,
    profileCardStyleUpdatedAt,
    themeColor,
    themeColorUpdatedAt,
    countryCode: getUserCountryCode(user)
  };
}

function getProfileSettingsUpdatedAt(settings = {}, fallback = 0) {
  if (settings && typeof settings === "object") {
    for (const key of PROFILE_SETTINGS_TIMESTAMP_ALIASES) {
      const timestamp = normalizeTimestampValue(settings[key]);
      if (timestamp) return timestamp;
    }
  }
  return normalizeTimestampValue(fallback);
}

function normalizeThemeColorServer(value, fallback = "#0070cc") {
  const raw = normalizeText(value, "").toLowerCase();
  if (!raw) return fallback;
  const color = raw.startsWith("#") ? raw : `#${raw}`;
  return /^#[0-9a-f]{6}$/i.test(color) ? color : fallback;
}

function getProfileThemeColorUpdatedAt(settings = {}, fallback = 0) {
  if (settings && typeof settings === "object") {
    for (const key of PROFILE_THEME_COLOR_TIMESTAMP_ALIASES) {
      const timestamp = normalizeTimestampValue(settings[key]);
      if (timestamp) return timestamp;
    }
  }
  return normalizeTimestampValue(fallback);
}

function hasThemeColorPayload(settings = {}, userData = {}) {
  return !!(
    (settings && typeof settings === "object" && Object.prototype.hasOwnProperty.call(settings, "themeColor")) ||
    (userData && typeof userData === "object" && Object.prototype.hasOwnProperty.call(userData, "themeColor"))
  );
}

function getUserThemeColorUpdatedAt(user = {}) {
  const settings = user && user.settingsData && typeof user.settingsData === "object" ? user.settingsData : {};
  return getProfileThemeColorUpdatedAt(settings, user.themeColorUpdatedAt || user.themeUpdatedAt || 0);
}

function reconcileIncomingThemeColor(currentUser = {}, incomingUser = {}, incomingSettings = {}) {
  if (!hasThemeColorPayload(incomingSettings, incomingUser)) {
    return { accepted: false, rejected: false };
  }

  const incomingTheme = normalizeThemeColorServer(
    (incomingSettings && incomingSettings.themeColor) || (incomingUser && incomingUser.themeColor) || "",
    ""
  );
  if (!incomingTheme) return { accepted: false, rejected: true };

  const currentTheme = normalizeThemeColorServer(currentUser.themeColor || (currentUser.settingsData && currentUser.settingsData.themeColor) || "#0070cc");
  const currentUpdatedAt = getUserThemeColorUpdatedAt(currentUser);
  const incomingUpdatedAt = Math.max(
    getProfileThemeColorUpdatedAt(incomingSettings || {}),
    getProfileThemeColorUpdatedAt(incomingUser || {})
  );

  const currentHasCustomTheme = currentTheme && currentTheme !== "#0070cc";
  const acceptIncoming = !!(
    (incomingUpdatedAt && (!currentUpdatedAt || incomingUpdatedAt >= currentUpdatedAt)) ||
    (!incomingUpdatedAt && !currentUpdatedAt && !currentHasCustomTheme)
  );

  if (!acceptIncoming) {
    return { accepted: false, rejected: true, currentTheme, currentUpdatedAt };
  }

  currentUser.themeColor = incomingTheme;
  currentUser.themeColorUpdatedAt = incomingUpdatedAt || currentUpdatedAt || normalizeTimestampValue(currentUser.profileUpdatedAt) || Date.now();
  currentUser.settingsData = currentUser.settingsData && typeof currentUser.settingsData === "object" ? currentUser.settingsData : {};
  currentUser.settingsData.themeColor = incomingTheme;
  currentUser.settingsData[PROFILE_THEME_COLOR_UPDATED_AT_KEY] = currentUser.themeColorUpdatedAt;
  return { accepted: currentTheme !== incomingTheme || incomingUpdatedAt > currentUpdatedAt, rejected: false, themeColor: incomingTheme, themeColorUpdatedAt: currentUser.themeColorUpdatedAt };
}

function hasRealtimeSettingsPayload(settings = {}) {
  if (!settings || typeof settings !== "object" || Array.isArray(settings)) return false;
  return Object.keys(settings).some(key => !PROFILE_BANNER_SETTING_KEYS.has(key) && !PROFILE_SETTINGS_META_KEYS.has(key));
}

function normalizeProfileRealtimeSettings(settings = {}) {
  const clean = normalizeProfileBannerSettings(settings || {});
  const updatedAt = getProfileSettingsUpdatedAt(settings || {});
  const themeUpdatedAt = getProfileThemeColorUpdatedAt(settings || {});
  if (settings && typeof settings === "object" && Object.prototype.hasOwnProperty.call(settings, "themeColor")) {
    clean.themeColor = normalizeThemeColorServer(settings.themeColor, "#0070cc");
  }
  if (themeUpdatedAt) clean[PROFILE_THEME_COLOR_UPDATED_AT_KEY] = themeUpdatedAt;
  if (updatedAt) clean[PROFILE_SETTINGS_UPDATED_AT_KEY] = updatedAt;
  PROFILE_SETTINGS_TIMESTAMP_ALIASES.forEach(key => {
    if (key !== PROFILE_SETTINGS_UPDATED_AT_KEY) delete clean[key];
  });
  PROFILE_THEME_COLOR_TIMESTAMP_ALIASES.forEach(key => {
    if (key !== PROFILE_THEME_COLOR_UPDATED_AT_KEY) delete clean[key];
  });
  return clean;
}

function normalizeProfileBannerSettings(settings = {}) {
  const clean = settings && typeof settings === "object" ? { ...settings } : {};
  const hasStylePayload = hasProfileBannerStylePayload(clean);
  const updatedAt = getProfileBannerUpdatedAt(clean);

  if (hasStylePayload) {
    const style = normalizeProfileCardStyleServer(clean.profileCardStyle || clean.profileCardEffect || clean.profileCardTheme || "default");
    clean.profileCardStyle = style;
    clean.profileCardEffect = style;
    delete clean.profileCardTheme;
  }

  if (updatedAt) clean[PROFILE_CARD_STYLE_UPDATED_AT_KEY] = updatedAt;
  PROFILE_CARD_STYLE_TIMESTAMP_ALIASES.forEach(key => {
    if (key !== PROFILE_CARD_STYLE_UPDATED_AT_KEY) delete clean[key];
  });

  return clean;
}

function mergeProfileBannerSettingsByTimestamp(currentSettings = {}, incomingSettings = {}, options = {}) {
  const current = normalizeProfileBannerSettings(currentSettings || {});
  const incoming = normalizeProfileBannerSettings(incomingSettings || {});
  const incomingHasBanner = hasProfileBannerStylePayload(incomingSettings);
  const currentHasBanner = hasProfileBannerStylePayload(current) || !!(current.profileCardStyle || current.profileCardEffect);
  const currentUpdatedAt = getProfileBannerUpdatedAt(current, options.currentFallback || 0);
  const incomingUpdatedAt = getProfileBannerUpdatedAt(incoming, options.incomingFallback || 0);
  const merged = { ...current, ...incoming };
  let bannerAccepted = false;
  let bannerRejected = false;

  if (incomingHasBanner) {
    const currentStyle = normalizeProfileCardStyleServer(current.profileCardStyle || current.profileCardEffect || current.profileCardTheme || "default");
    const incomingStyle = normalizeProfileCardStyleServer(incoming.profileCardStyle || incoming.profileCardEffect || incoming.profileCardTheme || "default");
    const acceptIncomingBanner = !!(
      (incomingUpdatedAt && (!currentUpdatedAt || incomingUpdatedAt >= currentUpdatedAt)) ||
      (!currentUpdatedAt && !incomingUpdatedAt) ||
      !currentHasBanner
    );

    if (acceptIncomingBanner) {
      merged.profileCardStyle = incomingStyle;
      merged.profileCardEffect = incomingStyle;
      if (incomingUpdatedAt) merged[PROFILE_CARD_STYLE_UPDATED_AT_KEY] = incomingUpdatedAt;
      else if (currentUpdatedAt) merged[PROFILE_CARD_STYLE_UPDATED_AT_KEY] = currentUpdatedAt;
      bannerAccepted = currentStyle !== incomingStyle || incomingUpdatedAt > currentUpdatedAt;
    } else {
      merged.profileCardStyle = currentStyle;
      merged.profileCardEffect = currentStyle;
      if (currentUpdatedAt) merged[PROFILE_CARD_STYLE_UPDATED_AT_KEY] = currentUpdatedAt;
      bannerRejected = true;
    }
  } else if (currentHasBanner) {
    const currentStyle = normalizeProfileCardStyleServer(current.profileCardStyle || current.profileCardEffect || current.profileCardTheme || "default");
    merged.profileCardStyle = currentStyle;
    merged.profileCardEffect = currentStyle;
    if (currentUpdatedAt) merged[PROFILE_CARD_STYLE_UPDATED_AT_KEY] = currentUpdatedAt;
  }

  delete merged.profileCardTheme;
  return { settingsData: merged, bannerAccepted, bannerRejected };
}

function mergeProfileSettingsByTimestamp(currentSettings = {}, incomingSettings = {}, options = {}) {
  const current = normalizeProfileRealtimeSettings(currentSettings || {});
  const incoming = normalizeProfileRealtimeSettings(incomingSettings || {});
  const bannerMerge = mergeProfileBannerSettingsByTimestamp(current, incoming, options);
  const currentSettingsUpdatedAt = getProfileSettingsUpdatedAt(current);
  const incomingSettingsUpdatedAt = getProfileSettingsUpdatedAt(incoming);
  const incomingHasSettings = hasRealtimeSettingsPayload(incomingSettings);
  const acceptIncomingSettings = !!(incomingHasSettings && (
    (incomingSettingsUpdatedAt && (!currentSettingsUpdatedAt || incomingSettingsUpdatedAt >= currentSettingsUpdatedAt)) ||
    (!currentSettingsUpdatedAt && !incomingSettingsUpdatedAt)
  ));

  const merged = { ...current };

  if (acceptIncomingSettings) {
    Object.keys(incoming).forEach(key => {
      if (PROFILE_BANNER_SETTING_KEYS.has(key) || PROFILE_SETTINGS_META_KEYS.has(key) || key === "themeColor") return;
      merged[key] = incoming[key];
    });
    if (incomingSettingsUpdatedAt) merged[PROFILE_SETTINGS_UPDATED_AT_KEY] = incomingSettingsUpdatedAt;
  } else if (currentSettingsUpdatedAt) {
    merged[PROFILE_SETTINGS_UPDATED_AT_KEY] = currentSettingsUpdatedAt;
  }

  ["profileCardStyle", "profileCardEffect", PROFILE_CARD_STYLE_UPDATED_AT_KEY].forEach(key => {
    if (Object.prototype.hasOwnProperty.call(bannerMerge.settingsData, key)) merged[key] = bannerMerge.settingsData[key];
  });
  delete merged.profileCardTheme;

  return {
    settingsData: merged,
    bannerAccepted: bannerMerge.bannerAccepted,
    bannerRejected: bannerMerge.bannerRejected,
    settingsAccepted: acceptIncomingSettings,
    settingsRejected: incomingHasSettings && !acceptIncomingSettings
  };
}

function emitPublicProfileBannerUpdate(name, user = null) {
  if (!name || !user) return;
  const settingsData = getPublicProfileSettings(user);
  io.emit("profile_public_update", {
    name,
    profileUpdatedAt: normalizeTimestampValue(user.profileUpdatedAt) || Date.now(),
    settingsData,
    profileCardStyle: settingsData.profileCardStyle,
    profileCardEffect: settingsData.profileCardEffect,
    profileCardStyleUpdatedAt: settingsData.profileCardStyleUpdatedAt,
    themeColor: settingsData.themeColor,
    themeColorUpdatedAt: settingsData.themeColorUpdatedAt,
    countryCode: settingsData.countryCode
  });
}

function getProfileArrayPayload(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeProfileArrayListServer(key, value, existing = []) {
  const list = getProfileArrayPayload(value);
  if (key === 'downloadsData') return normalizeDownloadHistoryRecordsServer(list).history;
  if (key === 'libraryData') return mergeLibraryRecordsServer(list, getProfileArrayPayload(existing));
  if (key === 'recentlyVisitedData') return normalizeRecentlyVisitedRecordsServer(list);
  return list;
}

function hasOwnPayload(target = {}, key = '') {
  return Object.prototype.hasOwnProperty.call(target || {}, key);
}


function normalizeProfileArrayPayloads(target = {}) {
  Object.keys(PROFILE_ARRAY_SYNC_KEYS).forEach(key => {
    const sync = PROFILE_ARRAY_SYNC_KEYS[key];
    const list = normalizeProfileArrayListServer(key, target[key]);
    target[key] = list;
    target[sync.countKey] = list.length;
    target[sync.versionKey] = normalizeTimestampValue(target[sync.versionKey]);
  });
  return target;
}

function reconcileIncomingProfileArrays(currentUser = {}, incomingUser = {}) {
  Object.keys(PROFILE_ARRAY_SYNC_KEYS).forEach(key => {
    const sync = PROFILE_ARRAY_SYNC_KEYS[key];
    const hasIncomingArray = hasOwnPayload(incomingUser, key);
    const hasIncomingCount = hasOwnPayload(incomingUser, sync.countKey);
    const hasIncomingVersion = hasOwnPayload(incomingUser, sync.versionKey);
    if (!hasIncomingArray && !hasIncomingCount && !hasIncomingVersion) return;

    const currentList = normalizeProfileArrayListServer(key, currentUser[key]);
    const incomingList = normalizeProfileArrayListServer(key, incomingUser[key]);
    const currentVersion = normalizeTimestampValue(currentUser[sync.versionKey]);
    const incomingVersion = normalizeTimestampValue(incomingUser[sync.versionKey]);
    const currentHasItems = currentList.length > 0;
    const incomingHasItems = incomingList.length > 0;

    let acceptIncoming = false;

    if (incomingVersion && (!currentVersion || incomingVersion >= currentVersion)) {
      acceptIncoming = hasIncomingArray;
    } else if (!currentVersion && !currentHasItems && incomingHasItems && hasIncomingArray) {
      // One-time migration/recovery path: old localStorage can seed an empty DB.
      acceptIncoming = true;
    } else if (!currentVersion && !incomingVersion && !currentHasItems && hasIncomingArray) {
      acceptIncoming = true;
    }

    currentUser[key] = currentList;
    currentUser[sync.countKey] = currentList.length;

    if (acceptIncoming) {
      const acceptedList = normalizeProfileArrayListServer(key, incomingList, currentList);
      incomingUser[key] = acceptedList;
      incomingUser[sync.countKey] = acceptedList.length;
      incomingUser[sync.versionKey] = incomingVersion || normalizeTimestampValue(incomingUser.profileUpdatedAt) || Date.now();
    } else {
      delete incomingUser[key];
      delete incomingUser[sync.countKey];
      delete incomingUser[sync.versionKey];
    }
  });

  return incomingUser;
}




function reconcileIncomingDownloads(currentUser = {}, incomingUser = {}) {
  const currentClearAt = normalizeTimestampValue(currentUser.downloadsClearedAt);
  const incomingClearAt = normalizeTimestampValue(incomingUser.downloadsClearedAt);
  const currentDownloadsUpdatedAt = normalizeTimestampValue(currentUser.downloadsUpdatedAt || currentUser.profileUpdatedAt);
  const incomingDownloadsUpdatedAt = normalizeTimestampValue(incomingUser.downloadsUpdatedAt || incomingUser.profileUpdatedAt);
  const hasIncomingDownloadsData = Object.prototype.hasOwnProperty.call(incomingUser, 'downloadsData');
  const hasIncomingDownloadsCount = Object.prototype.hasOwnProperty.call(incomingUser, 'downloads');
  const hasIncomingDownloadsVersion = Object.prototype.hasOwnProperty.call(incomingUser, 'downloadsUpdatedAt');

  if (incomingClearAt > currentClearAt && (!hasIncomingDownloadsData || incomingClearAt >= incomingDownloadsUpdatedAt)) {
    incomingUser.downloadsClearedAt = incomingClearAt;
    incomingUser.downloadsUpdatedAt = incomingDownloadsUpdatedAt || incomingClearAt;
    incomingUser.downloadsData = [];
    incomingUser.downloads = 0;
    return incomingUser;
  }

  if (hasIncomingDownloadsData && Array.isArray(incomingUser.downloadsData)) {
    const incomingList = normalizeDownloadHistoryRecordsServer(incomingUser.downloadsData).history;
    const currentList = normalizeDownloadHistoryRecordsServer(Array.isArray(currentUser.downloadsData) ? currentUser.downloadsData : []).history;
    incomingUser.downloadsData = incomingList;
    currentUser.downloadsData = currentList;
    const currentHasItems = currentList.length > 0;
    const incomingHasItems = incomingList.length > 0;
    const acceptIncoming = !!(
      (incomingDownloadsUpdatedAt && (!currentDownloadsUpdatedAt || incomingDownloadsUpdatedAt >= currentDownloadsUpdatedAt)) ||
      (!currentDownloadsUpdatedAt && !currentHasItems && incomingHasItems) ||
      (!currentDownloadsUpdatedAt && !incomingDownloadsUpdatedAt && !currentHasItems)
    );

    if (acceptIncoming) {
      incomingUser.downloads = incomingList.length;
      incomingUser.downloadsUpdatedAt = incomingDownloadsUpdatedAt || normalizeTimestampValue(incomingUser.profileUpdatedAt) || Date.now();
      if (incomingClearAt || currentClearAt) incomingUser.downloadsClearedAt = Math.max(incomingClearAt, currentClearAt);
      return incomingUser;
    }

    delete incomingUser.downloadsData;
    delete incomingUser.downloads;
    delete incomingUser.downloadsUpdatedAt;
  }

  if (currentClearAt > incomingClearAt && (hasIncomingDownloadsCount || hasIncomingDownloadsVersion)) {
    incomingUser.downloadsClearedAt = currentClearAt;
  }

  return incomingUser;
}

function normalizeMaintenanceState(data = {}) {
  const schedule = normalizeMaintenanceSchedule(data.schedule || {});
  const scheduled = getMaintenanceScheduleStatus(schedule);
  const now = Date.now();
  const rawSuppressedUntil = data.scheduleSuppressedUntil ? Date.parse(data.scheduleSuppressedUntil) : 0;
  const scheduleSuppressedUntil = rawSuppressedUntil && rawSuppressedUntil > now ? new Date(rawSuppressedUntil).toISOString() : null;
  const scheduleSuppressed = !!scheduleSuppressedUntil;
  const scheduledActive = scheduled.active && !scheduleSuppressed;
  const manualEnabled = data.manualEnabled === undefined ? !!data.enabled : !!data.manualEnabled;
  const enabled = manualEnabled || scheduledActive;

  return {
    enabled,
    manualEnabled,
    scheduledActive,
    activeUntil: scheduledActive ? scheduled.activeUntil : null,
    scheduleSuppressedUntil,
    message: normalizeText(data.message, DEFAULT_MAINTENANCE_MESSAGE) || DEFAULT_MAINTENANCE_MESSAGE,
    by: normalizeText(data.by, ""),
    at: data.at || (enabled ? new Date().toISOString() : null),
    schedule
  };
}

function normalizeChatControls(data = {}) {
  return {
    locked: !!data.locked,
    slowSeconds: Math.max(0, Math.min(600, parseInt(data.slowSeconds || 0, 10) || 0)),
    by: normalizeText(data.by, ""),
    at: data.at || new Date().toISOString()
  };
}

function getPublicUserData(username, user = {}, includeAdminFields = false) {
  const safe = {
    id: user.id || null,
    name: username,
    avatar: user.avatar || DEFAULT_AVATAR,
    isAdmin: isUserAdmin(username, user),
    role: getUserRole(username, user),
    level: user.level || 1,
    joined: user.joined || "2026",
    countryCode: getUserCountryCode(user),
    online: !!user.online,
    lastSeen: user.lastSeen || null,
    ps3Status: user.ps3Status || null,
    profileCardStyle: getUserProfileCardStyle(user),
    profileCardEffect: getUserProfileCardStyle(user),
    settingsData: getPublicProfileSettings(user),
    downloads: Array.isArray(user.downloadsData) ? user.downloadsData.length : (user.downloads || 0),
    wishlist: Array.isArray(user.wishlistData) ? user.wishlistData.length : (user.wishlist || 0),
    favorites: Array.isArray(user.favoritesData) ? user.favoritesData.length : (user.favorites || 0),
    trophies: user.trophies || 0,
    library: Array.isArray(user.libraryData) ? user.libraryData.length : (user.library || 0)
  };

  if (includeAdminFields) {
    safe.banned = isUserBanned(user);
    safe.banReason = user.banReason || "";
    safe.bannedAt = user.bannedAt || null;
    safe.bannedBy = user.bannedBy || "";
    safe.passwordResetAt = user.passwordResetAt || null;
  }

  return safe;
}


const COMPACT_PROFILE_SETTING_KEYS = new Set([
  'audio', 'ux', 'hapticFeedback', 'haptics', 'cardBlur', 'cardBlurEnabled', 'gameCardBlur',
  'chatSound', 'chatAutoTranslate', 'interfaceAutoTranslate', 'ps3Ip', 'companionPlugin',
  'fpsCounterPlugin', 'fpsCounter', 'consoleFanMode', 'consoleFanSpeed', 'consoleFanTarget',
  'performanceMode', 'performanceRsx', 'performanceVram', 'siteDisclaimerSkipToday',
  'settingsUpdatedAt', 'settingsSyncedAt', 'settingsVersion', 'themeColor', 'themeColorUpdatedAt',
  'themeUpdatedAt', 'themeColorSyncedAt', 'profileCardStyle', 'profileCardEffect', 'profileCardTheme',
  'profileCardStyleUpdatedAt', 'profileCardEffectUpdatedAt', 'profileCardThemeUpdatedAt',
  'countryCode', 'country_code', 'country'
]);

function buildCompactSettingsData(user = {}) {
  const source = user && user.settingsData && typeof user.settingsData === 'object' && !Array.isArray(user.settingsData) ? user.settingsData : {};
  const compact = {};
  Object.keys(source).forEach(key => {
    if (COMPACT_PROFILE_SETTING_KEYS.has(key)) compact[key] = source[key];
  });
  return { ...compact, ...getPublicProfileSettings(user) };
}

function buildCompactCountersData(value = {}) {
  const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  return {
    msgs: source.msgs || '0',
    imgs: source.imgs || '0',
    reactions: source.reactions || '0',
    trailers: source.trailers || '0',
    days: source.days || '[]',
    lastChatRead: source.lastChatRead || 0
  };
}

function buildCompactUserSummary(name, userData = {}) {
  const source = (userData && typeof userData === 'object' && !Array.isArray(userData)) ? userData : {};
  const downloads = Array.isArray(source.downloadsData) ? source.downloadsData.length : Number(source.downloads || 0);
  const wishlist = Array.isArray(source.wishlistData) ? source.wishlistData.length : Number(source.wishlist || 0);
  const favorites = Array.isArray(source.favoritesData) ? source.favoritesData.length : Number(source.favorites || 0);
  const library = Array.isArray(source.libraryData) ? source.libraryData.length : Number(source.library || 0);
  const friends = Array.isArray(source.friendsData) ? source.friendsData.length : Number(source.friends || 0);
  const recentlyVisitedData = normalizeRecentlyVisitedRecordsServer(source.recentlyVisitedData);
  const recentlyVisited = recentlyVisitedData.length;
  const trophies = source.trophiesData && typeof source.trophiesData === 'object' && !Array.isArray(source.trophiesData)
    ? countUnlockedTrophiesPayload(source.trophiesData)
    : Number(source.trophies || 0);

  return normalizeUserRecord(name, {
    name,
    id: source.id || null,
    avatar: source.avatar || DEFAULT_AVATAR,
    joined: source.joined || '2026',
    countryCode: getUserCountryCode(source),
    role: getUserRole(name, source),
    banned: isUserBanned(source),
    ...(source.banReason ? { banReason: source.banReason } : {}),
    ...(source.bannedAt ? { bannedAt: source.bannedAt } : {}),
    ...(source.bannedBy ? { bannedBy: source.bannedBy } : {}),
    online: source.online === true,
    lastSeen: source.lastSeen || null,
    ps3Status: source.ps3Status || null,
    level: Number(source.level || 1),
    xp: Number(source.xp || 0),
    downloads,
    wishlist,
    favorites,
    trophies,
    library,
    friends,
    recentlyVisited,
    recentlyVisitedData,
    recentlyVisitedUpdatedAt: normalizeTimestampValue(source.recentlyVisitedUpdatedAt),
    countersData: buildCompactCountersData(source.countersData),
    themeColor: normalizeThemeColorServer(source.themeColor || (source.settingsData && source.settingsData.themeColor) || '#0070cc'),
    themeColorUpdatedAt: getUserThemeColorUpdatedAt(source),
    profileCardStyle: getUserProfileCardStyle(source),
    profileCardEffect: getUserProfileCardStyle(source),
    profileCardStyleUpdatedAt: getUserProfileCardStyleUpdatedAt(source),
    settingsData: buildCompactSettingsData(source),
    profileUpdatedAt: normalizeTimestampValue(source.profileUpdatedAt),
    downloadsUpdatedAt: normalizeTimestampValue(source.downloadsUpdatedAt),
    downloadsClearedAt: normalizeTimestampValue(source.downloadsClearedAt),
    wishlistUpdatedAt: normalizeTimestampValue(source.wishlistUpdatedAt),
    favoritesUpdatedAt: normalizeTimestampValue(source.favoritesUpdatedAt),
    libraryUpdatedAt: normalizeTimestampValue(source.libraryUpdatedAt),
    friendsUpdatedAt: normalizeTimestampValue(source.friendsUpdatedAt),
    passwordResetAt: source.passwordResetAt || null,
    passwordResetBy: source.passwordResetBy || '',
    passwordResetRequired: source.passwordResetRequired === true,
    passwordResetExpiresAt: normalizeTimestampValue(source.passwordResetExpiresAt),
    passwordResetCompletedAt: source.passwordResetCompletedAt || null
  });
}

function compactCachedUser(name) {
  if (!name || !userDatabase[name]) return null;
  const current = userDatabase[name];
  const online = current.online === true;
  const id = current.id || null;
  const lastSeen = current.lastSeen || null;
  userDatabase[name] = {
    ...buildCompactUserSummary(name, current),
    online,
    id,
    lastSeen
  };
  fullUserCacheNames.delete(name);
  userCacheMeta[name] = Date.now();
  invalidateOnlineListCache('user-cache-compact');
  return userDatabase[name];
}

function invalidateTrendingCache() {
  trendingCache = null;
  trendingCacheAt = 0;
}

function invalidateGlobalTrophyStatsCache() {
  globalTrophyStatsCache = null;
  globalTrophyStatsCacheAt = 0;
}

function scheduleTrendingRefreshBroadcast(delayMs = 1500) {
  clearContentDownloadCountCache();
  if (trendingRefreshTimer) return;
  trendingRefreshTimer = setTimeout(async () => {
    trendingRefreshTimer = null;
    try {
      if (trendingBuildInFlight) await trendingBuildInFlight.catch(() => null);
      invalidateTrendingCache();
      await emitTrendingFromDb(null, { force: true });
    } catch (err) {
      console.error('[TRENDING SCHEDULED REFRESH ERROR]:', err);
    }
  }, Math.max(0, delayMs));
}

function scheduleTrophyStatsRefreshBroadcast(delayMs = 1500) {
  if (trophyStatsRefreshTimer) return;
  trophyStatsRefreshTimer = setTimeout(async () => {
    trophyStatsRefreshTimer = null;
    try {
      if (globalTrophyStatsBuildInFlight) await globalTrophyStatsBuildInFlight.catch(() => null);
      invalidateGlobalTrophyStatsCache();
      const stats = await getGlobalTrophyStats({ force: true });
      io.emit('global_trophy_stats', stats);
    } catch (err) {
      console.error('[TROPHY STATS SCHEDULED REFRESH ERROR]:', err);
    }
  }, Math.max(0, delayMs));
}

async function refreshSingleUserSummaryFromDb(name, options = {}) {
  const safeName = normalizeText(name, '');
  if (!safeName) return null;

  const userRes = await queryDbWithRetry(`
    SELECT
      name,
      data - ARRAY['downloadsData','libraryData','wishlistData','favoritesData','trophiesData','friendsData','passwordHash','password']::text[] AS data,
      CASE WHEN jsonb_typeof(data->'downloadsData') = 'array' THEN jsonb_array_length(data->'downloadsData') ELSE NULL END AS downloads_count,
      CASE WHEN jsonb_typeof(data->'wishlistData') = 'array' THEN jsonb_array_length(data->'wishlistData') ELSE NULL END AS wishlist_count,
      CASE WHEN jsonb_typeof(data->'favoritesData') = 'array' THEN jsonb_array_length(data->'favoritesData') ELSE NULL END AS favorites_count,
      CASE WHEN jsonb_typeof(data->'libraryData') = 'array' THEN jsonb_array_length(data->'libraryData') ELSE NULL END AS library_count,
      CASE WHEN jsonb_typeof(data->'friendsData') = 'array' THEN jsonb_array_length(data->'friendsData') ELSE NULL END AS friends_count
    FROM users
    WHERE name = $1
  `, [safeName], { attempts: 2, label: 'USER SUMMARY READ' });

  if (!userRes.rows.length) {
    delete userDatabase[safeName];
    delete userCacheMeta[safeName];
    fullUserCacheNames.delete(safeName);
    invalidateOnlineListCache('single-user-summary-missing');
    return null;
  }

  const row = userRes.rows[0];
  const summaryData = { ...(row.data || {}) };
  if (row.downloads_count !== null) summaryData.downloads = Number(row.downloads_count) || 0;
  if (row.wishlist_count !== null) summaryData.wishlist = Number(row.wishlist_count) || 0;
  if (row.favorites_count !== null) summaryData.favorites = Number(row.favorites_count) || 0;
  if (row.library_count !== null) summaryData.library = Number(row.library_count) || 0;
  if (row.friends_count !== null) summaryData.friends = Number(row.friends_count) || 0;

  const localUser = userDatabase[safeName] || {};
  const preserveOnline = options.preserveOnline !== false;
  userDatabase[safeName] = {
    ...buildCompactUserSummary(safeName, summaryData),
    online: preserveOnline ? localUser.online === true : false,
    id: preserveOnline ? (localUser.id || summaryData.id || null) : (summaryData.id || null),
    lastSeen: preserveOnline ? (localUser.lastSeen || summaryData.lastSeen || null) : (summaryData.lastSeen || null)
  };
  fullUserCacheNames.delete(safeName);
  userCacheMeta[safeName] = Date.now();
  invalidateOnlineListCache('single-user-summary-refresh');
  return userDatabase[safeName];
}


async function refreshAllUsersCacheFromDb(options = {}) {
  if (userCacheRefreshInFlight) return userCacheRefreshInFlight;

  const preserveOnline = options.preserveOnline !== false;
  userCacheRefreshInFlight = (async () => {
    const now = Date.now();
    const usersRes = await queryDbWithRetry(`
      SELECT
        name,
        data - ARRAY['downloadsData','libraryData','wishlistData','favoritesData','trophiesData','friendsData','passwordHash','password']::text[] AS data,
        CASE WHEN jsonb_typeof(data->'downloadsData') = 'array' THEN jsonb_array_length(data->'downloadsData') ELSE NULL END AS downloads_count,
        CASE WHEN jsonb_typeof(data->'wishlistData') = 'array' THEN jsonb_array_length(data->'wishlistData') ELSE NULL END AS wishlist_count,
        CASE WHEN jsonb_typeof(data->'favoritesData') = 'array' THEN jsonb_array_length(data->'favoritesData') ELSE NULL END AS favorites_count,
        CASE WHEN jsonb_typeof(data->'libraryData') = 'array' THEN jsonb_array_length(data->'libraryData') ELSE NULL END AS library_count,
        CASE WHEN jsonb_typeof(data->'friendsData') = 'array' THEN jsonb_array_length(data->'friendsData') ELSE NULL END AS friends_count
      FROM users
      ORDER BY LOWER(name) ASC
    `, [], { attempts: 3, label: 'USER CACHE STARTUP READ' });
    const nextDatabase = {};
    const nextMeta = {};
    const nextFullNames = new Set();

    usersRes.rows.forEach(row => {
      const username = row.name;
      const localUser = userDatabase[username] || {};

      if ((userProfileWriteInFlight.has(username) || fullUserCacheNames.has(username)) && userDatabase[username]) {
        nextDatabase[username] = normalizeUserRecord(username, localUser);
        nextMeta[username] = userCacheMeta[username] || now;
        nextFullNames.add(username);
        return;
      }

      const summaryData = { ...(row.data || {}) };
      if (row.downloads_count !== null) summaryData.downloads = Number(row.downloads_count) || 0;
      if (row.wishlist_count !== null) summaryData.wishlist = Number(row.wishlist_count) || 0;
      if (row.favorites_count !== null) summaryData.favorites = Number(row.favorites_count) || 0;
      if (row.library_count !== null) summaryData.library = Number(row.library_count) || 0;
      if (row.friends_count !== null) summaryData.friends = Number(row.friends_count) || 0;

      nextDatabase[username] = {
        ...buildCompactUserSummary(username, summaryData),
        online: preserveOnline ? localUser.online === true : false,
        id: preserveOnline ? (localUser.id || summaryData.id || null) : (summaryData.id || null),
        lastSeen: preserveOnline ? (localUser.lastSeen || summaryData.lastSeen || null) : (summaryData.lastSeen || null)
      };
      nextMeta[username] = now;
    });

    userDatabase = nextDatabase;
    userCacheMeta = nextMeta;
    fullUserCacheNames.clear();
    nextFullNames.forEach(name => fullUserCacheNames.add(name));
    userCacheLastFullRefresh = now;
    await syncPresenceOnlineFromDb();
    invalidateOnlineListCache('users-summary-refresh');
    console.log(`[USER CACHE] ${Object.keys(userDatabase).length} compact user summaries loaded into RAM on ${INSTANCE_ID}. Full profile payloads load only for active/targeted users.`);
    if (MEMORY_TRACE_ENABLED) {
      let approxBytes = 0;
      let largestName = '';
      let largestBytes = 0;
      Object.entries(userDatabase).forEach(([cacheName, cacheUser]) => {
        const estimate = estimateValueBytes(cacheUser, { maxNodes: 5000, maxBytes: 2 * 1024 * 1024 });
        approxBytes += estimate.bytes;
        if (estimate.bytes > largestBytes) { largestBytes = estimate.bytes; largestName = cacheName; }
      });
      logMemoryTrace('user-cache:ready', `users=${Object.keys(userDatabase).length} approx=${formatApproxBytes(approxBytes)} largest=${largestName || '-'}:${formatApproxBytes(largestBytes)}`);
    }
    return userDatabase;
  })();

  try {
    return await userCacheRefreshInFlight;
  } finally {
    userCacheRefreshInFlight = null;
  }
}

async function refreshSingleUserCacheFromDb(name, options = {}) {
  const safeName = normalizeText(name, "");
  if (!safeName) return null;
  if (userProfileWriteInFlight.has(safeName) && options.forceDuringWrite !== true) return userDatabase[safeName] || null;

  const userRes = await queryDbWithRetry('SELECT data FROM users WHERE name = $1', [safeName], { attempts: 3, label: 'USER PROFILE READ' });
  if (!userRes.rows.length) {
    delete userDatabase[safeName];
    delete userCacheMeta[safeName];
    invalidateOnlineListCache("single-user-missing");
    return null;
  }

  const dbUser = normalizeUserRecord(safeName, userRes.rows[0].data || {});
  const localUser = userDatabase[safeName] || {};
  const preserveOnline = options.preserveOnline !== false;

  userDatabase[safeName] = {
    ...dbUser,
    online: preserveOnline ? localUser.online === true : false,
    id: preserveOnline ? (localUser.id || dbUser.id || null) : (dbUser.id || null),
    lastSeen: preserveOnline ? (localUser.lastSeen || dbUser.lastSeen || null) : (dbUser.lastSeen || null)
  };
  userCacheMeta[safeName] = Date.now();
  fullUserCacheNames.add(safeName);
  invalidateOnlineListCache("single-user-refresh");
  return userDatabase[safeName];
}

async function ensureUserCacheReady() {
  if (!Object.keys(userDatabase).length) {
    await refreshAllUsersCacheFromDb();
  }
  return userDatabase;
}

function startUserCacheWarmup() {
  if (process.env.ENABLE_USER_CACHE_WARMUP !== "1") {
    console.log('[USER CACHE] background full refresh disabled; using startup RAM cache + targeted refresh only.');
    return;
  }

  setInterval(() => {
    refreshAllUsersCacheFromDb()
      .catch(err => console.error('[USER CACHE REFRESH ERROR]:', err));
  }, USER_CACHE_REFRESH_INTERVAL_MS);

  setInterval(() => {
    const age = Date.now() - userCacheLastFullRefresh;
    if (!userCacheLastFullRefresh || age > USER_CACHE_WARMUP_INTERVAL_MS) {
      refreshAllUsersCacheFromDb()
        .catch(err => console.error('[USER CACHE WARMUP ERROR]:', err));
    }
  }, USER_CACHE_WARMUP_INTERVAL_MS);
}

function getEmptyUserDataPayload(type) {
  return (type === 'trophies') ? {} : [];
}

function getUserDataPayloadFromCache(targetName, type) {
  const safeTargetName = normalizeText(targetName, "");
  if (!safeTargetName || !userDatabase[safeTargetName]) return null;

  const targetUser = userDatabase[safeTargetName];
  const keyMap = {
    favs: 'favoritesData',
    favorites: 'favoritesData',
    wishlist: 'wishlistData',
    downloads: 'downloadsData',
    library: 'libraryData',
    trophies: 'trophiesData'
  };
  const dataKey = keyMap[type] || `${type}Data`;
  const payload = targetUser[dataKey] || (dataKey === 'trophiesData' ? {} : []);
  if (dataKey === 'downloadsData') return normalizeDownloadHistoryRecordsServer(payload).history;
  if (dataKey === 'libraryData') return mergeLibraryRecordsServer(payload, []);
  if (dataKey === 'recentlyVisitedData') return normalizeRecentlyVisitedRecordsServer(payload);
  return payload;
}

function searchUserNamesFromCache(query) {
  const searchTerm = normalizeText(query, '').toLowerCase();
  const isAllCommand = (searchTerm === '@all' || searchTerm === '*');
  if (!isAllCommand && searchTerm.length < 2) return [];
  return Object.keys(userDatabase)
    .filter(username => isAllCommand || username.toLowerCase().includes(searchTerm))
    .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
    .map(name => name);
}

function searchUsersFromCache(query, includeAdminFields = false, includeAllMatches = false) {
  const searchTerm = normalizeText(query, "").toLowerCase();
  const isAllCommand = (searchTerm === '@all' || searchTerm === '*');
  if (!isAllCommand && searchTerm.length < 2) return [];

  const matches = Object.keys(userDatabase)
    .filter(username => isAllCommand || username.toLowerCase().includes(searchTerm))
    .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
  const visibleMatches = (isAllCommand || includeAllMatches) ? matches : matches.slice(0, 15);
  return visibleMatches.map(username => getPublicUserData(username, userDatabase[username], includeAdminFields));
}

async function calculateTrendingFromDb() {
  const trendingStartedAt = Date.now();
  logMemoryTrace('trending:build:start', `cacheUsers=${Object.keys(userDatabase).length}`);
  const rows = await queryDbWithRetry(`
    WITH game_downloads AS (
      SELECT
        u.name AS username,
        CASE WHEN upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) IN ('MISSING','N/A','NONE','NULL','UNDEFINED') THEN '' ELSE upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) END AS title_id,
        regexp_replace(lower(trim(COALESCE(item->>'category', item->>'rawCategory', 'games'))), '[[:space:]-]+', '_', 'g') AS raw_category
      FROM users u
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(u.data->'downloadsData') = 'array' THEN u.data->'downloadsData' ELSE '[]'::jsonb END
      ) item
    ), wishlist_items AS (
      SELECT
        u.name AS username,
        CASE WHEN upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) IN ('MISSING','N/A','NONE','NULL','UNDEFINED') THEN '' ELSE upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) END AS title_id
      FROM users u
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(u.data->'wishlistData') = 'array' THEN u.data->'wishlistData' ELSE '[]'::jsonb END
      ) item
    ), top_games AS (
      SELECT title_id AS key, COUNT(DISTINCT username)::int AS count
      FROM game_downloads
      WHERE raw_category IN ('game','games') AND title_id <> ''
      GROUP BY title_id
      ORDER BY count DESC, title_id ASC
      LIMIT 50
    ), top_wishlist AS (
      SELECT title_id AS key, COUNT(*)::int AS count
      FROM wishlist_items
      WHERE title_id <> ''
      GROUP BY title_id
      ORDER BY count DESC, title_id ASC
      LIMIT 50
    )
    SELECT 'game' AS kind, key, count FROM top_games
    UNION ALL
    SELECT 'wishlist' AS kind, key, count FROM top_wishlist
  `, [], { attempts: 2, label: 'TRENDING AGGREGATE' });
  logMemoryTrace('trending:query:done', `rows=${rows && Array.isArray(rows.rows) ? rows.rows.length : 0} ms=${Date.now() - trendingStartedAt}`);

  const topDownloads = [];
  const topWishlist = [];
  rows.rows.forEach(row => {
    const item = { id: row.key, count: Number(row.count) || 0 };
    if (row.kind === 'game') topDownloads.push(item);
    else if (row.kind === 'wishlist') topWishlist.push(item);
  });

  const result = { topDownloads, topWishlist };
  logMemoryTrace('trending:build:done', `downloads=${topDownloads.length} wishlist=${topWishlist.length} ms=${Date.now() - trendingStartedAt}`);
  return result;
}

const CONTENT_DOWNLOAD_COUNT_CACHE_TTL_MS = Math.max(30000, parseInt(process.env.CONTENT_DOWNLOAD_COUNT_CACHE_TTL_MS || '300000', 10) || 300000);
const CONTENT_DOWNLOAD_COUNT_CACHE_MAX = Math.max(250, parseInt(process.env.CONTENT_DOWNLOAD_COUNT_CACHE_MAX || '4000', 10) || 4000);
const CONTENT_DOWNLOAD_COUNT_REQUEST_MAX = Math.max(25, Math.min(500, parseInt(process.env.CONTENT_DOWNLOAD_COUNT_REQUEST_MAX || '200', 10) || 200));
const contentDownloadCountCache = new Map();
let contentDownloadCountQueryQueue = Promise.resolve();

function runSerializedContentDownloadCountQuery(task) {
  const run = contentDownloadCountQueryQueue.catch(() => null).then(task);
  contentDownloadCountQueryQueue = run.then(() => null, () => null);
  return run;
}

function clearContentDownloadCountCache() {
  contentDownloadCountCache.clear();
}

function setCachedContentDownloadCount(key, count) {
  if (!key) return;
  if (contentDownloadCountCache.has(key)) contentDownloadCountCache.delete(key);
  contentDownloadCountCache.set(key, { count: Math.max(0, Number(count) || 0), at: Date.now() });
  while (contentDownloadCountCache.size > CONTENT_DOWNLOAD_COUNT_CACHE_MAX) {
    const oldestKey = contentDownloadCountCache.keys().next().value;
    if (oldestKey === undefined) break;
    contentDownloadCountCache.delete(oldestKey);
  }
}

async function getContentDownloadCountsForKeys(rawKeys = []) {
  const keys = [...new Set((Array.isArray(rawKeys) ? rawKeys : [])
    .map(value => String(value || '').trim())
    .filter(value => value && value.length <= 512))]
    .slice(0, CONTENT_DOWNLOAD_COUNT_REQUEST_MAX);
  if (!keys.length) return {};

  const now = Date.now();
  const counts = {};
  const missing = [];
  keys.forEach(key => {
    const cached = contentDownloadCountCache.get(key);
    if (cached && now - cached.at < CONTENT_DOWNLOAD_COUNT_CACHE_TTL_MS) {
      counts[key] = cached.count;
      contentDownloadCountCache.delete(key);
      contentDownloadCountCache.set(key, cached);
    } else {
      if (cached) contentDownloadCountCache.delete(key);
      missing.push(key);
    }
  });

  if (missing.length) {
    await runSerializedContentDownloadCountQuery(async () => {
      const queryKeys = [];
      const recheckNow = Date.now();
      missing.forEach(key => {
        const cached = contentDownloadCountCache.get(key);
        if (cached && recheckNow - cached.at < CONTENT_DOWNLOAD_COUNT_CACHE_TTL_MS) {
          counts[key] = cached.count;
          contentDownloadCountCache.delete(key);
          contentDownloadCountCache.set(key, cached);
        } else {
          if (cached) contentDownloadCountCache.delete(key);
          queryKeys.push(key);
        }
      });
      if (!queryKeys.length) return;

      const rows = await queryDbWithRetry(`
        WITH download_items AS (
          SELECT
            u.name AS username,
            CASE raw_category
              WHEN 'game' THEN 'games'
              WHEN 'app' THEN 'apps'
              WHEN 'demo' THEN 'demos'
              WHEN 'dlc' THEN 'dlcs'
              WHEN 'update' THEN 'updates'
              WHEN 'avatar' THEN 'avatars'
              WHEN 'theme' THEN 'themes'
              WHEN 'homebrew' THEN 'homebrew_games'
              WHEN 'port' THEN 'ports'
              WHEN 'prototype' THEN 'prototypes'
              WHEN 'emulator' THEN 'emulators'
              WHEN 'launcher' THEN 'launchers'
              WHEN 'tool' THEN 'tools'
              WHEN 'dev_tool' THEN 'dev_tools'
              WHEN 'manager' THEN 'backup_manager'
              ELSE COALESCE(NULLIF(raw_category, ''), 'games')
            END AS category,
            CASE WHEN upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) IN ('MISSING','N/A','NONE','NULL','UNDEFINED') THEN '' ELSE upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) END AS title_id,
            CASE WHEN upper(trim(COALESCE(item->>'contentId', item->>'contentID', ''))) IN ('MISSING','N/A','NONE','NULL','UNDEFINED') THEN '' ELSE upper(trim(COALESCE(item->>'contentId', item->>'contentID', ''))) END AS content_id,
            regexp_replace(lower(replace(trim(COALESCE(item->>'cleanName', item->>'name', item->>'title', item->>'rawName', '')), '&amp;', '&')), '[^a-z0-9]+', '', 'g') AS normalized_name
          FROM users u
          CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(u.data->'downloadsData') = 'array' THEN u.data->'downloadsData' ELSE '[]'::jsonb END
          ) item
          CROSS JOIN LATERAL (
            SELECT regexp_replace(lower(trim(COALESCE(item->>'category', item->>'rawCategory', 'games'))), '[[:space:]-]+', '_', 'g') AS raw_category
          ) cat
        ), keyed_downloads AS (
          SELECT
            username,
            CASE
              WHEN category = 'games' AND title_id <> '' THEN category || '|T:' || title_id
              WHEN content_id <> '' THEN category || '|C:' || content_id
              WHEN title_id <> '' AND normalized_name <> '' THEN category || '|T:' || title_id || '|N:' || normalized_name
              WHEN title_id <> '' THEN category || '|T:' || title_id
              WHEN normalized_name <> '' THEN category || '|N:' || normalized_name
              ELSE ''
            END AS content_key
          FROM download_items
        )
        SELECT content_key AS key, COUNT(DISTINCT username)::int AS count
        FROM keyed_downloads
        WHERE content_key = ANY($1::text[])
        GROUP BY content_key
      `, [queryKeys], { attempts: 2, label: 'CONTENT DOWNLOAD COUNTS' });

      const found = new Map(rows.rows.map(row => [String(row.key || ''), Math.max(0, Number(row.count) || 0)]));
      queryKeys.forEach(key => {
        const count = found.get(key) || 0;
        counts[key] = count;
        setCachedContentDownloadCount(key, count);
      });
    });
  }
  return counts;
}

async function getTrendingActivity(options = {}) {
  const force = options.force === true;
  const now = Date.now();
  if (!force && trendingCache && now - trendingCacheAt < TRENDING_CACHE_MS) return trendingCache;
  if (trendingBuildInFlight) return trendingBuildInFlight;

  trendingBuildInFlight = calculateTrendingFromDb()
    .then(payload => {
      trendingCache = payload;
      trendingCacheAt = Date.now();
      return payload;
    })
    .finally(() => { trendingBuildInFlight = null; });
  return trendingBuildInFlight;
}

function buildContentDownloadCountsPayload(counts = {}, options = {}) {
  return {
    success: true,
    counts,
    updatedAt: Date.now(),
    uniqueUsers: true,
    partial: options.partial === true,
    source: options.partial === true ? 'database-targeted' : 'database-aggregate'
  };
}

function buildTrendingViewPayload(payload = {}) {
  return {
    topDownloads: Array.isArray(payload.topDownloads) ? payload.topDownloads : [],
    topWishlist: Array.isArray(payload.topWishlist) ? payload.topWishlist : [],
    ...(payload.stale === true ? { stale: true } : {}),
    ...(payload.unavailable === true ? { unavailable: true } : {})
  };
}

async function emitTrendingFromDb(targetSocket = null, options = {}) {
  try {
    const payload = await getTrendingActivity(options);
    const trendingViewPayload = buildTrendingViewPayload(payload);
    if (MEMORY_TRACE_ENABLED) {
      const estimate = estimateValueBytes(trendingViewPayload, { maxNodes: 20000, maxBytes: 8 * 1024 * 1024 });
      logMemoryTrace('trending:send', `target=${targetSocket && targetSocket.userName ? targetSocket.userName : 'broadcast'} items=${(trendingViewPayload.topDownloads || []).length + (trendingViewPayload.topWishlist || []).length} approx=${formatApproxBytes(estimate.bytes)}${estimate.truncated ? '+' : ''}`);
    }
    if (targetSocket && targetSocket.connected) targetSocket.emit('trending_data', trendingViewPayload);
    else io.emit('trending_data', trendingViewPayload);
    return payload;
  } catch (err) {
    console.error('[TRENDING DB EMIT ERROR]:', err);
    const fallbackPayload = trendingCache
      ? { ...trendingCache, stale: true, unavailable: false }
      : { topDownloads: [], topWishlist: [], stale: false, unavailable: true };
    if (targetSocket && targetSocket.connected) targetSocket.emit('trending_data', buildTrendingViewPayload(fallbackPayload));
    return fallbackPayload;
  }
}

async function getGlobalTrophyStats(options = {}) {
  const force = options.force === true;
  const now = Date.now();
  if (!force && globalTrophyStatsCache && now - globalTrophyStatsCacheAt < TROPHY_STATS_CACHE_MS) return globalTrophyStatsCache;
  if (globalTrophyStatsBuildInFlight) return globalTrophyStatsBuildInFlight;

  globalTrophyStatsBuildInFlight = calculateGlobalTrophyStatsFromDb()
    .then(stats => {
      globalTrophyStatsCache = stats;
      globalTrophyStatsCacheAt = Date.now();
      return stats;
    })
    .finally(() => { globalTrophyStatsBuildInFlight = null; });
  return globalTrophyStatsBuildInFlight;
}

function profileUpdateTouchesTrending(userData = {}) {
  return !!(userData && (
    Object.prototype.hasOwnProperty.call(userData, 'downloadsData') ||
    Object.prototype.hasOwnProperty.call(userData, 'downloads') ||
    Object.prototype.hasOwnProperty.call(userData, 'downloadsClearedAt') ||
    Object.prototype.hasOwnProperty.call(userData, 'downloadsUpdatedAt') ||
    Object.prototype.hasOwnProperty.call(userData, 'wishlistData') ||
    Object.prototype.hasOwnProperty.call(userData, 'wishlist') ||
    Object.prototype.hasOwnProperty.call(userData, 'wishlistUpdatedAt')
  ));
}

function withTimeout(promise, ms, fallbackValue) {
  let timeoutId = null;
  const timeoutPromise = new Promise(resolve => {
    timeoutId = setTimeout(() => resolve(fallbackValue), ms);
  });
  return Promise.race([
    Promise.resolve(promise).finally(() => {
      if (timeoutId) clearTimeout(timeoutId);
    }),
    timeoutPromise
  ]);
}


async function getUserFromDb(name) {
  return await refreshSingleUserCacheFromDb(name, { forceDuringWrite: true });
}

async function refreshReportsFromDb() {
  const reportsRes = await pool.query('SELECT data FROM reports WHERE resolved = false ORDER BY created_at DESC LIMIT 100');
  adminReports = reportsRes.rows.map(r => r.data);
  return adminReports;
}

async function searchUsersFromDb(query, includeAdminFields = false, includeAllMatches = false) {
  await ensureUserCacheReady();
  return searchUsersFromCache(query, includeAdminFields, includeAllMatches);
}

async function getUserDataPayloadFromDb(targetName, type) {
  const safeTargetName = normalizeText(targetName, '');
  if (!safeTargetName) return null;

  const keyMap = {
    favs: 'favoritesData',
    favorites: 'favoritesData',
    wishlist: 'wishlistData',
    downloads: 'downloadsData',
    library: 'libraryData',
    trophies: 'trophiesData'
  };
  const dataKey = keyMap[type] || `${type}Data`;
  const result = await queryDbWithRetry('SELECT data -> $2 AS payload FROM users WHERE name = $1', [safeTargetName, dataKey], { attempts: 3, label: 'USER DATA READ' });
  if (!result.rows.length) return null;

  let payload = result.rows[0].payload;
  if (dataKey === 'trophiesData') {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) payload = {};
    return payload;
  }
  if (!Array.isArray(payload)) payload = [];
  if (dataKey === 'downloadsData') return normalizeDownloadHistoryRecordsServer(payload).history;
  if (dataKey === 'libraryData') return mergeLibraryRecordsServer(payload, []);
  if (dataKey === 'recentlyVisitedData') return normalizeRecentlyVisitedRecordsServer(payload);
  return payload;
}


async function getAuthUserRecordFromDb(name) {
  const safeName = normalizeText(name, '');
  if (!safeName) return null;

  const result = await queryDbWithRetry(`
    SELECT
      data - ARRAY['downloadsData','libraryData','wishlistData','favoritesData','trophiesData','friendsData']::text[] AS data,
      CASE WHEN jsonb_typeof(data->'downloadsData') = 'array' THEN jsonb_array_length(data->'downloadsData') ELSE NULL END AS downloads_count,
      CASE WHEN jsonb_typeof(data->'wishlistData') = 'array' THEN jsonb_array_length(data->'wishlistData') ELSE NULL END AS wishlist_count,
      CASE WHEN jsonb_typeof(data->'favoritesData') = 'array' THEN jsonb_array_length(data->'favoritesData') ELSE NULL END AS favorites_count,
      CASE WHEN jsonb_typeof(data->'libraryData') = 'array' THEN jsonb_array_length(data->'libraryData') ELSE NULL END AS library_count,
      CASE WHEN jsonb_typeof(data->'friendsData') = 'array' THEN jsonb_array_length(data->'friendsData') ELSE NULL END AS friends_count
    FROM users
    WHERE name = $1
  `, [safeName], { attempts: 3, label: 'AUTH USER LOOKUP' });

  if (!result.rows.length) return null;
  const row = result.rows[0];
  const data = { ...(row.data || {}) };
  if (row.downloads_count !== null) data.downloads = Number(row.downloads_count) || 0;
  if (row.wishlist_count !== null) data.wishlist = Number(row.wishlist_count) || 0;
  if (row.favorites_count !== null) data.favorites = Number(row.favorites_count) || 0;
  if (row.library_count !== null) data.library = Number(row.library_count) || 0;
  if (row.friends_count !== null) data.friends = Number(row.friends_count) || 0;

  const compact = buildCompactUserSummary(safeName, data);
  ['passwordHash', 'password', 'passwordMigratedAt'].forEach(key => {
    if (Object.prototype.hasOwnProperty.call(data, key)) compact[key] = data[key];
  });
  return compact;
}

function runSerializedProfileHydration(task) {
  profileHydrationQueued += 1;
  const lane = profileHydrationNextLane++ % profileHydrationQueues.length;
  const run = profileHydrationQueues[lane].catch(() => null).then(async () => {
    profileHydrationQueued = Math.max(0, profileHydrationQueued - 1);
    profileHydrationActive += 1;
    try {
      return await task();
    } finally {
      profileHydrationActive = Math.max(0, profileHydrationActive - 1);
    }
  });
  profileHydrationQueues[lane] = run.then(() => null, () => null);
  return run;
}

async function loadFullUserRecordTransient(name) {
  const safeName = normalizeText(name, '');
  if (!safeName) return null;
  const result = await queryDbWithRetry('SELECT data FROM users WHERE name = $1', [safeName], { attempts: 3, label: 'FULL PROFILE TRANSIENT READ' });
  if (!result.rows.length) return null;
  return normalizeUserRecord(safeName, result.rows[0].data || {});
}

async function patchUserDataInternal(name, patch = {}, deleteKeys = [], label = 'INTERNAL USER PATCH') {
  if (!name || !patch || typeof patch !== 'object' || Array.isArray(patch)) return false;
  const outgoing = { ...patch };
  const removals = Array.isArray(deleteKeys) ? deleteKeys.filter(Boolean) : [];
  const result = await queryDbWithRetry(`
    UPDATE users
    SET data = (COALESCE(data, '{}'::jsonb) - $3::text[]) || $1::jsonb
    WHERE name = $2
    RETURNING name
  `, [outgoing, name, removals], { attempts: 2, label });
  return result.rows.length > 0;
}

const PROFILE_HEAVY_SECTION_META = {
  trophiesData: { type: 'trophies', countKey: 'trophies', versionKeys: [] },
  downloadsData: { type: 'downloads', countKey: 'downloads', versionKeys: ['downloadsUpdatedAt', 'downloadsClearedAt'] },
  wishlistData: { type: 'wishlist', countKey: 'wishlist', versionKeys: ['wishlistUpdatedAt'] },
  favoritesData: { type: 'favorites', countKey: 'favorites', versionKeys: ['favoritesUpdatedAt'] },
  libraryData: { type: 'library', countKey: 'library', versionKeys: ['libraryUpdatedAt'] },
  friendsData: { type: 'friends', countKey: 'friends', versionKeys: ['friendsUpdatedAt'] }
};

function incomingTouchesHeavyProfileSection(incoming = {}, dataKey, meta = {}) {
  if (!incoming || typeof incoming !== 'object') return false;
  if (Object.prototype.hasOwnProperty.call(incoming, dataKey)) return true;
  if (meta.countKey && Object.prototype.hasOwnProperty.call(incoming, meta.countKey)) return true;
  return (meta.versionKeys || []).some(key => Object.prototype.hasOwnProperty.call(incoming, key));
}

async function buildWorkingUserForProfileUpdate(name, incoming = {}) {
  const base = { ...(userDatabase[name] || {}) };
  for (const [dataKey, meta] of Object.entries(PROFILE_HEAVY_SECTION_META)) {
    if (!incomingTouchesHeavyProfileSection(incoming, dataKey, meta)) continue;
    const payload = await getUserDataPayloadFromDb(name, meta.type);
    if (payload !== null) base[dataKey] = payload;
  }
  return base;
}

function updateCompactUserCacheFromPatch(name, workingUser = {}, patch = {}) {
  if (!name || !userDatabase[name]) return;
  const compact = { ...userDatabase[name] };
  const heavyKeys = new Set(USER_HEAVY_CACHE_KEYS);

  Object.keys(patch || {}).forEach(key => {
    if (heavyKeys.has(key)) return;
    compact[key] = patch[key];
  });

  if (Object.prototype.hasOwnProperty.call(patch, 'downloadsData')) compact.downloads = Array.isArray(workingUser.downloadsData) ? workingUser.downloadsData.length : Number(workingUser.downloads || 0);
  if (Object.prototype.hasOwnProperty.call(patch, 'wishlistData')) compact.wishlist = Array.isArray(workingUser.wishlistData) ? workingUser.wishlistData.length : Number(workingUser.wishlist || 0);
  if (Object.prototype.hasOwnProperty.call(patch, 'favoritesData')) compact.favorites = Array.isArray(workingUser.favoritesData) ? workingUser.favoritesData.length : Number(workingUser.favorites || 0);
  if (Object.prototype.hasOwnProperty.call(patch, 'libraryData')) compact.library = Array.isArray(workingUser.libraryData) ? workingUser.libraryData.length : Number(workingUser.library || 0);
  if (Object.prototype.hasOwnProperty.call(patch, 'friendsData')) compact.friends = Array.isArray(workingUser.friendsData) ? workingUser.friendsData.length : Number(workingUser.friends || 0);
  if (Object.prototype.hasOwnProperty.call(patch, 'trophiesData')) compact.trophies = countUnlockedTrophiesPayload(workingUser.trophiesData || {});

  USER_HEAVY_CACHE_KEYS.forEach(key => delete compact[key]);
  userDatabase[name] = normalizeUserRecord(name, compact);
  fullUserCacheNames.delete(name);
  userCacheMeta[name] = Date.now();
}

function buildLightProfileUserData(name, user = {}) {
  return {
    _lightAuth: true,
    id: user.id || null,
    name,
    avatar: user.avatar || DEFAULT_AVATAR,
    joined: user.joined || '2026',
    countryCode: getUserCountryCode(user),
    role: getUserRole(name, user),
    isAdmin: isUserAdmin(name, user),
    isModerator: isUserModerator(name, user),
    banned: isUserBanned(user),
    lastSeen: user.lastSeen || null,
    profileUpdatedAt: normalizeTimestampValue(user.profileUpdatedAt),
    ps3Status: user.ps3Status || null,
    level: user.level || 1,
    xp: user.xp || 0,
    downloads: Number(user.downloads || 0),
    wishlist: Number(user.wishlist || 0),
    favorites: Number(user.favorites || 0),
    trophies: Number(user.trophies || 0),
    library: Number(user.library || 0),
    recentlyVisited: Array.isArray(user.recentlyVisitedData) ? user.recentlyVisitedData.length : Number(user.recentlyVisited || 0),
    recentlyVisitedData: normalizeRecentlyVisitedRecordsServer(user.recentlyVisitedData),
    recentlyVisitedUpdatedAt: normalizeTimestampValue(user.recentlyVisitedUpdatedAt),
    countersData: buildCompactCountersData(user.countersData),
    themeColor: normalizeThemeColorServer(user.themeColor || (user.settingsData && user.settingsData.themeColor) || '#0070cc'),
    themeColorUpdatedAt: getUserThemeColorUpdatedAt(user),
    settingsData: buildCompactSettingsData(user)
  };
}

async function emitProfileSyncPacket(socket, payload, options = {}) {
  const requireAck = socket && socket.profileSyncAckV1 === true;
  const timeoutMs = Math.max(3000, Number(options.timeoutMs || 15000));
  const label = String(options.label || 'profile');
  if (!socket || !socket.connected) return Promise.reject(new Error(`Socket disconnected before ${label}.`));

  if (!requireAck) {
    socket.emit('profile_sync', payload);
    const drain = await waitForSocketWriteBufferDrain(socket, { maxPending: 1, timeoutMs });
    return { acked: false, legacy: true, drained: drain.drained === true, elapsedMs: drain.elapsedMs || 0, pending: drain.pending || 0 };
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    const startedAt = Date.now();
    const finish = result => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { socket.off('disconnect', onDisconnect); } catch (e) {}
      resolve(result);
    };
    const onDisconnect = () => finish({ acked: false, cancelled: true, elapsedMs: Date.now() - startedAt });
    const timer = setTimeout(async () => {
      if (settled) return;
      if (!socket.connected) {
        finish({ acked: false, cancelled: true, ackTimeout: true, elapsedMs: Date.now() - startedAt });
        return;
      }
      const drain = await waitForSocketWriteBufferDrain(socket, { maxPending: 1, timeoutMs: 2500 });
      if (settled) return;
      finish({
        acked: false,
        cancelled: false,
        ackTimeout: true,
        drained: drain && drain.drained === true,
        elapsedMs: Date.now() - startedAt,
        pending: drain && Number(drain.pending || 0) || 0
      });
    }, timeoutMs);

    const done = response => {
      if (settled) return;
      if (response && response.ok === false) {
        settled = true;
        clearTimeout(timer);
        try { socket.off('disconnect', onDisconnect); } catch (e) {}
        reject(new Error(response.error || `Client rejected ${label}.`));
        return;
      }
      finish({ acked: true, cancelled: false, elapsedMs: Date.now() - startedAt });
    };

    try { socket.once('disconnect', onDisconnect); } catch (e) {}
    try {
      socket.emit('profile_sync', payload, done);
    } catch (err) {
      if (!settled) {
        clearTimeout(timer);
        settled = true;
        try { socket.off('disconnect', onDisconnect); } catch (e) {}
      }
      reject(err);
    }
  });
}

async function emitChunkedProfileSyncToSocket(socket, name, options = {}) {
  if (!socket || !socket.connected || !name) return;
  if (socket.__profileChunkSyncInFlight) return socket.__profileChunkSyncInFlight;

  profileSyncActiveSockets += 1;
  socket.__profileChunkSyncInFlight = (async () => {
    const syncStartedAt = Date.now();
    logMemoryTrace('profile-sync:start', `user=${name} socket=${socket.id} ack=${socket.profileSyncAckV1 === true}`);
    if (options.forceRefresh === true || !userDatabase[name]) await refreshSingleUserSummaryFromDb(name);
    const compactUser = userDatabase[name];
    if (!compactUser || !socket.connected) return;

    const requestedKeys = Array.isArray(options.changedKeys) ? new Set(options.changedKeys) : null;
    const profileUpdatedAt = normalizeTimestampValue(compactUser.profileUpdatedAt) || Date.now();
    const heavyKeys = new Set(Object.keys(PROFILE_HEAVY_SECTION_META));
    const shouldSendCore = !requestedKeys || Array.from(requestedKeys).some(key => !heavyKeys.has(key));

    if (shouldSendCore) {
      const corePayload = {
        name,
        sourceSocketId: options.sourceSocketId || null,
        profileUpdatedAt,
        userData: buildLightProfileUserData(name, compactUser)
      };
      const coreSize = estimateValueBytes(corePayload, { maxNodes: 10000, maxBytes: 4 * 1024 * 1024 });
      logMemoryTrace('profile-sync:core:send', `user=${name} socket=${socket.id} approx=${formatApproxBytes(coreSize.bytes)}${coreSize.truncated ? '+' : ''} buffer=${getSocketWriteBufferLength(socket)}`);
      const result = await emitProfileSyncPacket(socket, corePayload, { label: 'core', timeoutMs: 8000 });
      if (result && result.cancelled) { logMemoryTrace('profile-sync:cancel', `user=${name} socket=${socket.id} stage=core`); return; }
      if (result && result.ackTimeout) {
        logMemoryTrace('profile-sync:core:ack-timeout', `user=${name} socket=${socket.id} drained=${result.drained === true} ms=${result.elapsedMs || 0} pending=${result.pending || 0} buffer=${getSocketWriteBufferLength(socket)}`);
      } else {
        logMemoryTrace('profile-sync:core:ack', `user=${name} socket=${socket.id} ack=${result.acked === true} ms=${result.elapsedMs || 0} buffer=${getSocketWriteBufferLength(socket)}`);
      }
    }

    const sections = [
      ['trophiesData', 'trophies', null],
      ['downloadsData', 'downloads', 'downloadsUpdatedAt'],
      ['wishlistData', 'wishlist', 'wishlistUpdatedAt'],
      ['favoritesData', 'favorites', 'favoritesUpdatedAt'],
      ['libraryData', 'library', 'libraryUpdatedAt'],
      ['friendsData', 'friends', 'friendsUpdatedAt']
    ];

    for (const [dataKey, type, versionKey] of sections) {
      const meta = PROFILE_HEAVY_SECTION_META[dataKey];
      const sectionRequested = !requestedKeys || requestedKeys.has(dataKey) || requestedKeys.has(meta.countKey) || (meta.versionKeys || []).some(key => requestedKeys.has(key));
      if (!sectionRequested) continue;
      if (!socket.connected) { logMemoryTrace('profile-sync:cancel', `user=${name} socket=${socket.id} stage=${dataKey}`); return; }

      let sectionCancelled = false;
      await runSerializedProfileHydration(async () => {
        if (!socket.connected) { sectionCancelled = true; return; }
        logMemoryTrace('profile-sync:section:load', `user=${name} socket=${socket.id} section=${dataKey}`);
        const rawData = await getUserDataPayloadFromDb(name, type);
        if (rawData === null) throw new Error(`Could not load ${dataKey}.`);
        if (!socket.connected) { sectionCancelled = true; return; }
        const estimate = estimateValueBytes(rawData);
        const itemCount = getPayloadItemCount(rawData);
        logMemoryTrace('profile-sync:section:loaded', `user=${name} socket=${socket.id} section=${dataKey} items=${itemCount} approx=${formatApproxBytes(estimate.bytes)}${estimate.truncated ? '+' : ''}`);

        const userData = { profileUpdatedAt, [dataKey]: rawData };
        if (dataKey === 'trophiesData') userData.trophies = countUnlockedTrophiesPayload(rawData || {});
        else userData[meta.countKey] = Array.isArray(rawData) ? rawData.length : 0;
        if (versionKey) userData[versionKey] = normalizeTimestampValue(compactUser[versionKey]);
        if (dataKey === 'downloadsData') userData.downloadsClearedAt = normalizeTimestampValue(compactUser.downloadsClearedAt);

        const packet = { name, sourceSocketId: options.sourceSocketId || null, profileUpdatedAt, userData };
        logMemoryTrace('profile-sync:section:send', `user=${name} socket=${socket.id} section=${dataKey} items=${itemCount} approx=${formatApproxBytes(estimate.bytes)}${estimate.truncated ? '+' : ''} buffer=${getSocketWriteBufferLength(socket)}`);
        const result = await emitProfileSyncPacket(socket, packet, { label: dataKey, timeoutMs: 10000 });
        if (result && result.cancelled) { sectionCancelled = true; return; }
        if (result && result.ackTimeout) {
          logMemoryTrace('profile-sync:section:ack-timeout', `user=${name} socket=${socket.id} section=${dataKey} drained=${result.drained === true} ms=${result.elapsedMs || 0} pending=${result.pending || 0} buffer=${getSocketWriteBufferLength(socket)}`);
        } else {
          logMemoryTrace('profile-sync:section:ack', `user=${name} socket=${socket.id} section=${dataKey} ack=${result.acked === true} ms=${result.elapsedMs || 0} buffer=${getSocketWriteBufferLength(socket)}`);
        }

        if (dataKey === 'trophiesData' && typeof options.onCriticalReady === 'function') {
          const clientConfirmed = result && (result.acked === true || (result.legacy === true && result.drained === true));
          if (clientConfirmed) {
            try { options.onCriticalReady({ section: dataKey, profileUpdatedAt }); } catch (err) {}
          }
        }
      });
      if (sectionCancelled || !socket.connected) { logMemoryTrace('profile-sync:cancel', `user=${name} socket=${socket.id} stage=${dataKey}`); return; }
    }

    logMemoryTrace('profile-sync:done', `user=${name} socket=${socket.id} ms=${Date.now() - syncStartedAt}`);
  })().finally(() => {
    socket.__profileChunkSyncInFlight = null;
    profileSyncActiveSockets = Math.max(0, profileSyncActiveSockets - 1);
  });

  return socket.__profileChunkSyncInFlight;
}

function emitChatHistoryToSocket(socket) {
  if (!socket) return Promise.resolve();
  if (socket.__chatHistoryInFlight) return socket.__chatHistoryInFlight;

  const run = chatHistoryEmitQueue.catch(() => null).then(async () => {
    if (!socket.connected) return;
    const history = getPublicChatHistoryForUser(socket.userName || '');
    const requiresAck = socket.chatHistoryAckV1 === true;
    if (MEMORY_TRACE_ENABLED) {
      const estimate = estimateValueBytes(history, { maxNodes: 100000, maxBytes: 64 * 1024 * 1024 });
      logMemoryTrace('chat-history:send', `user=${socket.userName || '-'} socket=${socket.id} items=${Array.isArray(history) ? history.length : 0} approx=${formatApproxBytes(estimate.bytes)}${estimate.truncated ? '+' : ''} ack=${requiresAck} buffer=${getSocketWriteBufferLength(socket)}`);
    }

    const startedAt = Date.now();
    if (requiresAck) {
      const result = await new Promise((resolve, reject) => {
        let settled = false;
        const finish = value => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          try { socket.off('disconnect', onDisconnect); } catch (e) {}
          resolve(value);
        };
        const onDisconnect = () => finish({ cancelled: true });
        const timer = setTimeout(() => {
          if (settled) return;
          settled = true;
          try { socket.off('disconnect', onDisconnect); } catch (e) {}
          reject(new Error('Chat history ACK timeout.'));
        }, 20000);
        try { socket.once('disconnect', onDisconnect); } catch (e) {}
        try {
          socket.emit('chat_history', history, response => {
            if (settled) return;
            if (response && response.ok === false) {
              settled = true;
              clearTimeout(timer);
              try { socket.off('disconnect', onDisconnect); } catch (e) {}
              reject(new Error(response.error || 'Chat history rejected by client.'));
              return;
            }
            finish({ cancelled: false, response: response || null });
          });
        } catch (err) {
          if (!settled) {
            settled = true;
            clearTimeout(timer);
            try { socket.off('disconnect', onDisconnect); } catch (e) {}
          }
          reject(err);
        }
      });
      if (result && result.cancelled) {
        logMemoryTrace('chat-history:cancel', `user=${socket.userName || '-'} socket=${socket.id} ms=${Date.now() - startedAt}`);
        return;
      }
      const rendered = !(result && result.response && result.response.rendered === false);
      const received = result && result.response && Number.isFinite(Number(result.response.received)) ? Number(result.response.received) : -1;
      logMemoryTrace('chat-history:ack', `user=${socket.userName || '-'} socket=${socket.id} ms=${Date.now() - startedAt} rendered=${rendered} received=${received} buffer=${getSocketWriteBufferLength(socket)}`);
      return;
    }

    socket.emit('chat_history', history);
    const drain = await waitForSocketWriteBufferDrain(socket, { maxPending: 1, timeoutMs: 20000 });
    logMemoryTrace('chat-history:drain', `user=${socket.userName || '-'} socket=${socket.id} drained=${drain.drained === true} ms=${drain.elapsedMs || 0} buffer=${getSocketWriteBufferLength(socket)}`);
  });

  socket.__chatHistoryInFlight = run.finally(() => {
    socket.__chatHistoryInFlight = null;
  });
  chatHistoryEmitQueue = socket.__chatHistoryInFlight.then(() => null, () => null);
  return socket.__chatHistoryInFlight;
}

async function ensureFullUserCacheForWrite(name) {
  if (!name || !userDatabase[name]) return null;
  if (fullUserCacheNames.has(name)) return userDatabase[name];
  const loaded = await refreshSingleUserCacheFromDb(name, { forceDuringWrite: true });
  if (!loaded) return null;
  return userDatabase[name];
}

async function updateUserDataPreservingCredentials(name, userData, label = 'USER DATA SAVE') {
  const outgoing = normalizeUserRecord(name, userData || {});
  const outgoingHasPasswordHash = Object.prototype.hasOwnProperty.call(outgoing, 'passwordHash') && !!outgoing.passwordHash;
  const outgoingHasLegacyPassword = Object.prototype.hasOwnProperty.call(outgoing, 'password') && typeof outgoing.password === 'string' && outgoing.password.length > 0;
  const result = await queryDbWithRetry(`
    UPDATE users
    SET data = $1::jsonb
      || CASE WHEN NOT ($1::jsonb ? 'passwordHash') AND data ? 'passwordHash' THEN jsonb_build_object('passwordHash', data->'passwordHash') ELSE '{}'::jsonb END
      || CASE WHEN NOT ($1::jsonb ? 'password') AND data ? 'password' THEN jsonb_build_object('password', data->'password') ELSE '{}'::jsonb END
      || CASE WHEN NOT ($1::jsonb ? 'passwordMigratedAt') AND data ? 'passwordMigratedAt' THEN jsonb_build_object('passwordMigratedAt', data->'passwordMigratedAt') ELSE '{}'::jsonb END
      || CASE WHEN NOT ($1::jsonb ? 'passwordResetAt') AND data ? 'passwordResetAt' THEN jsonb_build_object('passwordResetAt', data->'passwordResetAt') ELSE '{}'::jsonb END
      || CASE WHEN NOT ($1::jsonb ? 'passwordResetBy') AND data ? 'passwordResetBy' THEN jsonb_build_object('passwordResetBy', data->'passwordResetBy') ELSE '{}'::jsonb END
      || CASE WHEN NOT ($1::jsonb ? 'passwordResetRequired') AND data ? 'passwordResetRequired' THEN jsonb_build_object('passwordResetRequired', data->'passwordResetRequired') ELSE '{}'::jsonb END
      || CASE WHEN NOT ($1::jsonb ? 'passwordResetExpiresAt') AND data ? 'passwordResetExpiresAt' THEN jsonb_build_object('passwordResetExpiresAt', data->'passwordResetExpiresAt') ELSE '{}'::jsonb END
      || CASE WHEN NOT ($1::jsonb ? 'passwordResetCompletedAt') AND data ? 'passwordResetCompletedAt' THEN jsonb_build_object('passwordResetCompletedAt', data->'passwordResetCompletedAt') ELSE '{}'::jsonb END
    WHERE name = $2
    RETURNING data
  `, [outgoing, name], { attempts: 2, label });

  if (!result.rows.length) return null;
  const saved = normalizeUserRecord(name, result.rows[0].data || {});
  if ((!outgoingHasPasswordHash && saved.passwordHash) || (!outgoingHasLegacyPassword && saved.password)) {
    console.warn(`[CREDENTIAL GUARD] ${label} preserved existing credentials for ${name}. Outgoing cache was missing credential fields.`);
  }
  userDatabase[name] = saved;
  userCacheMeta[name] = Date.now();
  fullUserCacheNames.add(name);
  return saved;
}

async function patchUserData(name, patch = {}, label = 'USER DATA PATCH') {
  if (!name || !patch || typeof patch !== 'object' || Array.isArray(patch)) return false;
  const outgoing = { ...patch };
  delete outgoing.passwordHash;
  delete outgoing.password;
  const keys = Object.keys(outgoing);
  if (!keys.length) return true;

  const result = await queryDbWithRetry(`
    UPDATE users
    SET data = COALESCE(data, '{}'::jsonb) || $1::jsonb
    WHERE name = $2
    RETURNING name
  `, [outgoing, name], { attempts: 2, label });

  if (!result.rows.length) return false;
  userCacheMeta[name] = Date.now();
  fullUserCacheNames.delete(name);
  return true;
}

async function saveUser(name, options = {}) {
  if (!name || !userDatabase[name]) return;
  const loaded = await ensureFullUserCacheForWrite(name);
  if (!loaded) return;
  userDatabase[name] = normalizeUserRecord(name, userDatabase[name]);
  userDatabase[name].profileUpdatedAt = Date.now();
  userCacheMeta[name] = Date.now();
  const saved = await updateUserDataPreservingCredentials(name, userDatabase[name], 'USER PROFILE SAVE');
  if (!saved) return;
  invalidateOnlineListCache('save-user');
  if (options.notify !== false) {
    await notifyProfileSyncAcrossInstances(name, options.sourceSocketId || null, userDatabase[name].profileUpdatedAt);
  }
  deferServerTask('USER CACHE COMPACT AFTER SAVE', () => compactCachedUser(name), 250);
}

async function saveAdminState(key, data) {
  await queryDbWithRetry(
    'INSERT INTO admin_state (state_key, data) VALUES ($1, $2) ON CONFLICT (state_key) DO UPDATE SET data = $2',
    [key, data],
    { attempts: 3, label: 'ADMIN STATE SAVE' }
  );
}

function cleanChatMessage(message = {}) {
  const clean = { ...(message || {}) };
  delete clean._dbId;
  return clean;
}

const pendingSeenMessageWrites = new Map();
let seenMessageFlushTimer = null;
let seenMessageFlushInFlight = false;
const SEEN_MESSAGE_FLUSH_DELAY_MS = 180;
const SEEN_MESSAGE_BATCH_SIZE = 150;

function getSeenMessageWriteKey(entry = {}) {
  const id = Number(entry.id || 0);
  if (id > 0) return `id:${id}`;
  const time = String(entry.time || '');
  return time ? `time:${time}` : '';
}

function scheduleSeenMessageFlush(delayMs = SEEN_MESSAGE_FLUSH_DELAY_MS) {
  if (seenMessageFlushTimer || seenMessageFlushInFlight || !pendingSeenMessageWrites.size) return;
  seenMessageFlushTimer = setTimeout(() => {
    seenMessageFlushTimer = null;
    flushPendingSeenMessageWrites().catch(err => console.error('[SEEN BATCH ERROR]:', err));
  }, Math.max(0, Number(delayMs) || 0));
}

function queueSeenMessagePersist(message = {}) {
  const entry = {
    id: Number(message && message._dbId) || 0,
    time: String(message && message.time || ''),
    message: cleanChatMessage(message)
  };
  const key = getSeenMessageWriteKey(entry);
  if (!key) return false;
  pendingSeenMessageWrites.set(key, entry);
  scheduleSeenMessageFlush(pendingSeenMessageWrites.size >= SEEN_MESSAGE_BATCH_SIZE ? 0 : SEEN_MESSAGE_FLUSH_DELAY_MS);
  return true;
}

async function flushPendingSeenMessageWrites() {
  if (seenMessageFlushInFlight || !pendingSeenMessageWrites.size) return;
  seenMessageFlushInFlight = true;

  const batchEntries = Array.from(pendingSeenMessageWrites.entries()).slice(0, SEEN_MESSAGE_BATCH_SIZE);
  batchEntries.forEach(([key]) => pendingSeenMessageWrites.delete(key));
  const batch = batchEntries.map(([, entry]) => entry);

  try {
    const byId = batch.filter(entry => entry.id > 0);
    const byTime = batch.filter(entry => entry.id <= 0 && entry.time);

    if (byId.length) {
      const payload = byId.map(entry => ({ id: entry.id, message: entry.message }));
      await queryDbWithRetry(`
        WITH updates AS (
          SELECT id, message
          FROM jsonb_to_recordset($1::jsonb) AS x(id bigint, message jsonb)
        )
        UPDATE chat AS c
        SET message = updates.message
        FROM updates
        WHERE c.id = updates.id
      `, [JSON.stringify(payload)], { attempts: 2, label: 'SEEN BATCH SAVE' });
    }

    if (byTime.length) {
      const payload = byTime.map(entry => ({ msg_time: entry.time, message: entry.message }));
      await queryDbWithRetry(`
        WITH updates AS (
          SELECT msg_time, message
          FROM jsonb_to_recordset($1::jsonb) AS x(msg_time text, message jsonb)
        )
        UPDATE chat AS c
        SET message = updates.message
        FROM updates
        WHERE c.message->>'time' = updates.msg_time
      `, [JSON.stringify(payload)], { attempts: 2, label: 'SEEN BATCH SAVE' });
    }
  } catch (err) {
    batch.forEach(entry => {
      const key = getSeenMessageWriteKey(entry);
      if (key && !pendingSeenMessageWrites.has(key)) pendingSeenMessageWrites.set(key, entry);
    });
    console.error(`[SEEN BATCH ERROR] Failed to persist ${batch.length} seen update(s):`, err);
  } finally {
    seenMessageFlushInFlight = false;
    if (pendingSeenMessageWrites.size) scheduleSeenMessageFlush(120);
  }
}

function attachChatDbId(message, dbId) {
  if (!message || typeof message !== 'object') return message;
  Object.defineProperty(message, '_dbId', {
    value: Number(dbId) || 0,
    writable: true,
    configurable: true,
    enumerable: false
  });
  return message;
}

function getPublicChatHistory() {
  return messageHistory;
}

function getPublicChatMessageForUser(message, userName = '') {
  const targetUser = String(userName || '');
  const clean = cleanChatMessage(message);
  if (!targetUser || String(clean.user || '') === targetUser) return clean;

  const seenBy = Array.isArray(clean.seenBy) ? clean.seenBy : [];
  const wasSeenByTarget = seenBy.includes(targetUser);
  clean.seenBy = wasSeenByTarget ? [targetUser] : [];

  const seenAt = clean.seenAt && typeof clean.seenAt === 'object' && !Array.isArray(clean.seenAt) ? clean.seenAt : {};
  clean.seenAt = wasSeenByTarget && seenAt[targetUser] ? { [targetUser]: seenAt[targetUser] } : {};
  return clean;
}

function getPublicChatHistoryForUser(userName = '') {
  return messageHistory.map(message => getPublicChatMessageForUser(message, userName));
}

function getChatSeenSnapshotForUser(userName = '') {
  const targetUser = String(userName || '');
  if (!targetUser) return [];
  const events = [];
  messageHistory.forEach(message => {
    if (!message || !message.time) return;
    const msgId = String(new Date(message.time).getTime());
    if (!msgId || msgId === 'NaN') return;
    const seenBy = Array.isArray(message.seenBy) ? message.seenBy : [];
    const seenAt = message.seenAt && typeof message.seenAt === 'object' && !Array.isArray(message.seenAt) ? message.seenAt : {};
    if (String(message.user || '') === targetUser) {
      seenBy.forEach(reader => {
        const name = String(reader || '');
        if (!name) return;
        events.push({ msgId, reader: name, seenAt: String(seenAt[name] || '') });
      });
      return;
    }
    if (seenBy.includes(targetUser)) events.push({ msgId, reader: targetUser, seenAt: String(seenAt[targetUser] || '') });
  });
  return events;
}

async function getChatSeenSyncForUser(userName = '', cursor = 0) {
  const targetUser = String(userName || '');
  const safeCursor = Math.max(0, Number(cursor) || 0);
  const maxRes = await queryDbWithRetry('SELECT COALESCE(MAX(id), 0)::bigint AS max_id FROM chat_seen_events', [], { attempts: 2, label: 'CHAT SEEN CURSOR READ' });
  const maxId = Math.max(0, Number(maxRes.rows[0]?.max_id) || 0);

  if (!safeCursor || safeCursor > maxId) {
    return { mode: 'snapshot', cursor: maxId, events: getChatSeenSnapshotForUser(targetUser) };
  }

  const res = await queryDbWithRetry(
    'SELECT id, message_id, sender, reader, seen_at FROM chat_seen_events WHERE id > $1 AND (sender = $2 OR reader = $2) ORDER BY id ASC LIMIT 2001',
    [safeCursor, targetUser],
    { attempts: 2, label: 'CHAT SEEN DELTA READ' }
  );
  if (res.rows.length > 2000) {
    return { mode: 'snapshot', cursor: maxId, events: getChatSeenSnapshotForUser(targetUser) };
  }
  return {
    mode: 'changes',
    cursor: maxId,
    events: res.rows.map(row => ({
      eventId: Math.max(0, Number(row.id) || 0),
      msgId: String(row.message_id || ''),
      reader: String(row.reader || ''),
      seenAt: row.seen_at ? new Date(row.seen_at).toISOString() : ''
    }))
  };
}

async function recordChatSeenEvent(msgId = '', sender = '', reader = '', seenAt = '') {
  const safeMsgId = String(msgId || '');
  const safeSender = String(sender || '');
  const safeReader = String(reader || '');
  if (!safeMsgId || !safeSender || !safeReader) return null;
  const result = await queryDbWithRetry(`
    INSERT INTO chat_seen_events (message_id, sender, reader, seen_at)
    VALUES ($1, $2, $3, COALESCE($4::timestamptz, NOW()))
    ON CONFLICT (message_id, reader) DO UPDATE SET seen_at = EXCLUDED.seen_at
    RETURNING id, message_id, sender, reader, seen_at
  `, [safeMsgId, safeSender, safeReader, seenAt || null], { attempts: 2, label: 'CHAT SEEN EVENT SAVE' });
  const row = result.rows[0];
  if (!row) return null;
  return {
    eventId: Math.max(0, Number(row.id) || 0),
    msgId: String(row.message_id || safeMsgId),
    sender: String(row.sender || safeSender),
    reader: String(row.reader || safeReader),
    seenAt: row.seen_at ? new Date(row.seen_at).toISOString() : String(seenAt || '')
  };
}

function emitChatSeenSyncChange(event) {
  if (!event || !event.eventId || !event.msgId || !event.reader) return;
  const payload = {
    seenSyncV1: true,
    cursor: event.eventId,
    msgId: event.msgId,
    reader: event.reader,
    seenAt: event.seenAt || ''
  };
  const recipients = new Set([String(event.sender || ''), String(event.reader || '')].filter(Boolean));
  io.sockets.sockets.forEach(client => {
    if (!client || !client.connected || !client.userName || !recipients.has(String(client.userName))) return;
    client.emit('chat_seen_sync_change', payload);
  });
}

async function refreshChatSyncStateFromDb() {
  const result = await queryDbWithRetry(
    'SELECT epoch, revision FROM chat_sync_state WHERE id = 1',
    [],
    { attempts: 2, label: 'CHAT SYNC STATE READ' }
  );
  if (result.rows.length) {
    chatSyncState = {
      epoch: String(result.rows[0].epoch || ''),
      revision: Math.max(0, Number(result.rows[0].revision) || 0)
    };
  }
  return chatSyncState;
}

async function recordChatSyncChange(changeType, messageId = '', message = null) {
  const type = String(changeType || '').toLowerCase();
  if (!['upsert', 'delete'].includes(type)) throw new Error(`Unsupported chat sync change: ${type}`);
  const cleanMessage = message && typeof message === 'object' ? cleanChatMessage(message) : null;
  const result = await queryDbWithRetry(`
    WITH next_state AS (
      UPDATE chat_sync_state
      SET revision = revision + 1, updated_at = NOW()
      WHERE id = 1
      RETURNING epoch, revision
    ), inserted AS (
      INSERT INTO chat_changes (revision, epoch, change_type, message_id, message)
      SELECT revision, epoch, $1, $2, $3::jsonb FROM next_state
      RETURNING revision, epoch, change_type, message_id, message
    )
    SELECT revision, epoch, change_type, message_id, message FROM inserted
  `, [type, String(messageId || ''), cleanMessage], { attempts: 2, label: 'CHAT SYNC CHANGE SAVE' });

  if (!result.rows.length) throw new Error('Chat sync state is unavailable.');
  const row = result.rows[0];
  const change = {
    epoch: String(row.epoch || ''),
    revision: Math.max(0, Number(row.revision) || 0),
    type: String(row.change_type || type),
    msgId: String(row.message_id || messageId || ''),
    message: row.message || cleanMessage || null
  };
  chatSyncState = { epoch: change.epoch, revision: change.revision };

  const pruneBefore = change.revision - CHAT_SYNC_CHANGE_LOG_MAX;
  if (pruneBefore > 0) {
    pool.query('DELETE FROM chat_changes WHERE epoch = $1 AND revision <= $2', [change.epoch, pruneBefore]).catch(err => {
      if (process.env.DEBUG_CHAT_SYNC === '1') console.warn('[CHAT SYNC PRUNE ERROR]:', err && err.message ? err.message : err);
    });
  }
  return change;
}

async function recordChatSyncChangeSafe(changeType, messageId = '', message = null) {
  try {
    return await recordChatSyncChange(changeType, messageId, message);
  } catch (err) {
    console.error('[CHAT SYNC CHANGE ERROR]:', err && err.message ? err.message : err);
    return null;
  }
}

function emitChatSyncChange(change) {
  if (!change || !change.epoch || !change.revision) return;
  io.sockets.sockets.forEach(client => {
    if (!client || !client.connected || !client.userName) return;
    const payload = {
      syncV1: true,
      epoch: change.epoch,
      revision: change.revision,
      type: change.type,
      msgId: change.msgId,
      message: change.message ? getPublicChatMessageForUser(change.message, client.userName) : null
    };
    client.emit('chat_sync_change', payload);
  });
}

async function resetChatSyncEpoch() {
  const nextEpoch = `${Date.now()}-${INSTANCE_ID}-${Math.random().toString(36).slice(2, 10)}`;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('UPDATE chat_sync_state SET epoch = $1, revision = 0, updated_at = NOW() WHERE id = 1', [nextEpoch]);
    await client.query('DELETE FROM chat_changes');
    await client.query('TRUNCATE chat_seen_events RESTART IDENTITY');
    await client.query('COMMIT');
    chatSyncState = { epoch: nextEpoch, revision: 0 };
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (e) {}
    throw err;
  } finally {
    client.release();
  }
  return chatSyncState;
}

async function getChatSyncSnapshotForUser(userName = '') {
  const result = await queryDbWithRetry(
    'SELECT id, message, (COUNT(*) OVER())::int AS total_count FROM chat ORDER BY id DESC LIMIT $1',
    [MAX_CHAT_HISTORY],
    { attempts: 2, label: 'CHAT SYNC SNAPSHOT READ' }
  );
  const rows = result.rows.reverse();
  const totalCount = rows.length ? Math.max(0, Number(rows[0].total_count) || 0) : 0;
  return {
    messages: rows.map(row => getPublicChatMessageForUser(attachChatDbId({ ...(row.message || {}) }, row.id), userName)),
    expectedCount: Math.min(MAX_CHAT_HISTORY, totalCount)
  };
}

async function getStableInitialChatSyncForUser(userName = '', clientSeenCursor = 0) {
  const deadline = Date.now() + 28000;
  let attempts = 0;

  while (Date.now() < deadline) {
    attempts++;
    const stateBefore = await refreshChatSyncStateFromDb();
    const snapshot = await getChatSyncSnapshotForUser(userName);
    const stateAfter = await refreshChatSyncStateFromDb();
    const messages = Array.isArray(snapshot && snapshot.messages) ? snapshot.messages : [];
    const expectedCount = Math.max(0, Number(snapshot && snapshot.expectedCount) || 0);
    const sameSnapshotRevision = String(stateBefore.epoch || '') === String(stateAfter.epoch || '')
      && Math.max(0, Number(stateBefore.revision) || 0) === Math.max(0, Number(stateAfter.revision) || 0);
    const completeSnapshot = messages.length === expectedCount;

    if (sameSnapshotRevision && completeSnapshot) {
      await new Promise(resolve => setTimeout(resolve, 120));
      const confirmedState = await refreshChatSyncStateFromDb();
      const stillStable = String(stateAfter.epoch || '') === String(confirmedState.epoch || '')
        && Math.max(0, Number(stateAfter.revision) || 0) === Math.max(0, Number(confirmedState.revision) || 0);

      if (stillStable) {
        const seenSync = await getChatSeenSyncForUser(userName, clientSeenCursor);
        return {
          success: true,
          syncV1: true,
          mode: 'snapshot',
          reason: 'initial-stable-snapshot',
          epoch: String(confirmedState.epoch || ''),
          revision: Math.max(0, Number(confirmedState.revision) || 0),
          messages,
          snapshotCount: messages.length,
          snapshotExpectedCount: expectedCount,
          snapshotComplete: true,
          seenSyncV1: true,
          seenMode: seenSync.mode,
          seenCursor: seenSync.cursor,
          seenEvents: seenSync.events,
          maxHistory: MAX_CHAT_HISTORY,
          initialStableSync: true,
          attempts
        };
      }
    }

    await new Promise(resolve => setTimeout(resolve, 40));
  }

  return null;
}

function sanitizeChatSyncChangeForUser(row, userName = '') {
  return {
    revision: Math.max(0, Number(row.revision) || 0),
    type: String(row.change_type || row.type || ''),
    msgId: String(row.message_id || row.msgId || ''),
    message: row.message ? getPublicChatMessageForUser(row.message, userName) : null
  };
}

async function refreshChatHistoryFromDb() {
  const chatRes = await pool.query('SELECT id, message FROM chat ORDER BY id DESC LIMIT $1', [MAX_CHAT_HISTORY]);
  const rows = chatRes.rows.reverse();
  messageHistory = rows.map(row => attachChatDbId({ ...(row.message || {}) }, row.id));
  lastChatDbId = rows.length ? Math.max(...rows.map(row => Number(row.id) || 0)) : 0;
  return messageHistory;
}

async function backupChatHistory(byUser = "Admin", reason = "manual clear") {
  const snapshot = getPublicChatHistory();
  await pool.query(
    'INSERT INTO chat_backups (by_user, reason, messages) VALUES ($1, $2, $3)',
    [byUser, reason, snapshot]
  );
  return snapshot.length;
}

async function clearChatHistorySafely(byUser = "Admin", reason = "manual clear") {
  await refreshChatHistoryFromDb();
  const backedUpCount = await backupChatHistory(byUser, reason);
  messageHistory = [];
  lastChatDbId = 0;
  await pool.query('TRUNCATE chat RESTART IDENTITY');
  await resetChatSyncEpoch();
  return backedUpCount;
}

async function syncChatAcrossInstances() {
  try {
    const newRows = await queryDbWithRetry(
      'SELECT id, message FROM chat WHERE id > $1 ORDER BY id ASC LIMIT $2',
      [lastChatDbId, MAX_CHAT_HISTORY],
      { attempts: 2, label: 'CHAT SYNC READ' }
    );

    if (newRows.rows.length > 0) {
      for (const row of newRows.rows) {
        const dbId = Number(row.id) || 0;
        if (messageHistory.some(m => Number(m._dbId) === dbId || (m.time && row.message && m.time === row.message.time))) {
          lastChatDbId = Math.max(lastChatDbId, dbId);
          continue;
        }
        const message = attachChatDbId({ ...(row.message || {}) }, dbId);
        messageHistory.push(message);
        if (messageHistory.length > MAX_CHAT_HISTORY) messageHistory.shift();
        lastChatDbId = Math.max(lastChatDbId, dbId);
        const publicSyncedMessage = cleanChatMessage(message);
        io.sockets.sockets.forEach(client => {
          if (!client || !client.connected || client.__chatInitialFullSyncInFlight === true) return;
          client.emit('chat_message', publicSyncedMessage);
        });
      }
      return;
    }

    if (messageHistory.length > 0) {
      const meta = await queryDbWithRetry('SELECT COUNT(*)::int AS total, COALESCE(MAX(id), 0)::int AS max_id FROM chat', [], { attempts: 2, label: 'CHAT SYNC META' });
      const total = Number(meta.rows[0]?.total || 0);
      const maxId = Number(meta.rows[0]?.max_id || 0);
      if (total === 0) {
        messageHistory = [];
        lastChatDbId = 0;
        io.emit('chat_cleared', { by: 'Server Sync' });
      } else if (maxId < lastChatDbId) {
        await refreshChatHistoryFromDb();
        const publicHistory = getPublicChatHistory();
        io.sockets.sockets.forEach(client => {
          if (!client || !client.connected || client.__chatInitialFullSyncInFlight === true) return;
          client.emit('chat_history', publicHistory);
        });
      }
    }
  } catch (err) {
    console.error('[CHAT SYNC ERROR]:', err);
  }
}

async function emitAdminState(socket) {
  socket.emit('maintenance_mode', adminState.maintenance);
  socket.emit('chat_controls', adminState.chatControls);
  socket.emit('admin_pinned_announcement', adminState.pinnedAnnouncement || { clear: true });

  if (socket.isAdmin === true) {
    await refreshReportsFromDb();
    await refreshServerLogFromDb();
    socket.emit('admin_state', {
      maintenance: adminState.maintenance,
      chatControls: adminState.chatControls,
      pinnedAnnouncement: adminState.pinnedAnnouncement || null,
      reports: adminReports,
      serverLog,
      registeredUsers: Object.keys(userDatabase).length,
      countryStats: getAdminCountryStats()
    });
    socket.emit('admin_chat_controls_state', adminState.chatControls);
    socket.emit('reports_list', adminReports);
    socket.emit('admin_server_log_list', serverLog);
  }
}

function emitToAdmins(event, payload) {
  io.sockets.sockets.forEach(client => {
    if (client.isAdmin === true) client.emit(event, payload);
  });
}

async function addModerationLog(type, message, detail = {}, admin = "System") {
  const entry = {
    id: `log-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    type,
    message,
    detail,
    admin,
    time: new Date().toISOString()
  };

  moderationLog.unshift(entry);
  moderationLog = moderationLog.slice(0, 100);

  try {
    await pool.query('INSERT INTO moderation_log (entry) VALUES ($1)', [entry]);
  } catch (err) {
    console.error('[ADMIN LOG ERROR]:', err);
  }

  emitToAdmins('admin_moderation_log', entry);
  return entry;
}


async function addServerLog(type, message, detail = {}, user = "Server") {
  const entry = {
    id: `server-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    type,
    message,
    detail,
    user,
    time: new Date().toISOString()
  };

  serverLog.unshift(entry);
  serverLog = serverLog.slice(0, 120);

  try {
    await pool.query('INSERT INTO server_log (entry) VALUES ($1)', [entry]);
  } catch (err) {
    console.error('[SERVER LOG ERROR]:', err);
  }

  emitToAdmins('admin_server_log', entry);
  return entry;
}

function getSocketsByUserName(name) {
  const sockets = [];
  io.sockets.sockets.forEach(client => {
    if (client.userName === name) sockets.push(client);
  });
  return sockets;
}


function buildFullProfileSyncPayload(name, user = {}, sourceSocketId = null, options = {}) {
  const safe = options.normalized === true ? user : normalizeUserRecord(name, user || {});
  return {
    name,
    sourceSocketId,
    profileUpdatedAt: safe.profileUpdatedAt || Date.now(),
    userData: {
      id: safe.id || null,
      name,
      avatar: safe.avatar || DEFAULT_AVATAR,
      joined: safe.joined || '2026',
      countryCode: getUserCountryCode(safe),
      role: getUserRole(name, safe),
      isAdmin: isUserAdmin(name, safe),
      isModerator: isUserModerator(name, safe),
      banned: isUserBanned(safe),
      lastSeen: safe.lastSeen || null,
      profileUpdatedAt: safe.profileUpdatedAt || 0,
      ps3Status: safe.ps3Status || null,
      level: safe.level || 1,
      xp: safe.xp || 0,
      downloads: Array.isArray(safe.downloadsData) ? safe.downloadsData.length : (safe.downloads || 0),
      wishlist: Array.isArray(safe.wishlistData) ? safe.wishlistData.length : (safe.wishlist || 0),
      favorites: Array.isArray(safe.favoritesData) ? safe.favoritesData.length : (safe.favorites || 0),
      trophies: safe.trophies || 0,
      library: Array.isArray(safe.libraryData) ? safe.libraryData.length : (safe.library || 0),
      trophiesData: safe.trophiesData || {},
      downloadsData: Array.isArray(safe.downloadsData) ? safe.downloadsData : [],
      downloadsClearedAt: normalizeTimestampValue(safe.downloadsClearedAt),
      downloadsUpdatedAt: normalizeTimestampValue(safe.downloadsUpdatedAt),
      wishlistData: Array.isArray(safe.wishlistData) ? safe.wishlistData : [],
      wishlistUpdatedAt: normalizeTimestampValue(safe.wishlistUpdatedAt),
      favoritesData: Array.isArray(safe.favoritesData) ? safe.favoritesData : [],
      favoritesUpdatedAt: normalizeTimestampValue(safe.favoritesUpdatedAt),
      libraryData: Array.isArray(safe.libraryData) ? safe.libraryData : [],
      libraryUpdatedAt: normalizeTimestampValue(safe.libraryUpdatedAt),
      friendsData: Array.isArray(safe.friendsData) ? safe.friendsData : [],
      friendsUpdatedAt: normalizeTimestampValue(safe.friendsUpdatedAt),
      recentlyVisited: Array.isArray(safe.recentlyVisitedData) ? safe.recentlyVisitedData.length : Number(safe.recentlyVisited || 0),
      recentlyVisitedData: normalizeRecentlyVisitedRecordsServer(safe.recentlyVisitedData),
      recentlyVisitedUpdatedAt: normalizeTimestampValue(safe.recentlyVisitedUpdatedAt),
      countersData: safe.countersData || {},
      themeColor: normalizeThemeColorServer(safe.themeColor || (safe.settingsData && safe.settingsData.themeColor) || '#0070cc'),
      themeColorUpdatedAt: getUserThemeColorUpdatedAt(safe),
      settingsData: { ...normalizeProfileRealtimeSettings(safe.settingsData || {}), ...getPublicProfileSettings(safe) }
    }
  };
}


const PROFILE_SYNC_PATCH_KEYS = new Set([
  'id', 'name', 'avatar', 'joined', 'countryCode', 'role', 'isAdmin', 'isModerator', 'banned',
  'lastSeen', 'ps3Status', 'level', 'xp', 'downloads', 'wishlist', 'favorites', 'trophies', 'library',
  'trophiesData', 'downloadsData', 'downloadsClearedAt', 'downloadsUpdatedAt',
  'wishlistData', 'wishlistUpdatedAt', 'favoritesData', 'favoritesUpdatedAt',
  'libraryData', 'libraryUpdatedAt', 'friendsData', 'friendsUpdatedAt',
  'recentlyVisited', 'recentlyVisitedData', 'recentlyVisitedUpdatedAt',
  'countersData', 'themeColor', 'themeColorUpdatedAt', 'settingsData'
]);

function buildProfileSyncPatchPayload(name, user = {}, changedKeys = [], sourceSocketId = null) {
  const keys = new Set(Array.isArray(changedKeys) ? changedKeys : []);
  const profileUpdatedAt = normalizeTimestampValue(user.profileUpdatedAt) || Date.now();
  const payload = { name, sourceSocketId, profileUpdatedAt, userData: { profileUpdatedAt } };
  const target = payload.userData;
  const include = key => keys.has(key) && PROFILE_SYNC_PATCH_KEYS.has(key);

  if (include('id')) target.id = user.id || null;
  if (include('name')) target.name = name;
  if (include('avatar')) target.avatar = user.avatar || DEFAULT_AVATAR;
  if (include('joined')) target.joined = user.joined || '2026';
  if (include('countryCode')) target.countryCode = getUserCountryCode(user);
  if (include('role')) target.role = getUserRole(name, user);
  if (include('isAdmin')) target.isAdmin = isUserAdmin(name, user);
  if (include('isModerator')) target.isModerator = isUserModerator(name, user);
  if (include('banned')) target.banned = isUserBanned(user);
  if (include('lastSeen')) target.lastSeen = user.lastSeen || null;
  if (include('ps3Status')) target.ps3Status = user.ps3Status || null;
  if (include('level')) target.level = user.level || 1;
  if (include('xp')) target.xp = user.xp || 0;
  if (include('downloads')) target.downloads = Array.isArray(user.downloadsData) ? user.downloadsData.length : Number(user.downloads || 0);
  if (include('wishlist')) target.wishlist = Array.isArray(user.wishlistData) ? user.wishlistData.length : Number(user.wishlist || 0);
  if (include('favorites')) target.favorites = Array.isArray(user.favoritesData) ? user.favoritesData.length : Number(user.favorites || 0);
  if (include('trophies')) target.trophies = Number(user.trophies || 0);
  if (include('library')) target.library = Array.isArray(user.libraryData) ? user.libraryData.length : Number(user.library || 0);
  if (include('trophiesData')) target.trophiesData = user.trophiesData && typeof user.trophiesData === 'object' && !Array.isArray(user.trophiesData) ? user.trophiesData : {};
  if (include('downloadsData')) target.downloadsData = Array.isArray(user.downloadsData) ? user.downloadsData : [];
  if (include('downloadsClearedAt')) target.downloadsClearedAt = normalizeTimestampValue(user.downloadsClearedAt);
  if (include('downloadsUpdatedAt')) target.downloadsUpdatedAt = normalizeTimestampValue(user.downloadsUpdatedAt);
  if (include('wishlistData')) target.wishlistData = Array.isArray(user.wishlistData) ? user.wishlistData : [];
  if (include('wishlistUpdatedAt')) target.wishlistUpdatedAt = normalizeTimestampValue(user.wishlistUpdatedAt);
  if (include('favoritesData')) target.favoritesData = Array.isArray(user.favoritesData) ? user.favoritesData : [];
  if (include('favoritesUpdatedAt')) target.favoritesUpdatedAt = normalizeTimestampValue(user.favoritesUpdatedAt);
  if (include('libraryData')) target.libraryData = Array.isArray(user.libraryData) ? user.libraryData : [];
  if (include('libraryUpdatedAt')) target.libraryUpdatedAt = normalizeTimestampValue(user.libraryUpdatedAt);
  if (include('friendsData')) target.friendsData = Array.isArray(user.friendsData) ? user.friendsData : [];
  if (include('friendsUpdatedAt')) target.friendsUpdatedAt = normalizeTimestampValue(user.friendsUpdatedAt);
  if (include('recentlyVisited')) target.recentlyVisited = Array.isArray(user.recentlyVisitedData) ? user.recentlyVisitedData.length : Number(user.recentlyVisited || 0);
  if (include('recentlyVisitedData')) target.recentlyVisitedData = normalizeRecentlyVisitedRecordsServer(user.recentlyVisitedData);
  if (include('recentlyVisitedUpdatedAt')) target.recentlyVisitedUpdatedAt = normalizeTimestampValue(user.recentlyVisitedUpdatedAt);
  if (include('countersData')) target.countersData = user.countersData || {};
  if (include('themeColor')) target.themeColor = normalizeThemeColorServer(user.themeColor || (user.settingsData && user.settingsData.themeColor) || '#0070cc');
  if (include('themeColorUpdatedAt')) target.themeColorUpdatedAt = getUserThemeColorUpdatedAt(user);
  if (include('settingsData')) target.settingsData = { ...normalizeProfileRealtimeSettings(user.settingsData || {}), ...getPublicProfileSettings(user) };

  return payload;
}

function emitProfileSyncPatchFromUser(name, user = {}, changedKeys = [], sourceSocketId = null) {
  if (!name || !user) return;
  const safeKeys = Array.isArray(changedKeys) ? [...new Set(changedKeys.filter(key => PROFILE_SYNC_PATCH_KEYS.has(key)))] : [];
  if (!safeKeys.length) return;
  const payload = buildProfileSyncPatchPayload(name, user, safeKeys, sourceSocketId);
  getSocketsByUserName(name).forEach(client => {
    if (sourceSocketId && client.id === sourceSocketId) return;
    client.emit('profile_sync', payload);
  });
}

function emitProfileCountsUpdate(name, user = null) {
  if (!name) return;
  const source = user || userDatabase[name];
  if (!source) return;
  io.emit('profile_counts_update', {
    name,
    downloads: Array.isArray(source.downloadsData) ? source.downloadsData.length : Number(source.downloads || 0),
    wishlist: Array.isArray(source.wishlistData) ? source.wishlistData.length : Number(source.wishlist || 0),
    favorites: Array.isArray(source.favoritesData) ? source.favoritesData.length : Number(source.favorites || 0),
    trophies: source.trophiesData && typeof source.trophiesData === 'object' && !Array.isArray(source.trophiesData) ? countUnlockedTrophiesPayload(source.trophiesData) : Number(source.trophies || 0),
    library: Array.isArray(source.libraryData) ? source.libraryData.length : Number(source.library || 0),
    profileUpdatedAt: normalizeTimestampValue(source.profileUpdatedAt) || Date.now()
  });
}

function buildPresenceUpdatePayload(name, user = null) {
  if (!name) return null;
  const source = user || userDatabase[name];
  if (!source) return null;
  return {
    name,
    id: source.online === true ? (source.id || null) : null,
    online: source.online === true,
    lastSeen: normalizeTimestampValue(source.lastSeen) || null,
    avatar: source.avatar || DEFAULT_AVATAR,
    level: Number(source.level || 1),
    joined: source.joined || '2026',
    countryCode: getUserCountryCode(source),
    role: getUserRole(name, source),
    isAdmin: isUserAdmin(name, source),
    banned: isUserBanned(source),
    ps3Status: source.online === true ? (source.ps3Status || null) : null,
    downloads: Array.isArray(source.downloadsData) ? source.downloadsData.length : Number(source.downloads || 0),
    wishlist: Array.isArray(source.wishlistData) ? source.wishlistData.length : Number(source.wishlist || 0),
    favorites: Array.isArray(source.favoritesData) ? source.favoritesData.length : Number(source.favorites || 0),
    trophies: Number(source.trophies || 0),
    library: Array.isArray(source.libraryData) ? source.libraryData.length : Number(source.library || 0)
  };
}

function emitPresenceUpdate(name, user = null) {
  const payload = buildPresenceUpdatePayload(name, user);
  if (!payload) return null;
  io.emit('presence_update', payload);
  return payload;
}

async function notifyPresenceAcrossInstances(name, user = null) {
  const payload = buildPresenceUpdatePayload(name, user);
  if (!payload) return;
  try {
    await pool.query('SELECT pg_notify($1, $2)', ['presence_sync', JSON.stringify({ ...payload, instanceId: INSTANCE_ID })]);
  } catch (err) {
    console.error('[PRESENCE NOTIFY ERROR]:', err);
  }
}

async function notifyPs3PlayTimeAcrossInstances(payload = {}) {
  const name = normalizeText(payload.name, '');
  const playTime = normalizePs3PlayTimeServer(payload.playTime);
  if (!name || !playTime) return;

  const message = {
    name,
    titleId: normalizeText(payload.titleId, '').toUpperCase().slice(0, 32),
    playTime,
    playTimeUpdatedAt: normalizeTimestampValue(payload.playTimeUpdatedAt) || Date.now(),
    instanceId: INSTANCE_ID
  };

  try {
    await pool.query('SELECT pg_notify($1, $2)', ['ps3_playtime_sync', JSON.stringify(message)]);
  } catch (err) {
    console.error('[PS3 PLAYTIME NOTIFY ERROR]:', err);
  }
}

function profileUpdateTouchesPublicCounts(userData = {}) {
  return !!(userData && [
    'downloadsData', 'downloads', 'downloadsClearedAt', 'downloadsUpdatedAt',
    'wishlistData', 'wishlist', 'wishlistUpdatedAt',
    'favoritesData', 'favorites', 'favoritesUpdatedAt',
    'libraryData', 'library', 'libraryUpdatedAt',
    'trophiesData', 'trophies'
  ].some(key => Object.prototype.hasOwnProperty.call(userData, key)));
}


function emitSettingsRealtimeSync(name, sourceSocketId = null, extra = {}) {
  if (!name || !userDatabase[name]) return;
  const safe = normalizeUserRecord(name, userDatabase[name] || {});
  const payload = {
    name,
    sourceSocketId,
    profileUpdatedAt: normalizeTimestampValue(safe.profileUpdatedAt) || Date.now(),
    settingsUpdatedAt: getProfileSettingsUpdatedAt(safe.settingsData || {}),
    themeColor: normalizeThemeColorServer(safe.themeColor || (safe.settingsData && safe.settingsData.themeColor) || '#0070cc'),
    themeColorUpdatedAt: getUserThemeColorUpdatedAt(safe),
    settingsData: normalizeProfileRealtimeSettings(safe.settingsData || {}),
    userData: {
      profileUpdatedAt: normalizeTimestampValue(safe.profileUpdatedAt) || Date.now(),
      themeColor: normalizeThemeColorServer(safe.themeColor || (safe.settingsData && safe.settingsData.themeColor) || '#0070cc'),
      themeColorUpdatedAt: getUserThemeColorUpdatedAt(safe),
      settingsData: normalizeProfileRealtimeSettings(safe.settingsData || {})
    },
    ...extra
  };
  getSocketsByUserName(name).forEach(client => {
    if (sourceSocketId && client.id === sourceSocketId) return;
    client.emit('settings_realtime_sync', payload);
  });
}


async function syncActiveProfilesAcrossInstances() {
  const activeNames = [...new Set(Array.from(io.sockets.sockets.values())
    .filter(client => client.connected && client.userName)
    .map(client => client.userName))];

  for (const name of activeNames) {
    const localVersion = normalizeTimestampValue(userDatabase[name] && userDatabase[name].profileUpdatedAt);
    const refreshedUser = await refreshSingleUserSummaryFromDb(name);
    if (!refreshedUser) continue;
    const dbVersion = normalizeTimestampValue(refreshedUser.profileUpdatedAt);
    if (!dbVersion || dbVersion <= localVersion) continue;

    const localSockets = getSocketsByUserName(name);
    for (const client of localSockets) {
      if (client.profileSyncV2 === true) {
        await emitChunkedProfileSyncToSocket(client, name, { forceRefresh: false });
      } else {
        const fullUser = await runSerializedProfileHydration(() => loadFullUserRecordTransient(name));
        if (fullUser && client.connected) client.emit('profile_sync', buildFullProfileSyncPayload(name, fullUser, null, { normalized: true }));
      }
    }
    emitPublicProfileBannerUpdate(name, refreshedUser);
  }
}


async function notifyProfileSyncAcrossInstances(name, sourceSocketId = null, profileUpdatedAt = Date.now(), changes = {}) {
  if (!name) return;
  const payload = {
    name,
    sourceSocketId,
    profileUpdatedAt,
    instanceId: INSTANCE_ID,
    changes: {
      trending: changes && changes.trending === true,
      trophies: changes && changes.trophies === true,
      counts: changes && changes.counts === true,
      keys: Array.isArray(changes && changes.keys) ? [...new Set(changes.keys.filter(key => PROFILE_SYNC_PATCH_KEYS.has(key)))] : []
    }
  };

  try {
    await pool.query('SELECT pg_notify($1, $2)', ['profile_sync', JSON.stringify(payload)]);
  } catch (err) {
    console.error('[PROFILE NOTIFY ERROR]:', err);
  }
}

async function notifyAdminStateAcrossInstances(key, state) {
  if (!key) return;
  const payload = { key, state, instanceId: INSTANCE_ID, at: Date.now() };
  try {
    await pool.query('SELECT pg_notify($1, $2)', ['admin_state_sync', JSON.stringify(payload)]);
  } catch (err) {
    console.error('[ADMIN STATE NOTIFY ERROR]:', err);
  }
}

async function initProfileSyncNotifications() {
  if (profileSyncNotifyClient) return;

  const client = new Client(pgConnectionOptions);
  profileSyncNotifyClient = client;

  client.on('notification', async (message) => {
    if (!message || !['profile_sync', 'presence_sync', 'ps3_playtime_sync', 'admin_state_sync', 'friend_activity_sync', 'user_notification_sync'].includes(message.channel)) return;

    try {
      const data = JSON.parse(message.payload || '{}');
      if (data.instanceId === INSTANCE_ID) return;

      if (message.channel === 'friend_activity_sync') {
        if (data.event) emitFriendActivityToLocalSubscribers(data.event);
        return;
      }

      if (message.channel === 'user_notification_sync') {
        if (data.event) emitUserNotificationToLocalUser(data.event);
        if (data.readState && data.readState.userName) {
          emitUserNotificationReadStateToLocalUser(data.readState.userName, data.readState.lastReadId, data.readState.unreadCount);
        }
        return;
      }

      if (message.channel === 'admin_state_sync') {
        const key = normalizeText(data.key, '');
        if (key === ADMIN_STATE_KEYS.maintenance) {
          adminState.maintenance = normalizeMaintenanceState(data.state || {});
          adminStateLastRefreshAt = Date.now();
          io.emit('maintenance_mode', adminState.maintenance);
          emitToAdmins('admin_state', {
            maintenance: adminState.maintenance,
            chatControls: adminState.chatControls,
            pinnedAnnouncement: adminState.pinnedAnnouncement || null,
            reports: adminReports,
            serverLog,
            registeredUsers: Object.keys(userDatabase).length,
            countryStats: getAdminCountryStats()
          });
        }
        return;
      }

      const name = normalizeText(data.name, '');
      if (!name) return;

      if (message.channel === 'ps3_playtime_sync') {
        const playTime = normalizePs3PlayTimeServer(data.playTime);
        if (!playTime) return;

        if (!userDatabase[name]) await refreshSingleUserSummaryFromDb(name);
        if (!userDatabase[name]) return;

        const currentStatus = userDatabase[name].ps3Status;
        if (!currentStatus || currentStatus.status !== 'playing') return;

        const incomingTitleId = normalizeText(data.titleId, '').toUpperCase();
        const currentTitleId = normalizeText(currentStatus.titleId, '').toUpperCase();
        if (incomingTitleId && currentTitleId && incomingTitleId !== currentTitleId) return;

        const updatedAt = normalizeTimestampValue(data.playTimeUpdatedAt) || Date.now();
        userDatabase[name].ps3Status = {
          ...currentStatus,
          playTime,
          playTimeUpdatedAt: updatedAt
        };

        invalidateOnlineListCache('ps3-playtime-listen');
        io.emit('ps3_playtime_update', {
          name,
          titleId: currentTitleId || incomingTitleId,
          playTime,
          playTimeUpdatedAt: updatedAt
        });
        return;
      }

      const hasLocalSession = Array.from(io.sockets.sockets.values()).some(activeSocket => (
        activeSocket.connected && activeSocket.userName === name
      ));

      if (message.channel === 'presence_sync') {
        if (!userDatabase[name]) await refreshSingleUserSummaryFromDb(name);
        if (!userDatabase[name]) return;

        const incomingLastSeen = normalizeTimestampValue(data.lastSeen);
        userDatabase[name].online = hasLocalSession ? true : data.online === true;
        if (data.online === true && data.id) userDatabase[name].id = data.id;
        if (incomingLastSeen) userDatabase[name].lastSeen = Math.max(Number(userDatabase[name].lastSeen || 0), incomingLastSeen);
        if (Object.prototype.hasOwnProperty.call(data, 'ps3Status')) userDatabase[name].ps3Status = data.ps3Status || null;
        invalidateOnlineListCache('presence-listen');
        emitPresenceUpdate(name, userDatabase[name]);
        deferServerTask('PRESENCE LISTEN ONLINE LIST', () => emitOnlineList(), 80);
        return;
      }

      const refreshedUser = await refreshSingleUserSummaryFromDb(name);
      if (!refreshedUser) {
        invalidateOnlineListCache("profile-sync-listen-missing");
        deferServerTask('PROFILE LISTEN ONLINE LIST', () => emitOnlineList(), 1000);
        return;
      }

      if (hasLocalSession && refreshedUser.passwordResetRequired === true) {
        const resetExpiresAt = Number(refreshedUser.passwordResetExpiresAt || 0);
        if (!resetExpiresAt || resetExpiresAt > Date.now()) {
          disconnectUserSessions(name, 'password_reset_by_admin', {
            targetName: name,
            by: refreshedUser.passwordResetBy || 'Admin',
            resetAt: refreshedUser.passwordResetAt || null,
            expiresAt: resetExpiresAt || (Date.now() + PASSWORD_RESET_WINDOW_MS),
            expiresInMs: resetExpiresAt ? Math.max(0, resetExpiresAt - Date.now()) : PASSWORD_RESET_WINDOW_MS
          });
        }
      }

      if (data.changes && data.changes.trending === true) {
        invalidateTrendingCache();
        scheduleTrendingRefreshBroadcast(1200);
      }
      if (data.changes && data.changes.trophies === true) {
        invalidateGlobalTrophyStatsCache();
        scheduleTrophyStatsRefreshBroadcast(1200);
      }
      if (data.changes && data.changes.counts === true) emitProfileCountsUpdate(name, refreshedUser);
      if (hasLocalSession) {
        const changedKeys = data.changes && Array.isArray(data.changes.keys) ? data.changes.keys : [];
        const localSockets = getSocketsByUserName(name).filter(client => !(data.sourceSocketId && client.id === data.sourceSocketId));
        for (const client of localSockets) {
          if (client.profileSyncV2 === true) {
            await emitChunkedProfileSyncToSocket(client, name, { forceRefresh: false, changedKeys, sourceSocketId: data.sourceSocketId || null });
          } else {
            const fullUser = await runSerializedProfileHydration(() => loadFullUserRecordTransient(name));
            if (fullUser && client.connected) client.emit('profile_sync', buildFullProfileSyncPayload(name, fullUser, data.sourceSocketId || null, { normalized: true }));
          }
        }
      }
      emitPublicProfileBannerUpdate(name, refreshedUser);
      invalidateOnlineListCache("profile-sync-listen");
      deferServerTask('PROFILE LISTEN ONLINE LIST', () => emitOnlineList(), 1000);
    } catch (err) {
      console.error(`[${message && message.channel === 'presence_sync' ? 'PRESENCE' : 'PROFILE'} LISTEN ERROR]:`, err);
    }
  });

  client.on('error', (err) => {
    console.error('[PROFILE LISTEN CONNECTION ERROR]:', err && err.message ? err.message : err);
    if (profileSyncNotifyClient === client) profileSyncNotifyClient = null;
    client.end().catch(() => {});
    scheduleProfileSyncReconnect(isPgConnectionLimitError(err) ? 15000 : 5000);
  });

  client.on('end', () => {
    if (profileSyncNotifyClient === client) profileSyncNotifyClient = null;
    scheduleProfileSyncReconnect(5000);
  });

  try {
    await client.connect();
    await client.query('LISTEN profile_sync');
    await client.query('LISTEN presence_sync');
    await client.query('LISTEN ps3_playtime_sync');
    await client.query('LISTEN admin_state_sync');
    await client.query('LISTEN friend_activity_sync');
    await client.query('LISTEN user_notification_sync');
    console.log('[PROFILE SYNC] Postgres LISTEN enabled.');
    console.log('[PRESENCE SYNC] Postgres LISTEN enabled.');
    console.log('[PS3 PLAYTIME SYNC] Postgres LISTEN enabled.');
    console.log('[ADMIN STATE SYNC] Postgres LISTEN enabled.');
    console.log('[FRIEND ACTIVITY] Postgres LISTEN enabled.');
    console.log('[NOTIFICATIONS] Postgres LISTEN enabled.');
  } catch (err) {
    if (profileSyncNotifyClient === client) profileSyncNotifyClient = null;
    console.error('[PROFILE LISTEN INIT ERROR]:', err && err.message ? err.message : err);
    await client.end().catch(() => {});
    scheduleProfileSyncReconnect(isPgConnectionLimitError(err) ? 15000 : 5000);
  }
}

const FRIEND_ACTIVITY_TYPES = new Set(['online', 'playing', 'xmb', 'trophy']);
const FRIEND_ACTIVITY_LIMIT = 60;
const FRIEND_ACTIVITY_ROOM_PREFIX = 'friend-activity:';

function normalizeFriendActivityName(value) {
  return normalizeText(value, '').slice(0, 80);
}

function normalizeFriendActivityData(type, rawData = {}) {
  const source = rawData && typeof rawData === 'object' && !Array.isArray(rawData) ? rawData : {};
  if (type === 'playing' || type === 'xmb') {
    return {
      titleId: normalizeText(source.titleId, '').toUpperCase().slice(0, 32),
      title: normalizeText(source.title, '').slice(0, 140),
      appVersion: normalizeText(source.appVersion, '').slice(0, 24)
    };
  }
  if (type === 'trophy') {
    const trophyType = normalizeText(source.trophyType, '').toLowerCase();
    return {
      trophyId: normalizeText(source.trophyId, '').slice(0, 80),
      title: normalizeText(source.title, 'Trophy').slice(0, 140),
      trophyType: ['bronze', 'silver', 'gold', 'platinum'].includes(trophyType) ? trophyType : 'bronze'
    };
  }
  return {};
}

function serializeFriendActivityRow(row = {}) {
  const type = normalizeText(row.event_type || row.type, '').toLowerCase();
  if (!FRIEND_ACTIVITY_TYPES.has(type)) return null;
  const actor = normalizeFriendActivityName(row.actor_name || row.actor);
  if (!actor) return null;
  const createdAtRaw = row.created_at instanceof Date ? row.created_at.getTime() : Number(row.created_at || row.createdAt || Date.now());
  return {
    id: String(row.id || ''),
    actor,
    type,
    data: normalizeFriendActivityData(type, row.data),
    createdAt: Number.isFinite(createdAtRaw) && createdAtRaw > 0 ? createdAtRaw : Date.now()
  };
}

function friendActivityRoom(actorName) {
  return `${FRIEND_ACTIVITY_ROOM_PREFIX}${normalizeFriendActivityName(actorName).toLowerCase()}`;
}

function extractFriendActivityNames(list) {
  if (!Array.isArray(list)) return [];
  const names = [];
  const seen = new Set();
  list.forEach(item => {
    const name = normalizeFriendActivityName(item && typeof item === 'object' ? item.name : item);
    const key = name.toLowerCase();
    if (!name || seen.has(key)) return;
    seen.add(key);
    names.push(name);
  });
  return names.slice(0, 500);
}

function clearFriendActivitySubscription(socket) {
  if (!socket) return;
  const rooms = socket.__friendActivityRooms instanceof Set ? Array.from(socket.__friendActivityRooms) : [];
  rooms.forEach(room => { try { socket.leave(room); } catch (err) {} });
  socket.__friendActivityRooms = new Set();
  socket.__friendActivitySubscribed = false;
}

function setFriendActivitySubscription(socket, friendNames = []) {
  if (!socket) return [];
  clearFriendActivitySubscription(socket);
  const names = extractFriendActivityNames(friendNames);
  const rooms = new Set();
  names.forEach(name => {
    const room = friendActivityRoom(name);
    socket.join(room);
    rooms.add(room);
  });
  socket.__friendActivityRooms = rooms;
  socket.__friendActivitySubscribed = true;
  return names;
}

function emitFriendActivityToLocalSubscribers(event) {
  const safeEvent = serializeFriendActivityRow(event);
  if (!safeEvent) return;
  io.to(friendActivityRoom(safeEvent.actor)).emit('friend_activity_event', safeEvent);
}

async function notifyFriendActivityAcrossInstances(event) {
  const safeEvent = serializeFriendActivityRow(event);
  if (!safeEvent) return;
  try {
    await pool.query('SELECT pg_notify($1, $2)', ['friend_activity_sync', JSON.stringify({ event: safeEvent, instanceId: INSTANCE_ID })]);
  } catch (err) {
    console.error('[FRIEND ACTIVITY NOTIFY ERROR]:', err);
  }
}

async function recordFriendActivity(actorName, type, rawData = {}, options = {}) {
  const actor = normalizeFriendActivityName(actorName);
  const eventType = normalizeText(type, '').toLowerCase();
  if (!actor || !FRIEND_ACTIVITY_TYPES.has(eventType)) return null;
  const data = normalizeFriendActivityData(eventType, rawData);
  const at = normalizeTimestampValue(options.at) || Date.now();
  const detailKey = eventType === 'playing' || eventType === 'xmb'
    ? `${data.titleId || data.title}`
    : (eventType === 'trophy' ? `${data.trophyId || data.title}` : 'online');
  const dedupeKey = normalizeText(options.dedupeKey, '') || `${actor.toLowerCase()}:${eventType}:${String(detailKey || '').toLowerCase()}:${Math.floor(at / 5000)}`;

  try {
    const result = await queryDbWithRetry(
      `INSERT INTO friend_activity (actor_name, event_type, data, dedupe_key, created_at)
       VALUES ($1, $2, $3::jsonb, $4, to_timestamp($5::double precision / 1000.0))
       ON CONFLICT (dedupe_key) DO NOTHING
       RETURNING id, actor_name, event_type, data, created_at`,
      [actor, eventType, JSON.stringify(data), dedupeKey, at],
      { attempts: 2, label: 'FRIEND ACTIVITY INSERT' }
    );
    if (!result.rows[0]) return null;
    const event = serializeFriendActivityRow(result.rows[0]);
    if (!event) return null;
    deferServerTask('FRIEND ACTIVITY RETENTION', async () => {
      await queryDbWithRetry(
        `DELETE FROM friend_activity
         WHERE actor_name = $1
           AND id < COALESCE((
             SELECT id
             FROM friend_activity
             WHERE actor_name = $1
             ORDER BY id DESC
             OFFSET $2
             LIMIT 1
           ), 0)`,
        [actor, FRIEND_ACTIVITY_MAX_PER_USER - 1],
        { attempts: 2, label: 'FRIEND ACTIVITY RETENTION' }
      );
    }, 0);
    emitFriendActivityToLocalSubscribers(event);
    deferServerTask('FRIEND ACTIVITY NOTIFY', () => notifyFriendActivityAcrossInstances(event), 0);
    return event;
  } catch (err) {
    console.error('[FRIEND ACTIVITY INSERT ERROR]:', err && err.message ? err.message : err);
    return null;
  }
}

async function getFriendActivityReadId(userName) {
  const name = normalizeFriendActivityName(userName);
  if (!name) return 0;
  const result = await queryDbWithRetry(
    'SELECT last_read_id FROM friend_activity_read_state WHERE user_name = $1 LIMIT 1',
    [name],
    { attempts: 2, label: 'FRIEND ACTIVITY READ STATE' }
  );
  return Math.max(0, Number(result.rows[0] && result.rows[0].last_read_id) || 0);
}

async function setFriendActivityReadId(userName, lastReadId) {
  const name = normalizeFriendActivityName(userName);
  const safeId = Math.max(0, Math.floor(Number(lastReadId) || 0));
  if (!name || !safeId) return 0;
  const result = await queryDbWithRetry(
    `INSERT INTO friend_activity_read_state (user_name, last_read_id, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (user_name)
     DO UPDATE SET last_read_id = GREATEST(friend_activity_read_state.last_read_id, EXCLUDED.last_read_id), updated_at = NOW()
     RETURNING last_read_id`,
    [name, safeId],
    { attempts: 2, label: 'FRIEND ACTIVITY MARK READ' }
  );
  return Math.max(0, Number(result.rows[0] && result.rows[0].last_read_id) || safeId);
}

async function getFriendActivityHistoryForUser(userName, limit = FRIEND_ACTIVITY_LIMIT, afterId = 0, resolvedFriendNames = null) {
  const friendNames = Array.isArray(resolvedFriendNames)
    ? extractFriendActivityNames(resolvedFriendNames)
    : extractFriendActivityNames(await getUserDataPayloadFromDb(userName, 'friends'));
  const safeLimit = Math.max(1, Math.min(FRIEND_ACTIVITY_LIMIT, Number(limit) || FRIEND_ACTIVITY_LIMIT));
  const safeAfterId = Math.max(0, Math.floor(Number(afterId) || 0));
  if (!friendNames.length) return { friendNames, items: [], lastReadId: await getFriendActivityReadId(userName), delta: safeAfterId > 0 };
  const params = safeAfterId > 0 ? [friendNames, safeAfterId, safeLimit] : [friendNames, safeLimit];
  const result = await queryDbWithRetry(
    safeAfterId > 0
      ? `SELECT id, actor_name, event_type, data, created_at
         FROM friend_activity
         WHERE actor_name = ANY($1::text[]) AND id > $2
         ORDER BY id DESC
         LIMIT $3`
      : `SELECT id, actor_name, event_type, data, created_at
         FROM friend_activity
         WHERE actor_name = ANY($1::text[])
         ORDER BY id DESC
         LIMIT $2`,
    params,
    { attempts: 2, label: safeAfterId > 0 ? 'FRIEND ACTIVITY DELTA' : 'FRIEND ACTIVITY READ' }
  );
  const lastReadId = await getFriendActivityReadId(userName);
  return { friendNames, items: result.rows.map(serializeFriendActivityRow).filter(Boolean), lastReadId, delta: safeAfterId > 0 };
}

const USER_NOTIFICATION_TYPES = new Set(['mention', 'reply', 'trophy']);
const USER_NOTIFICATION_LIMIT = 60;

function normalizeUserNotificationName(value) {
  return normalizeText(value, '').slice(0, 80);
}

function normalizeUserNotificationData(type, rawData = {}) {
  const source = rawData && typeof rawData === 'object' && !Array.isArray(rawData) ? rawData : {};
  if (type === 'mention' || type === 'reply') {
    return {
      actor: normalizeUserNotificationName(source.actor),
      messageId: normalizeText(source.messageId, '').slice(0, 100),
      text: normalizeText(source.text, '').slice(0, 280)
    };
  }
  if (type === 'trophy') {
    const trophyType = normalizeText(source.trophyType, '').toLowerCase();
    return {
      trophyId: normalizeText(source.trophyId, '').slice(0, 80),
      title: normalizeText(source.title, 'Trophy').slice(0, 140),
      trophyType: ['bronze', 'silver', 'gold', 'platinum'].includes(trophyType) ? trophyType : 'bronze'
    };
  }
  return {};
}

function serializeUserNotificationRow(row = {}) {
  const type = normalizeText(row.event_type || row.type, '').toLowerCase();
  if (!USER_NOTIFICATION_TYPES.has(type)) return null;
  const user = normalizeUserNotificationName(row.user_name || row.user);
  if (!user) return null;
  const createdAtRaw = row.created_at instanceof Date ? row.created_at.getTime() : Number(row.created_at || row.createdAt || Date.now());
  return {
    id: String(row.id || ''),
    user,
    type,
    data: normalizeUserNotificationData(type, row.data),
    createdAt: Number.isFinite(createdAtRaw) && createdAtRaw > 0 ? createdAtRaw : Date.now()
  };
}

function emitUserNotificationToLocalUser(event) {
  const safeEvent = serializeUserNotificationRow(event);
  if (!safeEvent) return;
  getSocketsByUserName(safeEvent.user).forEach(client => {
    if (client && client.connected) client.emit('user_notification_event', safeEvent);
  });
}

function emitUserNotificationReadStateToLocalUser(userName, lastReadId, unreadCount = 0) {
  const name = normalizeUserNotificationName(userName);
  if (!name) return;
  const payload = {
    lastReadId: Math.max(0, Math.floor(Number(lastReadId) || 0)),
    unreadCount: Math.max(0, Math.floor(Number(unreadCount) || 0))
  };
  getSocketsByUserName(name).forEach(client => {
    if (client && client.connected) client.emit('user_notification_read_state', payload);
  });
}

async function notifyUserNotificationAcrossInstances(event) {
  const safeEvent = serializeUserNotificationRow(event);
  if (!safeEvent) return;
  try {
    await pool.query('SELECT pg_notify($1, $2)', ['user_notification_sync', JSON.stringify({ event: safeEvent, instanceId: INSTANCE_ID })]);
  } catch (err) {
    console.error('[NOTIFICATION SYNC ERROR]:', err);
  }
}

async function notifyUserNotificationReadStateAcrossInstances(userName, lastReadId, unreadCount = 0) {
  const name = normalizeUserNotificationName(userName);
  if (!name) return;
  try {
    await pool.query('SELECT pg_notify($1, $2)', ['user_notification_sync', JSON.stringify({
      readState: { userName: name, lastReadId: Math.max(0, Number(lastReadId) || 0), unreadCount: Math.max(0, Number(unreadCount) || 0) },
      instanceId: INSTANCE_ID
    })]);
  } catch (err) {
    console.error('[NOTIFICATION READ SYNC ERROR]:', err);
  }
}

async function recordUserNotification(userName, type, rawData = {}, options = {}) {
  const user = normalizeUserNotificationName(userName);
  const eventType = normalizeText(type, '').toLowerCase();
  if (!user || !USER_NOTIFICATION_TYPES.has(eventType)) return null;
  const data = normalizeUserNotificationData(eventType, rawData);
  const at = normalizeTimestampValue(options.at) || Date.now();
  const fallbackDetail = eventType === 'trophy'
    ? `${data.trophyId || data.title}`
    : `${data.messageId || data.actor || at}`;
  const dedupeKey = normalizeText(options.dedupeKey, '') || `${user.toLowerCase()}:${eventType}:${String(fallbackDetail || '').toLowerCase()}`;

  try {
    const result = await queryDbWithRetry(
      `INSERT INTO user_notifications (user_name, event_type, data, dedupe_key, created_at)
       VALUES ($1, $2, $3::jsonb, $4, to_timestamp($5::double precision / 1000.0))
       ON CONFLICT (dedupe_key) DO NOTHING
       RETURNING id, user_name, event_type, data, created_at`,
      [user, eventType, JSON.stringify(data), dedupeKey, at],
      { attempts: 2, label: 'USER NOTIFICATION INSERT' }
    );
    if (!result.rows[0]) return null;
    const event = serializeUserNotificationRow(result.rows[0]);
    if (!event) return null;
    deferServerTask('USER NOTIFICATION RETENTION', async () => {
      await queryDbWithRetry(
        `DELETE FROM user_notifications
         WHERE user_name = $1
           AND id < COALESCE((
             SELECT id
             FROM user_notifications
             WHERE user_name = $1
             ORDER BY id DESC
             OFFSET $2
             LIMIT 1
           ), 0)`,
        [user, USER_NOTIFICATION_MAX_PER_USER - 1],
        { attempts: 2, label: 'USER NOTIFICATION RETENTION' }
      );
    }, 0);
    emitUserNotificationToLocalUser(event);
    deferServerTask('USER NOTIFICATION SYNC', () => notifyUserNotificationAcrossInstances(event), 0);
    return event;
  } catch (err) {
    console.error('[USER NOTIFICATION INSERT ERROR]:', err && err.message ? err.message : err);
    return null;
  }
}

async function getUserNotificationReadId(userName) {
  const name = normalizeUserNotificationName(userName);
  if (!name) return 0;
  const result = await queryDbWithRetry(
    'SELECT last_read_id FROM user_notification_read_state WHERE user_name = $1 LIMIT 1',
    [name],
    { attempts: 2, label: 'USER NOTIFICATION READ STATE' }
  );
  return Math.max(0, Number(result.rows[0] && result.rows[0].last_read_id) || 0);
}

async function setUserNotificationReadId(userName, lastReadId) {
  const name = normalizeUserNotificationName(userName);
  const safeId = Math.max(0, Math.floor(Number(lastReadId) || 0));
  if (!name || !safeId) return 0;
  const result = await queryDbWithRetry(
    `INSERT INTO user_notification_read_state (user_name, last_read_id, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (user_name)
     DO UPDATE SET last_read_id = GREATEST(user_notification_read_state.last_read_id, EXCLUDED.last_read_id), updated_at = NOW()
     RETURNING last_read_id`,
    [name, safeId],
    { attempts: 2, label: 'USER NOTIFICATION MARK READ' }
  );
  return Math.max(0, Number(result.rows[0] && result.rows[0].last_read_id) || safeId);
}

async function getUserNotificationUnreadCount(userName, lastReadId = null) {
  const name = normalizeUserNotificationName(userName);
  if (!name) return 0;
  const readId = lastReadId === null ? await getUserNotificationReadId(name) : Math.max(0, Number(lastReadId) || 0);
  const result = await queryDbWithRetry(
    'SELECT COUNT(*)::int AS count FROM user_notifications WHERE user_name = $1 AND id > $2',
    [name, readId],
    { attempts: 2, label: 'USER NOTIFICATION UNREAD COUNT' }
  );
  return Math.max(0, Number(result.rows[0] && result.rows[0].count) || 0);
}

async function getUserNotificationHistory(userName, limit = USER_NOTIFICATION_LIMIT, afterId = 0) {
  const name = normalizeUserNotificationName(userName);
  const safeLimit = Math.max(1, Math.min(USER_NOTIFICATION_LIMIT, Number(limit) || USER_NOTIFICATION_LIMIT));
  const safeAfterId = Math.max(0, Math.floor(Number(afterId) || 0));
  if (!name) return { items: [], lastReadId: 0, unreadCount: 0, delta: safeAfterId > 0 };
  const result = await queryDbWithRetry(
    safeAfterId > 0
      ? `SELECT id, user_name, event_type, data, created_at
         FROM user_notifications
         WHERE user_name = $1 AND id > $2
         ORDER BY id DESC
         LIMIT $3`
      : `SELECT id, user_name, event_type, data, created_at
         FROM user_notifications
         WHERE user_name = $1
         ORDER BY id DESC
         LIMIT $2`,
    safeAfterId > 0 ? [name, safeAfterId, safeLimit] : [name, safeLimit],
    { attempts: 2, label: safeAfterId > 0 ? 'USER NOTIFICATION DELTA' : 'USER NOTIFICATION READ' }
  );
  const lastReadId = await getUserNotificationReadId(name);
  const unreadCount = await getUserNotificationUnreadCount(name, lastReadId);
  return {
    items: result.rows.map(serializeUserNotificationRow).filter(Boolean),
    lastReadId,
    unreadCount,
    delta: safeAfterId > 0
  };
}

function resolveKnownNotificationUserName(value) {
  const requested = normalizeUserNotificationName(value);
  if (!requested) return '';
  if (userDatabase[requested]) return requested;
  const lower = requested.toLowerCase();
  return Object.keys(userDatabase).find(name => name.toLowerCase() === lower) || '';
}

function getChatNotificationPreview(text) {
  return normalizeText(String(text || '')
    .replace(/<br\s*\/?\s*>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/\s+/g, ' '), '').slice(0, 240);
}

function extractChatMentionTargets(text, senderName) {
  const source = String(text || '');
  if (!source.includes('@')) return [];
  const lowerSource = source.toLowerCase();
  const senderLower = String(senderName || '').toLowerCase();
  const occupied = [];
  const found = [];
  const names = Object.keys(userDatabase)
    .filter(name => name && name.toLowerCase() !== senderLower)
    .sort((a, b) => b.length - a.length);
  const isBoundary = ch => !ch || /[\s.,!?;:()\[\]{}<>"'`]/.test(ch);

  names.forEach(name => {
    const needle = `@${name.toLowerCase()}`;
    let from = 0;
    while (from < lowerSource.length) {
      const index = lowerSource.indexOf(needle, from);
      if (index < 0) break;
      const end = index + needle.length;
      from = index + 1;
      if (!isBoundary(index > 0 ? lowerSource[index - 1] : '') || !isBoundary(lowerSource[end] || '')) continue;
      if (occupied.some(range => index < range[1] && end > range[0])) continue;
      occupied.push([index, end]);
      found.push(name);
      break;
    }
  });

  return found;
}

async function recordChatUserNotifications(message = {}) {
  const sender = normalizeUserNotificationName(message.user);
  if (!sender) return;
  const preview = getChatNotificationPreview(message.text || '');
  const messageAt = new Date(message.time).getTime();
  const messageId = String((Number.isFinite(messageAt) && messageAt > 0 ? messageAt : 0) || message.time || '');
  if (!messageId) return;

  const targets = new Map();
  const replyTarget = resolveKnownNotificationUserName(message.replyTo && message.replyTo.user);
  if (replyTarget && replyTarget.toLowerCase() !== sender.toLowerCase()) targets.set(replyTarget.toLowerCase(), { name: replyTarget, type: 'reply' });
  extractChatMentionTargets(message.text || '', sender).forEach(name => {
    const key = name.toLowerCase();
    if (!targets.has(key)) targets.set(key, { name, type: 'mention' });
  });

  if (!targets.size) return;
  await Promise.all(Array.from(targets.values()).map(target => recordUserNotification(target.name, target.type, {
    actor: sender,
    messageId,
    text: preview
  }, {
    dedupeKey: `chat:${messageId}:${target.name.toLowerCase()}`,
    at: Number.isFinite(messageAt) && messageAt > 0 ? messageAt : Date.now()
  })));
}

function getPs3FriendActivityTransition(previousStatus, currentStatus) {
  const previous = previousStatus && typeof previousStatus === 'object' ? previousStatus : null;
  const current = currentStatus && typeof currentStatus === 'object' ? currentStatus : null;
  const previousPlaying = previous && previous.status === 'playing';
  const currentPlaying = current && current.status === 'playing';
  const previousId = normalizeText(previous && previous.titleId, '').toUpperCase();
  const currentId = normalizeText(current && current.titleId, '').toUpperCase();
  const previousTitle = normalizeText(previous && previous.title, '');
  const currentTitle = normalizeText(current && current.title, '');

  if (currentPlaying && (!previousPlaying || currentId !== previousId || (!currentId && currentTitle !== previousTitle))) {
    return { type: 'playing', data: current };
  }
  if (previousPlaying && current && current.status === 'idle') {
    return { type: 'xmb', data: previous };
  }
  return null;
}

function disconnectUserSessions(name, eventName = 'user_kicked', payload = {}) {
  const isPasswordResetDisconnect = eventName === 'password_reset_by_admin';
  getSocketsByUserName(name).forEach(client => {
    if (isPasswordResetDisconnect) {
      client.__passwordResetRevoked = true;
      client.isAdmin = false;
      client.isModerator = false;
    }
    client.emit(eventName, payload);
    setTimeout(() => {
      if (client.connected) client.disconnect(true);
    }, isPasswordResetDisconnect ? 120 : 1200);
  });
}

async function upsertPresenceForSocket(socket, name) {
  if (!socket || !name) return;
  const shouldAnnouncePresence = socket.__presenceAnnounced !== true;
  const wasOnline = !!(userDatabase[name] && userDatabase[name].online === true);
  if (userDatabase[name]) {
    userDatabase[name].online = true;
    userDatabase[name].id = socket.id;
    userDatabase[name].lastSeen = Date.now();
  }
  await queryDbWithRetry(
    `INSERT INTO presence_sessions (socket_id, name, instance_id, connected_at, last_seen, data)
     VALUES ($1, $2, $3, NOW(), NOW(), $4)
     ON CONFLICT (socket_id) DO UPDATE SET name = $2, instance_id = $3, last_seen = NOW(), data = $4`,
    [socket.id, name, INSTANCE_ID, { role: getUserRole(name, userDatabase[name] || null) }],
    { attempts: 3, label: 'PRESENCE UPSERT' }
  );
  invalidateOnlineListCache('presence-upsert');
  if (shouldAnnouncePresence && userDatabase[name]) {
    socket.__presenceAnnounced = true;
    emitPresenceUpdate(name, userDatabase[name]);
    deferServerTask('PRESENCE ONLINE NOTIFY', () => notifyPresenceAcrossInstances(name, userDatabase[name]), 0);
    if (!wasOnline) deferServerTask('FRIEND ACTIVITY ONLINE', () => recordFriendActivity(name, 'online', {}, { at: Date.now() }), 0);
  }
}

async function syncPresenceOnlineFromDb() {
  const previousOnlineState = new Map(Object.entries(userDatabase).map(([username, user]) => [
    username,
    { online: user && user.online === true, lastSeen: Number(user && user.lastSeen || 0) }
  ]));

  const expiredRes = await queryDbWithRetry(`
    WITH expired AS (
      DELETE FROM presence_sessions
      WHERE last_seen < NOW() - INTERVAL '${PRESENCE_TTL_SECONDS} seconds'
      RETURNING name, last_seen
    )
    SELECT name, MAX(last_seen) AS last_seen
    FROM expired
    GROUP BY name
  `, [], { attempts: 2, label: 'PRESENCE EXPIRE' });

  const presenceRes = await queryDbWithRetry(`
    SELECT name,
           MAX(last_seen) AS last_seen,
           (ARRAY_AGG(socket_id ORDER BY last_seen DESC))[1] AS socket_id
    FROM presence_sessions
    GROUP BY name
  `, [], { attempts: 2, label: 'PRESENCE READ' });
  const activeNames = new Set(presenceRes.rows.map(row => row.name));
  const expiredOfflineRows = expiredRes.rows.filter(row => row && row.name && !activeNames.has(row.name));

  Object.entries(userDatabase).forEach(([, user]) => {
    user.online = false;
    if (!user.lastSeen) user.lastSeen = null;
  });

  expiredOfflineRows.forEach(row => {
    const username = row.name;
    if (!userDatabase[username]) return;
    const expiredLastSeen = row.last_seen ? new Date(row.last_seen).getTime() : Date.now();
    userDatabase[username].lastSeen = Math.max(Number(userDatabase[username].lastSeen || 0), expiredLastSeen);
    userCacheMeta[username] = Date.now();
  });

  presenceRes.rows.forEach(row => {
    const username = row.name;
    if (!userDatabase[username]) return;
    userDatabase[username].online = true;
    userDatabase[username].id = row.socket_id || userDatabase[username].id;
    userDatabase[username].lastSeen = row.last_seen ? new Date(row.last_seen).getTime() : Date.now();
  });

  if (expiredOfflineRows.length > 0) {
    const names = [];
    const lastSeenValues = [];
    expiredOfflineRows.forEach(row => {
      if (!userDatabase[row.name]) return;
      names.push(row.name);
      lastSeenValues.push(Number(userDatabase[row.name].lastSeen || Date.now()));
    });
    if (names.length > 0) {
      await queryDbWithRetry(
        `UPDATE users AS u
         SET data = COALESCE(u.data, '{}'::jsonb) || jsonb_build_object('online', false, 'lastSeen', v.last_seen)
         FROM UNNEST($1::text[], $2::bigint[]) AS v(name, last_seen)
         WHERE u.name = v.name`,
        [names, lastSeenValues],
        { attempts: 2, label: 'PRESENCE LAST SEEN SAVE' }
      );
    }
  }

  invalidateOnlineListCache("presence-sync");

  Object.entries(userDatabase).forEach(([username, user]) => {
    const previous = previousOnlineState.get(username);
    if (!previous) return;
    const onlineChanged = previous.online !== (user && user.online === true);
    const offlineLastSeenChanged = user && user.online !== true && Number(user.lastSeen || 0) > previous.lastSeen;
    if (!onlineChanged && !offlineLastSeenChanged) return;
    const payload = emitPresenceUpdate(username, user);
    if (payload) deferServerTask('PRESENCE STATE NOTIFY', () => notifyPresenceAcrossInstances(username, user), 0);
  });

  return userDatabase;
}

async function emitOnlineList(targetSocket = null, options = {}) {
  try {
    const list = await getSanitizedOnlineListFromDb(options);
    if (Array.isArray(list) && list.length > 0) lastKnownOnlineList = list;

    if (targetSocket) {
      if (MEMORY_TRACE_ENABLED) {
        const estimate = estimateValueBytes(list, { maxNodes: 20000, maxBytes: 8 * 1024 * 1024 });
        logMemoryTrace('online-list:send', `user=${targetSocket.userName || '-'} socket=${targetSocket.id} items=${Array.isArray(list) ? list.length : 0} approx=${formatApproxBytes(estimate.bytes)}${estimate.truncated ? '+' : ''} buffer=${getSocketWriteBufferLength(targetSocket)}`);
      }
      targetSocket.emit('online_list', list);
      return list;
    }

    const signature = buildOnlineListSignature(list);
    if (ONLINE_LIST_UNCHANGED_SKIP_ENABLED && options.force !== true && signature === lastBroadcastOnlineListSignature) {
      return list;
    }

    lastBroadcastOnlineListSignature = signature;
    io.emit('online_list', list);
    return list;
  } catch (err) {
    console.error('[PRESENCE SYNC ERROR]:', err);
    const fallback = Array.isArray(lastKnownOnlineList) ? lastKnownOnlineList : [];

    // Never broadcast a fake empty presence list after a temporary DB/reconnect hiccup.
    // Mobile browsers can resume before Postgres answers, and replacing everyone with
    // [] is what made the UI show "0 Online" until the next good refresh.
    if (targetSocket && fallback.length > 0) {
      targetSocket.emit('online_list', fallback);
    }
    return fallback;
  }
}

async function heartbeatPresenceSessions() {
  const activeSockets = [];
  io.sockets.sockets.forEach(client => {
    if (client.connected && client.userName) activeSockets.push(client);
  });

  if (activeSockets.length > 0) {
    const socketIds = [];
    const names = [];
    const instanceIds = [];
    const payloads = [];

    activeSockets.forEach(client => {
      socketIds.push(client.id);
      names.push(client.userName);
      instanceIds.push(INSTANCE_ID);
      payloads.push({ role: getUserRole(client.userName, userDatabase[client.userName] || null) });
      if (userDatabase[client.userName]) userDatabase[client.userName].lastSeen = Date.now();
    });

    await queryDbWithRetry(
      `INSERT INTO presence_sessions (socket_id, name, instance_id, connected_at, last_seen, data)
       SELECT socket_id, name, instance_id, NOW(), NOW(), data
       FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[]) AS t(socket_id, name, instance_id, data)
       ON CONFLICT (socket_id) DO UPDATE SET
         name = EXCLUDED.name,
         instance_id = EXCLUDED.instance_id,
         last_seen = NOW(),
         data = EXCLUDED.data`,
      [socketIds, names, instanceIds, payloads],
      { attempts: 2, label: 'PRESENCE HEARTBEAT SAVE' }
    );
    invalidateOnlineListCache("presence-heartbeat");
  }

  await syncPresenceOnlineFromDb();
  await emitOnlineList();
}

async function setUserRole(targetName, role, adminName) {
  if (!targetName) {
    return { success: false, message: "User not found." };
  }

  await getUserFromDb(targetName);
  if (!userDatabase[targetName]) {
    return { success: false, message: "User not found." };
  }

  const normalizedRole = role === "moderator" ? "mod" : normalizeText(role, "user").toLowerCase();
  if (!VALID_USER_ROLES.has(normalizedRole)) {
    return { success: false, message: "Invalid role. Use user, trusted, mod, or admin." };
  }

  if (ADMIN_USERS.includes(targetName) && normalizedRole !== "admin") {
    return { success: false, message: "Hardcoded admins cannot be demoted." };
  }

  userDatabase[targetName].role = normalizedRole;
  userDatabase[targetName].name = targetName;
  await saveUser(targetName);

  getSocketsByUserName(targetName).forEach(client => {
    client.isAdmin = isUserAdmin(targetName, userDatabase[targetName]);
    client.role = getUserRole(targetName, userDatabase[targetName]);
    client.emit('role_updated', {
      role: client.role,
      isAdmin: client.isAdmin,
      isModerator: client.role === 'mod',
      banned: isUserBanned(userDatabase[targetName])
    });
  });

  await emitOnlineList();
  return { success: true, role: getUserRole(targetName, userDatabase[targetName]), banned: isUserBanned(userDatabase[targetName]) };
}


function resolveCommandTarget(rawArgs = "", options = {}) {
  const args = normalizeText(rawArgs, "");
  if (!args) return { targetName: "", rest: "" };

  const allowOnlyBanned = options.onlyBanned === true;
  const withoutAt = args.startsWith('@') ? args.slice(1).trim() : args;
  const lowerArgs = withoutAt.toLowerCase();
  const names = Object.keys(userDatabase)
    .filter(name => !allowOnlyBanned || isUserBanned(userDatabase[name]))
    .sort((a, b) => b.length - a.length);

  for (const name of names) {
    const lowerName = name.toLowerCase();
    if (lowerArgs === lowerName || lowerArgs.startsWith(`${lowerName} `)) {
      return {
        targetName: name,
        rest: withoutAt.slice(name.length).trim()
      };
    }
  }

  const firstToken = withoutAt.split(/\s+/)[0] || "";
  const exact = names.find(name => name.toLowerCase() === firstToken.toLowerCase());
  if (exact) {
    return {
      targetName: exact,
      rest: withoutAt.slice(firstToken.length).trim()
    };
  }

  return { targetName: "", rest: withoutAt };
}

async function banUser(targetName, reason, adminName) {
  if (!targetName) return { success: false, message: "Missing target user." };
  await getUserFromDb(targetName);
  if (!userDatabase[targetName]) return { success: false, message: "User not found." };
  if (ADMIN_USERS.includes(targetName)) return { success: false, message: "Hardcoded admins cannot be banned." };

  userDatabase[targetName] = normalizeUserRecord(targetName, userDatabase[targetName]);
  userDatabase[targetName].banned = true;
  userDatabase[targetName].banReason = normalizeText(reason, "Banned by administrator") || "Banned by administrator";
  userDatabase[targetName].bannedBy = adminName || "Admin";
  userDatabase[targetName].bannedAt = new Date().toISOString();
  await saveUser(targetName);

  disconnectUserSessions(targetName, 'user_banned', { reason: userDatabase[targetName].banReason, by: adminName || 'Admin' });
  await emitOnlineList();
  return { success: true, targetName, reason: userDatabase[targetName].banReason };
}

async function unbanUser(targetName, adminName) {
  if (!targetName) return { success: false, message: "Missing target user." };
  await getUserFromDb(targetName);
  if (!userDatabase[targetName]) return { success: false, message: "User not found." };

  userDatabase[targetName] = normalizeUserRecord(targetName, userDatabase[targetName]);
  if (!isUserBanned(userDatabase[targetName])) {
    return { success: false, message: "User is not banned." };
  }

  userDatabase[targetName].banned = false;
  delete userDatabase[targetName].banReason;
  delete userDatabase[targetName].bannedBy;
  delete userDatabase[targetName].bannedAt;
  await saveUser(targetName);

  await emitOnlineList();
  return { success: true, targetName };
}

async function resetUserPassword(targetName, adminName) {
  if (!targetName) return { success: false, message: "Missing target user." };
  await getUserFromDb(targetName);
  if (!userDatabase[targetName]) return { success: false, message: "User not found." };

  const resetRequestedAt = Date.now();
  const resetExpiresAt = resetRequestedAt + PASSWORD_RESET_WINDOW_MS;
  userDatabase[targetName].passwordHash = null;
  delete userDatabase[targetName].password;
  userDatabase[targetName].passwordResetRequired = true;
  userDatabase[targetName].passwordResetAt = new Date(resetRequestedAt).toISOString();
  userDatabase[targetName].passwordResetExpiresAt = resetExpiresAt;
  userDatabase[targetName].passwordResetBy = adminName || "Admin";
  delete userDatabase[targetName].passwordResetCompletedAt;
  await saveUser(targetName);

  disconnectUserSessions(targetName, 'password_reset_by_admin', {
    targetName,
    by: adminName || 'Admin',
    resetAt: userDatabase[targetName].passwordResetAt,
    expiresAt: resetExpiresAt,
    expiresInMs: PASSWORD_RESET_WINDOW_MS
  });
  return { success: true, targetName, resetExpiresAt, expiresInMs: PASSWORD_RESET_WINDOW_MS };
}

async function deleteUserAccount(targetName, reason, adminName) {
  if (!targetName) return { success: false, message: "Missing target user." };
  await getUserFromDb(targetName);
  if (!userDatabase[targetName]) return { success: false, message: "User not found." };
  if (ADMIN_USERS.includes(targetName)) return { success: false, message: "Hardcoded admins cannot be deleted." };

  const deletedAt = new Date().toISOString();
  const deleteReason = normalizeText(reason, "Account deleted by administrator.") || "Account deleted by administrator.";
  const deletedData = {
    name: targetName,
    reason: deleteReason,
    deletedBy: adminName || "Admin",
    deletedAt
  };

  await pool.query(
    'INSERT INTO deleted_accounts (name, data, deleted_at) VALUES ($1, $2, NOW()) ON CONFLICT (name) DO UPDATE SET data = $2, deleted_at = NOW()',
    [targetName, deletedData]
  );
  await pool.query('DELETE FROM users WHERE name = $1', [targetName]);
  await pool.query('DELETE FROM friend_activity_read_state WHERE user_name = $1', [targetName]);
  await pool.query('DELETE FROM friend_activity WHERE actor_name = $1', [targetName]);
  await pool.query('DELETE FROM user_notification_read_state WHERE user_name = $1', [targetName]);
  await pool.query('DELETE FROM user_notifications WHERE user_name = $1', [targetName]);
  await notifyProfileSyncAcrossInstances(targetName, null, Date.now());

  delete userDatabase[targetName];
  delete userCacheMeta[targetName];
  fullUserCacheNames.delete(targetName);
  disconnectUserSessions(targetName, 'account_deleted', { reason: deleteReason, by: adminName || 'Admin' });
  await emitOnlineList();

  return { success: true, targetName, reason: deleteReason };
}

async function createReport(data = {}, reporterName = "Unknown") {
  const report = {
    id: normalizeText(data.id, `report-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`),
    reporter: normalizeText(data.reporter, reporterName),
    targetUser: normalizeText(data.targetUser || data.user, ""),
    msgId: data.msgId || null,
    reason: normalizeText(data.reason, "No reason provided."),
    messageText: normalizeText(data.messageText || data.text, ""),
    time: data.time || new Date().toISOString(),
    status: "open"
  };

  adminReports.unshift(report);
  adminReports = adminReports.slice(0, 100);

  await pool.query(
    'INSERT INTO reports (id, data, resolved) VALUES ($1, $2, false) ON CONFLICT (id) DO UPDATE SET data = $2, resolved = false',
    [report.id, report]
  );

  emitToAdmins('admin_report_created', report);
  return report;
}



const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e7
});

async function syncAdminStateAcrossInstances() {
  const previous = JSON.stringify(adminState);
  const adminConnected = hasAdminSockets();
  const previousServerLog = adminConnected ? JSON.stringify(serverLog) : "";

  await refreshAdminStateThrottled(8000);

  if (adminConnected) {
    await refreshServerLogFromDb();

    if (JSON.stringify(serverLog) !== previousServerLog) {
      emitToAdmins('admin_server_log_list', serverLog);
    }
  }

  if (JSON.stringify(adminState) === previous) return;

  io.emit('maintenance_mode', adminState.maintenance);
  io.emit('chat_controls', adminState.chatControls);
  io.emit('admin_pinned_announcement', adminState.pinnedAnnouncement || { clear: true });
  emitToAdmins('admin_state', {
    maintenance: adminState.maintenance,
    chatControls: adminState.chatControls,
    pinnedAnnouncement: adminState.pinnedAnnouncement || null,
    reports: adminConnected ? adminReports : [],
    serverLog: adminConnected ? serverLog : [],
    registeredUsers: Object.keys(userDatabase).length,
    countryStats: getAdminCountryStats()
  });
}

function deferServerTask(label, taskFn, delayMs = 0) {
  const run = () => Promise.resolve()
    .then(taskFn)
    .catch(err => console.error(`[${label} DEFERRED ERROR]:`, err));
  if (delayMs > 0) setTimeout(run, delayMs);
  else setImmediate(run);
}

function markSocketAuthenticated(socket) {
  if (socket) socket.__authenticatedAt = Date.now();
}

function getPostAuthRemainingDelay(socket, totalDelayMs) {
  const start = Number(socket && socket.__authenticatedAt || 0);
  if (!start) return 0;
  return Math.max(0, totalDelayMs - (Date.now() - start));
}


const syncAdminStateIntervalTask = runNonOverlappingTask('ADMIN STATE SYNC', syncAdminStateAcrossInstances);
const presenceHeartbeatIntervalTask = runNonOverlappingTask('PRESENCE HEARTBEAT', heartbeatPresenceSessions);
const chatPollIntervalTask = runNonOverlappingTask('CHAT POLL', syncChatAcrossInstances);
const profileSyncIntervalTask = runNonOverlappingTask('PROFILE SYNC', syncActiveProfilesAcrossInstances);

let backgroundTasksStarted = false;
function startBackgroundTasks() {
  if (backgroundTasksStarted) return;
  backgroundTasksStarted = true;
  setInterval(syncAdminStateIntervalTask, 15000);
  setInterval(presenceHeartbeatIntervalTask, PRESENCE_HEARTBEAT_MS);
  setInterval(chatPollIntervalTask, CHAT_SYNC_INTERVAL_MS);
  setInterval(() => logMemoryPressureIfNeeded('periodic'), 15000);
  if (ENABLE_PROFILE_PERIODIC_SYNC) {
    setInterval(profileSyncIntervalTask, PROFILE_SYNC_INTERVAL_MS);
  } else {
    console.log('[PROFILE SYNC] Periodic fallback disabled. Realtime LISTEN/NOTIFY remains enabled.');
  }
}

io.on('connection', (socket) => {
  console.log('[NETWORK] Socket connected. ID: ' + socket.id);
  deferServerTask('CONNECTION INIT', () => emitAdminState(socket), 0);

  socket.on('authenticate_user', async (data = {}) => {
    try {
      const { name, password, isNewAccount, adminMaintenanceBypass, passwordResetSubmission } = data;
      const safeUserData = (data.userData && typeof data.userData === 'object') ? data.userData : {};
      
      const supportsProfileSyncV2 = data && data.profileSyncV2 === true;
      const supportsProfileSyncAckV1 = data && data.profileSyncAckV1 === true;
      const supportsChatHistoryAckV1 = data && data.chatHistoryAckV1 === true;
      const supportsChatHistoryPullV1 = data && data.chatHistoryPullV1 === true;
      logMemoryTrace('auth:start', `user=${name || ''} socket=${socket.id} new=${isNewAccount === true} v2=${supportsProfileSyncV2} ack=${supportsProfileSyncAckV1} chatAck=${supportsChatHistoryAckV1} chatPull=${supportsChatHistoryPullV1}`);
      let dbUser = await getAuthUserRecordFromDb(name);
      if (dbUser) {
        const authEstimate = estimateValueBytes(dbUser, { maxNodes: 10000, maxBytes: 8 * 1024 * 1024 });
        logMemoryTrace('auth:record', `user=${name} socket=${socket.id} keys=${Object.keys(dbUser).length} approx=${formatApproxBytes(authEstimate.bytes)}${authEstimate.truncated ? '+' : ''}`);
      }
      let wasDeletedAccount = false;

      const isHardcodedAdmin = ADMIN_USERS.includes(name);
      const isAdmin = isUserAdmin(name, dbUser);

      if (!dbUser && !isHardcodedAdmin) {
        const deletedRes = await queryDbWithRetry('SELECT data FROM deleted_accounts WHERE name = $1', [name], { attempts: 3, label: 'AUTH DELETED ACCOUNT LOOKUP' });
        if (deletedRes.rows.length > 0) {
          wasDeletedAccount = true;
          if (isNewAccount !== true) {
            const deletedData = deletedRes.rows[0].data || {};
            const reason = normalizeText(deletedData.reason, 'This account was deleted by an administrator.');
            socket.emit('auth_error', `${reason} Use Create New Account again or choose another Online ID.`);
            return;
          }
        }
      }

      if (adminMaintenanceBypass === true && !isAdmin) {
        socket.emit('auth_error', 'Maintenance admin login rejected. This account is not an administrator.');
        return;
      }

      if (dbUser && isUserBanned({ ...dbUser, name }) && !isHardcodedAdmin) {
        socket.emit('auth_error', dbUser.banReason ? `This account is banned: ${dbUser.banReason}` : 'This account is banned.');
        return;
      }

      if (dbUser) {
        const resetRequired = dbUser.passwordResetRequired === true;
        const resetExpiresAt = Number(dbUser.passwordResetExpiresAt || 0);

        if (resetRequired) {
          if (!resetExpiresAt || resetExpiresAt <= Date.now()) {
            delete dbUser.passwordResetRequired;
            delete dbUser.passwordResetExpiresAt;
            dbUser.profileUpdatedAt = Date.now();
            await patchUserDataInternal(name, { profileUpdatedAt: dbUser.profileUpdatedAt }, ['passwordResetRequired', 'passwordResetExpiresAt'], 'PASSWORD RESET EXPIRED');
            socket.emit('password_reset_expired', {
              targetName: name,
              by: dbUser.passwordResetBy || 'Admin',
              resetAt: dbUser.passwordResetAt || null
            });
            return;
          }

          if (passwordResetSubmission !== true) {
            socket.emit('password_reset_required', {
              targetName: name,
              by: dbUser.passwordResetBy || 'Admin',
              resetAt: dbUser.passwordResetAt || null,
              expiresAt: resetExpiresAt,
              expiresInMs: Math.max(0, resetExpiresAt - Date.now())
            });
            return;
          }

          const nextPassword = String(password || '').trim();
          if (nextPassword.length < 4) {
            socket.emit('auth_error', 'New password is too short. Minimum 4 characters.');
            return;
          }

          dbUser.passwordHash = await bcrypt.hash(nextPassword, 10);
          delete dbUser.password;
          delete dbUser.passwordResetRequired;
          delete dbUser.passwordResetExpiresAt;
          dbUser.passwordResetCompletedAt = new Date().toISOString();
          dbUser.profileUpdatedAt = Date.now();
          await patchUserDataInternal(name, { passwordHash: dbUser.passwordHash, passwordResetCompletedAt: dbUser.passwordResetCompletedAt, profileUpdatedAt: dbUser.profileUpdatedAt }, ['password', 'passwordResetRequired', 'passwordResetExpiresAt'], 'PASSWORD RESET COMPLETE');
          console.log(`[AUTH] ${name} created a new password after an administrator reset.`);
        } else if (passwordResetSubmission === true) {
          socket.emit('auth_error', 'Password reset expired. Ask an administrator to authorize another reset.');
          return;
        }

        if (!dbUser.passwordHash) {
          if (isNewAccount === true) {
            socket.emit('auth_error', 'This Online ID is already taken...');
            return;
          }

          const legacyPassword = typeof dbUser.password === 'string' ? dbUser.password : '';
          if (!legacyPassword) {
            const resetRequestedAt = Date.parse(String(dbUser.passwordResetAt || ''));
            const resetCompletedAt = Date.parse(String(dbUser.passwordResetCompletedAt || ''));
            const hasUnresolvedAdminReset = Number.isFinite(resetRequestedAt)
              && (!Number.isFinite(resetCompletedAt) || resetCompletedAt < resetRequestedAt);
            if (hasUnresolvedAdminReset) {
              socket.emit('auth_error', 'Password reset expired. Ask an administrator to authorize another reset.');
              return;
            }
            const recoveryPassword = String(password || '').trim();
            if (recoveryPassword.length < 4) {
              socket.emit('auth_error', 'Enter a password with at least 4 characters to recover this account.');
              return;
            }
            dbUser.passwordHash = await bcrypt.hash(recoveryPassword, 10);
            dbUser.passwordRecoveredAt = new Date().toISOString();
            dbUser.passwordRecoverySource = 'missing_credentials_login';
            dbUser.profileUpdatedAt = Date.now();
            await patchUserDataInternal(name, { passwordHash: dbUser.passwordHash, passwordRecoveredAt: dbUser.passwordRecoveredAt, passwordRecoverySource: dbUser.passwordRecoverySource, profileUpdatedAt: dbUser.profileUpdatedAt }, [], 'MISSING CREDENTIAL RECOVERY');
            console.warn(`[AUTH] Rebuilt missing credentials for ${name} from a login password.`);
          } else {
            if (String(password || '') !== legacyPassword) {
              socket.emit('auth_error', 'Incorrect password. Access denied.');
              return;
            }

            dbUser.passwordHash = await bcrypt.hash(password, 10);
            delete dbUser.password;
            dbUser.passwordMigratedAt = new Date().toISOString();
            dbUser.profileUpdatedAt = Date.now();
            await patchUserDataInternal(name, { passwordHash: dbUser.passwordHash, passwordMigratedAt: dbUser.passwordMigratedAt, profileUpdatedAt: dbUser.profileUpdatedAt }, ['password'], 'LEGACY PASSWORD MIGRATION');
            console.log(`[AUTH] Migrated legacy password for ${name} to bcrypt.`);
          }
        }

        const match = await bcrypt.compare(password, dbUser.passwordHash);
        
        if (match) {
          socket.__passwordResetRevoked = false;
          socket.userName = name;
          socket.isAdmin = isAdmin;
          socket.role = getUserRole(name, dbUser);

          const serverUser = buildCompactUserSummary(name, dbUser);

          userDatabase[name] = {
            ...serverUser,
            online: true,
            id: socket.id,
            lastSeen: Date.now(),
            name: name,
            role: getUserRole(name, serverUser),
            banned: isUserBanned(serverUser),
            profileUpdatedAt: normalizeTimestampValue(serverUser.profileUpdatedAt)
          };
          USER_HEAVY_CACHE_KEYS.forEach(key => delete userDatabase[name][key]);
          userCacheMeta[name] = Date.now();
          fullUserCacheNames.delete(name);
          socket.profileSyncV2 = supportsProfileSyncV2;
          socket.profileSyncAckV1 = supportsProfileSyncAckV1;
          socket.chatHistoryAckV1 = supportsChatHistoryAckV1;
          socket.chatHistoryPullV1 = supportsChatHistoryPullV1;
          
          markSocketAuthenticated(socket);
          invalidateOnlineListCache('auth-existing-db');
          deferServerTask('AUTH EXISTING PRESENCE', () => upsertPresenceForSocket(socket, name), 250);

          console.log(`[NETWORK] ${name} logged in. Admin: ${isAdmin}`);
          logMemoryTrace('auth:accepted', `user=${name} socket=${socket.id} compactKeys=${Object.keys(userDatabase[name] || {}).length}`);
          deferServerTask('AUTH LOGIN LOG', async () => {
            await addServerLog('login', `${name} signed in${isAdmin ? ' as admin' : ''}`, { socketId: socket.id, role: getUserRole(name, userDatabase[name]) }, name);
          }, 2400);

          if (supportsProfileSyncV2) {
            socket.emit('auth_success', {
              name,
              userData: buildLightProfileUserData(name, userDatabase[name]),
              isAdmin: isAdmin,
              role: getUserRole(name, userDatabase[name]),
              isModerator: isUserModerator(name, userDatabase[name]),
              serverAuthoritative: true,
              lightAuth: true,
              fullProfileDeferred: true
            });
          } else {
            const fullAuthUser = await runSerializedProfileHydration(() => loadFullUserRecordTransient(name));
            if (!fullAuthUser) {
              socket.emit('auth_error', 'Server Error: Profile could not be loaded.');
              return;
            }
            socket.emit('auth_success', {
              name,
              userData: buildFullProfileSyncPayload(name, fullAuthUser, socket.id, { normalized: true }).userData,
              isAdmin: isAdmin,
              role: getUserRole(name, fullAuthUser),
              isModerator: isUserModerator(name, fullAuthUser),
              serverAuthoritative: true
            });
          }

          socket.emit('pinned_list', pinnedMessages);
          if (!supportsProfileSyncV2) deferServerTask('POST AUTH CHAT HISTORY', () => emitChatHistoryToSocket(socket), POST_AUTH_CHAT_HISTORY_DELAY_MS);
          deferServerTask('POST AUTH ADMIN STATE', () => emitAdminState(socket), socket.isAdmin === true ? POST_AUTH_ADMIN_STATE_DELAY_MS : 120);
          deferServerTask('POST AUTH ONLINE LIST', () => emitOnlineList(), POST_AUTH_ONLINE_LIST_DELAY_MS);
        } else {
          if (adminMaintenanceBypass === true) {
            socket.emit('auth_error', 'Incorrect admin password. Access denied.');
          } else if (isNewAccount) {
            socket.emit('auth_error', 'This Online ID is already taken...');
          } else {
            socket.emit('auth_error', 'Incorrect password. Access denied.');
          }
        }
      } else {
        const hash = await bcrypt.hash(password, 10);
        socket.__passwordResetRevoked = false;
        socket.userName = name;
        socket.isAdmin = isAdmin;

        userDatabase[name] = normalizeUserRecord(name, {
          ...safeUserData,
          name: name,
          passwordHash: hash,
          id: socket.id,
          online: true,
          lastSeen: Date.now(),
          avatar: safeUserData.avatar || DEFAULT_AVATAR,
          joined: safeUserData.joined || '2026',
          settingsData: normalizeProfileRealtimeSettings(safeUserData.settingsData || { audio: "1", ux: "1", cardBlur: "0", chatSound: "1", settingsUpdatedAt: Date.now(), profileCardStyle: "default", profileCardEffect: "default", ps3Ip: "", companionPlugin: "1", fpsCounterPlugin: "0", consoleFanMode: "dynamic", consoleFanSpeed: "35", consoleFanTarget: "68", performanceMode: "balanced", performanceRsx: "650", performanceVram: "850" }),
          trophiesData: safeUserData.trophiesData || {},
          wishlistData: safeUserData.wishlistData || [],
          favoritesData: safeUserData.favoritesData || [],
          downloadsData: Array.isArray(safeUserData.downloadsData) ? safeUserData.downloadsData : [],
          downloadsClearedAt: normalizeTimestampValue(safeUserData.downloadsClearedAt),
          downloadsUpdatedAt: normalizeTimestampValue(safeUserData.downloadsUpdatedAt),
          libraryData: safeUserData.libraryData || [],
          friendsData: safeUserData.friendsData || [],
          countersData: safeUserData.countersData || {},
          themeColor: safeUserData.themeColor || '#0070cc',
          role: isAdmin ? "admin" : "user",
          banned: false,
          migratedFromLocalProfile: isNewAccount !== true,
          migratedAt: new Date().toISOString(),
          profileUpdatedAt: Date.now()
        });
        socket.role = getUserRole(name, userDatabase[name]);
        socket.profileSyncV2 = supportsProfileSyncV2;
        socket.profileSyncAckV1 = supportsProfileSyncAckV1;
        socket.chatHistoryAckV1 = supportsChatHistoryAckV1;
        socket.chatHistoryPullV1 = supportsChatHistoryPullV1;
        normalizeProfileArrayPayloads(userDatabase[name]);
        userCacheMeta[name] = Date.now();
        fullUserCacheNames.delete(name);
        markSocketAuthenticated(socket);

        await pool.query(
          'INSERT INTO users (name, data) VALUES ($1, $2)',
          [name, userDatabase[name]]
        );
        invalidateOnlineListCache('auth-new-user');
        deferServerTask('AUTH NEW NOTIFY', () => notifyProfileSyncAcrossInstances(name, socket.id, userDatabase[name].profileUpdatedAt), 0);
        deferServerTask('AUTH NEW PRESENCE', () => upsertPresenceForSocket(socket, name), 0);
        if (wasDeletedAccount) {
          await pool.query('DELETE FROM deleted_accounts WHERE name = $1', [name]);
        }
        
        console.log(`[NETWORK] ${name} created a new account. Admin: ${isAdmin}`);
        logMemoryTrace('auth:created', `user=${name} socket=${socket.id}`);
        deferServerTask('AUTH SIGNUP LOG', async () => {
          await addServerLog('signup', `${name} created an account${isAdmin ? ' as admin' : ''}`, { socketId: socket.id, role: getUserRole(name, userDatabase[name]) }, name);
        }, 0);

        socket.emit('auth_success', {
          name,
          userData: supportsProfileSyncV2 ? buildLightProfileUserData(name, userDatabase[name]) : buildFullProfileSyncPayload(name, userDatabase[name], socket.id).userData,
          isAdmin: isAdmin,
          role: getUserRole(name, userDatabase[name]),
          isModerator: isUserModerator(name, userDatabase[name]),
          serverAuthoritative: true,
          ...(supportsProfileSyncV2 ? { lightAuth: true, fullProfileDeferred: true } : {})
        });
        compactCachedUser(name);

        socket.emit('pinned_list', pinnedMessages);
        if (!supportsProfileSyncV2) deferServerTask('POST AUTH CHAT HISTORY', () => emitChatHistoryToSocket(socket), POST_AUTH_CHAT_HISTORY_DELAY_MS);
        deferServerTask('POST AUTH ADMIN STATE', () => emitAdminState(socket), socket.isAdmin === true ? POST_AUTH_ADMIN_STATE_DELAY_MS : 120);
        deferServerTask('POST AUTH ONLINE LIST', () => emitOnlineList(), POST_AUTH_ONLINE_LIST_DELAY_MS);
      }
    } catch (error) {
      console.error("[AUTH ERROR]:", error);
      socket.emit('auth_error', 'Server Error: Auth failed.');
    }
  });


  socket.on('settings_realtime_update', async (payload = {}) => {
    const name = socket.userName;
    if (!name || !userDatabase[name]) return;

    const incomingSettingsData = (payload && payload.settingsData && typeof payload.settingsData === "object")
      ? { ...payload.settingsData }
      : ((payload && typeof payload === "object") ? { ...payload } : null);
    if (!incomingSettingsData || Array.isArray(incomingSettingsData)) return;

    const previousCountryCode = getUserCountryCode(userDatabase[name]);
    normalizeIncomingProfileCountry({}, incomingSettingsData);

    const incomingStamp = normalizeTimestampValue(
      incomingSettingsData.settingsUpdatedAt ||
      incomingSettingsData.settingsSyncedAt ||
      incomingSettingsData.settingsVersion ||
      payload.settingsUpdatedAt ||
      payload.clientSentAt
    ) || Date.now();
    incomingSettingsData.settingsUpdatedAt = incomingStamp;

    const mergedSettings = mergeProfileSettingsByTimestamp(userDatabase[name].settingsData || {}, incomingSettingsData, {
      currentFallback: normalizeTimestampValue(userDatabase[name].profileUpdatedAt),
      incomingFallback: incomingStamp
    });

    userDatabase[name].settingsData = mergedSettings.settingsData;
    const currentCountryCode = getUserCountryCode(userDatabase[name]);
    if (currentCountryCode) userDatabase[name].countryCode = currentCountryCode;
    const countryChanged = currentCountryCode !== previousCountryCode;
    const incomingThemePayload = {
      themeColor: payload && payload.themeColor,
      themeColorUpdatedAt: payload && (payload.themeColorUpdatedAt || payload.themeUpdatedAt)
    };
    const themeMerge = reconcileIncomingThemeColor(userDatabase[name], incomingThemePayload, incomingSettingsData);
    userDatabase[name].lastSeen = Date.now();
    userDatabase[name].profileUpdatedAt = Date.now();
    const settingsDbPatch = {
      settingsData: userDatabase[name].settingsData,
      lastSeen: userDatabase[name].lastSeen,
      profileUpdatedAt: userDatabase[name].profileUpdatedAt
    };
    if (currentCountryCode) settingsDbPatch.countryCode = currentCountryCode;
    if (userDatabase[name].themeColor) settingsDbPatch.themeColor = userDatabase[name].themeColor;
    if (userDatabase[name].themeColorUpdatedAt) settingsDbPatch.themeColorUpdatedAt = userDatabase[name].themeColorUpdatedAt;

    userProfileWriteInFlight.add(name);
    try {
      await upsertPresenceForSocket(socket, name);
      userCacheMeta[name] = Date.now();
      const savedUser = await patchUserData(name, settingsDbPatch, 'SETTINGS PATCH SAVE');
      if (!savedUser) return;
      invalidateOnlineListCache('settings-realtime-save');
    } catch (err) {
      console.error(`[DATABASE ERROR] Failed to save realtime settings for ${name}:`, err);
    } finally {
      userProfileWriteInFlight.delete(name);
    }

    const sourceSocketId = (mergedSettings.settingsRejected === true || mergedSettings.bannerRejected === true || themeMerge.rejected === true) ? null : socket.id;
    emitSettingsRealtimeSync(name, sourceSocketId, { reason: payload.reason || 'settings_realtime' });

    if (mergedSettings.bannerAccepted === true || themeMerge.accepted === true || countryChanged) {
      emitPublicProfileBannerUpdate(name, userDatabase[name]);
    }

    deferServerTask('SETTINGS PROFILE NOTIFY', () => notifyProfileSyncAcrossInstances(
      name,
      sourceSocketId,
      userDatabase[name].profileUpdatedAt,
      { keys: Object.keys(settingsDbPatch) }
    ), 0);
  });

  socket.on('update_profile', async (userData, ack) => {
    const respond = response => {
      if (typeof ack !== 'function' || !socket.connected) return;
      try { ack(response); } catch (err) {}
    };
    const name = socket.userName;
    if (!name || !userDatabase[name]) { respond({ ok: false, error: 'Profile is not available.' }); return; }
    userData = (userData && typeof userData === "object") ? { ...userData } : {};
    const syncRequestId = normalizeText(userData._syncRequestId, '');
    const syncReason = normalizeText(userData._syncReason, '');
    const requestedSyncSections = Array.isArray(userData._syncSections)
      ? [...new Set(userData._syncSections.map(value => normalizeText(value, '').toLowerCase()).filter(value => Object.prototype.hasOwnProperty.call(PROFILE_REPLAY_SECTION_DATA_KEYS, value)))]
      : [];
    delete userData._syncRequestId;
    delete userData._syncReason;
    delete userData._syncSections;
    let workingUser;
    try {
      workingUser = await buildWorkingUserForProfileUpdate(name, userData);
    } catch (err) {
      console.error(`[DATABASE ERROR] Failed to prepare profile for ${name}:`, err);
      respond({ ok: false, requestId: syncRequestId, error: 'Profile database read failed.' });
      return;
    }
    if (!workingUser) { respond({ ok: false, requestId: syncRequestId, error: 'Profile could not be prepared.' }); return; }
    const previousPs3StatusForActivity = workingUser.ps3Status && typeof workingUser.ps3Status === 'object' ? { ...workingUser.ps3Status } : null;
    const incomingPs3StatusForActivity = Object.prototype.hasOwnProperty.call(userData, 'ps3Status');
    const incomingSettingsData = (userData.settingsData && typeof userData.settingsData === "object") ? userData.settingsData : null;
    const previousCountryCode = name && userDatabase[name] ? getUserCountryCode(workingUser) : "";
    normalizeIncomingProfileCountry(userData, incomingSettingsData);
    let shouldBroadcastProfileBanner = false;
    let shouldForceProfileSyncToSource = false;
    let profileThemeMerge = { accepted: false, rejected: false };
    const shouldEmitTrendingUpdate = profileUpdateTouchesTrending(userData || {});
    if (name && userDatabase[name]) {
        
        if (incomingSettingsData) {
            const mergedSettings = mergeProfileSettingsByTimestamp(workingUser.settingsData || {}, incomingSettingsData, {
                currentFallback: normalizeTimestampValue(workingUser.profileUpdatedAt),
                incomingFallback: normalizeTimestampValue(userData.profileCardStyleUpdatedAt || userData.profileUpdatedAt)
            });
            workingUser.settingsData = mergedSettings.settingsData;
            profileThemeMerge = reconcileIncomingThemeColor(workingUser, userData, incomingSettingsData || {});
            shouldBroadcastProfileBanner = mergedSettings.bannerAccepted === true || profileThemeMerge.accepted === true;
            shouldForceProfileSyncToSource = mergedSettings.settingsRejected === true || mergedSettings.bannerRejected === true || profileThemeMerge.rejected === true;
            delete userData.settingsData;
            delete userData.themeColor;
            delete userData.themeColorUpdatedAt;
            delete userData.themeUpdatedAt;
        } else {
            profileThemeMerge = reconcileIncomingThemeColor(workingUser, userData, {});
            shouldBroadcastProfileBanner = profileThemeMerge.accepted === true;
            shouldForceProfileSyncToSource = profileThemeMerge.rejected === true;
            delete userData.themeColor;
            delete userData.themeColorUpdatedAt;
            delete userData.themeUpdatedAt;
        }

        if (userData.avatar === null || userData.avatar === undefined) {
            delete userData.avatar;
        }

        if (socket.isAdmin !== true) {
            delete userData.role;
            delete userData.banned;
            delete userData.banReason;
            delete userData.bannedBy;
            delete userData.bannedAt;
            delete userData.passwordHash;
            delete userData.password;
            delete userData.passwordResetAt;
            delete userData.passwordResetBy;
            delete userData.passwordResetRequired;
            delete userData.passwordResetExpiresAt;
            delete userData.passwordResetCompletedAt;
            delete userData.passwordRecoveredAt;
            delete userData.passwordRecoverySource;
        }

        if (isUserBanned(workingUser) && !ADMIN_USERS.includes(name)) {
            socket.emit('auth_error', 'This account is banned.');
            respond({ ok: false, requestId: syncRequestId, error: 'This account is banned.' });
            return;
        }

        if (hasObjectPayload(userData.trophiesData)) {
            if (!shouldAcceptIncomingTrophies(workingUser, userData)) {
                delete userData.trophiesData;
                delete userData.trophies;
                delete userData.level;
                delete userData.xp;
            } else {
                userData.trophiesData = mergeIncomingTrophiesPreservingUnlockState(workingUser.trophiesData, userData.trophiesData);
                userData.trophies = countUnlockedTrophiesPayload(userData.trophiesData);
            }
        }

        userData = reconcileIncomingDownloads(workingUser, userData || {});
        userData = reconcileIncomingProfileArrays(workingUser, userData || {});
        const replaySectionStatus = {};
        requestedSyncSections.forEach(section => {
          const dataKey = PROFILE_REPLAY_SECTION_DATA_KEYS[section];
          replaySectionStatus[section] = dataKey && Object.prototype.hasOwnProperty.call(userData, dataKey) ? 'accepted' : 'rejected';
        });
        if (requestedSyncSections.some(section => replaySectionStatus[section] === 'rejected')) shouldForceProfileSyncToSource = true;
        const publicCountsChanged = profileUpdateTouchesPublicCounts(userData);
        
        Object.assign(workingUser, userData);
        const currentCountryCode = getUserCountryCode(workingUser);
        if (currentCountryCode) {
            workingUser.countryCode = currentCountryCode;
            workingUser.settingsData = workingUser.settingsData && typeof workingUser.settingsData === "object" && !Array.isArray(workingUser.settingsData)
                ? { ...workingUser.settingsData, countryCode: currentCountryCode }
                : { countryCode: currentCountryCode };
        }
        const countryChanged = currentCountryCode !== previousCountryCode;
        if (countryChanged) shouldBroadcastProfileBanner = true;
        if (Array.isArray(workingUser.downloadsData)) workingUser.downloads = workingUser.downloadsData.length;
        workingUser.downloadsClearedAt = normalizeTimestampValue(workingUser.downloadsClearedAt);
        Object.entries(PROFILE_ARRAY_SYNC_KEYS).forEach(([key, sync]) => {
          if (!Object.prototype.hasOwnProperty.call(userData, key)) return;
          const list = normalizeProfileArrayListServer(key, workingUser[key]);
          workingUser[key] = list;
          workingUser[sync.countKey] = list.length;
          workingUser[sync.versionKey] = normalizeTimestampValue(workingUser[sync.versionKey]);
        });
        workingUser.lastSeen = Date.now();
        workingUser.profileUpdatedAt = Date.now();

        const profileDbPatch = {};
        Object.keys(userData).forEach(key => {
            if (Object.prototype.hasOwnProperty.call(workingUser, key)) profileDbPatch[key] = workingUser[key];
        });
        if (incomingSettingsData) profileDbPatch.settingsData = workingUser.settingsData;
        if (currentCountryCode && (countryChanged || Object.prototype.hasOwnProperty.call(userData, 'countryCode'))) {
            profileDbPatch.countryCode = currentCountryCode;
            profileDbPatch.settingsData = workingUser.settingsData;
        }
        if (profileThemeMerge.accepted === true || profileThemeMerge.rejected === true || Object.prototype.hasOwnProperty.call(userData, 'themeColor')) {
            profileDbPatch.themeColor = workingUser.themeColor;
            profileDbPatch.themeColorUpdatedAt = workingUser.themeColorUpdatedAt;
            profileDbPatch.settingsData = workingUser.settingsData;
        }
        profileDbPatch.lastSeen = workingUser.lastSeen;
        profileDbPatch.profileUpdatedAt = workingUser.profileUpdatedAt;

        userProfileWriteInFlight.add(name);
        try {
            await upsertPresenceForSocket(socket, name);
            userCacheMeta[name] = Date.now();
            const savedUser = await patchUserData(name, profileDbPatch, 'PROFILE PATCH SAVE');
            if (!savedUser) {
              respond({ ok: false, requestId: syncRequestId, error: 'Profile was not saved.' });
              return;
            }
            invalidateOnlineListCache('profile-update-save');
        } catch (err) {
            console.error(`[DATABASE ERROR] Failed to save profile for ${name}:`, err);
            respond({ ok: false, requestId: syncRequestId, error: 'Profile database save failed.' });
            return;
        } finally {
            userProfileWriteInFlight.delete(name);
        }

        updateCompactUserCacheFromPatch(name, workingUser, profileDbPatch);
        if (socket.__friendActivitySubscribed && Object.prototype.hasOwnProperty.call(profileDbPatch, 'friendsData')) {
          setFriendActivitySubscription(socket, workingUser.friendsData || []);
        }
        if (incomingPs3StatusForActivity) {
          const activityTransition = getPs3FriendActivityTransition(previousPs3StatusForActivity, workingUser.ps3Status);
          if (activityTransition) {
            deferServerTask('FRIEND ACTIVITY PS3', () => recordFriendActivity(name, activityTransition.type, activityTransition.data, { at: workingUser.profileUpdatedAt }), 0);
          }
        }
        if (publicCountsChanged) emitProfileCountsUpdate(name, userDatabase[name]);

        if (shouldEmitTrendingUpdate) {
            invalidateTrendingCache();
            scheduleTrendingRefreshBroadcast(900);
        }

        deferServerTask('PROFILE ONLINE LIST', () => emitOnlineList(), 450);
        if (shouldBroadcastProfileBanner) {
            emitPublicProfileBannerUpdate(name, workingUser);
        }
        const profileChangedKeys = Object.keys(profileDbPatch);
        emitProfileSyncPatchFromUser(name, workingUser, profileChangedKeys, shouldForceProfileSyncToSource ? null : socket.id);
        const trophiesChanged = !!userData.trophiesData;
        deferServerTask('PROFILE NOTIFY', () => notifyProfileSyncAcrossInstances(
            name,
            shouldForceProfileSyncToSource ? null : socket.id,
            workingUser.profileUpdatedAt,
            { trending: shouldEmitTrendingUpdate, trophies: trophiesChanged, counts: publicCountsChanged, keys: profileChangedKeys }
        ), 0);

        if (trophiesChanged) {
            invalidateGlobalTrophyStatsCache();
            scheduleTrophyStatsRefreshBroadcast(900);
        }

        const acceptedSections = requestedSyncSections.filter(section => replaySectionStatus[section] === 'accepted');
        const rejectedSections = requestedSyncSections.filter(section => replaySectionStatus[section] === 'rejected');
        respond({
          ok: true,
          requestId: syncRequestId,
          reason: syncReason || undefined,
          acceptedSections,
          rejectedSections,
          profileUpdatedAt: normalizeTimestampValue(workingUser.profileUpdatedAt) || Date.now(),
          versions: {
            downloads: normalizeTimestampValue(workingUser.downloadsUpdatedAt),
            wishlist: normalizeTimestampValue(workingUser.wishlistUpdatedAt),
            favorites: normalizeTimestampValue(workingUser.favoritesUpdatedAt),
            library: normalizeTimestampValue(workingUser.libraryUpdatedAt),
            friends: normalizeTimestampValue(workingUser.friendsUpdatedAt),
            recentlyVisited: normalizeTimestampValue(workingUser.recentlyVisitedUpdatedAt)
          }
        });
    }
  });

  socket.on('request_user_notifications', async (data = {}, ack) => {
    const respond = payload => { if (typeof ack === 'function' && socket.connected) { try { ack(payload); } catch (err) {} } };
    const name = socket.userName;
    if (!name || !userDatabase[name]) { respond({ ok: false, items: [], unreadCount: 0, error: 'Profile is not available.' }); return; }
    try {
      const result = await getUserNotificationHistory(name, data && data.limit, data && data.afterId);
      respond({
        ok: true,
        items: result.items,
        lastReadId: result.lastReadId || 0,
        unreadCount: result.unreadCount || 0,
        delta: result.delta === true
      });
    } catch (err) {
      console.error('[USER NOTIFICATION READ ERROR]:', err && err.message ? err.message : err);
      respond({ ok: false, items: [], unreadCount: 0, error: 'Notifications are temporarily unavailable.' });
    }
  });

  socket.on('user_notifications_mark_read', async (data = {}, ack) => {
    const respond = payload => { if (typeof ack === 'function' && socket.connected) { try { ack(payload); } catch (err) {} } };
    const name = socket.userName;
    if (!name || !userDatabase[name]) { respond({ ok: false, lastReadId: 0, unreadCount: 0 }); return; }
    try {
      const lastReadId = await setUserNotificationReadId(name, data && data.lastReadId);
      const unreadCount = await getUserNotificationUnreadCount(name, lastReadId);
      emitUserNotificationReadStateToLocalUser(name, lastReadId, unreadCount);
      deferServerTask('USER NOTIFICATION READ SYNC', () => notifyUserNotificationReadStateAcrossInstances(name, lastReadId, unreadCount), 0);
      respond({ ok: true, lastReadId, unreadCount });
    } catch (err) {
      console.error('[USER NOTIFICATION MARK READ ERROR]:', err && err.message ? err.message : err);
      respond({ ok: false, lastReadId: 0, unreadCount: 0 });
    }
  });

  socket.on('request_friend_activity', async (data = {}, ack) => {
    const respond = payload => { if (typeof ack === 'function' && socket.connected) { try { ack(payload); } catch (err) {} } };
    const name = socket.userName;
    if (!name || !userDatabase[name]) { respond({ ok: false, items: [], error: 'Profile is not available.' }); return; }
    const requestToken = (Number(socket.__friendActivityRequestToken) || 0) + 1;
    socket.__friendActivityRequestToken = requestToken;
    try {
      const friendNames = extractFriendActivityNames(await getUserDataPayloadFromDb(name, 'friends'));
      if (!socket.connected || socket.__friendActivityRequestToken !== requestToken) return;
      setFriendActivitySubscription(socket, friendNames);
      const result = await getFriendActivityHistoryForUser(name, data && data.limit, data && data.afterId, friendNames);
      if (!socket.connected || socket.__friendActivityRequestToken !== requestToken) return;
      respond({
        ok: true,
        items: result.items,
        friendCount: result.friendNames.length,
        lastReadId: result.lastReadId || 0,
        delta: result.delta === true
      });
    } catch (err) {
      if (socket.__friendActivityRequestToken !== requestToken) return;
      console.error('[FRIEND ACTIVITY READ ERROR]:', err && err.message ? err.message : err);
      respond({ ok: false, items: [], error: 'Friend activity is temporarily unavailable.' });
    }
  });

  socket.on('friend_activity_mark_read', async (data = {}, ack) => {
    const respond = payload => { if (typeof ack === 'function' && socket.connected) { try { ack(payload); } catch (err) {} } };
    const name = socket.userName;
    if (!name || !userDatabase[name]) { respond({ ok: false, lastReadId: 0 }); return; }
    try {
      const lastReadId = await setFriendActivityReadId(name, data && data.lastReadId);
      getSocketsByUserName(name).forEach(client => {
        if (client.connected) client.emit('friend_activity_read_state', { lastReadId });
      });
      respond({ ok: true, lastReadId });
    } catch (err) {
      console.error('[FRIEND ACTIVITY MARK READ ERROR]:', err && err.message ? err.message : err);
      respond({ ok: false, lastReadId: 0 });
    }
  });

  socket.on('friend_activity_trophy', (data = {}) => {
    const name = socket.userName;
    if (!name || !userDatabase[name]) return;
    const trophyId = normalizeText(data.trophyId, '').slice(0, 80);
    const title = normalizeText(data.title, 'Trophy').slice(0, 140);
    const trophyType = normalizeText(data.trophyType, '').toLowerCase();
    if (!trophyId || !['bronze', 'silver', 'gold', 'platinum'].includes(trophyType)) return;
    deferServerTask('TROPHY ACTIVITY AND NOTIFICATION', async () => {
      await Promise.all([
        recordFriendActivity(name, 'trophy', { trophyId, title, trophyType }, { dedupeKey: `${name.toLowerCase()}:trophy:${trophyId.toLowerCase()}` }),
        recordUserNotification(name, 'trophy', { trophyId, title, trophyType }, { dedupeKey: `${name.toLowerCase()}:notification:trophy:${trophyId.toLowerCase()}` })
      ]);
    }, 0);
  });

  socket.on('ps3_playtime_update', (data = {}) => {
    const name = socket.userName;
    if (!name || !userDatabase[name] || userDatabase[name].online !== true) return;

    const currentStatus = userDatabase[name].ps3Status;
    if (!currentStatus || currentStatus.status !== 'playing') return;

    const playTime = normalizePs3PlayTimeServer(data.playTime);
    if (!playTime) return;

    const incomingTitleId = normalizeText(data.titleId, '').toUpperCase();
    const currentTitleId = normalizeText(currentStatus.titleId, '').toUpperCase();
    if (incomingTitleId && currentTitleId && incomingTitleId !== currentTitleId) return;

    const now = Date.now();
    const previousAt = normalizeTimestampValue(currentStatus.playTimeUpdatedAt);
    const previousPlayTime = normalizePs3PlayTimeServer(currentStatus.playTime);

    if (previousPlayTime === playTime && previousAt && now - previousAt < 45000) return;

    userDatabase[name].ps3Status = {
      ...currentStatus,
      playTime,
      playTimeUpdatedAt: now
    };
    invalidateOnlineListCache('ps3-playtime-update');

    const payload = {
      name,
      titleId: currentTitleId || incomingTitleId,
      playTime,
      playTimeUpdatedAt: now
    };

    socket.broadcast.emit('ps3_playtime_update', payload);
    deferServerTask('PS3 PLAYTIME NOTIFY', () => notifyPs3PlayTimeAcrossInstances(payload), 0);
  });

  socket.on('request_user_data', async (data = {}) => {
    const { targetName, type, requestId } = data;
    try {
      let rawData = fullUserCacheNames.has(targetName) ? getUserDataPayloadFromCache(targetName, type) : null;

      if (rawData === null) {
        rawData = await withTimeout(
          getUserDataPayloadFromDb(targetName, type),
          4500,
          null
        );
        if (rawData === null) rawData = getEmptyUserDataPayload(type);
      }

      socket.emit('user_data_response', { targetName, type, requestId, rawData });
    } catch (err) {
      console.error('[REQUEST USER DATA CACHE ERROR]:', err);
      socket.emit('user_data_response', {
        targetName,
        type,
        requestId,
        rawData: getEmptyUserDataPayload(type),
        error: 'Unable to load this list from the server cache.'
      });
    }
  });

  socket.on('request_profile_sync', async (data = {}, ack) => {
    const name = socket.userName;
    if (!name || !userDatabase[name]) {
      if (typeof ack === 'function') ack({ ok: false, error: 'Profile is not available.' });
      return;
    }

    const wantsCriticalReadyAck = socket.profileSyncV2 === true && data && data.criticalReadyAck === true;
    let ackSent = false;
    const sendAck = response => {
      if (ackSent || typeof ack !== 'function' || !socket.connected) return;
      ackSent = true;
      try { ack(response); } catch (err) {}
    };

    if (socket.__profileSyncRequestInFlight) {
      logMemoryTrace('profile-sync:request:dedupe', `user=${name} socket=${socket.id} reason=${normalizeText(data && data.reason, 'duplicate')}`);
      if (wantsCriticalReadyAck && socket.__profileSyncCriticalReady === true) {
        sendAck({
          ok: true,
          criticalReady: true,
          criticalSection: 'trophiesData',
          profileSyncComplete: false,
          profileUpdatedAt: normalizeTimestampValue(userDatabase[name] && userDatabase[name].profileUpdatedAt)
        });
        return;
      }
      const existingResult = await socket.__profileSyncRequestInFlight.catch(() => ({ ok: false, error: 'Profile synchronization failed.' }));
      sendAck(existingResult);
      return;
    }

    socket.__profileSyncCriticalReady = false;

    const runRequest = async () => {
      const remainingDelay = (data && data.forceRefresh === true) ? 0 : getPostAuthRemainingDelay(socket, POST_AUTH_PROFILE_SYNC_DELAY_MS);
      if (remainingDelay > 0) await waitMs(remainingDelay);
      if (!socket.connected) return { ok: false, cancelled: true, error: 'Socket disconnected.' };

      try {
        if (socket.profileSyncV2 === true) {
          await emitChunkedProfileSyncToSocket(socket, name, {
            forceRefresh: data && data.forceRefresh === true,
            onCriticalReady: info => {
              socket.__profileSyncCriticalReady = true;
              logMemoryTrace('profile-sync:critical-ready', `user=${name} socket=${socket.id} section=${info && info.section || 'trophiesData'}`);
              if (wantsCriticalReadyAck) {
                sendAck({
                  ok: true,
                  criticalReady: true,
                  criticalSection: info && info.section || 'trophiesData',
                  profileSyncComplete: false,
                  profileUpdatedAt: normalizeTimestampValue(info && info.profileUpdatedAt) || normalizeTimestampValue(userDatabase[name] && userDatabase[name].profileUpdatedAt)
                });
              }
            }
          });
          if (socket.connected) {
            socket.emit('profile_sync_complete', {
              name,
              ok: true,
              profileUpdatedAt: normalizeTimestampValue(userDatabase[name] && userDatabase[name].profileUpdatedAt)
            });
          }
        } else {
          const fullUser = await runSerializedProfileHydration(() => loadFullUserRecordTransient(name));
          if (!fullUser || !socket.connected) throw new Error('Profile could not be loaded.');
          socket.emit('profile_sync', buildFullProfileSyncPayload(name, fullUser, null, { normalized: true }));
          compactCachedUser(name);
        }
        return {
          ok: true,
          profileSyncComplete: true,
          profileUpdatedAt: normalizeTimestampValue(userDatabase[name] && userDatabase[name].profileUpdatedAt)
        };
      } catch (err) {
        const message = String(err && err.message || err || '');
        const expectedDisconnect = !socket.connected || /socket disconnected/i.test(message);
        if (!expectedDisconnect) console.error(`[REQUEST PROFILE SYNC ERROR] user=${name} socket=${socket.id}:`, err);
        else logMemoryTrace('profile-sync:cancel', `user=${name} socket=${socket.id} reason=disconnect`);
        if (socket.connected && socket.profileSyncV2 === true) {
          socket.emit('profile_sync_complete', {
            name,
            ok: false,
            error: 'Profile synchronization failed.'
          });
        }
        return { ok: false, cancelled: expectedDisconnect, error: 'Profile synchronization failed.' };
      }
    };

    socket.__profileSyncRequestInFlight = runRequest().finally(() => {
      socket.__profileSyncRequestInFlight = null;
      socket.__profileSyncCriticalReady = false;
    });

    const result = await socket.__profileSyncRequestInFlight;
    sendAck(result);
  });

  socket.on('request_online_list', async () => {
    const sendOnlineList = async () => {
      if (socket.userName && userDatabase[socket.userName]) {
        const currentUser = userDatabase[socket.userName];
        const presenceChanged = currentUser.online !== true || currentUser.id !== socket.id;
        currentUser.online = true;
        currentUser.id = socket.id;
        currentUser.lastSeen = Date.now();
        if (presenceChanged) invalidateOnlineListCache('request-online-local-presence');
      }
      await emitOnlineList(socket);
    };

    try {
      const remaining = getPostAuthRemainingDelay(socket, POST_AUTH_ONLINE_LIST_DELAY_MS);
      if (remaining > 0) {
        deferServerTask('REQUEST ONLINE LIST', sendOnlineList, remaining);
        return;
      }
      await sendOnlineList();
    } catch (err) {
      console.error('[REQUEST ONLINE LIST ERROR]:', err);
      if (Array.isArray(lastKnownOnlineList) && lastKnownOnlineList.length > 0) {
        socket.emit('online_list', lastKnownOnlineList);
      }
    }
  });

  socket.on('chat_render_error', (data = {}) => {
    try {
      const stage = normalizeText(data.stage, 'unknown').slice(0, 40);
      const index = Number.isFinite(Number(data.index)) ? Number(data.index) : -1;
      const msgId = normalizeText(data.msgId, '').slice(0, 80);
      const message = normalizeText(data.message, '').slice(0, 500);
      console.error(`[CHAT RENDER CLIENT ERROR] user=${socket.userName || '-'} socket=${socket.id} stage=${stage} index=${index} msgId=${msgId} error=${message}`);
    } catch (err) {
      console.error('[CHAT RENDER CLIENT ERROR] Could not format client render diagnostic.');
    }
  });

  socket.on('request_chat_sync', async (request = {}, respond) => {
    const reply = typeof respond === 'function' ? respond : () => {};
    try {
      if (!socket.userName) return reply({ success: false, syncV1: true, error: 'Not authenticated.' });
      if (socket.__chatSyncRequestInFlight) return reply({ success: false, syncV1: true, busy: true, error: 'Chat synchronization already running.' });

      const clientEpoch = normalizeText(request && request.epoch, '').slice(0, 160);
      const clientRevision = Math.max(0, Number(request && request.revision) || 0);
      const clientSeenCursor = Math.max(0, Number(request && request.seenCursor) || 0);
      const initialFullSync = request && request.initialFullSync === true && !clientEpoch && clientRevision === 0;
      if (initialFullSync) socket.__chatInitialFullSyncInFlight = true;

      socket.__chatSyncRequestInFlight = (async () => {
        if (initialFullSync) {
          const stableSnapshot = await getStableInitialChatSyncForUser(socket.userName, clientSeenCursor);
          if (!stableSnapshot) return { success: false, syncV1: true, retry: true, error: 'Initial chat snapshot did not reach a stable revision.' };
          return stableSnapshot;
        }

        const state = await refreshChatSyncStateFromDb();
        const forceSnapshot = request && request.forceSnapshot === true;
        const seenSync = await getChatSeenSyncForUser(socket.userName, clientSeenCursor);

        const withSeenSync = payload => ({
          ...payload,
          seenSyncV1: true,
          seenMode: seenSync.mode,
          seenCursor: seenSync.cursor,
          seenEvents: seenSync.events
        });

        const sendSnapshot = async reason => {
          const snapshot = await getChatSyncSnapshotForUser(socket.userName);
          const messages = Array.isArray(snapshot && snapshot.messages) ? snapshot.messages : [];
          const snapshotExpectedCount = Math.max(0, Number(snapshot && snapshot.expectedCount) || 0);
          const stateAfterSnapshot = await refreshChatSyncStateFromDb();
          return withSeenSync({
            success: true,
            syncV1: true,
            mode: 'snapshot',
            reason,
            epoch: state.epoch,
            revision: state.revision,
            messages,
            snapshotCount: messages.length,
            snapshotExpectedCount,
            snapshotComplete: messages.length === snapshotExpectedCount,
            serverEpochAfterSnapshot: stateAfterSnapshot.epoch,
            serverRevisionAfterSnapshot: stateAfterSnapshot.revision,
            maxHistory: MAX_CHAT_HISTORY
          });
        };

        if (forceSnapshot || !clientEpoch || clientEpoch !== state.epoch || clientRevision > state.revision) {
          return sendSnapshot(forceSnapshot ? 'forced' : (!clientEpoch ? 'no-cache' : 'epoch-mismatch'));
        }

        if (clientRevision === state.revision) {
          return withSeenSync({
            success: true,
            syncV1: true,
            mode: 'changes',
            epoch: state.epoch,
            revision: state.revision,
            changes: [],
            maxHistory: MAX_CHAT_HISTORY
          });
        }

        const changesRes = await queryDbWithRetry(
          'SELECT revision, change_type, message_id, message FROM chat_changes WHERE epoch = $1 AND revision > $2 ORDER BY revision ASC LIMIT $3',
          [state.epoch, clientRevision, CHAT_SYNC_MAX_DELTA + 1],
          { attempts: 2, label: 'CHAT SYNC DELTA READ' }
        );
        const rows = changesRes.rows;
        const contiguous = rows.length > 0
          && Number(rows[0].revision) === clientRevision + 1
          && Number(rows[Math.min(rows.length, CHAT_SYNC_MAX_DELTA) - 1].revision) >= Math.min(state.revision, clientRevision + CHAT_SYNC_MAX_DELTA);

        if (!contiguous || rows.length > CHAT_SYNC_MAX_DELTA || Number(rows[rows.length - 1]?.revision || 0) !== state.revision) {
          return sendSnapshot(!contiguous ? 'delta-gap' : 'delta-too-large');
        }

        return withSeenSync({
          success: true,
          syncV1: true,
          mode: 'changes',
          epoch: state.epoch,
          revision: state.revision,
          changes: rows.map(row => sanitizeChatSyncChangeForUser(row, socket.userName)),
          maxHistory: MAX_CHAT_HISTORY
        });
      })();

      const result = await socket.__chatSyncRequestInFlight;
      reply(result);
    } catch (err) {
      console.error('[REQUEST CHAT SYNC ERROR]:', err);
      reply({ success: false, syncV1: true, error: 'Chat synchronization failed.' });
    } finally {
      socket.__chatSyncRequestInFlight = null;
      socket.__chatInitialFullSyncInFlight = false;
    }
  });

  socket.on('request_chat_history', async () => {
    try {
      if (!socket.userName) return;
      if (socket.__chatHistoryRequestInFlight) return socket.__chatHistoryRequestInFlight;
      const now = Date.now();
      if (socket.__lastChatHistoryRequestAt && now - socket.__lastChatHistoryRequestAt < 900) return;
      socket.__lastChatHistoryRequestAt = now;
      socket.__chatHistoryRequestInFlight = emitChatHistoryToSocket(socket).finally(() => { socket.__chatHistoryRequestInFlight = null; });
      await socket.__chatHistoryRequestInFlight;
    } catch (err) {
      socket.__chatHistoryRequestInFlight = null;
      console.error('[REQUEST CHAT HISTORY ERROR]:', err);
    }
  });

  socket.on('presence_ping', async (data = {}, respond = () => {}) => {
    try {
      const name = socket.userName;
      if (!name || !userDatabase[name]) {
        respond({ success: false, authenticated: false });
        return;
      }

      const currentUser = userDatabase[name];
      const presenceChanged = currentUser.online !== true || currentUser.id !== socket.id;
      currentUser.lastSeen = Date.now();
      currentUser.online = true;
      currentUser.id = socket.id;
      if (presenceChanged) invalidateOnlineListCache('presence-ping-local');
      const cachedList = getSanitizedOnlineList();
      const sendList = () => emitOnlineList(socket);
      const remaining = getPostAuthRemainingDelay(socket, POST_AUTH_ONLINE_LIST_DELAY_MS);
      if (remaining > 0) deferServerTask('PRESENCE PING ONLINE LIST', sendList, remaining);
      else await sendList();
      respond({ success: true, authenticated: true, onlineCount: getOnlineCountFromList(cachedList) });
    } catch (err) {
      console.error('[PRESENCE PING ERROR]:', err);
      respond({ success: false, authenticated: !!socket.userName });
    }
  });

  socket.on('search_users', async (request) => {
    const query = request && typeof request === 'object' ? request.query : request;
    const purpose = request && typeof request === 'object' ? normalizeText(request.purpose, '') : '';
    if (!query) return;

    try {
      await ensureUserCacheReady();
      if (purpose === 'mentions') {
        socket.emit('mention_users_results', searchUserNamesFromCache(query));
        return;
      }
      if (purpose === 'admin') {
        if (socket.isAdmin !== true) {
          socket.emit('admin_users_results', { query, offset: 0, limit: 0, total: 0, results: [] });
          return;
        }
        const offset = Math.max(0, parseInt(request && request.offset, 10) || 0);
        const limit = Math.max(1, Math.min(1000, parseInt(request && request.limit, 10) || 25));
        const filter = normalizeText(request && request.filter, 'all').toLowerCase();
        const names = searchUserNamesFromCache(query).filter(username => {
          const user = userDatabase[username] || {};
          const role = getUserRole(username, user);
          const banned = isUserBanned(user);
          if (filter === 'online') return user.online === true;
          if (filter === 'offline') return user.online !== true;
          if (filter === 'admin') return isUserAdmin(username, user);
          if (filter === 'mod') return role === 'mod';
          if (filter === 'trusted') return role === 'trusted';
          if (filter === 'banned') return banned;
          if (filter === 'user') return !isUserAdmin(username, user) && !banned && role === 'user';
          return true;
        });
        const results = names.slice(offset, offset + limit).map(username => getPublicUserData(username, userDatabase[username], true));
        socket.emit('admin_users_results', { query, filter, offset, limit, total: names.length, results, countryStats: getAdminCountryStats() });
        return;
      }
      const results = await searchUsersFromDb(query, false, purpose === 'friends');
      socket.emit('global_search_results', results);
    } catch (err) {
      console.error('[SEARCH USERS DB ERROR]:', err);
      if (purpose === 'mentions') socket.emit('mention_users_results', []);
      else if (purpose === 'admin') socket.emit('admin_users_results', { query, offset: 0, limit: 0, total: 0, results: [] });
      else socket.emit('global_search_results', []);
    }
  });
  
  socket.on('request_trophy_stats', async () => {
    try {
      const stats = await getGlobalTrophyStats();
      socket.emit('global_trophy_stats', stats);
    } catch (err) {
      console.error('[TROPHY STATS DB ERROR]:', err);
      socket.emit('global_trophy_stats', globalTrophyStatsCache || {});
    }
  });

  socket.on('request_trending', async () => {
    try {
      await emitTrendingFromDb(socket);
    } catch (err) {
      console.error('[TRENDING DB ERROR]:', err);
      socket.emit('trending_data', buildTrendingViewPayload(trendingCache || {
        topDownloads: [],
        topWishlist: []
      }));
    }
  });

  socket.on('request_content_download_counts', async (request = {}, callback) => {
    try {
      const keys = Array.isArray(request && request.keys) ? request.keys : [];
      const counts = await getContentDownloadCountsForKeys(keys);
      const payload = buildContentDownloadCountsPayload(counts, { partial: true });

      if (typeof callback === 'function') callback(payload);
      else socket.emit('content_download_counts', payload);
    } catch (err) {
      console.error('[CONTENT DOWNLOAD COUNTS ERROR]:', err);
      const payload = {
        success: false,
        counts: {},
        updatedAt: Date.now(),
        uniqueUsers: true,
        partial: true,
        error: 'Failed to calculate content download counts.'
      };

      if (typeof callback === 'function') callback(payload);
      else socket.emit('content_download_counts', payload);
    }
  });

  socket.on('admin_redeem', (data, callback) => {
    if (!data || typeof callback !== 'function') return;
    const { code } = data;
    if (!code) return callback({ success: false, message: "Enter a code." });

    const cleanCode = code.replace(/-/g, "").toUpperCase();
    if (cleanCode === "PLATINUMCODE") return callback({ success: true, type: 'PLATINUM_UNLOCK' });
    if (cleanCode === "UNLOCKALLDB1") return callback({ success: true, type: 'SINGLE_TROPHY' });

    callback({ success: false, message: "Invalid code." });
  });

  socket.on('chat_message', async (msg, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    if (socket.__passwordResetRevoked === true) {
      const blocked = { success: false, reason: 'password_reset', message: 'Your session ended because an administrator reset your password.' };
      socket.emit('chat_blocked', blocked);
      respond(blocked);
      return;
    }
    let messageData = { ...(typeof msg === 'object' ? msg : { text: msg }), time: new Date().toISOString(), seenBy: [], seenAt: {} };
    const isAdmin = socket.isAdmin === true;
    const canModerate = canModerateSocket(socket);
    const actorRole = getActorRole(socket);
    const senderName = socket.userName || messageData.user;

    const text = normalizeText(messageData.text, "");
    const lowerText = text.toLowerCase();

    if (senderName && userDatabase[senderName] && isUserBanned(userDatabase[senderName]) && !ADMIN_USERS.includes(senderName)) {
      const blocked = { success: false, reason: 'banned', message: 'Your account is banned.' };
      socket.emit('chat_blocked', blocked);
      respond(blocked);
      return;
    }

    if (lowerText.startsWith('/kick')) {
      if (!canModerate) {
        const blocked = { success: false, reason: 'permission', message: 'Only admins/moderators can use /kick.' };
        socket.emit('chat_blocked', blocked);
        respond(blocked);
        return;
      }

      const { targetName } = resolveCommandTarget(text.slice('/kick'.length));
      const targetSocket = getSocketsByUserName(targetName)[0];
      if (!targetName || !targetSocket) {
        const error = { success: false, message: 'User not found or offline.' };
        respond(error);
        return;
      }
      if (!canModerateTarget(socket, targetName)) {
        const error = { success: false, message: 'You cannot kick this user.' };
        respond(error);
        return;
      }

      targetSocket.emit('user_kicked', { by: senderName, role: actorRole });
      socket.emit('kick_success', { targetId: targetSocket.id, targetName });
      await addModerationLog('kick', `Kicked ${targetName} via chat command`, { targetName, targetId: targetSocket.id }, senderName || 'Moderator');
      setTimeout(() => {
        if (targetSocket.connected) targetSocket.disconnect(true);
      }, 2500);
      respond({ success: true, command: 'kick', targetName });
      return;
    }

    if (lowerText.startsWith('/ban')) {
      if (socket.isAdmin !== true) {
        const blocked = { success: false, reason: 'permission', message: 'Only admins can use /ban.' };
        socket.emit('chat_blocked', blocked);
        respond(blocked);
        return;
      }

      const { targetName, rest } = resolveCommandTarget(text.slice('/ban'.length));
      const reason = rest || 'Banned by administrator';
      const result = await banUser(targetName, reason, senderName || 'Admin');
      if (result.success) {
        await addModerationLog('ban', `Banned ${targetName} via chat command`, { targetName, reason: result.reason }, senderName || 'Admin');
      }
      respond({ command: 'ban', ...result });
      return;
    }

    if (lowerText.startsWith('/unban')) {
      if (socket.isAdmin !== true) {
        const blocked = { success: false, reason: 'permission', message: 'Only admins can use /unban.' };
        socket.emit('chat_blocked', blocked);
        respond(blocked);
        return;
      }

      const { targetName } = resolveCommandTarget(text.slice('/unban'.length), { onlyBanned: true });
      const result = await unbanUser(targetName, senderName || 'Admin');
      if (result.success) {
        await addModerationLog('unban', `Unbanned ${targetName} via chat command`, { targetName }, senderName || 'Admin');
      }
      respond({ command: 'unban', ...result });
      return;
    }

    if (lowerText.startsWith('/role')) {
      if (socket.isAdmin !== true) {
        const blocked = { success: false, reason: 'permission', message: 'Only admins can use /role.' };
        socket.emit('chat_blocked', blocked);
        respond(blocked);
        return;
      }

      const { targetName, rest } = resolveCommandTarget(text.slice('/role'.length));
      const role = normalizeText(rest, '').toLowerCase();
      const result = await setUserRole(targetName, role, senderName || 'Admin');
      if (result.success) {
        await addModerationLog('role', `Changed ${targetName}'s role to ${result.role} via chat command`, { targetName, role: result.role }, senderName || 'Admin');
      }
      respond({ command: 'role', targetName, ...result });
      return;
    }

    if (lowerText.startsWith('/resetpassword') || lowerText.startsWith('/reset_password')) {
      if (socket.isAdmin !== true) {
        const blocked = { success: false, reason: 'permission', message: 'Only admins can use /resetpassword.' };
        socket.emit('chat_blocked', blocked);
        respond(blocked);
        return;
      }

      const commandName = lowerText.startsWith('/reset_password') ? '/reset_password' : '/resetpassword';
      const { targetName } = resolveCommandTarget(text.slice(commandName.length));
      const result = await resetUserPassword(targetName, senderName || 'Admin');
      if (result.success) {
        await addModerationLog('reset_password', `Authorized a 10-minute password reset for ${targetName} via chat command`, { targetName }, senderName || 'Admin');
      }
      respond({ command: 'resetpassword', ...result });
      return;
    }

    if ((lowerText === '/reload' || lowerText === '/force_reload') && isAdmin) {
      socket.broadcast.emit('force_reload');
      await addModerationLog('reload', 'Forced reload for connected users', {}, senderName || 'Admin');
      respond({ success: true, command: 'reload' });
      return;
    }

    if ((lowerText === '/clean' || lowerText === '/clear_chat' || lowerText === '/clean confirm' || lowerText === '/clear_chat confirm') && isAdmin) {
      const confirmed = lowerText.endsWith(' confirm');
      if (!confirmed) {
        const warning = { success: false, command: 'clear_chat', message: 'Type /clean confirm to permanently clear the chat. A backup will be saved first.' };
        socket.emit('chat_blocked', warning);
        respond(warning);
        return;
      }

      const backedUpCount = await clearChatHistorySafely(senderName || 'Admin', 'chat command');
      io.emit('chat_cleared', { by: senderName || 'Admin', backedUpCount, chatEpoch: chatSyncState.epoch, chatRevision: chatSyncState.revision });
      await addModerationLog('clear_chat', `Cleared global chat history via command (${backedUpCount} messages backed up)`, { backedUpCount }, senderName || 'Admin');
      respond({ success: true, command: 'clear_chat', backedUpCount });
      return;
    }

    if (!canModerate) {
      const controls = adminState.chatControls || {};
      if (controls.locked) {
        const blocked = { success: false, reason: 'locked', message: 'Chat is locked by admin.', controls };
        socket.emit('chat_blocked', blocked);
        respond(blocked);
        return;
      }

      const slowSeconds = parseInt(controls.slowSeconds || 0, 10) || 0;
      if (slowSeconds > 0 && socket.lastChatAt) {
        const elapsed = Date.now() - socket.lastChatAt;
        const waitMs = (slowSeconds * 1000) - elapsed;
        if (waitMs > 0) {
          const blocked = { success: false, reason: 'slow_mode', waitSeconds: Math.ceil(waitMs / 1000), controls };
          socket.emit('chat_blocked', blocked);
          respond(blocked);
          return;
        }
      }
      socket.lastChatAt = Date.now();
    }

    messageData.isAdmin = isAdmin;
    messageData.role = actorRole;
    messageData.isModerator = actorRole === 'mod';
    messageData.user = senderName;

    try {
      const savedMessage = cleanChatMessage(messageData);
      const savedRes = await pool.query('INSERT INTO chat (message) VALUES ($1) RETURNING id', [savedMessage]);
      attachChatDbId(messageData, savedRes.rows[0]?.id);
      lastChatDbId = Math.max(lastChatDbId, messageData._dbId || 0);

      messageHistory.push(messageData);
      if (messageHistory.length > MAX_CHAT_HISTORY) messageHistory.shift();

      const publicMessage = cleanChatMessage(messageData);
      deferServerTask('CHAT USER NOTIFICATIONS', () => recordChatUserNotifications(publicMessage), 0);
      const syncChange = await recordChatSyncChangeSafe('upsert', String(new Date(messageData.time).getTime()), publicMessage);
      io.sockets.sockets.forEach(client => {
        if (!client || !client.connected) return;
        if (client.__chatInitialFullSyncInFlight === true) return;
        client.emit('chat_message', publicMessage);
      });
      if (syncChange) emitChatSyncChange(syncChange);
      respond({ success: true, message: publicMessage });
    } catch (err) {
      console.error('[CHAT SAVE ERROR]:', err);
      const failed = { success: false, reason: 'database', message: 'Message was not saved. Please try again.' };
      socket.emit('chat_blocked', failed);
      respond(failed);
    }
  });


  socket.on('admin_ping' , async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};

    try {
      await pool.query(`DELETE FROM presence_sessions WHERE last_seen < NOW() - INTERVAL '${PRESENCE_TTL_SECONDS} seconds'`);

      const statsRes = await pool.query(`
        SELECT
          (SELECT COUNT(*)::int FROM users) AS users,
          (SELECT COUNT(DISTINCT name)::int FROM presence_sessions WHERE last_seen > NOW() - INTERVAL '${PRESENCE_TTL_SECONDS} seconds') AS online
      `);

      const stats = statsRes.rows[0] || {};

      respond({
        success: true,
        serverTime: new Date().toISOString(),
        uptimeSeconds: Math.floor((Date.now() - SERVER_STARTED_AT) / 1000),
        users: Number(stats.users || 0),
        online: Number(stats.online || 0)
      });
    } catch (err) {
      console.error('[ADMIN PING ERROR]:', err);
      respond({
        success: false,
        message: 'Database error while loading server stats.',
        serverTime: new Date().toISOString(),
        uptimeSeconds: Math.floor((Date.now() - SERVER_STARTED_AT) / 1000),
        users: 0,
        online: 0
      });
    }
  });

  socket.on('admin_reset_password', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const targetName = normalizeText(data && data.targetName, "");
      const result = await resetUserPassword(targetName, socket.userName || normalizeText(data && data.adminUser, "Admin"));

      if (result.success) {
        await addModerationLog('reset_password', `Authorized a 10-minute password reset for ${targetName}`, { targetName }, socket.userName || 'Admin');
      }

      respond(result);
    } catch (err) {
      console.error('[ADMIN RESET PASSWORD ERROR]:', err);
      respond({ success: false, message: "Server error while resetting password." });
    }
  });

  socket.on('admin_delete_account', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const targetName = normalizeText(data && data.targetName, "");
      if (targetName === socket.userName) return respond({ success: false, message: "You cannot delete your own account while logged in." });

      const reason = normalizeText(data && data.reason, "Account deleted by administrator.");
      const result = await deleteUserAccount(targetName, reason, socket.userName || normalizeText(data && data.adminUser, "Admin"));

      if (result.success) {
        await addModerationLog('delete_account', `Deleted account ${targetName}`, { targetName, reason: result.reason }, socket.userName || 'Admin');
        await addServerLog('account_deleted', `${targetName} account deleted`, { targetName, reason: result.reason }, socket.userName || 'Admin');
      }

      respond(result);
    } catch (err) {
      console.error('[ADMIN DELETE ACCOUNT ERROR]:', err);
      respond({ success: false, message: "Server error while deleting account." });
    }
  });

  socket.on('admin_ban_user', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const targetName = normalizeText(data && data.targetName, "");
      const reason = normalizeText(data && data.reason, "Banned by administrator");
      const result = await banUser(targetName, reason, socket.userName || normalizeText(data && data.adminUser, "Admin"));

      if (result.success) {
        await addModerationLog('ban', `Banned ${targetName}`, { targetName, reason: result.reason }, socket.userName || 'Admin');
      }

      respond(result);
    } catch (err) {
      console.error('[ADMIN BAN ERROR]:', err);
      respond({ success: false, message: "Server error while banning user." });
    }
  });

  socket.on('admin_unban_user', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const targetName = normalizeText(data && data.targetName, "");
      const result = await unbanUser(targetName, socket.userName || normalizeText(data && data.adminUser, "Admin"));

      if (result.success) {
        await addModerationLog('unban', `Unbanned ${targetName}`, { targetName }, socket.userName || 'Admin');
      }

      respond(result);
    } catch (err) {
      console.error('[ADMIN UNBAN ERROR]:', err);
      respond({ success: false, message: "Server error while unbanning user." });
    }
  });

  socket.on('admin_set_role', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const targetName = normalizeText(data && data.targetName, "");
      const role = normalizeText(data && data.role, "user").toLowerCase();
      const result = await setUserRole(targetName, role, socket.userName || normalizeText(data.adminUser, "Admin"));

      if (result.success) {
        await addModerationLog('role', `Changed ${targetName}'s role to ${result.role}`, { targetName, role: result.role }, socket.userName || 'Admin');
      }

      respond(result);
    } catch (err) {
      console.error('[ADMIN ROLE ERROR]:', err);
      respond({ success: false, message: "Server error while changing role." });
    }
  });

  socket.on('admin_maintenance_mode', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const nextMaintenance = normalizeMaintenanceState({
        ...(data || {}),
        by: socket.userName || (data && data.by) || "Admin",
        at: new Date().toISOString()
      });

      await saveAdminState(ADMIN_STATE_KEYS.maintenance, nextMaintenance);
      adminState.maintenance = nextMaintenance;
      adminStateLastRefreshAt = Date.now();
      io.emit('maintenance_mode', adminState.maintenance);
      emitToAdmins('admin_state', {
        maintenance: adminState.maintenance,
        chatControls: adminState.chatControls,
        pinnedAnnouncement: adminState.pinnedAnnouncement || null,
        reports: adminReports,
        serverLog,
        registeredUsers: Object.keys(userDatabase).length,
        countryStats: getAdminCountryStats()
      });
      deferServerTask('ADMIN MAINTENANCE NOTIFY', () => notifyAdminStateAcrossInstances(ADMIN_STATE_KEYS.maintenance, nextMaintenance), 0);
      deferServerTask('ADMIN MAINTENANCE LOG', () => addModerationLog(nextMaintenance.enabled ? 'maintenance_on' : 'maintenance_off', nextMaintenance.enabled ? 'Enabled maintenance mode' : 'Disabled maintenance mode', nextMaintenance, socket.userName || 'Admin'), 0);
      respond({ success: true, state: nextMaintenance });
    } catch (err) {
      console.error('[ADMIN MAINTENANCE ERROR]:', err);
      respond({ success: false, message: "Server error while updating maintenance mode." });
    }
  });

  socket.on('request_maintenance_state', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      await refreshAdminStateThrottled(1500);
      const state = adminState.maintenance || normalizeMaintenanceState({});
      socket.emit('maintenance_mode', state);
      respond({ success: true, state });
    } catch (err) {
      console.error('[MAINTENANCE STATE REQUEST ERROR]:', err);
      respond({ success: false, message: 'Server error while loading maintenance state.' });
    }
  });

  socket.on('admin_request_chat_controls', async (data, callback) => {
    try {
      await refreshAdminStateThrottled(3000);
      const payload = adminState.chatControls || normalizeChatControls({});
      socket.emit('chat_controls', payload);
      if (socket.isAdmin === true) socket.emit('admin_chat_controls_state', payload);
      if (typeof callback === 'function') callback({ success: true, state: payload });
    } catch (err) {
      console.error('[ADMIN CHAT CONTROLS REQUEST ERROR]:', err);
      if (typeof callback === 'function') callback({ success: false, message: 'Server error while loading chat controls.' });
    }
  });

  socket.on('admin_request_admin_state', async (data, callback) => {
    try {
      await refreshAdminStateThrottled(3000);
      if (socket.isAdmin === true) {
        await refreshReportsFromDb();
        await refreshServerLogFromDb();
      }
      const payload = {
        maintenance: adminState.maintenance,
        chatControls: adminState.chatControls,
        pinnedAnnouncement: adminState.pinnedAnnouncement || null,
        reports: socket.isAdmin === true ? adminReports : [],
        serverLog: socket.isAdmin === true ? serverLog : [],
        registeredUsers: socket.isAdmin === true ? Object.keys(userDatabase).length : 0,
        countryStats: socket.isAdmin === true ? getAdminCountryStats() : { total: 0, known: 0, unknown: 0, countries: [] }
      };
      socket.emit('admin_state', payload);
      socket.emit('maintenance_mode', adminState.maintenance);
      socket.emit('chat_controls', adminState.chatControls);
      socket.emit('admin_pinned_announcement', adminState.pinnedAnnouncement || { clear: true });
      if (typeof callback === 'function') callback({ success: true, state: payload });
    } catch (err) {
      console.error('[ADMIN STATE REQUEST ERROR]:', err);
      if (typeof callback === 'function') callback({ success: false, message: 'Server error while loading admin state.' });
    }
  });

  socket.on('admin_chat_controls', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      adminState.chatControls = normalizeChatControls({
        ...(data || {}),
        by: socket.userName || (data && data.by) || "Admin",
        at: new Date().toISOString()
      });

      await saveAdminState(ADMIN_STATE_KEYS.chatControls, adminState.chatControls);
      io.emit('chat_controls', adminState.chatControls);
      io.emit('admin_chat_controls', adminState.chatControls);
      emitToAdmins('admin_chat_controls_state', adminState.chatControls);
      emitToAdmins('admin_state', {
        maintenance: adminState.maintenance,
        chatControls: adminState.chatControls,
        pinnedAnnouncement: adminState.pinnedAnnouncement || null,
        reports: adminReports,
        serverLog,
        registeredUsers: Object.keys(userDatabase).length,
        countryStats: getAdminCountryStats()
      });
      await addModerationLog('chat_controls', `Updated chat controls: ${adminState.chatControls.locked ? 'locked' : 'open'}, slow ${adminState.chatControls.slowSeconds}s`, adminState.chatControls, socket.userName || 'Admin');
      respond({ success: true, state: adminState.chatControls });
    } catch (err) {
      console.error('[ADMIN CHAT CONTROLS ERROR]:', err);
      respond({ success: false, message: "Server error while updating chat controls." });
    }
  });

  socket.on('admin_pinned_announcement', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const shouldClear = !data || data.clear || !normalizeText(data.text, "");
      adminState.pinnedAnnouncement = shouldClear ? null : {
        id: data.id || `admin-announcement-${Date.now()}`,
        text: normalizeText(data.text, ""),
        color: /^#[0-9a-f]{6}$/i.test(normalizeText(data.color, "")) ? normalizeText(data.color, "").toLowerCase() : "#ffcc00",
        by: socket.userName || data.by || "Admin",
        at: data.at || new Date().toISOString()
      };

      await saveAdminState(ADMIN_STATE_KEYS.pinnedAnnouncement, adminState.pinnedAnnouncement || { clear: true });
      io.emit('admin_pinned_announcement', adminState.pinnedAnnouncement || { clear: true });
      await addModerationLog(shouldClear ? 'unpin' : 'pin', shouldClear ? 'Cleared pinned announcement' : 'Pinned announcement', adminState.pinnedAnnouncement || {}, socket.userName || 'Admin');
      respond({ success: true, announcement: adminState.pinnedAnnouncement });
    } catch (err) {
      console.error('[ADMIN ANNOUNCEMENT ERROR]:', err);
      respond({ success: false, message: "Server error while updating announcement." });
    }
  });

  socket.on('admin_request_moderation_log', async () => {
    if (socket.isAdmin === true) {
      await refreshModerationLogFromDb();
      socket.emit('admin_moderation_log_list', moderationLog);
    }
  });

  socket.on('admin_clear_moderation_log', async () => {
    if (socket.isAdmin !== true) return;
    moderationLog = [];
    try {
      await pool.query('TRUNCATE moderation_log');
    } catch (err) {
      console.error('[ADMIN LOG CLEAR ERROR]:', err);
    }
    emitToAdmins('admin_moderation_log_list', moderationLog);
  });

  socket.on('admin_request_server_log', async () => {
    if (socket.isAdmin === true) {
      await refreshServerLogFromDb();
      socket.emit('admin_server_log_list', serverLog);
    }
  });

  socket.on('admin_clear_server_log', async () => {
    if (socket.isAdmin !== true) return;
    serverLog = [];
    try {
      await pool.query('TRUNCATE server_log');
    } catch (err) {
      console.error('[SERVER LOG CLEAR ERROR]:', err);
    }
    emitToAdmins('admin_server_log_list', serverLog);
  });

  socket.on('admin_request_reports', async () => {
    if (socket.isAdmin === true) {
      await refreshReportsFromDb();
      socket.emit('reports_list', adminReports);
    }
  });

  socket.on('admin_clear_reports', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      adminReports = [];
      await pool.query('UPDATE reports SET resolved = true');
      emitToAdmins('reports_list', adminReports);
      await addModerationLog('reports', 'Cleared report center', {}, socket.userName || 'Admin');
      respond({ success: true });
    } catch (err) {
      console.error('[ADMIN CLEAR REPORTS ERROR]:', err);
      respond({ success: false, message: "Server error while clearing reports." });
    }
  });

  socket.on('admin_resolve_report', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const reportId = normalizeText(data && data.reportId, "");
      if (!reportId) return respond({ success: false, message: "Missing report id." });

      adminReports = adminReports.filter(r => String(r.id || r.time) !== String(reportId));
      await pool.query('UPDATE reports SET resolved = true WHERE id = $1', [reportId]);
      emitToAdmins('reports_list', adminReports);
      await addModerationLog('reports', 'Resolved report', { reportId }, socket.userName || 'Admin');
      respond({ success: true });
    } catch (err) {
      console.error('[ADMIN RESOLVE REPORT ERROR]:', err);
      respond({ success: false, message: "Server error while resolving report." });
    }
  });

  socket.on('report_message', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      const report = await createReport(data || {}, socket.userName || 'Unknown');
      respond({ success: true, report });
    } catch (err) {
      console.error('[REPORT ERROR]:', err);
      respond({ success: false, message: "Server error while creating report." });
    }
  });


  socket.on('message_reaction', async (data) => {
    const msg = messageHistory.find(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msg) {
        if (!msg.reactions) msg.reactions = [];
        let react = msg.reactions.find(r => r.emoji === data.emoji);
        
        if (react) {
            const idx = react.users.indexOf(data.user);
            if (idx > -1) { react.users.splice(idx, 1); react.count--; }
            else { react.users.push(data.user); react.count++; }
            if (react.count <= 0) msg.reactions = msg.reactions.filter(r => r.emoji !== data.emoji);
        } else {
            msg.reactions.push({ emoji: data.emoji, count: 1, users: [data.user] });
        }

        try {
            await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [cleanChatMessage(msg), msg.time]);
            const syncChange = await recordChatSyncChangeSafe('upsert', String(data.msgId || new Date(msg.time).getTime()), msg);
            io.emit('message_reaction', data);
            if (syncChange) emitChatSyncChange(syncChange);
        } catch (err) { console.error("Reaction Sync Error:", err); }
    }
  });

  socket.on('poll_vote', async (data) => {
    const msgIndex = messageHistory.findIndex(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msgIndex > -1) {
        const msg = messageHistory[msgIndex];
        if (msg.type === 'poll' && msg.content) {
            const poll = msg.content;
            
            poll.options.forEach(opt => {
                if (opt.voters) {
                    opt.voters = opt.voters.filter(u => u !== data.user);
                }
            });

            if (!poll.options[data.optionIndex].voters) poll.options[data.optionIndex].voters = [];
            poll.options[data.optionIndex].voters.push(data.user);
            
            poll.totalVotes = poll.options.reduce((sum, opt) => sum + (opt.voters ? opt.voters.length : 0), 0);

            try {
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [cleanChatMessage(msg), msg.time]);
                const syncChange = await recordChatSyncChangeSafe('upsert', String(data.msgId || new Date(msg.time).getTime()), msg);
                
                io.emit('message_edited', { 
                    msgId: data.msgId, 
                    newText: msg.text, 
                    type: 'poll', 
                    content: poll,
                    editedByAdmin: msg.editedByAdmin 
                });
                if (syncChange) emitChatSyncChange(syncChange);
                
                const pinned = pinnedMessages.find(p => p.id === data.msgId);
                if (pinned) {
                    pinned.content = poll;
                    await pool.query('UPDATE pinned_messages SET data = $1 WHERE message_id = $2', [pinned, data.msgId]);
                    io.emit('pinned_list', pinnedMessages);
                }
            } catch (err) { console.error("Poll Sync Error:", err); }
        }
    }
  });

  socket.on('mark_as_read', (data) => {
    const msg = messageHistory.find(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msg && msg.user !== data.user) {
        if (!msg.seenBy) msg.seenBy = [];
        if (!msg.seenAt || typeof msg.seenAt !== 'object' || Array.isArray(msg.seenAt)) msg.seenAt = {};
        if (!msg.seenBy.includes(data.user)) {
            msg.seenBy.push(data.user);
            msg.seenAt[data.user] = new Date().toISOString();
            const fullSeenPayload = { msgId: data.msgId, seenBy: msg.seenBy, seenAt: msg.seenAt };
            getSocketsByUserName(msg.user).forEach(client => {
                if (client && client.connected) client.emit('message_seen', fullSeenPayload);
            });
            const readerSeenPayload = { msgId: data.msgId, seenBy: [data.user], seenAt: { [data.user]: msg.seenAt[data.user] } };
            getSocketsByUserName(data.user).forEach(client => {
                if (client && client.connected && client.userName !== msg.user) client.emit('message_seen', readerSeenPayload);
            });
            queueSeenMessagePersist(msg);
            recordChatSeenEvent(data.msgId, msg.user, data.user, msg.seenAt[data.user])
              .then(event => { if (event) emitChatSeenSyncChange(event); })
              .catch(err => console.error('[CHAT SEEN SYNC ERROR]:', err && err.message ? err.message : err));
        }
    }
  });

  socket.on('edit_message', async (data) => {
    const msgIndex = messageHistory.findIndex(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msgIndex > -1) {
        const isAdmin = socket.isAdmin === true;
        const canModerate = canModerateSocket(socket);
        const actorRole = getActorRole(socket);
        const msg = messageHistory[msgIndex];
        const isOwner = msg.user === socket.userName;
        const canEditTarget = isOwner || (canModerate && canModerateTarget(socket, msg.user));

        if (canEditTarget) {
            const wasEditedByStaff = (!isOwner && canModerate);
            const wasEditedByAdmin = (!isOwner && isAdmin);
            
            msg.text = data.newText;
            msg.edited = true;
            msg.editedByAdmin = wasEditedByAdmin;
            msg.editedByMod = wasEditedByStaff && !wasEditedByAdmin;
            if (wasEditedByStaff) {
              msg.editedBy = socket.userName;
              msg.editedByRole = actorRole;
            }

            if (data.content) {
                msg.type = data.type || 'image';
                msg.content = data.content;
            }
            
            try {
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [cleanChatMessage(msg), msg.time]);
                const syncChange = await recordChatSyncChangeSafe('upsert', String(data.msgId || new Date(msg.time).getTime()), msg);
                io.emit('message_edited', { 
                    msgId: data.msgId, 
                    newText: data.newText, 
                    type: msg.type, 
                    content: msg.content,
                    editedByAdmin: wasEditedByAdmin,
                    editedByMod: msg.editedByMod === true,
                    editedBy: msg.editedBy || null,
                    editedByRole: msg.editedByRole || null 
                });
                if (syncChange) emitChatSyncChange(syncChange);

                const pinned = pinnedMessages.find(p => p.id === data.msgId);
                if (pinned) {
                    pinned.text = data.newText;
                    pinned.type = msg.type || 'text';
                    pinned.content = msg.content || null;
                    
                    await pool.query('UPDATE pinned_messages SET data = $1 WHERE message_id = $2', [pinned, data.msgId]);
                    io.emit('pinned_list', pinnedMessages);
                }

            } catch (err) { console.error("Edit Sync Error:", err); }
        }
    }
  });

  socket.on('delete_message', async (data) => {
    const msgIndex = messageHistory.findIndex(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msgIndex > -1) {
        const canModerate = canModerateSocket(socket);
        const msg = messageHistory[msgIndex];
        const msgTime = msg.time;
        const isOwner = msg.user === socket.userName;

        if (isOwner || (canModerate && canModerateTarget(socket, msg.user))) {
            messageHistory.splice(msgIndex, 1);
            try {
                await pool.query("DELETE FROM chat WHERE message->>'time' = $1", [msgTime]);
            } catch (err) {
                console.error("Erro ao deletar mensagem do banco:", err);
            }

            const syncChange = await recordChatSyncChangeSafe('delete', String(data.msgId || ''), null);
            io.emit('message_deleted', data.msgId);
            if (syncChange) emitChatSyncChange(syncChange);
            if (!isOwner) {
                await addModerationLog('delete_message', `Deleted message from ${msg.user}`, { msgId: data.msgId, targetUser: msg.user }, socket.userName || 'Moderator');
            }

            const isPinned = pinnedMessages.find(p => p.id === data.msgId);
            if (isPinned) {
                pinnedMessages = pinnedMessages.filter(p => p.id !== data.msgId);
                pool.query('DELETE FROM pinned_messages WHERE message_id = $1', [data.msgId]).catch(e => {});
                io.emit('pinned_list', pinnedMessages);
            }
        }
    }
  });

  socket.on('clear_chat', async (data = {}, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    if (socket.isAdmin !== true) return respond({ success: false, message: 'Admin only.' });

    const byUser = socket.userName || data.user || data.adminUser || 'Admin';
    const backedUpCount = await clearChatHistorySafely(byUser, 'admin clear button');
    io.emit('chat_cleared', { by: byUser, user: byUser, backedUpCount, chatEpoch: chatSyncState.epoch, chatRevision: chatSyncState.revision });

    pinnedMessages = [];
    await pool.query('TRUNCATE pinned_messages');
    io.emit('pinned_list', pinnedMessages);

    await addModerationLog('clear_chat', `Cleared global chat history (${backedUpCount} messages backed up)`, { backedUpCount }, byUser);
    respond({ success: true, backedUpCount });
  });

  socket.on('kick_user', async (data) => {
    if (canModerateSocket(socket)) {
        const targetSocket = io.sockets.sockets.get(data.targetId);
        if (targetSocket) {
            const targetName = targetSocket.userName || normalizeText(data.targetName, 'Unknown');
            if (!canModerateTarget(socket, targetName)) {
                return;
            }
            targetSocket.emit('user_kicked', { by: socket.userName, role: getActorRole(socket) });
            socket.emit('kick_success', { targetId: data.targetId, targetName });
            await addModerationLog('kick', `Kicked ${targetName}`, { targetId: data.targetId, targetName }, socket.userName || 'Moderator');
            
            setTimeout(() => { 
                if (targetSocket.connected) {
                    targetSocket.disconnect(true);
                }
            }, 2500);
        }
    }
  });

  socket.on('pin_message', async (data) => {
    if (canModerateSocket(socket)) {
      const msg = messageHistory.find(m => String(new Date(m.time).getTime()) === String(data.msgId));
      if (msg && !pinnedMessages.find(p => p.id === data.msgId)) {
        const pinData = { 
            id: data.msgId, 
            text: msg.text || "", 
            user: msg.user, 
            type: msg.type || 'text', 
            content: msg.content || null 
        };
        pinnedMessages.push(pinData);
        
        try {
          await pool.query('INSERT INTO pinned_messages (message_id, data) VALUES ($1, $2) ON CONFLICT (message_id) DO UPDATE SET data = $2', [data.msgId, pinData]);
          io.emit('pinned_list', pinnedMessages);
          await addModerationLog('pin_message', `Pinned message from ${msg.user}`, { msgId: data.msgId, targetUser: msg.user }, socket.userName || 'Moderator');
        } catch (e) { console.error("Pin DB Error:", e); }
      }
    }
  });

  socket.on('unpin_message', async (data) => {
    if (canModerateSocket(socket)) {
      pinnedMessages = pinnedMessages.filter(p => p.id !== data.msgId);
      
      try {
        await pool.query('DELETE FROM pinned_messages WHERE message_id = $1', [data.msgId]);
      } catch (e) { console.error("Unpin DB Error:", e); }

      io.emit('pinned_list', pinnedMessages);
      await addModerationLog('unpin_message', 'Unpinned a chat message', { msgId: data.msgId }, socket.userName || 'Moderator');
    }
  });

  socket.on('typing_start', () => {
    const name = socket.userName;
    if (name && userDatabase[name]) {
      socket.broadcast.emit('user_typing', { name: name, avatar: userDatabase[name].avatar });
    }
  });

  socket.on('typing_stop', () => {
    const name = socket.userName;
    if (name) socket.broadcast.emit('user_stopped_typing', { name: name });
  });

  socket.on('disconnect', async () => {
    clearFriendActivitySubscription(socket);
    const name = socket.userName;
    if (!name || !userDatabase[name]) return;

    try {
      const presenceState = await pool.query(`
        WITH removed AS (
          DELETE FROM presence_sessions WHERE socket_id = $1 RETURNING socket_id
        )
        SELECT socket_id, last_seen
        FROM presence_sessions
        WHERE name = $2 AND last_seen >= NOW() - INTERVAL '${PRESENCE_TTL_SECONDS} seconds'
        ORDER BY last_seen DESC
        LIMIT 1
      `, [socket.id, name]);
      invalidateOnlineListCache('presence-remove');
      socket.broadcast.emit('user_stopped_typing', { name });

      const remainingPresence = presenceState.rows[0] || null;
      const stillOnline = !!remainingPresence;
      if (stillOnline) {
        userDatabase[name].online = true;
        userDatabase[name].id = remainingPresence.socket_id || userDatabase[name].id;
        userDatabase[name].lastSeen = remainingPresence.last_seen ? new Date(remainingPresence.last_seen).getTime() : Date.now();
      } else {
        const lastSeen = Date.now();
        userDatabase[name].online = false;
        userDatabase[name].lastSeen = lastSeen;
        await pool.query(
          `UPDATE users
           SET data = COALESCE(data, '{}'::jsonb) || jsonb_build_object('online', false, 'lastSeen', $2::bigint)
           WHERE name = $1`,
          [name, lastSeen]
        );
        userCacheMeta[name] = Date.now();
        invalidateOnlineListCache('disconnect-presence-save');
        deferServerTask('LOGOUT SERVER LOG', () => addServerLog('logout', `${name} disconnected`, { socketId: socket.id }, name), 0);
      }

      const presencePayload = emitPresenceUpdate(name, userDatabase[name]);
      if (presencePayload) deferServerTask('PRESENCE DISCONNECT NOTIFY', () => notifyPresenceAcrossInstances(name, userDatabase[name]), 0);

      const stillHasLocalSession = getSocketsByUserName(name).some(client => client && client.connected);
      if (!stillHasLocalSession) compactCachedUser(name);
      await emitOnlineList();
    } catch (err) {
      console.error('[DISCONNECT CLEANUP ERROR]:', err);
      if (userDatabase[name]) userDatabase[name].lastSeen = Date.now();
      const stillHasLocalSession = getSocketsByUserName(name).some(client => client && client.connected);
      if (!stillHasLocalSession) compactCachedUser(name);
    }
  });
});

const PORT = process.env.PORT || 3000;
let startupInFlight = false;
let startupRetryTimer = null;
let serverListening = false;

async function startServer() {
  if (startupInFlight || serverListening) return;
  startupInFlight = true;
  try {
    await initDb();
    await initProfileSyncNotifications();
    startUserCacheWarmup();
    startBackgroundTasks();
    serverListening = true;
    server.listen(PORT, () => {
      console.log(`PSN Database Server running on port ${PORT} (pg pool max ${PG_POOL_MAX}, connect timeout ${PG_CONNECTION_TIMEOUT_MS}ms, idle timeout ${PG_IDLE_TIMEOUT_MS}ms, online cache ${ONLINE_LIST_CACHE_MS}ms, chat sync ${CHAT_SYNC_INTERVAL_MS}ms, instance ${INSTANCE_ID})`);
      startKeepAlivePings();
    });
  } catch (err) {
    console.error('[STARTUP ERROR]:', err);
    if (!startupRetryTimer) {
      startupRetryTimer = setTimeout(() => {
        startupRetryTimer = null;
        startServer();
      }, 5000);
    }
  } finally {
    startupInFlight = false;
  }
}

startServer();
