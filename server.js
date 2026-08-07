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

const SERVER_STARTED_AT = Date.now();
const INSTANCE_ID = process.env.RENDER_INSTANCE_ID || process.env.RAILWAY_REPLICA_ID || process.env.HOSTNAME || `instance-${Math.random().toString(36).slice(2, 10)}`;
const PRESENCE_TTL_SECONDS = 90;
const PRESENCE_HEARTBEAT_MS = 25000;
const CHAT_SYNC_INTERVAL_MS = 3000;
const KEEP_ALIVE_INTERVAL_MS = Math.max(60000, parseInt(process.env.KEEP_ALIVE_INTERVAL_MS || "600000", 10) || 600000);
const KEEP_ALIVE_TIMEOUT_MS = Math.max(1000, parseInt(process.env.KEEP_ALIVE_TIMEOUT_MS || "10000", 10) || 10000);
const KEEP_ALIVE_URLS = [
  "https://psn-content-0u8u.onrender.com/",
];
const PROFILE_SYNC_INTERVAL_MS = Math.max(10000, parseInt(process.env.PROFILE_SYNC_INTERVAL_MS || "15000", 10) || 15000);
const ENABLE_PROFILE_PERIODIC_SYNC = process.env.ENABLE_PROFILE_PERIODIC_SYNC === "1";
const POST_AUTH_CHAT_HISTORY_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_CHAT_HISTORY_DELAY_MS || "180", 10) || 180);
const POST_AUTH_ADMIN_STATE_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_ADMIN_STATE_DELAY_MS || "550", 10) || 550);
const POST_AUTH_ONLINE_LIST_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_ONLINE_LIST_DELAY_MS || "1400", 10) || 1400);
const POST_AUTH_PROFILE_SYNC_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_PROFILE_SYNC_DELAY_MS || "1800", 10) || 1800);
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
      stableStringifySmall(user && user.ps3Status)
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
  if (['08000','08001','08003','08004','08006','08007','08P01','57P01','57P02','57P03','53300'].includes(code)) return true;
  const message = String(err.message || err).toLowerCase();
  return /connection terminated|connection timeout|timeout exceeded when trying to connect|connection reset|econnreset|etimedout|ehostunreach|enetunreach|socket hang up|broken pipe|server closed the connection|terminating connection|the database system is starting up|too many clients|remaining connection slots/.test(message);
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

let userDatabase = {};
let userCacheMeta = {};
let userCacheLastFullRefresh = 0;
let userCacheRefreshInFlight = null;
const userProfileWriteInFlight = new Set();
const fullUserCacheNames = new Set();
const USER_HEAVY_CACHE_KEYS = ['downloadsData', 'libraryData', 'wishlistData', 'favoritesData', 'trophiesData', 'friendsData'];
let trendingCache = null;
let trendingCacheAt = 0;
let trendingBuildInFlight = null;
let globalTrophyStatsCache = null;
let globalTrophyStatsCacheAt = 0;
let globalTrophyStatsBuildInFlight = null;
let trendingRefreshTimer = null;
let trophyStatsRefreshTimer = null;
const TRENDING_CACHE_MS = Math.max(10000, parseInt(process.env.TRENDING_CACHE_MS || '300000', 10) || 300000);
const TROPHY_STATS_CACHE_MS = Math.max(10000, parseInt(process.env.TROPHY_STATS_CACHE_MS || '30000', 10) || 30000);
let messageHistory = [];
let lastChatDbId = 0;
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
  `);

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

const PROFILE_ARRAY_SYNC_KEYS = {
  downloadsData: { versionKey: 'downloadsUpdatedAt', countKey: 'downloads' },
  wishlistData: { versionKey: 'wishlistUpdatedAt', countKey: 'wishlist' },
  favoritesData: { versionKey: 'favoritesUpdatedAt', countKey: 'favorites' },
  libraryData: { versionKey: 'libraryUpdatedAt', countKey: 'library' },
  friendsData: { versionKey: 'friendsUpdatedAt', countKey: 'friends' }
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

function hasOwnPayload(target = {}, key = '') {
  return Object.prototype.hasOwnProperty.call(target || {}, key);
}


function normalizeProfileArrayPayloads(target = {}) {
  Object.keys(PROFILE_ARRAY_SYNC_KEYS).forEach(key => {
    const sync = PROFILE_ARRAY_SYNC_KEYS[key];
    const rawList = Array.isArray(target[key]) ? target[key] : [];
    const list = key === 'downloadsData'
      ? normalizeDownloadHistoryRecordsServer(rawList).history
      : (key === 'libraryData' ? mergeLibraryRecordsServer(rawList, []) : rawList);
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

    const currentRawList = getProfileArrayPayload(currentUser[key]);
    const incomingRawList = getProfileArrayPayload(incomingUser[key]);
    const currentList = key === 'downloadsData'
      ? normalizeDownloadHistoryRecordsServer(currentRawList).history
      : (key === 'libraryData' ? mergeLibraryRecordsServer(currentRawList, []) : currentRawList);
    const incomingList = key === 'downloadsData'
      ? normalizeDownloadHistoryRecordsServer(incomingRawList).history
      : (key === 'libraryData' ? mergeLibraryRecordsServer(incomingRawList, []) : incomingRawList);
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
      const acceptedList = key === 'libraryData'
        ? mergeLibraryRecordsServer(incomingList, currentList)
        : incomingList;
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
  const manualEnabled = data.manualEnabled === undefined ? !!data.enabled : !!data.manualEnabled;
  const enabled = manualEnabled || scheduled.active;

  return {
    enabled,
    manualEnabled,
    scheduledActive: scheduled.active,
    activeUntil: scheduled.activeUntil || data.activeUntil || null,
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


function buildCompactUserSummary(name, userData = {}) {
  const source = (userData && typeof userData === 'object' && !Array.isArray(userData)) ? userData : {};
  const compact = { ...source };

  if (Array.isArray(source.downloadsData)) compact.downloads = source.downloadsData.length;
  if (Array.isArray(source.wishlistData)) compact.wishlist = source.wishlistData.length;
  if (Array.isArray(source.favoritesData)) compact.favorites = source.favoritesData.length;
  if (Array.isArray(source.libraryData)) compact.library = source.libraryData.length;
  if (Array.isArray(source.friendsData)) compact.friends = source.friendsData.length;
  if (source.trophiesData && typeof source.trophiesData === 'object' && !Array.isArray(source.trophiesData)) {
    compact.trophies = countUnlockedTrophiesPayload(source.trophiesData);
  }

  USER_HEAVY_CACHE_KEYS.forEach(key => delete compact[key]);
  delete compact.passwordHash;
  delete compact.password;

  return normalizeUserRecord(name, compact);
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
        category,
        title_id,
        CASE
          WHEN category = 'games' AND title_id <> '' THEN category || '|T:' || title_id
          WHEN content_id <> '' THEN category || '|C:' || content_id
          WHEN title_id <> '' AND normalized_name <> '' THEN category || '|T:' || title_id || '|N:' || normalized_name
          WHEN title_id <> '' THEN category || '|T:' || title_id
          WHEN normalized_name <> '' THEN category || '|N:' || normalized_name
          ELSE ''
        END AS content_key
      FROM download_items
    ), wishlist_items AS (
      SELECT
        u.name AS username,
        CASE WHEN upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) IN ('MISSING','N/A','NONE','NULL','UNDEFINED') THEN '' ELSE upper(trim(COALESCE(item->>'titleId', item->>'id', ''))) END AS title_id
      FROM users u
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(u.data->'wishlistData') = 'array' THEN u.data->'wishlistData' ELSE '[]'::jsonb END
      ) item
    )
    SELECT 'game' AS kind, title_id AS key, COUNT(DISTINCT username)::int AS count
    FROM keyed_downloads
    WHERE category = 'games' AND title_id <> ''
    GROUP BY title_id
    UNION ALL
    SELECT 'content' AS kind, content_key AS key, COUNT(DISTINCT username)::int AS count
    FROM keyed_downloads
    WHERE content_key <> ''
    GROUP BY content_key
    UNION ALL
    SELECT 'wishlist' AS kind, title_id AS key, COUNT(*)::int AS count
    FROM wishlist_items
    WHERE title_id <> ''
    GROUP BY title_id
  `, [], { attempts: 2, label: 'TRENDING AGGREGATE' });

  const gameCounts = [];
  const wishCounts = [];
  const contentDownloadCounts = {};

  rows.rows.forEach(row => {
    const count = Number(row.count) || 0;
    if (row.kind === 'game') gameCounts.push({ id: row.key, count });
    else if (row.kind === 'wishlist') wishCounts.push({ id: row.key, count });
    else if (row.kind === 'content' && row.key) contentDownloadCounts[row.key] = count;
  });

  const sortTop = list => list.sort((a, b) => b.count - a.count || String(a.id).localeCompare(String(b.id))).slice(0, 50);
  return {
    topDownloads: sortTop(gameCounts),
    topWishlist: sortTop(wishCounts),
    contentDownloadCounts
  };
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

function buildContentDownloadCountsPayload(counts = {}) {
  return {
    success: true,
    counts,
    updatedAt: Date.now(),
    uniqueUsers: true,
    source: 'database-aggregate'
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
    const downloadCountsPayload = buildContentDownloadCountsPayload(payload.contentDownloadCounts);

    const trendingViewPayload = buildTrendingViewPayload(payload);
    if (targetSocket && targetSocket.connected) {
      targetSocket.emit('trending_data', trendingViewPayload);
      targetSocket.emit('content_download_counts', downloadCountsPayload);
    } else {
      io.emit('trending_data', trendingViewPayload);
      io.emit('content_download_counts', downloadCountsPayload);
    }
    return payload;
  } catch (err) {
    console.error('[TRENDING DB EMIT ERROR]:', err);
    const fallbackPayload = trendingCache
      ? { ...trendingCache, stale: true, unavailable: false }
      : { topDownloads: [], topWishlist: [], contentDownloadCounts: {}, stale: false, unavailable: true };
    const fallbackDownloadCountsPayload = buildContentDownloadCountsPayload(fallbackPayload.contentDownloadCounts || {});
    fallbackDownloadCountsPayload.stale = fallbackPayload.stale === true;
    fallbackDownloadCountsPayload.unavailable = fallbackPayload.unavailable === true;
    if (targetSocket && targetSocket.connected) {
      targetSocket.emit('trending_data', buildTrendingViewPayload(fallbackPayload));
      targetSocket.emit('content_download_counts', fallbackDownloadCountsPayload);
    }
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
  return payload;
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
      || CASE WHEN NOT ($1::jsonb ? 'passwordResetPending') AND data ? 'passwordResetPending' THEN jsonb_build_object('passwordResetPending', data->'passwordResetPending') ELSE '{}'::jsonb END
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
  const hasLocalSession = getSocketsByUserName(name).some(client => client && client.connected);
  if (!hasLocalSession) {
    deferServerTask('USER CACHE COMPACT AFTER SAVE', () => compactCachedUser(name), 250);
  }
}

async function saveAdminState(key, data) {
  await pool.query(
    'INSERT INTO admin_state (state_key, data) VALUES ($1, $2) ON CONFLICT (state_key) DO UPDATE SET data = $2',
    [key, data]
  );
}

function cleanChatMessage(message = {}) {
  const clean = { ...(message || {}) };
  delete clean._dbId;
  return clean;
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
        io.emit('chat_message', cleanChatMessage(message));
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
        io.emit('chat_history', getPublicChatHistory());
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
      registeredUsers: Object.keys(userDatabase).length
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


function buildFullProfileSyncPayload(name, user = {}, sourceSocketId = null) {
  const safe = normalizeUserRecord(name, user || {});
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
      countersData: safe.countersData || {},
      themeColor: normalizeThemeColorServer(safe.themeColor || (safe.settingsData && safe.settingsData.themeColor) || '#0070cc'),
      themeColorUpdatedAt: getUserThemeColorUpdatedAt(safe),
      settingsData: { ...normalizeProfileRealtimeSettings(safe.settingsData || {}), ...getPublicProfileSettings(safe) }
    }
  };
}

function emitProfileSync(name, sourceSocketId = null) {
  if (!name || !userDatabase[name]) return;
  const payload = buildFullProfileSyncPayload(name, userDatabase[name], sourceSocketId);
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

  if (!activeNames.length) return;

  const dbRes = await pool.query('SELECT name, data FROM users WHERE name = ANY($1)', [activeNames]);
  dbRes.rows.forEach(row => {
    const name = row.name;
    const dbUser = normalizeUserRecord(name, row.data || {});
    const localUser = userDatabase[name] || {};
    const dbVersion = Number(dbUser.profileUpdatedAt || 0);
    const localVersion = Number(localUser.profileUpdatedAt || 0);

    if (!dbVersion || dbVersion <= localVersion) return;

    userDatabase[name] = {
      ...dbUser,
      online: localUser.online === true,
      id: localUser.id || dbUser.id,
      lastSeen: localUser.lastSeen || dbUser.lastSeen || Date.now()
    };

    emitProfileSync(name, null);
    emitPublicProfileBannerUpdate(name, userDatabase[name]);
  });
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
      counts: changes && changes.counts === true
    }
  };

  try {
    await pool.query('SELECT pg_notify($1, $2)', ['profile_sync', JSON.stringify(payload)]);
  } catch (err) {
    console.error('[PROFILE NOTIFY ERROR]:', err);
  }
}

async function initProfileSyncNotifications() {
  if (profileSyncNotifyClient) return;

  const client = new Client(pgConnectionOptions);
  profileSyncNotifyClient = client;

  client.on('notification', async (message) => {
    if (!message || !['profile_sync', 'presence_sync'].includes(message.channel)) return;

    try {
      const data = JSON.parse(message.payload || '{}');
      const name = normalizeText(data.name, '');
      if (!name || data.instanceId === INSTANCE_ID) return;

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

      const refreshedUser = hasLocalSession
        ? await refreshSingleUserCacheFromDb(name)
        : await refreshSingleUserSummaryFromDb(name);
      if (!refreshedUser) {
        invalidateOnlineListCache("profile-sync-listen-missing");
        deferServerTask('PROFILE LISTEN ONLINE LIST', () => emitOnlineList(), 1000);
        return;
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
      if (hasLocalSession) emitProfileSync(name, data.sourceSocketId || null);
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
    console.log('[PROFILE SYNC] Postgres LISTEN enabled.');
    console.log('[PRESENCE SYNC] Postgres LISTEN enabled.');
  } catch (err) {
    if (profileSyncNotifyClient === client) profileSyncNotifyClient = null;
    console.error('[PROFILE LISTEN INIT ERROR]:', err && err.message ? err.message : err);
    await client.end().catch(() => {});
    scheduleProfileSyncReconnect(isPgConnectionLimitError(err) ? 15000 : 5000);
  }
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
    }, isPasswordResetDisconnect ? 250 : 1200);
  });
}

async function upsertPresenceForSocket(socket, name) {
  if (!socket || !name) return;
  const shouldAnnouncePresence = socket.__presenceAnnounced !== true;
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
      targetSocket.emit('online_list', list);
      targetSocket.emit('online_count', { count: getOnlineCountFromList(list) });
      return list;
    }

    const signature = buildOnlineListSignature(list);
    if (ONLINE_LIST_UNCHANGED_SKIP_ENABLED && options.force !== true && signature === lastBroadcastOnlineListSignature) {
      return list;
    }

    lastBroadcastOnlineListSignature = signature;
    io.emit('online_list', list);
    io.emit('online_count', { count: getOnlineCountFromList(list) });
    return list;
  } catch (err) {
    console.error('[PRESENCE SYNC ERROR]:', err);
    const fallback = Array.isArray(lastKnownOnlineList) ? lastKnownOnlineList : [];

    // Never broadcast a fake empty presence list after a temporary DB/reconnect hiccup.
    // Mobile browsers can resume before Postgres answers, and replacing everyone with
    // [] is what made the UI show "0 Online" until the next good refresh.
    if (targetSocket && fallback.length > 0) {
      targetSocket.emit('online_list', fallback);
      targetSocket.emit('online_count', { count: getOnlineCountFromList(fallback), stale: true });
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

function generateTemporaryPassword() {
  return `PSN-${Math.random().toString(36).slice(2, 8).toUpperCase()}`;
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
  userDatabase[targetName].passwordResetRequired = true;
  userDatabase[targetName].passwordResetAt = new Date(resetRequestedAt).toISOString();
  userDatabase[targetName].passwordResetExpiresAt = resetExpiresAt;
  userDatabase[targetName].passwordResetBy = adminName || "Admin";
  delete userDatabase[targetName].passwordResetPending;
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
    serverLog: adminConnected ? serverLog : []
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

function deferAfterAuthSettle(socket, label, taskFn, totalDelayMs = POST_AUTH_PROFILE_SYNC_DELAY_MS) {
  deferServerTask(label, taskFn, getPostAuthRemainingDelay(socket, totalDelayMs));
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
      
      const dbRes = await queryDbWithRetry('SELECT data FROM users WHERE name = $1', [name], { attempts: 3, label: 'AUTH USER LOOKUP' });
      let dbUser = dbRes.rows.length > 0 ? normalizeUserRecord(name, dbRes.rows[0].data || {}) : null;
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
        const legacyResetPending = dbUser.passwordResetPending === true;
        const resetRequired = dbUser.passwordResetRequired === true || legacyResetPending;
        const parsedResetAt = Date.parse(String(dbUser.passwordResetAt || ''));
        const explicitResetExpiresAt = Number(dbUser.passwordResetExpiresAt || 0);
        const resetExpiresAt = explicitResetExpiresAt > 0
          ? explicitResetExpiresAt
          : (Number.isFinite(parsedResetAt) ? parsedResetAt + PASSWORD_RESET_WINDOW_MS : 0);

        if (resetRequired) {
          if (!resetExpiresAt || resetExpiresAt <= Date.now()) {
            delete dbUser.passwordResetRequired;
            delete dbUser.passwordResetPending;
            delete dbUser.passwordResetExpiresAt;
            dbUser.profileUpdatedAt = Date.now();
            await queryDbWithRetry('UPDATE users SET data = $1 WHERE name = $2', [dbUser, name], { attempts: 2, label: 'PASSWORD RESET EXPIRED' });
            socket.emit('password_reset_expired', {
              targetName: name,
              by: dbUser.passwordResetBy || 'Admin',
              resetAt: dbUser.passwordResetAt || null
            });
            return;
          }

          if (passwordResetSubmission === true) {
            const nextPassword = String(password || '').trim();
            if (nextPassword.length < 4) {
              socket.emit('auth_error', 'New password is too short. Minimum 4 characters.');
              return;
            }

            dbUser.passwordHash = await bcrypt.hash(nextPassword, 10);
            delete dbUser.password;
            delete dbUser.passwordResetRequired;
            delete dbUser.passwordResetPending;
            delete dbUser.passwordResetExpiresAt;
            dbUser.passwordResetCompletedAt = new Date().toISOString();
            dbUser.profileUpdatedAt = Date.now();
            await queryDbWithRetry('UPDATE users SET data = $1 WHERE name = $2', [dbUser, name], { attempts: 2, label: 'PASSWORD RESET COMPLETE' });
            console.log(`[AUTH] ${name} created a new password after an administrator reset.`);
          } else {
            socket.emit('password_reset_required', {
              targetName: name,
              by: dbUser.passwordResetBy || 'Admin',
              resetAt: dbUser.passwordResetAt || null,
              expiresAt: resetExpiresAt,
              expiresInMs: Math.max(0, resetExpiresAt - Date.now())
            });
            return;
          }
        }

        if (!dbUser.passwordHash) {
          if (isNewAccount === true) {
            socket.emit('auth_error', 'This Online ID is already taken...');
            return;
          }

          const legacyPassword = typeof dbUser.password === 'string' ? dbUser.password : '';
          if (!legacyPassword) {
            const recoveryPassword = String(password || '').trim();
            if (recoveryPassword.length < 4) {
              socket.emit('auth_error', 'Enter a password with at least 4 characters to recover this account.');
              return;
            }
            dbUser.passwordHash = await bcrypt.hash(recoveryPassword, 10);
            dbUser.passwordRecoveredAt = new Date().toISOString();
            dbUser.passwordRecoverySource = 'missing_credentials_login';
            dbUser.profileUpdatedAt = Date.now();
            await queryDbWithRetry('UPDATE users SET data = $1 WHERE name = $2', [dbUser, name], { attempts: 2, label: 'MISSING CREDENTIAL RECOVERY' });
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
            await queryDbWithRetry('UPDATE users SET data = $1 WHERE name = $2', [dbUser, name], { attempts: 2, label: 'LEGACY PASSWORD MIGRATION' });
            console.log(`[AUTH] Migrated legacy password for ${name} to bcrypt.`);
          }
        }

        const match = await bcrypt.compare(password, dbUser.passwordHash);
        
        if (match) {
          socket.__passwordResetRevoked = false;
          socket.userName = name;
          socket.isAdmin = isAdmin;
          socket.role = getUserRole(name, dbUser);

          const serverUser = normalizeUserRecord(name, dbUser);

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
          normalizeProfileArrayPayloads(userDatabase[name]);
          userCacheMeta[name] = Date.now();
          fullUserCacheNames.add(name);
          
          markSocketAuthenticated(socket);
          invalidateOnlineListCache('auth-existing-db');
          deferServerTask('AUTH EXISTING PRESENCE', () => upsertPresenceForSocket(socket, name), 250);

          console.log(`[NETWORK] ${name} logged in. Admin: ${isAdmin}`);
          deferServerTask('AUTH LOGIN LOG', async () => {
            await addServerLog('login', `${name} signed in${isAdmin ? ' as admin' : ''}`, { socketId: socket.id, role: getUserRole(name, userDatabase[name]) }, name);
          }, 2400);

          socket.emit('auth_success', { 
            name, 
            userData: buildFullProfileSyncPayload(name, userDatabase[name], socket.id).userData,
            isAdmin: isAdmin,
            role: getUserRole(name, userDatabase[name]),
            isModerator: isUserModerator(name, userDatabase[name]),
            serverAuthoritative: true
          });

          socket.emit('pinned_list', pinnedMessages);
          deferServerTask('POST AUTH CHAT HISTORY', () => socket.emit('chat_history', getPublicChatHistory()), POST_AUTH_CHAT_HISTORY_DELAY_MS);
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
        normalizeProfileArrayPayloads(userDatabase[name]);
        userCacheMeta[name] = Date.now();
        fullUserCacheNames.add(name);
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
        deferServerTask('AUTH SIGNUP LOG', async () => {
          await addServerLog('signup', `${name} created an account${isAdmin ? ' as admin' : ''}`, { socketId: socket.id, role: getUserRole(name, userDatabase[name]) }, name);
        }, 0);

        socket.emit('auth_success', { 
          name, 
          userData: buildFullProfileSyncPayload(name, userDatabase[name], socket.id).userData,
          isAdmin: isAdmin,
          role: getUserRole(name, userDatabase[name]),
          isModerator: isUserModerator(name, userDatabase[name]),
          serverAuthoritative: true
        });

        socket.emit('pinned_list', pinnedMessages);
        deferServerTask('POST AUTH CHAT HISTORY', () => socket.emit('chat_history', getPublicChatHistory()), POST_AUTH_CHAT_HISTORY_DELAY_MS);
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
    const fullUser = await ensureFullUserCacheForWrite(name);
    if (!fullUser) return;

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
    userProfileWriteInFlight.add(name);
    try {
      await upsertPresenceForSocket(socket, name);
      userCacheMeta[name] = Date.now();
      const savedUser = await updateUserDataPreservingCredentials(name, userDatabase[name], 'SETTINGS SAVE');
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

    deferServerTask('SETTINGS PROFILE NOTIFY', () => notifyProfileSyncAcrossInstances(name, sourceSocketId, userDatabase[name].profileUpdatedAt), 0);
  });

  socket.on('update_profile', async (userData) => {
    const name = socket.userName;
    if (!name || !userDatabase[name]) return;
    const fullUser = await ensureFullUserCacheForWrite(name);
    if (!fullUser) return;
    userData = (userData && typeof userData === "object") ? userData : {};
    const incomingSettingsData = (userData.settingsData && typeof userData.settingsData === "object") ? userData.settingsData : null;
    const previousCountryCode = name && userDatabase[name] ? getUserCountryCode(userDatabase[name]) : "";
    normalizeIncomingProfileCountry(userData, incomingSettingsData);
    let shouldBroadcastProfileBanner = false;
    let shouldForceProfileSyncToSource = false;
    const shouldEmitTrendingUpdate = profileUpdateTouchesTrending(userData || {});
    if (name && userDatabase[name]) {
        
        if (incomingSettingsData) {
            const mergedSettings = mergeProfileSettingsByTimestamp(userDatabase[name].settingsData || {}, incomingSettingsData, {
                currentFallback: normalizeTimestampValue(userDatabase[name].profileUpdatedAt),
                incomingFallback: normalizeTimestampValue(userData.profileCardStyleUpdatedAt || userData.profileUpdatedAt)
            });
            userDatabase[name].settingsData = mergedSettings.settingsData;
            const themeMerge = reconcileIncomingThemeColor(userDatabase[name], userData, incomingSettingsData || {});
            shouldBroadcastProfileBanner = mergedSettings.bannerAccepted === true || themeMerge.accepted === true;
            shouldForceProfileSyncToSource = mergedSettings.settingsRejected === true || mergedSettings.bannerRejected === true || themeMerge.rejected === true;
            delete userData.settingsData;
            delete userData.themeColor;
            delete userData.themeColorUpdatedAt;
            delete userData.themeUpdatedAt;
        } else {
            const themeMerge = reconcileIncomingThemeColor(userDatabase[name], userData, {});
            shouldBroadcastProfileBanner = themeMerge.accepted === true;
            shouldForceProfileSyncToSource = themeMerge.rejected === true;
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
            delete userData.passwordResetPending;
            delete userData.passwordResetCompletedAt;
            delete userData.passwordRecoveredAt;
            delete userData.passwordRecoverySource;
        }

        if (isUserBanned(userDatabase[name]) && !ADMIN_USERS.includes(name)) {
            socket.emit('auth_error', 'This account is banned.');
            return;
        }

        if (hasObjectPayload(userData.trophiesData)) {
            if (!shouldAcceptIncomingTrophies(userDatabase[name], userData)) {
                delete userData.trophiesData;
                delete userData.trophies;
                delete userData.level;
                delete userData.xp;
            }
        }

        userData = reconcileIncomingDownloads(userDatabase[name], userData || {});
        userData = reconcileIncomingProfileArrays(userDatabase[name], userData || {});
        const publicCountsChanged = profileUpdateTouchesPublicCounts(userData);
        
        Object.assign(userDatabase[name], userData);
        const currentCountryCode = getUserCountryCode(userDatabase[name]);
        if (currentCountryCode) {
            userDatabase[name].countryCode = currentCountryCode;
            userDatabase[name].settingsData = userDatabase[name].settingsData && typeof userDatabase[name].settingsData === "object" && !Array.isArray(userDatabase[name].settingsData)
                ? { ...userDatabase[name].settingsData, countryCode: currentCountryCode }
                : { countryCode: currentCountryCode };
        }
        const countryChanged = currentCountryCode !== previousCountryCode;
        if (countryChanged) shouldBroadcastProfileBanner = true;
        if (Array.isArray(userDatabase[name].downloadsData)) userDatabase[name].downloads = userDatabase[name].downloadsData.length;
        userDatabase[name].downloadsClearedAt = normalizeTimestampValue(userDatabase[name].downloadsClearedAt);
        normalizeProfileArrayPayloads(userDatabase[name]);
        userDatabase[name].lastSeen = Date.now();
        userDatabase[name].profileUpdatedAt = Date.now();
        userProfileWriteInFlight.add(name);
        try {
            await upsertPresenceForSocket(socket, name);
            userCacheMeta[name] = Date.now();
            const savedUser = await updateUserDataPreservingCredentials(name, userDatabase[name], 'PROFILE SAVE');
            if (!savedUser) return;
            invalidateOnlineListCache('profile-update-save');
        } catch (err) {
            console.error(`[DATABASE ERROR] Failed to save profile for ${name}:`, err);
        } finally {
            userProfileWriteInFlight.delete(name);
        }

        if (publicCountsChanged) emitProfileCountsUpdate(name, userDatabase[name]);

        if (shouldEmitTrendingUpdate) {
            invalidateTrendingCache();
            scheduleTrendingRefreshBroadcast(900);
        }

        deferServerTask('PROFILE ONLINE LIST', () => emitOnlineList(), 450);
        if (shouldBroadcastProfileBanner) {
            emitPublicProfileBannerUpdate(name, userDatabase[name]);
        }
        emitProfileSync(name, shouldForceProfileSyncToSource ? null : socket.id);
        const trophiesChanged = !!userData.trophiesData;
        deferServerTask('PROFILE NOTIFY', () => notifyProfileSyncAcrossInstances(
            name,
            shouldForceProfileSyncToSource ? null : socket.id,
            userDatabase[name].profileUpdatedAt,
            { trending: shouldEmitTrendingUpdate, trophies: trophiesChanged, counts: publicCountsChanged }
        ), 0);

        if (trophiesChanged) {
            invalidateGlobalTrophyStatsCache();
            scheduleTrophyStatsRefreshBroadcast(900);
        }
    }
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

  socket.on('request_profile_sync', async (data = {}) => {
    const name = socket.userName;
    if (!name || !userDatabase[name]) return;

    try {
      const sendProfileSync = async () => {
        if (data && data.forceRefresh === true) {
          await refreshSingleUserCacheFromDb(name);
        }
        socket.emit('profile_sync', buildFullProfileSyncPayload(name, userDatabase[name], null));
      };

      if (!(data && data.forceRefresh === true) && getPostAuthRemainingDelay(socket, POST_AUTH_PROFILE_SYNC_DELAY_MS) > 0) {
        deferAfterAuthSettle(socket, 'REQUEST PROFILE SYNC', sendProfileSync, POST_AUTH_PROFILE_SYNC_DELAY_MS);
        return;
      }

      await sendProfileSync();
    } catch (err) {
      console.error('[REQUEST PROFILE SYNC ERROR]:', err);
    }
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

  socket.on('request_chat_history', () => {
    try {
      if (!socket.userName) return;
      const now = Date.now();
      if (socket.__lastChatHistoryRequestAt && now - socket.__lastChatHistoryRequestAt < 900) return;
      socket.__lastChatHistoryRequestAt = now;
      socket.emit('chat_history', getPublicChatHistory());
    } catch (err) {
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
        socket.emit('admin_users_results', { query, filter, offset, limit, total: names.length, results });
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
      socket.emit('content_download_counts', buildContentDownloadCountsPayload((trendingCache && trendingCache.contentDownloadCounts) || {}));
    }
  });

  socket.on('request_content_download_counts', async (_request = {}, callback) => {
    try {
      const activity = await getTrendingActivity();
      const payload = buildContentDownloadCountsPayload(activity.contentDownloadCounts);

      if (typeof callback === 'function') callback(payload);
      else socket.emit('content_download_counts', payload);
    } catch (err) {
      console.error('[CONTENT DOWNLOAD COUNTS ERROR]:', err);
      const payload = {
        success: false,
        counts: {},
        updatedAt: Date.now(),
        uniqueUsers: true,
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
        socket.emit('kick_error', error);
        respond(error);
        return;
      }
      if (!canModerateTarget(socket, targetName)) {
        const error = { success: false, message: 'You cannot kick this user.' };
        socket.emit('kick_error', error);
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
        socket.emit('admin_command_result', { command: 'ban', ...result });
      } else {
        socket.emit('admin_command_error', { command: 'ban', ...result });
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
        socket.emit('admin_command_result', { command: 'unban', ...result });
      } else {
        socket.emit('admin_command_error', { command: 'unban', ...result });
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
        socket.emit('admin_command_result', { command: 'role', targetName, ...result });
      } else {
        socket.emit('admin_command_error', { command: 'role', targetName, ...result });
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
        socket.emit('admin_command_result', { command: 'resetpassword', ...result });
      } else {
        socket.emit('admin_command_error', { command: 'resetpassword', ...result });
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
      io.emit('chat_cleared', { by: senderName || 'Admin', backedUpCount });
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
      io.emit('chat_message', publicMessage);
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

      adminState.maintenance = normalizeMaintenanceState({
        ...(data || {}),
        by: socket.userName || (data && data.by) || "Admin",
        at: new Date().toISOString()
      });

      await saveAdminState(ADMIN_STATE_KEYS.maintenance, adminState.maintenance);
      io.emit('maintenance_mode', adminState.maintenance);
      io.emit('admin_maintenance_mode', adminState.maintenance);
      emitToAdmins('admin_state', {
        maintenance: adminState.maintenance,
        chatControls: adminState.chatControls,
        pinnedAnnouncement: adminState.pinnedAnnouncement || null,
        reports: adminReports,
        serverLog
      });
      await addModerationLog(adminState.maintenance.enabled ? 'maintenance_on' : 'maintenance_off', adminState.maintenance.enabled ? 'Enabled maintenance mode' : 'Disabled maintenance mode', adminState.maintenance, socket.userName || 'Admin');
      respond({ success: true, state: adminState.maintenance });
    } catch (err) {
      console.error('[ADMIN MAINTENANCE ERROR]:', err);
      respond({ success: false, message: "Server error while updating maintenance mode." });
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
        registeredUsers: socket.isAdmin === true ? Object.keys(userDatabase).length : 0
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
        serverLog
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
            io.emit('message_reaction', data);
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
                
                io.emit('message_edited', { 
                    msgId: data.msgId, 
                    newText: msg.text, 
                    type: 'poll', 
                    content: poll,
                    editedByAdmin: msg.editedByAdmin 
                });
                
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

  socket.on('mark_as_read', async (data) => {
    const msg = messageHistory.find(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msg && msg.user !== data.user) {
        if (!msg.seenBy) msg.seenBy = [];
        if (!msg.seenAt || typeof msg.seenAt !== 'object' || Array.isArray(msg.seenAt)) msg.seenAt = {};
        if (!msg.seenBy.includes(data.user)) {
            msg.seenBy.push(data.user);
            msg.seenAt[data.user] = new Date().toISOString();

            try {
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [cleanChatMessage(msg), msg.time]);
                io.emit('message_seen', { msgId: data.msgId, seenBy: msg.seenBy, seenAt: msg.seenAt });
            } catch (err) { console.error("Seen Mark Error:", err); }
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

            io.emit('message_deleted', data.msgId);
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
    io.emit('chat_cleared', { by: byUser, user: byUser, backedUpCount });

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
                socket.emit('kick_error', { targetId: data.targetId, targetName, message: 'You cannot kick this user.' });
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
