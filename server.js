const express = require('express');
const http = require('http');
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
const PROFILE_SYNC_INTERVAL_MS = Math.max(10000, parseInt(process.env.PROFILE_SYNC_INTERVAL_MS || "15000", 10) || 15000);
const ENABLE_PROFILE_PERIODIC_SYNC = process.env.ENABLE_PROFILE_PERIODIC_SYNC === "1";
const POST_AUTH_CHAT_HISTORY_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_CHAT_HISTORY_DELAY_MS || "180", 10) || 180);
const POST_AUTH_ADMIN_STATE_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_ADMIN_STATE_DELAY_MS || "550", 10) || 550);
const POST_AUTH_ONLINE_LIST_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_ONLINE_LIST_DELAY_MS || "1400", 10) || 1400);
const POST_AUTH_PROFILE_SYNC_DELAY_MS = Math.max(0, parseInt(process.env.POST_AUTH_PROFILE_SYNC_DELAY_MS || "1800", 10) || 1800);
const USER_CACHE_REFRESH_INTERVAL_MS = 30000;
const USER_CACHE_WARMUP_INTERVAL_MS = 120000;
const DEFAULT_MAINTENANCE_MESSAGE = "The service is under maintenance. Please try again soon.";
const VALID_USER_ROLES = new Set(["user", "trusted", "mod", "admin"]);
const ADMIN_STATE_KEYS = {
  maintenance: "maintenance",
  chatControls: "chat_controls",
  pinnedAnnouncement: "pinned_announcement"
};

const PG_POOL_MAX = Math.max(1, Math.min(5, parseInt(process.env.PG_POOL_MAX || process.env.DB_POOL_MAX || "3", 10) || 3));
const PG_CONNECTION_TIMEOUT_MS = Math.max(1000, parseInt(process.env.PG_CONNECTION_TIMEOUT_MS || "5000", 10) || 5000);
const PG_IDLE_TIMEOUT_MS = Math.max(5000, parseInt(process.env.PG_IDLE_TIMEOUT_MS || "10000", 10) || 10000);
const PG_QUERY_TIMEOUT_MS = Math.max(5000, parseInt(process.env.PG_QUERY_TIMEOUT_MS || "20000", 10) || 20000);
const PG_STATEMENT_TIMEOUT_MS = Math.max(5000, parseInt(process.env.PG_STATEMENT_TIMEOUT_MS || "15000", 10) || 15000);
const PG_MAX_USES = Math.max(100, parseInt(process.env.PG_MAX_USES || "750", 10) || 750);
const ONLINE_LIST_CACHE_MS = Math.max(250, parseInt(process.env.ONLINE_LIST_CACHE_MS || "1200", 10) || 1200);
const ONLINE_LIST_UNCHANGED_SKIP_ENABLED = process.env.ONLINE_LIST_SKIP_UNCHANGED !== "0";

const pgConnectionOptions = {
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  application_name: String(`psn-db-${INSTANCE_ID}`).slice(0, 63),
  connectionTimeoutMillis: PG_CONNECTION_TIMEOUT_MS,
  statement_timeout: PG_STATEMENT_TIMEOUT_MS,
  query_timeout: PG_QUERY_TIMEOUT_MS
};

const pool = new Pool({
  ...pgConnectionOptions,
  max: PG_POOL_MAX,
  min: 0,
  idleTimeoutMillis: PG_IDLE_TIMEOUT_MS,
  maxUses: PG_MAX_USES,
  allowExitOnIdle: false
});
let profileSyncNotifyClient = null;
let profileSyncReconnectTimer = null;

let onlineListCache = null;
let onlineListCacheAt = 0;
let onlineListBuildInFlight = null;
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



app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

app.get('/debug-db', async (req, res) => {
  try {
    const info = await pool.query(`
      SELECT
        current_database() AS database,
        current_schema() AS schema,
        current_user AS user,
        current_setting('search_path') AS search_path,
        to_regclass('chat') AS active_chat_table,
        to_regclass('public.chat') AS public_chat_table
    `);

    const chat = await pool.query(`
      SELECT COUNT(*)::int AS total, COALESCE(MAX(id), 0)::int AS max_id
      FROM chat
    `);

    res.json({
      instance: INSTANCE_ID,
      startedAt: new Date(SERVER_STARTED_AT).toISOString(),
      db: info.rows[0],
      chat: chat.rows[0],
      memoryMessages: messageHistory.length,
      cachedUsers: Object.keys(userDatabase).length,
      userCacheLastFullRefresh: userCacheLastFullRefresh ? new Date(userCacheLastFullRefresh).toISOString() : null,
      lastChatDbId
    });
  } catch (err) {
    res.status(500).json({
      error: err.message,
      instance: INSTANCE_ID
    });
  }
});

let userDatabase = {};
let userCacheMeta = {};
let userCacheLastFullRefresh = 0;
let userCacheRefreshInFlight = null;
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
    const stateRes = await pool.query('SELECT state_key, data FROM admin_state');
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

  await pool.query('DELETE FROM presence_sessions WHERE instance_id = $1 OR last_seen < NOW() - INTERVAL \'90 seconds\'', [INSTANCE_ID]);

  await refreshAllUsersCacheFromDb({ preserveOnline: false });

  await refreshChatHistoryFromDb();
  
  const pinnedRes = await pool.query('SELECT data FROM pinned_messages ORDER BY id ASC');
  pinnedMessages = pinnedRes.rows.map(r => r.data);

  await refreshAdminStateFromDb();

  const reportsRes = await pool.query('SELECT data FROM reports WHERE resolved = false ORDER BY created_at DESC LIMIT 100');
  adminReports = reportsRes.rows.map(r => r.data);

  const modLogRes = await pool.query('SELECT entry FROM moderation_log ORDER BY created_at DESC LIMIT 100');
  moderationLog = modLogRes.rows.map(r => r.entry);

  const serverLogRes = await pool.query('SELECT entry FROM server_log ORDER BY created_at DESC LIMIT 120');
  serverLog = serverLogRes.rows.map(r => r.entry);

  console.log(`[DB] Database initialized. ${messageHistory.length} messages, ${pinnedMessages.length} pins, ${Object.keys(userDatabase).length} users loaded.`);
}

initDb()
  .then(async () => {
    await initProfileSyncNotifications();
    startUserCacheWarmup();
  })
  .catch(console.error);



function getSanitizedOnlineList() {
  return Object.entries(userDatabase).map(([username, u]) => ({
    id: u.id,
    name: username,
    avatar: u.avatar || DEFAULT_AVATAR,
    isAdmin: isUserAdmin(username, u),
    role: getUserRole(username, u),
    banned: isUserBanned(u),
    banReason: u.banReason || "",
    level: u.level || 1,
    joined: u.joined || '2026',
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
  const list = getSanitizedOnlineList();
  onlineListCache = list;
  onlineListCacheAt = Date.now();
  return list;
}
async function calculateGlobalTrophyStatsFromDb() {
  const stats = {};

  const trophyRes = await pool.query(`
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
  `);

  trophyRes.rows.forEach(row => {
    if (row.trophy_id) stats[row.trophy_id] = Number(row.percentage) || 0;
  });

  return stats;
}


function normalizeText(value, fallback = "") {
  return String(value === undefined || value === null ? fallback : value).trim();
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

  if (!normalized.banned) {
    delete normalized.banReason;
    delete normalized.bannedBy;
    delete normalized.bannedAt;
  }

  return normalized;
}



function hasObjectPayload(value) {
  return !!(value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length > 0);
}

function hasArrayPayload(value) {
  return Array.isArray(value) && value.length > 0;
}

function isDefaultAvatarValue(value) {
  const text = normalizeText(value, "");
  return !text || text === DEFAULT_AVATAR || /\/avatars\/default\.png(?:$|\?)/.test(text);
}

function preferLocalArrayPayload(currentValue, localValue) {
  const currentLength = Array.isArray(currentValue) ? currentValue.length : 0;
  const localLength = Array.isArray(localValue) ? localValue.length : 0;
  return localLength > currentLength ? localValue : currentValue;
}

function preferLocalObjectPayload(currentValue, localValue) {
  const currentSize = hasObjectPayload(currentValue) ? Object.keys(currentValue).length : 0;
  const localSize = hasObjectPayload(localValue) ? Object.keys(localValue).length : 0;
  return localSize > currentSize ? localValue : currentValue;
}

function countUnlockedTrophiesPayload(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return 0;
  return Object.values(value).reduce((count, trophy) => {
    if (!trophy || typeof trophy !== 'object') return count;
    const unlocked = String(trophy.unlocked || '').toLowerCase();
    return count + (unlocked === 'true' || unlocked === '1' || unlocked === 'yes' ? 1 : 0);
  }, 0);
}

function preferBestTrophiesPayload(currentValue, localValue) {
  const currentSize = hasObjectPayload(currentValue) ? Object.keys(currentValue).length : 0;
  const localSize = hasObjectPayload(localValue) ? Object.keys(localValue).length : 0;
  const currentUnlocked = countUnlockedTrophiesPayload(currentValue);
  const localUnlocked = countUnlockedTrophiesPayload(localValue);
  if (localUnlocked > currentUnlocked) return localValue;
  if (localUnlocked === currentUnlocked && localSize > currentSize) return localValue;
  return currentValue;
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
    themeColorUpdatedAt
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
    themeColorUpdatedAt: settingsData.themeColorUpdatedAt
  });
}

function getProfileArrayPayload(value) {
  return Array.isArray(value) ? value : [];
}

function hasOwnPayload(target = {}, key = '') {
  return Object.prototype.hasOwnProperty.call(target || {}, key);
}

function setProfileArrayPayload(target = {}, key = '', list = [], version = 0) {
  const sync = PROFILE_ARRAY_SYNC_KEYS[key];
  if (!sync) return target;
  const safeList = Array.isArray(list) ? list : [];
  target[key] = safeList;
  target[sync.countKey] = safeList.length;
  const safeVersion = normalizeTimestampValue(version);
  if (safeVersion) target[sync.versionKey] = safeVersion;
  else target[sync.versionKey] = normalizeTimestampValue(target[sync.versionKey]);
  return target;
}

function normalizeProfileArrayPayloads(target = {}) {
  Object.keys(PROFILE_ARRAY_SYNC_KEYS).forEach(key => {
    const sync = PROFILE_ARRAY_SYNC_KEYS[key];
    const list = Array.isArray(target[key]) ? target[key] : [];
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

    const currentList = getProfileArrayPayload(currentUser[key]);
    const incomingList = getProfileArrayPayload(incomingUser[key]);
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

    if (acceptIncoming) {
      incomingUser[key] = incomingList;
      incomingUser[sync.countKey] = incomingList.length;
      incomingUser[sync.versionKey] = incomingVersion || normalizeTimestampValue(incomingUser.profileUpdatedAt) || Date.now();
    } else {
      delete incomingUser[key];
      delete incomingUser[sync.countKey];
      delete incomingUser[sync.versionKey];
    }
  });

  return incomingUser;
}

function applyLocalRecoveryArrayPayload(merged = {}, localData = {}, key = '') {
  const sync = PROFILE_ARRAY_SYNC_KEYS[key];
  if (!sync) return merged;

  const dbList = getProfileArrayPayload(merged[key]);
  const localList = getProfileArrayPayload(localData[key]);
  const dbVersion = normalizeTimestampValue(merged[sync.versionKey]);
  const localVersion = normalizeTimestampValue(localData[sync.versionKey]);
  const dbHasItems = dbList.length > 0;
  const localHasItems = localList.length > 0;

  if (localVersion && (!dbVersion || localVersion > dbVersion)) {
    return setProfileArrayPayload(merged, key, localList, localVersion);
  }

  if (!dbVersion && !dbHasItems && localHasItems) {
    return setProfileArrayPayload(merged, key, localList, localVersion || normalizeTimestampValue(localData.profileUpdatedAt) || Date.now());
  }

  return setProfileArrayPayload(merged, key, dbList, dbVersion);
}

function applyDownloadsClearedState(target = {}, clearAt = 0) {
  const normalizedClearAt = normalizeTimestampValue(clearAt);
  if (!normalizedClearAt) return target;
  target.downloadsClearedAt = normalizedClearAt;
  target.downloadsData = [];
  target.downloads = 0;
  return target;
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
    const incomingList = incomingUser.downloadsData;
    const currentList = Array.isArray(currentUser.downloadsData) ? currentUser.downloadsData : [];
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
function mergeLocalRecoveryData(dbUser = {}, localData = {}) {
  if (!hasObjectPayload(localData)) return dbUser;
  const merged = { ...dbUser };
  const dbDownloadsClearedAt = normalizeTimestampValue(merged.downloadsClearedAt);
  const localDownloadsClearedAt = normalizeTimestampValue(localData.downloadsClearedAt);

  if (localData.avatar && isDefaultAvatarValue(merged.avatar) && !isDefaultAvatarValue(localData.avatar)) merged.avatar = localData.avatar;
  if ((!merged.joined || merged.joined === '2026') && localData.joined) merged.joined = localData.joined;
  reconcileIncomingThemeColor(merged, localData, localData.settingsData || {});
  const preferredTrophies = preferBestTrophiesPayload(merged.trophiesData, localData.trophiesData);
  if (preferredTrophies !== merged.trophiesData) merged.trophiesData = preferredTrophies;
  const preferredCounters = preferLocalObjectPayload(merged.countersData, localData.countersData);
  if (preferredCounters !== merged.countersData) merged.countersData = preferredCounters;

  merged.downloadsClearedAt = Math.max(dbDownloadsClearedAt, localDownloadsClearedAt) || 0;

  Object.keys(PROFILE_ARRAY_SYNC_KEYS).forEach(key => applyLocalRecoveryArrayPayload(merged, localData, key));

  if (localDownloadsClearedAt > dbDownloadsClearedAt) {
    const localDownloadsUpdatedAt = normalizeTimestampValue(localData.downloadsUpdatedAt || localData.profileUpdatedAt);
    if (!Array.isArray(localData.downloadsData) || localData.downloadsData.length === 0 || !localDownloadsUpdatedAt || localDownloadsClearedAt >= localDownloadsUpdatedAt) {
      applyDownloadsClearedState(merged, localDownloadsClearedAt);
      merged.downloadsUpdatedAt = Math.max(normalizeTimestampValue(merged.downloadsUpdatedAt), localDownloadsClearedAt);
    }
  }
  if (hasObjectPayload(localData.settingsData)) {
    const mergedSettings = mergeProfileSettingsByTimestamp(merged.settingsData || {}, localData.settingsData, {
      currentFallback: normalizeTimestampValue(merged.profileUpdatedAt),
      incomingFallback: normalizeTimestampValue(localData.profileCardStyleUpdatedAt || localData.profileUpdatedAt)
    });
    merged.settingsData = mergedSettings.settingsData;
  }
  ['trophies', 'level', 'xp'].forEach(key => {
    const current = Number(merged[key] || 0);
    const incoming = Number(localData[key] || 0);
    if (incoming > current) merged[key] = incoming;
  });
  if (localDownloadsClearedAt === dbDownloadsClearedAt) {
    const currentDownloads = Number(merged.downloads || 0);
    const incomingDownloads = Number(localData.downloads || 0);
    if (incomingDownloads > currentDownloads) merged.downloads = incomingDownloads;
  }
  if (Array.isArray(merged.downloadsData)) merged.downloads = merged.downloadsData.length;
  normalizeProfileArrayPayloads(merged);
  return merged;
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


async function refreshAllUsersCacheFromDb(options = {}) {
  if (userCacheRefreshInFlight) return userCacheRefreshInFlight;

  const preserveOnline = options.preserveOnline !== false;
  userCacheRefreshInFlight = (async () => {
    const now = Date.now();
    const usersRes = await pool.query('SELECT name, data FROM users ORDER BY LOWER(name) ASC');
    const nextDatabase = {};
    const nextMeta = {};

    usersRes.rows.forEach(row => {
      const username = row.name;
      const dbUser = normalizeUserRecord(username, row.data || {});
      const localUser = userDatabase[username] || {};
      const dbVersion = normalizeTimestampValue(dbUser.profileUpdatedAt);
      const localVersion = normalizeTimestampValue(localUser.profileUpdatedAt);
      const keepLocalProfile = !!(localVersion && (!dbVersion || localVersion > dbVersion));
      const baseUser = keepLocalProfile ? normalizeUserRecord(username, localUser) : dbUser;

      nextDatabase[username] = {
        ...baseUser,
        online: preserveOnline ? localUser.online === true : false,
        id: preserveOnline ? (localUser.id || baseUser.id || null) : (baseUser.id || null),
        lastSeen: preserveOnline ? (localUser.lastSeen || baseUser.lastSeen || null) : (baseUser.lastSeen || null)
      };
      nextMeta[username] = keepLocalProfile ? (userCacheMeta[username] || now) : now;
    });

    userDatabase = nextDatabase;
    userCacheMeta = nextMeta;
    userCacheLastFullRefresh = now;
    await syncPresenceOnlineFromDb();
    invalidateOnlineListCache("users-full-refresh");
    console.log(`[USER CACHE] ${Object.keys(userDatabase).length} users loaded from DB into RAM on ${INSTANCE_ID}.`);
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

  const userRes = await pool.query('SELECT data FROM users WHERE name = $1', [safeName]);
  if (!userRes.rows.length) {
    delete userDatabase[safeName];
    delete userCacheMeta[safeName];
    invalidateOnlineListCache("single-user-missing");
    return null;
  }

  const dbUser = normalizeUserRecord(safeName, userRes.rows[0].data || {});
  const localUser = userDatabase[safeName] || {};
  const preserveOnline = options.preserveOnline !== false;
  const dbVersion = normalizeTimestampValue(dbUser.profileUpdatedAt);
  const localVersion = normalizeTimestampValue(localUser.profileUpdatedAt);
  const keepLocalProfile = !options.force && !!(localVersion && (!dbVersion || localVersion > dbVersion));
  const baseUser = keepLocalProfile ? normalizeUserRecord(safeName, localUser) : dbUser;

  userDatabase[safeName] = {
    ...baseUser,
    online: preserveOnline ? localUser.online === true : false,
    id: preserveOnline ? (localUser.id || baseUser.id || null) : (baseUser.id || null),
    lastSeen: preserveOnline ? (localUser.lastSeen || baseUser.lastSeen || null) : (baseUser.lastSeen || null)
  };
  userCacheMeta[safeName] = keepLocalProfile ? (userCacheMeta[safeName] || Date.now()) : Date.now();
  invalidateOnlineListCache("single-user-refresh");
  return userDatabase[safeName];
}

async function ensureUserCacheReady() {
  if (!Object.keys(userDatabase).length) {
    await refreshAllUsersCacheFromDb();
  }
  return userDatabase;
}

async function getUserCached(name) {
  const safeName = normalizeText(name, "");
  if (!safeName) return null;

  if (userDatabase[safeName]) {
    return userDatabase[safeName];
  }

  return await refreshSingleUserCacheFromDb(safeName);
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
  return targetUser[dataKey] || (dataKey === 'trophiesData' ? {} : []);
}

function searchUsersFromCache(query, includeAdminFields = false) {
  const searchTerm = normalizeText(query, "").toLowerCase();
  const isAllCommand = (searchTerm === '@all' || searchTerm === '*');
  if (!isAllCommand && searchTerm.length < 2) return [];

  return Object.keys(userDatabase)
    .filter(username => isAllCommand || username.toLowerCase().includes(searchTerm))
    .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
    .slice(0, isAllCommand ? Object.keys(userDatabase).length : 15)
    .map(username => getPublicUserData(username, userDatabase[username], includeAdminFields));
}

function calculateGlobalTrophyStatsFromCache() {
  const users = Object.values(userDatabase);
  const totalUsers = users.length;
  const unlockedCounts = {};

  if (!totalUsers) return {};

  users.forEach(user => {
    const trophiesData = user && typeof user.trophiesData === 'object' && !Array.isArray(user.trophiesData)
      ? user.trophiesData
      : {};

    Object.entries(trophiesData).forEach(([trophyId, trophy]) => {
      const unlocked = trophy && String(trophy.unlocked || '').toLowerCase();
      if (unlocked === 'true' || unlocked === '1' || unlocked === 'yes') {
        unlockedCounts[trophyId] = (unlockedCounts[trophyId] || 0) + 1;
      }
    });
  });

  const stats = {};
  Object.entries(unlockedCounts).forEach(([trophyId, count]) => {
    stats[trophyId] = (Number(count) / totalUsers) * 100;
  });
  return stats;
}

function normalizeDownloadCountCategory(value) {
  const raw = normalizeText(value, 'games').toLowerCase().replace(/[\s-]+/g, '_');
  const aliases = {
    game: 'games', app: 'apps', demo: 'demos', dlc: 'dlcs', update: 'updates',
    avatar: 'avatars', theme: 'themes', homebrew: 'homebrew_games', port: 'ports',
    prototype: 'prototypes', emulator: 'emulators', launcher: 'launchers', tool: 'tools',
    dev_tool: 'dev_tools', manager: 'backup_manager'
  };
  return aliases[raw] || raw || 'games';
}

function normalizeDownloadCountId(value) {
  const normalized = normalizeText(value, '').toUpperCase();
  return /^(MISSING|N\/A|NONE|NULL|UNDEFINED)$/.test(normalized) ? '' : normalized;
}

function normalizeDownloadCountName(value) {
  return normalizeText(value, '').toLowerCase().replace(/&amp;/g, '&').replace(/[^a-z0-9]+/g, '');
}

function getContentDownloadCountKey(item = {}) {
  const category = normalizeDownloadCountCategory(item.category || item.rawCategory || 'games');
  const titleId = normalizeDownloadCountId(item.titleId || item.id);
  const contentId = normalizeDownloadCountId(item.contentId || item.contentID);
  const name = normalizeDownloadCountName(item.cleanName || item.name || item.title || item.rawName);

  if (category === 'games' && titleId) return `${category}|T:${titleId}`;
  if (contentId) return `${category}|C:${contentId}`;
  if (titleId && name) return `${category}|T:${titleId}|N:${name}`;
  if (titleId) return `${category}|T:${titleId}`;
  if (name) return `${category}|N:${name}`;
  return '';
}

function calculateTrendingFromCache() {
  const dlCounts = {};
  const wishCounts = {};
  const contentDownloadCounts = {};

  Object.values(userDatabase).forEach(user => {
    const downloads = Array.isArray(user.downloadsData) ? user.downloadsData : [];
    const wishlist = Array.isArray(user.wishlistData) ? user.wishlistData : [];
    const userGameDownloadIds = new Set();
    const userContentKeys = new Set();

    downloads.forEach(item => {
      const safeItem = item || {};
      const category = normalizeDownloadCountCategory(safeItem.category || safeItem.rawCategory || 'games');
      const titleId = normalizeDownloadCountId(safeItem.titleId || safeItem.id);

      // Trending Games and the counter shown on a game card use the same metric:
      // one unique user per game Title ID. DLCs, updates and repeated downloads do not inflate it.
      if (category === 'games' && titleId) userGameDownloadIds.add(titleId);

      const contentKey = getContentDownloadCountKey(safeItem);
      if (contentKey) userContentKeys.add(contentKey);
    });

    userGameDownloadIds.forEach(id => {
      dlCounts[id] = (dlCounts[id] || 0) + 1;
    });

    userContentKeys.forEach(key => {
      contentDownloadCounts[key] = (contentDownloadCounts[key] || 0) + 1;
    });

    wishlist.forEach(item => {
      const id = normalizeDownloadCountId(item && (item.titleId || item.id));
      if (id) wishCounts[id] = (wishCounts[id] || 0) + 1;
    });
  });

  const sortTop = counts => Object.entries(counts)
    .map(([id, count]) => ({ id, count: Number(count) || 0 }))
    .sort((a, b) => b.count - a.count || String(a.id).localeCompare(String(b.id)))
    .slice(0, 50);

  return {
    topDownloads: sortTop(dlCounts),
    topWishlist: sortTop(wishCounts),
    contentDownloadCounts
  };
}

function buildContentDownloadCountsPayload(counts = {}) {
  return {
    success: true,
    counts,
    updatedAt: Date.now(),
    uniqueUsers: true,
    source: 'user-cache'
  };
}

function emitTrendingFromCache(targetSocket = null) {
  try {
    const payload = calculateTrendingFromCache();
    const downloadCountsPayload = buildContentDownloadCountsPayload(payload.contentDownloadCounts);

    if (targetSocket && targetSocket.connected) {
      targetSocket.emit('trending_data', payload);
      targetSocket.emit('content_download_counts', downloadCountsPayload);
    } else {
      io.emit('trending_data', payload);
      io.emit('content_download_counts', downloadCountsPayload);
    }
    return payload;
  } catch (err) {
    console.error('[TRENDING CACHE EMIT ERROR]:', err);
    const emptyPayload = { topDownloads: [], topWishlist: [], contentDownloadCounts: {} };
    const emptyDownloadCountsPayload = buildContentDownloadCountsPayload({});

    if (targetSocket && targetSocket.connected) {
      targetSocket.emit('trending_data', emptyPayload);
      targetSocket.emit('content_download_counts', emptyDownloadCountsPayload);
    } else {
      io.emit('trending_data', emptyPayload);
      io.emit('content_download_counts', emptyDownloadCountsPayload);
    }
    return emptyPayload;
  }
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
  return await refreshSingleUserCacheFromDb(name);
}

async function refreshReportsFromDb() {
  const reportsRes = await pool.query('SELECT data FROM reports WHERE resolved = false ORDER BY created_at DESC LIMIT 100');
  adminReports = reportsRes.rows.map(r => r.data);
  return adminReports;
}

async function searchUsersFromDb(query, includeAdminFields = false) {
  await ensureUserCacheReady();
  return searchUsersFromCache(query, includeAdminFields);
}

async function getUserDataPayloadFromDb(targetName, type) {
  const safeTargetName = normalizeText(targetName, "");
  if (!safeTargetName) return null;

  let targetUser = await getUserCached(safeTargetName);
  if (!targetUser) return null;

  return getUserDataPayloadFromCache(safeTargetName, type);
}

async function saveUser(name, options = {}) {
  if (!name || !userDatabase[name]) return;
  userDatabase[name] = normalizeUserRecord(name, userDatabase[name]);
  userDatabase[name].profileUpdatedAt = userDatabase[name].profileUpdatedAt || Date.now();
  userCacheMeta[name] = Date.now();
  await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
  invalidateOnlineListCache('save-user');
  if (options.notify !== false) {
    await notifyProfileSyncAcrossInstances(name, options.sourceSocketId || null, userDatabase[name].profileUpdatedAt);
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

function getPublicChatHistory() {
  return messageHistory.map(cleanChatMessage);
}

async function refreshChatHistoryFromDb() {
  const chatRes = await pool.query('SELECT id, message FROM chat ORDER BY id DESC LIMIT $1', [MAX_CHAT_HISTORY]);
  const rows = chatRes.rows.reverse();
  messageHistory = rows.map(row => ({ ...(row.message || {}), _dbId: row.id }));
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
    const newRows = await pool.query(
      'SELECT id, message FROM chat WHERE id > $1 ORDER BY id ASC LIMIT $2',
      [lastChatDbId, MAX_CHAT_HISTORY]
    );

    if (newRows.rows.length > 0) {
      for (const row of newRows.rows) {
        const dbId = Number(row.id) || 0;
        if (messageHistory.some(m => Number(m._dbId) === dbId || (m.time && row.message && m.time === row.message.time))) {
          lastChatDbId = Math.max(lastChatDbId, dbId);
          continue;
        }
        const message = { ...(row.message || {}), _dbId: dbId };
        messageHistory.push(message);
        if (messageHistory.length > MAX_CHAT_HISTORY) messageHistory.shift();
        lastChatDbId = Math.max(lastChatDbId, dbId);
        io.emit('chat_message', cleanChatMessage(message));
      }
      return;
    }

    if (messageHistory.length > 0) {
      const meta = await pool.query('SELECT COUNT(*)::int AS total, COALESCE(MAX(id), 0)::int AS max_id FROM chat');
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
      serverLog
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


async function notifyProfileSyncAcrossInstances(name, sourceSocketId = null, profileUpdatedAt = Date.now()) {
  if (!name) return;
  const payload = {
    name,
    sourceSocketId,
    profileUpdatedAt,
    instanceId: INSTANCE_ID
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
    if (!message || message.channel !== 'profile_sync') return;

    try {
      const data = JSON.parse(message.payload || '{}');
      const name = normalizeText(data.name, '');
      if (!name || data.instanceId === INSTANCE_ID) return;

      const hasLocalSession = Array.from(io.sockets.sockets.values()).some(activeSocket => (
        activeSocket.connected && activeSocket.userName === name
      ));
      if (!hasLocalSession) return;

      const refreshedUser = await refreshSingleUserCacheFromDb(name);
      if (!refreshedUser) return;

      emitTrendingFromCache();
      emitProfileSync(name, data.sourceSocketId || null);
      emitPublicProfileBannerUpdate(name, refreshedUser);
      invalidateOnlineListCache("profile-sync-listen");
      deferServerTask('PROFILE LISTEN ONLINE LIST', () => emitOnlineList(), 1000);
    } catch (err) {
      console.error('[PROFILE LISTEN ERROR]:', err);
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
    console.log('[PROFILE SYNC] Postgres LISTEN enabled.');
  } catch (err) {
    if (profileSyncNotifyClient === client) profileSyncNotifyClient = null;
    console.error('[PROFILE LISTEN INIT ERROR]:', err && err.message ? err.message : err);
    await client.end().catch(() => {});
    scheduleProfileSyncReconnect(isPgConnectionLimitError(err) ? 15000 : 5000);
  }
}

function disconnectUserSessions(name, eventName = 'user_kicked', payload = {}) {
  getSocketsByUserName(name).forEach(client => {
    client.emit(eventName, payload);
    setTimeout(() => {
      if (client.connected) client.disconnect(true);
    }, 1200);
  });
}

async function upsertPresenceForSocket(socket, name) {
  if (!socket || !name) return;
  await pool.query(
    `INSERT INTO presence_sessions (socket_id, name, instance_id, connected_at, last_seen, data)
     VALUES ($1, $2, $3, NOW(), NOW(), $4)
     ON CONFLICT (socket_id) DO UPDATE SET name = $2, instance_id = $3, last_seen = NOW(), data = $4`,
    [socket.id, name, INSTANCE_ID, { role: getUserRole(name, userDatabase[name] || null) }]
  );
  invalidateOnlineListCache('presence-upsert');
}

async function removePresenceForSocket(socket) {
  if (!socket || !socket.id) return;
  await pool.query('DELETE FROM presence_sessions WHERE socket_id = $1', [socket.id]);
  invalidateOnlineListCache('presence-remove');
}

async function syncPresenceOnlineFromDb() {
  await pool.query(`DELETE FROM presence_sessions WHERE last_seen < NOW() - INTERVAL '90 seconds'`);

  const presenceRes = await pool.query(`
    SELECT name,
           MAX(last_seen) AS last_seen,
           (ARRAY_AGG(socket_id ORDER BY last_seen DESC))[1] AS socket_id
    FROM presence_sessions
    GROUP BY name
  `);

  Object.entries(userDatabase).forEach(([username, user]) => {
    user.online = false;
    if (!user.lastSeen) user.lastSeen = null;
  });

  presenceRes.rows.forEach(row => {
    const username = row.name;
    if (!userDatabase[username]) return;
    userDatabase[username].online = true;
    userDatabase[username].id = row.socket_id || userDatabase[username].id;
    userDatabase[username].lastSeen = row.last_seen ? new Date(row.last_seen).getTime() : Date.now();
  });

  invalidateOnlineListCache("presence-sync");
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

    await pool.query(
      `INSERT INTO presence_sessions (socket_id, name, instance_id, connected_at, last_seen, data)
       SELECT socket_id, name, instance_id, NOW(), NOW(), data
       FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[]) AS t(socket_id, name, instance_id, data)
       ON CONFLICT (socket_id) DO UPDATE SET
         name = EXCLUDED.name,
         instance_id = EXCLUDED.instance_id,
         last_seen = NOW(),
         data = EXCLUDED.data`,
      [socketIds, names, instanceIds, payloads]
    );
    invalidateOnlineListCache("presence-heartbeat");
  }

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

async function resetUserPassword(targetName, newPassword, adminName) {
  if (!targetName) return { success: false, message: "Missing target user." };
  await getUserFromDb(targetName);
  if (!userDatabase[targetName]) return { success: false, message: "User not found." };

  const temporaryPassword = normalizeText(newPassword, "") || generateTemporaryPassword();
  const hash = await bcrypt.hash(temporaryPassword, 10);
  userDatabase[targetName].passwordHash = hash;
  userDatabase[targetName].passwordResetAt = new Date().toISOString();
  userDatabase[targetName].passwordResetBy = adminName || "Admin";
  await saveUser(targetName);

  disconnectUserSessions(targetName, 'password_reset_by_admin', { by: adminName || 'Admin' });
  return { success: true, targetName, temporaryPassword };
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

setInterval(syncAdminStateIntervalTask, 15000);
setInterval(presenceHeartbeatIntervalTask, PRESENCE_HEARTBEAT_MS);
setInterval(chatPollIntervalTask, CHAT_SYNC_INTERVAL_MS);
if (ENABLE_PROFILE_PERIODIC_SYNC) { setInterval(profileSyncIntervalTask, PROFILE_SYNC_INTERVAL_MS); } else { console.log('[PROFILE SYNC] Periodic fallback disabled. Realtime LISTEN/NOTIFY remains enabled.'); }

io.on('connection', (socket) => {
  console.log('[NETWORK] Socket connected. ID: ' + socket.id);
  deferServerTask('CONNECTION INIT', async () => {
    await refreshAdminStateThrottled(5000);
    await emitAdminState(socket);
  }, 0);

  socket.on('authenticate_user', async (data = {}) => {
    try {
      const { name, password, isNewAccount, adminMaintenanceBypass } = data;
      const safeUserData = (data.userData && typeof data.userData === 'object') ? data.userData : {};
      
      const dbRes = await pool.query('SELECT data FROM users WHERE name = $1', [name]);
      let dbUser = dbRes.rows.length > 0 ? normalizeUserRecord(name, dbRes.rows[0].data || {}) : null;
      let wasDeletedAccount = false;

      const isHardcodedAdmin = ADMIN_USERS.includes(name);
      const isAdmin = isUserAdmin(name, dbUser);

      if (!dbUser && !isHardcodedAdmin) {
        const deletedRes = await pool.query('SELECT data FROM deleted_accounts WHERE name = $1', [name]);
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
        if (!dbUser.passwordHash) {
          socket.emit('auth_error', 'Legacy account detected! Please recreate your ID.');
          return;
        }

        const match = await bcrypt.compare(password, dbUser.passwordHash);
        
        if (match) {
          socket.userName = name;
          socket.isAdmin = isAdmin;
          socket.role = getUserRole(name, dbUser);

          const recoveredDbUser = mergeLocalRecoveryData(dbUser, safeUserData);

          userDatabase[name] = {
            ...recoveredDbUser,
            online: true,
            id: socket.id,
            lastSeen: Date.now(),
            name: name,
            role: getUserRole(name, recoveredDbUser),
            banned: isUserBanned(recoveredDbUser),
            profileUpdatedAt: recoveredDbUser.profileUpdatedAt || Date.now()
          };
          normalizeProfileArrayPayloads(userDatabase[name]);
          userCacheMeta[name] = Date.now();
          
          markSocketAuthenticated(socket);
          invalidateOnlineListCache('auth-existing-memory');
          deferServerTask('AUTH EXISTING SAVE', async () => {
            await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
            invalidateOnlineListCache('auth-existing-save');
            await notifyProfileSyncAcrossInstances(name, socket.id, userDatabase[name].profileUpdatedAt);
          }, 2200);
          deferServerTask('AUTH EXISTING PRESENCE', () => upsertPresenceForSocket(socket, name), 250);

          console.log(`[NETWORK] ${name} logged in. Admin: ${isAdmin}`);
          deferServerTask('AUTH LOGIN LOG', async () => {
            await addServerLog('login', `${name} signed in${isAdmin ? ' as admin' : ''}`, { socketId: socket.id, role: getUserRole(name, userDatabase[name]) }, name);
          }, 2400);

          socket.emit('auth_success', { 
            name, 
            userData: userDatabase[name],
            isAdmin: isAdmin,
            role: getUserRole(name, userDatabase[name]),
            isModerator: isUserModerator(name, userDatabase[name])
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
          userData: userDatabase[name],
          isAdmin: isAdmin,
          role: getUserRole(name, userDatabase[name]),
          isModerator: isUserModerator(name, userDatabase[name])
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

    const incomingSettingsData = (payload && payload.settingsData && typeof payload.settingsData === "object")
      ? { ...payload.settingsData }
      : ((payload && typeof payload === "object") ? { ...payload } : null);
    if (!incomingSettingsData || Array.isArray(incomingSettingsData)) return;

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
    const incomingThemePayload = {
      themeColor: payload && payload.themeColor,
      themeColorUpdatedAt: payload && (payload.themeColorUpdatedAt || payload.themeUpdatedAt)
    };
    const themeMerge = reconcileIncomingThemeColor(userDatabase[name], incomingThemePayload, incomingSettingsData);
    userDatabase[name].lastSeen = Date.now();
    userDatabase[name].profileUpdatedAt = Date.now();
    await upsertPresenceForSocket(socket, name);

    try {
      userCacheMeta[name] = Date.now();
      await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
      invalidateOnlineListCache('settings-realtime-save');
    } catch (err) {
      console.error(`[DATABASE ERROR] Failed to save realtime settings for ${name}:`, err);
    }

    const sourceSocketId = (mergedSettings.settingsRejected === true || mergedSettings.bannerRejected === true || themeMerge.rejected === true) ? null : socket.id;
    emitSettingsRealtimeSync(name, sourceSocketId, { reason: payload.reason || 'settings_realtime' });

    if (mergedSettings.bannerAccepted === true || themeMerge.accepted === true) {
      emitPublicProfileBannerUpdate(name, userDatabase[name]);
    }

    deferServerTask('SETTINGS PROFILE NOTIFY', () => notifyProfileSyncAcrossInstances(name, sourceSocketId, userDatabase[name].profileUpdatedAt), 0);
  });

  socket.on('update_profile', async (userData) => {
    const name = socket.userName;
    userData = (userData && typeof userData === "object") ? userData : {};
    const incomingSettingsData = (userData.settingsData && typeof userData.settingsData === "object") ? userData.settingsData : null;
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
            delete userData.passwordResetAt;
            delete userData.passwordResetBy;
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
        
        Object.assign(userDatabase[name], userData);
        if (Array.isArray(userDatabase[name].downloadsData)) userDatabase[name].downloads = userDatabase[name].downloadsData.length;
        userDatabase[name].downloadsClearedAt = normalizeTimestampValue(userDatabase[name].downloadsClearedAt);
        normalizeProfileArrayPayloads(userDatabase[name]);
        userDatabase[name].lastSeen = Date.now();
        userDatabase[name].profileUpdatedAt = Date.now();
        await upsertPresenceForSocket(socket, name);
        
        try {
            userCacheMeta[name] = Date.now();
            await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
            invalidateOnlineListCache('profile-update-save');
        } catch (err) {
            console.error(`[DATABASE ERROR] Failed to save profile for ${name}:`, err);
        }

        if (shouldEmitTrendingUpdate) {
            emitTrendingFromCache();
        }

        deferServerTask('PROFILE ONLINE LIST', () => emitOnlineList(), 450);
        if (shouldBroadcastProfileBanner) {
            emitPublicProfileBannerUpdate(name, userDatabase[name]);
        }
        emitProfileSync(name, shouldForceProfileSyncToSource ? null : socket.id);
        deferServerTask('PROFILE NOTIFY', () => notifyProfileSyncAcrossInstances(name, shouldForceProfileSyncToSource ? null : socket.id, userDatabase[name].profileUpdatedAt), 0);

        if (userData.trophiesData) {
            try {
                io.emit('global_trophy_stats', calculateGlobalTrophyStatsFromCache());
            } catch (err) {
                console.error('[TROPHY STATS CACHE ERROR]:', err);
            }
        }
    }
  });

  socket.on('request_user_data', async (data = {}) => {
    const { targetName, type, requestId } = data;
    try {
      let rawData = getUserDataPayloadFromCache(targetName, type);

      if (rawData === null) {
        const refreshedUser = await withTimeout(
          refreshSingleUserCacheFromDb(targetName),
          4500,
          null
        );
        rawData = refreshedUser ? getUserDataPayloadFromCache(targetName, type) : getEmptyUserDataPayload(type);
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
        userDatabase[socket.userName].lastSeen = Date.now();
        deferServerTask('REQUEST ONLINE PRESENCE UPSERT', () => upsertPresenceForSocket(socket, socket.userName), 0);
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

      userDatabase[name].lastSeen = Date.now();
      deferServerTask('PRESENCE PING UPSERT', () => upsertPresenceForSocket(socket, name), 0);
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

  socket.on('search_users', async (query) => {
    if (!query) return;

    try {
      const results = await searchUsersFromDb(query, socket.isAdmin === true);
      socket.emit('global_search_results', results);
    } catch (err) {
      console.error('[SEARCH USERS DB ERROR]:', err);
      socket.emit('global_search_results', []);
    }
  });
  
  socket.on('request_trophy_stats', async () => {
    try {
      await ensureUserCacheReady();
      socket.emit('global_trophy_stats', calculateGlobalTrophyStatsFromCache());
    } catch (err) {
      console.error('[TROPHY STATS CACHE ERROR]:', err);
      socket.emit('global_trophy_stats', {});
    }
  });

  socket.on('request_trending', async () => {
    try {
      if (!Object.keys(userDatabase).length) {
        await withTimeout(ensureUserCacheReady(), 2000, null);
      }
      emitTrendingFromCache(socket);
    } catch (err) {
      console.error('[TRENDING CACHE ERROR]:', err);
      socket.emit('trending_data', {
        topDownloads: [],
        topWishlist: [],
        contentDownloadCounts: {}
      });
      socket.emit('content_download_counts', buildContentDownloadCountsPayload({}));
    }
  });

  socket.on('request_content_download_counts', async (_request = {}, callback) => {
    try {
      if (!Object.keys(userDatabase).length) {
        await withTimeout(ensureUserCacheReady(), 2000, null);
      }

      const activity = calculateTrendingFromCache();
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
    const { code, user } = data;
    if (!code) return callback({ success: false, message: "Enter a code." });

    const cleanCode = code.replace(/-/g, "").toUpperCase();
    if (cleanCode === "PLATINUMCODE") return callback({ success: true, type: 'PLATINUM_UNLOCK' });
    if (cleanCode === "UNLOCKALLDB1") return callback({ success: true, type: 'SINGLE_TROPHY' });

    callback({ success: false, message: "Invalid code." });
  });

  socket.on('chat_message', async (msg, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
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
      const { targetName, rest } = resolveCommandTarget(text.slice(commandName.length));
      const result = await resetUserPassword(targetName, rest, senderName || 'Admin');
      if (result.success) {
        await addModerationLog('reset_password', `Reset password for ${targetName} via chat command`, { targetName }, senderName || 'Admin');
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
      messageData._dbId = Number(savedRes.rows[0]?.id || 0);
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
      await pool.query(`DELETE FROM presence_sessions WHERE last_seen < NOW() - INTERVAL '90 seconds'`);

      const statsRes = await pool.query(`
        SELECT
          (SELECT COUNT(*)::int FROM users) AS users,
          (SELECT COUNT(DISTINCT name)::int FROM presence_sessions WHERE last_seen > NOW() - INTERVAL '90 seconds') AS online
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
      const newPassword = normalizeText(data && data.newPassword, "");
      const result = await resetUserPassword(targetName, newPassword, socket.userName || normalizeText(data && data.adminUser, "Admin"));

      if (result.success) {
        await addModerationLog('reset_password', `Reset temporary password for ${targetName}`, { targetName }, socket.userName || 'Admin');
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
        serverLog: socket.isAdmin === true ? serverLog : []
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
        const isAdmin = socket.isAdmin === true;
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
      await removePresenceForSocket(socket);
      userDatabase[name].lastSeen = Date.now();
      socket.broadcast.emit('user_stopped_typing', { name: name });

      await syncPresenceOnlineFromDb();
      const stillOnline = userDatabase[name].online === true;
      if (!stillOnline) {
        userDatabase[name].online = false;
        await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
        invalidateOnlineListCache('disconnect-save');
        await addServerLog('logout', `${name} disconnected`, { socketId: socket.id }, name);
      }

      await emitOnlineList();
    } catch (err) {
      console.error('[DISCONNECT CLEANUP ERROR]:', err);
      userDatabase[name].lastSeen = Date.now();
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`PSN Database Server running on port ${PORT} (pg pool max ${PG_POOL_MAX}, online cache ${ONLINE_LIST_CACHE_MS}ms, chat sync ${CHAT_SYNC_INTERVAL_MS}ms, instance ${INSTANCE_ID})`);
});
