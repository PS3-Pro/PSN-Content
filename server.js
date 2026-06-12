const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");
const { Pool, Client } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const server = http.createServer(app);

const APP_URLS = [
  "https://psn-content-cb4c.onrender.com/ping",
  "https://psn-content-nnb8.onrender.com/ping",
  "https://psn-content-fbni.onrender.com/ping",
  "https://psn-content-4sof.onrender.com/ping",
  "https://psn-content-0st4.onrender.com/ping",
  "https://psn-content-mwp5.onrender.com/ping",
];

const ADMIN_USERS = ["Luan Teles", "Goku Cheats", "JumpSuit"];

const DEFAULT_AVATAR = "https://raw.githubusercontent.com/PS3-Pro/PSN-Content/master/resources/interface/modern/images/avatars/default.png";

const MAX_CHAT_HISTORY = 1000; 

const SERVER_STARTED_AT = Date.now();
const INSTANCE_ID = process.env.RENDER_INSTANCE_ID || process.env.RAILWAY_REPLICA_ID || process.env.HOSTNAME || `instance-${Math.random().toString(36).slice(2, 10)}`;
const PRESENCE_TTL_SECONDS = 90;
const PRESENCE_HEARTBEAT_MS = 25000;
const CHAT_SYNC_INTERVAL_MS = 3000;
const PROFILE_SYNC_INTERVAL_MS = 5000;
const DEFAULT_MAINTENANCE_MESSAGE = "The service is under maintenance. Please try again soon.";
const VALID_USER_ROLES = new Set(["user", "trusted", "mod", "admin"]);
const ADMIN_STATE_KEYS = {
  maintenance: "maintenance",
  chatControls: "chat_controls",
  pinnedAnnouncement: "pinned_announcement"
};

const pgConnectionOptions = {
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
};

const pool = new Pool(pgConnectionOptions);
let profileSyncNotifyClient = null;

app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

let userDatabase = {};
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
  } catch (err) {
    console.error('[ADMIN STATE REFRESH ERROR]:', err);
  }
  return adminState;
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

  const usersRes = await pool.query('SELECT * FROM users');
  usersRes.rows.forEach(row => {
    userDatabase[row.name] = normalizeUserRecord(row.name, row.data || {});
    userDatabase[row.name].online = false;
  });

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
  .then(() => initProfileSyncNotifications())
  .catch(console.error);


setInterval(() => {
  APP_URLS.forEach(url => {
    https.get(url, (res) => {
      console.log(`Auto-ping [${url}]: Status ${res.statusCode}`);
    }).on('error', (err) => {
      console.error(`Auto-ping error [${url}]:`, err.message);
    });
  });
}, 840000);

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
        downloads: u.downloads || 0,
        wishlist: u.wishlist || 0,
        favorites: u.favorites || 0,
        trophies: u.trophies || 0,
        library: u.library || 0
    }));
}

function calculateGlobalTrophyStats() {
  const stats = {};
  const users = Object.values(userDatabase);
  const totalUsers = users.length;

  if (totalUsers === 0) return stats;

  const trophyCounts = {};

  users.forEach(user => {
    if (user.trophiesData) {
      Object.keys(user.trophiesData).forEach(trophyId => {
        if (user.trophiesData[trophyId] && user.trophiesData[trophyId].unlocked) {
          trophyCounts[trophyId] = (trophyCounts[trophyId] || 0) + 1;
        }
      });
    }
  });

  Object.keys(trophyCounts).forEach(trophyId => {
    stats[trophyId] = (trophyCounts[trophyId] / totalUsers) * 100;
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

function normalizeTimestampValue(value) {
  const timestamp = Number(value || 0);
  return Number.isFinite(timestamp) && timestamp > 0 ? timestamp : 0;
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
  const hasIncomingDownloadsData = Object.prototype.hasOwnProperty.call(incomingUser, 'downloadsData');
  const hasIncomingDownloadsCount = Object.prototype.hasOwnProperty.call(incomingUser, 'downloads');

  if (incomingClearAt > currentClearAt) {
    applyDownloadsClearedState(currentUser, incomingClearAt);
    incomingUser.downloadsClearedAt = incomingClearAt;
    incomingUser.downloadsData = [];
    incomingUser.downloads = 0;
    return incomingUser;
  }

  if (currentClearAt > incomingClearAt && (hasIncomingDownloadsData || hasIncomingDownloadsCount)) {
    delete incomingUser.downloadsData;
    delete incomingUser.downloads;
    incomingUser.downloadsClearedAt = currentClearAt;
    return incomingUser;
  }

  if (incomingClearAt === currentClearAt && hasIncomingDownloadsData && Array.isArray(incomingUser.downloadsData)) {
    incomingUser.downloads = incomingUser.downloadsData.length;
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
  if ((!merged.themeColor || merged.themeColor === '#0070cc') && localData.themeColor) merged.themeColor = localData.themeColor;
  ['trophiesData', 'countersData'].forEach(key => {
    const preferred = preferLocalObjectPayload(merged[key], localData[key]);
    if (preferred !== merged[key]) merged[key] = preferred;
  });

  if (localDownloadsClearedAt > dbDownloadsClearedAt) {
    applyDownloadsClearedState(merged, localDownloadsClearedAt);
  } else if (localDownloadsClearedAt === dbDownloadsClearedAt) {
    const preferredDownloads = preferLocalArrayPayload(merged.downloadsData, localData.downloadsData);
    if (preferredDownloads !== merged.downloadsData) merged.downloadsData = preferredDownloads;
  } else if (dbDownloadsClearedAt > localDownloadsClearedAt) {
    merged.downloadsClearedAt = dbDownloadsClearedAt;
  }

  ['wishlistData', 'favoritesData', 'libraryData', 'friendsData'].forEach(key => {
    const preferred = preferLocalArrayPayload(merged[key], localData[key]);
    if (preferred !== merged[key]) merged[key] = preferred;
  });
  if (hasObjectPayload(localData.settingsData)) {
    merged.settingsData = { ...(merged.settingsData || {}), ...localData.settingsData };
  }
  ['wishlist', 'favorites', 'trophies', 'library', 'level', 'xp'].forEach(key => {
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

async function saveUser(name) {
  if (!name || !userDatabase[name]) return;
  userDatabase[name] = normalizeUserRecord(name, userDatabase[name]);
  await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
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

function emitAdminState(socket) {
  socket.emit('maintenance_mode', adminState.maintenance);
  socket.emit('chat_controls', adminState.chatControls);
  socket.emit('admin_pinned_announcement', adminState.pinnedAnnouncement || { clear: true });

  if (socket.isAdmin === true) {
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
      wishlistData: Array.isArray(safe.wishlistData) ? safe.wishlistData : [],
      favoritesData: Array.isArray(safe.favoritesData) ? safe.favoritesData : [],
      libraryData: Array.isArray(safe.libraryData) ? safe.libraryData : [],
      friendsData: Array.isArray(safe.friendsData) ? safe.friendsData : [],
      countersData: safe.countersData || {},
      themeColor: safe.themeColor || '#0070cc',
      settingsData: safe.settingsData || {}
    }
  };
}

function emitProfileSync(name, sourceSocketId = null) {
  if (!name || !userDatabase[name]) return;
  const payload = buildFullProfileSyncPayload(name, userDatabase[name], sourceSocketId);
  getSocketsByUserName(name).forEach(client => client.emit('profile_sync', payload));
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

      const dbRes = await pool.query('SELECT data FROM users WHERE name = $1', [name]);
      if (!dbRes.rows.length) return;

      const dbUser = normalizeUserRecord(name, dbRes.rows[0].data || {});
      const localUser = userDatabase[name] || {};
      const dbVersion = Number(dbUser.profileUpdatedAt || data.profileUpdatedAt || 0);
      const localVersion = Number(localUser.profileUpdatedAt || 0);
      if (dbVersion && localVersion && dbVersion <= localVersion) return;

      userDatabase[name] = {
        ...dbUser,
        online: localUser.online === true,
        id: localUser.id || dbUser.id,
        lastSeen: localUser.lastSeen || dbUser.lastSeen || Date.now()
      };

      emitProfileSync(name, data.sourceSocketId || null);
      await emitOnlineList();
    } catch (err) {
      console.error('[PROFILE LISTEN ERROR]:', err);
    }
  });

  client.on('error', (err) => {
    console.error('[PROFILE LISTEN CONNECTION ERROR]:', err);
    profileSyncNotifyClient = null;
    setTimeout(() => initProfileSyncNotifications().catch(e => console.error('[PROFILE LISTEN RECONNECT ERROR]:', e)), 5000);
  });

  try {
    await client.connect();
    await client.query('LISTEN profile_sync');
    console.log('[PROFILE SYNC] Postgres LISTEN enabled.');
  } catch (err) {
    profileSyncNotifyClient = null;
    console.error('[PROFILE LISTEN INIT ERROR]:', err);
    setTimeout(() => initProfileSyncNotifications().catch(e => console.error('[PROFILE LISTEN RECONNECT ERROR]:', e)), 5000);
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
}

async function removePresenceForSocket(socket) {
  if (!socket || !socket.id) return;
  await pool.query('DELETE FROM presence_sessions WHERE socket_id = $1', [socket.id]);
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

  return userDatabase;
}

async function emitOnlineList(targetSocket = null) {
  try {
    await syncPresenceOnlineFromDb();
  } catch (err) {
    console.error('[PRESENCE SYNC ERROR]:', err);
  }
  const list = getSanitizedOnlineList();
  if (targetSocket) targetSocket.emit('online_list', list);
  else io.emit('online_list', list);
  return list;
}

async function heartbeatPresenceSessions() {
  const activeSockets = [];
  io.sockets.sockets.forEach(client => {
    if (client.connected && client.userName) activeSockets.push(client);
  });

  for (const client of activeSockets) {
    await upsertPresenceForSocket(client, client.userName);
  }

  await emitOnlineList();
}

async function setUserRole(targetName, role, adminName) {
  if (!targetName || !userDatabase[targetName]) {
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

  delete userDatabase[targetName];
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
  const previousServerLog = JSON.stringify(serverLog);
  await refreshAdminStateFromDb();
  await refreshServerLogFromDb();

  if (JSON.stringify(serverLog) !== previousServerLog) {
    emitToAdmins('admin_server_log_list', serverLog);
  }

  if (JSON.stringify(adminState) === previous) return;

  io.emit('maintenance_mode', adminState.maintenance);
  io.emit('chat_controls', adminState.chatControls);
  io.emit('admin_pinned_announcement', adminState.pinnedAnnouncement || { clear: true });
  emitToAdmins('admin_state', {
    maintenance: adminState.maintenance,
    chatControls: adminState.chatControls,
    pinnedAnnouncement: adminState.pinnedAnnouncement || null,
    reports: adminReports,
    serverLog
  });
}

setInterval(() => {
  syncAdminStateAcrossInstances().catch(err => console.error('[ADMIN STATE SYNC ERROR]:', err));
}, 15000);

setInterval(() => {
  heartbeatPresenceSessions().catch(err => console.error('[PRESENCE HEARTBEAT ERROR]:', err));
}, PRESENCE_HEARTBEAT_MS);

setInterval(() => {
  syncChatAcrossInstances().catch(err => console.error('[CHAT POLL ERROR]:', err));
}, CHAT_SYNC_INTERVAL_MS);

setInterval(() => {
  syncActiveProfilesAcrossInstances().catch(err => console.error('[PROFILE SYNC ERROR]:', err));
}, PROFILE_SYNC_INTERVAL_MS);

io.on('connection', async (socket) => {
  console.log('[NETWORK] Socket connected. ID: ' + socket.id);
  await refreshAdminStateFromDb();
  emitAdminState(socket);

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
          
          await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
          await upsertPresenceForSocket(socket, name);
          await refreshChatHistoryFromDb();

          console.log(`[NETWORK] ${name} logged in. Admin: ${isAdmin}`);
          await addServerLog('login', `${name} signed in${isAdmin ? ' as admin' : ''}`, { socketId: socket.id, role: getUserRole(name, userDatabase[name]) }, name);

          socket.emit('auth_success', { 
            name, 
            userData: userDatabase[name],
            isAdmin: isAdmin,
            role: getUserRole(name, userDatabase[name]),
            isModerator: isUserModerator(name, userDatabase[name])
          });

          socket.emit('chat_history', getPublicChatHistory());
          socket.emit('pinned_list', pinnedMessages);
          emitAdminState(socket);

          await emitOnlineList();
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
          settingsData: safeUserData.settingsData || { audio: "1", ux: "1", chatSound: "1", ps3Ip: "", companionPlugin: "1", fpsCounterPlugin: "0", consoleFanMode: "dynamic", consoleFanSpeed: "35", consoleFanTarget: "68", performanceMode: "balanced", performanceRsx: "650", performanceVram: "850" },
          trophiesData: safeUserData.trophiesData || {},
          wishlistData: safeUserData.wishlistData || [],
          favoritesData: safeUserData.favoritesData || [],
          downloadsData: normalizeTimestampValue(safeUserData.downloadsClearedAt) ? [] : (safeUserData.downloadsData || []),
          downloadsClearedAt: normalizeTimestampValue(safeUserData.downloadsClearedAt),
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

        await pool.query(
          'INSERT INTO users (name, data) VALUES ($1, $2)',
          [name, userDatabase[name]]
        );
        await upsertPresenceForSocket(socket, name);
        await refreshChatHistoryFromDb();
        if (wasDeletedAccount) {
          await pool.query('DELETE FROM deleted_accounts WHERE name = $1', [name]);
        }
        
        console.log(`[NETWORK] ${name} created a new account. Admin: ${isAdmin}`);
        await addServerLog('signup', `${name} created an account${isAdmin ? ' as admin' : ''}`, { socketId: socket.id, role: getUserRole(name, userDatabase[name]) }, name);

        socket.emit('auth_success', { 
          name, 
          userData: userDatabase[name],
          isAdmin: isAdmin,
          role: getUserRole(name, userDatabase[name]),
          isModerator: isUserModerator(name, userDatabase[name])
        });

        socket.emit('chat_history', getPublicChatHistory());
        socket.emit('pinned_list', pinnedMessages);
        emitAdminState(socket);

        await emitOnlineList();
      }
    } catch (error) {
      console.error("[AUTH ERROR]:", error);
      socket.emit('auth_error', 'Server Error: Auth failed.');
    }
  });

  socket.on('update_profile', async (userData) => {
    const name = socket.userName;
    if (name && userDatabase[name]) {
        
        if (userData.settingsData) {
            userDatabase[name].settingsData = {
                ...(userDatabase[name].settingsData || {}),
                ...userData.settingsData
            };
            delete userData.settingsData;
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

        userData = reconcileIncomingDownloads(userDatabase[name], userData || {});
        
        Object.assign(userDatabase[name], userData);
        if (Array.isArray(userDatabase[name].downloadsData)) userDatabase[name].downloads = userDatabase[name].downloadsData.length;
        userDatabase[name].downloadsClearedAt = normalizeTimestampValue(userDatabase[name].downloadsClearedAt);
        userDatabase[name].lastSeen = Date.now();
        userDatabase[name].profileUpdatedAt = Date.now();
        
        try {
            await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
        } catch (err) {
            console.error(`[DATABASE ERROR] Failed to save profile for ${name}:`, err);
        }

        await emitOnlineList();
        emitProfileSync(name, socket.id);
        await notifyProfileSyncAcrossInstances(name, socket.id, userDatabase[name].profileUpdatedAt);

        if (userData.trophiesData) {
            io.emit('global_trophy_stats', calculateGlobalTrophyStats());
        }
    }
  });

  socket.on('request_user_data', (data) => {
    const { targetName, type } = data;
    const targetUser = userDatabase[targetName];
    if (targetUser) {
        const keyMap = { 'favs': 'favoritesData', 'wishlist': 'wishlistData', 'downloads': 'downloadsData', 'library': 'libraryData', 'trophies': 'trophiesData' };
        const dataKey = keyMap[type] || (type + 'Data');
        socket.emit('user_data_response', { targetName, type, rawData: targetUser[dataKey] || [] });
    }
  });

  socket.on('request_online_list', async () => {
    await emitOnlineList(socket);
  });

  socket.on('search_users', (query) => {
    if (!query) return;
    
    const searchTerm = query.toLowerCase().trim();
    const isAllCommand = (searchTerm === '@all' || searchTerm === '*');

    if (!isAllCommand && searchTerm.length < 2) return;

    const results = Object.entries(userDatabase)
      .filter(([username, u]) => isAllCommand ? true : username.toLowerCase().includes(searchTerm))
      .map(([username, u]) => getPublicUserData(username, u, socket.isAdmin === true))
      .slice(0, isAllCommand ? Object.keys(userDatabase).length : 15);

    socket.emit('global_search_results', results);
  });
  
  socket.on('request_trophy_stats', () => {
    const stats = calculateGlobalTrophyStats();
    socket.emit('global_trophy_stats', stats);
  });

  socket.on('request_trending', () => {
    let dlCounts = {};
    let wishCounts = {};

    Object.values(userDatabase).forEach(user => {
        if (user.downloadsData) {
            user.downloadsData.forEach(item => {
                const id = item.titleId || item.id;
                if (id) dlCounts[id] = (dlCounts[id] || 0) + 1;
            });
        }
        if (user.wishlistData) {
            user.wishlistData.forEach(item => {
                const id = item.titleId || item.id;
                if (id) wishCounts[id] = (wishCounts[id] || 0) + 1;
            });
        }
    });

    const getTop = (countsObj) => {
        return Object.entries(countsObj)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 50)
            .map(entry => ({ id: entry[0], count: entry[1] }));
    };

    socket.emit('trending_data', {
        topDownloads: getTop(dlCounts),
        topWishlist: getTop(wishCounts)
    });
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
    let messageData = { ...(typeof msg === 'object' ? msg : { text: msg }), time: new Date().toISOString(), seenBy: [] };
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


  socket.on('admin_ping' , (data, callback) => {
    if (typeof callback === 'function') {
      callback({
        success: true,
        serverTime: new Date().toISOString(),
        uptimeSeconds: Math.floor((Date.now() - SERVER_STARTED_AT) / 1000),
        users: Object.keys(userDatabase).length,
        online: Object.values(userDatabase).filter(u => u.online).length
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
    await refreshAdminStateFromDb();
    const payload = adminState.chatControls || normalizeChatControls({});
    socket.emit('chat_controls', payload);
    if (socket.isAdmin === true) socket.emit('admin_chat_controls_state', payload);
    if (typeof callback === 'function') callback({ success: true, state: payload });
  });

  socket.on('admin_request_admin_state', async (data, callback) => {
    await refreshAdminStateFromDb();
    if (socket.isAdmin === true) await refreshServerLogFromDb();
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

  socket.on('admin_request_reports', () => {
    if (socket.isAdmin === true) socket.emit('reports_list', adminReports);
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
        if (!msg.seenBy.includes(data.user)) {
            msg.seenBy.push(data.user);
            
            try {
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [cleanChatMessage(msg), msg.time]);
                io.emit('message_seen', { msgId: data.msgId, seenBy: msg.seenBy });
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
    if (name && userDatabase[name]) {
      await removePresenceForSocket(socket);
      userDatabase[name].lastSeen = Date.now();
      socket.broadcast.emit('user_stopped_typing', { name: name });

      await syncPresenceOnlineFromDb();
      const stillOnline = userDatabase[name].online === true;
      if (!stillOnline) {
        userDatabase[name].online = false;
        await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
        await addServerLog('logout', `${name} disconnected`, { socketId: socket.id }, name);
      }

      await emitOnlineList();
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`PSN Database Server running on port ${PORT}`);
});