const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");
const { Pool } = require('pg');
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

const ADMIN_USERS = ["Luan Teles", "Goku Cheats"];

const DEFAULT_AVATAR = "https://raw.githubusercontent.com/PS3-Pro/PSN-Content/master/resources/interface/modern/images/avatars/default.png";

const MAX_CHAT_HISTORY = 1000; 

const SERVER_STARTED_AT = Date.now();
const DEFAULT_MAINTENANCE_MESSAGE = "The service is under maintenance. Please try again soon.";
const VALID_USER_ROLES = new Set(["user", "trusted", "mod", "admin", "banned"]);
const ADMIN_STATE_KEYS = {
  maintenance: "maintenance",
  chatControls: "chat_controls",
  pinnedAnnouncement: "pinned_announcement"
};

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

let userDatabase = {};
let messageHistory = [];
let pinnedMessages = [];

let adminState = {
  maintenance: { enabled: false, message: DEFAULT_MAINTENANCE_MESSAGE, by: "", at: null },
  chatControls: { locked: false, slowSeconds: 0, by: "", at: null },
  pinnedAnnouncement: null
};
let moderationLog = [];
let adminReports = [];

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
    CREATE TABLE IF NOT EXISTS reports (
      id TEXT PRIMARY KEY,
      data JSONB,
      resolved BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  const usersRes = await pool.query('SELECT * FROM users');
  usersRes.rows.forEach(row => {
    userDatabase[row.name] = row.data;
    userDatabase[row.name].name = row.name; 
    userDatabase[row.name].online = false;
  });

  const chatRes = await pool.query(`SELECT message FROM chat ORDER BY id DESC LIMIT ${MAX_CHAT_HISTORY}`);
  messageHistory = chatRes.rows.map(r => r.message).reverse();
  
  const pinnedRes = await pool.query('SELECT data FROM pinned_messages ORDER BY id ASC');
  pinnedMessages = pinnedRes.rows.map(r => r.data);

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

  const reportsRes = await pool.query('SELECT data FROM reports WHERE resolved = false ORDER BY created_at DESC LIMIT 100');
  adminReports = reportsRes.rows.map(r => r.data);

  const modLogRes = await pool.query('SELECT entry FROM moderation_log ORDER BY created_at DESC LIMIT 100');
  moderationLog = modLogRes.rows.map(r => r.entry);

  console.log(`[DB] Database initialized. ${messageHistory.length} messages, ${pinnedMessages.length} pins, ${Object.keys(userDatabase).length} users loaded.`);
}

initDb().catch(console.error);

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

function getUserRole(name, userData = null) {
  const rawRole = normalizeText(userData && userData.role, ADMIN_USERS.includes(name) ? "admin" : "user").toLowerCase();
  if (rawRole === "moderator") return "mod";
  return VALID_USER_ROLES.has(rawRole) ? rawRole : (ADMIN_USERS.includes(name) ? "admin" : "user");
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
  // Moderators can moderate regular/trusted/banned users, but not admins or other mods.
  return !["admin", "mod"].includes(targetRole);
}

function getActorRole(socket) {
  if (!socket || !socket.userName) return "user";
  return socket.isAdmin === true ? "admin" : getUserRole(socket.userName, userDatabase[socket.userName] || null);
}

function isUserBanned(userData = null) {
  return !!(userData && (userData.banned === true || getUserRole(userData.name || "", userData) === "banned"));
}

function normalizeMaintenanceState(data = {}) {
  return {
    enabled: !!data.enabled,
    message: normalizeText(data.message, DEFAULT_MAINTENANCE_MESSAGE) || DEFAULT_MAINTENANCE_MESSAGE,
    by: normalizeText(data.by, ""),
    at: data.at || (data.enabled ? new Date().toISOString() : null)
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
  await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
}

async function saveAdminState(key, data) {
  await pool.query(
    'INSERT INTO admin_state (state_key, data) VALUES ($1, $2) ON CONFLICT (state_key) DO UPDATE SET data = $2',
    [key, data]
  );
}

function emitAdminState(socket) {
  socket.emit('maintenance_mode', adminState.maintenance);
  socket.emit('chat_controls', adminState.chatControls);
  socket.emit('admin_pinned_announcement', adminState.pinnedAnnouncement || { clear: true });

  if (socket.isAdmin === true) {
    socket.emit('reports_list', adminReports);
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

function getSocketsByUserName(name) {
  const sockets = [];
  io.sockets.sockets.forEach(client => {
    if (client.userName === name) sockets.push(client);
  });
  return sockets;
}

function disconnectUserSessions(name, eventName = 'user_kicked', payload = {}) {
  getSocketsByUserName(name).forEach(client => {
    client.emit(eventName, payload);
    setTimeout(() => {
      if (client.connected) client.disconnect(true);
    }, 1200);
  });
}

async function setUserRole(targetName, role, adminName) {
  if (!targetName || !userDatabase[targetName]) {
    return { success: false, message: "User not found." };
  }

  const normalizedRole = role === "moderator" ? "mod" : normalizeText(role, "user").toLowerCase();
  if (!VALID_USER_ROLES.has(normalizedRole)) {
    return { success: false, message: "Invalid role." };
  }

  if (ADMIN_USERS.includes(targetName) && normalizedRole !== "admin") {
    return { success: false, message: "Hardcoded admins cannot be demoted or banned from the panel." };
  }

  userDatabase[targetName].role = normalizedRole;
  userDatabase[targetName].name = targetName;

  if (normalizedRole === "banned") {
    userDatabase[targetName].banned = true;
    userDatabase[targetName].banReason = userDatabase[targetName].banReason || "Banned by administrator";
    userDatabase[targetName].bannedBy = adminName;
    userDatabase[targetName].bannedAt = new Date().toISOString();
  } else {
    userDatabase[targetName].banned = false;
    delete userDatabase[targetName].banReason;
    delete userDatabase[targetName].bannedBy;
    delete userDatabase[targetName].bannedAt;
  }

  await saveUser(targetName);

  getSocketsByUserName(targetName).forEach(client => {
    client.isAdmin = isUserAdmin(targetName, userDatabase[targetName]);
    client.role = getUserRole(targetName, userDatabase[targetName]);
    client.emit('role_updated', { role: normalizedRole, isAdmin: client.isAdmin, isModerator: normalizedRole === 'mod' });
  });

  io.emit('online_list', getSanitizedOnlineList());
  return { success: true, role: normalizedRole };
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

io.on('connection', (socket) => {
  console.log('[NETWORK] Socket connected. ID: ' + socket.id);
  emitAdminState(socket);

  socket.on('authenticate_user', async (data) => {
    try {
      const { name, password, userData, isNewAccount } = data;
      
      const dbRes = await pool.query('SELECT data FROM users WHERE name = $1', [name]);
      const dbUser = dbRes.rows.length > 0 ? dbRes.rows[0].data : null;

      const isAdmin = isUserAdmin(name, dbUser);

      if (dbUser && isUserBanned({ ...dbUser, name }) && !isAdmin) {
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

          userDatabase[name] = {
            ...dbUser,
            online: true,
            id: socket.id,
            lastSeen: Date.now(),
            name: name,
            role: dbUser.role || (isAdmin ? "admin" : "user"),
            banned: !!dbUser.banned
          };
          
          await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
          
          console.log(`[NETWORK] ${name} logged in. Admin: ${isAdmin}`);

          socket.emit('auth_success', { 
            name, 
            userData: userDatabase[name],
            isAdmin: isAdmin,
            role: getUserRole(name, userDatabase[name]),
            isModerator: isUserModerator(name, userDatabase[name])
          });

          socket.emit('chat_history', messageHistory);
          socket.emit('pinned_list', pinnedMessages);
          emitAdminState(socket);

          io.emit('online_list', getSanitizedOnlineList());
        } else {
          if (isNewAccount) {
            socket.emit('auth_error', 'This Online ID is already taken...');
          } else {
            socket.emit('auth_error', 'Incorrect password. Access denied.');
          }
        }
      } else {
        const hash = await bcrypt.hash(password, 10);
        socket.userName = name;
        socket.isAdmin = isAdmin;

        userDatabase[name] = {
          ...userData,
          name: name,
          passwordHash: hash,
          id: socket.id,
          online: true,
          lastSeen: Date.now(),
          avatar: userData.avatar || DEFAULT_AVATAR,
          joined: userData.joined || '2026',
          settingsData: userData.settingsData || { audio: "1", ux: "1", chatSound: "1", ps3Ip: "" },
          trophiesData: userData.trophiesData || {},
          wishlistData: userData.wishlistData || [],
          favoritesData: userData.favoritesData || [],
          downloadsData: userData.downloadsData || [],
          libraryData: userData.libraryData || [],
          friendsData: userData.friendsData || [],
          countersData: userData.countersData || {},
          themeColor: userData.themeColor || '#0070cc',
          role: isAdmin ? "admin" : "user",
          banned: false
        };

        await pool.query(
          'INSERT INTO users (name, data) VALUES ($1, $2)',
          [name, userDatabase[name]]
        );
        
        console.log(`[NETWORK] ${name} created a new account. Admin: ${isAdmin}`);

        socket.emit('auth_success', { 
          name, 
          userData: userDatabase[name],
          isAdmin: isAdmin,
          role: getUserRole(name, userDatabase[name]),
          isModerator: isUserModerator(name, userDatabase[name])
        });

        socket.emit('chat_history', messageHistory);
        socket.emit('pinned_list', pinnedMessages);
        emitAdminState(socket);

        io.emit('online_list', getSanitizedOnlineList());
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

        if (isUserBanned(userDatabase[name]) && socket.isAdmin !== true) {
            socket.emit('auth_error', 'This account is banned.');
            return;
        }
        
        Object.assign(userDatabase[name], userData);
        userDatabase[name].lastSeen = Date.now();
        
        try {
            await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
        } catch (err) {
            console.error(`[DATABASE ERROR] Failed to save profile for ${name}:`, err);
        }

        io.emit('online_list', getSanitizedOnlineList());

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

  socket.on('request_online_list', () => {
    socket.emit('online_list', getSanitizedOnlineList());
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

  socket.on('chat_message', async (msg) => {
    let messageData = { ...(typeof msg === 'object' ? msg : { text: msg }), time: new Date().toISOString(), seenBy: [] };
    const isAdmin = socket.isAdmin === true;
    const canModerate = canModerateSocket(socket);
    const actorRole = getActorRole(socket);
    const senderName = socket.userName || messageData.user;

    if (senderName && userDatabase[senderName] && isUserBanned(userDatabase[senderName]) && !isAdmin) {
      socket.emit('chat_blocked', { reason: 'banned', message: 'Your account is banned.' });
      return;
    }

    const text = normalizeText(messageData.text, "");

    if (text.toLowerCase().startsWith('/kick')) {
      if (!canModerate) {
        socket.emit('chat_blocked', { reason: 'permission', message: 'Only admins/moderators can use /kick.' });
        return;
      }

      const targetName = text.split(' ').slice(1).join(' ').replace('@', '').trim();
      const targetSocket = getSocketsByUserName(targetName)[0];
      if (!targetName || !targetSocket) {
        socket.emit('kick_error', { message: 'User not found or offline.' });
        return;
      }
      if (!canModerateTarget(socket, targetName)) {
        socket.emit('kick_error', { message: 'You cannot kick this user.' });
        return;
      }

      targetSocket.emit('user_kicked', { by: senderName, role: actorRole });
      socket.emit('kick_success', { targetId: targetSocket.id, targetName });
      await addModerationLog('kick', `Kicked ${targetName} via chat command`, { targetName, targetId: targetSocket.id }, senderName || 'Moderator');
      setTimeout(() => {
        if (targetSocket.connected) targetSocket.disconnect(true);
      }, 2500);
      return;
    }

    if ((text === '/reload' || text === '/force_reload') && isAdmin) {
      socket.broadcast.emit('force_reload');
      await addModerationLog('reload', 'Forced reload for connected users', {}, senderName || 'Admin');
      return;
    }

    if ((text === '/clean' || text === '/clear_chat') && isAdmin) {
      messageHistory = [];
      await pool.query('TRUNCATE chat');
      io.emit('chat_cleared');
      await addModerationLog('clear_chat', 'Cleared global chat history via command', {}, senderName || 'Admin');
      return;
    }

    if (!canModerate) {
      const controls = adminState.chatControls || {};
      if (controls.locked) {
        socket.emit('chat_blocked', { reason: 'locked', message: 'Chat is locked by admin.', controls });
        return;
      }

      const slowSeconds = parseInt(controls.slowSeconds || 0, 10) || 0;
      if (slowSeconds > 0 && socket.lastChatAt) {
        const elapsed = Date.now() - socket.lastChatAt;
        const waitMs = (slowSeconds * 1000) - elapsed;
        if (waitMs > 0) {
          socket.emit('chat_blocked', { reason: 'slow_mode', waitSeconds: Math.ceil(waitMs / 1000), controls });
          return;
        }
      }
      socket.lastChatAt = Date.now();
    }

    messageData.isAdmin = isAdmin;
    messageData.role = actorRole;
    messageData.isModerator = actorRole === 'mod';
    messageData.user = senderName;

    messageHistory.push(messageData);

    if (messageHistory.length > MAX_CHAT_HISTORY) messageHistory.shift(); 

    await pool.query('INSERT INTO chat (message) VALUES ($1)', [messageData]);
    io.emit('chat_message', messageData); 
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
      if (!targetName || !newPassword) return respond({ success: false, message: "Missing target or password." });
      if (!userDatabase[targetName]) return respond({ success: false, message: "User not found." });

      const hash = await bcrypt.hash(newPassword, 10);
      userDatabase[targetName].passwordHash = hash;
      userDatabase[targetName].passwordResetAt = new Date().toISOString();
      userDatabase[targetName].passwordResetBy = socket.userName || normalizeText(data.adminUser, "Admin");
      await saveUser(targetName);

      getSocketsByUserName(targetName).forEach(client => {
        client.emit('password_reset_by_admin', { by: socket.userName || 'Admin' });
      });

      await addModerationLog('reset_password', `Reset temporary password for ${targetName}`, { targetName }, socket.userName || 'Admin');
      respond({ success: true });
    } catch (err) {
      console.error('[ADMIN RESET PASSWORD ERROR]:', err);
      respond({ success: false, message: "Server error while resetting password." });
    }
  });

  socket.on('admin_ban_user', async (data, callback) => {
    const respond = typeof callback === 'function' ? callback : () => {};
    try {
      if (socket.isAdmin !== true) return respond({ success: false, message: "Admin only." });

      const targetName = normalizeText(data && data.targetName, "");
      const reason = normalizeText(data && data.reason, "Banned by administrator");
      if (!targetName) return respond({ success: false, message: "Missing target user." });
      if (!userDatabase[targetName]) return respond({ success: false, message: "User not found." });
      if (ADMIN_USERS.includes(targetName)) return respond({ success: false, message: "Hardcoded admins cannot be banned from the panel." });

      userDatabase[targetName].role = "banned";
      userDatabase[targetName].banned = true;
      userDatabase[targetName].banReason = reason;
      userDatabase[targetName].bannedBy = socket.userName || normalizeText(data.adminUser, "Admin");
      userDatabase[targetName].bannedAt = new Date().toISOString();
      await saveUser(targetName);

      disconnectUserSessions(targetName, 'user_banned', { reason });
      io.emit('online_list', getSanitizedOnlineList());

      await addModerationLog('ban', `Banned ${targetName}`, { targetName, reason }, socket.userName || 'Admin');
      respond({ success: true });
    } catch (err) {
      console.error('[ADMIN BAN ERROR]:', err);
      respond({ success: false, message: "Server error while banning user." });
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
        if (result.role === "banned") disconnectUserSessions(targetName, 'user_banned', { reason: userDatabase[targetName].banReason || "Banned by administrator" });
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
      await addModerationLog(adminState.maintenance.enabled ? 'maintenance_on' : 'maintenance_off', adminState.maintenance.enabled ? 'Enabled maintenance mode' : 'Disabled maintenance mode', adminState.maintenance, socket.userName || 'Admin');
      respond({ success: true, state: adminState.maintenance });
    } catch (err) {
      console.error('[ADMIN MAINTENANCE ERROR]:', err);
      respond({ success: false, message: "Server error while updating maintenance mode." });
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

  socket.on('admin_request_moderation_log', () => {
    if (socket.isAdmin === true) {
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
            await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [msg, msg.time]);
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
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [msg, msg.time]);
                
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
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [msg, msg.time]);
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
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [msg, msg.time]);
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

  socket.on('clear_chat', async () => {
    if (socket.isAdmin === true) {
        messageHistory = [];
        await pool.query('TRUNCATE chat');
        io.emit('chat_cleared');

        pinnedMessages = [];
        await pool.query('TRUNCATE pinned_messages');
        io.emit('pinned_list', pinnedMessages);

        await addModerationLog('clear_chat', 'Cleared global chat history', {}, socket.userName || 'Admin');
    }
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
      userDatabase[name].online = false;
      userDatabase[name].lastSeen = Date.now();
      socket.broadcast.emit('user_stopped_typing', { name: name });
      
      await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
      io.emit('online_list', getSanitizedOnlineList());
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`PSN Database Server running on port ${PORT}`);
});