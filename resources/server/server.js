const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content-8o6c.onrender.com/ping";
const ADMIN_USERS = ["Luan Teles", "Goku Cheats"];
const ADMIN_SECRET = process.env.ADMIN_SECRET || "ADMINENABLED";

const DEFAULT_AVATAR = "https://raw.githubusercontent.com/PS3-Pro/PSN-Content/master/resources/interface/modern/images/avatars/default.png";

const MAX_CHAT_HISTORY = 1000; 

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

  console.log(`[DB] Database initialized. ${messageHistory.length} messages and ${pinnedMessages.length} pins loaded.`);
}

initDb().catch(console.error);

setInterval(() => {
  https.get(APP_URL, (res) => {
    console.log(`Auto-ping: Status ${res.statusCode}`);
  }).on('error', (err) => {
    console.error("Auto-ping error:", err.message);
  });
}, 840000);

function getSanitizedOnlineList() {
    return Object.entries(userDatabase).map(([username, u]) => ({
        id: u.id,
        name: username, 
        avatar: u.avatar || DEFAULT_AVATAR,
        isAdmin: ADMIN_USERS.includes(username), 
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

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e7
});

io.on('connection', (socket) => {
  console.log('[NETWORK] Socket connected. ID: ' + socket.id);

  socket.on('authenticate_user', async (data) => {
    try {
      const { name, password, userData, isNewAccount } = data;
      
      const dbRes = await pool.query('SELECT data FROM users WHERE name = $1', [name]);
      const dbUser = dbRes.rows.length > 0 ? dbRes.rows[0].data : null;

      const isAdmin = ADMIN_USERS.includes(name);

      if (dbUser) {
        if (!dbUser.passwordHash) {
          socket.emit('auth_error', 'Legacy account detected! Please recreate your ID.');
          return;
        }

        const match = await bcrypt.compare(password, dbUser.passwordHash);
        
        if (match) {
          socket.userName = name;
          socket.isAdmin = isAdmin;

          userDatabase[name] = {
            ...dbUser,
            online: true,
            id: socket.id,
            lastSeen: Date.now(),
            name: name
          };
          
          await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
          
          console.log(`[NETWORK] ${name} logged in. Admin: ${isAdmin}`);

          socket.emit('auth_success', { 
            name, 
            userData: userDatabase[name],
            isAdmin: isAdmin 
          });

          socket.emit('chat_history', messageHistory);
          socket.emit('pinned_list', pinnedMessages);

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
          trophiesData: userData.trophiesData || {},
          wishlistData: userData.wishlistData || [],
          favoritesData: userData.favoritesData || [],
          downloadsData: userData.downloadsData || [],
          libraryData: userData.libraryData || [],
          friendsData: userData.friendsData || [],
          countersData: userData.countersData || {},
          themeColor: userData.themeColor || '#0070cc'
        };

        await pool.query(
          'INSERT INTO users (name, data) VALUES ($1, $2)',
          [name, userDatabase[name]]
        );
        
        console.log(`[NETWORK] ${name} created a new account. Admin: ${isAdmin}`);

        socket.emit('auth_success', { 
          name, 
          userData: userDatabase[name],
          isAdmin: isAdmin 
        });

        socket.emit('chat_history', messageHistory);
        socket.emit('pinned_list', pinnedMessages);

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
      if (userData.avatar === null || userData.avatar === undefined) {
          delete userData.avatar;
      }
      
      Object.assign(userDatabase[name], userData);
      userDatabase[name].lastSeen = Date.now();
      
      try {
        await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
        console.log(`[DATABASE] Profile for ${name} updated successfully.`);
      } catch (err) {
        console.error(`[DATABASE ERROR] Failed to save profile for ${name}:`, err);
      }

      io.emit('online_list', getSanitizedOnlineList());
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

  socket.on('search_users', (query) => {
    if (!query || query.length < 2) return;
    const searchTerm = query.toLowerCase();
    const results = Object.entries(userDatabase)
      .filter(([username, u]) => username.toLowerCase().includes(searchTerm))
      .map(([username, u]) => ({
        name: username, avatar: u.avatar, online: u.online, lastSeen: u.lastSeen, level: u.level, ps3Status: u.ps3Status || null 
      })).slice(0, 15);
    socket.emit('global_search_results', results);
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

    if (cleanCode === ADMIN_SECRET.toUpperCase()) {
        const isNameValid = user && ADMIN_USERS.some(admin => admin.toLowerCase() === user.toLowerCase());
        if (isNameValid) {
            socket.isAdmin = true;
            return callback({ success: true, type: 'ADMIN_LOGIN', secret: ADMIN_SECRET });
        } else {
            return callback({ success: false, message: "Code valid, but your ID is not Admin." });
        }
    }
    callback({ success: false, message: "Invalid code." });
  });

  socket.on('chat_message', async (msg) => {
    let messageData = { ...(typeof msg === 'object' ? msg : { text: msg }), time: new Date().toISOString(), seenBy: [] };
    const isAdmin = socket.isAdmin === true;
    
    if (messageData.text === '/reload' && isAdmin) return socket.broadcast.emit('force_reload');
    
    messageData.isAdmin = isAdmin;
    messageData.user = socket.userName || messageData.user;
    
    messageHistory.push(messageData);
    
    if (messageHistory.length > MAX_CHAT_HISTORY) messageHistory.shift(); 
    
    await pool.query('INSERT INTO chat (message) VALUES ($1)', [messageData]);
    io.emit('chat_message', messageData); 
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
        const msg = messageHistory[msgIndex];

        if (msg.user === socket.userName || isAdmin) {
            const wasEditedByAdmin = (isAdmin && msg.user !== socket.userName);
            
            msg.text = data.newText;
            msg.edited = true;
            msg.editedByAdmin = wasEditedByAdmin;

            if (data.content) {
                msg.type = 'image';
                msg.content = data.content;
            }
            
            try {
                await pool.query("UPDATE chat SET message = $1 WHERE message->>'time' = $2", [msg, msg.time]);
                io.emit('message_edited', { 
                    msgId: data.msgId, 
                    newText: data.newText, 
                    type: msg.type, 
                    content: msg.content,
                    editedByAdmin: wasEditedByAdmin 
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
        const msgTime = messageHistory[msgIndex].time;

        if (messageHistory[msgIndex].user === socket.userName || isAdmin) {
            messageHistory.splice(msgIndex, 1);
            try {
                await pool.query("DELETE FROM chat WHERE message->>'time' = $1", [msgTime]);
            } catch (err) {
                console.error("Erro ao deletar mensagem do banco:", err);
            }

            io.emit('message_deleted', data.msgId);

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
    }
  });

  socket.on('kick_user', (data) => {
    if (socket.isAdmin === true) {
        const targetSocket = io.sockets.sockets.get(data.targetId);
        if (targetSocket) {
            targetSocket.emit('user_kicked');
            socket.emit('kick_success', { targetId: data.targetId });
            
            setTimeout(() => { 
                if (targetSocket.connected) {
                    targetSocket.disconnect(true);
                }
            }, 2500);
        }
    }
  });

  socket.on('pin_message', async (data) => {
    if (socket.isAdmin === true) {
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
        } catch (e) { console.error("Pin DB Error:", e); }
      }
    }
  });

  socket.on('unpin_message', async (data) => {
    if (socket.isAdmin === true) {
      pinnedMessages = pinnedMessages.filter(p => p.id !== data.msgId);
      
      try {
        await pool.query('DELETE FROM pinned_messages WHERE message_id = $1', [data.msgId]);
      } catch (e) { console.error("Unpin DB Error:", e); }

      io.emit('pinned_list', pinnedMessages);
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