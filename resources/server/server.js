const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content.onrender.com/ping";
const ADMIN_USERS = ["Luan Teles", "Goku Cheats", "Admin"];
const ADMIN_SECRET = process.env.ADMIN_SECRET || "ADMINENABLED";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

let userDatabase = {};
let messageHistory = [];

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
  `);

  const usersRes = await pool.query('SELECT * FROM users');
  usersRes.rows.forEach(row => {
    userDatabase[row.name] = row.data;
    userDatabase[row.name].online = false;
  });

  const chatRes = await pool.query('SELECT message FROM chat ORDER BY id ASC LIMIT 100');
  messageHistory = chatRes.rows.map(r => r.message);
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
    return Object.values(userDatabase).map(u => ({
        id: u.id,
        name: u.name,
        avatar: u.avatar,
        level: u.level,
        joined: u.joined,
        online: u.online,
        lastSeen: u.lastSeen,
        ps3Status: u.ps3Status,
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
  
  socket.emit('chat_history', messageHistory);

  socket.on('authenticate_user', async (data) => {
    try {
        const { name, password, userData } = data;
        const user = userDatabase[name];

        if (user) {
            if (!user.passwordHash) {
                socket.emit('auth_error', 'Legacy account detected! Please delete it from the Neon Database or create a new User ID.');
                return;
            }

            const match = await bcrypt.compare(password, user.passwordHash);
            if (match) {
                socket.userName = name;
                userDatabase[name].online = true;
                userDatabase[name].id = socket.id;
                userDatabase[name].lastSeen = Date.now();
                
                await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
                
                socket.emit('auth_success', { name, userData: userDatabase[name] });
                io.emit('online_list', getSanitizedOnlineList());
            } else {
                socket.emit('auth_error', 'Incorrect password. Access denied.');
            }
        } else {
            const hash = await bcrypt.hash(password, 10);
            
            socket.userName = name;
            userDatabase[name] = {
                ...userData,
                passwordHash: hash,
                id: socket.id,
                online: true,
                lastSeen: Date.now()
            };

            await pool.query(
                'INSERT INTO users (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = $2',
                [name, userDatabase[name]]
            );
            
            socket.emit('auth_success', { name, userData: userDatabase[name] });
            io.emit('online_list', getSanitizedOnlineList());
        }
    } catch (error) {
        console.error("[AUTH ERROR]:", error);
        socket.emit('auth_error', 'Server Error: Something went wrong.');
    }
  });

  socket.on('update_profile', async (userData) => {
    const name = socket.userName;
    if (name && userDatabase[name]) {
      Object.assign(userDatabase[name], userData);
      userDatabase[name].lastSeen = Date.now();
      
      await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
      io.emit('online_list', getSanitizedOnlineList());
    }
  });

  socket.on('request_user_data', (data) => {
    const { targetName, type } = data;
    const targetUser = userDatabase[targetName];

    if (targetUser) {
        const dataKey = type + 'Data';
        const rawData = targetUser[dataKey] || [];

        socket.emit('user_data_response', {
            targetName: targetName,
            type: type,
            rawData: rawData
        });
    }
  });

  socket.on('search_users', (query) => {
    if (!query || query.length < 2) return;
    const searchTerm = query.toLowerCase();
    const results = Object.values(userDatabase)
      .filter(u => u.name.toLowerCase().includes(searchTerm))
      .map(u => ({
        name: u.name,
        avatar: u.avatar,
        online: u.online,
        lastSeen: u.lastSeen,
        level: u.level,
        ps3Status: u.ps3Status || null 
      }))
      .slice(0, 15);
    socket.emit('global_search_results', results);
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

  socket.on('chat_message', async (msg) => {
    let messageData = {
      ...(typeof msg === 'object' ? msg : { text: msg }),
      time: new Date().toISOString(),
      seenBy: []
    };
    
    const isAdmin = ADMIN_USERS.includes(messageData.user) && messageData.secret === ADMIN_SECRET;
    
    if (messageData.text === '/reload' && isAdmin) {
        return socket.broadcast.emit('force_reload');
    }
    
    delete messageData.secret;
    messageHistory.push(messageData);
    if (messageHistory.length > 100) messageHistory.shift();
    
    await pool.query('INSERT INTO chat (message) VALUES ($1)', [messageData]);
    io.emit('chat_message', messageData); 
  });

  socket.on('edit_message', async (data) => {
    const msgIndex = messageHistory.findIndex(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
    });
    if (msgIndex > -1) {
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
        if (messageHistory[msgIndex].user === data.user || isAdmin) {
            messageHistory[msgIndex].text = data.newText;
            messageHistory[msgIndex].edited = true;
            
            if (data.content) {
                messageHistory[msgIndex].type = 'image';
                messageHistory[msgIndex].content = data.content;
            }

            const wasEditedByAdmin = (isAdmin && messageHistory[msgIndex].user !== data.user);
            
            await pool.query('DELETE FROM chat');
            for(const m of messageHistory) {
                await pool.query('INSERT INTO chat (message) VALUES ($1)', [m]);
            }
            
            io.emit('message_edited', { 
                msgId: data.msgId, 
                newText: data.newText,
                type: data.content ? 'image' : messageHistory[msgIndex].type,
                content: data.content || messageHistory[msgIndex].content,
                editedByAdmin: wasEditedByAdmin
            });
        }
    }
  });

  socket.on('delete_message', async (data) => {
    const msgIndex = messageHistory.findIndex(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
    });
    if (msgIndex > -1) {
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
        if (messageHistory[msgIndex].user === data.user || isAdmin) {
            messageHistory.splice(msgIndex, 1);
            
            await pool.query('DELETE FROM chat');
            for(const m of messageHistory) {
                await pool.query('INSERT INTO chat (message) VALUES ($1)', [m]);
            }

            io.emit('message_deleted', data.msgId);
        }
    }
  });

  socket.on('clear_chat', async (data) => {
    const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
    if (isAdmin) {
        messageHistory = [];
        await pool.query('TRUNCATE chat');
        io.emit('chat_cleared');
    }
  });

  socket.on('kick_user', (data) => {
    const isAdmin = ADMIN_USERS.includes(data.adminUser) && data.secret === ADMIN_SECRET;
    if (isAdmin && data.targetId) {
        const targetSocket = io.sockets.sockets.get(data.targetId);
        if (targetSocket) {
            targetSocket.emit('user_kicked');
            socket.emit('kick_success', { targetId: data.targetId });
            setTimeout(() => { targetSocket.disconnect(true); }, 500);
        }
    }
  });

  socket.on('mark_as_read', async (data) => {
    const msg = messageHistory.find(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msg && msg.user !== data.user) {
        if (!msg.seenBy) msg.seenBy = [];
        if (!msg.seenBy.includes(data.user)) {
            msg.seenBy.push(data.user);
            
            await pool.query('DELETE FROM chat');
            for(const m of messageHistory) {
                await pool.query('INSERT INTO chat (message) VALUES ($1)', [m]);
            }

            io.emit('message_seen', { msgId: data.msgId, seenBy: msg.seenBy });
        }
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
        
        await pool.query('DELETE FROM chat');
        for(const m of messageHistory) {
            await pool.query('INSERT INTO chat (message) VALUES ($1)', [m]);
        }

        io.emit('message_reaction', data);
    }
  });

  socket.on('admin_redeem', (data, callback) => {
    const { code, user } = data;
    const cleanCode = code.replace(/-/g, "").toUpperCase();
    if (cleanCode === "PLATINUMCODE") return callback({ success: true, type: 'PLATINUM_UNLOCK' });
    if (cleanCode === "UNLOCKALLDB1") return callback({ success: true, type: 'SINGLE_TROPHY' });
    if (cleanCode === ADMIN_SECRET && ADMIN_USERS.includes(user)) {
        return callback({ success: true, type: 'ADMIN_LOGIN', secret: ADMIN_SECRET });
    }
    callback({ success: false, message: "Invalid code." });
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
  console.log(`Chat server running on port ${PORT}`);
});