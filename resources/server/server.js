const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content-8o6c.onrender.com/ping";
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
    userDatabase[row.name].name = row.name; 
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
    return Object.entries(userDatabase).map(([username, u]) => ({
        id: u.id,
        name: username, 
        avatar: u.avatar || 'default_avatar.png', 
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
  console.log('[NETWORK] Socket conectado. ID: ' + socket.id);
  
  socket.emit('chat_history', messageHistory);

  socket.on('authenticate_user', async (data) => {
    try {
        const { name, password, userData, isNewAccount } = data;
        const user = userDatabase[name];

        if (user) {
           if (!user.passwordHash) {
                socket.emit('auth_error', 'Legacy account detected! Please delete it or create a new ID.');
                return;
            }

            const match = await bcrypt.compare(password, user.passwordHash);
            
            if (match) {
                socket.userName = name;
                userDatabase[name].online = true;
                userDatabase[name].id = socket.id;
                userDatabase[name].lastSeen = Date.now();
                userDatabase[name].name = name;
                
                await pool.query('UPDATE users SET data = $1 WHERE name = $2', [userDatabase[name], name]);
                
                console.log(`[NETWORK] ${name} entrou (Redirecionado para Login).`);
                socket.emit('auth_success', { name, userData: userDatabase[name] });
                io.emit('online_list', getSanitizedOnlineList());
            } else {
                if (isNewAccount) {
                    socket.emit('auth_error', 'This Online ID is already taken. Please choose another one.');
                } else {
                    socket.emit('auth_error', 'Incorrect password. Access denied.');
                }
            }
        } else {
            const hash = await bcrypt.hash(password, 10);
            
            socket.userName = name;
            userDatabase[name] = {
                ...userData,
                name: name,
                passwordHash: hash,
                id: socket.id,
                online: true,
                lastSeen: Date.now(),
                avatar: userData.avatar || 'default_avatar.png',
                joined: userData.joined || '2026',
                trophiesData: userData.trophiesData || {},
                wishlistData: userData.wishlistData || [],
                favoritesData: userData.favoritesData || [],
                downloadsData: userData.downloadsData || [],
                libraryData: userData.libraryData || []
            };

            await pool.query(
                'INSERT INTO users (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = $2',
                [name, userDatabase[name]]
            );
            
            console.log(`[NETWORK] ${name} criou uma conta nova.`);
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
      if (userData.avatar === null || userData.avatar === undefined) {
          delete userData.avatar;
      }

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
        const keyMap = {
            'favs': 'favoritesData',
            'wishlist': 'wishlistData',
            'downloads': 'downloadsData',
            'library': 'libraryData',
            'trophies': 'trophiesData'
        };

        const dataKey = keyMap[type] || (type + 'Data');
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
    const results = Object.entries(userDatabase)
      .filter(([username, u]) => username.toLowerCase().includes(searchTerm))
      .map(([username, u]) => ({
        name: username,
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
    const msgIndex = messageHistory.findIndex(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msgIndex > -1) {
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
        if (messageHistory[msgIndex].user === data.user || isAdmin) {
            messageHistory[msgIndex].text = data.newText;
            messageHistory[msgIndex].edited = true;
            if (data.content) {
                messageHistory[msgIndex].type = 'image';
                messageHistory[msgIndex].content = data.content;
            }
            
            await pool.query('DELETE FROM chat');
            for(const m of messageHistory) { await pool.query('INSERT INTO chat (message) VALUES ($1)', [m]); }
            
            io.emit('message_edited', { 
                msgId: data.msgId, 
                newText: data.newText,
                type: data.content ? 'image' : messageHistory[msgIndex].type,
                content: data.content || messageHistory[msgIndex].content
            });
        }
    }
  });

  socket.on('delete_message', async (data) => {
    const msgIndex = messageHistory.findIndex(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msgIndex > -1) {
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
        if (messageHistory[msgIndex].user === data.user || isAdmin) {
            messageHistory.splice(msgIndex, 1);
            await pool.query('DELETE FROM chat');
            for(const m of messageHistory) { await pool.query('INSERT INTO chat (message) VALUES ($1)', [m]); }
            io.emit('message_deleted', data.msgId);
        }
    }
  });

  socket.on('clear_chat', async (data) => {
    if (ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET) {
        messageHistory = [];
        await pool.query('TRUNCATE chat');
        io.emit('chat_cleared');
    }
  });

  socket.on('kick_user', (data) => {
    if (ADMIN_USERS.includes(data.adminUser) && data.secret === ADMIN_SECRET) {
        const targetSocket = io.sockets.sockets.get(data.targetId);
        if (targetSocket) {
            targetSocket.emit('user_kicked');
            socket.emit('kick_success', { targetId: data.targetId });
            setTimeout(() => { targetSocket.disconnect(true); }, 500);
        }
    }
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
  console.log(`Neon Server running on port ${PORT}`);
});