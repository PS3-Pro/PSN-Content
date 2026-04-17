const express = require('express');
const http = require('http');
const https = require('https');
const fs = require('fs'); 
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content.onrender.com/ping"; 
const ADMIN_USERS = ["Luan Teles", "Goku Cheats", "Admin"];
const ADMIN_SECRET = process.env.ADMIN_SECRET || "ADMINENABLED"; 

const USER_DB_FILE = './userDatabase.json';
const CHAT_DB_FILE = './chatHistory.json';

app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

function loadData(filePath, defaultData) {
    try {
        if (fs.existsSync(filePath)) {
            const data = fs.readFileSync(filePath, 'utf8');
            return JSON.parse(data);
        }
    } catch (err) {
        console.error(`Erro ao carregar ${filePath}:`, err);
    }
    return defaultData;
}

function saveData(filePath, data) {
    try {
        fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
    } catch (err) {
        console.error(`Erro ao salvar ${filePath}:`, err);
    }
}

let userDatabase = loadData(USER_DB_FILE, {}); 
let messageHistory = loadData(CHAT_DB_FILE, []); 

setInterval(() => {
  https.get(APP_URL, (res) => {
    console.log(`Auto-ping: Status ${res.statusCode} - Engine running!`);
  }).on('error', (err) => {
    console.error("Auto-ping error:", err.message);
  });
}, 840000);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e7
});

io.on('connection', (socket) => {
  console.log('User connected: ' + socket.id);

  socket.emit('chat_history', messageHistory);

  socket.on('register_user', (userData) => {
    if (userData && userData.name) {
      const name = userData.name;
      socket.userName = name;

      userDatabase[name] = {
        id: socket.id,
        name: name,
        avatar: userData.avatar || null,
        level: userData.level || 1,
        joined: userData.joined || "2026",
        xp: userData.xp || 0,
        downloads: userData.downloads || 0,
        wishlist: userData.wishlist || 0,
        favorites: userData.favorites || 0,
        trophies: userData.trophies || 0,
        library: userData.library || 0,
        downloadsData: userData.downloadsData || [],
        wishlistData: userData.wishlistData || [],
        favoritesData: userData.favoritesData || [],
        libraryData: userData.libraryData || [],
        trophiesData: userData.trophiesData || {},
        ps3Status: userData.ps3Status || null,
        
        online: true,
        lastSeen: Date.now() 
      };

      console.log(`[NETWORK] ${name} is now Online.`);
      saveData(USER_DB_FILE, userDatabase);
      io.emit('online_list', Object.values(userDatabase));
    }
  });

  socket.on('update_profile', (userData) => {
    const name = socket.userName;
    if (name && userDatabase[name]) {
      Object.assign(userDatabase[name], userData);
      userDatabase[name].lastSeen = Date.now();
      saveData(USER_DB_FILE, userDatabase);
      io.emit('online_list', Object.values(userDatabase));
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

  socket.on('chat_message', (msg) => {
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
    
    saveData(CHAT_DB_FILE, messageHistory);
    io.emit('chat_message', messageData); 
  });

  socket.on('edit_message', (data) => {
    const msgIndex = messageHistory.findIndex(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
    });
    if (msgIndex > -1) {
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
        if (messageHistory[msgIndex].user === data.user || isAdmin) {
            messageHistory[msgIndex].text = data.newText;
            messageHistory[msgIndex].edited = true;
            saveData(CHAT_DB_FILE, messageHistory);
            io.emit('message_edited', { msgId: data.msgId, newText: data.newText });
        }
    }
  });

  socket.on('delete_message', (data) => {
    const msgIndex = messageHistory.findIndex(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
    });
    if (msgIndex > -1) {
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
        if (messageHistory[msgIndex].user === data.user || isAdmin) {
            messageHistory.splice(msgIndex, 1);
            saveData(CHAT_DB_FILE, messageHistory);
            io.emit('message_deleted', data.msgId);
        }
    }
  });

  socket.on('clear_chat', (data) => {
    const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
    if (isAdmin) {
        messageHistory = [];
        saveData(CHAT_DB_FILE, messageHistory);
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

  socket.on('mark_as_read', (data) => {
    const msg = messageHistory.find(m => String(new Date(m.time).getTime()) === String(data.msgId));
    if (msg && msg.user !== data.user) {
        if (!msg.seenBy) msg.seenBy = [];
        if (!msg.seenBy.includes(data.user)) {
            msg.seenBy.push(data.user);
            saveData(CHAT_DB_FILE, messageHistory); 
            io.emit('message_seen', { msgId: data.msgId, seenBy: msg.seenBy });
        }
    }
  });

  socket.on('message_reaction', (data) => {
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
        saveData(CHAT_DB_FILE, messageHistory); 
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

  socket.on('disconnect', () => {
    const name = socket.userName;
    if (name && userDatabase[name]) {
      userDatabase[name].online = false;
      userDatabase[name].lastSeen = Date.now();
      saveData(USER_DB_FILE, userDatabase);
      io.emit('online_list', Object.values(userDatabase));
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Chat server running on port ${PORT}`);
});