const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const server = http.createServer(app);

const APP_URL = "https://server-7lsr.onrender.com/ping";
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
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS chat (
      id SERIAL PRIMARY KEY,
      message JSONB
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pinned_messages (
      id SERIAL PRIMARY KEY,
      message_id TEXT UNIQUE,
      data JSONB
    )
  `);

  const usersRes = await pool.query('SELECT * FROM users');

  usersRes.rows.forEach(row => {
    userDatabase[row.name] = row.data;
    userDatabase[row.name].name = row.name;
    userDatabase[row.name].online = false;
  });

  const chatRes = await pool.query(`
    SELECT message
    FROM chat
    ORDER BY id DESC
    LIMIT $1
  `, [MAX_CHAT_HISTORY]);

  messageHistory = chatRes.rows.map(r => r.message).reverse();

  const pinnedRes = await pool.query(`
    SELECT data
    FROM pinned_messages
    ORDER BY id ASC
  `);

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
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 1e7
});

io.on('connection', (socket) => {
  console.log('[NETWORK] Socket connected. ID: ' + socket.id);

  socket.on('authenticate_user', async (data) => {
    try {
      const { name, password, userData, isNewAccount } = data;

      if (!name || !password) {
        socket.emit('auth_error', 'Missing credentials.');
        return;
      }

      const dbRes = await pool.query(
        'SELECT data FROM users WHERE name = $1',
        [name]
      );

      const dbUser = dbRes.rows.length > 0
        ? dbRes.rows[0].data
        : null;

      const isAdmin = ADMIN_USERS.includes(name);

      if (dbUser) {

        if (!dbUser.passwordHash) {
          socket.emit('auth_error', 'Legacy account detected! Please recreate your ID.');
          return;
        }

        const match = await bcrypt.compare(
          password,
          dbUser.passwordHash
        );

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

          await pool.query(
            'UPDATE users SET data = $1 WHERE name = $2',
            [userDatabase[name], name]
          );

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
          settingsData: userData.settingsData || {
            audio: "1",
            ux: "1",
            chatSound: "1",
            ps3Ip: ""
          },
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

  socket.on('chat_message', async (msg) => {
    try {

      let messageData = {
        ...(typeof msg === 'object' ? msg : { text: msg }),
        time: new Date().toISOString(),
        seenBy: []
      };

      const isAdmin = socket.isAdmin === true;

      if (messageData.text === '/reload' && isAdmin) {
        return socket.broadcast.emit('force_reload');
      }

      messageData.isAdmin = isAdmin;
      messageData.user = socket.userName || messageData.user;

      messageHistory.push(messageData);

      if (messageHistory.length > MAX_CHAT_HISTORY) {
        messageHistory.shift();
      }

      await pool.query(
        'INSERT INTO chat (message) VALUES ($1)',
        [messageData]
      );

      await pool.query(`
        DELETE FROM chat
        WHERE id NOT IN (
          SELECT id
          FROM chat
          ORDER BY id DESC
          LIMIT $1
        )
      `, [MAX_CHAT_HISTORY]);

      io.emit('chat_message', messageData);

    } catch (err) {

      console.error("Chat Save Error:", err);

    }
  });

  socket.on('disconnect', async () => {
    try {

      const name = socket.userName;

      if (name && userDatabase[name]) {

        userDatabase[name].online = false;
        userDatabase[name].lastSeen = Date.now();

        await pool.query(
          'UPDATE users SET data = $1 WHERE name = $2',
          [userDatabase[name], name]
        );

        io.emit('online_list', getSanitizedOnlineList());
      }

    } catch (err) {

      console.error("Disconnect Error:", err);

    }
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`PSN Database Server running on port ${PORT}`);
});