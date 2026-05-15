const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const app = express();
const server = http.createServer(app);

const ADMIN_USERS = ["Luan Teles", "Goku Cheats"];

const ADMIN_SECRET = process.env.ADMIN_SECRET || "ADMINENABLED";

const DEFAULT_AVATAR = "https://raw.githubusercontent.com/PS3-Pro/PSN-Content/master/resources/interface/modern/images/avatars/default.png";

const MAX_CHAT_HISTORY = 1000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL
    ? { rejectUnauthorized: false }
    : false
});

app.get('/', (req, res) => {
  res.send('PSN Database Server Online');
});

app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

let userDatabase = {};
let messageHistory = [];
let pinnedMessages = [];

async function initDb() {

  try {

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

    messageHistory = chatRes.rows
      .map(r => r.message)
      .reverse();

    const pinnedRes = await pool.query(`
      SELECT data
      FROM pinned_messages
      ORDER BY id ASC
    `);

    pinnedMessages = pinnedRes.rows.map(r => r.data);

    console.log(`[DB] Loaded ${messageHistory.length} messages`);

  } catch (err) {

    console.error("[DB ERROR]", err);

  }

}

initDb();

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

  transports: ["websocket", "polling"],

  maxHttpBufferSize: 1e7
});

io.on('connection', (socket) => {

  console.log('[NETWORK] Connected:', socket.id);

  socket.on('authenticate_user', async (data) => {

    try {

      const {
        name,
        password,
        userData,
        isNewAccount
      } = data;

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
          socket.emit(
            'auth_error',
            'Legacy account detected! Please recreate your ID.'
          );
          return;
        }

        const match = await bcrypt.compare(
          password,
          dbUser.passwordHash
        );

        if (!match) {

          if (isNewAccount) {
            socket.emit(
              'auth_error',
              'This Online ID is already taken...'
            );
          } else {
            socket.emit(
              'auth_error',
              'Incorrect password.'
            );
          }

          return;
        }

        socket.userName = name;
        socket.isAdmin = isAdmin;

        userDatabase[name] = {
          ...dbUser,
          online: true,
          id: socket.id,
          lastSeen: Date.now(),
          name
        };

        pool.query(
          'UPDATE users SET data = $1 WHERE name = $2',
          [userDatabase[name], name]
        ).catch(console.error);

        socket.emit('auth_success', {
          name,
          userData: userDatabase[name],
          isAdmin
        });

        socket.emit('chat_history', messageHistory);
        socket.emit('pinned_list', pinnedMessages);

        io.emit('online_list', getSanitizedOnlineList());

        console.log(`[LOGIN] ${name}`);

      } else {

        const hash = await bcrypt.hash(password, 10);

        socket.userName = name;
        socket.isAdmin = isAdmin;

        userDatabase[name] = {
          ...userData,
          name,
          passwordHash: hash,
          id: socket.id,
          online: true,
          lastSeen: Date.now(),
          avatar: userData?.avatar || DEFAULT_AVATAR,
          joined: userData?.joined || '2026',
          settingsData: userData?.settingsData || {},
          trophiesData: userData?.trophiesData || {},
          wishlistData: userData?.wishlistData || [],
          favoritesData: userData?.favoritesData || [],
          downloadsData: userData?.downloadsData || [],
          libraryData: userData?.libraryData || [],
          friendsData: userData?.friendsData || [],
          countersData: userData?.countersData || {},
          themeColor: userData?.themeColor || '#0070cc'
        };

        pool.query(
          'INSERT INTO users (name, data) VALUES ($1, $2)',
          [name, userDatabase[name]]
        ).catch(console.error);

        socket.emit('auth_success', {
          name,
          userData: userDatabase[name],
          isAdmin
        });

        socket.emit('chat_history', messageHistory);
        socket.emit('pinned_list', pinnedMessages);

        io.emit('online_list', getSanitizedOnlineList());

        console.log(`[REGISTER] ${name}`);
      }

    } catch (err) {

      console.error("[AUTH ERROR]", err);

      socket.emit(
        'auth_error',
        'Server Error.'
      );
    }

  });

});

const PORT = process.env.PORT || 3000;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`PSN Database Server running on ${PORT}`);
});