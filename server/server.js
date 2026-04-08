const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content.onrender.com/ping"; 

app.get('/ping', (req, res) => {
  res.send('Server is Awake!');
});

setInterval(() => {
  https.get(APP_URL, (res) => {
    console.log(`Auto-ping: Status ${res.statusCode} - Keeping the engine running!`);
  }).on('error', (err) => {
    console.error("Auto-ping error:", err.message);
  });
}, 840000);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 5e6
});

let messageHistory = []; 
let onlineUsers = {};

io.on('connection', (socket) => {
  console.log('User connected: ' + socket.id);

  socket.emit('chat_history', messageHistory);

  socket.on('register_user', (username) => {
    if (username) {
      onlineUsers[socket.id] = username;
      io.emit('online_list', Object.values(onlineUsers));
    }
  });

  socket.on('delete_message', (data) => {
    const msgIndex = messageHistory.findIndex(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return mId == data.msgId;
    });

    if (msgIndex > -1) {
        if (messageHistory[msgIndex].user === data.user) {
            messageHistory.splice(msgIndex, 1);
            io.emit('message_deleted', data.msgId);
        }
    }
  });

  socket.on('message_reaction', (data) => {
    const message = messageHistory.find(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return mId == data.msgId;
    });

    if (message) {
        if (!message.reactions) message.reactions = [];
        let existingReaction = message.reactions.find(r => r.emoji === data.emoji);
        
        if (existingReaction) {
            const userIndex = existingReaction.users.indexOf(data.user);
            if (userIndex > -1) {
                existingReaction.users.splice(userIndex, 1);
                existingReaction.count--;
            } else {
                existingReaction.users.push(data.user);
                existingReaction.count++;
            }
            if (existingReaction.count <= 0) {
                message.reactions = message.reactions.filter(r => r.emoji !== data.emoji);
            }
        } else {
            message.reactions.push({ 
                emoji: data.emoji, 
                count: 1, 
                users: [data.user] 
            });
        }
    }
    io.emit('message_reaction', data); 
  });

  socket.on('chat_message', (msg) => {
    let messageData = {
      ...(typeof msg === 'object' ? msg : { text: msg }),
      time: new Date().toISOString() // Carimbo oficial do servidor
    };

    messageHistory.push(messageData);
    if (messageHistory.length > 1000) messageHistory.shift();

    io.emit('chat_message', messageData); 
  });

  socket.on('disconnect', () => {
    if (onlineUsers[socket.id]) {
      const usernameSair = onlineUsers[socket.id];
      delete onlineUsers[socket.id];
      io.emit('online_list', Object.values(onlineUsers));
    }
    console.log('User disconnected');
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Chat server running on port ${PORT}`);
});