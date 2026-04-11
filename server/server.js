const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content.onrender.com/ping"; 

const ADMIN_USERS = ["Luan Teles", "Admin"];
const ADMIN_SECRET = process.env.ADMIN_SECRET || "311680"; 

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
        return String(mId) === String(data.msgId);
    });

    if (msgIndex > -1) {
        const isMessageOwner = messageHistory[msgIndex].user === data.user;
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;

        if (isMessageOwner || isAdmin) {
            messageHistory.splice(msgIndex, 1);
            io.emit('message_deleted', data.msgId);
            console.log(`Message deleted: ID ${data.msgId} by ${data.user} (Admin: ${isAdmin})`);
        } else {
            console.log(`BLOCKED: User ${data.user} tried to delete a message without permission or wrong secret.`);
        }
    }
  });

  socket.on('edit_message', (data) => {
    console.log(`Edit attempt: ID ${data.msgId} by ${data.user}`);

    const msgIndex = messageHistory.findIndex(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
    });

    if (msgIndex > -1) {
        const msgOwner = messageHistory[msgIndex].user;
        const isMessageOwner = msgOwner === data.user;
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;

        if (isMessageOwner || isAdmin) {
            console.log(`Message found! Updating: "${messageHistory[msgIndex].text}" to "${data.newText}"`);
            
            const wasEditedByAdmin = (isAdmin && !isMessageOwner);
            
            messageHistory[msgIndex].text = data.newText;
            messageHistory[msgIndex].edited = true;
            messageHistory[msgIndex].editedByAdmin = wasEditedByAdmin;
            
            io.emit('message_edited', { 
                msgId: data.msgId, 
                newText: data.newText,
                editedByAdmin: wasEditedByAdmin 
            });
        } else {
            console.log(`BLOCKED: User ${data.user} tried to edit ${msgOwner}'s message (Wrong secret or not owner).`);
        }
    } else {
        console.log(`Error: Message ID ${data.msgId} not found in history.`);
    }
  });

  socket.on('mark_as_read', (data) => {
    const msg = messageHistory.find(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
    });

    if (msg && msg.user !== data.user) {
        if (!msg.seenBy) msg.seenBy = [];
        
        if (!msg.seenBy.includes(data.user)) {
            msg.seenBy.push(data.user);
            console.log(`Message seen: ID ${data.msgId} by ${data.user}`);
            
            io.emit('message_seen', { msgId: data.msgId, seenBy: msg.seenBy });
        }
    }
  });
 
  socket.on('message_reaction', (data) => {
    const message = messageHistory.find(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
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
      time: new Date().toISOString(),
      seenBy: []
    };

    messageHistory.push(messageData);
    if (messageHistory.length > 100) messageHistory.shift();

    io.emit('chat_message', messageData); 
    console.log(`New message from ${messageData.user}`);
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