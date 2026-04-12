const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content.onrender.com/ping"; 
const ADMIN_USERS = ["Luan Teles", "Admin"];
const ADMIN_SECRET = process.env.ADMIN_SECRET || "ADMINENABLED"; 

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

  socket.on('admin_redeem', (data, callback) => {
    const { code, user } = data;
    const cleanCode = code.replace(/-/g, "").toUpperCase();

    if (cleanCode === ADMIN_SECRET && ADMIN_USERS.includes(user)) {
        console.log(`[AUTH] GOD MODE ACTIVATED for: ${user}`);
        callback({ 
            success: true, 
            secret: ADMIN_SECRET 
        });
    } else {
        console.log(`[AUTH] Failed Admin attempt by: ${user}`);
        callback({ 
            success: false, 
            message: "Unauthorized: Username not in Admin List or wrong code." 
        });
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
            console.log(`Message deleted: ID ${data.msgId} by ${data.user}`);
        }
    }
  });

  socket.on('edit_message', (data) => {
    const msgIndex = messageHistory.findIndex(m => {
        const mId = m.time ? new Date(m.time).getTime() : null;
        return String(mId) === String(data.msgId);
    });

    if (msgIndex > -1) {
        const msgOwner = messageHistory[msgIndex].user;
        const isMessageOwner = msgOwner === data.user;
        const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;

        if (isMessageOwner || isAdmin) {
            let textToSave = data.newText;
            let pingAtivo = false;

            if (textToSave && textToSave.includes('@everyone')) {
                if (isAdmin) {
                    pingAtivo = true;
                } else {
                    textToSave = textToSave.replace(/@everyone/g, "everyone");
                }
            }
            
            const wasEditedByAdmin = (isAdmin && !isMessageOwner);
            
            messageHistory[msgIndex].text = textToSave;
            messageHistory[msgIndex].edited = true;
            messageHistory[msgIndex].editedByAdmin = wasEditedByAdmin;
            messageHistory[msgIndex].isGlobalPing = pingAtivo;
            
            io.emit('message_edited', { 
                msgId: data.msgId, 
                newText: textToSave,
                editedByAdmin: wasEditedByAdmin,
                isGlobalPing: pingAtivo
            });
        }
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
            message.reactions.push({ emoji: data.emoji, count: 1, users: [data.user] });
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

    const isAdmin = ADMIN_USERS.includes(messageData.user) && messageData.secret === ADMIN_SECRET;
    
    if (messageData.text && messageData.text.includes('@everyone')) {
        if (isAdmin) {
            messageData.isGlobalPing = true;
        } else {
            messageData.isGlobalPing = false;
            messageData.text = messageData.text.replace(/@everyone/g, "everyone");
        }
    }

    delete messageData.secret;

    messageHistory.push(messageData);
    if (messageHistory.length > 100) messageHistory.shift();

    io.emit('chat_message', messageData); 
  });

  socket.on('clear_chat', (data) => {
    const isAdmin = ADMIN_USERS.includes(data.user) && data.secret === ADMIN_SECRET;
    
    if (isAdmin) {
        messageHistory = []; 
        io.emit('chat_cleared'); 
        console.log(`[ADMIN] Chat entirely cleared by: ${data.user}`);
    } else {
        console.log(`[AUTH] Unauthorized /clean attempt by: ${data.user}`);
    }
  });

  socket.on('disconnect', () => {
    if (onlineUsers[socket.id]) {
      delete onlineUsers[socket.id];
      io.emit('online_list', Object.values(onlineUsers));
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Chat server running on port ${PORT}`);
});