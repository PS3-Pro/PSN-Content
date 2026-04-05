const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const APP_URL = "https://psn-content.onrender.com/ping"; 

app.get('/ping', (req, res) => {
  res.send('Servidor Acordado!');
});

setInterval(() => {
  https.get(APP_URL, (res) => {
    console.log(`Auto-ping: Status ${res.statusCode} - Mantendo o motor ligado!`);
  }).on('error', (err) => {
    console.error("Erro no auto-ping:", err.message);
  });
}, 840000);
// ---------------------------------

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let messageHistory = []; 

io.on('connection', (socket) => {
  console.log('User connected: ' + socket.id);

  socket.emit('chat_history', messageHistory);

  socket.on('chat_message', (msg) => {
    messageHistory.push(msg);

    if (messageHistory.length > 1000) {
      messageHistory.shift(); 
    }

    socket.broadcast.emit('chat_message', msg); 
  });

  socket.on('disconnect', () => {
    console.log('User disconnected');
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Chat server running on port ${PORT}`);
});