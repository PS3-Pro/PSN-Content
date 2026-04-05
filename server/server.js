const express = require('express');
const http = require('http');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let messageHistory = []; 

io.on('connection', (socket) => {
  console.log('User connected: ' + socket.id);

  socket.emit('chat_history', messageHistory);

  socket.on('chat_message', (msg) => {
    messageHistory.push(msg);

    if (messageHistory.length > 500) {
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