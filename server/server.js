const express = require('express');
const http = require('http');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*", 
    methods: ["GET", "POST"]
  }
});

io.on('connection', (socket) => {
  console.log('Um usuário conectou: ' + socket.id);

  socket.on('mensagem_chat', (msg) => {
    io.emit('mensagem_chat', msg); 
  });

  socket.on('disconnect', () => {
    console.log('Usuário desconectou');
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Chat sever running in ${PORT}`);
});