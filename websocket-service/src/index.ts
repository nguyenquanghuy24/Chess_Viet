import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import jwt from 'jsonwebtoken';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';

const service = 'websocket-service';
const port = Number(process.env.PORT || 3005);
const jwtSecret = process.env.JWT_SECRET || 'dev-secret-change-me';
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });
const redis = createClient({ url: process.env.REDIS_URL || `redis://${process.env.REDIS_HOST || 'redis'}:6379` });
const kafka = new Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: `${service}-group` });
type SocketUser = { id: string; username?: string; guest?: boolean };

app.get('/health', (_req, res) => res.json({ service, status: 'ok', sockets: io.engine.clientsCount }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\nwebsocket_clients ${io.engine.clientsCount}\n`));

io.use((socket, next) => {
  const token = socket.handshake.auth?.token || socket.handshake.query?.token;
  if (!token && process.env.AUTH_REQUIRED === 'false') {
    socket.data.user = { id: `guest-${socket.id}`, guest: true } satisfies SocketUser;
    return next();
  }
  if (!token) return next(new Error('missing_token'));
  try {
    const decoded = jwt.verify(String(token), jwtSecret) as jwt.JwtPayload;
    socket.data.user = { id: String(decoded.sub), username: decoded.username ? String(decoded.username) : undefined } satisfies SocketUser;
    next();
  } catch {
    next(new Error('invalid_token'));
  }
});

io.on('connection', (socket) => {
  const authedUser = socket.data.user as SocketUser;

  socket.on('game:join', async ({ gameId, spectator }) => {
    socket.join(`game:${gameId}`);
    await redis.hSet(`presence:${gameId}`, socket.id, JSON.stringify({ userId: authedUser.id, username: authedUser.username, spectator: !!spectator, at: Date.now() }));
    const state = await redis.hGetAll(`game:${gameId}`);
    socket.emit('game:state', state);
    io.to(`game:${gameId}`).emit('presence:changed', await redis.hLen(`presence:${gameId}`));
  });

  socket.on('game:move', async (payload, ack) => {
    await producer.send({ topic: 'move.requested', messages: [{ key: payload.gameId, value: JSON.stringify({ gameId: payload.gameId, playerId: authedUser.id, from: payload.from, to: payload.to, promotion: payload.promotion || 'q', socketId: socket.id, at: new Date().toISOString() }) }] }).catch(console.warn);
    ack?.({ accepted: true, queued: true });
  });

  socket.on('chat:message', async (payload) => {
    const event = { ...payload, userId: authedUser.id, username: authedUser.username, body: String(payload.body || '').slice(0, 500) };
    await producer.send({ topic: 'chat.message.sent', messages: [{ key: event.roomId || event.gameId, value: JSON.stringify(event) }] }).catch(console.warn);
    io.to(`game:${event.gameId}`).emit('chat:message', event);
  });

  socket.on('disconnecting', async () => {
    for (const room of socket.rooms) {
      if (room.startsWith('game:')) {
        const gameId = room.replace('game:', '');
        await redis.hDel(`presence:${gameId}`, socket.id);
        io.to(room).emit('presence:changed', await redis.hLen(`presence:${gameId}`));
      }
    }
  });
});

async function main() {
  await Promise.all([redis.connect(), producer.connect().catch(() => undefined), consumer.connect().catch(() => undefined)]);
  await consumer.subscribe({ topic: 'move.played', fromBeginning: false }).catch(() => undefined);
  await consumer.subscribe({ topic: 'game.started', fromBeginning: false }).catch(() => undefined);
  await consumer.subscribe({ topic: 'game.finished', fromBeginning: false }).catch(() => undefined);
  await consumer.subscribe({ topic: 'timer.tick', fromBeginning: false }).catch(() => undefined);
  await consumer.subscribe({ topic: 'draw.offered', fromBeginning: false }).catch(() => undefined);
  await consumer.subscribe({ topic: 'friend.invited', fromBeginning: false }).catch(() => undefined);
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      io.to(`game:${event.gameId || event.matchId}`).emit(topic, event);
      if (topic === 'move.played') io.to(`game:${event.gameId}`).emit('game:state:patch', event);
    }
  }).catch(() => undefined);
  server.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => { console.error(error); process.exit(1); });
