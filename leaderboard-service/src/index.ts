import express from 'express';
import { Kafka } from 'kafkajs';
import mysql from 'mysql2/promise';
import { createClient } from 'redis';

const service = 'leaderboard-service';
const port = Number(process.env.PORT || 3006);
const kafka = new Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const consumer = kafka.consumer({ groupId: `${service}-group` });
const redis = createClient({ url: process.env.REDIS_URL || `redis://${process.env.REDIS_HOST || 'redis'}:6379` });
const pool = mysql.createPool({
  host: process.env.DB_HOST || 'mysql',
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || 'chess_user',
  password: process.env.DB_PASSWORD || 'chess_password',
  database: process.env.DB_NAME || 'chess_leaderboard',
  waitForConnections: true
});

async function initDb() {
  await pool.query(`CREATE TABLE IF NOT EXISTS leaderboard (
    user_id VARCHAR(36) PRIMARY KEY, username VARCHAR(40), rating INT DEFAULT 1200,
    wins INT DEFAULT 0, losses INT DEFAULT 0, draws INT DEFAULT 0, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  )`);
}

async function retry<T>(label: string, fn: () => Promise<T>, attempts = 30): Promise<T> {
  let lastError: unknown;
  for (let i = 1; i <= attempts; i += 1) {
    try { return await fn(); } catch (error) {
      lastError = error;
      console.log(`${label} not ready (${i}/${attempts})`);
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
  throw lastError;
}

const app = express();
app.use(express.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));
app.get('/', async (req, res) => {
  const limit = Number(req.query.limit || 20);
  const rows = await redis.zRangeWithScores('leaderboard:rating', 0, limit - 1, { REV: true });
  res.json(rows.map((row, index) => ({ rank: index + 1, userId: row.value, rating: row.score })));
});
app.post('/seed', async (req, res) => {
  await redis.zAdd('leaderboard:rating', [{ value: req.body.userId, score: Number(req.body.rating || 1200) }]);
  await pool.execute('INSERT INTO leaderboard (user_id, username, rating) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE rating=VALUES(rating)', [req.body.userId, req.body.username || req.body.userId, req.body.rating || 1200]);
  res.status(201).json({ ok: true });
});

async function main() {
  await Promise.all([retry('redis', () => redis.connect()), consumer.connect().catch(() => undefined), retry('mysql', initDb)]);
  await consumer.subscribe({ topic: 'game.finished', fromBeginning: false }).catch(() => undefined);
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      if (event.winnerId) await redis.zIncrBy('leaderboard:rating', 16, event.winnerId);
      if (event.loserId) await redis.zIncrBy('leaderboard:rating', -16, event.loserId);
    }
  }).catch(() => undefined);
  app.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => { console.error(error); process.exit(1); });
