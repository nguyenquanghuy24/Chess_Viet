"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const kafkajs_1 = require("kafkajs");
const promise_1 = __importDefault(require("mysql2/promise"));
const redis_1 = require("redis");
const crypto_1 = require("crypto");
const service = 'matchmaking-service';
const port = Number(process.env.PORT || 3002);
const jwtSecret = process.env.JWT_SECRET || 'dev-secret-change-me';
const kafka = new kafkajs_1.Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const producer = kafka.producer();
const redis = (0, redis_1.createClient)({ url: process.env.REDIS_URL || `redis://${process.env.REDIS_HOST || 'redis'}:6379` });
const pool = promise_1.default.createPool({
    host: process.env.DB_HOST || 'mysql',
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER || 'chess_user',
    password: process.env.DB_PASSWORD || 'chess_password',
    database: process.env.DB_NAME || 'chess_matchmaking',
    waitForConnections: true
});
async function initDb() {
    await pool.query(`CREATE TABLE IF NOT EXISTS queue_events (
    id VARCHAR(36) PRIMARY KEY, user_id VARCHAR(36), rating INT, time_control VARCHAR(20),
    event_type VARCHAR(40), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);
}
function requireAuth(req, res, next) {
    if (process.env.AUTH_REQUIRED === 'false')
        return next();
    const raw = req.headers.authorization?.replace('Bearer ', '');
    if (!raw) {
        res.status(401).json({ error: 'missing_token' });
        return;
    }
    try {
        const decoded = jsonwebtoken_1.default.verify(raw, jwtSecret);
        req.user = { id: String(decoded.sub), username: decoded.username ? String(decoded.username) : undefined };
        next();
    }
    catch {
        res.status(401).json({ error: 'invalid_token' });
    }
}
async function retry(label, fn, attempts = 30) {
    let lastError;
    for (let i = 1; i <= attempts; i += 1) {
        try {
            return await fn();
        }
        catch (error) {
            lastError = error;
            console.log(`${label} not ready (${i}/${attempts})`);
            await new Promise((resolve) => setTimeout(resolve, 2000));
        }
    }
    throw lastError;
}
async function publish(topic, key, value) {
    await producer.send({ topic, messages: [{ key, value: JSON.stringify({ ...value, service, at: new Date().toISOString() }) }] }).catch(console.warn);
}
async function matchCandidate(userId, rating, timeControl) {
    const key = `queue:${timeControl}`;
    const min = rating - 150;
    const max = rating + 150;
    const candidates = await redis.zRangeByScoreWithScores(key, min, max, { LIMIT: { offset: 0, count: 10 } });
    const opponent = candidates.find((c) => c.value !== userId);
    if (!opponent)
        return null;
    await redis.zRem(key, opponent.value);
    await redis.zRem(key, userId);
    return { opponentId: opponent.value, opponentRating: opponent.score };
}
const app = (0, express_1.default)();
app.use(express_1.default.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));
app.post('/queue', requireAuth, async (req, res, next) => {
    try {
        const userId = req.body.userId || req.user.id;
        const { rating = 1200, timeControl = 'rapid' } = req.body;
        const key = `queue:${timeControl}`;
        await redis.zAdd(key, [{ score: Number(rating), value: userId }]);
        await pool.execute('INSERT INTO queue_events (id, user_id, rating, time_control, event_type) VALUES (?, ?, ?, ?, ?)', [(0, crypto_1.randomUUID)(), userId, rating, timeControl, 'queued']);
        const match = await matchCandidate(userId, Number(rating), timeControl);
        if (!match)
            return res.status(202).json({ status: 'queued', userId, rating, timeControl });
        const matchId = (0, crypto_1.randomUUID)();
        const event = { matchId, whiteId: userId, blackId: match.opponentId, timeControl, initialTimeMs: 300000, incrementMs: 2000 };
        await publish('match.created', matchId, event);
        res.status(201).json({ status: 'matched', ...event });
    }
    catch (error) {
        next(error);
    }
});
app.delete('/queue/:userId', async (req, res) => {
    const pattern = 'queue:*';
    for await (const key of redis.scanIterator({ MATCH: pattern }))
        await redis.zRem(String(key), req.params.userId);
    res.json({ status: 'cancelled', userId: req.params.userId });
});
app.use((err, _req, res, _next) => res.status(500).json({ error: err.message }));
async function main() {
    await Promise.all([retry('redis', () => redis.connect()), producer.connect().catch(() => undefined), retry('mysql', initDb)]);
    app.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => { console.error(error); process.exit(1); });
