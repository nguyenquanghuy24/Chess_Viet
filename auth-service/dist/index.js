"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const bcryptjs_1 = __importDefault(require("bcryptjs"));
const cors_1 = __importDefault(require("cors"));
const express_1 = __importDefault(require("express"));
const helmet_1 = __importDefault(require("helmet"));
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const kafkajs_1 = require("kafkajs");
const promise_1 = __importDefault(require("mysql2/promise"));
const crypto_1 = require("crypto");
const service = 'auth-service';
const port = Number(process.env.PORT || 3001);
const jwtSecret = process.env.JWT_SECRET || 'dev-secret-change-me';
const kafka = new kafkajs_1.Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const producer = kafka.producer();
const pool = promise_1.default.createPool({
    host: process.env.DB_HOST || 'mysql',
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER || 'chess_user',
    password: process.env.DB_PASSWORD || 'chess_password',
    database: process.env.DB_NAME || 'chess_auth',
    waitForConnections: true,
    connectionLimit: 10
});
function asyncHandler(fn) {
    return (req, res, next) => fn(req, res, next).catch(next);
}
async function publish(topic, key, value) {
    try {
        await producer.send({ topic, messages: [{ key, value: JSON.stringify({ ...value, service, at: new Date().toISOString() }) }] });
    }
    catch (error) {
        console.warn('kafka unavailable', error);
    }
}
function tokenFor(user) {
    return jsonwebtoken_1.default.sign({ sub: user.id, username: user.username, email: user.email, rating: user.rating }, jwtSecret, { expiresIn: '2h' });
}
function requireAuth(req, res, next) {
    const raw = req.headers.authorization?.replace('Bearer ', '');
    if (!raw) {
        res.status(401).json({ error: 'missing_token' });
        return;
    }
    try {
        const decoded = jsonwebtoken_1.default.verify(raw, jwtSecret);
        req.user = { id: String(decoded.sub), username: String(decoded.username) };
        next();
    }
    catch {
        res.status(401).json({ error: 'invalid_token' });
    }
}
async function initDb() {
    await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id VARCHAR(36) PRIMARY KEY,
      username VARCHAR(40) UNIQUE NOT NULL,
      email VARCHAR(160) UNIQUE NOT NULL,
      password_hash VARCHAR(255) NOT NULL,
      rating INT NOT NULL DEFAULT 1200,
      wins INT NOT NULL DEFAULT 0,
      losses INT NOT NULL DEFAULT 0,
      draws INT NOT NULL DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
    await pool.query(`CREATE TABLE IF NOT EXISTS friends (
    id VARCHAR(36) PRIMARY KEY,
    requester_id VARCHAR(36) NOT NULL,
    addressee_id VARCHAR(36) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_friend_pair (requester_id, addressee_id)
  )`);
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
const app = (0, express_1.default)();
app.use((0, helmet_1.default)());
app.use((0, cors_1.default)());
app.use(express_1.default.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));
app.post('/register', asyncHandler(async (req, res) => {
    const { username, email, password } = req.body;
    if (!username || !email || !password) {
        res.status(400).json({ error: 'username_email_password_required' });
        return;
    }
    const id = (0, crypto_1.randomUUID)();
    const passwordHash = await bcryptjs_1.default.hash(password, 10);
    await pool.execute('INSERT INTO users (id, username, email, password_hash) VALUES (?, ?, ?, ?)', [id, username, email, passwordHash]);
    const user = { id, username, email, rating: 1200 };
    await publish('user.registered', id, user);
    res.status(201).json({ user, token: tokenFor(user) });
}));
app.post('/login', asyncHandler(async (req, res) => {
    const { email, password } = req.body;
    const [rows] = await pool.execute('SELECT * FROM users WHERE email = ?', [email]);
    const user = rows[0];
    if (!user || !(await bcryptjs_1.default.compare(password, user.password_hash))) {
        res.status(401).json({ error: 'invalid_credentials' });
        return;
    }
    res.json({ user: { id: user.id, username: user.username, email: user.email, rating: user.rating }, token: tokenFor(user) });
}));
app.get('/me', requireAuth, asyncHandler(async (req, res) => {
    const [rows] = await pool.execute('SELECT id, username, email, rating, wins, losses, draws FROM users WHERE id = ?', [req.user.id]);
    res.json(rows[0]);
}));
app.get('/friends', requireAuth, asyncHandler(async (req, res) => {
    const [rows] = await pool.execute('SELECT * FROM friends WHERE requester_id = ? OR addressee_id = ? ORDER BY created_at DESC', [req.user.id, req.user.id]);
    res.json(rows);
}));
app.post('/friends/request', requireAuth, asyncHandler(async (req, res) => {
    const addresseeId = req.body.userId;
    if (!addresseeId || addresseeId === req.user.id) {
        res.status(400).json({ error: 'valid_userId_required' });
        return;
    }
    const id = (0, crypto_1.randomUUID)();
    await pool.execute('INSERT IGNORE INTO friends (id, requester_id, addressee_id, status) VALUES (?, ?, ?, ?)', [id, req.user.id, addresseeId, 'pending']);
    const event = { id, requesterId: req.user.id, addresseeId, userId: addresseeId };
    await publish('friend.requested', id, event);
    res.status(201).json(event);
}));
app.post('/friends/:id/respond', requireAuth, asyncHandler(async (req, res) => {
    const status = req.body.accept ? 'accepted' : 'declined';
    await pool.execute('UPDATE friends SET status = ? WHERE id = ? AND addressee_id = ?', [status, req.params.id, req.user.id]);
    const event = { id: req.params.id, userId: req.user.id, status };
    await publish('friend.responded', req.params.id, event);
    res.json(event);
}));
app.post('/friends/invite', requireAuth, asyncHandler(async (req, res) => {
    const event = { id: (0, crypto_1.randomUUID)(), fromUserId: req.user.id, toUserId: req.body.userId, gameId: req.body.gameId, userId: req.body.userId };
    await publish('friend.invited', event.id, event);
    res.status(201).json(event);
}));
app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: 'internal_error', message: err.message });
});
async function main() {
    await retry('mysql', initDb);
    await producer.connect().catch(() => undefined);
    app.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => {
    console.error(error);
    process.exit(1);
});
//# sourceMappingURL=index.js.map