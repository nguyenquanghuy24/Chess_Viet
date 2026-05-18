"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const chess_js_1 = require("chess.js");
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const kafkajs_1 = require("kafkajs");
const promise_1 = __importDefault(require("mysql2/promise"));
const redis_1 = require("redis");
const crypto_1 = require("crypto");
const service = 'game-session-service';
const port = Number(process.env.PORT || 3003);
const jwtSecret = process.env.JWT_SECRET || 'dev-secret-change-me';
const kafka = new kafkajs_1.Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: `${service}-group` });
const redis = (0, redis_1.createClient)({ url: process.env.REDIS_URL || `redis://${process.env.REDIS_HOST || 'redis'}:6379` });
const pool = promise_1.default.createPool({
    host: process.env.DB_HOST || 'mysql',
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER || 'chess_user',
    password: process.env.DB_PASSWORD || 'chess_password',
    database: process.env.DB_NAME || 'chess_games',
    waitForConnections: true
});
async function initDb() {
    await pool.query(`CREATE TABLE IF NOT EXISTS games (
    id VARCHAR(36) PRIMARY KEY, white_id VARCHAR(36), black_id VARCHAR(36), status VARCHAR(20),
    fen TEXT, pgn TEXT, turn VARCHAR(10), winner_id VARCHAR(36) NULL, result VARCHAR(20) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP NULL
  )`);
    await pool.query(`ALTER TABLE games ADD COLUMN winner_id VARCHAR(36) NULL`).catch(() => undefined);
    await pool.query(`ALTER TABLE games ADD COLUMN result VARCHAR(20) NULL`).catch(() => undefined);
    await pool.query(`CREATE TABLE IF NOT EXISTS game_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY, game_id VARCHAR(36), event_type VARCHAR(50), payload JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, INDEX(game_id)
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
async function withLock(key, ttlMs, fn) {
    const token = (0, crypto_1.randomUUID)();
    const ok = await redis.set(key, token, { NX: true, PX: ttlMs });
    if (!ok)
        throw new Error('move_conflict_locked');
    try {
        return await fn();
    }
    finally {
        if ((await redis.get(key)) === token)
            await redis.del(key);
    }
}
async function createGame(event) {
    const initialTimeMs = Number(event.initialTimeMs || 300000);
    const incrementMs = Number(event.incrementMs || 0);
    await pool.execute('INSERT IGNORE INTO games (id, white_id, black_id, status, fen, turn) VALUES (?, ?, ?, ?, ?, ?)', [
        event.matchId, event.whiteId, event.blackId, 'active', new chess_js_1.Chess().fen(), 'white'
    ]);
    await pool.execute('INSERT INTO game_events (game_id, event_type, payload) VALUES (?, ?, ?)', [event.matchId, 'game.created', JSON.stringify(event)]);
    await redis.hSet(`game:${event.matchId}`, {
        id: event.matchId,
        whiteId: event.whiteId,
        blackId: event.blackId,
        fen: new chess_js_1.Chess().fen(),
        pgn: '',
        turn: 'white',
        moveNumber: '0',
        status: 'active',
        whiteTimeMs: String(initialTimeMs),
        blackTimeMs: String(initialTimeMs),
        incrementMs: String(incrementMs),
        lastTickAt: String(Date.now())
    });
    await publish('game.started', event.matchId, event);
}
function colorOf(playerId, state) {
    if (playerId === state.whiteId)
        return 'white';
    if (playerId === state.blackId)
        return 'black';
    if (playerId === 'ai-bot')
        return state.turn;
    return null;
}
async function finishGame(gameId, state, reason, winnerId, result, patch = {}) {
    if (state.status === 'finished')
        return { gameId, winnerId, loserId: winnerId ? (winnerId === state.whiteId ? state.blackId : state.whiteId) : null, result, reason, fen: patch.fen || state.fen, pgn: patch.pgn || state.pgn || '' };
    const loserId = winnerId ? (winnerId === state.whiteId ? state.blackId : state.whiteId) : null;
    const finished = { gameId, winnerId, loserId, result, reason, fen: patch.fen || state.fen, pgn: patch.pgn || state.pgn || '' };
    await redis.hSet(`game:${gameId}`, { ...patch, status: 'finished', winnerId: winnerId || '', result, finishReason: reason });
    await pool.execute('UPDATE games SET fen = ?, pgn = ?, status = ?, winner_id = ?, result = ?, finished_at = CURRENT_TIMESTAMP WHERE id = ?', [
        finished.fen,
        finished.pgn,
        'finished',
        winnerId,
        result,
        gameId
    ]);
    await pool.execute('INSERT INTO game_events (game_id, event_type, payload) VALUES (?, ?, ?)', [gameId, 'game.finished', JSON.stringify(finished)]);
    await publish('game.finished', gameId, finished);
    return finished;
}
async function tickTimers() {
    const now = Date.now();
    for await (const key of redis.scanIterator({ MATCH: 'game:*', COUNT: 50 })) {
        const gameKey = String(key);
        const gameId = gameKey.replace('game:', '');
        const state = await redis.hGetAll(gameKey);
        if (!state.id || state.status !== 'active')
            continue;
        const lastTickAt = Number(state.lastTickAt || now);
        const elapsed = Math.max(0, now - lastTickAt);
        const turn = state.turn;
        const whiteTimeMs = Math.max(0, Number(state.whiteTimeMs || 300000) - (turn === 'white' ? elapsed : 0));
        const blackTimeMs = Math.max(0, Number(state.blackTimeMs || 300000) - (turn === 'black' ? elapsed : 0));
        await redis.hSet(gameKey, { whiteTimeMs: String(whiteTimeMs), blackTimeMs: String(blackTimeMs), lastTickAt: String(now) });
        await publish('timer.tick', gameId, { gameId, whiteTimeMs, blackTimeMs, turn });
        if (whiteTimeMs <= 0 || blackTimeMs <= 0) {
            const winnerId = whiteTimeMs <= 0 ? state.blackId : state.whiteId;
            const result = whiteTimeMs <= 0 ? '0-1' : '1-0';
            await finishGame(gameId, { ...state, whiteTimeMs: String(whiteTimeMs), blackTimeMs: String(blackTimeMs) }, 'timeout', winnerId, result, {
                whiteTimeMs: String(whiteTimeMs),
                blackTimeMs: String(blackTimeMs),
                lastTickAt: String(now)
            }).catch(console.warn);
        }
    }
}
function applyClock(state, movingColor) {
    const now = Date.now();
    const lastTickAt = Number(state.lastTickAt || now);
    const elapsed = Math.max(0, now - lastTickAt);
    const whiteTimeMs = Number(state.whiteTimeMs || 300000);
    const blackTimeMs = Number(state.blackTimeMs || 300000);
    const incrementMs = Number(state.incrementMs || 0);
    const nextWhite = movingColor === 'white' ? whiteTimeMs - elapsed + incrementMs : whiteTimeMs;
    const nextBlack = movingColor === 'black' ? blackTimeMs - elapsed + incrementMs : blackTimeMs;
    return { whiteTimeMs: Math.max(0, nextWhite), blackTimeMs: Math.max(0, nextBlack), now };
}
async function applyMove(gameId, payload) {
    return withLock(`lock:game:${gameId}`, 3000, async () => {
        const state = await redis.hGetAll(`game:${gameId}`);
        if (!state.id)
            throw new Error('game_not_found');
        if (state.status === 'finished')
            throw new Error('game_already_finished');
        const playerId = payload.playerId || payload.userId;
        const expectedColor = state.turn;
        const playerColor = colorOf(playerId, state);
        if (!playerColor)
            throw new Error('player_not_in_game');
        if (playerColor !== expectedColor)
            throw new Error('not_your_turn');
        const clock = applyClock(state, expectedColor);
        if ((expectedColor === 'white' && clock.whiteTimeMs <= 0) || (expectedColor === 'black' && clock.blackTimeMs <= 0)) {
            const winnerId = expectedColor === 'white' ? state.blackId : state.whiteId;
            const result = expectedColor === 'white' ? '0-1' : '1-0';
            const finished = await finishGame(gameId, state, 'timeout', winnerId, result, {
                whiteTimeMs: String(clock.whiteTimeMs),
                blackTimeMs: String(clock.blackTimeMs),
                lastTickAt: String(clock.now)
            });
            return { finished };
        }
        const chess = new chess_js_1.Chess(state.fen && state.fen !== 'startpos' ? state.fen : undefined);
        const legalMove = chess.move({ from: payload.from, to: payload.to, promotion: payload.promotion || 'q' });
        if (!legalMove)
            throw new Error('illegal_move');
        const moveNumber = Number(state.moveNumber || 0) + 1;
        const nextTurn = expectedColor === 'white' ? 'black' : 'white';
        const move = {
            id: payload.id || (0, crypto_1.randomUUID)(),
            gameId,
            moveNumber,
            playerId,
            from: payload.from,
            to: payload.to,
            san: legalMove.san,
            fen: chess.fen(),
            pgn: chess.pgn(),
            check: chess.isCheck(),
            checkmate: chess.isCheckmate()
        };
        const gameEnded = chess.isGameOver();
        const winnerId = chess.isCheckmate() ? playerId : null;
        const loserId = winnerId ? (winnerId === state.whiteId ? state.blackId : state.whiteId) : null;
        const result = chess.isCheckmate()
            ? (winnerId === state.whiteId ? '1-0' : '0-1')
            : gameEnded
                ? '1/2-1/2'
                : null;
        const finishReason = chess.isCheckmate()
            ? 'checkmate'
            : chess.isStalemate()
                ? 'stalemate'
                : chess.isDraw()
                    ? 'draw'
                    : chess.isGameOver()
                        ? 'game_over'
                        : null;
        await redis.hSet(`game:${gameId}`, {
            turn: nextTurn,
            moveNumber: String(moveNumber),
            lastMove: JSON.stringify(move),
            fen: chess.fen(),
            pgn: chess.pgn(),
            status: gameEnded ? 'finished' : 'active',
            whiteTimeMs: String(clock.whiteTimeMs),
            blackTimeMs: String(clock.blackTimeMs),
            lastTickAt: String(clock.now),
            ...(winnerId ? { winnerId } : {}),
            ...(result ? { result } : {}),
            ...(finishReason ? { finishReason } : {})
        });
        await pool.execute('INSERT INTO game_events (game_id, event_type, payload) VALUES (?, ?, ?)', [gameId, 'move.played', JSON.stringify(move)]);
        await pool.execute('UPDATE games SET fen = ?, pgn = ?, turn = ?, status = ?, winner_id = ?, result = ?, finished_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE finished_at END WHERE id = ?', [
            chess.fen(),
            chess.pgn(),
            nextTurn,
            gameEnded ? 'finished' : 'active',
            winnerId,
            result,
            gameEnded,
            gameId
        ]);
        await publish('move.played', gameId, { ...move, nextTurn, status: gameEnded ? 'finished' : 'active', whiteTimeMs: clock.whiteTimeMs, blackTimeMs: clock.blackTimeMs });
        if (gameEnded) {
            const finished = { gameId, winnerId, loserId, result, reason: finishReason, fen: chess.fen(), pgn: chess.pgn(), lastMove: move };
            await pool.execute('INSERT INTO game_events (game_id, event_type, payload) VALUES (?, ?, ?)', [gameId, 'game.finished', JSON.stringify(finished)]);
            await publish('game.finished', gameId, finished);
            return { move, nextTurn, finished };
        }
        return { move, nextTurn };
    });
}
const app = (0, express_1.default)();
app.use(express_1.default.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));
app.post('/games', requireAuth, async (req, res, next) => {
    try {
        const event = {
            matchId: (0, crypto_1.randomUUID)(),
            whiteId: req.body.whiteId || req.user.id,
            blackId: req.body.blackId,
            timeControl: req.body.timeControl || 'rapid',
            initialTimeMs: req.body.initialTimeMs || 300000,
            incrementMs: req.body.incrementMs || 2000
        };
        await createGame(event);
        res.status(201).json(event);
    }
    catch (error) {
        next(error);
    }
});
app.get('/games', requireAuth, async (req, res) => {
    const [rows] = await pool.execute('SELECT id, white_id, black_id, status, result, winner_id, created_at, finished_at FROM games WHERE white_id = ? OR black_id = ? ORDER BY created_at DESC LIMIT 30', [req.user.id, req.user.id]);
    res.json(rows);
});
app.get('/games/:id', requireAuth, async (req, res) => {
    const state = await redis.hGetAll(`game:${req.params.id}`);
    if (Object.keys(state).length)
        return res.json(state);
    const [rows] = await pool.execute('SELECT * FROM games WHERE id = ?', [req.params.id]);
    res.json(rows[0] || null);
});
app.post('/games/:id/move', requireAuth, async (req, res, next) => {
    try {
        const gameId = req.params.id;
        const result = await applyMove(gameId, { ...req.body, playerId: req.user.id });
        res.json(result);
    }
    catch (error) {
        next(error);
    }
});
app.post('/games/:id/resign', requireAuth, async (req, res, next) => {
    try {
        const gameId = req.params.id;
        const result = await withLock(`lock:game:${gameId}`, 3000, async () => {
            const state = await redis.hGetAll(`game:${gameId}`);
            if (!state.id)
                throw new Error('game_not_found');
            if (state.status === 'finished')
                throw new Error('game_already_finished');
            const loserColor = colorOf(req.user.id, state);
            if (!loserColor)
                throw new Error('player_not_in_game');
            const winnerId = loserColor === 'white' ? state.blackId : state.whiteId;
            const resultText = loserColor === 'white' ? '0-1' : '1-0';
            return finishGame(gameId, state, 'resign', winnerId, resultText);
        });
        res.json(result);
    }
    catch (error) {
        next(error);
    }
});
app.post('/games/:id/draw', requireAuth, async (req, res, next) => {
    try {
        const gameId = req.params.id;
        const result = await withLock(`lock:game:${gameId}`, 3000, async () => {
            const state = await redis.hGetAll(`game:${gameId}`);
            if (!state.id)
                throw new Error('game_not_found');
            if (!colorOf(req.user.id, state))
                throw new Error('player_not_in_game');
            if (req.body.accept === true)
                return finishGame(gameId, state, 'draw_agreement', null, '1/2-1/2');
            await redis.set(`draw:${gameId}`, req.user.id, { EX: 60 });
            await publish('draw.offered', gameId, { gameId, fromUserId: req.user.id });
            return { offered: true };
        });
        res.json(result);
    }
    catch (error) {
        next(error);
    }
});
app.use((err, _req, res, _next) => res.status(409).json({ error: err.message }));
async function main() {
    await Promise.all([retry('redis', () => redis.connect()), producer.connect().catch(() => undefined), consumer.connect().catch(() => undefined), retry('mysql', initDb)]);
    await consumer.subscribe({ topic: 'match.created', fromBeginning: true }).catch(() => undefined);
    await consumer.subscribe({ topic: 'move.validated', fromBeginning: false }).catch(() => undefined);
    await consumer.run({ eachMessage: async ({ topic, message }) => {
            if (!message.value)
                return;
            const event = JSON.parse(message.value.toString());
            if (topic === 'match.created')
                await createGame(event);
            if (topic === 'move.validated')
                await applyMove(event.gameId, event).catch(console.warn);
        } }).catch(() => undefined);
    setInterval(() => tickTimers().catch(console.warn), 1000);
    app.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => { console.error(error); process.exit(1); });
