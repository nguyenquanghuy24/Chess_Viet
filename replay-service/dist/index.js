"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const promise_1 = __importDefault(require("mysql2/promise"));
const service = 'replay-service';
const port = Number(process.env.PORT || 3010);
const jwtSecret = process.env.JWT_SECRET || 'dev-secret-change-me';
const pool = promise_1.default.createPool({ host: process.env.DB_HOST || 'mysql', user: process.env.DB_USER || 'chess_user', password: process.env.DB_PASSWORD || 'chess_password', database: process.env.DB_NAME || 'chess_games' });
const app = (0, express_1.default)();
function requireAuth(req, res, next) {
    if (process.env.AUTH_REQUIRED === 'false')
        return next();
    const raw = req.headers.authorization?.replace('Bearer ', '');
    if (!raw) {
        res.status(401).json({ error: 'missing_token' });
        return;
    }
    try {
        jsonwebtoken_1.default.verify(raw, jwtSecret);
        next();
    }
    catch {
        res.status(401).json({ error: 'invalid_token' });
    }
}
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));
app.get('/games/:gameId/replay', requireAuth, async (req, res) => {
    const [rows] = await pool.execute('SELECT event_type, payload, created_at FROM game_events WHERE game_id = ? ORDER BY id ASC', [req.params.gameId]);
    res.json({ gameId: req.params.gameId, events: rows });
});
app.listen(port, () => console.log(`${service} listening on ${port}`));
