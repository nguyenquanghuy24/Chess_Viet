"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const chess_js_1 = require("chess.js");
const kafkajs_1 = require("kafkajs");
const service = 'ai-bot-service';
const port = Number(process.env.PORT || 3011);
const kafka = new kafkajs_1.Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: `${service}-group` });
const app = (0, express_1.default)();
const botGames = new Map();
app.use(express_1.default.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));
function normalizeFen(fen) {
    return !fen || fen === 'startpos' ? undefined : fen;
}
function safeChess(fen) {
    try {
        return new chess_js_1.Chess(normalizeFen(fen));
    }
    catch {
        return new chess_js_1.Chess();
    }
}
function chooseSimpleMove(chess) {
    const moves = chess.moves({ verbose: true });
    if (!moves.length)
        return null;
    return moves[Math.floor(Math.random() * moves.length)];
}
async function publishMove(gameId, fen, config) {
    const chess = safeChess(fen);
    if (chess.isGameOver())
        return { gameOver: true, fen: chess.fen() };
    const expectedTurn = config.botColor === 'white' ? 'w' : 'b';
    if (chess.turn() !== expectedTurn)
        return { waiting: true, fen: chess.fen() };
    const move = chooseSimpleMove(chess);
    if (!move)
        return { gameOver: true, fen: chess.fen() };
    const payload = {
        gameId,
        playerId: config.playerId,
        from: move.from,
        to: move.to,
        promotion: move.promotion,
        san: move.san,
        fen: chess.fen()
    };
    await producer.send({ topic: 'move.requested', messages: [{ key: gameId, value: JSON.stringify(payload) }] }).catch(console.warn);
    return { ...payload, mode: 'simple', botColor: config.botColor };
}
app.get('/levels', (_req, res) => res.json([{ id: 'simple', label: 'Simple random legal move' }]));
app.post('/games/:gameId/configure', async (req, res) => {
    const config = {
        botColor: req.body.botColor === 'white' ? 'white' : 'black',
        playerId: req.body.playerId || 'ai-bot'
    };
    botGames.set(req.params.gameId, config);
    const firstMove = await publishMove(req.params.gameId, req.body.fen, config);
    res.status(201).json({ gameId: req.params.gameId, ...config, mode: 'simple', firstMove });
});
app.post('/move', async (req, res) => {
    const config = {
        botColor: req.body.color === 'white' ? 'white' : req.body.color === 'black' ? 'black' : safeChess(req.body.fen).turn() === 'w' ? 'white' : 'black',
        playerId: req.body.playerId || 'ai-bot'
    };
    res.json(await publishMove(req.body.gameId, req.body.fen, config));
});
async function main() {
    await Promise.all([producer.connect().catch(() => undefined), consumer.connect().catch(() => undefined)]);
    await consumer.subscribe({ topic: 'game.started', fromBeginning: false }).catch(() => undefined);
    await consumer.subscribe({ topic: 'move.played', fromBeginning: false }).catch(() => undefined);
    await consumer.subscribe({ topic: 'game.finished', fromBeginning: false }).catch(() => undefined);
    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            if (!message.value)
                return;
            const event = JSON.parse(message.value.toString());
            const gameId = event.gameId || event.matchId;
            if (!gameId)
                return;
            if (topic === 'game.finished') {
                botGames.delete(gameId);
                return;
            }
            const config = botGames.get(gameId);
            if (config)
                await publishMove(gameId, event.fen, config);
        }
    }).catch(() => undefined);
    app.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => { console.error(error); process.exit(1); });
