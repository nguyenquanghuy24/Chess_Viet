import express from 'express';
import { Chess } from 'chess.js';
import { Kafka } from 'kafkajs';

const service = 'ai-bot-service';
const port = Number(process.env.PORT || 3011);
const kafka = new Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: `${service}-group` });
const app = express();
type Difficulty = 'beginner' | 'easy' | 'medium' | 'hard' | 'expert';
type BotConfig = { botColor: 'white' | 'black'; difficulty: Difficulty; playerId: string };
type ScoredMove = ReturnType<Chess['moves']>[number] & { score?: number };

const botGames = new Map<string, BotConfig>();
const pieceValue: Record<string, number> = { p: 100, n: 320, b: 330, r: 500, q: 900, k: 20000 };

app.use(express.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));

function normalizeFen(fen?: string) {
  return !fen || fen === 'startpos' ? undefined : fen;
}

function safeChess(fen?: string) {
  try {
    return new Chess(normalizeFen(fen));
  } catch {
    return new Chess();
  }
}

function evaluate(chess: Chess) {
  if (chess.isCheckmate()) return chess.turn() === 'w' ? -100000 : 100000;
  if (chess.isDraw() || chess.isStalemate()) return 0;

  let score = 0;
  for (const row of chess.board()) {
    for (const piece of row) {
      if (!piece) continue;
      const value = pieceValue[piece.type] || 0;
      score += piece.color === 'w' ? value : -value;
    }
  }
  if (chess.isCheck()) score += chess.turn() === 'w' ? -25 : 25;
  return score;
}

function minimax(chess: Chess, depth: number, alpha: number, beta: number): number {
  if (depth === 0 || chess.isGameOver()) return evaluate(chess);
  const maximizing = chess.turn() === 'w';
  const moves = chess.moves({ verbose: true });

  if (maximizing) {
    let best = -Infinity;
    for (const move of moves) {
      chess.move(move);
      best = Math.max(best, minimax(chess, depth - 1, alpha, beta));
      chess.undo();
      alpha = Math.max(alpha, best);
      if (beta <= alpha) break;
    }
    return best;
  }

  let best = Infinity;
  for (const move of moves) {
    chess.move(move);
    best = Math.min(best, minimax(chess, depth - 1, alpha, beta));
    chess.undo();
    beta = Math.min(beta, best);
    if (beta <= alpha) break;
  }
  return best;
}

function tacticalScore(move: ScoredMove) {
  let score = Math.random() * 5;
  if (move.captured) score += pieceValue[move.captured] || 0;
  if (move.san.includes('+')) score += 30;
  if (move.san.includes('#')) score += 100000;
  if (move.promotion) score += pieceValue[move.promotion] || 0;
  return score;
}

function chooseMove(chess: Chess, difficulty: Difficulty) {
  const moves = chess.moves({ verbose: true }) as ScoredMove[];
  if (!moves.length) return null;
  if (difficulty === 'beginner') return moves[Math.floor(Math.random() * moves.length)];

  if (difficulty === 'easy') {
    const sorted = moves.map((move) => ({ ...move, score: tacticalScore(move) })).sort((a, b) => (b.score || 0) - (a.score || 0));
    return sorted[Math.floor(Math.random() * Math.min(3, sorted.length))];
  }

  const depthByDifficulty: Record<Difficulty, number> = { beginner: 0, easy: 0, medium: 1, hard: 2, expert: 3 };
  const maximizing = chess.turn() === 'w';
  let bestMove = moves[0];
  let bestScore = maximizing ? -Infinity : Infinity;

  for (const move of moves) {
    chess.move(move);
    const score = minimax(chess, depthByDifficulty[difficulty], -Infinity, Infinity);
    chess.undo();
    if ((maximizing && score > bestScore) || (!maximizing && score < bestScore)) {
      bestScore = score;
      bestMove = move;
    }
  }
  return bestMove;
}

async function publishMove(gameId: string, fen: string | undefined, config: BotConfig) {
  const chess = safeChess(fen);
  if (chess.isGameOver()) return { gameOver: true, fen: chess.fen() };

  const expectedTurn = config.botColor === 'white' ? 'w' : 'b';
  if (chess.turn() !== expectedTurn) return { waiting: true, fen: chess.fen() };

  const move = chooseMove(chess, config.difficulty);
  if (!move) return { gameOver: true, fen: chess.fen() };

  chess.move(move);
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
  return { ...payload, difficulty: config.difficulty, botColor: config.botColor };
}

app.get('/levels', (_req, res) => res.json([
  { id: 'beginner', label: 'Beginner', depth: 0 },
  { id: 'easy', label: 'Easy', depth: 0 },
  { id: 'medium', label: 'Medium', depth: 1 },
  { id: 'hard', label: 'Hard', depth: 2 },
  { id: 'expert', label: 'Expert', depth: 3 }
]));

app.post('/games/:gameId/configure', async (req, res) => {
  const config: BotConfig = {
    botColor: req.body.botColor === 'white' ? 'white' : 'black',
    difficulty: req.body.difficulty || 'medium',
    playerId: req.body.playerId || 'ai-bot'
  };
  botGames.set(req.params.gameId, config);
  const firstMove = await publishMove(req.params.gameId, req.body.fen, config);
  res.status(201).json({ gameId: req.params.gameId, ...config, firstMove });
});

app.post('/move', async (req, res) => {
  const config: BotConfig = {
    botColor: req.body.color === 'white' ? 'white' : req.body.color === 'black' ? 'black' : safeChess(req.body.fen).turn() === 'w' ? 'white' : 'black',
    difficulty: req.body.difficulty || 'medium',
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
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      const gameId = event.gameId || event.matchId;
      if (!gameId) return;
      if (topic === 'game.finished') {
        botGames.delete(gameId);
        return;
      }
      const config = botGames.get(gameId);
      if (config) await publishMove(gameId, event.fen, config);
    }
  }).catch(() => undefined);
  app.listen(port, () => console.log(`${service} listening on ${port}`));
}

main().catch((error) => { console.error(error); process.exit(1); });
