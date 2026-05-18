import express from 'express';
import { Chess } from 'chess.js';
import { Kafka } from 'kafkajs';

const service = 'move-validation-service';
const port = Number(process.env.PORT || 3004);
const kafka = new Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: `${service}-group` });
const boards = new Map<string, Chess>();

const app = express();
app.use(express.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\n`));
app.post('/validate', (req, res) => {
  const chess = new Chess(req.body.fen || undefined);
  const move = chess.move({ from: req.body.from, to: req.body.to, promotion: req.body.promotion || 'q' });
  res.json({
    legal: !!move,
    fen: move ? chess.fen() : req.body.fen,
    san: move?.san,
    check: move ? chess.isCheck() : false,
    checkmate: move ? chess.isCheckmate() : false,
    draw: move ? chess.isDraw() : false,
    stalemate: move ? chess.isStalemate() : false,
    gameOver: move ? chess.isGameOver() : false
  });
});

async function publish(topic: string, key: string, value: object) {
  await producer.send({ topic, messages: [{ key, value: JSON.stringify({ ...value, service, at: new Date().toISOString() }) }] }).catch(console.warn);
}

async function main() {
  await Promise.all([producer.connect().catch(() => undefined), consumer.connect().catch(() => undefined)]);
  await consumer.subscribe({ topic: 'move.requested', fromBeginning: false }).catch(() => undefined);
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = JSON.parse(message.value.toString());
      const chess = boards.get(event.gameId) || new Chess();
      const move = chess.move({ from: event.from, to: event.to, promotion: event.promotion || 'q' });
      if (!move) {
        await publish('move.rejected', event.gameId, { ...event, reason: 'illegal_move' });
        return;
      }
      boards.set(event.gameId, chess);
      await publish('move.validated', event.gameId, {
        ...event,
        san: move.san,
        fen: chess.fen(),
        check: chess.isCheck(),
        checkmate: chess.isCheckmate(),
        draw: chess.isDraw(),
        stalemate: chess.isStalemate(),
        insufficientMaterial: chess.isInsufficientMaterial(),
        threefoldRepetition: chess.isThreefoldRepetition(),
        gameOver: chess.isGameOver()
      });
    }
  }).catch(() => undefined);
  app.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => { console.error(error); process.exit(1); });
