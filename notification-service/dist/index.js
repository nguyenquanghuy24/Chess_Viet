"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const kafkajs_1 = require("kafkajs");
const service = 'notification-service';
const port = Number(process.env.PORT || 3007);
const kafka = new kafkajs_1.Kafka({ clientId: service, brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(',') });
const consumer = kafka.consumer({ groupId: `${service}-group` });
const notifications = [];
const app = (0, express_1.default)();
app.use(express_1.default.json());
app.get('/health', (_req, res) => res.json({ service, status: 'ok' }));
app.get('/metrics', (_req, res) => res.type('text/plain').send(`service_up{service="${service}"} 1\nnotifications_total ${notifications.length}\n`));
app.get('/:userId', (req, res) => res.json(notifications.filter((n) => n.userId === req.params.userId || n.toUserId === req.params.userId).slice(-50)));
async function main() {
    await consumer.connect().catch(() => undefined);
    for (const topic of ['match.created', 'game.started', 'game.finished', 'move.rejected', 'draw.offered', 'friend.requested', 'friend.responded', 'friend.invited']) {
        await consumer.subscribe({ topic, fromBeginning: false }).catch(() => undefined);
    }
    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            if (!message.value)
                return;
            const event = JSON.parse(message.value.toString());
            notifications.push({ id: `${Date.now()}-${notifications.length}`, topic, userId: event.userId || event.toUserId || event.whiteId || event.blackId, toUserId: event.toUserId, event, read: false });
            console.log('notification', topic, event);
        }
    }).catch(() => undefined);
    app.listen(port, () => console.log(`${service} listening on ${port}`));
}
main().catch((error) => { console.error(error); process.exit(1); });
