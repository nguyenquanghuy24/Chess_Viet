# Chess Viet Distributed Realtime Chess

Demo microservices cho website chơi cờ vua realtime, dùng Node.js/Express, React/Vite, Socket.IO, MySQL, Redis, Kafka, Nginx, Prometheus và Grafana.

## Kiến trúc

```text
Browser React/Vite
  | HTTP + Socket.IO
Nginx API Gateway
  |-- /api/auth          -> auth-service
  |-- /api/matchmaking   -> matchmaking-service
  |-- /api/games         -> game-session-service
  |-- /api/validate      -> move-validation-service
  |-- /api/leaderboard   -> leaderboard-service
  |-- /api/chat          -> chat-service
  |-- /api/replay        -> replay-service
  |-- /api/ai            -> ai-bot-service
  |-- /socket.io         -> websocket-service

Redis: queue, presence, game state cache, distributed lock
Kafka: match.created, move.requested, move.validated, move.played, chat.message.sent
MySQL: users, queue_events, game event store, leaderboard, chat messages
Prometheus/Grafana: scrape /metrics endpoints
```

## Service ports

| Service | Port | Vai trò |
| --- | ---: | --- |
| frontend | 3000 | UI dark theme giống chess server |
| api-gateway | 80 | Reverse proxy và load balancing entrypoint |
| auth-service | 3001 | Register/login/JWT |
| matchmaking-service | 3002 | Queue theo Elo bằng Redis sorted set |
| game-session-service | 3003 | Game state, event sourcing, Redis lock |
| move-validation-service | 3004 | Validate nước đi bằng chess.js |
| websocket-service | 3005 | Socket.IO sync, reconnect, spectator presence |
| leaderboard-service | 3006 | Redis live leaderboard |
| notification-service | 3007 | Kafka consumer notification log |
| chat-service | 3009 | Chat persistence + Kafka event |
| replay-service | 3010 | Replay từ game_events |
| ai-bot-service | 3011 | Bot chọn nước hợp lệ ngẫu nhiên |
| prometheus | 9090 | Metrics |
| grafana | 3008 | Dashboard, admin/admin |

## Chạy local

```bash
docker compose up --build
```

Mở:

- App: `http://localhost`
- Frontend direct: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3008` (`admin` / `admin`)

## API mẫu

```bash
curl -X POST http://localhost/api/auth/register \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"huy\",\"email\":\"huy@example.com\",\"password\":\"123456\"}"

curl -X POST http://localhost/api/matchmaking/queue \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"u1\",\"rating\":1200,\"timeControl\":\"rapid\"}"

curl -X POST http://localhost/api/games \
  -H "Content-Type: application/json" \
  -d "{\"whiteId\":\"u1\",\"blackId\":\"u2\"}"

curl -X POST http://localhost/api/ai/games/{gameId}/configure \
  -H "Content-Type: application/json" \
  -d "{\"botColor\":\"black\",\"difficulty\":\"hard\",\"fen\":\"startpos\"}"
```

## WebSocket events

Client kết nối Socket.IO qua `/socket.io`.

| Event | Direction | Payload |
| --- | --- | --- |
| `game:join` | client -> server | `{ gameId, userId, spectator?: boolean }` |
| `game:state` | server -> client | Redis game state |
| `game:move` | client -> server | `{ gameId, userId, from, to, san?, fen? }` |
| `move.played` | server -> client | committed move from Kafka |
| `game:state:patch` | server -> client | realtime state patch |
| `game.finished` | server -> client | `{ gameId, winnerId, result, reason, fen, lastMove }` |
| `chat:message` | both | `{ gameId, roomId, userId, body }` |
| `presence:changed` | server -> client | spectator/player count |

## AI bot

`ai-bot-service` há»— trá»£ cÃ¡c má»©c `beginner`, `easy`, `medium`, `hard`, `expert`.
Bot cÃ³ thá»ƒ Ä‘Æ°á»£c gáº¯n vÃ o má»™t váº¡n báº±ng `POST /api/ai/games/:gameId/configure`, chá»n mÃ u qua `botColor`.
Khi Ä‘áº¿n lÆ°á»£t bot, service tá»± publish `move.requested` lÃªn Kafka Ä‘á»ƒ Ä‘i tiáº¿p cho tá»›i khi váº¡n káº¿t thÃºc.
`move-validation-service` phÃ¡t hiá»‡n `checkmate`, `draw`, `stalemate`, `gameOver`; `game-session-service` cáº­p nháº­t game thÃ nh `finished`, lÆ°u `game.finished` vÃ  publish event cho WebSocket/leaderboard/bot.

## Distributed systems points

- Redis distributed lock: `lock:game:{gameId}` trong `game-session-service`.
- Kafka event streaming: các service publish/consume event thay vì gọi đồng bộ toàn bộ.
- Event sourcing: bảng `game_events` lưu `game.created` và `move.played`, `replay-service` đọc lại theo thứ tự.
- Server authoritative game lifecycle: `game-session-service` dùng `chess.js` để tự validate legal move, FEN, PGN, checkmate, stalemate, draw, resign và timeout.
- JWT protection: REST API chính và Socket.IO đều dùng JWT; frontend có nút tạo demo player để lấy token.
- Server timer: `game-session-service` tick mỗi giây, publish `timer.tick`, và tự phát `game.finished` khi hết giờ.
- Social/demo features: friend invite, notification stream, match history, replay event view và tournament pairing đơn giản.
- Fault tolerance demo: nếu Kafka tạm lỗi, REST health vẫn hoạt động; Socket.IO tự reconnect; game state nóng ở Redis.
- Service discovery demo: Docker Compose DNS dùng tên service như `mysql`, `redis`, `kafka`.
- Scale demo: có thể nhân bản stateless service, ví dụ `docker compose up --scale websocket-service=2 --scale game-session-service=2`.

## Ghi chú

Đây là scaffold demo cho môn Hệ thống phân tán, chưa phải production hardening. Các phần nên nâng cấp khi triển khai thật: TLS thật, refresh token, schema migration, Kafka retry topic/DLQ, Redis Redlock đa node, OpenTelemetry tracing, MySQL replication thực tế và anti-cheat nâng cao.
