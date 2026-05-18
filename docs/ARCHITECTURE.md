# Architecture Notes

## Flow tạo trận

1. Player gọi `POST /api/matchmaking/queue`.
2. `matchmaking-service` lưu queue vào Redis sorted set theo Elo.
3. Khi tìm được đối thủ trong khoảng rating, service publish `match.created`.
4. `game-session-service` consume event, tạo game, cache state trong Redis và publish `game.started`.
5. `websocket-service` broadcast game mới tới các client đang join room.

## Flow nước đi realtime

1. Client emit `game:move`.
2. `websocket-service` publish `move.requested` lên Kafka.
3. `move-validation-service` validate bằng `chess.js`, publish `move.validated` hoặc `move.rejected`.
4. `game-session-service` nhận `move.validated`, lấy Redis lock `lock:game:{id}`, ghi `game_events`, cập nhật Redis state, publish `move.played`.
5. `websocket-service` consume `move.played` và broadcast patch về room.

## Clean architecture mức demo

Mỗi service đang tách theo bounded context. Trong demo, code gói trong một entrypoint để dễ đọc và chạy. Khi mở rộng, nên tách:

- `domain`: entity, rule, event.
- `application`: use case.
- `infrastructure`: Kafka, Redis, MySQL.
- `interfaces`: REST, Socket.IO handlers.
