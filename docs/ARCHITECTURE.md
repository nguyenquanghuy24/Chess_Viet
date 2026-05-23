# Architecture Notes

Scope demo hiện tại đã bỏ `tournament mode` và `leaderboard` khỏi luồng chạy chính để giảm độ phức tạp. Các điểm distributed systems được giữ lại gồm API Gateway, JWT auth, matchmaking, realtime game sync, chat, replay, notification, Redis lock, Kafka event streaming và monitoring.

## Flow tạo trận

1. Player gọi `POST /api/matchmaking/queue`.
2. `matchmaking-service` lưu queue vào Redis sorted set theo Elo.
3. Khi tìm được đối thủ trong khoảng rating, service publish `match.created`.
4. `game-session-service` consume event, tạo game, cache state trong Redis và publish `game.started`.
5. `websocket-service` consume `match.created`, gửi `match:found` tới room riêng `user:{id}` của cả hai người chơi.
6. Frontend tự set `matchId`, emit `game:join`, rồi nhận `game:state`/`game.started`.

## Flow nước đi realtime

1. Client emit `game:move`.
2. `websocket-service` publish `move.requested` lên Kafka.
3. `move-validation-service` validate bằng `chess.js` từ FEN hiện tại được gửi kèm event, publish `move.validated` hoặc `move.rejected`.
4. `game-session-service` nhận `move.validated`, lấy Redis lock `lock:game:{id}`, ghi `game_events`, cập nhật Redis state, publish `move.played`.
5. `websocket-service` consume `move.played` và broadcast patch về room.

## Presence và spectator

1. Mỗi socket sau khi xác thực được join room riêng `user:{id}` để nhận event cá nhân.
2. Khi client emit `game:join`, `websocket-service` lưu presence vào Redis hash `presence:{gameId}`.
3. Service phân vai presence theo Redis game state: `white`, `black`, hoặc `spectator`.
4. Mỗi lần join/disconnect/game.started, service emit `presence:changed` với danh sách player và spectator online.

## Clean architecture mức demo

Mỗi service đang tách theo bounded context. Trong demo, code gói trong một entrypoint để dễ đọc và chạy. Khi mở rộng, nên tách:

- `domain`: entity, rule, event.
- `application`: use case.
- `infrastructure`: Kafka, Redis, MySQL.
- `interfaces`: REST, Socket.IO handlers.
