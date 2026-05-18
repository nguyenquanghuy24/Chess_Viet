CREATE DATABASE IF NOT EXISTS chess_auth;
CREATE DATABASE IF NOT EXISTS chess_matchmaking;
CREATE DATABASE IF NOT EXISTS chess_games;
CREATE DATABASE IF NOT EXISTS chess_leaderboard;
CREATE DATABASE IF NOT EXISTS chess_chat;

GRANT ALL PRIVILEGES ON chess_auth.* TO 'chess_user'@'%';
GRANT ALL PRIVILEGES ON chess_matchmaking.* TO 'chess_user'@'%';
GRANT ALL PRIVILEGES ON chess_games.* TO 'chess_user'@'%';
GRANT ALL PRIVILEGES ON chess_leaderboard.* TO 'chess_user'@'%';
GRANT ALL PRIVILEGES ON chess_chat.* TO 'chess_user'@'%';
FLUSH PRIVILEGES;
