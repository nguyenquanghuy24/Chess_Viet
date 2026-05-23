-- Create databases for each service
CREATE DATABASE chess_auth;
CREATE DATABASE chess_matchmaking;
CREATE DATABASE chess_games;

-- Create user for all databases
CREATE USER chess_user WITH PASSWORD 'chess_password';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE chess_auth TO chess_user;
GRANT ALL PRIVILEGES ON DATABASE chess_matchmaking TO chess_user;
GRANT ALL PRIVILEGES ON DATABASE chess_games TO chess_user;
