// User types
export interface User {
  id: string;
  username: string;
  email: string;
  rating: number;
  gamesPlayed: number;
  wins: number;
  losses: number;
  draws: number;
  createdAt: Date;
  updatedAt: Date;
}

// Game types
export interface GameSession {
  id: string;
  player1Id: string;
  player2Id: string;
  status: 'WAITING' | 'ACTIVE' | 'FINISHED' | 'ABORTED';
  winnerId?: string;
  pgnText?: string;
  timeControl: 'BLITZ' | 'RAPID' | 'CLASSICAL';
  initialTime: number;
  increment: number;
  createdAt: Date;
  finishedAt?: Date;
}

export interface GameState {
  id: string;
  board: string;
  turn: 'white' | 'black';
  status: 'WAITING' | 'ACTIVE' | 'FINISHED' | 'ABORTED';
  player1Time: number;
  player2Time: number;
  lastMove?: {
    from: string;
    to: string;
    piece: string;
    captured?: string;
  };
}

// Move types
export interface Move {
  id: string;
  gameId: string;
  playerId: string;
  moveNumber: number;
  from: string;
  to: string;
  piece: string;
  captured?: string;
  isCheck: boolean;
  isCheckmate: boolean;
  timestamp: Date;
}

// Matchmaking types
export interface PlayerQueue {
  id: string;
  userId: string;
  rating: number;
  gameType: 'RATED' | 'CASUAL';
  timeControl: 'BLITZ' | 'RAPID' | 'CLASSICAL';
  queuedAt: Date;
}

// WebSocket message types
export interface WebSocketMessage {
  type: string;
  gameId?: string;
  userId?: string;
  data?: any;
  timestamp: Date;
}

export enum WebSocketMessageType {
  JOIN_GAME = 'join_game',
  LEAVE_GAME = 'leave_game',
  MAKE_MOVE = 'make_move',
  GAME_STATE = 'game_state',
  MOVE_ACCEPTED = 'move_accepted',
  MOVE_REJECTED = 'move_rejected',
  GAME_FINISHED = 'game_finished',
  OPPONENT_DISCONNECTED = 'opponent_disconnected',
  RECONNECTED = 'reconnected'
}

// API response types
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
    details?: any;
  };
  timestamp: Date;
}

// Kafka event types
export interface KafkaEvent {
  topic: string;
  key: string;
  value: any;
  timestamp: number;
}