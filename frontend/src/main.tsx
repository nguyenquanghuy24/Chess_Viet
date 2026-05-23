import React, { useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Chess } from 'chess.js';
import { Chessboard } from 'react-chessboard';
import { io } from 'socket.io-client';
import { Bell, Bot, Clock, Eye, Flag, Handshake, History, MessageSquare, Swords, Users, Wifi } from 'lucide-react';
import './styles.css';

const api = import.meta.env.VITE_API_URL || '/api';
const socket = io(import.meta.env.VITE_WS_URL || '/', { transports: ['websocket'], autoConnect: false, reconnection: true });

type User = { id: string; username: string; email: string; rating: number };
type PresencePerson = {
  userId: string;
  username?: string;
  role: 'white' | 'black' | 'spectator' | 'viewer';
  connections?: number;
};
type PresenceState = {
  gameId: string;
  total: number;
  playersOnline: number;
  spectatorsOnline: number;
  white: PresencePerson | null;
  black: PresencePerson | null;
  players: PresencePerson[];
  spectators: PresencePerson[];
};

function authHeaders(token: string) {
  return { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
}

function formatClock(ms: number) {
  const safe = Math.max(0, Math.floor(ms / 1000));
  const minutes = String(Math.floor(safe / 60)).padStart(2, '0');
  const seconds = String(safe % 60).padStart(2, '0');
  return `${minutes}:${seconds}`;
}

function emptyPresence(gameId: string): PresenceState {
  return { gameId, total: 0, playersOnline: 0, spectatorsOnline: 0, white: null, black: null, players: [], spectators: [] };
}

function normalizePresence(payload: any, fallbackGameId: string): PresenceState {
  if (typeof payload === 'number') {
    return { ...emptyPresence(fallbackGameId), total: payload };
  }
  return {
    gameId: payload?.gameId || fallbackGameId,
    total: Number(payload?.total || 0),
    playersOnline: Number(payload?.playersOnline || 0),
    spectatorsOnline: Number(payload?.spectatorsOnline || 0),
    white: payload?.white || null,
    black: payload?.black || null,
    players: payload?.players || [],
    spectators: payload?.spectators || []
  };
}

function displayPresenceName(person?: PresencePerson | null) {
  if (!person) return 'Offline';
  const base = person.username || person.userId.slice(0, 8);
  return person.connections && person.connections > 1 ? `${base} (${person.connections})` : base;
}

function App() {
  const chess = useMemo(() => new Chess(), []);
  const [token, setToken] = useState(localStorage.getItem('token') || '');
  const [user, setUser] = useState<User | null>(JSON.parse(localStorage.getItem('user') || 'null'));
  const [fen, setFen] = useState(chess.fen());
  const [pgn, setPgn] = useState('');
  const [gameId, setGameId] = useState('demo-room');
  const [messages, setMessages] = useState<any[]>([]);
  const [history, setHistory] = useState<any[]>([]);
  const [replay, setReplay] = useState<any[]>([]);
  const [notifications, setNotifications] = useState<any[]>([]);
  const [connected, setConnected] = useState(socket.connected);
  const [whiteTimeMs, setWhiteTimeMs] = useState(300000);
  const [blackTimeMs, setBlackTimeMs] = useState(300000);
  const [aiColor, setAiColor] = useState<'white' | 'black'>('black');
  const [gameStatus, setGameStatus] = useState('idle');
  const [matchmakingStatus, setMatchmakingStatus] = useState('Ready');
  const [searching, setSearching] = useState(false);
  const [presence, setPresence] = useState<PresenceState>(emptyPresence(gameId));
  const gameIdRef = useRef(gameId);

  async function request(path: string, options: RequestInit = {}) {
    const res = await fetch(`${api}${path}`, { ...options, headers: { ...(token ? authHeaders(token) : { 'Content-Type': 'application/json' }), ...(options.headers || {}) } });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  }

  async function demoLogin() {
    const suffix = Math.floor(Math.random() * 100000);
    const payload = { username: `demo${suffix}`, email: `demo${suffix}@chess.local`, password: '123456' };
    const auth = await fetch(`${api}/auth/register`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).then((r) => r.json());
    localStorage.setItem('token', auth.token);
    localStorage.setItem('user', JSON.stringify(auth.user));
    setToken(auth.token);
    setUser(auth.user);
  }

  function resetBoard() {
    chess.reset();
    setFen(chess.fen());
    setPgn('');
    setWhiteTimeMs(300000);
    setBlackTimeMs(300000);
    setPresence(emptyPresence(gameIdRef.current));
  }

  function joinMatchedGame(match: any) {
    const nextGameId = match.matchId || match.gameId;
    if (!nextGameId || !user) return;
    resetBoard();
    setGameId(nextGameId);
    setPresence(emptyPresence(nextGameId));
    setWhiteTimeMs(Number(match.initialTimeMs || 300000));
    setBlackTimeMs(Number(match.initialTimeMs || 300000));
    setSearching(false);
    setMatchmakingStatus(`Matched as ${match.color || (match.whiteId === user.id ? 'white' : 'black')}`);
    setGameStatus('active');
    socket.emit('game:join', { gameId: nextGameId });
    refreshData();
  }

  useEffect(() => {
    gameIdRef.current = gameId;
    setPresence(emptyPresence(gameId));
    if (token && user && socket.connected) socket.emit('game:join', { gameId });
  }, [token, user?.id, gameId]);

  useEffect(() => {
    if (!token || !user) return;
    socket.auth = { token };
    socket.connect();
    socket.on('connect', () => { setConnected(true); socket.emit('game:join', { gameId: gameIdRef.current }); });
    socket.on('disconnect', () => setConnected(false));
    socket.on('game:state', (state) => {
      if (state.fen) {
        chess.load(state.fen);
        setFen(state.fen);
      }
      if (state.pgn) setPgn(state.pgn);
      if (state.whiteTimeMs) setWhiteTimeMs(Number(state.whiteTimeMs));
      if (state.blackTimeMs) setBlackTimeMs(Number(state.blackTimeMs));
      if (state.status) setGameStatus(state.status);
    });
    socket.on('move.played', (event) => {
      if (event.fen) {
        chess.load(event.fen);
        setFen(event.fen);
      }
      if (event.pgn) setPgn(event.pgn);
      if (event.whiteTimeMs !== undefined) setWhiteTimeMs(event.whiteTimeMs);
      if (event.blackTimeMs !== undefined) setBlackTimeMs(event.blackTimeMs);
      if (event.status === 'finished') setGameStatus('finished');
    });
    socket.on('timer.tick', (event) => {
      setWhiteTimeMs(event.whiteTimeMs);
      setBlackTimeMs(event.blackTimeMs);
    });
    socket.on('game.finished', (event) => setGameStatus(`${event.reason}: ${event.result}`));
    socket.on('move.rejected', (event) => setGameStatus(`move rejected: ${event.reason}`));
    socket.on('match:found', joinMatchedGame);
    socket.on('presence:changed', (payload) => setPresence(normalizePresence(payload, gameIdRef.current)));
    socket.on('draw.offered', () => setNotifications((items) => [{ topic: 'draw.offered', event: { message: 'Draw offered' } }, ...items]));
    socket.on('friend.invited', (event) => setNotifications((items) => [{ topic: 'friend.invited', event }, ...items]));
    socket.on('chat:message', (event) => setMessages((items) => [...items.slice(-20), event]));
    socket.emit('game:join', { gameId: gameIdRef.current });
    refreshData();
    return () => { socket.removeAllListeners(); socket.disconnect(); };
  }, [token, user?.id]);

  async function refreshData() {
    if (!token || !user) return;
    request('/games').then(setHistory).catch(() => setHistory([]));
    request(`/notifications/${user.id}`).then(setNotifications).catch(() => setNotifications([]));
  }

  function onDrop(sourceSquare: string, targetSquare: string) {
    if (!token || !socket.connected || gameStatus.includes('finished') || gameStatus.includes('checkmate') || gameStatus.includes('draw')) return false;
    socket.emit('game:move', { gameId, from: sourceSquare, to: targetSquare, promotion: 'q' }, (ack: any) => {
      if (!ack?.accepted) setGameStatus(`move rejected: ${ack?.reason || 'unknown'}`);
    });
    return true;
  }

  async function findMatch() {
    if (!user || searching) return;
    resetBoard();
    setSearching(true);
    setMatchmakingStatus('Searching...');
    setGameStatus('Searching...');
    try {
      const result = await request('/matchmaking/queue', {
        method: 'POST',
        body: JSON.stringify({ rating: user.rating || 1200, timeControl: 'rapid' })
      });
      if (result.status === 'matched') joinMatchedGame(result);
    } catch (error) {
      setSearching(false);
      setMatchmakingStatus('Matchmaking failed');
      setGameStatus('matchmaking failed');
    }
  }

  function watchGame() {
    if (!gameId || !socket.connected) return;
    setSearching(false);
    setMatchmakingStatus('Ready');
    setGameStatus('spectating');
    socket.emit('game:join', { gameId, spectator: true });
  }

  async function startAiGame() {
    if (!user) return;
    resetBoard();
    setSearching(false);
    setMatchmakingStatus('Ready');
    setGameStatus('creating');
    const body = aiColor === 'black'
      ? { whiteId: user.id, blackId: 'ai-bot', timeControl: 'rapid' }
      : { whiteId: 'ai-bot', blackId: user.id, timeControl: 'rapid' };
    const game = await request('/games', { method: 'POST', body: JSON.stringify(body) });
    setGameId(game.matchId);
    socket.emit('game:join', { gameId: game.matchId });
    await request(`/ai/games/${game.matchId}/configure`, { method: 'POST', body: JSON.stringify({ botColor: aiColor, fen: chess.fen() }) });
    setGameStatus(`playing simple AI (${aiColor})`);
  }

  async function resign() {
    await request(`/games/${gameId}/resign`, { method: 'POST', body: '{}' });
  }

  async function offerDraw() {
    await request(`/games/${gameId}/draw`, { method: 'POST', body: JSON.stringify({ offer: true }) });
  }

  async function loadReplay(id: string) {
    const data = await request(`/replay/games/${id}/replay`);
    setReplay(data.events || []);
    setGameId(id);
  }

  async function inviteFriend(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const input = event.currentTarget.elements.namedItem('friendId') as HTMLInputElement;
    if (!input.value.trim()) return;
    await request('/auth/friends/invite', { method: 'POST', body: JSON.stringify({ userId: input.value, gameId }) });
    input.value = '';
    refreshData();
  }

  function sendMessage(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const input = event.currentTarget.elements.namedItem('message') as HTMLInputElement;
    if (!input.value.trim()) return;
    socket.emit('chat:message', { gameId, roomId: gameId, body: input.value });
    input.value = '';
  }

  if (!token || !user) {
    return (
      <main className="login">
        <h1>Chess Viet</h1>
        <p>Demo login tạo JWT để bảo vệ REST API và Socket.IO.</p>
        <button onClick={demoLogin}>Create demo player</button>
      </main>
    );
  }

  return (
    <main className="shell">
      <aside className="rail">
        <h1>Chess Viet</h1>
        <button onClick={findMatch}><Swords size={18} /> Find Match</button>
        <button onClick={watchGame}><Eye size={18} /> Watch</button>
        <button><Bot size={18} /> AI Bot</button>
        <button onClick={() => { localStorage.clear(); location.reload(); }}><Users size={18} /> Logout</button>
      </aside>

      <section className="boardArea">
        <div className="topbar">
          <span><Wifi size={16} /> {connected ? 'Realtime online' : 'Reconnecting'}</span>
          <input value={gameId} onChange={(e) => setGameId(e.target.value)} />
        </div>
        <div className="players"><strong>Black</strong><span><Clock size={16} /> {formatClock(blackTimeMs)}</span></div>
        <div className="board"><Chessboard position={fen} onPieceDrop={onDrop} boardWidth={Math.min(680, Math.max(320, window.innerWidth - 440))} customDarkSquareStyle={{ backgroundColor: '#779556' }} customLightSquareStyle={{ backgroundColor: '#ebecd0' }} /></div>
        <div className="players"><strong>{user.username}</strong><span><Clock size={16} /> {formatClock(whiteTimeMs)}</span></div>
        <div className="actionbar">
          <button onClick={resign}><Flag size={16} /> Resign</button>
          <button onClick={offerDraw}><Handshake size={16} /> Draw</button>
          <span>{gameStatus}</span>
        </div>
      </section>

      <aside className="panel">
        <section>
          <h2><Swords size={18} /> Play Online</h2>
          <button className="wide" onClick={findMatch} disabled={searching}>{searching ? 'Searching...' : 'Find Match'}</button>
          <p className="statusLine">{matchmakingStatus}</p>
        </section>

        <section>
          <h2><Bot size={18} /> AI Bot</h2>
          <div className="controls">
            <select value={aiColor} onChange={(e) => setAiColor(e.target.value as 'white' | 'black')}><option value="black">Bot black</option><option value="white">Bot white</option></select>
            <button onClick={startAiGame}>Start</button>
          </div>
        </section>

        <section>
          <h2><Eye size={18} /> Spectators</h2>
          <div className="presenceMeta">
            <span><Users size={15} /> {presence.playersOnline}/2</span>
            <span><Eye size={15} /> {presence.spectatorsOnline}</span>
          </div>
          <div className="presenceRows">
            <p><b>White</b><span>{displayPresenceName(presence.white)}</span></p>
            <p><b>Black</b><span>{displayPresenceName(presence.black)}</span></p>
          </div>
          <div className="presenceRows compact">
            {presence.spectators.length
              ? presence.spectators.map((person) => <p key={`${person.role}:${person.userId}`}><b>{person.role === 'viewer' ? 'Viewer' : 'Spectator'}</b><span>{displayPresenceName(person)}</span></p>)
              : <p><b>Spectators</b><span>0 online</span></p>}
          </div>
          <button className="wide secondary" onClick={watchGame}>Watch</button>
        </section>

        <section>
          <h2><History size={18} /> Match History</h2>
          <div className="list">{history.map((g) => <button key={g.id} onClick={() => loadReplay(g.id)}>{g.id.slice(0, 8)} {g.status} {g.result || ''}</button>)}</div>
          <pre>{replay.slice(-6).map((e) => `${e.event_type}: ${JSON.stringify(e.payload).slice(0, 90)}`).join('\n')}</pre>
        </section>

        <section>
          <h2><Users size={18} /> Friend Invite</h2>
          <form onSubmit={inviteFriend}><input name="friendId" placeholder="Friend user id" /><button>Invite</button></form>
        </section>

        <section>
          <h2><Bell size={18} /> Notifications</h2>
          <div className="list">{notifications.slice(0, 5).map((n, i) => <p key={i}><b>{n.topic}</b><span>{JSON.stringify(n.event).slice(0, 60)}</span></p>)}</div>
        </section>

        <section>
          <h2><MessageSquare size={18} /> Chat</h2>
          <div className="chat">{messages.map((m, i) => <p key={i}><b>{m.username || m.userId}</b> {m.body}</p>)}</div>
          <form onSubmit={sendMessage}><input name="message" placeholder="Message" /><button>Send</button></form>
        </section>
      </aside>
    </main>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
