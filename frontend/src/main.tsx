import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Chess } from 'chess.js';
import { Chessboard } from 'react-chessboard';
import { io } from 'socket.io-client';
import { Bell, Bot, Clock, Eye, Flag, Handshake, History, MessageSquare, Swords, Trophy, Users, Wifi } from 'lucide-react';
import './styles.css';

const api = import.meta.env.VITE_API_URL || '/api';
const socket = io(import.meta.env.VITE_WS_URL || '/', { transports: ['websocket'], autoConnect: false, reconnection: true });

type User = { id: string; username: string; email: string; rating: number };

function authHeaders(token: string) {
  return { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
}

function formatClock(ms: number) {
  const safe = Math.max(0, Math.floor(ms / 1000));
  const minutes = String(Math.floor(safe / 60)).padStart(2, '0');
  const seconds = String(safe % 60).padStart(2, '0');
  return `${minutes}:${seconds}`;
}

function App() {
  const chess = useMemo(() => new Chess(), []);
  const [token, setToken] = useState(localStorage.getItem('token') || '');
  const [user, setUser] = useState<User | null>(JSON.parse(localStorage.getItem('user') || 'null'));
  const [fen, setFen] = useState(chess.fen());
  const [pgn, setPgn] = useState('');
  const [gameId, setGameId] = useState('demo-room');
  const [messages, setMessages] = useState<any[]>([]);
  const [leaderboard, setLeaderboard] = useState<any[]>([]);
  const [history, setHistory] = useState<any[]>([]);
  const [replay, setReplay] = useState<any[]>([]);
  const [notifications, setNotifications] = useState<any[]>([]);
  const [tournaments, setTournaments] = useState<any[]>([]);
  const [connected, setConnected] = useState(socket.connected);
  const [whiteTimeMs, setWhiteTimeMs] = useState(300000);
  const [blackTimeMs, setBlackTimeMs] = useState(300000);
  const [aiColor, setAiColor] = useState<'white' | 'black'>('black');
  const [difficulty, setDifficulty] = useState('medium');
  const [gameStatus, setGameStatus] = useState('idle');

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

  useEffect(() => {
    if (!token || !user) return;
    socket.auth = { token };
    socket.connect();
    socket.on('connect', () => { setConnected(true); socket.emit('game:join', { gameId }); });
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
    socket.on('draw.offered', () => setNotifications((items) => [{ topic: 'draw.offered', event: { message: 'Draw offered' } }, ...items]));
    socket.on('friend.invited', (event) => setNotifications((items) => [{ topic: 'friend.invited', event }, ...items]));
    socket.on('chat:message', (event) => setMessages((items) => [...items.slice(-20), event]));
    socket.emit('game:join', { gameId });
    refreshData();
    return () => { socket.removeAllListeners(); socket.disconnect(); };
  }, [token, user?.id, gameId]);

  async function refreshData() {
    if (!token || !user) return;
    fetch(`${api}/leaderboard`).then((r) => r.json()).then(setLeaderboard).catch(() => setLeaderboard([]));
    request('/games').then(setHistory).catch(() => setHistory([]));
    request(`/notifications/${user.id}`).then(setNotifications).catch(() => setNotifications([]));
    fetch(`${api}/matchmaking/tournaments`).then((r) => r.json()).then(setTournaments).catch(() => setTournaments([]));
  }

  function onDrop(sourceSquare: string, targetSquare: string) {
    if (!token || gameStatus.includes('finished') || gameStatus.includes('checkmate') || gameStatus.includes('draw')) return false;
    socket.emit('game:move', { gameId, from: sourceSquare, to: targetSquare, promotion: 'q' });
    return true;
  }

  async function startAiGame() {
    if (!user) return;
    chess.reset();
    setFen(chess.fen());
    setGameStatus('creating');
    const body = aiColor === 'black'
      ? { whiteId: user.id, blackId: 'ai-bot', timeControl: 'rapid' }
      : { whiteId: 'ai-bot', blackId: user.id, timeControl: 'rapid' };
    const game = await request('/games', { method: 'POST', body: JSON.stringify(body) });
    setGameId(game.matchId);
    socket.emit('game:join', { gameId: game.matchId });
    await request(`/ai/games/${game.matchId}/configure`, { method: 'POST', body: JSON.stringify({ botColor: aiColor, difficulty, fen: chess.fen() }) });
    setGameStatus(`playing AI ${aiColor} / ${difficulty}`);
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

  async function createTournament() {
    await request('/matchmaking/tournaments', { method: 'POST', body: JSON.stringify({ name: `Arena ${new Date().toLocaleTimeString()}` }) });
    refreshData();
  }

  async function joinTournament(id: string) {
    await request(`/matchmaking/tournaments/${id}/join`, { method: 'POST', body: JSON.stringify({ rating: user?.rating || 1200 }) });
    refreshData();
  }

  async function pairTournament(id: string) {
    await request(`/matchmaking/tournaments/${id}/pairings`, { method: 'POST', body: '{}' });
    refreshData();
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
        <button><Swords size={18} /> Play</button>
        <button><Eye size={18} /> Watch</button>
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
          <h2><Bot size={18} /> AI Bot</h2>
          <div className="controls">
            <select value={aiColor} onChange={(e) => setAiColor(e.target.value as 'white' | 'black')}><option value="black">Bot black</option><option value="white">Bot white</option></select>
            <select value={difficulty} onChange={(e) => setDifficulty(e.target.value)}><option value="beginner">Beginner</option><option value="easy">Easy</option><option value="medium">Medium</option><option value="hard">Hard</option><option value="expert">Expert</option></select>
            <button onClick={startAiGame}>Start</button>
          </div>
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
          <h2><Trophy size={18} /> Tournament</h2>
          <button className="wide" onClick={createTournament}>Create arena</button>
          <div className="list">{tournaments.map((t) => <p key={t.id}><b>{t.name}</b><button onClick={() => joinTournament(t.id)}>Join</button><button onClick={() => pairTournament(t.id)}>Pair</button></p>)}</div>
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

        <section>
          <h2><Trophy size={18} /> Live Leaderboard</h2>
          {leaderboard.length ? leaderboard.map((row) => <p key={row.userId}>#{row.rank} {row.userId} <b>{row.rating}</b></p>) : <p>No ratings yet</p>}
        </section>
      </aside>
    </main>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
