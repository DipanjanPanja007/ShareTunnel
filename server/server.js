/**
 * ShareTunnel — Signaling Server
 * Stack: Node.js (LTS) + Express 4 + ws 8
 * Module: ES Modules ("type": "module")
 *
 * Responsibilities:
 *   - Session creation (POST /api/session)
 *   - Session verification (POST /api/session/verify)
 *   - WebSocket signaling relay (SDP + ICE candidates)
 *
 * Does NOT handle any file data — pure P2P via WebRTC DataChannel.
 */

import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { v4 as uuidv4 } from 'uuid';

// ─── Environment config ───────────────────────────────────────────────────────
// All values have safe defaults for local development.
// In production set PORT and ALLOWED_ORIGINS as environment variables.

const PORT = process.env.PORT ?? 3001;

// ALLOWED_ORIGINS: comma-separated list of allowed frontend origins.
// If not set, defaults to permissive '*' (fine for local dev / open source).
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
  : '*';

// ─── App Setup ───────────────────────────────────────────────────────────────

const app = express();
app.use(cors({
  origin: ALLOWED_ORIGINS,
  methods: ['GET', 'POST', 'OPTIONS'],
}));
app.use(express.json());

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer });

// ─── In-Memory Session Store ─────────────────────────────────────────────────
//
// session shape:
// {
//   sessionId: string,
//   verificationCode: string,
//   sender: WebSocket | null,
//   receiver: WebSocket | null,
//   fileInfo: object | null,
//   createdAt: number,
// }

const sessions = new Map();

// ws → { sessionId, role }
const wsInfo = new Map();

// ─── Helpers ─────────────────────────────────────────────────────────────────

function rand4() {
  return String(Math.floor(1000 + Math.random() * 9000));
}

function safeSend(ws, payload) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function peerOf(session, role) {
  return role === 'sender' ? session.receiver : session.sender;
}

// ─── REST API ─────────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  res.json({ ok: true, sessions: sessions.size });
});

/** Sender calls this first to create a session. */
app.post('/api/session', (_req, res) => {
  const sessionId = uuidv4();
  const verificationCode = rand4();
  sessions.set(sessionId, {
    sessionId,
    verificationCode,
    sender: null,
    receiver: null,
    fileInfo: null,
    createdAt: Date.now(),
  });
  console.log(`[session] created ${sessionId} code=${verificationCode}`);
  res.json({ sessionId, verificationCode });
});

/** Receiver calls this to validate code before opening WebSocket. */
app.post('/api/session/verify', (req, res) => {
  const { sessionId, verificationCode } = req.body;
  const session = sessions.get(sessionId);

  if (!session) return res.status(404).json({ error: 'Session not found' });
  if (session.verificationCode !== String(verificationCode))
    return res.status(401).json({ error: 'Invalid verification code' });
  if (session.receiver?.readyState === WebSocket.OPEN)
    return res.status(409).json({ error: 'Session already occupied' });

  res.json({ ok: true, fileInfo: session.fileInfo });
});

// ─── WebSocket Signaling ──────────────────────────────────────────────────────

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    dispatch(ws, msg);
  });

  ws.on('close', () => handleClose(ws));
  ws.on('error', (err) => console.error('[ws] error:', err.message));
});

function dispatch(ws, msg) {
  switch (msg.type) {
    case 'register-sender':   return onRegisterSender(ws, msg);
    case 'register-receiver': return onRegisterReceiver(ws, msg);
    case 'file-info':         return onFileInfo(ws, msg);
    // WebRTC signaling — relay to peer
    case 'offer':
    case 'answer':
    case 'ice-candidate':
    // Transfer events — relay to peer
    case 'transfer-complete':
    case 'peer-ready':
      return relay(ws, msg);
    default:
      safeSend(ws, { type: 'error', message: `Unknown type: ${msg.type}` });
  }
}

function onRegisterSender(ws, { sessionId }) {
  const session = sessions.get(sessionId);
  if (!session) { safeSend(ws, { type: 'error', message: 'Session not found' }); return; }

  session.sender = ws;
  wsInfo.set(ws, { sessionId, role: 'sender' });
  safeSend(ws, { type: 'registered', role: 'sender' });
  console.log(`[session] sender registered ${sessionId}`);
}

function onRegisterReceiver(ws, { sessionId, verificationCode }) {
  const session = sessions.get(sessionId);
  if (!session) { safeSend(ws, { type: 'error', message: 'Session not found' }); return; }
  if (session.verificationCode !== String(verificationCode)) {
    safeSend(ws, { type: 'error', message: 'Invalid verification code' }); return;
  }
  if (session.receiver?.readyState === WebSocket.OPEN) {
    safeSend(ws, { type: 'error', message: 'Session already occupied' }); return;
  }

  session.receiver = ws;
  wsInfo.set(ws, { sessionId, role: 'receiver' });
  safeSend(ws, { type: 'registered', role: 'receiver', fileInfo: session.fileInfo });
  safeSend(session.sender, { type: 'receiver-joined' });
  console.log(`[session] receiver joined ${sessionId}`);
}

function onFileInfo(ws, { fileInfo }) {
  const info = wsInfo.get(ws);
  if (!info || info.role !== 'sender') return;
  const session = sessions.get(info.sessionId);
  if (session) session.fileInfo = fileInfo;
}

function relay(ws, msg) {
  const info = wsInfo.get(ws);
  if (!info) return;
  const session = sessions.get(info.sessionId);
  if (!session) return;
  safeSend(peerOf(session, info.role), msg);
}

function handleClose(ws) {
  const info = wsInfo.get(ws);
  if (!info) return;
  const { sessionId, role } = info;
  const session = sessions.get(sessionId);
  console.log(`[session] ${role} disconnected ${sessionId}`);

  if (session) {
    safeSend(peerOf(session, role), { type: 'peer-disconnected', role });
    if (role === 'sender') session.sender = null;
    else session.receiver = null;
  }
  wsInfo.delete(ws);
}

// ─── Session Expiry (1 h TTL, checked every 10 min) ──────────────────────────

setInterval(() => {
  const cutoff = Date.now() - 60 * 60 * 1000;
  for (const [id, s] of sessions) {
    if (s.createdAt < cutoff) { sessions.delete(id); console.log(`[session] expired ${id}`); }
  }
}, 10 * 60 * 1000);

// ─── Start ────────────────────────────────────────────────────────────────────

httpServer.listen(PORT, () => console.log(`[sharetunnel] signaling server → http://localhost:${PORT}`));
