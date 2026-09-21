<<<<<<< HEAD
# ⟆ ShareTunnel — P2P File Transfer

Secure peer-to-peer file transfer via WebRTC DataChannels.
Files travel **directly** between browsers — the server handles only WebRTC signaling.

---

## Exact Dependency Versions

### Client (`/client`)
| Package | Version |
|---|---|
| `react` | `^19.1.0` |
| `react-dom` | `^19.1.0` |
| `qrcode` | `^1.5.4` |
| `vite` | `^6.3.2` |
| `@vitejs/plugin-react` | `^4.3.4` |
| `tailwindcss` | `^4.1.4` |
| `@tailwindcss/vite` | `^4.1.4` |

### Server (`/server`)
| Package | Version |
|---|---|
| `express` | `^4.19.2` |
| `ws` | `^8.18.0` |
| `uuid` | `^9.0.1` |
| `cors` | `^2.8.5` |

**Runtime:** Node.js ≥ 20 (LTS)

---

## Quick Start (Local)

### 1 — Signaling Server

```bash
cd server
npm install
node server.js
# → http://localhost:3001
```

For development with auto-restart (Node 18+ built-in):
```bash
node --watch server.js
```

### 2 — React Frontend

```bash
cd client
npm install
npm run dev
# → http://localhost:3000
```

### 3 — Test a Transfer

1. Open **http://localhost:3000** in Browser A
2. Drop any file onto the page
3. Copy the share link that appears
4. Open the link in Browser B (or another device on the same network)
5. Enter the 4-digit verification code shown in Browser A
6. Watch the transfer complete in real time

---

## Architecture

```
Browser A (Sender)                    Browser B (Receiver)
  │                                         │
  │◄──── WebRTC DataChannel (P2P) ─────────►│
  │         (64 KB chunks, binary)           │
  │                                         │
  │         Signaling Server                │
  ├──── WebSocket ─── Express + ws ─────────┤
       (offer / answer / ICE only)
       NO file data ever passes here
```

### Signaling Server — `server/server.js`
- `POST /api/session` — creates session → returns `sessionId` + `verificationCode`
- `POST /api/session/verify` — validates code (called by receiver before WS)
- **WebSocket** — relays `offer`, `answer`, `ice-candidate` between the two peers
- Sessions expire after 1 hour; checked every 10 minutes

### Transfer Protocol (over DataChannel)

```
Sender → Receiver   file-header  { fileName, fileSize, totalChunks, … }
Sender → Receiver   chunk frame  [4B meta length][meta JSON][data] × N
Receiver → Sender   ACK          { type:"ACK", seq }  (batched every 80ms)
Receiver → Sender   MISSING      { type:"MISSING", seqs:[…] }  (every 3s scan)
Sender → Receiver   file-footer  { sha256 }
Receiver → Sender   DONE
```

### Reliability System
- **Sliding window** — 32 chunks in-flight simultaneously
- **ACK timeout** — retransmit any unacknowledged chunk after 5 s
- **MISSING scan** — receiver requests gaps every 3 s
- **Duplicate guard** — receiver ignores already-received seq numbers
- **Out-of-order** — Map-based storage, assembled in order at the end
- **SHA-256** — full file integrity check before download triggers
- **Backpressure** — sender pauses when `bufferedAmount > 8 MB`

---

## Project Structure

```
dropr/
├── server/
│   ├── package.json        ES modules + dependencies
│   └── server.js           Express + ws signaling server
│
└── client/
    ├── index.html          Vite entry (root level, not /public)
    ├── vite.config.js      Vite 6 + React + Tailwind v4
    ├── package.json        All client dependencies
    └── src/
        ├── main.jsx        React 19 createRoot entry
        ├── App.jsx         Hash router (/ = send, #receive/ID = receive)
        ├── index.css       @import "tailwindcss" (Tailwind v4 syntax)
        ├── utils.js        fmtBytes, fmtSpeed, fmtEta, QR, clipboard, download
        ├── webrtc/
        │   └── WebRTCManager.js   RTCPeerConnection + DataChannel setup
        ├── transfer/
        │   ├── ChunkSender.js     Sliding window, ACK handling, retransmit
        │   └── ChunkReceiver.js   Frame parser, ACK batching, MISSING, SHA-256
        ├── hooks/
        │   └── useSignaling.js    WebSocket React hook
        └── pages/
            ├── SenderPage.jsx     Drop zone, QR, code display, progress
            └── ReceiverPage.jsx   Code input, progress bar, auto-download
```

---

## Tailwind v4 Notes

Tailwind CSS v4 is a major breaking change from v3:

| v3 | v4 (this project) |
|---|---|
| `tailwind.config.js` required | No config file needed |
| `@tailwind base/components/utilities` | `@import "tailwindcss"` |
| PostCSS plugin | `@tailwindcss/vite` Vite plugin |
| `npx tailwindcss init` | Nothing to init |

---

## Environment Variables

### Client (prefix: `VITE_`)
| Variable | Default | Description |
|---|---|---|
| `VITE_SERVER_URL` | `http://localhost:3001` | Signaling server URL |

Create `client/.env.local` for local overrides:
```
VITE_SERVER_URL=http://localhost:3001
```

---

## Deployment

### Backend → Render

1. Create a **Web Service** pointing to `/server`
2. Build command: `npm install`
3. Start command: `node server.js`
4. Note the deployed URL (e.g. `https://sharetunnel-server.onrender.com`)

### Frontend → Vercel

1. Import `/client` as a new Vercel project
2. Framework preset: **Vite**
3. Add environment variable:
   ```
   VITE_SERVER_URL=https://sharetunnel-server.onrender.com
   ```
4. Build command: `npm run build`
5. Output directory: `dist`

---

## Adding TURN Servers (Production NAT Traversal)

Without a TURN server, transfers may fail when both peers are behind symmetric NAT
(common in corporate/mobile networks). Edit `client/src/webrtc/WebRTCManager.js`:

```js
const ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  {
    urls:       'turn:your-turn-server.com:3478',
    username:   'your-username',
    credential: 'your-credential',
  },
];
```

Free/cheap TURN providers: **Twilio**, **Xirsys**, **Metered.ca**, or self-host **coturn**.

---

## Security

- All DataChannel traffic is DTLS-encrypted (WebRTC default)
- Session requires both `sessionId` (in URL) and `verificationCode` (verbal)
- Only one receiver per session — duplicate attempts are rejected (HTTP 409)
- Server validates code via REST before receiver opens WebSocket
- SHA-256 verifies file integrity end-to-end
- Sessions auto-expire after 1 hour

---

## Edge Cases Handled

| Scenario | Handling |
|---|---|
| Packet loss | ACK timeout (5 s) → retransmit |
| Out-of-order chunks | Map-based, assembled in order |
| Duplicate chunks | Duplicate guard on receiver |
| Network freeze | Periodic MISSING scan + retransmit |
| Sender disconnects mid-transfer | Receiver shows message; resume on reconnect |
| Slow receiver | Backpressure: pause when buffer > 8 MB |
| Wrong verification code | HTTP 401 before WS is opened |
| Session already occupied | HTTP 409 |
| File corruption | SHA-256 mismatch → error, no download |
| Large files (10 GB+) | Streaming slices — never fully in memory |
=======
# ShareTunnel
Peer to Peer File transfer application with no file size limit
