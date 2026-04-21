import { useState, useCallback, useRef, useEffect } from 'react';
import { WebRTCManager } from '../webrtc/WebRTCManager.jsx';
import { ChunkSender }   from '../transfer/ChunkSender.jsx';
import { useSignaling }  from '../hooks/useSignaling.jsx';
import {
  fmtBytes, fmtSpeed, fmtEta,
  buildShareLink, makeQrDataUrl, copyText, fileEmoji,
} from '../utils.jsx';
import Footer from '../components/Footer.jsx';

const SERVER = import.meta.env.VITE_SERVER_URL ?? 'http://localhost:3001';

const S = {
  IDLE:       'idle',
  WAITING:    'waiting',
  CONNECTING: 'connecting',
  SENDING:    'sending',
  DONE:       'done',
  ERROR:      'error',
};

export default function SenderPage() {
  const [status,  setStatus]  = useState(S.IDLE);
  const [file,    setFile]    = useState(null);
  const [session, setSession] = useState(null);
  const [prog,    setProg]    = useState(null);
  const [copied,  setCopied]  = useState(false);
  const [errMsg,  setErrMsg]  = useState('');

  const rtcRef  = useRef(null);
  const sidRef  = useRef(null);
  const sendRef = useRef(null);
  const fileRef = useRef(null); // tracks current file for WS onOpen file-info

  // ── Signaling ─────────────────────────────────────────────────────────────

  const onWsMsg = useCallback(async (msg) => {
    switch (msg.type) {
      case 'receiver-joined':
        setStatus(S.CONNECTING);
        await rtcRef.current?.initSender();
        break;
      case 'offer':
      case 'answer':
      case 'ice-candidate':
        await rtcRef.current?.handleSignal(msg);
        break;
      case 'peer-disconnected':
        setStatus((s) => (s === S.SENDING ? S.WAITING : s));
        break;
      case 'error':
        setErrMsg(msg.message);
        setStatus(S.ERROR);
        break;
      default:
        break;
    }
  }, []);

  const onWsOpen = useCallback((liveSend) => {
    if (sidRef.current) {
      liveSend({ type: 'register-sender', sessionId: sidRef.current });
      // Send file-info immediately so receiver can get it from /api/session/verify
      // BEFORE the DataChannel opens (fixes the picker showing 'download.download')
      if (fileRef.current) {
        liveSend({
          type: 'file-info',
          fileInfo: { name: fileRef.current.name, size: fileRef.current.size, type: fileRef.current.type },
        });
      }
    }
  }, []);

  const { connect, send, disconnect } = useSignaling({
    onMessage: onWsMsg,
    onOpen:    onWsOpen,
  });

  useEffect(() => { sendRef.current = send; }, [send]);

  // Warn user if they try to close/refresh tab during transfer
  useEffect(() => {
    const active = status === S.SENDING || status === S.CONNECTING;
    if (!active) return;
    const handler = (e) => {
      e.preventDefault();
      e.returnValue = 'Transfer in progress — are you sure you want to leave?';
      return e.returnValue;
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [status]);

  // ── Session + WebRTC setup ────────────────────────────────────────────────

  const initSession = useCallback(async (f) => {
    setFile(f);
    fileRef.current = f;
    setErrMsg('');

    try {
      const res = await fetch(`${SERVER}/api/session`, { method: 'POST' });
      if (!res.ok) throw new Error('Server error');
      const { sessionId, verificationCode } = await res.json();

      sidRef.current = sessionId;
      const shareLink = buildShareLink(sessionId);
      const qr        = await makeQrDataUrl(shareLink);
      setSession({ sessionId, verificationCode, shareLink, qr });
      setStatus(S.WAITING);

      const rtc = new WebRTCManager({
        sendSignal: (msg) => sendRef.current(msg),

        onStateChange: (state) => {
          if (state === 'failed' || state === 'disconnected') {
            setStatus((s) => (s === S.SENDING ? S.WAITING : s));
          }
        },

        onDataChannel: (dc) => {
          setStatus(S.SENDING);

          const sender = new ChunkSender(dc, f, {
            onProgress: setProg,
            onComplete: () => setStatus(S.DONE),
            onError:    (e) => { setErrMsg(String(e)); setStatus(S.ERROR); },
          });

          dc.addEventListener('message', ({ data }) => {
            if (typeof data === 'string') {
              try { sender.onControlMessage(JSON.parse(data)); } catch { /* ignore */ }
            }
          });

          sendRef.current({
            type: 'file-info',
            fileInfo: { name: f.name, size: f.size, type: f.type },
          });

          sender.start();
        },
      });

      rtcRef.current = rtc;
      connect();
    } catch {
      setErrMsg('Could not reach the signaling server. Is it running?');
      setStatus(S.ERROR);
    }
  }, [connect]);

  // ── Reset ─────────────────────────────────────────────────────────────────

  const reset = useCallback(() => {
    rtcRef.current?.close();
    disconnect();
    setStatus(S.IDLE);
    setFile(null);
    fileRef.current = null;
    setSession(null);
    setProg(null);
    setErrMsg('');
    sidRef.current = null;
  }, [disconnect]);

  // ── Drag & drop ───────────────────────────────────────────────────────────

  const onDrop = useCallback((e) => {
    e.preventDefault();
    const f = e.dataTransfer.files[0];
    if (f) initSession(f);
  }, [initSession]);

  const onFileChange = useCallback((e) => {
    const f = e.target.files[0];
    if (f) initSession(f);
  }, [initSession]);

  const handleCopy = useCallback(async () => {
    if (!session) return;
    if (await copyText(session.shareLink)) {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, [session]);

  const pct = prog
    ? Math.min(100, Math.round((prog.chunksAcked / prog.totalChunks) * 100))
    : 0;

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <div className="min-h-screen bg-[#090910] flex flex-col items-center justify-center px-4 py-14 text-[#e8e8f0]">
      <div className="w-full max-w-[520px] space-y-5 animate-slide-up">

        {/* Logo */}
        <header className="text-center space-y-1">
          <h1 className="text-4xl font-bold tracking-tight text-[#c8fb4a]">ShareTunnel</h1>
          <p className="text-[#6b6b80] text-xs tracking-widest uppercase">
            peer-to-peer · encrypted · zero storage
          </p>
        </header>

        {/* ── Drop zone ── */}
        {status === S.IDLE && (
          <label
            className="flex flex-col items-center justify-center gap-3 min-h-52 border-2 border-dashed border-[#252538] rounded-2xl cursor-pointer hover:border-[#c8fb4a66] hover:bg-[#c8fb4a07] transition-all duration-200"
            onDrop={onDrop}
            onDragOver={(e) => e.preventDefault()}
          >
            <span className="text-5xl select-none">📂</span>
            <span className="text-[#8888a8] text-sm">
              drop a file or{' '}
              <span className="text-[#c8fb4a] underline underline-offset-2">browse</span>
            </span>
            <span className="text-[#4a4a60] text-xs">any file · any size</span>
            <input type="file" className="sr-only" onChange={onFileChange} />
          </label>
        )}

        {/* ── Main card ── */}
        {file && status !== S.IDLE && (
          <div className="bg-[#0f0f1a] border border-[#252538] rounded-2xl divide-y divide-[#1e1e30] overflow-hidden">

            {/* File row */}
            <div className="flex items-center gap-3 px-5 py-4">
              <span className="text-2xl">{fileEmoji(file.name)}</span>
              <div className="flex-1 min-w-0">
                <p className="font-semibold text-[#e8e8f8] text-sm truncate">{file.name}</p>
                <p className="text-[#7070a0] text-xs mt-0.5">{fmtBytes(file.size)}</p>
              </div>
              <StatusBadge status={status} />
            </div>

            {/* ── Share panel ── */}
            {(status === S.WAITING || status === S.CONNECTING) && session && (
              <div className="px-5 py-5 space-y-4">

                {/* Share link */}
                <div>
                  <p className="text-[#7070a0] text-[10px] uppercase tracking-widest mb-2">
                    Share link with receiver
                  </p>
                  <div className="flex gap-2">
                    <div className="flex-1 bg-[#090910] border border-[#2a2a45] rounded-xl px-3 py-2.5 text-xs text-[#a0a0c0] truncate font-mono">
                      {session.shareLink}
                    </div>
                    <button
                      onClick={handleCopy}
                      className="shrink-0 px-4 py-2 bg-[#c8fb4a] text-black text-xs font-bold rounded-xl hover:bg-[#d8ff60] active:scale-95 transition-all"
                    >
                      {copied ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                </div>

                {/* QR + verification code */}
                <div className="flex items-start gap-4">
                  {session.qr && (
                    <div className="rounded-xl overflow-hidden shrink-0 border border-[#252538]">
                      <img src={session.qr} alt="QR code" className="w-24 h-24 block" />
                    </div>
                  )}
                  <div className="space-y-2 flex-1">
                    <p className="text-[#7070a0] text-[10px] uppercase tracking-widest">
                      Verification Code
                    </p>
                    <div className="flex gap-1.5">
                      {session.verificationCode.split('').map((d, i) => (
                        <div
                          key={i}
                          className="w-10 h-12 flex items-center justify-center bg-[#090910] border-2 border-[#c8fb4a33] rounded-lg text-xl font-bold text-[#c8fb4a]"
                        >
                          {d}
                        </div>
                      ))}
                    </div>
                    <p className="text-[#6b6b80] text-[11px]">
                      Share this code verbally with the receiver
                    </p>
                  </div>
                </div>

                {/* Status line */}
                <StatusLine status={status} />
              </div>
            )}

            {/* ── Progress panel ── */}
            {(status === S.SENDING || status === S.DONE) && (
              <div className="px-5 py-5 space-y-3">
                {/* Bar */}
                <div className="space-y-2">
                  <div className="flex justify-between text-sm font-medium">
                    <span className="text-[#a0a0c0]">
                      {prog ? fmtBytes(Math.round(pct * prog.total / 100)) : '—'}
                    </span>
                    {status === S.DONE
                      ? <span className="text-[#c8fb4a]">✓ Complete</span>
                      : <span className="text-[#c8fb4a]">{pct}%</span>
                    }
                  </div>
                  <div className="h-2 bg-[#1a1a2e] rounded-full overflow-hidden">
                    <div
                      className="h-full bg-[#c8fb4a] rounded-full transition-[width] duration-300 ease-out"
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                  {prog && (
                    <p className="text-[#6b6b80] text-xs text-right">
                      of {fmtBytes(prog.total)}
                    </p>
                  )}
                </div>

                {/* Speed + ETA — only during active sending */}
                {status === S.SENDING && prog && (
                  <div className="flex justify-between text-sm text-[#a0a0c0]">
                    <span>⚡ {fmtSpeed(prog.speed)}</span>
                    <span>ETA {fmtEta(prog.eta)}</span>
                  </div>
                )}

                {/* Don't close tab warning */}
                {status === S.SENDING && (
                  <div className="flex items-center gap-2 text-yellow-400/80 text-xs bg-yellow-950/20 border border-yellow-900/30 rounded-lg px-3 py-2 mt-1">
                    <span className="shrink-0">⚠</span>
                    <span>Keep this tab open until transfer completes</span>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* ── Done banner ── */}
        {status === S.DONE && (
          <div className="bg-[#c8fb4a0d] border border-[#c8fb4a33] rounded-2xl px-5 py-4 text-[#c8fb4a] text-sm font-medium text-center animate-slide-up">
            Transfer complete — receiver has the file.
          </div>
        )}

        {/* ── Error ── */}
        {status === S.ERROR && (
          <div className="bg-red-950/30 border border-red-700/50 rounded-2xl px-5 py-4 space-y-3 animate-slide-up">
            <p className="text-red-400 text-sm font-medium">⚠ {errMsg}</p>
            <button
              onClick={reset}
              className="text-xs underline text-red-300/80 hover:text-red-200 transition-colors"
            >
              Start over
            </button>
          </div>
        )}

        {/* ── Send another ── */}
        {status === S.DONE && (
          <button
            onClick={reset}
            className="w-full py-2.5 border border-[#2a2a45] rounded-xl text-[#8888a8] text-sm hover:border-[#c8fb4a44] hover:text-[#c8fb4a] transition-all"
          >
            Send another file
          </button>
        )}

        <Footer />
      </div>
    </div>
  );
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function StatusBadge({ status }) {
  const configs = {
    [S.WAITING]:    { label: 'Waiting',    cls: 'bg-yellow-950/60 text-yellow-300 border-yellow-700/60' },
    [S.CONNECTING]: { label: 'Connecting', cls: 'bg-blue-950/60   text-blue-300   border-blue-700/60'   },
    [S.SENDING]:    { label: 'Sending',    cls: 'bg-[#c8fb4a11]   text-[#c8fb4a]  border-[#c8fb4a44]'   },
    [S.DONE]:       { label: 'Done',       cls: 'bg-green-950/60  text-green-300  border-green-700/60'   },
    [S.ERROR]:      { label: 'Error',      cls: 'bg-red-950/60    text-red-300    border-red-700/60'     },
  };
  const c = configs[status];
  if (!c) return null;
  return (
    <span className={`text-[10px] font-bold px-2.5 py-1 rounded-full border shrink-0 ${c.cls}`}>
      {c.label}
    </span>
  );
}

function StatusLine({ status }) {
  if (status === S.WAITING) return (
    <div className="flex items-center gap-2 text-[#8888a8] text-sm">
      <span className="w-2 h-2 rounded-full bg-[#c8fb4a] animate-pulse-dot shrink-0" />
      Waiting for receiver to connect…
    </div>
  );
  if (status === S.CONNECTING) return (
    <div className="flex items-center gap-2 text-[#8888a8] text-sm">
      <span className="w-2 h-2 rounded-full bg-blue-400 animate-pulse-dot shrink-0" />
      Establishing secure connection…
    </div>
  );
  return null;
}
