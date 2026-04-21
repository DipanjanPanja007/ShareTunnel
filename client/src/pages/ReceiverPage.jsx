import { useState, useCallback, useRef, useEffect } from 'react';
import { WebRTCManager }    from '../webrtc/WebRTCManager.jsx';
import { ChunkReceiver, STREAMING_SUPPORTED, SAVE_PICKER_SUPPORTED } from '../transfer/ChunkReceiver.jsx';
import { useSignaling }     from '../hooks/useSignaling.jsx';
import { fmtBytes, fmtSpeed, fmtEta, downloadBlob, fileEmoji } from '../utils.jsx';
import Footer from '../components/Footer.jsx';

const SERVER = import.meta.env.VITE_SERVER_URL ?? 'http://localhost:3001';

const S = {
  VERIFY:      'verify',
  SAVING:      'saving',      // showSaveFilePicker open
  CONNECTING:  'connecting',
  RECEIVING:   'receiving',
  DONE:        'done',
  CANCELLED:   'cancelled',
  ERROR:       'error',
};

export default function ReceiverPage({ sessionId }) {
  const [status,   setStatus]   = useState(S.VERIFY);
  const [digits,   setDigits]   = useState(['', '', '', '']);
  const [fileInfo, setFileInfo] = useState(null);
  const [prog,     setProg]     = useState(null);
  const [saved,    setSaved]    = useState(null);
  const [errMsg,   setErrMsg]   = useState('');
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);

  const inputRefs       = useRef([]);
  const rtcRef          = useRef(null);
  const recvRef         = useRef(null);
  const codeRef         = useRef('');
  const fileInfoFromWSRef = useRef(null); // fileInfo received via 'registered' WS msg

  // ── Signaling ─────────────────────────────────────────────────────────────

  const onWsMsg = useCallback(async (msg) => {
    switch (msg.type) {
      case 'registered':
        if (msg.fileInfo) {
          setFileInfo(msg.fileInfo);
          fileInfoFromWSRef.current = msg.fileInfo; // capture for picker
        }
        break;
      case 'offer':
      case 'answer':
      case 'ice-candidate':
        await rtcRef.current?.handleSignal(msg);
        break;
      case 'peer-disconnected':
        setErrMsg('Sender disconnected — transfer may resume if they reconnect.');
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
    liveSend({ type: 'register-receiver', sessionId, verificationCode: codeRef.current });
  }, [sessionId]);

  const { connect, send, disconnect } = useSignaling({ onMessage: onWsMsg, onOpen: onWsOpen });

  useEffect(() => () => {
    rtcRef.current?.close();
    recvRef.current?.destroy();
    disconnect();
  }, [disconnect]);

  // Prevent accidental tab close during transfer
  useEffect(() => {
    const active = status === S.RECEIVING || status === S.CONNECTING;
    if (!active) return;
    const h = (e) => {
      e.preventDefault();
      e.returnValue = 'Transfer in progress — leave?';
    };
    window.addEventListener('beforeunload', h);
    return () => window.removeEventListener('beforeunload', h);
  }, [status]);

  // ── Digit input ───────────────────────────────────────────────────────────

  const setDigit = useCallback((i, val) => {
    if (!/^\d?$/.test(val)) return;
    setDigits(prev => { const n = [...prev]; n[i] = val; return n; });
    if (val && i < 3) inputRefs.current[i + 1]?.focus();
  }, []);

  const onKeyDown = useCallback((i, e) => {
    if (e.key === 'Backspace' && !digits[i] && i > 0) inputRefs.current[i - 1]?.focus();
    if (e.key === 'Enter') handleVerify();
  }, [digits]); // eslint-disable-line

  const onPaste = useCallback((e) => {
    e.preventDefault();
    const t = e.clipboardData.getData('text').replace(/\D/g, '').slice(0, 4);
    if (t.length === 4) { setDigits(t.split('')); inputRefs.current[3]?.focus(); }
  }, []);

  // ── Cancel flow ───────────────────────────────────────────────────────────

  const handleCancelClick = () => setShowCancelConfirm(true);

  const handleCancelConfirm = useCallback(() => {
    setShowCancelConfirm(false);
    recvRef.current?.destroy();
    recvRef.current = null;
    rtcRef.current?.close();
    rtcRef.current = null;
    disconnect();
    setStatus(S.CANCELLED);
    setErrMsg('');
  }, [disconnect]);

  // ── Main flow: verify → pick save location → connect → receive ────────────
  //
  // CRITICAL ORDER:
  //   1. Verify code (server HTTP)
  //   2. showSaveFilePicker() ← MUST be in this click handler (user gesture)
  //   3. Only if picker confirmed: set up WebRTC and connect
  //   4. Pass the opened writable to ChunkReceiver constructor
  //
  // If showSaveFilePicker is called inside an async callback (DataChannel,
  // setTimeout, etc.) Chrome blocks it. Must be in direct click handler.

  const handleVerify = useCallback(async () => {
    const code = digits.join('');
    if (code.length !== 4) return;
    setErrMsg('');
    codeRef.current = code;

    // Step 1: Verify code with server
    let sessionFileInfo = null;
    try {
      const res  = await fetch(`${SERVER}/api/session/verify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ sessionId, verificationCode: code }),
      });
      const data = await res.json();
      if (!res.ok) { setErrMsg(data.error ?? 'Verification failed'); return; }
      sessionFileInfo = data.fileInfo;
      if (sessionFileInfo) setFileInfo(sessionFileInfo);
    } catch {
      setErrMsg('Network error — is the server reachable?');
      return;
    }

    // Step 1b: If fileInfo not yet known (sender may not have connected yet),
    // open WS and wait briefly for the 'registered' message which carries fileInfo.
    // We do this BEFORE the picker so the picker shows the real filename.
    // showSaveFilePicker() is still inside this click handler — user gesture is preserved
    // because await on a network call does NOT break the user-activation chain in Chrome.
    if (!sessionFileInfo && SAVE_PICKER_SUPPORTED) {
      fileInfoFromWSRef.current = null;
      connect(); // opens WS, onWsOpen fires register-receiver → server replies 'registered'
      // Wait up to 3 s for 'registered' message that carries fileInfo
      await new Promise(resolve => {
        const deadline = Date.now() + 3000;
        const t = setInterval(() => {
          if (fileInfoFromWSRef.current || Date.now() >= deadline) {
            clearInterval(t);
            resolve();
          }
        }, 50);
      });
      if (fileInfoFromWSRef.current) {
        sessionFileInfo = fileInfoFromWSRef.current;
        setFileInfo(sessionFileInfo);
      }
    }

    // Step 2: Open save location picker — INSIDE this click handler (user gesture).
    // At this point sessionFileInfo has the real filename if sender was connected.
    let writable = null; // null = SW streaming or Blob fallback

    if (SAVE_PICKER_SUPPORTED) {
      setStatus(S.SAVING);
      try {
        const suggestedName = sessionFileInfo?.name ?? '';
        const mimeType      = sessionFileInfo?.type ?? 'application/octet-stream';
        // Extract extension: only if filename has a dot that isn't the first char
        const dotIdx = suggestedName.lastIndexOf('.');
        const ext    = (dotIdx > 0 && dotIdx < suggestedName.length - 1)
                        ? suggestedName.slice(dotIdx) // e.g. ".rar", ".mp4", ".pdf"
                        : '';

        const pickerOpts = { types: [{ description: 'File', accept: { [mimeType]: ext ? [ext] : [] } }] };
        if (suggestedName) pickerOpts.suggestedName = suggestedName;

        const handle = await window.showSaveFilePicker(pickerOpts);
        writable = await handle.createWritable();
        console.log('[receiver] Save location chosen:', handle.name);
      } catch (e) {
        if (e.name === 'AbortError') {
          // User pressed Cancel in the picker → abort entire transfer
          setStatus(S.CANCELLED);
          return;
        }
        // Other error → fall back to SW stream
        console.warn('[receiver] showSaveFilePicker failed, fallback:', e.message);
        writable = null;
      }
    }
    // If SAVE_PICKER_SUPPORTED is false: writable stays null → SW or Blob

    // Step 3: Set up WebRTC (only reached if user did not cancel picker)
    setStatus(S.CONNECTING);

    try {
      const rtc = new WebRTCManager({
        sendSignal: send,
        onStateChange: (state) => {
          if (state === 'connected') setStatus(S.RECEIVING);
          if (state === 'failed')    setErrMsg('Connection failed. Check your network.');
        },
        onDataChannel: (dc) => {
          setStatus(S.RECEIVING);

          // Step 4: Pass pre-opened writable to ChunkReceiver
          // ChunkReceiver writes each chunk immediately and discards it
          const recv = new ChunkReceiver(dc, {
            writable, // null if picker not available/declined → SW or Blob

            onProgress: (p) => {
              setProg(p);
              if (!fileInfo && p.total) {
                setFileInfo(fi => fi ?? { name: 'Incoming file…', size: p.total });
              }
            },

            onComplete: (blob, name, isStreamed) => {
              setStatus(S.DONE);
              if (isStreamed) {
                setSaved({ blob: null, name, isStreamed: true });
              } else {
                setSaved({ blob, name, isStreamed: false });
                downloadBlob(blob, name);
              }
            },

            onCancelled: () => setStatus(S.CANCELLED),

            onError: (e) => { setErrMsg(String(e)); setStatus(S.ERROR); },
          });

          recvRef.current = recv;
        },
      });

      rtcRef.current = rtc;
      rtc.initReceiver();
      connect();
    } catch {
      setErrMsg('Failed to start connection.');
      setStatus(S.ERROR);
    }
  }, [digits, sessionId, connect, send, fileInfo]);

  const pct = prog
    ? Math.min(100, Math.round((prog.chunksReceived / prog.totalChunks) * 100))
    : 0;

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <div className="min-h-screen bg-[#090910] flex flex-col items-center justify-center px-4 py-14 text-[#e8e8f0]">
      <div className="w-full max-w-[420px] space-y-5 animate-slide-up">

        <header className="text-center space-y-1">
          <h1 className="text-4xl font-bold tracking-tight text-[#c8fb4a]">ShareTunnel</h1>
          <p className="text-[#6b6b80] text-xs tracking-widest uppercase">receiving a file</p>
        </header>

        {/* Capability notice */}
        {status === S.VERIFY && (
          <div className={`rounded-xl px-4 py-3 text-xs leading-relaxed border ${
            SAVE_PICKER_SUPPORTED
              ? 'bg-[#c8fb4a08] border-[#c8fb4a22] text-[#8888a8]'
              : STREAMING_SUPPORTED
              ? 'bg-[#c8fb4a08] border-[#c8fb4a22] text-[#8888a8]'
              : 'bg-yellow-950/30 border-yellow-700/40 text-yellow-300/90'
          }`}>
            {SAVE_PICKER_SUPPORTED
              ? '✓ You will choose where to save the file before transfer starts'
              : STREAMING_SUPPORTED
              ? '✓ Streaming mode — file saved to Downloads folder'
              : '⚠ Fallback mode — file must fit in RAM. Use Chrome for large files.'}
          </div>
        )}

        {/* ── Verify ── */}
        {status === S.VERIFY && (
          <div className="bg-[#0f0f1a] border border-[#252538] rounded-2xl p-6 space-y-5">
            <div>
              <p className="text-[#7070a0] text-[10px] uppercase tracking-widest mb-1">Session ID</p>
              <p className="text-[#4a4a6a] text-xs font-mono truncate">{sessionId}</p>
            </div>
            <div className="space-y-4">
              <p className="text-[#c8c8e0] text-sm font-medium">Enter the 4-digit code from the sender</p>
              <div className="flex gap-2 justify-center">
                {digits.map((d, i) => (
                  <input
                    key={i}
                    ref={el => { inputRefs.current[i] = el; }}
                    type="text" inputMode="numeric" maxLength={1}
                    value={d} autoFocus={i === 0}
                    onChange={e => setDigit(i, e.target.value)}
                    onKeyDown={e => onKeyDown(i, e)}
                    onPaste={onPaste}
                    className="w-14 h-16 text-center text-2xl font-bold bg-[#090910] border-2 border-[#2a2a45] rounded-xl text-[#c8fb4a] focus:outline-none focus:border-[#c8fb4a88] transition-colors caret-transparent"
                  />
                ))}
              </div>
              {errMsg && <p className="text-red-400 text-sm text-center font-medium">{errMsg}</p>}
              <button
                onClick={handleVerify}
                disabled={digits.join('').length !== 4}
                className="w-full py-3 bg-[#c8fb4a] text-black text-sm font-bold rounded-xl hover:bg-[#d8ff60] disabled:opacity-25 disabled:cursor-not-allowed active:scale-[0.98] transition-all"
              >
                Connect &amp; Receive
              </button>
            </div>
          </div>
        )}

        {/* ── Saving (picker open) ── */}
        {status === S.SAVING && (
          <div className="bg-[#0f0f1a] border border-[#252538] rounded-2xl p-10 text-center space-y-3">
            <div className="text-3xl">📁</div>
            <p className="text-[#c8c8e0] text-sm font-medium">Choose where to save the file…</p>
            <p className="text-[#6b6b80] text-xs">The save dialog should be open in your browser.</p>
          </div>
        )}

        {/* ── Connecting ── */}
        {status === S.CONNECTING && (
          <div className="bg-[#0f0f1a] border border-[#252538] rounded-2xl p-10 text-center space-y-3">
            <div className="text-3xl animate-spin">⟳</div>
            <p className="text-[#c8c8e0] text-sm font-medium">Establishing encrypted connection…</p>
            <p className="text-[#6b6b80] text-xs">WebRTC · DTLS-SRTP end-to-end encryption</p>
          </div>
        )}

        {/* ── Receiving ── */}
        {status === S.RECEIVING && (
          <div className="bg-[#0f0f1a] border border-[#252538] rounded-2xl overflow-hidden divide-y divide-[#1e1e30]">
            {fileInfo && (
              <div className="flex items-center gap-3 px-5 py-4">
                <span className="text-3xl">{fileEmoji(fileInfo.name ?? '')}</span>
                <div className="min-w-0 flex-1">
                  <p className="font-semibold text-[#e8e8f8] text-sm truncate">
                    {fileInfo.name ?? 'Incoming file…'}
                  </p>
                  {fileInfo.size > 0 && (
                    <p className="text-[#7070a0] text-xs mt-0.5">{fmtBytes(fileInfo.size)}</p>
                  )}
                </div>
                <span className="text-[10px] font-semibold px-2.5 py-1 rounded-full border bg-[#c8fb4a11] text-[#c8fb4a] border-[#c8fb4a44] shrink-0">
                  Receiving
                </span>
              </div>
            )}
            <div className="px-5 py-5 space-y-3">
              <div className="space-y-2">
                <div className="flex justify-between text-sm font-medium">
                  <span className="text-[#a0a0c0]">{prog ? fmtBytes(prog.received) : 'Waiting for data…'}</span>
                  <span className="text-[#c8fb4a]">{pct}%</span>
                </div>
                <div className="h-2 bg-[#1a1a2e] rounded-full overflow-hidden">
                  <div
                    className="h-full bg-[#c8fb4a] rounded-full transition-[width] duration-300 ease-out"
                    style={{ width: `${pct}%` }}
                  />
                </div>
                {prog && prog.total > 0 && (
                  <p className="text-[#6b6b80] text-xs text-right">of {fmtBytes(prog.total)}</p>
                )}
              </div>

              {prog && pct > 0 && (
                <div className="flex justify-between text-sm">
                  <span className="text-[#a0a0c0]">⚡ {fmtSpeed(prog.speed)}</span>
                  <span className="text-[#a0a0c0]">ETA {fmtEta(prog.eta)}</span>
                </div>
              )}

              {/* Don't-close warning + cancel button */}
              <div className="flex items-center justify-between gap-3 bg-yellow-950/20 border border-yellow-900/30 rounded-lg px-3 py-2">
                <div className="flex items-center gap-2 text-yellow-400/80 text-xs">
                  <span className="shrink-0">⚠</span>
                  <span>Keep this tab open until saved</span>
                </div>
                <button
                  onClick={handleCancelClick}
                  className="text-xs text-red-400/70 hover:text-red-400 underline shrink-0 transition-colors"
                >
                  Cancel
                </button>
              </div>

              {errMsg && (
                <p className="text-yellow-400 text-xs bg-yellow-950/30 border border-yellow-900/40 rounded-lg px-3 py-2">
                  ⚠ {errMsg}
                </p>
              )}
            </div>
          </div>
        )}

        {/* ── Done ── */}
        {status === S.DONE && saved && (
          <div className="bg-[#0f0f1a] border border-[#c8fb4a33] rounded-2xl p-6 space-y-4 text-center animate-slide-up">
            <div className="text-5xl">{fileEmoji(saved.name)}</div>
            <div className="space-y-1">
              <p className="text-[#c8fb4a] text-base font-bold">
                {saved.isStreamed ? 'File saved!' : 'Download complete!'}
              </p>
              <p className="text-[#a0a0c0] text-sm truncate px-4">{saved.name}</p>
            </div>
            {!saved.isStreamed && saved.blob && (
              <button
                onClick={() => downloadBlob(saved.blob, saved.name)}
                className="w-full py-3 bg-[#c8fb4a] text-black text-sm font-bold rounded-xl hover:bg-[#d8ff60] active:scale-[0.98] transition-all"
              >
                Download Again
              </button>
            )}
          </div>
        )}

        {/* ── Cancelled ── */}
        {status === S.CANCELLED && (
          <div className="bg-[#0f0f1a] border border-[#252538] rounded-2xl px-5 py-6 text-center space-y-3 animate-slide-up">
            <p className="text-[#6b6b80] text-2xl">✕</p>
            <p className="text-[#a0a0c0] text-sm font-medium">Transfer cancelled</p>
            <p className="text-[#6b6b80] text-xs">No file was downloaded.</p>
            <button
              onClick={() => { setStatus(S.VERIFY); setDigits(['','','','']); codeRef.current = ''; setProg(null); setErrMsg(''); }}
              className="text-xs underline text-[#7070a0] hover:text-[#c8fb4a] transition-colors"
            >
              Start over
            </button>
          </div>
        )}

        {/* ── Error ── */}
        {status === S.ERROR && (
          <div className="bg-red-950/30 border border-red-700/50 rounded-2xl px-5 py-4 space-y-3 animate-slide-up">
            <p className="text-red-400 text-sm font-medium">⚠ {errMsg}</p>
            <button
              onClick={() => {
                recvRef.current?.destroy(); recvRef.current = null;
                rtcRef.current?.close();    rtcRef.current = null;
                disconnect();
                setStatus(S.VERIFY); setErrMsg('');
                setDigits(['','','','']); codeRef.current = '';
              }}
              className="text-xs underline text-red-300/80 hover:text-red-200 transition-colors"
            >
              Try again
            </button>
          </div>
        )}

        <Footer />
      </div>

      {/* ── Cancel confirmation popup ── */}
      {showCancelConfirm && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 px-4">
          <div className="bg-[#0f0f1a] border border-[#252538] rounded-2xl p-6 w-full max-w-sm space-y-4 animate-slide-up">
            <div className="space-y-1">
              <h2 className="text-[#e8e8f8] text-base font-bold">Cancel transfer?</h2>
              <p className="text-[#7070a0] text-xs">
                The file will not be saved. This cannot be undone.
              </p>
            </div>
            <div className="flex gap-3">
              <button
                onClick={() => setShowCancelConfirm(false)}
                className="flex-1 py-2.5 border border-[#2a2a45] rounded-xl text-[#8888a8] text-sm hover:border-[#c8fb4a33] transition-all"
              >
                Keep going
              </button>
              <button
                onClick={handleCancelConfirm}
                className="flex-1 py-2.5 bg-red-600 text-white text-sm font-bold rounded-xl hover:bg-red-500 active:scale-[0.98] transition-all"
              >
                Cancel transfer
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
