/**
 * useSignaling
 * WebSocket hook for the ShareTunnel signaling server.
 *
 * onOpen(send) — receives the send fn directly so the caller
 * can fire registration messages the moment the socket opens.
 * No fragile setTimeout needed.
 */

import { useRef, useCallback, useEffect } from 'react';

const RAW_URL = import.meta.env.VITE_SERVER_URL ?? 'http://localhost:3001';
const WS_URL  = RAW_URL.replace(/^http/, 'ws');

export function useSignaling({ onMessage, onOpen, onClose } = {}) {
  const wsRef      = useRef(null);
  const onMsgRef   = useRef(onMessage);
  const onOpenRef  = useRef(onOpen);
  const onCloseRef = useRef(onClose);

  useEffect(() => { onMsgRef.current   = onMessage; }, [onMessage]);
  useEffect(() => { onOpenRef.current  = onOpen;    }, [onOpen]);
  useEffect(() => { onCloseRef.current = onClose;   }, [onClose]);

  const send = useCallback((msg) => {
    const ws = wsRef.current;
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    } else {
      console.warn('[ws] send skipped — socket not open');
    }
  }, []);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('[ws] connected');
      // Pass send so caller can register immediately without setTimeout
      onOpenRef.current?.(send);
    };

    ws.onmessage = ({ data }) => {
      try { onMsgRef.current?.(JSON.parse(data)); } catch { /* ignore */ }
    };

    ws.onclose = () => {
      console.log('[ws] disconnected');
      onCloseRef.current?.();
    };

    ws.onerror = (e) => console.error('[ws] error', e);
  }, [send]);

  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
  }, []);

  useEffect(() => () => disconnect(), [disconnect]);

  return { connect, send, disconnect };
}
