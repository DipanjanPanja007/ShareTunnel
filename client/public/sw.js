/**
 * ShareTunnel Service Worker — streaming download handler
 *
 * DESIGN: push-based with a bounded queue.
 * Main thread pushes chunks as they arrive from the DataChannel.
 * The ReadableStream queue has highWaterMark:2 so Chrome always has
 * something to write — no stalls, no 0-byte file problem.
 *
 * This mirrors exactly how a normal HTTP server works:
 * data arrives from the network → Chrome buffers a little → writes to disk.
 *
 * The .crswap rename is fast because Chrome writes chunks to disk
 * incrementally as they arrive in the ReadableStream, instead of
 * buffering everything and flushing at the end.
 *
 * MEMORY SAFETY (FIX):
 * slot.waiters is bounded by MAX_WAITERS. Under the STREAM_READY backpressure
 * protocol the sender does not pump until the receiver confirms the streaming
 * path is live (which only happens AFTER init() resolves, i.e. after the SW
 * has confirmed {ready:true}). So waiters should contain at most 0–1 items
 * in normal operation. The cap is a hard safety net against protocol bugs.
 *
 * If the cap is exceeded the SW signals an error back to the main thread so
 * the transfer is aborted rather than letting RAM grow unboundedly.
 */

// FIX: Maximum chunks buffered in slot.waiters before the ReadableStream
// controller is assigned. With the STREAM_READY protocol, this window is
// effectively 0 in normal operation. The cap guards against rogue senders.
const MAX_WAITERS = 64; // 64 × 64 KB = 4 MB absolute ceiling

const MAP = new Map(); // token → { port, filename, size, controller, waiters }

self.addEventListener('install',  () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

// Main thread sends chunks here via the MessageChannel port
self.addEventListener('message', (e) => {
  if (!e.data) return;

  if (e.data.type === 'init') {
    const { token, port, filename, size } = e.data;
    const slot = { port, filename, size, controller: null, waiters: [] };
    MAP.set(token, slot);

    port.onmessage = ({ data }) => {
      const s = MAP.get(token);
      if (!s) return;

      if (data.chunk) {
        const chunk = data.chunk instanceof ArrayBuffer
          ? new Uint8Array(data.chunk)
          : data.chunk;

        if (s.controller) {
          // ReadableStream is live — enqueue directly
          s.controller.enqueue(chunk);
        } else {
          // FIX: Hard cap on waiters — prevents O(file_size) buffering in SW.
          // Under the STREAM_READY protocol this branch should rarely execute.
          if (s.waiters.length >= MAX_WAITERS) {
            console.error(
              `[SW] waiters overflow (cap=${MAX_WAITERS}) for token=${token} — aborting stream`
            );
            // Signal error back to main thread so the transfer is aborted
            s.port.postMessage({ error: 'sw_waiters_overflow' });
            // Clean up the slot to release memory
            MAP.delete(token);
            return;
          }
          // Stream not started yet — buffer until it starts
          s.waiters.push({ chunk });
        }
      } else if (data.done) {
        if (s.controller) {
          s.controller.close();
        } else {
          s.waiters.push({ done: true });
        }
        MAP.delete(token);
      } else if (data.error) {
        if (s.controller) {
          s.controller.error(new Error(data.error));
        }
        // FIX: Release waiters on error to free memory
        s.waiters = [];
        MAP.delete(token);
      }
    };
  }
});

// Intercept the fake download fetch
self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (!url.pathname.startsWith('/_sharetunnel_dl/')) return;

  const token = url.pathname.slice('/_sharetunnel_dl/'.length);
  const slot  = MAP.get(token);
  if (!slot) return;

  const { filename, size } = slot;

  const stream = new ReadableStream({
    start(controller) {
      slot.controller = controller;

      // Drain anything buffered before stream started
      for (const item of slot.waiters) {
        if (item.chunk)    controller.enqueue(item.chunk);
        else if (item.done) {
          // FIX: Release waiters array before closing stream
          slot.waiters = [];
          controller.close();
          MAP.delete(token);
          return;
        }
      }
      // FIX: Release waiters array after drain — they're now in the ReadableStream
      slot.waiters = [];

      // Tell main thread we're ready — it can now start sending chunks
      slot.port.postMessage({ ready: true });
    },
    cancel() {
      slot.port.postMessage({ cancelled: true });
      // FIX: Release waiters on cancel
      slot.waiters = [];
      MAP.delete(token);
    },
  // highWaterMark:2 — keep 2 chunks buffered in the ReadableStream queue.
  // This prevents stalls: while Chrome writes chunk N to disk, chunk N+1
  // is already queued, so Chrome never has to wait for the next chunk.
  }, new CountQueuingStrategy({ highWaterMark: 2 }));

  e.respondWith(new Response(stream, {
    headers: new Headers({
      'Content-Type':        'application/octet-stream; charset=utf-8',
      'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(filename)}`,
      'Content-Length':      String(size),
      'X-Content-Type-Options': 'nosniff',
    }),
    status: 200,
  }));
});
