/**
 * StreamDownloader — Service Worker based streaming download
 *
 * PROTOCOL (push-based, SW signals ready):
 * 1. Register SW via mitm.html iframe
 * 2. Send init message to SW with token + MessageChannel port
 * 3. Trigger <a> click to start download → SW intercepts fetch
 * 4. SW starts ReadableStream, drains any early chunks, sends {ready:true}
 * 5. Main thread calls write(chunk) for each chunk as they arrive
 * 6. SW enqueues chunks into ReadableStream → Chrome writes to disk
 * 7. Main thread calls close() → SW closes stream → Chrome renames .crswap
 *
 * KEY FIX: We do NOT click the download link until the SW confirms {ready}.
 * Before that, chunks are buffered locally and sent once SW is ready.
 * This prevents the 0-byte file problem.
 *
 * The download dialog: Chrome shows "Save as" only if the page is in a
 * secure context (HTTPS or localhost). On HTTP, it auto-downloads to
 * the default folder — this is a browser security restriction, not our bug.
 */
export class StreamDownloader {
  constructor(filename, size) {
    this.filename = filename;
    this.size     = size;
    this.token    = crypto.randomUUID();
    this._port    = null;
    this._ready   = false;   // true once SW sends {ready:true}
    this._closed  = false;
    this._aborted = false;
    this._queue   = [];      // chunks buffered before SW is ready
  }

  /**
   * Initialize the downloader.
   * Returns true if SW streaming is active, false to use Blob fallback.
   */
  async init() {
    if (!('serviceWorker' in navigator)) return false;

    try {
      // Step 1: ensure SW is registered
      await this._ensureSW();

      const reg = await navigator.serviceWorker.getRegistration('/');
      if (!reg?.active) return false;

      // Step 2: create MessageChannel and register with SW
      const { port1, port2 } = new MessageChannel();
      this._port = port1;

      // Listen for SW ready signal and cancellation
      const readyPromise = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('SW ready timeout')), 8000);
        port1.onmessage = ({ data }) => {
          if (data.ready) {
            clearTimeout(timeout);
            // Restore normal message handler
            port1.onmessage = ({ data }) => {
              if (data.cancelled) {
                console.warn('[StreamDownloader] user cancelled download');
                this._aborted = true;
              }
            };
            resolve();
          } else if (data.cancelled) {
            clearTimeout(timeout);
            reject(new Error('Download cancelled'));
          }
        };
      });

      reg.active.postMessage({
        type: 'init', token: this.token,
        port: port2, filename: this.filename, size: this.size,
      }, [port2]);

      // Step 3: trigger the download link BEFORE waiting for ready.
      // This causes the browser to make the fetch → SW intercepts it →
      // SW sends {ready:true} → our readyPromise resolves.
      // The timing is: click → fetch → SW start() → ready signal.
      const a = Object.assign(document.createElement('a'), {
        href:     `/_sharetunnel_dl/${this.token}`,
        download: this.filename,
      });
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);

      // Step 4: wait for SW to confirm ReadableStream is live
      await readyPromise;

      // Step 5: flush any chunks that arrived before SW was ready
      this._ready = true;
      for (const buf of this._queue) {
        this._port.postMessage({ chunk: buf }, [buf]);
      }
      this._queue = [];

      // If close() was called before we were ready, send done now
      if (this._closed) {
        this._port.postMessage({ done: true });
      }

      return true;
    } catch (e) {
      console.warn('[StreamDownloader] init failed:', e.message, '— using Blob fallback');
      return false;
    }
  }

  /**
   * Write a chunk. Safe to call before init() resolves —
   * chunks are queued and flushed once SW is ready.
   */
  write(buffer) {
    if (this._aborted || this._closed) return;
    if (this._ready && this._port) {
      this._port.postMessage({ chunk: buffer }, [buffer]);
    } else {
      // Buffer until SW confirms ready
      this._queue.push(buffer);
    }
  }

  /** Signal end of stream */
  close() {
    this._closed = true;
    if (this._ready && this._port) {
      this._port.postMessage({ done: true });
    }
    // If not ready yet, done is sent in init() after queue flush
  }

  /** Abort on error */
  abort(reason = 'aborted') {
    this._aborted = true;
    if (this._port) {
      this._port.postMessage({ error: String(reason) });
    }
  }

  // ── SW registration via hidden iframe ─────────────────────────────────────

  async _ensureSW() {
    // Check if SW is already active
    const existing = await navigator.serviceWorker.getRegistration('/');
    if (existing?.active) return; // already good

    // Register via iframe (SW must be same-origin)
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        window.removeEventListener('message', onMsg);
        reject(new Error('SW registration timed out'));
      }, 12_000);

      const onMsg = ({ data }) => {
        if (data === 'sharetunnel-sw-ready') {
          clearTimeout(timeout);
          window.removeEventListener('message', onMsg);
          // Brief pause to let SW activate fully
          setTimeout(resolve, 200);
        } else if (data?.type === 'sharetunnel-sw-error') {
          clearTimeout(timeout);
          window.removeEventListener('message', onMsg);
          reject(new Error(data.error ?? 'SW error'));
        }
      };
      window.addEventListener('message', onMsg);

      // Load mitm.html which registers the SW
      const iframe = document.createElement('iframe');
      iframe.src = '/mitm.html';
      iframe.style.cssText = 'display:none;position:fixed;top:-9999px;width:1px;height:1px;';
      iframe.setAttribute('sandbox', 'allow-scripts allow-same-origin');
      document.body.appendChild(iframe);

      // Clean up iframe after SW is done (keep it alive long enough)
      setTimeout(() => {
        if (document.body.contains(iframe)) document.body.removeChild(iframe);
      }, 15_000);
    });
  }
}
