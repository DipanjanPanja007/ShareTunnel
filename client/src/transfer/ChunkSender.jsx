/**
 * ChunkSender — flow-controlled WebRTC DataChannel sender
 *
 * ISSUES FIXED IN THIS VERSION:
 * ─────────────────────────────────────────────────────────────────────────
 * 1. DEADLOCK: _waitForBufferDrain had no timeout → waited forever if
 *    'bufferedamountlow' never fired (browser quirk / race condition).
 *    FIX: 200ms timeout fallback always unblocks the wait.
 *
 * 2. RACE CONDITION: bufferedamountlow could fire between the outer
 *    while-check and addEventListener, causing the listener to miss it.
 *    FIX: Check bufferedAmount AGAIN inside the Promise before attaching
 *    the listener — if already drained, resolve immediately.
 *
 * 3. SEND QUEUE OVERFLOW: inner pump loop sent WINDOW_SIZE chunks without
 *    checking bufferedAmount between each send.
 *    FIX: Every dc.send() goes through _safeSend() which checks first.
 *
 * 4. MISSING retransmit fire-and-forget: _onMissing called safeSend().then()
 *    without await — if safeSend stalled, _armTimer never re-armed.
 *    FIX: _onMissing now kicks off retransmit in a proper async context.
 *
 * 5. CPU waste: outer pump slept only 2ms waiting for window to open.
 *    FIX: Progressive backoff up to 50ms.
 *
 * THRESHOLDS (tuned per spec):
 *   CHUNK_SIZE  = 64 KB
 *   WINDOW_SIZE = 16       (max in-flight chunks)
 *   BP_THRESHOLD            = 2 MB  (pause sending when queue reaches this)
 *   bufferedAmountLowThreshold = 1 MB  (resume when queue drains to this)
 *   DRAIN_TIMEOUT           = 200ms (fallback if bufferedamountlow doesn't fire)
 */

const CHUNK_SIZE              = 64 * 1024;
const WINDOW_SIZE             = 16;
const ACK_TIMEOUT             = 5_000;
const BP_THRESHOLD            = 2 * 1024 * 1024; // 2 MB — pause threshold
const BUFFERED_AMOUNT_LOW     = 1 * 1024 * 1024; // 1 MB — resume threshold (< BP)
const DRAIN_TIMEOUT           = 200;              // ms — fallback if event never fires
const HASH_LIMIT              = 512 * 1024 * 1024;

export class ChunkSender {
  constructor(dc, file, { onProgress, onComplete, onError } = {}) {
    this.dc          = dc;
    this.file        = file;
    this.onProgress  = onProgress;
    this.onComplete  = onComplete;
    this.onError     = onError;

    this.fileId      = crypto.randomUUID();
    this.totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    this.frameCache  = new Map(); // seq → {frame, dataLen} — freed on ACK
    this.acked       = new Set(); // pruned: max WINDOW_SIZE entries
    this.timers      = new Map();

    this.nextSeq     = 0;
    this.windowBase  = 0;
    this.aborted     = false;

    this._datBytesSent = 0;
    this._lastStatAt   = Date.now();
    this._lastDatBytes = 0;

    // CRITICAL: set threshold so browser knows when to fire bufferedamountlow
    // Must be LESS than BP_THRESHOLD or the event fires after we already paused
    this.dc.bufferedAmountLowThreshold = BUFFERED_AMOUNT_LOW;
  }

  // ── Public API ────────────────────────────────────────────────────────────

  async start() {
    console.log(`[sender] "${this.file.name}" — ${this.totalChunks} chunks`);
    this._hashPromise = this._sha256();
    this._sendJSON({
      type: 'file-header', fileId: this.fileId,
      fileName: this.file.name, fileSize: this.file.size,
      mimeType: this.file.type || 'application/octet-stream',
      totalChunks: this.totalChunks,
    });
    await this._pump();
  }

  onControlMessage(msg) {
    if (msg.type === 'ACK')     this._onAck(msg.seq);
    if (msg.type === 'MISSING') this._onMissing(msg.seqs);
    if (msg.type === 'DONE')    this.aborted = true;
  }

  abort() {
    this.aborted = true;
    for (const id of this.timers.values()) clearTimeout(id);
    this.timers.clear();
  }

  // ── JSON send (headers/footers) — no backpressure needed ─────────────────
  _sendJSON(obj) {
    try {
      if (this.dc.readyState !== 'open') return false;
      this.dc.send(JSON.stringify(obj));
      return true;
    } catch (e) {
      console.error('[sender] JSON send error:', e.message);
      return false;
    }
  }

  // ── _waitForDrain — FIXED: timeout fallback + race-condition guard ────────
  //
  // FIX 1 (deadlock): Added DRAIN_TIMEOUT (200ms) so if 'bufferedamountlow'
  //   never fires, we unblock and retry. This prevents the permanent stall.
  //
  // FIX 2 (race condition): Check bufferedAmount INSIDE the Promise constructor,
  //   AFTER the timeout is set but BEFORE addEventListener. This covers the gap
  //   where the event could fire between the outer while-check and the listener
  //   being attached.
  //
  _waitForDrain() {
    return new Promise((resolve) => {
      // Fast path: already drained (check before setting up listeners)
      if (this.dc.bufferedAmount <= BUFFERED_AMOUNT_LOW ||
          this.dc.readyState !== 'open') {
        resolve();
        return;
      }

      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        this.dc.removeEventListener('bufferedamountlow', onLow);
        this.dc.removeEventListener('close',             onClose);
        this.dc.removeEventListener('error',             onClose);
        resolve();
      };

      // FIX 1: timeout fallback — guarantees we never wait forever
      const timer = setTimeout(finish, DRAIN_TIMEOUT);

      // FIX 2: attach listener BEFORE the second bufferedAmount check
      // so there's no gap where the event can slip through
      const onLow   = finish;
      const onClose = finish;
      this.dc.addEventListener('bufferedamountlow', onLow,   { once: true });
      this.dc.addEventListener('close',             onClose, { once: true });
      this.dc.addEventListener('error',             onClose, { once: true });

      // FIX 2: Re-check AFTER attaching listener.
      // If it drained between the first check and addEventListener, we catch it here.
      if (this.dc.bufferedAmount <= BUFFERED_AMOUNT_LOW ||
          this.dc.readyState !== 'open') {
        finish();
      }
    });
  }

  // ── _safeSend — guards every binary send against overflow ─────────────────
  async _safeSend(frame) {
    if (this.dc.readyState !== 'open' || this.aborted) return false;

    // Wait until queue is below threshold before sending
    while (this.dc.bufferedAmount > BP_THRESHOLD) {
      await this._waitForDrain();
      if (this.dc.readyState !== 'open' || this.aborted) return false;
    }

    try {
      this.dc.send(frame);
      return true;
    } catch (e) {
      console.error('[sender] binary send error:', e.message);
      return false;
    }
  }

  // ── _pump — sliding window with per-chunk backpressure ───────────────────
  async _pump() {
    if (this.totalChunks === 0) { await this._finalize(); return; }

    let idleMs = 2; // progressive backoff when window is full

    while (!this.aborted) {
      let sent = 0;

      // Send as many chunks as the window allows — each waits if buffer full
      while (!this.aborted &&
             this.nextSeq < this.totalChunks &&
             this.nextSeq - this.windowBase < WINDOW_SIZE) {
        await this._sendChunk(this.nextSeq++);
        sent++;
        // Yield to event loop between chunks so ACKs and UI can process
        await this._sleep(0);
      }

      // Done?
      if (!this.aborted && this.windowBase >= this.totalChunks) {
        await this._finalize();
        return;
      }

      if (sent > 0) {
        idleMs = 2; // reset backoff — we're making progress
      } else {
        // Window is full — wait for ACKs with progressive backoff
        // (ACKs arrive as DataChannel messages, not from here)
        await this._sleep(idleMs);
        idleMs = Math.min(idleMs * 1.5, 50); // back off up to 50ms max
      }
    }
  }

  // ── _sendChunk ────────────────────────────────────────────────────────────
  async _sendChunk(seq) {
    const start     = seq * CHUNK_SIZE;
    const data      = await this.file.slice(start, start + CHUNK_SIZE).arrayBuffer();
    const metaBytes = new TextEncoder().encode(
      JSON.stringify({ type: 'chunk', fileId: this.fileId, seq, totalChunks: this.totalChunks })
    );
    const frame = new ArrayBuffer(4 + metaBytes.byteLength + data.byteLength);
    const dv    = new DataView(frame);
    dv.setUint32(0, metaBytes.byteLength, true);
    new Uint8Array(frame, 4, metaBytes.byteLength).set(metaBytes);
    new Uint8Array(frame, 4 + metaBytes.byteLength).set(new Uint8Array(data));

    this.frameCache.set(seq, { frame, dataLen: data.byteLength });

    if (await this._safeSend(frame)) {
      this._datBytesSent += data.byteLength;
      this._reportProgress();
    }
    this._armTimer(seq);
  }

  // ── Retransmit timer ──────────────────────────────────────────────────────
  _armTimer(seq) {
    this._clearTimer(seq);
    const id = setTimeout(() => {
      if (this.acked.has(seq) || this.windowBase > seq || this.aborted) return;
      console.warn(`[sender] retransmit seq=${seq}`);
      // FIX 4: proper async context — don't fire-and-forget
      this._retransmit(seq);
    }, ACK_TIMEOUT);
    this.timers.set(seq, id);
  }

  async _retransmit(seq) {
    if (this.aborted || this.acked.has(seq) || this.windowBase > seq) return;
    const cached = this.frameCache.get(seq);
    if (cached) {
      await this._safeSend(cached.frame);
    } else {
      await this._sendChunk(seq); // re-read 64 KB from file
    }
    // Re-arm only if still needed
    if (!this.aborted && !this.acked.has(seq) && this.windowBase <= seq) {
      this._armTimer(seq);
    }
  }

  _clearTimer(seq) {
    const id = this.timers.get(seq);
    if (id !== undefined) { clearTimeout(id); this.timers.delete(seq); }
  }

  // ── ACK handling ──────────────────────────────────────────────────────────
  _onAck(seq) {
    if (this.acked.has(seq)) return;
    if (seq < this.windowBase) return; // already advanced past this
    this.acked.add(seq);
    this._clearTimer(seq);
    this.frameCache.delete(seq);
    // Advance window and PRUNE acked Set (keeps it bounded at ≤ WINDOW_SIZE)
    while (this.acked.has(this.windowBase)) {
      this.acked.delete(this.windowBase);
      this.windowBase++;
    }
  }

  // ── MISSING retransmit ────────────────────────────────────────────────────
  _onMissing(seqs) {
    if (!Array.isArray(seqs)) return;
    for (const seq of seqs) {
      if (typeof seq !== 'number' || seq < 0 || seq >= this.totalChunks) continue;
      if (this.acked.has(seq) || this.windowBase > seq) continue; // already delivered
      // FIX 4: use proper async retransmit (not fire-and-forget .then())
      this._retransmit(seq);
    }
  }

  // ── Finalize ──────────────────────────────────────────────────────────────
  async _finalize() {
    this.onProgress?.({
      sent: this.file.size, total: this.file.size,
      chunksAcked: this.totalChunks, totalChunks: this.totalChunks,
      speed: 0, eta: 0,
    });
    const hash = await this._hashPromise;
    this._sendJSON({ type: 'file-footer', fileId: this.fileId, sha256: hash });
    this.onComplete?.();
  }

  // ── SHA-256 (reads file in 4 MB slices, not full load) ───────────────────
  async _sha256() {
    if (this.file.size === 0)
      return 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
    if (this.file.size > HASH_LIMIT) {
      console.warn('[sender] >512 MB — skipping SHA-256');
      return '';
    }
    try {
      const SLICE = 4 * 1024 * 1024;
      const parts = [];
      let   off   = 0;
      while (off < this.file.size) {
        const buf = await this.file.slice(off, off + SLICE).arrayBuffer();
        parts.push(new Uint8Array(buf));
        off += buf.byteLength;
        await this._sleep(0); // yield between slices
      }
      const total = parts.reduce((s, p) => s + p.byteLength, 0);
      const all   = new Uint8Array(total);
      let   pos   = 0;
      for (const p of parts) { all.set(p, pos); pos += p.byteLength; }
      const h = await crypto.subtle.digest('SHA-256', all);
      return [...new Uint8Array(h)].map(b => b.toString(16).padStart(2, '0')).join('');
    } catch (e) {
      console.error('[sender] hash error:', e);
      return '';
    }
  }

  // ── Progress ──────────────────────────────────────────────────────────────
  _reportProgress() {
    const now = Date.now();
    const dt  = now - this._lastStatAt;
    if (dt < 150) return;
    const delta = this._datBytesSent - this._lastDatBytes;
    const speed = (delta / dt) * 1000;
    const sent  = Math.min(this._datBytesSent, this.file.size);
    const eta   = speed > 0 ? Math.max(0, this.file.size - sent) / speed : Infinity;
    this.onProgress?.({
      sent, total: this.file.size,
      chunksAcked: this.windowBase,
      totalChunks: this.totalChunks,
      speed, eta,
    });
    this._lastStatAt   = now;
    this._lastDatBytes = this._datBytesSent;
  }

  _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
}
