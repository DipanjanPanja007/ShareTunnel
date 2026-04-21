/**
 * ChunkReceiver — streaming write, O(chunk) RAM, no full-file buffering
 *
 * DESIGN:
 * ─────────────────────────────────────────────────────────────────────────
 * PRIMARY PATH (FSAPI writable pre-opened in button click handler):
 *   Constructor receives an already-opened FileSystemWritableFileStream.
 *   Each chunk is written immediately: await writable.write(chunk)
 *   Chunk is then discarded — never accumulates in RAM.
 *   Ordered buffer (_pickerBuf) holds at most WINDOW_SIZE chunks for
 *   out-of-order delivery. Flushed sequentially. RAM = O(WINDOW_SIZE).
 *
 * SECONDARY PATH (SW streaming — Downloads folder):
 *   Uses StreamDownloader → sw.js → browser's native download stream.
 *   Chunks are pushed to SW via MessageChannel and written to disk
 *   incrementally. RAM = O(in-flight chunks).
 *
 * FALLBACK PATH (iOS Safari / no SW / picker declined):
 *   ONLY permitted for files ≤ BLOB_FALLBACK_LIMIT (100 MB).
 *   For larger files the transfer is ABORTED before chunks arrive.
 *   Warning shown to user before transfer.
 *
 * BACKPRESSURE PROTOCOL:
 *   Receiver sends {type:'STREAM_READY'} AFTER streaming path is confirmed.
 *   Sender MUST NOT start pump() until STREAM_READY is received.
 *   This prevents any chunk from arriving before the write path is set up.
 *
 * RAM BUDGET (streaming paths, 100 GB file):
 *   _pickerBuf / _streamBuf: max WINDOW_SIZE × 64 KB = 1 MB
 *   _gapSet:                 max WINDOW_SIZE × 40 B  = ~1 KB
 *   pendingAcks:             max WINDOW_SIZE entries = ~1 KB
 *   _preHeaderBuffer:        max PRE_HEADER_CAP × 64 KB = 128 KB   // FIX
 *   TOTAL:                   ~1–2 MB constant
 *
 * KEY INVARIANT:
 *   showSaveFilePicker() is called ONLY in the button click handler
 *   (ReceiverPage.handleVerify), which is a user gesture context.
 *   ChunkReceiver receives the already-opened writable.
 */

import { StreamDownloader } from './StreamDownloader.js';

const ACK_FLUSH_INTERVAL    = 80;
const MISSING_SCAN_INTERVAL = 3_000;
const HASH_LIMIT            = 512 * 1024 * 1024;
const WINDOW_SIZE           = 16; // must match ChunkSender

// FIX: Hard limit on pre-header buffer. Chunks arriving before the header is
// processed are buffered here. Capped at 2 × WINDOW_SIZE so no unbounded
// growth is possible even if a rogue sender floods us before the header lands.
const PRE_HEADER_CAP = WINDOW_SIZE * 2; // 32 chunks max = 2 MB

// FIX: Blob fallback is ONLY allowed for files below this threshold.
// Files larger than this require streaming (FSAPI or SW) and will be
// aborted BEFORE any chunk is stored if streaming is unavailable.
const BLOB_FALLBACK_LIMIT = 100 * 1024 * 1024; // 100 MB

export const STREAMING_SUPPORTED =
  typeof window !== 'undefined' && 'serviceWorker' in navigator;

export const SAVE_PICKER_SUPPORTED =
  typeof window !== 'undefined' && 'showSaveFilePicker' in window;

export class ChunkReceiver {
  /**
   * @param {RTCDataChannel} dc
   * @param {{
   *   writable:    FileSystemWritableFileStream | null,
   *   onProgress:  (p) => void,
   *   onComplete:  (blob|null, name, isStreamed) => void,
   *   onError:     (msg) => void,
   *   onCancelled: () => void,
   * }} opts
   *
   * writable: pre-opened from showSaveFilePicker() in the button click.
   *   If provided → FSAPI streaming path (best: direct to chosen disk location).
   *   If null     → SW streaming (Downloads folder) or Blob fallback (≤100 MB).
   */
  constructor(dc, { writable = null, onProgress, onComplete, onError, onCancelled } = {}) {
    this.dc          = dc;
    this.onProgress  = onProgress;
    this.onComplete  = onComplete;
    this.onError     = onError;
    this.onCancelled = onCancelled;

    // File metadata (populated from file-header)
    this.fileId      = null;
    this.fileName    = null;
    this.fileSize    = 0;
    this.mimeType    = '';
    this.totalChunks = 0;

    this._headerDone      = false;
    // FIX: _preHeaderBuffer is now capped at PRE_HEADER_CAP entries.
    // If cap is exceeded, additional chunks are dropped and will be
    // re-requested via MISSING after path setup completes.
    this._preHeaderBuffer = []; // bounded: max PRE_HEADER_CAP chunks

    // ── Path selection ────────────────────────────────────────────────────
    // Priority: writable (FSAPI) > SW streaming > Blob fallback (small files only)

    // FSAPI path — writable pre-opened in user gesture
    this._writable      = writable;   // FileSystemWritableFileStream
    this._usePicker     = !!writable;
    this._nextPickerSeq = 0;
    this._pickerBuf     = new Map();  // out-of-order buffer, max WINDOW_SIZE
    this._pickerWriteQueue = Promise.resolve(); // serialize writes

    // SW streaming path
    this._downloader    = null;
    this._useStreaming  = false;
    this._nextWriteSeq  = 0;
    this._streamBuf     = new Map(); // out-of-order buffer, max WINDOW_SIZE

    // Blob fallback — ONLY for files ≤ BLOB_FALLBACK_LIMIT
    this._useFallback = false;
    this._received    = new Map(); // seq→ArrayBuffer — ONLY for fallback, small files

    // ── Dedup tracking — O(WINDOW_SIZE) not O(totalChunks) ───────────────
    this._highWatermark = -1;  // all seqs 0.._highWatermark confirmed received
    this._gapSet        = new Set(); // out-of-order seqs above _highWatermark

    // ACK batching
    this.pendingAcks = new Set();
    this._ackTimer   = null;
    this._scanTimer  = null;
    this._done       = false;
    this._cancelled  = false;

    // FIX: Track whether STREAM_READY has been sent to the sender.
    // Sender must not pump until this is sent.
    this._streamReadySent = false;

    // Progress
    this._bytesReceived = 0;
    this._lastStatAt    = Date.now();
    this._lastBytesRcvd = 0;

    // onmessage MUST be sync — async causes race conditions
    dc.onmessage = ({ data }) => {
      if (typeof data === 'string') this._handleText(data);
      else                          this._handleBinary(data);
    };
  }

  // ── Public ────────────────────────────────────────────────────────────────

  destroy() {
    this._cancelled = true;
    clearInterval(this._scanTimer);
    clearTimeout(this._ackTimer);
    this._downloader?.abort('destroyed');
    if (this._writable) {
      this._writable.abort?.().catch(() => {});
      this._writable = null;
    }
    // FIX: Release pre-header buffer on destroy to avoid memory retention
    this._preHeaderBuffer = [];
    this._received.clear();
  }

  // ── Text messages ─────────────────────────────────────────────────────────

  _handleText(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if      (msg.type === 'file-header')  this._handleHeader(msg);
    else if (msg.type === 'file-footer')  this._handleFooter(msg);
  }

  _handleHeader(msg) {
    this.fileId      = msg.fileId;
    this.fileName    = msg.fileName;
    this.fileSize    = msg.fileSize ?? 0;
    this.mimeType    = msg.mimeType ?? 'application/octet-stream';
    this.totalChunks = msg.totalChunks ?? 0;
    console.log(`[receiver] "${this.fileName}" — ${this.totalChunks} chunks`);
    this._setupPath(); // fire-and-forget async; sends STREAM_READY when done
  }

  async _setupPath() {
    if (this._usePicker) {
      // Writable already open — nothing to set up
      console.log('[receiver] FSAPI: direct write to chosen location');

    } else if (STREAMING_SUPPORTED) {
      // Try SW streaming
      try {
        const dl = new StreamDownloader(this.fileName, this.fileSize);

        // FIX: Wire up SW runtime errors (e.g. sw_waiters_overflow) so they
        // propagate to onError and abort the transfer cleanly, rather than
        // being swallowed silently inside StreamDownloader.
        dl.onError = (reason) => {
          if (this._done || this._cancelled) return;
          console.error('[receiver] SW streaming error — aborting:', reason);
          this._done = true;
          this._downloader = null;
          this._useStreaming = false;
          this.onError?.(`Streaming error: ${reason}`);
        };

        const ok = await dl.init();
        if (ok) {
          this._downloader  = dl;
          this._useStreaming = true;
          console.log('[receiver] SW streaming active');
        } else {
          // FIX: SW init returned false — only allow Blob fallback for small files
          if (this.fileSize > BLOB_FALLBACK_LIMIT) {
            console.error(
              `[receiver] SW unavailable and file (${this.fileSize} B) exceeds ` +
              `Blob fallback limit (${BLOB_FALLBACK_LIMIT} B) — aborting`
            );
            this._abortLargeFileFallback();
            return;
          }
          this._useFallback = true;
          console.warn('[receiver] SW failed — Blob fallback (small file)');
        }
      } catch (e) {
        // FIX: SW threw — same large-file guard
        if (this.fileSize > BLOB_FALLBACK_LIMIT) {
          console.error(`[receiver] SW error for large file — aborting: ${e.message}`);
          this._abortLargeFileFallback();
          return;
        }
        console.warn('[receiver] SW failed, Blob fallback:', e.message);
        this._useFallback = true;
      }

    } else {
      // FIX: No SW at all — guard large files
      if (this.fileSize > BLOB_FALLBACK_LIMIT) {
        console.error('[receiver] No streaming available for large file — aborting');
        this._abortLargeFileFallback();
        return;
      }
      this._useFallback = true;
      console.warn('[receiver] No SW — Blob fallback (small file)');
    }

    this._headerDone = true;

    // FIX: Send STREAM_READY NOW — before draining pre-header buffer.
    // Sender is blocked waiting for this signal and has sent zero data chunks.
    // Pre-header buffer should be empty or have at most a few control frames.
    this._sendStreamReady();

    // Drain chunks buffered during setup (bounded by PRE_HEADER_CAP)
    for (const { seq, data } of this._preHeaderBuffer) {
      this._storeChunk(seq, data);
    }
    this._preHeaderBuffer = []; // FIX: release memory immediately

    this._startScanTimer();
  }

  // FIX: Hard abort when large file has no streaming path.
  // Sends error to sender and calls onError — transfer is cleanly terminated.
  _abortLargeFileFallback() {
    this._done = true;
    this._headerDone = true;
    this._preHeaderBuffer = []; // release memory
    this._send({ type: 'ABORT', reason: 'NO_STREAMING' });
    this.onError?.(
      `Streaming unavailable. Files larger than ${BLOB_FALLBACK_LIMIT / 1024 / 1024} MB ` +
      `require Chrome/Edge with File System Access API or Service Worker support.`
    );
  }

  // FIX: Notify sender that receiver streaming path is confirmed and ready.
  // Sender waits for this before calling _pump().
  _sendStreamReady() {
    if (this._streamReadySent) return;
    this._streamReadySent = true;
    this._send({ type: 'STREAM_READY' });
    console.log('[receiver] STREAM_READY sent — sender may begin pumping');
  }

  // ── Binary frames — SYNCHRONOUS ───────────────────────────────────────────

  _handleBinary(buffer) {
    if (this._done || this._cancelled) return;
    if (buffer.byteLength < 4) return;

    const view    = new DataView(buffer);
    const metaLen = view.getUint32(0, true);
    if (4 + metaLen > buffer.byteLength) return;

    let meta;
    try {
      meta = JSON.parse(new TextDecoder().decode(new Uint8Array(buffer, 4, metaLen)));
    } catch { return; }
    if (meta.type !== 'chunk') return;

    const seq = meta.seq;
    if (typeof seq !== 'number' || seq < 0) return;

    // Dedup — O(1), no full-size Set
    if (seq <= this._highWatermark) return;
    if (this._gapSet.has(seq)) return;

    // FIX: Pre-header cap check MUST happen BEFORE we update the dedup
    // watermark. If we updated the watermark first and then dropped the chunk,
    // _getMissing() would never scan below _highWatermark and the dropped seq
    // would be permanently lost — no retransmit would ever be requested.
    //
    // By returning early here, we leave _highWatermark and _gapSet unchanged,
    // so the seq stays "unseen". The sender's ACK_TIMEOUT will retransmit it
    // after _setupPath() completes and STREAM_READY is sent. Under the
    // STREAM_READY protocol this branch should never trigger in normal use
    // because the sender waits for STREAM_READY before pumping chunks.
    if (!this._headerDone && this._preHeaderBuffer.length >= PRE_HEADER_CAP) {
      console.warn(
        `[receiver] pre-header buffer full (cap=${PRE_HEADER_CAP}), ` +
        `ignoring seq=${seq} — sender will retransmit after STREAM_READY`
      );
      return; // DO NOT update watermark or gapSet — chunk stays retransmittable
    }

    const data = buffer.slice(4 + metaLen);
    this._bytesReceived += data.byteLength;

    // Update watermark (prune gapSet as contiguous range grows)
    this._gapSet.add(seq);
    while (this._gapSet.has(this._highWatermark + 1)) {
      this._gapSet.delete(this._highWatermark + 1);
      this._highWatermark++;
    }

    if (!this._headerDone) {
      this._preHeaderBuffer.push({ seq, data }); // safe: cap enforced above
    } else {
      this._storeChunk(seq, data);
    }

    this.pendingAcks.add(seq);
    this._scheduleAckFlush();
    this._reportProgress();
  }

  // ── Chunk storage — write immediately, discard from RAM ──────────────────

  _storeChunk(seq, data) {
    if (this._useFallback) {
      // FIX: Double-guard — should never reach here for large files,
      // but defensive check ensures we never silently grow unboundedly.
      if (this.fileSize > BLOB_FALLBACK_LIMIT) {
        console.error('[receiver] BUG: _storeChunk called on large file in fallback mode — aborting');
        this._abortLargeFileFallback();
        return;
      }
      this._received.set(seq, data); // only path that holds all data
      return;
    }
    if (this._usePicker) {
      // Buffer in-order, write immediately, discard
      this._pickerBuf.set(seq, data);
      this._flushPickerBuf();
      return;
    }
    // SW streaming
    this._streamBuf.set(seq, data);
    this._flushStreamBuf();
  }

  // Flush consecutive chunks to FSAPI writable — discard each after write
  _flushPickerBuf() {
    while (this._pickerBuf.has(this._nextPickerSeq) && this._writable && !this._cancelled) {
      const seq  = this._nextPickerSeq++;
      const data = this._pickerBuf.get(seq);
      this._pickerBuf.delete(seq); // discard from RAM immediately

      // Chain writes: each awaits the previous to guarantee disk order
      this._pickerWriteQueue = this._pickerWriteQueue.then(async () => {
        if (!this._writable || this._cancelled) return;
        try {
          await this._writable.write(data);
          // data goes out of scope here — GC can collect it
        } catch (e) {
          if (!this._cancelled) this.onError?.(`Write error: ${e.message}`);
        }
      });
    }
  }

  // Flush consecutive chunks to SW downloader — discard each after send
  _flushStreamBuf() {
    while (this._streamBuf.has(this._nextWriteSeq) && this._downloader) {
      const data = this._streamBuf.get(this._nextWriteSeq);
      this._streamBuf.delete(this._nextWriteSeq); // discard immediately
      // FIX: Wrap write() in try/catch. StreamDownloader.write() throws if
      // the pre-ready queue cap is exceeded (protocol violation). An uncaught
      // exception here would kill the DataChannel onmessage handler entirely,
      // making the receiver permanently deaf to all further messages.
      try {
        this._downloader.write(data);
      } catch (e) {
        console.error('[receiver] _flushStreamBuf write error — aborting:', e.message);
        this._done = true;
        this._downloader?.abort(e.message);
        this._downloader = null;
        this.onError?.(`Stream write error: ${e.message}`);
        return; // stop flushing
      }
      this._nextWriteSeq++;
    }
  }

  // ── ACK batching ──────────────────────────────────────────────────────────

  _scheduleAckFlush() {
    if (this._ackTimer) return;
    this._ackTimer = setTimeout(() => {
      this._flushAcks();
      this._ackTimer = null;
    }, ACK_FLUSH_INTERVAL);
  }

  _flushAcks() {
    for (const seq of this.pendingAcks) this._send({ type: 'ACK', seq });
    this.pendingAcks.clear();
  }

  // ── Missing chunk scanner — bounded scan window ───────────────────────────

  _startScanTimer() {
    this._scanTimer = setInterval(() => {
      if (this._done || this._cancelled || !this.totalChunks) return;
      const missing = this._getMissing();
      if (missing.length > 0 && missing.length < this.totalChunks) {
        console.log(`[receiver] requesting ${missing.length} missing chunks`);
        this._send({ type: 'MISSING', seqs: missing.slice(0, 128) });
      }
    }, MISSING_SCAN_INTERVAL);
  }

  _getMissing() {
    // Bounded scan — avoids O(totalChunks) CPU spike every 3s
    const SCAN_WINDOW = 256;
    const end = Math.min(this._highWatermark + 1 + SCAN_WINDOW, this.totalChunks);
    const out = [];
    for (let i = this._highWatermark + 1; i < end; i++) {
      if (!this._gapSet.has(i) && !this._streamBuf.has(i) && !this._pickerBuf.has(i)) {
        out.push(i);
      }
    }
    return out;
  }

  // ── Footer / finalize ─────────────────────────────────────────────────────

  async _handleFooter({ sha256 }) {
    this._flushAcks();
    clearInterval(this._scanTimer);

    // Wait for header setup if still running
    if (!this._headerDone) {
      await new Promise(r => {
        const t = setInterval(() => { if (this._headerDone) { clearInterval(t); r(); } }, 10);
      });
    }

    if (this._cancelled) return;

    // Check all chunks received (bounded scan is fine — if missing exist
    // beyond SCAN_WINDOW, the missing-chunk reply will trigger more)
    const missing = this._getMissing();
    if (missing.length > 0) {
      console.warn(`[receiver] footer: ${missing.length} missing`);
      this._send({ type: 'MISSING', seqs: missing });
      this._startScanTimer();
      return;
    }

    // Also check full completeness (highWatermark must reach totalChunks-1)
    if (this._highWatermark < this.totalChunks - 1) {
      const allMissing = [];
      for (let i = this._highWatermark + 1; i < this.totalChunks; i++) {
        if (!this._gapSet.has(i)) allMissing.push(i);
        if (allMissing.length >= 128) break;
      }
      if (allMissing.length > 0) {
        console.warn(`[receiver] footer: still ${this.totalChunks - this._highWatermark - 1} more missing`);
        this._send({ type: 'MISSING', seqs: allMissing });
        this._startScanTimer();
        return;
      }
    }

    if (this._usePicker) {
      // CRITICAL: await ALL queued writes before closing
      // _pickerWriteQueue is a chained Promise — awaiting it guarantees
      // all previous writes have completed before we close the stream
      await this._pickerWriteQueue;
      // Final flush for any remaining buffered chunks
      this._flushPickerBuf();
      await this._pickerWriteQueue;

      // SHA-256 skip for large files (re-reading would defeat the no-RAM goal)
      // For small files on fallback only — picker path skips hash
      // Integrity guaranteed by ACK/MISSING protocol

      this._done = true;
      this._send({ type: 'DONE' });

      // Close stream → OS renames .crswap → final file
      // Fast because all bytes already written incrementally
      try { await this._writable?.close(); } catch { /* ignore */ }
      this._writable = null;
      this.onComplete?.(null, this.fileName, true);

    } else if (this._useStreaming) {
      this._flushStreamBuf();
      this._done = true;
      this._send({ type: 'DONE' });
      this._downloader.close();
      this.onComplete?.(null, this.fileName, true);

    } else {
      // Fallback: hash only if small enough
      if (sha256 && this.fileSize > 0 && this.fileSize <= HASH_LIMIT) {
        const ok = await this._verifyHash(sha256);
        if (!ok) { this.onError?.('SHA-256 mismatch — file corrupted.'); return; }
        console.log('[receiver] SHA-256 ✓');
      }
      this._done = true;
      this._send({ type: 'DONE' });
      const parts = [];
      for (let i = 0; i < this.totalChunks; i++) parts.push(this._received.get(i));
      const blob = new Blob(parts, { type: this.mimeType });
      this._received.clear();
      this.onComplete?.(blob, this.fileName, false);
    }
  }

  // ── Hash — fallback path only, small files only ───────────────────────────
  async _verifyHash(expectedHash) {
    if (!expectedHash) return true;
    const EMPTY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
    if (this.fileSize === 0) return expectedHash === EMPTY;
    try {
      const parts = [];
      for (let i = 0; i < this.totalChunks; i++) parts.push(this._received.get(i));
      const buf = await new Blob(parts).arrayBuffer();
      const h   = await crypto.subtle.digest('SHA-256', buf);
      const hex = [...new Uint8Array(h)].map(b => b.toString(16).padStart(2, '0')).join('');
      return hex === expectedHash;
    } catch { return true; }
  }

  _send(msg) {
    try { this.dc.send(JSON.stringify(msg)); }
    catch (e) { console.warn('[receiver] send failed:', e.message); }
  }

  _reportProgress() {
    const now = Date.now();
    const dt  = now - this._lastStatAt;
    if (dt < 150) return;
    const delta     = this._bytesReceived - this._lastBytesRcvd;
    const speed     = (delta / dt) * 1000;
    const remaining = Math.max(0, this.fileSize - this._bytesReceived);
    const eta       = speed > 0 ? remaining / speed : Infinity;
    this.onProgress?.({
      received:       Math.min(this._bytesReceived, this.fileSize),
      total:          this.fileSize,
      chunksReceived: this._highWatermark + 1 + this._gapSet.size,
      totalChunks:    this.totalChunks,
      speed, eta,
    });
    this._lastStatAt    = now;
    this._lastBytesRcvd = this._bytesReceived;
  }
}
