/**
 * WebRTCManager
 * Wraps RTCPeerConnection + DataChannel lifecycle.
 * Works symmetrically for sender (creates offer + channel)
 * and receiver (answers + receives channel via ondatachannel).
 */

const ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
  { urls: 'stun:stun2.l.google.com:19302' },
  // ↓ Add TURN credentials here for production NAT traversal
  // { urls: 'turn:your-server.com:3478', username: '…', credential: '…' },
];

export class WebRTCManager {
  /**
   * @param {{
   *   sendSignal: (msg: object) => void,
   *   onDataChannel: (dc: RTCDataChannel) => void,
   *   onStateChange: (state: string) => void,
   * }} opts
   */
  constructor({ sendSignal, onDataChannel, onStateChange }) {
    this.sendSignal = sendSignal;
    this.onDataChannel = onDataChannel;
    this.onStateChange = onStateChange;
    this.pc = null;
    this.dc = null;
  }

  // ── Internal ────────────────────────────────────────────────────────────────

  _buildPC() {
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

    pc.onicecandidate = ({ candidate }) => {
      if (candidate) this.sendSignal({ type: 'ice-candidate', candidate });
    };

    pc.onconnectionstatechange = () => {
      console.log('[rtc] connection:', pc.connectionState);
      this.onStateChange?.(pc.connectionState);
    };

    pc.oniceconnectionstatechange = () => {
      console.log('[rtc] ice:', pc.iceConnectionState);
    };

    return pc;
  }

  _attachChannel(dc) {
    dc.binaryType = 'arraybuffer';
    this.dc = dc;
    dc.onopen  = () => { console.log('[rtc] datachannel open'); this.onDataChannel?.(dc); };
    dc.onclose = () =>   console.log('[rtc] datachannel closed');
    dc.onerror = (e) => console.error('[rtc] datachannel error', e);
  }

  // ── Public API ──────────────────────────────────────────────────────────────

  /** Sender: create offer + DataChannel */
  async initSender() {
    this.pc = this._buildPC();
    const dc = this.pc.createDataChannel('sharetunnel', { ordered: true });
    this._attachChannel(dc);

    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    this.sendSignal({ type: 'offer', sdp: this.pc.localDescription });
  }

  /** Receiver: wait for DataChannel to arrive */
  initReceiver() {
    this.pc = this._buildPC();
    this.pc.ondatachannel = ({ channel }) => this._attachChannel(channel);
  }

  /** Handle inbound signaling messages */
  async handleSignal(msg) {
    const { pc } = this;
    if (!pc) return;

    if (msg.type === 'offer') {
      await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      this.sendSignal({ type: 'answer', sdp: pc.localDescription });

    } else if (msg.type === 'answer') {
      await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));

    } else if (msg.type === 'ice-candidate') {
      try { await pc.addIceCandidate(new RTCIceCandidate(msg.candidate)); }
      catch (e) { console.warn('[rtc] bad ICE candidate', e.message); }
    }
  }

  close() {
    this.dc?.close();
    this.pc?.close();
    this.pc = null;
    this.dc = null;
  }
}
