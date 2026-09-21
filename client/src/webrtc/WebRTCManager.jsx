const ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },

  // ✅ FIX 1: Add a real TURN server
  {
    urls: 'turn:openrelay.metered.ca:80',       // free tier for testing
    username: 'openrelayproject',
    credential: 'openrelayproject',
  },
  {
    urls: 'turn:openrelay.metered.ca:443?transport=tcp', // TCP fallback (firewalls)
    username: 'openrelayproject',
    credential: 'openrelayproject',
  },
];

export class WebRTCManager {
  constructor({ sendSignal, onDataChannel, onStateChange }) {
    this.sendSignal = sendSignal;
    this.onDataChannel = onDataChannel;
    this.onStateChange = onStateChange;
    this.pc = null;
    this.dc = null;

    // ✅ FIX 2: Queue for ICE candidates that arrive before remote desc is set
    this._pendingCandidates = [];
    this._remoteDescSet = false;
  }

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
    dc.onopen = () => { console.log('[rtc] datachannel open'); this.onDataChannel?.(dc); };
    dc.onclose = () => console.log('[rtc] datachannel closed');
    dc.onerror = (e) => console.error('[rtc] datachannel error', e);
  }

  // ✅ FIX 2: Flush any ICE candidates that arrived early
  async _flushPendingCandidates() {
    for (const candidate of this._pendingCandidates) {
      try {
        await this.pc.addIceCandidate(new RTCIceCandidate(candidate));
      } catch (e) {
        console.warn('[rtc] queued ICE candidate failed:', e.message);
      }
    }
    this._pendingCandidates = [];
  }

  async initSender() {
    this.pc = this._buildPC();
    const dc = this.pc.createDataChannel('sharetunnel', { ordered: true });
    this._attachChannel(dc);

    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    this.sendSignal({ type: 'offer', sdp: this.pc.localDescription });
  }

  initReceiver() {
    this.pc = this._buildPC();
    this.pc.ondatachannel = ({ channel }) => this._attachChannel(channel);
  }

  async handleSignal(msg) {
    const { pc } = this;
    if (!pc) return;

    if (msg.type === 'offer') {
      await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
      this._remoteDescSet = true;
      await this._flushPendingCandidates(); // ✅ apply any early candidates now
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      this.sendSignal({ type: 'answer', sdp: pc.localDescription });

    } else if (msg.type === 'answer') {
      await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
      this._remoteDescSet = true;
      await this._flushPendingCandidates(); // ✅ apply any early candidates now

    } else if (msg.type === 'ice-candidate') {
      if (!this._remoteDescSet) {
        // ✅ Remote desc not ready yet — queue it instead of dropping it
        this._pendingCandidates.push(msg.candidate);
      } else {
        try { await pc.addIceCandidate(new RTCIceCandidate(msg.candidate)); }
        catch (e) { console.warn('[rtc] bad ICE candidate', e.message); }
      }
    }
  }

  close() {
    this.dc?.close();
    this.pc?.close();
    this.pc = null;
    this.dc = null;
    this._pendingCandidates = []; // ✅ clean up
    this._remoteDescSet = false;
  }
}