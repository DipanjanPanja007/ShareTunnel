// ── Formatting ──────────────────────────────────────────────────────────────

export function fmtBytes(n) {
  if (!n || n === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
  return `${(n / 1024 ** i).toFixed(i ? 1 : 0)} ${units[i]}`;
}

export function fmtSpeed(bps) {
  if (!isFinite(bps) || bps <= 0) return '—';
  return `${fmtBytes(bps)}/s`;
}

export function fmtEta(secs) {
  if (!isFinite(secs) || secs <= 0) return '—';
  if (secs < 60)   return `${Math.ceil(secs)}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${Math.ceil(secs % 60)}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

// ── File ────────────────────────────────────────────────────────────────────

export function downloadBlob(blob, name) {
  const url = URL.createObjectURL(blob);
  const a = Object.assign(document.createElement('a'), { href: url, download: name });
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 1000);
}

export function fileEmoji(name = '') {
  const ext = name.split('.').pop().toLowerCase();
  const m = {
    pdf:'📄', zip:'🗜️', rar:'🗜️', gz:'🗜️', tar:'🗜️', '7z':'🗜️',
    jpg:'🖼️', jpeg:'🖼️', png:'🖼️', gif:'🖼️', webp:'🖼️', svg:'🖼️', avif:'🖼️',
    mp4:'🎬', mov:'🎬', avi:'🎬', mkv:'🎬', webm:'🎬',
    mp3:'🎵', wav:'🎵', flac:'🎵', aac:'🎵', ogg:'🎵',
    doc:'📝', docx:'📝', txt:'📝', md:'📝', rtf:'📝',
    xls:'📊', xlsx:'📊', csv:'📊',
    ppt:'📊', pptx:'📊',
    js:'💻', ts:'💻', jsx:'💻', tsx:'💻', py:'💻', go:'💻', rs:'💻',
    exe:'⚙️', dmg:'⚙️', apk:'⚙️', iso:'⚙️',
  };
  return m[ext] ?? '📁';
}

// ── Share link ───────────────────────────────────────────────────────────────

export function buildShareLink(sessionId) {
  const { origin, pathname } = window.location;
  return `${origin}${pathname}#receive/${sessionId}`;
}

// ── QR code ──────────────────────────────────────────────────────────────────

export async function makeQrDataUrl(text) {
  const QRCode = await import('qrcode');
  return QRCode.default.toDataURL(text, { width: 180, margin: 1, color: { dark: '#c8fb4a', light: '#111118' } });
}

// ── Clipboard ────────────────────────────────────────────────────────────────

export async function copyText(text) {
  try { await navigator.clipboard.writeText(text); return true; }
  catch { return false; }
}
