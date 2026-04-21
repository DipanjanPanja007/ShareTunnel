import { useState, useEffect } from 'react';
import SenderPage from './pages/SenderPage.jsx';
import ReceiverPage from './pages/ReceiverPage.jsx';

function parseRoute() {
  const m = window.location.hash.match(/^#receive\/([a-f0-9-]{36})$/);
  if (m) return { view: 'receive', sessionId: m[1] };
  return { view: 'send' };
}

export default function App() {
  const [route, setRoute] = useState(parseRoute);

  useEffect(() => {
    const handler = () => setRoute(parseRoute());
    window.addEventListener('hashchange', handler);
    return () => window.removeEventListener('hashchange', handler);
  }, []);

  if (route.view === 'receive') {
    // key={sessionId} forces a full unmount+remount whenever the sessionId changes.
    // This resets ALL state in ReceiverPage — no stale status, no stale WebRTC connections.
    // Without this, pasting a new receive link after a completed download would leave
    // the page showing the DONE screen from the previous session.
    return <ReceiverPage key={route.sessionId} sessionId={route.sessionId} />;
  }

  return <SenderPage />;
}
