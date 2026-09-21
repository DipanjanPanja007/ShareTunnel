// vite.config.js
// Vite 6 + React 19 + Tailwind CSS v4 (via @tailwindcss/vite)
// Tailwind v4 does NOT use tailwind.config.js — configuration is in CSS.

import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
  server: {
    port: 3000,
    proxy: {
      // Proxy REST API calls to the signaling server
      '/api': {
        // target: 'http://localhost:3001',
        target: process.env.VITE_SERVER_URL || 'http://localhost:3001',
        changeOrigin: true,
      },
    },
  },
});
