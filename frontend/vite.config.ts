import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    host: true,            // bind on 0.0.0.0 so LAN colleagues can hit it
    // Permit temporary tunnel hostnames such as trycloudflare.com/ngrok for demos.
    allowedHosts: true,
    port: 5173,
    strictPort: true,
    proxy: {
      // forward all /api/* calls to the FastAPI backend so the UI and API share the same origin
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
    },
  },
})
