import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// The app is served by Datatailr under /job/<env>/llm-admin/, so every asset
// URL must be relative. The build output ships inside the Python package.
export default defineConfig({
  base: './',
  plugins: [react(), tailwindcss()],
  build: {
    outDir: '../llm_platform/admin_app/static',
    emptyOutDir: true,
    sourcemap: false,
  },
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8008',
    },
  },
})
