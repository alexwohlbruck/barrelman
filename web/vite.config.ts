import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'

// The console is served by the Barrelman API under /console in production, so
// assets must resolve under that base. In dev, Vite proxies the admin API to
// the running Barrelman instance on :5001.
// Where to proxy the admin API in dev. On the host this is localhost:5001; when
// the dev server runs inside docker-compose it's the `barrelman` service, set
// via BARRELMAN_API_URL in docker-compose.dev.yml.
const apiTarget = process.env.BARRELMAN_API_URL || 'http://localhost:5001'

/**
 * Where the console is mounted, which is now a property of the deployment
 * rather than of the code.
 *
 * `/console/` is still the default, because that is where the API serves it
 * (src/lib/console-ui.ts) and that is what a self-hosted instance with one
 * origin gets. Behind the public edge it is served at the root of
 * console.barrelman.dev instead, so it is built with CONSOLE_BASE=/ — see the
 * Caddyfile. Vite bakes this into every asset URL, so a build made for one
 * layout 404s under the other; it cannot be switched at runtime.
 */
const base = process.env.CONSOLE_BASE || '/console/'

export default defineConfig({
  base,
  build: { target: 'es2022' },
  plugins: [vue(), tailwindcss()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    host: true, // bind 0.0.0.0 so the port is reachable from outside the container
    port: 5199,
    strictPort: true,
    proxy: Object.fromEntries(
      // Everything the console talks to. `changeOrigin` is deliberately off for
      // the account surface: the API's CSRF check compares the request Origin
      // against its allow-list, and rewriting it to the upstream host would
      // make every cookie-authenticated POST from the dev server look
      // same-origin, hiding CSRF regressions until production.
      ['/admin', '/auth', '/account', '/billing'].map((path) => [
        path,
        { target: apiTarget, changeOrigin: false },
      ]),
    ),
  },
})
