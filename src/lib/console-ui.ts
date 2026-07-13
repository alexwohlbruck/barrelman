/**
 * Serves the built admin console SPA (web/dist) at /console.
 *
 * Vite is configured with base '/console/', so asset URLs resolve under this
 * prefix. Unknown sub-paths fall back to index.html for client-side routing.
 * If the SPA hasn't been built yet, a friendly placeholder explains how.
 */
import Elysia from 'elysia'
import { existsSync } from 'node:fs'
import { join, resolve, normalize } from 'node:path'

const REPO_ROOT = resolve(import.meta.dir, '../..')
const DIST = join(REPO_ROOT, 'web', 'dist')
const INDEX = join(DIST, 'index.html')

const PLACEHOLDER = `<!doctype html><html><head><meta charset="utf-8"><title>Barrelman Console</title>
<style>body{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#0a0a0a;color:#e5e5e5;max-width:640px;margin:12vh auto;padding:0 24px;line-height:1.6}code{background:#1a1a1a;padding:2px 6px;border-radius:4px;color:#7dd3fc}h1{font-weight:600}a{color:#7dd3fc}</style>
</head><body>
<h1>Barrelman Admin Console</h1>
<p>The console UI hasn't been built yet. From the repo root:</p>
<p><code>cd web && bun install && bun run build</code></p>
<p>Then reload this page. For live development instead run <code>bun run dev</code> in <code>web/</code> (Vite dev server on :5199, proxied to this API).</p>
</body></html>`

function serveIndex() {
  if (existsSync(INDEX)) return new Response(Bun.file(INDEX), { headers: { 'content-type': 'text/html' } })
  return new Response(PLACEHOLDER, { headers: { 'content-type': 'text/html' } })
}

export const consoleUiRoutes = new Elysia()
  .get('/console', () => serveIndex())
  .get('/console/', () => serveIndex())
  .get('/console/*', ({ params }) => {
    const rel = (params as Record<string, string>)['*'] || ''
    // Resolve and confine to DIST — reject traversal.
    const target = normalize(join(DIST, rel))
    if (target.startsWith(DIST) && existsSync(target) && !target.endsWith('/')) {
      return new Response(Bun.file(target))
    }
    return serveIndex()
  })
