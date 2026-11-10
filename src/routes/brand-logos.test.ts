import { describe, test, expect } from 'bun:test'
import Elysia from 'elysia'
import { createBrandLogoRoutes, LOGO_CSP } from './brands'
import { etagOf } from '../lib/static-response'
import type { LogoFile } from '../services/brand-logo-files.service'

const SVG = new TextEncoder().encode('<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>')
const PNG = new Uint8Array([0x89, 0x50, 0x4e, 0x47])

function appWith(files: Record<string, LogoFile>) {
  const asked: string[] = []
  const app = new Elysia().use(
    createBrandLogoRoutes({
      getBrandLogoFile: async (qid: string) => {
        asked.push(qid)
        return files[qid] ?? null
      },
    }),
  )
  const get = (path: string, headers?: Record<string, string>) => app.handle(new Request(`http://localhost${path}`, { headers }))
  return { get, asked }
}

describe('GET /brand-logos/:qid', () => {
  const svg = { contentType: 'image/svg+xml', bytes: SVG, etag: etagOf(SVG) }
  const png = { contentType: 'image/png', bytes: PNG, etag: etagOf(PNG) }

  test('passes an SVG through with a CSP that stops its scripts', async () => {
    const { get } = appWith({ Q38076: svg })
    const res = await get('/brand-logos/Q38076?v=abc')
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toBe('image/svg+xml')
    expect(res.headers.get('content-security-policy')).toBe(LOGO_CSP)
    expect(LOGO_CSP).toBe("default-src 'none'; style-src 'unsafe-inline'")
    expect(res.headers.get('x-content-type-options')).toBe('nosniff')
    expect(res.headers.get('cache-control')).toContain('max-age=2592000')
    expect(res.headers.get('etag')).toBe(svg.etag)
    expect(new Uint8Array(await res.arrayBuffer())).toEqual(SVG)
  })

  test('serves a PNG with nosniff and no key', async () => {
    const { get } = appWith({ Q37158: png })
    const res = await get('/brand-logos/Q37158')
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toBe('image/png')
    expect(res.headers.get('x-content-type-options')).toBe('nosniff')
  })

  test('answers 304 to a matching If-None-Match', async () => {
    const { get } = appWith({ Q38076: svg })
    expect((await get('/brand-logos/Q38076', { 'if-none-match': svg.etag })).status).toBe(304)
  })

  test('404s a brand with no logo', async () => {
    const { get } = appWith({})
    const res = await get('/brand-logos/Q1')
    expect(res.status).toBe(404)
    expect(res.headers.get('x-content-type-options')).toBe('nosniff')
  })
})
