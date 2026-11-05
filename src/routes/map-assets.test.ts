import { describe, test, expect } from 'bun:test'
import Elysia from 'elysia'
import { createMapAssetRoutes } from './map-assets'
import { getGlyphs, parseGlyphRange, pickFont, VENDORED_FONTS } from '../services/map-assets.service'
import { groupForPath } from '../billing/plans'

const app = new Elysia().use(createMapAssetRoutes())
const get = (path: string, headers?: Record<string, string>) => app.handle(new Request(`http://localhost${path}`, { headers }))

describe('glyphs', () => {
  test('vendors the three Noto Sans faces the styles use', () => {
    expect(VENDORED_FONTS).toEqual(['Noto Sans Bold', 'Noto Sans Italic', 'Noto Sans Regular'])
  })

  test('serves a vendored range with no key and a long cache', async () => {
    const res = await get('/fonts/Noto%20Sans%20Bold/0-255.pbf')
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toBe('application/x-protobuf')
    expect(res.headers.get('cache-control')).toContain('max-age=2592000')
    expect((await res.arrayBuffer()).byteLength).toBeGreaterThan(1000)
  })

  test('serves the first font of a stack that we carry', async () => {
    expect(pickFont('Open Sans Bold,Noto Sans Italic,Noto Sans Regular')).toBe('Noto Sans Italic')
    const viaStack = await get('/fonts/Arial%20Unicode%20MS%20Regular,Noto%20Sans%20Bold/0-255.pbf')
    const direct = await get('/fonts/Noto%20Sans%20Bold/0-255.pbf')
    expect(viaStack.headers.get('etag')).toBe(direct.headers.get('etag'))
  })

  test('falls back to Noto Sans Regular for a stack naming none of ours', () => {
    expect(getGlyphs('Metropolis Light', '0-255.pbf')).toMatchObject({ font: 'Noto Sans Regular', empty: false })
    // A path in the font name is just an unknown font, never a file path.
    expect(getGlyphs('../../src', '0-255.pbf')?.font).toBe('Noto Sans Regular')
  })

  test('answers an empty 200 for a range we do not vendor', async () => {
    // CJK: MapLibre takes an empty set and leaves those labels undrawn.
    const res = await get('/fonts/Noto%20Sans%20Regular/19968-20223.pbf')
    expect(res.status).toBe(200)
    expect((await res.arrayBuffer()).byteLength).toBe(0)
    expect(res.headers.get('cache-control')).toBe('public, max-age=86400')
  })

  test('404s a malformed range', async () => {
    for (const range of ['0-256.pbf', '1-256.pbf', '0-255.png', '65536-65791.pbf']) {
      expect(parseGlyphRange(range)).toBeNull()
      expect((await get(`/fonts/Noto%20Sans%20Regular/${range}`)).status).toBe(404)
    }
  })

  test('answers 304 to a matching If-None-Match', async () => {
    const first = await get('/fonts/Noto%20Sans%20Regular/256-511.pbf')
    const etag = first.headers.get('etag')!
    const again = await get('/fonts/Noto%20Sans%20Regular/256-511.pbf', { 'if-none-match': etag })
    expect(again.status).toBe(304)
  })
})

describe('sprites', () => {
  test('serves each sheet at 1x and 2x', async () => {
    for (const name of ['osm-bright', 'dark-matter']) {
      const json = await get(`/sprites/${name}@2x.json`)
      expect(json.status).toBe(200)
      expect(json.headers.get('content-type')).toBe('application/json')
      expect(Object.keys(await json.json()).length).toBeGreaterThan(0)
      const png = await get(`/sprites/${name}.png`)
      expect(png.status).toBe(200)
      expect(png.headers.get('content-type')).toBe('image/png')
    }
  })

  test('404s anything else', async () => {
    for (const file of ['nope.json', 'osm-bright.svg', '..%2Fpackage.json']) {
      expect((await get(`/sprites/${file}`)).status).toBe(404)
    }
  })
})

test('fonts, sprites and logos are not metered', () => {
  for (const path of ['/fonts/Noto Sans Regular/0-255.pbf', '/sprites/osm-bright.json', '/brand-logos/Q38076']) {
    expect(groupForPath(path)).toBeNull()
  }
})
