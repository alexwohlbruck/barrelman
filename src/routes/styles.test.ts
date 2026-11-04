import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { buildStyle, createStyleRoutes } from './styles'
import { serverOrigin } from '../config/accounts.config'
import { groupForPath } from '../billing/plans'

const savedApiKey = process.env.BARRELMAN_API_KEY

beforeEach(() => {
  process.env.BARRELMAN_API_KEY = 'svc&b"c<'
})

afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

function get(path: string, headers?: Record<string, string>) {
  return new Elysia().use(createStyleRoutes()).handle(new Request(`http://localhost${path}`, { headers }))
}

type Style = {
  version: number
  sources: Record<string, { type: string; url: string; attribution: string }>
  layers: { source?: string }[]
  glyphs: string
  sprite: string
  metadata: Record<string, string>
}

describe('GET /styles/:style.json', () => {
  test('points its one source at our basemap TileJSON, with the basemap credits', async () => {
    const res = await get('/styles/basemap.json', { Authorization: 'Bearer svc&b"c<' })
    expect(res.status).toBe(200)
    const style = (await res.json()) as Style
    expect(style.version).toBe(8)
    expect(Object.keys(style.sources)).toEqual(['openmaptiles'])
    expect(style.sources.openmaptiles.url).toBe(`${serverOrigin}/tiles/basemap.json`)
    expect(style.sources.openmaptiles.attribution).toContain('© OpenStreetMap contributors')
    expect(style.sources.openmaptiles.attribution).toContain('© OpenMapTiles')
    // Every layer must draw from the source we rewrote, or it would reach for one that is gone.
    for (const layer of style.layers) {
      if (layer.source) expect(layer.source).toBe('openmaptiles')
    }
  })

  test('leaves no third-party key placeholder behind', async () => {
    for (const name of ['basemap', 'basemap-dark']) {
      const text = JSON.stringify(buildStyle(name, null))
      expect(text).not.toContain('{key}')
      expect(text).not.toContain('maptiler.com')
    }
  })

  test('injects a query key into the source URL, escaped', async () => {
    const res = await get('/styles/basemap.json?api_key=svc%26b%22c%3C')
    expect(res.status).toBe(200)
    const style = (await res.json()) as Style
    expect(style.sources.openmaptiles.url).toBe(`${serverOrigin}/tiles/basemap.json?api_key=svc%26b%22c%3C`)
  })

  test('never writes a Bearer key into the document', async () => {
    const res = await get('/styles/basemap-dark.json', { Authorization: 'Bearer svc&b"c<' })
    expect(res.status).toBe(200)
    expect(await res.text()).not.toContain('svc')
  })

  test('credits the style it is based on', () => {
    const style = buildStyle('basemap', null) as unknown as Style
    expect(style.metadata['barrelman:style-licence']).toContain('CC-BY 4.0')
    expect(style.glyphs).toContain('{fontstack}/{range}.pbf')
    expect(style.sprite).toContain('osm-bright-gl-style')
  })

  test('the dark style asks only for fonts the glyph host serves', () => {
    const text = JSON.stringify(buildStyle('basemap-dark', null))
    expect(text).not.toContain('Metropolis')
  })

  test('needs a key', async () => {
    expect((await get('/styles/basemap.json')).status).toBe(401)
  })

  test('404s an unknown style', async () => {
    expect((await get('/styles/nope.json?api_key=svc%26b%22c%3C')).status).toBe(404)
    expect((await get('/styles/basemap?api_key=svc%26b%22c%3C')).status).toBe(404)
  })

  test('is metered under tiles', () => {
    expect(groupForPath('/styles/basemap.json')).toBe('tiles')
  })
})
