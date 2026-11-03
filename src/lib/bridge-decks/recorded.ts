/**
 * Deck inputs recorded from OSM with their terrain, for tests: `fixtures/<name>.json.gz`,
 * every position as [lng, lat] and the terrain a grid of decimetres read bilinearly.
 */
import { gunzipSync } from 'node:zlib'
import type { DeckInput, Ground } from './build'
import type { Support } from './elevation'
import { mercator, type Point } from './profile'

type LngLat = number[]
type Recorded = {
  ways: Array<Omit<DeckInput['ways'][number], 'points'> & { points: LngLat[] }>
  onGround: LngLat[]
  raised?: LngLat[]
  outlines: Array<Omit<DeckInput['outlines'][number], 'rings' | 'supports'> & { rings: LngLat[][]; supports?: Array<Omit<Support, 'ring'> & { ring: LngLat[] }> }>
  kerbs: LngLat[]
  crossed: Array<{ kind: DeckInput['crossed'][number]['kind']; points: LngLat[] }>
  water: LngLat[][]
  wikidata: Array<[number, string]>
  ground: { west: number; north: number; dLng: number; dLat: number; cols: number; rows: number; decimetres: string }
}

const toPoint = ([lng, lat]: LngLat) => mercator(lng, lat)

export async function recorded(name: string): Promise<{ input: DeckInput; ground: Ground }> {
  const raw: Recorded = JSON.parse(gunzipSync(await Bun.file(new URL(`./fixtures/${name}.json.gz`, import.meta.url)).arrayBuffer()).toString())
  const input: DeckInput = {
    ways: raw.ways.map(w => ({ ...w, points: w.points.map(toPoint) })),
    onGround: new Set(raw.onGround.map(p => toPoint(p).join(','))),
    raised: new Set((raw.raised ?? []).map(p => toPoint(p).join(','))),
    outlines: raw.outlines.map(o => ({ ...o, rings: o.rings.map(r => r.map(toPoint)), supports: o.supports?.map(s => ({ ...s, ring: s.ring.map(toPoint) })) })),
    kerbs: raw.kerbs.map(toPoint),
    crossed: raw.crossed.map(c => ({ kind: c.kind, points: c.points.map(toPoint) })),
    water: raw.water.map(r => r.map(toPoint)),
    wikidata: new Map(raw.wikidata),
  }
  const { west, north, dLng, dLat, cols, rows, decimetres } = raw.ground
  const heights = new Int16Array(Buffer.from(decimetres, 'base64').buffer.slice(0))
  const ground: Ground = {
    async load() {},
    at([x, y]: Point) {
      const lng = x * 360 - 180
      const lat = (Math.atan(Math.sinh(Math.PI * (1 - 2 * y))) * 180) / Math.PI
      const [fx, fy] = [Math.min(cols - 1.001, Math.max(0, (lng - west) / dLng)), Math.min(rows - 1.001, Math.max(0, (north - lat) / dLat))]
      const [c, r] = [Math.floor(fx), Math.floor(fy)]
      const [tx, ty] = [fx - c, fy - r]
      const h = (cc: number, rr: number) => heights[rr * cols + cc] / 10
      return (h(c, r) * (1 - tx) + h(c + 1, r) * tx) * (1 - ty) + (h(c, r + 1) * (1 - tx) + h(c + 1, r + 1) * tx) * ty
    },
  }
  return { input, ground }
}
