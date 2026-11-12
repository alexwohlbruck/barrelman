/**
 * Audit Structures: scores every bridge deck and tunnel cut in a box against
 * the checks in src/lib/structure-audit and the generator's own continuity
 * checks, and ranks the worst. Run before and after a generator change to
 * measure it. Reads the tiles a client gets, or with --build builds the decks
 * from DATABASE_URL in memory with this checkout's generator; neither writes.
 *
 *   bun scripts/audit-structures.ts --bbox=-74.3,40.5,-73.7,40.95 \
 *     --tiles 'https://host/tiles/{source}/{z}/{x}/{y}' [--key KEY] [--out audit]
 *   DATABASE_URL=… bun scripts/audit-structures.ts --bbox=… --build [--out audit]
 */
import { mkdirSync, writeFileSync } from 'node:fs'
import { parseArgs } from 'node:util'
import postgres from 'postgres'
import { inputFor, waysAround } from '../import/bridge-deck-cells'
import { buildDecks, type Deck } from '../src/lib/bridge-decks/build'
import { deckDefects } from '../src/lib/bridge-decks/continuity'
import { Dem } from '../src/lib/bridge-decks/dem'
import { lngLat, type Point } from '../src/lib/bridge-decks/profile'
import { readLayer } from '../src/lib/line-elevation/mvt'
import { clashes, deckIssues, lurches } from '../src/lib/structure-audit/decks'
import { score } from '../src/lib/structure-audit/score'
import { servedCut, servedDeck, type ServedCut } from '../src/lib/structure-audit/served'
import { cutIssues } from '../src/lib/structure-audit/tunnels'

const { values } = parseArgs({ options: { bbox: { type: 'string' }, tiles: { type: 'string' }, key: { type: 'string' }, out: { type: 'string', default: 'audit' }, concurrency: { type: 'string', default: '8' }, build: { type: 'boolean', default: false } } })
if (!values.bbox || !(values.tiles || values.build)) throw new Error('--bbox=w,s,e,n and --tiles <url with {source}/{z}/{x}/{y}> or --build are required')
const [w, s, e, n] = values.bbox.split(',').map(Number)
const Z = 14
const tileX = (lng: number) => Math.floor(((lng + 180) / 360) * 2 ** Z)
const tileY = (lat: number) => Math.floor(((1 - Math.log(Math.tan((lat * Math.PI) / 180) + 1 / Math.cos((lat * Math.PI) / 180)) / Math.PI) / 2) * 2 ** Z)
const tiles: Array<[number, number]> = []
for (let x = tileX(w); x <= tileX(e); x++) for (let y = tileY(n); y <= tileY(s); y++) tiles.push([x, y])

async function fetchLayer(source: string): Promise<Array<Record<string, string | number | boolean>>> {
  const out: Array<Record<string, string | number | boolean>> = []
  const queue = [...tiles]
  const headers: Record<string, string> = values.key ? { Authorization: `Bearer ${values.key}` } : {}
  await Promise.all(Array.from({ length: Number(values.concurrency) }, async () => {
    for (let t = queue.pop(); t; t = queue.pop()) {
      const url = values.tiles!.replace('{source}', source).replace('{z}', String(Z)).replace('{x}', String(t[0])).replace('{y}', String(t[1]))
      for (let attempt = 0; ; attempt++) {
        const response = await fetch(url, { headers })
        if (response.status === 204 || response.status === 404) break
        if (!response.ok) { if (attempt < 3) continue; throw new Error(`${url}: ${response.status}`) }
        const layer = readLayer(new Uint8Array(await response.arrayBuffer()), source)
        for (const f of layer?.features ?? []) out.push(f.properties)
        break
      }
    }
  }))
  return out
}

const started = Date.now()
const dem = new Dem()

/** Decks built from the database with this checkout, kept to those whose midpoint lies in the box, as a build cell keeps its own. */
async function built(): Promise<Deck[]> {
  const sql = postgres(process.env.DATABASE_URL!, { max: 2, max_lifetime: null })
  try {
    const rows = await waysAround(sql, [w, s, e, n])
    const decks = rows.length ? await buildDecks(await inputFor(sql, rows), dem) : []
    return decks.filter(d => { const [lng, lat] = lngLat(d.midpoint); return lng >= w && lng < e && lat >= s && lat < n })
  } finally {
    await sql.end({ timeout: 5 })
  }
}

const [decks, cutRows] = await Promise.all([
  values.build ? built() : fetchLayer('bridge_decks').then(rows => [...new Map(rows.map(p => [String(p.id), p])).values()].map(servedDeck).filter((d): d is Deck => !!d)),
  values.tiles ? fetchLayer('tunnel_cuts') : Promise.resolve([]),
])
const cuts = [...new Map(cutRows.map(p => [String(p.id), p])).values()].map(servedCut).filter((c): c is ServedCut => !!c)
console.log(`${decks.length} decks ${values.build ? 'built' : 'served'}, ${cuts.length} tunnel cuts (${((Date.now() - started) / 1000).toFixed(0)} s)`)

await dem.load([...decks.flatMap(d => d.points), ...cuts.flatMap(c => c.points)])
const ground = dem.at.bind(dem)

const issues = [
  // Its `floating` reads the ground at the end alone, where a deck run on over a hollow in the terrain stands over it by design: `seam` reads past the end too.
  ...deckDefects(decks).filter(d => d.kind !== 'floating').map(d => ({ structure: d.deck, ...d })),
  ...decks.flatMap(d => deckIssues(d, ground)).map(d => ({ structure: d.deck, ...d })),
  ...clashes(decks).map(d => ({ structure: d.deck, ...d })),
  ...lurches(decks).map(d => ({ structure: d.deck, ...d })),
  ...cuts.flatMap(c => cutIssues(c, ground)).map(c => ({ structure: c.cut, ...c })),
]

const byId = new Map<string, { type: 'deck' | 'tunnel'; kind: string; layer?: number; length: number }>()
for (const d of decks) byId.set(d.id, { type: 'deck', kind: d.kind, layer: d.layer, length: Math.round(d.length) })
for (const c of cuts) byId.set(c.id, { type: 'tunnel', kind: c.kind, length: 0 })

const lines = new Map<string, Point[]>([...decks.map(d => [d.id, d.points] as const), ...cuts.map(c => [c.id, c.points.slice(c.open, c.at + 1)] as const)])

/** A camera that looks along the structure toward a point on it: lat,lng,zoom,pitch,bearing. */
function camera(id: string, at: [number, number]): string {
  const ll = lines.get(id)!.map(lngLat)
  let i = 0
  for (let k = 1; k < ll.length; k++) if (Math.hypot(ll[k][0] - at[0], ll[k][1] - at[1]) < Math.hypot(ll[i][0] - at[0], ll[i][1] - at[1])) i = k
  const [a, b] = i >= ll.length / 2 ? [ll[Math.max(0, i - 1)], ll[i]] : [ll[Math.min(ll.length - 1, i + 1)], ll[i]]
  const bearing = (Math.atan2((b[0] - a[0]) * Math.cos((a[1] * Math.PI) / 180), b[1] - a[1]) * 180) / Math.PI
  return [at[1].toFixed(5), at[0].toFixed(5), 19.2, 70, Math.round(bearing)].join(',')
}

const ranked = score(issues).map(r => ({ ...r, ...byId.get(r.structure), camera: camera(r.structure, r.at) }))
const counts: Record<string, number> = {}
for (const i of issues) counts[`${byId.get(i.structure)?.type}:${i.kind}`] = (counts[`${byId.get(i.structure)?.type}:${i.kind}`] ?? 0) + 1
const km = decks.reduce((t, d) => t + d.length, 0) / 1000
const summary = {
  bbox: [w, s, e, n], tiles: tiles.length, decks: decks.length, deckKm: Math.round(km * 10) / 10, cuts: cuts.length,
  flaggedDecks: ranked.filter(r => r.type === 'deck').length, flaggedCuts: ranked.filter(r => r.type === 'tunnel').length,
  counts: Object.fromEntries(Object.entries(counts).sort()), score: Math.round(ranked.reduce((t, r) => t + r.score, 0)),
}
mkdirSync(values.out!, { recursive: true })
writeFileSync(`${values.out}/summary.json`, JSON.stringify(summary, null, 2))
writeFileSync(`${values.out}/issues.json`, JSON.stringify(issues))
writeFileSync(`${values.out}/ranked.tsv`, ['score\ttype\tkind\tlayer\tlength\tstructure\tcamera\tissues', ...ranked.map(r => [r.score, r.type, r.kind, r.layer ?? '', r.length, r.structure, r.camera, r.summary].join('\t'))].join('\n'))
console.log(JSON.stringify(summary, null, 2))
process.exit(0)
