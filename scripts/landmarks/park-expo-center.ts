/**
 * The Park Expo and Conference Center, Charlotte — procedural, CC0-1.0.
 * bun scripts/landmarks/park-expo-center.ts
 *
 * Map frame: x east, y north, z up, metres; placed at bearing 0. The origin is
 * the area centroid of the five OSM buildings it replaces, whose outlines are
 * used as they are (rounded to 0.1 m):
 * - Independence Hall (way/836535381): the long brick hall on Independence
 *   Boulevard, with a glazed entrance vestibule on its north-east face;
 * - Freedom Hall (way/836535382): the taller brick hall to its west;
 * - the low lobby between them (way/836535383) and the covered link south to
 * - Liberty Hall (way/836535385): the big pale precast hall with the curved
 *   south wall.
 *
 * Nothing in OSM gives heights. They are read off the Commons aerial
 * "Bojangles Coliseum and The Park, Charlotte, NC" (panoramio), scaled by the
 * cars on the boulevard, and a Mapillary view of Liberty Hall's east end:
 * Independence 10 m, Freedom 13.5 m, Liberty 10.5 m, the lobby 6 m, the link 5 m.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const brick = new Part(), stone = new Part(), roof = new Part(), membrane = new Part()
const win = new Part(), door = new Part()

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3) {
  const N = n && [n, n, n]
  p.tri(a, b, c, undefined, undefined, undefined, N)
  p.tri(a, c, d, undefined, undefined, undefined, N)
}

/** Ear-clipping triangulation of a simple counter-clockwise polygon. */
function earcut(pts: XY[]): [number, number, number][] {
  const idx = pts.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) > 0 && crossz(b, c, p) > 0 && crossz(c, a, p) > 0
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = pts[i0], b = pts[i1], c = pts[i2]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && inside(pts[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}
/** Move every edge of a counter-clockwise ring outward by d, mitred. */
function offset(pts: XY[], d: number): XY[] {
  const n = pts.length
  return pts.map((p, i) => {
    const a = pts[(i + n - 1) % n], b = pts[(i + 1) % n]
    const e0 = unit([p[0] - a[0], p[1] - a[1], 0]), e1 = unit([b[0] - p[0], b[1] - p[1], 0])
    const n0: XY = [e0[1], -e0[0]], n1: XY = [e1[1], -e1[0]]
    const m = unit([n0[0] + n1[0], n0[1] + n1[1], 0]), k = d / Math.max(0.35, m[0] * n0[0] + m[1] * n0[1])
    return [p[0] + m[0] * k, p[1] + m[1] * k]
  })
}
const area = (pts: XY[]) => pts.reduce((s, p, i) => { const q = pts[(i + 1) % pts.length]; return s + p[0] * q[1] - q[0] * p[1] }, 0) / 2
const ccw = (pts: XY[]) => (area(pts) < 0 ? [...pts].reverse() : pts)

// ---------------------------------------------------------------------------
// Buildings.

type Opening = { s0: number; s1: number; z0: number; z1: number; part: Part }
type Building = {
  name: string; pts: XY[]; h: number; wall: Part; top: Part
  /** Pilaster spacing on long faces, if the real walls have them. */
  pilasters?: number
  /** Window and door panels, by the index of the edge they sit on. */
  openings?: Record<number, Opening[]>
}

const independence: Building = {
  name: 'Independence Hall', h: 10, wall: brick, top: roof, pilasters: 9.5,
  pts: [[-47.8, 70.6], [-27.7, 57.5], [18.3, 27.8], [56.3, 3.2], [74.1, 32.0], [80.3, 42.0], [89.1, 56.1],
    [45.5, 83.9], [47.3, 86.6], [31.8, 96.5], [30.2, 94.0], [-14.1, 122.4]],
  // The vestibule: doors below a band of glazing across its 18 m front, and
  // glazing on its two short returns.
  openings: {
    7: [{ s0: 0.5, s1: 2.7, z0: 1.0, z1: 8.6, part: win }],
    8: [{ s0: 1.2, s1: 17.2, z0: 0.2, z1: 3.4, part: door }, { s0: 1.2, s1: 17.2, z0: 4.0, z1: 8.6, part: win }],
    9: [{ s0: 0.5, s1: 2.7, z0: 1.0, z1: 8.6, part: win }],
  },
}
const freedom: Building = {
  name: 'Freedom Hall', h: 13.5, wall: brick, top: roof, pilasters: 8.5,
  pts: [[-47.8, 70.6], [-78.6, 89.1], [-83.8, 78.7], [-87.2, 81.5], [-94.9, 71.3], [-90.6, 68.8], [-101.4, 51.7],
    [-105.1, 54.3], [-109.2, 48.3], [-105.5, 46.2], [-116.7, 30.9], [-91.4, 13.9], [-86.7, 21.7], [-60.9, 5.3],
    [-57.2, 11.8], [-53.0, 9.6], [-48.2, 16.8], [-51.5, 19.4], [-41.0, 36.4], [-37.8, 41.6], [-27.7, 57.5]],
}
const lobby: Building = {
  name: 'lobby', h: 6, wall: stone, top: membrane,
  pts: [[-41.0, 36.4], [-28.2, 29.0], [-32.6, 6.3], [-31.0, 6.0], [-25.7, 5.1], [-10.5, 2.6], [3.6, 0.2],
    [12.9, 15.1], [10.6, 16.7], [18.3, 27.8], [-27.7, 57.5], [-37.8, 41.6]],
  // Its glazed east front on the parking side.
  openings: { 6: [{ s0: 1.5, s1: 16, z0: 0.2, z1: 4.6, part: win }] },
}
const link: Building = {
  name: 'link', h: 5, wall: stone, top: membrane,
  pts: [[-25.7, 5.1], [-30.1, -21.0], [-30.9, -25.7], [-31.7, -30.8], [-16.9, -33.4], [-10.5, 2.6]],
  openings: {
    0: [{ s0: 2, s1: 24, z0: 0.8, z1: 4.0, part: win }],
    4: [{ s0: 2, s1: 34.5, z0: 0.8, z1: 4.0, part: win }],
  },
}
const liberty: Building = {
  name: 'Liberty Hall', h: 10.5, wall: stone, top: membrane,
  pts: [[-31.7, -30.8], [-36.2, -30.0], [-37.5, -39.0], [-42.4, -38.1], [-43.2, -47.3], [-47.9, -46.5],
    [-49.0, -55.6], [-53.8, -54.6], [-55.4, -63.5], [-61.3, -62.0], [-63.1, -75.0], [-45.1, -90.9],
    [-21.0, -104.5], [0.0, -111.3], [23.8, -115.3], [46.8, -115.0], [66.7, -111.4], [88.6, -102.3],
    [93.6, -73.1], [84.8, -71.5], [88.3, -52.3], [-16.9, -33.4]],
}
const buildings = [independence, freedom, lobby, link, liberty]
for (const b of buildings) {
  const fixed = ccw(b.pts)
  if (fixed !== b.pts && b.openings) throw new Error(`${b.name}: openings are indexed on a clockwise ring`)
  b.pts = fixed
}

/** The height of the building whose outline runs along this edge, if another does. */
function neighbourHeight(self: Building, a: XY, b: XY) {
  const m: XY = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2]
  let h = 0
  for (const o of buildings) {
    if (o === self) continue
    for (let i = 0; i < o.pts.length; i++) {
      const p = o.pts[i], q = o.pts[(i + 1) % o.pts.length]
      const L = Math.hypot(q[0] - p[0], q[1] - p[1])
      const t = ((m[0] - p[0]) * (q[0] - p[0]) + (m[1] - p[1]) * (q[1] - p[1])) / (L * L)
      if (t < 0 || t > 1) continue
      const d = Math.abs((m[0] - p[0]) * (q[1] - p[1]) - (m[1] - p[1]) * (q[0] - p[0])) / L
      if (d < 0.3) h = Math.max(h, o.h)
    }
  }
  return h
}

const PARAPET = 0.45
for (const b of buildings) {
  const n = b.pts.length
  const top = offset(b.pts, -PARAPET)
  for (let i = 0; i < n; i++) {
    const A = b.pts[i], B = b.pts[(i + 1) % n], a = top[i], c = top[(i + 1) % n]
    const L = Math.hypot(B[0] - A[0], B[1] - A[1])
    const u: XY = [(B[0] - A[0]) / L, (B[1] - A[1]) / L]
    const out: V3 = [u[1], -u[0], 0]
    // A wall against a taller neighbour is inside it: start this one at its roof.
    const hidden = neighbourHeight(b, A, B)
    const z0 = hidden >= b.h ? b.h - PARAPET : Math.max(0, hidden - 0.5)
    const at = (s: number, z: number, d = 0): V3 => [A[0] + u[0] * s + out[0] * d, A[1] + u[1] * s + out[1] * d, z]
    if (z0 < b.h - PARAPET) quadN(b.wall, at(0, z0), at(L, z0), at(L, b.h - PARAPET), at(0, b.h - PARAPET), out)
    // The bevelled coping.
    quadN(b.wall, at(0, b.h - PARAPET), at(L, b.h - PARAPET), [c[0], c[1], b.h], [a[0], a[1], b.h], unit([out[0], out[1], 1]))
    if (hidden >= b.h) continue
    // Window and door panels, 4 cm proud of the wall.
    for (const o of b.openings?.[i] ?? [])
      quadN(o.part, at(o.s0, o.z0, 0.04), at(o.s1, o.z0, 0.04), at(o.s1, o.z1, 0.04), at(o.s0, o.z1, 0.04), out)
    // Pilasters: shallow bevelled piers, evenly spaced, clear of the corners.
    if (b.pilasters && L > 2.5 * b.pilasters && !b.openings?.[i]) {
      const count = Math.round(L / b.pilasters) - 1, w = 0.6, D = 0.35, ch = 0.12
      for (let k = 1; k <= count; k++) {
        const s = (L * k) / (count + 1)
        const zb = Math.max(z0, hidden), zt = b.h - PARAPET
        const prof: [number, number][] = [[-w, 0], [-w, D - ch], [-w + ch, D], [w - ch, D], [w, D - ch], [w, 0]]
        for (let j = 0; j < prof.length - 1; j++) {
          const [s0, d0] = prof[j], [s1, d1] = prof[j + 1]
          // The face's normal in the (along, out) plane: its direction turned clockwise.
          const nn = unit([(s1 - s0) * out[0] - (d1 - d0) * u[0], (s1 - s0) * out[1] - (d1 - d0) * u[1], 0])
          quadN(b.wall, at(s + s0, zb, d0), at(s + s1, zb, d1), at(s + s1, zt, d1), at(s + s0, zt, d0), nn)
        }
        const cap = prof.map(([ss, dd]) => at(s + ss, zt, dd))
        for (let j = 1; j < cap.length - 1; j++) b.wall.tri(cap[0], cap[j], cap[j + 1])
      }
    }
  }
  for (const [i, j, k] of earcut(top)) b.top.tri([top[i][0], top[i][1], b.h], [top[j][0], top[j][1], b.h], [top[k][0], top[k][1], b.h])
}

// ---------------------------------------------------------------------------

// Colours from the daylight Commons aerial and Mapillary: red-brown brick
// (pulled to the palette's lightness), pale buff precast on Liberty Hall, the
// lobby and the link, dark grey membrane on the brick halls and light grey on
// the rest, slate glazing.
const parts = [
  { part: brick, material: finish('expo-brick', 0xc4846d) },
  { part: stone, material: PALETTE.stone },
  { part: roof, material: PALETTE.roof },
  { part: membrane, material: finish('membrane', 0xd8d8d3) },
  { part: win, material: PALETTE.window },
  { part: door, material: PALETTE.entrance },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Park Expo and Conference Center', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 0, osm: buildings.map((b) => b.name), height: Math.max(...buildings.map((b) => b.h)),
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/park-expo-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
