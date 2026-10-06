/**
 * 56 Leonard Street ("the Jenga building", Herzog & de Meuron, 2017) —
 * original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/56-leonard.ts
 *
 * x = across the Tribeca grid (121°), y = along it (31°), z = metres up.
 * Anchor is the centroid of OSM way/261499928 (40.7176453, -74.0062596);
 * bearing 31°, elevation 0 (Tribeca is flat under the footprint). OSM records
 * no building:parts, so the stacking comes from photos: a near-straight shaft
 * of two-floor glass boxes with small irregular offsets, then a crown of
 * one- and two-floor boxes split in two or three, each level throwing one
 * side far out and turning a quarter round from the last. Every box sits on a pale slab, which is what stripes the tower.
 * Anish Kapoor's mirrored sculpture sits in a notch at the Leonard/Church
 * corner, under the building.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Rim = { points: XY[]; normals: V3[] }

const glass = new Part(), slab = new Part(), terrace = new Part()
const roof = new Part(), mirror = new Part()

const HEIGHT = 250 // OSM height, the main roof
const FLOOR = 4.17 // 60 levels over 250 m
const SLAB = 1.25 // the pale band at the foot of every box, thick enough to read on a phone
const RECESS = 0.45 // glass set back behind the slab edge
const BEVEL = 0.45

// OSM outline in the grid frame. The south-east corner has a 5.6 × 3.4 m
// notch that belongs to the neighbour; the low boxes keep out of it.
const X0 = -19.4, X1 = 19.9, Y0 = -17.9, Y1 = 17.2
const NOTCH: XY = [14.3, -14.1]

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map((n) => n / l) as V3 }

/** Round every convex corner with two points and smooth normals. */
function soften(ring: XY[], radius = BEVEL): Rim {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const l0 = Math.hypot(b[0] - a[0], b[1] - a[1]), l1 = Math.hypot(c[0] - b[0], c[1] - b[1])
    const u: XY = [(b[0] - a[0]) / l0, (b[1] - a[1]) / l0], v: XY = [(c[0] - b[0]) / l1, (c[1] - b[1]) / l1]
    const turn = u[0] * v[1] - u[1] * v[0]
    if (turn < -1e-5) { points.push(b); normals.push(unit([u[1] + v[1], -u[0] - v[0], 0])); continue }
    const r = Math.min(radius, l0 * 0.3, l1 * 0.3)
    points.push([b[0] - u[0] * r, b[1] - u[1] * r], [b[0] + v[0] * r, b[1] + v[1] * r])
    normals.push([u[1], -u[0], 0], [v[1], -v[0], 0])
  }
  return { points, normals }
}

/** A ring pushed in by `d` along each edge's normal (axis-aligned rings only). */
function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

function wall(p: Part, rim: Rim, z0: number, z1: number) {
  const n = rim.points.length
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n
    const [a, b] = [rim.points[i], rim.points[j]]
    const [na, nb] = [rim.normals[i], rim.normals[j]]
    p.tri([a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], undefined, undefined, undefined, [na, nb, nb])
    p.tri([a[0], a[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], undefined, undefined, undefined, [na, nb, na])
  }
}

/** A flat cap fanned from the centroid; the rings here are star-shaped about it. */
function cap(p: Part, ring: XY[], z: number, up: boolean) {
  const c: V3 = [ring.reduce((s, v) => s + v[0], 0) / ring.length, ring.reduce((s, v) => s + v[1], 0) / ring.length, z]
  for (let i = 0; i < ring.length; i++) {
    const a: V3 = [...ring[i], z], b: V3 = [...ring[(i + 1) % ring.length], z]
    if (up) p.tri(c, a, b); else p.tri(c, b, a)
  }
}

const rect = (x0: number, x1: number, y0: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]

/**
 * One stacked box: a pale slab, then glass set back behind it, capped with
 * a terrace where nothing above covers it.
 */
function box(ring: XY[], z0: number, z1: number, top = terrace) {
  const outer = soften(ring)
  wall(slab, outer, z0, z0 + SLAB)
  cap(slab, outer.points, z0, false)
  cap(slab, outer.points, z0 + SLAB, true)
  const inner = inset(ring, RECESS)
  wall(glass, soften(inner), z0 + SLAB, z1)
  cap(top, soften(inner).points, z1, true)
}

// Deterministic jitter, so the jumble is the same on every run.
let seed = 56
const rand = () => {
  seed = (seed + 0x6d2b79f5) | 0
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296
}
const span = (lo: number, hi: number) => lo + (hi - lo) * rand()

// The shaft's own plan, a metre inside the outline so the jitter can push out.
const C = { x0: X0 + 1, x1: X1 - 1, y0: Y0 + 1, y1: Y1 - 1 }

// Ground floors: glass lobby with the sculpture's corner cut out.
const KAPOOR = { x: X1 - 7.5, y: 2 }
{
  const ring: XY[] = [[C.x0, C.y0], [NOTCH[0], C.y0], [NOTCH[0], NOTCH[1]], [C.x1, NOTCH[1]],
    [C.x1, KAPOOR.y], [KAPOOR.x, KAPOOR.y], [KAPOOR.x, C.y1], [C.x0, C.y1]]
  box(ring, 0, 2 * FLOOR)
}

// The shaft: two floors a box, each side nudged a little in or out, growing
// slightly towards the crown as the real balconies do.
const SHAFT_TOP = 36 * FLOOR
let z = 2 * FLOOR
while (z < SHAFT_TOP - 1) {
  const t = z / SHAFT_TOP
  const j = () => span(-0.4, 0.4 + 2.2 * t * t)
  const x0 = C.x0 - j(), x1 = C.x1 + j(), y0 = C.y0 - j(), y1 = C.y1 + j()
  const ring: XY[] = z < 7 * FLOOR
    ? [[x0, y0], [NOTCH[0] - 0.5, y0], [NOTCH[0] - 0.5, NOTCH[1] - 0.5], [x1, NOTCH[1] - 0.5], [x1, y1], [x0, y1]]
    : rect(x0, x1, y0, y1)
  box(ring, z, z + 2 * FLOOR)
  z += 2 * FLOOR
}

// The crown: each level split along alternating axes, each piece
// cantilevered by its own amount, harder the higher it sits.
const CROWN_TOP = HEIGHT - 3.5
for (let k = 0; z < CROWN_TOP - 1; k++) {
  const h = Math.min(CROWN_TOP - z, (k % 3 === 1 ? 1 : 2) * FLOOR)
  const t = (z - SHAFT_TOP) / (CROWN_TOP - SHAFT_TOP)
  // Each level throws one side far out and nudges the rest, turning a
  // quarter each time so the overhangs spiral round the crown as they do.
  const reach = 4 + 7 * t
  const lead = (k * 3 + (rand() < 0.5 ? 0 : 2)) % 4 // 0 west, 1 east, 2 south, 3 north
  // The top few floors spread on every side, so the crown is wider than the shaft.
  const push = (side: number) => side === lead ? span(reach * 0.6, reach)
    : t > 0.7 ? span(1, 4) : (rand() < 0.35 ? span(-2.5, -0.5) : span(0, 2.5))
  const last = z + h >= CROWN_TOP - 1
  const top = last ? roof : terrace
  // Two or three pieces, split across alternating axes, each pushed out on
  // its own exposed sides: the Jenga stack.
  const pieces = k % 3 === 2 ? 3 : 2
  const alongX = k % 2 === 0
  const cuts = pieces === 2 ? [span(0.35, 0.65)] : [span(0.25, 0.4), span(0.6, 0.75)]
  const edges = [0, ...cuts, 1]
  for (let p = 0; p < pieces; p++) {
    const first = p === 0, lastPiece = p === pieces - 1
    if (alongX) {
      const a = C.x0 + (C.x1 - C.x0) * edges[p], b = C.x0 + (C.x1 - C.x0) * edges[p + 1]
      const w = push(0), e = push(1), so = push(2), n = push(3)
      box(rect(first ? a - w : a, lastPiece ? b + e : b, C.y0 - so, C.y1 + n), z, z + h, top)
    } else {
      const a = C.y0 + (C.y1 - C.y0) * edges[p], b = C.y0 + (C.y1 - C.y0) * edges[p + 1]
      const w = push(0), e = push(1), so = push(2), n = push(3)
      box(rect(C.x0 - w, C.x1 + e, first ? a - so : a, lastPiece ? b + n : b), z, z + h, top)
    }
  }
  z += h
}

// Roof: an open pale frame around the terrace, as on the real top.
{
  const o = rect(C.x0 + 2, C.x1 - 2, C.y0 + 2, C.y1 - 2), i = inset(o, 1)
  const z0 = CROWN_TOP, z1 = HEIGHT
  wall(slab, soften(o), z0, z1)
  const inn = soften(i)
  wall(slab, { points: [...inn.points].reverse(), normals: [...inn.normals].reverse().map((n) => n.map((v) => -v) as V3) }, z0, z1)
  const op = soften(o).points, ip = inn.points
  // Top of the frame: a strip between the two rounded rings, same point count.
  for (let k = 0; k < op.length; k++) {
    const l = (k + 1) % op.length
    slab.quad([...op[k], z1], [...op[l], z1], [...ip[l], z1], [...ip[k], z1])
  }
  // A plant room inside it.
  box(rect(C.x0 + 6, C.x0 + 18, C.y0 + 6, C.y1 - 8), z0, z0 + 3, roof)
}

// Kapoor's sculpture: a squashed mirror blob, 14.6 m long and 5.8 m high,
// pressed into the corner notch.
{
  const cx = (KAPOOR.x + C.x1) / 2 + 0.5, cy = (KAPOOR.y + C.y1) / 2 + 0.8
  const rx = 3.4, ry = 7.3, rz = 5.6
  const around = 16, up = 7
  const point = (i: number, k: number): [V3, V3] => {
    const a = (i / around) * Math.PI * 2, b = (k / up) * Math.PI / 2 // 0 = ground, π/2 = top
    const squash = 1 - 0.18 * Math.cos(2 * a) // pinched waist along its length
    const x = Math.cos(a) * Math.cos(b) * rx * squash, y = Math.sin(a) * Math.cos(b) * ry
    const h = Math.sin(b) * rz
    return [[cx + x, cy + y, h], unit([x / (rx * rx), y / (ry * ry), h / (rz * rz)])]
  }
  for (let k = 0; k < up; k++)
    for (let i = 0; i < around; i++) {
      const [a, na] = point(i, k), [b, nb] = point(i + 1, k), [c, nc] = point(i + 1, k + 1), [d, nd] = point(i, k + 1)
      mirror.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
      if (k < up - 1) mirror.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
    }
}

const parts = [
  { part: glass, material: { name: 'glass-bands', color: 0x93a8bb, roughness: 0.7 } },
  { part: slab, material: { name: 'slab-edges', color: 0xe4e1d9 } },
  { part: terrace, material: { name: 'concrete-terraces', color: 0xd3cfc6 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
  { part: mirror, material: { name: 'mirror-sculpture', color: 0xc9ced3, roughness: 0.35 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('56 Leonard Street', parts, {
  license: 'CC0-1.0', bearing: 31, elevation: 0, anchor: [40.7176453, -74.0062596], height: HEIGHT,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  footprint: { x: [X0, X1], y: [Y0, Y1] },
  note: 'Stacked glass boxes on pale slabs; crown cantilevers from photos (OSM has no parts)',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const outPath = new URL('../../landmarks/models/56-leonard.glb', import.meta.url).pathname
await Bun.write(outPath, glb)
console.log(`${outPath}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
