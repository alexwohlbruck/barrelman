/**
 * Charlotte Douglas International Airport (CLT): the terminal headhouse —
 * procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/clt-terminal.ts
 *
 * Also the small kit the other CLT models import (`clt-concourse-a`,
 * `clt-concourses-b-c`, `clt-concourses-d-e`, the parking decks and the
 * tower): footprint clean-up, ear-clipping caps, bevelled blocks, window
 * panels and banded parking floors. Importing this file builds nothing; the
 * terminal is only written when it is run directly.
 *
 * Frame. Every CLT model is drawn in one site frame: x along the terminal's
 * long axis (bearing 85.8°), y towards 355.8°, z up, metres, so all of them
 * are placed at bearing 355.8. Footprints are OSM outlines in that frame, and
 * each model is shifted so its anchor (the centre of its group's bounding
 * box) is the origin.
 *
 * The headhouse here is four OSM ways:
 * - Main Terminal (way/1414163868): ticketing and baggage claim. Its front
 *   strip, facing the curb, is the 2020s terminal lobby expansion: a glass
 *   wall under a big curved roof that rises from the old roofline to a crest
 *   and sweeps down to a deep eave over the curb.
 * - the roadway canopy (way/1414165678, building=roof): the glass canopy over
 *   the two-level roadway, about 13,600 m² (Wikipedia gives 146,000 sq ft).
 * - the Atrium (way/1347416658): the airside hall with rocking chairs, under a
 *   long glazed vault on white trusses (Commons "CLT Airport Atrium").
 * - The Plaza (way/1347417381): the three-storey 2019 east expansion.
 *
 * OSM gives no heights. These are judged from photos and storey counts: a
 * 16 m older roofline, the lobby crest at 29 m, a 12 m atrium with its vault
 * to 19 m, an 18 m Plaza, the canopy arched from 15 to 17 m over the departures deck.
 */
import { Part, writeGlb } from './mesh'
import { PALETTE, finish, type Swatch } from './palette'

export type XY = [number, number]
export const BEARING = 355.8

// ---------------------------------------------------------------------------
// Polygons

export const area = (r: XY[]) => {
  let s = 0
  for (let i = 0; i < r.length; i++) {
    const a = r[i], b = r[(i + 1) % r.length]
    s += a[0] * b[1] - b[0] * a[1]
  }
  return s / 2
}

/** Drop the closing point, near-duplicates and collinear points; make it counter-clockwise. */
export function clean(ring: XY[], tol = 0.35): XY[] {
  let r = ring.slice()
  if (r.length > 1 && r[0][0] === r.at(-1)![0] && r[0][1] === r.at(-1)![1]) r.pop()
  for (let changed = true; changed && r.length > 3;) {
    changed = false
    for (let i = 0; i < r.length && r.length > 3; i++) {
      const p = r[(i - 1 + r.length) % r.length], c = r[i], n = r[(i + 1) % r.length]
      const lpc = Math.hypot(c[0] - p[0], c[1] - p[1])
      const cr = Math.abs((c[0] - p[0]) * (n[1] - p[1]) - (c[1] - p[1]) * (n[0] - p[0]))
      const lpn = Math.hypot(n[0] - p[0], n[1] - p[1]) || 1
      if (lpc < tol || cr / lpn < tol * 0.25) { r.splice(i, 1); changed = true; i-- }
    }
  }
  if (area(r) < 0) r.reverse()
  return r
}

/** Offset a counter-clockwise ring inwards by d (outwards if negative), mitred and clamped. */
export function inset(r: XY[], d: number): XY[] {
  const n = r.length
  const nrm = (a: XY, b: XY): XY => {
    const l = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
    return [-(b[1] - a[1]) / l, (b[0] - a[0]) / l] // left of travel = inwards
  }
  return r.map((c, i) => {
    const n1 = nrm(r[(i - 1 + n) % n], c), n2 = nrm(c, r[(i + 1) % n])
    const k = 1 + n1[0] * n2[0] + n1[1] * n2[1]
    let ox = (n1[0] + n2[0]) / Math.max(k, 0.2), oy = (n1[1] + n2[1]) / Math.max(k, 0.2)
    const l = Math.hypot(ox, oy)
    if (l > 2.5) { ox *= 2.5 / l; oy *= 2.5 / l }
    return [c[0] + ox * d, c[1] + oy * d] as XY
  })
}

/** Ear clipping for a simple counter-clockwise ring. */
export function triangulate(r: XY[]): [number, number, number][] {
  const idx = r.map((_, i) => i)
  const out: [number, number, number][] = []
  const cross = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) =>
    cross(a, b, p) > 1e-9 && cross(b, c, p) > 1e-9 && cross(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 20000) {
    let cut = false
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const a = r[ia], b = r[ib], c = r[ic]
      if (cross(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== ia && j !== ib && j !== ic && inside(r[j], a, b, c))) continue
      out.push([ia, ib, ic]); idx.splice(i, 1); cut = true; break
    }
    if (!cut) { // degenerate leftovers: drop the flattest vertex
      idx.splice(0, 1)
    }
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

/** Sutherland–Hodgman clip against the half-plane a·x + b·y <= c. */
export function clip(r: XY[], a: number, b: number, c: number): XY[] {
  const out: XY[] = []
  const f = (p: XY) => a * p[0] + b * p[1] - c
  for (let i = 0; i < r.length; i++) {
    const p = r[i], q = r[(i + 1) % r.length], fp = f(p), fq = f(q)
    if (fp <= 0) out.push(p)
    if ((fp < 0 && fq > 0) || (fp > 0 && fq < 0)) {
      const t = fp / (fp - fq)
      out.push([p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t])
    }
  }
  return out
}

export const rect = (x0: number, y0: number, x1: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]

export function circle(cx: number, cy: number, r: number, seg = 16, a0 = 0): XY[] {
  return Array.from({ length: seg }, (_, i) => {
    const a = a0 + (i / seg) * 2 * Math.PI
    return [cx + r * Math.cos(a), cy + r * Math.sin(a)] as XY
  })
}

// ---------------------------------------------------------------------------
// Solids

/** Walls of a counter-clockwise ring between two heights, facing out. */
export function walls(p: Part, r: XY[], z0: number, z1: number, r1: XY[] = r) {
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length
    p.quad([r[i][0], r[i][1], z0], [r[j][0], r[j][1], z0], [r1[j][0], r1[j][1], z1], [r1[i][0], r1[i][1], z1])
  }
}

export function cap(p: Part, r: XY[], z: number, up = true) {
  for (const [a, b, c] of triangulate(r)) {
    const A: [number, number, number] = [r[a][0], r[a][1], z], B: [number, number, number] = [r[b][0], r[b][1], z], C: [number, number, number] = [r[c][0], r[c][1], z]
    if (up) p.tri(A, B, C)
    else p.tri(A, C, B)
  }
}

/** A flat-roofed block with a chamfered parapet edge. */
export function block(wall: Part, top: Part | null, ring: XY[], z0: number, z1: number, bevel = 0.5, bottom?: Part) {
  const r = clean(ring)
  if (bevel > 0) {
    walls(wall, r, z0, z1 - bevel)
    const ins = inset(r, bevel)
    walls(wall, r, z1 - bevel, z1, ins)
    if (top) cap(top, ins, z1)
  } else {
    walls(wall, r, z0, z1)
    if (top) cap(top, r, z1)
  }
  if (bottom) cap(bottom, r, z0, false)
  return r
}

/**
 * Window panels on every wall of a ring long enough to take one: slate
 * panels set 4 cm proud of the wall, in bays no wider than `bay`, with wall
 * showing between them as piers.
 */
export function panels(p: Part, ring: XY[], z0: number, z1: number, o: { bay?: number; gap?: number; end?: number; minLen?: number; out?: number } = {}) {
  const bay = o.bay ?? 9, gap = o.gap ?? 1.6, end = o.end ?? 1.2, minLen = o.minLen ?? 5, out = o.out ?? 0.04
  const r = ring
  for (let i = 0; i < r.length; i++) {
    const a = r[i], b = r[(i + 1) % r.length]
    const L = Math.hypot(b[0] - a[0], b[1] - a[1])
    if (L < minLen) continue
    const u: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L], n: XY = [u[1], -u[0]]
    const usable = L - 2 * end
    const k = Math.max(1, Math.round((usable + gap) / (bay + gap)))
    const w = (usable - (k - 1) * gap) / k
    for (let q = 0; q < k; q++) {
      const s0 = end + q * (w + gap), s1 = s0 + w
      const P = (s: number, z: number): [number, number, number] => [a[0] + u[0] * s + n[0] * out, a[1] + u[1] * s + n[1] * out, z]
      p.quad(P(s0, z0), P(s1, z0), P(s1, z1), P(s0, z1))
    }
  }
}

/**
 * A band around a ring: its outer face, with a top and bottom ledge running
 * in to `depth`. The parking decks are stacks of these over a dark core.
 */
export function band(p: Part, r: XY[], z0: number, z1: number, depth: number) {
  const ins = inset(r, depth)
  walls(p, r, z0, z1)
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length
    p.quad([r[i][0], r[i][1], z1], [r[j][0], r[j][1], z1], [ins[j][0], ins[j][1], z1], [ins[i][0], ins[i][1], z1])
    p.quad([r[i][0], r[i][1], z0], [ins[i][0], ins[i][1], z0], [ins[j][0], ins[j][1], z0], [r[j][0], r[j][1], z0])
  }
}

/** A surface of revolution about (cx, cy) from a (radius, z) profile, smooth-shaded. */
export function lathe(p: Part, cx: number, cy: number, prof: XY[], seg = 16, a0 = 0) {
  const nrm = prof.map((_, i) => {
    const [r0, z0] = prof[Math.max(0, i - 1)], [r1, z1] = prof[Math.min(prof.length - 1, i + 1)]
    const l = Math.hypot(z1 - z0, r1 - r0) || 1
    return [(z1 - z0) / l, -(r1 - r0) / l] as XY // (radial, up)
  })
  for (let k = 0; k < seg; k++) {
    const a = a0 + (k / seg) * 2 * Math.PI, b = a0 + ((k + 1) / seg) * 2 * Math.PI
    const P = ([r, z]: XY, t: number): [number, number, number] => [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
    const N = (n: XY, t: number): [number, number, number] => [n[0] * Math.cos(t), n[0] * Math.sin(t), n[1]]
    for (let i = 0; i < prof.length - 1; i++) {
      if (prof[i][0] < 1e-6 && prof[i + 1][0] < 1e-6) continue
      const A = P(prof[i], a), B = P(prof[i], b), C = P(prof[i + 1], b), D = P(prof[i + 1], a)
      const nA = N(nrm[i], a), nB = N(nrm[i], b), nC = N(nrm[i + 1], b), nD = N(nrm[i + 1], a)
      if (prof[i][0] > 1e-6) p.tri(A, B, C, undefined, undefined, undefined, [nA, nB, nC])
      if (prof[i + 1][0] > 1e-6 || prof[i][0] > 1e-6) p.tri(A, C, D, undefined, undefined, undefined, [nA, nC, nD])
    }
  }
}

/**
 * A glazed vault along the segment a→b, `w` wide, springing at z0 and rising
 * `rise`, with fan gables at both ends: the concourse skylights.
 */
export function vault(p: Part, a: XY, b: XY, w: number, z0: number, rise: number, seg = 8) {
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const u: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L], n: XY = [u[1], -u[0]] // n: right of a→b
  const at = (o: XY, t: number): [number, number, number] => {
    const s = -w / 2 + w * t
    return [o[0] + n[0] * s, o[1] + n[1] * s, z0 + rise * Math.sin(Math.PI * t)]
  }
  for (let i = 0; i < seg; i++) {
    const t0 = i / seg, t1 = (i + 1) / seg
    p.quad(at(b, t0), at(a, t0), at(a, t1), at(b, t1))
    if (i > 0) {
      p.tri(at(a, 0), at(a, t1), at(a, t0))
      p.tri(at(b, 0), at(b, t0), at(b, t1))
    }
  }
}

/** The long axis of a ring through its centroid, as the two ends of its extent. */
export function axis(r: XY[], trim0 = 0, trim1 = 0): [XY, XY] {
  const cx = r.reduce((s, p) => s + p[0], 0) / r.length, cy = r.reduce((s, p) => s + p[1], 0) / r.length
  let sxx = 0, syy = 0, sxy = 0
  for (const [x, y] of r) { sxx += (x - cx) ** 2; syy += (y - cy) ** 2; sxy += (x - cx) * (y - cy) }
  const th = 0.5 * Math.atan2(2 * sxy, sxx - syy), u: XY = [Math.cos(th), Math.sin(th)]
  const ss = r.map(([x, y]) => (x - cx) * u[0] + (y - cy) * u[1])
  const s0 = Math.min(...ss) + trim0, s1 = Math.max(...ss) - trim1
  return [[cx + u[0] * s0, cy + u[1] * s0], [cx + u[0] * s1, cy + u[1] * s1]]
}

/** A rounded rectangle, counter-clockwise; radii per corner (SW, SE, NE, NW). */
export function roundRect(x0: number, y0: number, x1: number, y1: number, rad: [number, number, number, number], seg = 4): XY[] {
  const out: XY[] = []
  const corners: [number, number, number, number][] = [
    [x0, y0, Math.PI, rad[0]], [x1, y0, 1.5 * Math.PI, rad[1]], [x1, y1, 0, rad[2]], [x0, y1, 0.5 * Math.PI, rad[3]],
  ]
  for (const [x, y, a0, r] of corners) {
    if (r <= 0) { out.push([x, y]); continue }
    const cx = x === x0 ? x0 + r : x1 - r, cy = y === y0 ? y0 + r : y1 - r
    for (let i = 0; i <= seg; i++) {
      const a = a0 + (i / seg) * (Math.PI / 2)
      out.push([cx + r * Math.cos(a), cy + r * Math.sin(a)])
    }
  }
  return out
}

/**
 * An open parking deck: a dark core set back from the edge, wrapped in
 * concrete spandrel bands at every floor, so it reads as horizontal bands of
 * concrete with dark open floors between. `floors` are the slab heights from
 * the ground up; the last is the roof deck, whose parapet band tops the deck.
 */
export function parkingDeck(parts: { concrete: Part; gap: Part; deck: Part }, ring: XY[], floors: number[], o: { band?: number; setback?: number } = {}) {
  const band_ = o.band ?? 1.1, setback = o.setback ?? 0.9
  const r = clean(ring, 1.0)
  const core = inset(r, setback)
  const top = floors.at(-1)!
  walls(parts.gap, core, 0, top)
  cap(parts.deck, core, top)
  for (const z of floors) ledgeBand(parts.concrete, r, Math.max(0, z - 0.35), z + band_, setback)
  return r
}

/** A band's outer face and its top ledge back to the core (the underside is never seen). */
export function ledgeBand(p: Part, r: XY[], z0: number, z1: number, depth: number) {
  const ins = inset(r, depth)
  walls(p, r, z0, z1)
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length
    p.quad([r[i][0], r[i][1], z1], [r[j][0], r[j][1], z1], [ins[j][0], ins[j][1], z1], [ins[i][0], ins[i][1], z1])
  }
}

/**
 * The open well in the middle of a helix ramp. The deck's outline already
 * bulges round the helix, so its bands wrap it; the well is a dark disc on
 * the roof deck with a parapet ring.
 */
export function helixWell(parts: { concrete: Part; gap: Part; deck: Part }, ring: XY[], top: number, seg = 14) {
  const r = clean(ring, 0.3)
  const cx = r.reduce((s, p) => s + p[0], 0) / r.length, cy = r.reduce((s, p) => s + p[1], 0) / r.length
  const rad = r.reduce((s, p) => s + Math.hypot(p[0] - cx, p[1] - cy), 0) / r.length
  const c = circle(cx, cy, rad, seg)
  cap(parts.gap, c, top + 0.03)
  // the parapet round the well, facing into it
  const inner = circle(cx, cy, rad - 0.5, seg)
  for (let i = 0; i < seg; i++) {
    const j = (i + 1) % seg
    parts.concrete.quad([c[j][0], c[j][1], top], [c[i][0], c[i][1], top], [c[i][0], c[i][1], top + 1.1], [c[j][0], c[j][1], top + 1.1])
    parts.concrete.quad([c[i][0], c[i][1], top + 1.1], [inner[i][0], inner[i][1], top + 1.1], [inner[j][0], inner[j][1], top + 1.1], [c[j][0], c[j][1], top + 1.1])
  }
}

// ---------------------------------------------------------------------------
// Output

/** Move a part's points so (cx, cy) in the site frame becomes the origin. */
function recentre(p: Part, cx: number, cy: number) {
  // Part stores glTF order (x, z_up, -y).
  for (let i = 0; i < p.pos.length; i += 3) { p.pos[i] -= cx; p.pos[i + 2] += cy }
}

export async function save(id: string, name: string, centre: XY, parts: { part: Part; material: Swatch }[], maxTri = 6500) {
  for (const { part } of parts) recentre(part, centre[0], centre[1])
  const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
  if (triangles > maxTri) throw new Error(`${id}: triangle budget exceeded: ${triangles}`)
  const glb = writeGlb(name, parts, {
    license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor', bearing: BEARING, elevation: 0,
  })
  if (glb.length > 250_000) throw new Error(`${id}: file budget exceeded: ${glb.length}`)
  const out = new URL(`../../landmarks/models/${id}.glb`, import.meta.url).pathname
  await Bun.write(out, glb)
  console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
}

/** Shared CLT colours: the terminal is white-panelled with grey glass and pale roofs. */
export const CLT = {
  stone: PALETTE.stone,
  trim: PALETTE.trim,
  roof: PALETTE.roof,
  window: PALETTE.window,
  glass: PALETTE.glass,
  // The silver metal of the new roofs' fascias and the canopy edges.
  silver: finish('silver', 0xd9dcdd, 0.5),
}

// ---------------------------------------------------------------------------
// The headhouse.

const MAIN: XY[] = [[63.8, 8.5], [59.7, 8.6], [59.6, 2.3], [58.7, 2.3], [58.7, -5.8], [56.4, -5.8], [56.4, -2.0], [33.8, -1.9], [33.8, -5.7], [21.9, -5.7], [-9.6, -5.7], [-9.4, -1.6], [-9.5, 13.3], [-63.9, 13.3], [-105.8, 13.4], [-179.3, 13.4], [-220.7, 13.1], [-271.5, 13.0], [-270.3, -8.3], [-266.5, -8.2], [-266.4, -44.5], [-266.5, -53.5], [-18.4, -53.6], [17.9, -54.7], [64.8, -54.7], [64.7, -74.4], [82.4, -74.3], [102.1, -74.3], [108.2, -74.2], [108.2, -44.2], [108.1, -37.8], [75.0, -37.7], [63.1, -37.6], [63.8, 8.5]]
const CANOPY: XY[] = [[-271.5, 13.0], [-274.1, 40.6], [-275.5, 55.6], [-276.2, 63.6], [-176.5, 63.3], [-110.8, 63.1], [-4.7, 62.7], [-7.1, 36.9], [-9.5, 13.3], [-63.9, 13.3], [-105.8, 13.4], [-179.3, 13.4], [-220.7, 13.1], [-271.5, 13.0]]
const ATRIUM: XY[] = [[-266.5, -53.5], [-266.6, -57.7], [-266.6, -64.3], [-255.2, -92.7], [-248.8, -94.4], [-224.7, -100.9], [-225.9, -97.6], [-195.9, -85.5], [-95.6, -85.2], [-92.0, -85.9], [-89.4, -87.2], [-68.5, -102.1], [-54.4, -112.3], [-41.1, -111.6], [-33.1, -109.7], [-31.7, -115.4], [-27.9, -114.7], [-30.8, -103.0], [-17.6, -84.3], [-14.5, -85.7], [-11.7, -86.2], [-7.7, -85.8], [-4.3, -84.3], [-1.5, -82.1], [0.5, -79.6], [1.4, -76.8], [1.5, -74.1], [1.1, -70.7], [-0.3, -67.8], [-1.9, -65.3], [-4.8, -63.1], [-8.1, -61.6], [-11.2, -61.2], [-11.2, -58.7], [-18.5, -58.6], [-18.4, -53.6], [-266.5, -53.5]]
const PLAZA: XY[] = [[84.6, 8.2], [79.2, 8.5], [63.8, 8.5], [63.1, -37.6], [75.0, -37.7], [108.1, -37.8], [108.2, -44.2], [165.3, -44.6], [165.6, -31.8], [157.5, -31.6], [146.7, -21.1], [137.7, -20.7], [137.8, -15.8], [139.0, -15.8], [138.9, -8.3], [133.5, -8.1], [133.6, -0.9], [124.6, -0.5], [124.5, -1.2], [121.8, -1.1], [121.8, -1.7], [96.7, -1.6], [96.8, 0.3], [91.3, 0.3], [91.2, 3.4], [84.5, 3.5], [84.6, 8.2]]
const CENTRE: XY = [-55.28, -25.9]

async function main() {
  const stone = new Part(), trim = new Part(), roof = new Part(), win = new Part(), glass = new Part(), silver = new Part()

  // --- Older terminal: everything but the lobby strip, at 16 m. -----------
  const LY = -24          // back of the lobby expansion
  const LX0 = -266.5, LX1 = -9.5
  const main = clean(MAIN)
  const back = clip(main, 0, 1, LY)                                   // y <= LY
  const eastStrip = clip(clip(main, 0, -1, -LY), -1, 0, -LX1)        // y >= LY, x >= LX1
  const westSliver = clip(clip(main, 0, -1, -LY), 1, 0, LX0)          // y >= LY, x <= LX0
  for (const r of [back, eastStrip, westSliver]) {
    const c = block(stone, roof, r, 0, 16, 0.5)
    panels(win, c, 4, 13, { bay: 10 })
  }

  // --- The lobby expansion: a curved roof over a glass wall. --------------
  // Section through the roof, from the back (y = LY) to the eave past the
  // front wall (y = 13): it rises out of the old roofline to a crest and
  // sweeps down to a deep eave over the departures curb.
  const FRONT = 13.2, EAVE = FRONT + 3.5
  const sect: XY[] = []
  for (let i = 0; i <= 10; i++) {
    const t = i / 10, y = LY + (EAVE - LY) * t
    // Crest at 60% of the way out, curved on both sides.
    const z = t < 0.62 ? 17 + 12 * Math.sin((t / 0.62) * Math.PI / 2) : 29 - 4 * (1 - Math.cos(((t - 0.62) / 0.38) * Math.PI / 2))
    sect.push([y, z])
  }
  const T = 1.2 // roof thickness at the edges, shown as a silver fascia
  const zAt = (y: number) => {
    for (let i = 0; i < sect.length - 1; i++)
      if (y <= sect[i + 1][0]) { const t = (y - sect[i][0]) / (sect[i + 1][0] - sect[i][0]); return sect[i][1] + (sect[i + 1][1] - sect[i][1]) * t }
    return sect.at(-1)![1]
  }
  const RX0 = -271.5, RX1 = LX1 + 0.8 // roof runs the full front, a little past the walls
  for (let i = 0; i < sect.length - 1; i++) {
    const [y0, z0] = sect[i], [y1, z1] = sect[i + 1]
    silver.quad([RX0, y0, z0], [RX1, y0, z0], [RX1, y1, z1], [RX0, y1, z1])
    // the ends: a fascia band following the curve
    trim.quad([RX0, y1, z1], [RX0, y1, z1 - T], [RX0, y0, z0 - T], [RX0, y0, z0])
    trim.quad([RX1, y0, z0], [RX1, y0, z0 - T], [RX1, y1, z1 - T], [RX1, y1, z1])
  }
  // Front fascia at the eave and the soffit back to the glass.
  const [ye, ze] = sect.at(-1)!
  trim.quad([RX0, ye, ze - T], [RX1, ye, ze - T], [RX1, ye, ze], [RX0, ye, ze])
  silver.quad([RX0, FRONT, zAt(FRONT) - T], [RX1, FRONT, zAt(FRONT) - T], [RX1, ye, ze - T], [RX0, ye, ze - T])
  // The back edge meets the old roof just above it.
  silver.quad([RX1, LY, 17 - T], [RX0, LY, 17 - T], [RX0, LY, 17], [RX1, LY, 17])

  // Walls under the roof: the glass front over the curb, stone ends.
  const yb = LY, zf = zAt(FRONT) - T
  win.quad([LX1, FRONT, 0], [LX0 - 4.9, FRONT, 0], [LX0 - 4.9, FRONT, zf], [LX1, FRONT, zf])
  // a few pale mullions and the departures floor line on the glass
  const span = LX1 - (LX0 - 4.9)
  for (let k = 1; k < 12; k++) {
    const x = LX0 - 4.9 + (span * k) / 12
    trim.quad([x + 0.4, FRONT + 0.05, 0], [x - 0.4, FRONT + 0.05, 0], [x - 0.4, FRONT + 0.05, zf], [x + 0.4, FRONT + 0.05, zf])
  }
  trim.quad([LX1, FRONT + 0.06, 7.6], [LX0 - 4.9, FRONT + 0.06, 7.6], [LX0 - 4.9, FRONT + 0.06, 8.6], [LX1, FRONT + 0.06, 8.6])
  // End walls follow the roof's underside, from the old roof up.
  for (const [x, dir] of [[LX1, 1], [LX0 - 4.9, -1]] as [number, number][]) {
    for (let i = 0; i < sect.length - 1; i++) {
      const y0 = Math.max(sect[i][0], yb), y1 = Math.min(sect[i + 1][0], FRONT)
      if (y1 <= y0) continue
      const a: [number, number, number] = [x, y0, 16], b: [number, number, number] = [x, y1, 16]
      const c: [number, number, number] = [x, y1, zAt(y1) - T], d: [number, number, number] = [x, y0, zAt(y0) - T]
      if (dir > 0) stone.quad(a, b, c, d)
      else stone.quad(b, a, d, c)
    }
  }
  // Below the old roofline the west end is the sliver block's wall; the east
  // end needs its own wall where the lobby stands proud of the east wing.
  stone.quad([LX1, -1.6, 0], [LX1, FRONT, 0], [LX1, FRONT, 16], [LX1, -1.6, 16])
  // A clerestory band of glass under the crest on the back slope.
  {
    const y0 = LY + 1, y1 = LY + 9
    win.quad([LX1 - 2, y0, zAt(y0) + 0.05], [LX0 + 2, y0, zAt(y0) + 0.05], [LX0 + 2, y1, zAt(y1) + 0.05], [LX1 - 2, y1, zAt(y1) + 0.05])
  }

  // --- Roadway canopy: a glass roof arched across the roadway, on silver
  // cross-beams and two rows of columns. ------------------------------------
  {
    const r = clean(CANOPY)
    const Y0 = 13.3, Y1 = 63.4, Z = 15, RISE = 2.2, SEG = 8
    const zOf = (y: number) => Z + RISE * Math.sin(Math.PI * Math.min(1, Math.max(0, (y - Y0) / (Y1 - Y0))))
    // x extent of the canopy at height y, from its slanted ends
    const xs = (y: number): [number, number] => {
      const t = (y - Y0) / (Y1 - Y0)
      return [-271.5 + (-276.2 + 271.5) * t, -9.5 + (-4.7 + 9.5) * t]
    }
    for (let i = 0; i < SEG; i++) {
      const y0 = Y0 + ((Y1 - Y0) * i) / SEG, y1 = Y0 + ((Y1 - Y0) * (i + 1)) / SEG
      const [a0, b0] = xs(y0), [a1, b1] = xs(y1)
      glass.quad([a0, y0, zOf(y0)], [b0, y0, zOf(y0)], [b1, y1, zOf(y1)], [a1, y1, zOf(y1)])
      glass.quad([a1, y1, zOf(y1) - 0.05], [b1, y1, zOf(y1) - 0.05], [b0, y0, zOf(y0) - 0.05], [a0, y0, zOf(y0) - 0.05])
      // the end edges and the cross-beams, as silver bands over the glass
      for (const [x0, x1, xx0, xx1] of [[a0, a0 + 1.2, a1, a1 + 1.2], [b0 - 1.2, b0, b1 - 1.2, b1]])
        silver.quad([x0, y0, zOf(y0) + 0.12], [x1, y0, zOf(y0) + 0.12], [xx1, y1, zOf(y1) + 0.12], [xx0, y1, zOf(y1) + 0.12])
      for (let x = -258; x <= -20; x += 16.85)
        silver.quad([x - 0.45, y0, zOf(y0) + 0.1], [x + 0.45, y0, zOf(y0) + 0.1], [x + 0.45, y1, zOf(y1) + 0.1], [x - 0.45, y1, zOf(y1) + 0.1])
    }
    // the outer edge beam along the far side of the roadway
    {
      const [a, b] = xs(Y1)
      silver.quad([a, Y1, Z - 1], [b, Y1, Z - 1], [b, Y1, Z + 0.15], [a, Y1, Z + 0.15])
      silver.quad([b, Y1 - 1.2, Z + 0.15], [a, Y1 - 1.2, Z + 0.15], [a, Y1, Z + 0.15], [b, Y1, Z + 0.15])
    }
    for (const y of [29, 47]) for (let x = -258; x <= -22; x += 33.7) block(silver, null, rect(x - 0.6, y - 0.6, x + 0.6, y + 0.6), 0, zOf(y), 0)
  }

  // --- The Atrium: glass walls under a glazed vault. ----------------------
  {
    const c = block(stone, roof, ATRIUM, 0, 12, 0.5)
    panels(win, c, 2.5, 10.5, { bay: 8, gap: 1.2 })
    const X0 = -246, X1 = -32, Y0 = -82, Y1 = -57, RISE = 7, SEG = 10
    const arc: XY[] = []
    for (let i = 0; i <= SEG; i++) {
      const t = i / SEG, y = Y0 + (Y1 - Y0) * t
      arc.push([y, 12 + RISE * Math.sin(Math.PI * t)])
    }
    for (let i = 0; i < SEG; i++) {
      const [y0, z0] = arc[i], [y1, z1] = arc[i + 1]
      glass.quad([X0, y0, z0], [X1, y0, z0], [X1, y1, z1], [X0, y1, z1])
    }
    for (let i = 1; i < SEG; i++) {
      // end gables, as fans
      const [y0, z0] = arc[i], [y1, z1] = arc[i + 1]
      glass.tri([X1, Y0, 12], [X1, y0, z0], [X1, y1, z1])
      glass.tri([X0, Y0, 12], [X0, y1, z1], [X0, y0, z0])
    }
    // white truss ribs across the vault, as raised bands
    for (let x = X0 + 12; x < X1 - 6; x += 21.4) {
      for (let i = 0; i < SEG; i++) {
        const [y0, z0] = arc[i], [y1, z1] = arc[i + 1]
        const n0 = 0.25
        trim.quad([x - 0.5, y0, z0 + n0], [x + 0.5, y0, z0 + n0], [x + 0.5, y1, z1 + n0], [x - 0.5, y1, z1 + n0])
      }
    }
  }

  // --- The Plaza: three storeys, two window bands. -------------------------
  {
    const c = block(stone, roof, PLAZA, 0, 18, 0.5)
    panels(win, c, 2.5, 8, { bay: 8 })
    panels(win, c, 10, 15.5, { bay: 8 })
  }

  await save('clt-terminal', 'Charlotte Douglas International Airport Terminal', CENTRE, [
    { part: stone, material: CLT.stone },
    { part: win, material: CLT.window },
    { part: trim, material: CLT.trim },
    { part: roof, material: CLT.roof },
    { part: glass, material: CLT.glass },
    { part: silver, material: CLT.silver },
  ])
}

if (import.meta.main) await main()
