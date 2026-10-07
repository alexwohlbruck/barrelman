/**
 * Charlotte Douglas International Airport (CLT): the terminal headhouse —
 * procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/clt-terminal.ts
 *
 * Also the small kit the other CLT models import (`clt-concourse-a`,
 * `clt-concourses-b-c`, `clt-concourses-d-e`, the parking decks and the
 * tower): footprint clean-up, ear-clipping caps, bevelled blocks, window
 * panels, roofs that follow a section, banded parking floors and helix
 * drums. Importing this file builds nothing; the terminal is only written
 * when it is run directly.
 *
 * Frame. Every CLT model is drawn in one site frame: x along the terminal's
 * long axis (bearing 85.8°), y towards 355.8°, z up, metres, so all of them
 * are placed at bearing 355.8. Footprints are OSM outlines in that frame, and
 * each model is shifted so its anchor (the centre of its group's bounding
 * box) is the origin.
 *
 * The headhouse here is four OSM ways:
 * - Main Terminal (way/1414163868): the 1982 terminal, flat roofs at 13 m
 *   with a 10 m strip between ticketing and baggage claim, a 17 m east wing
 *   with a set-back upper storey; and along its curb front the new
 *   Terminal Lobby Expansion. The lobby's silver roof rises out of the old
 *   roofline towards the curb, from 14.6 m to 21 m, and stops short of the
 *   front, where the tall glass wall carries on up to the canopy. Two stair
 *   and lift cores stand up through its low back half.
 * - the roadway canopy (way/1414165678, building=roof): one long silver
 *   shell over the two-level roadway, 23.4 m just off the glass and falling
 *   in a long curve to 17.6 m over the far curb, on a single row of columns
 *   24 m apart (Commons, "Charlotte Airport December 2022 Pickup Zone"
 *   shows the lower roadway under the deck; Wikipedia gives the canopy
 *   146,000 sq ft).
 * - the Atrium (way/1347416658): the airside hall with rocking chairs,
 *   under a glazed barrel vault on white ribs (Commons "CLT Airport
 *   Atrium"), between lower wings, with a square block standing at its
 *   middle.
 * - The Plaza (way/1347417381): the 2019 east expansion, 17 m with a set-back
 *   upper storey and lower wings.
 *
 * OSM gives no heights. These, the lobby's and canopy's sections, the column
 * row and which blocks stand up where were checked against renders of
 * Mapbox's 3D buildings (tileset mapbox.mapbox-3dbuildings-v1), used only
 * as a visual reference: every outline here is OSM's, and the geometry is
 * our own modelling.
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

/**
 * A flat cap over a counter-clockwise ring with round holes in it. Each hole
 * is joined to the outline by a slit to its nearest corner, which makes one
 * simple ring that ear clipping can take.
 */
export function capHoles(p: Part, r: XY[], holes: XY[][], z: number) {
  let ring = r.slice()
  for (const h0 of holes) {
    const h = area(h0) > 0 ? h0.slice().reverse() : h0.slice() // holes run clockwise
    let best = [0, 0], d = Infinity
    for (let i = 0; i < ring.length; i++) for (let k = 0; k < h.length; k++) {
      const e = Math.hypot(ring[i][0] - h[k][0], ring[i][1] - h[k][1])
      if (e < d) { d = e; best = [i, k] }
    }
    const [i, k] = best, hk = [...h.slice(k), ...h.slice(0, k), h[k]]
    ring = [...ring.slice(0, i + 1), ...hk, ...ring.slice(i)]
  }
  cap(p, ring, z)
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
export function panels(p: Part, ring: XY[], z0: number, z1: number, o: { bay?: number; gap?: number; end?: number; minLen?: number; out?: number; keep?: (a: XY, b: XY) => boolean } = {}) {
  const bay = o.bay ?? 9, gap = o.gap ?? 1.6, end = o.end ?? 1.2, minLen = o.minLen ?? 5, out = o.out ?? 0.04
  const r = ring
  for (let i = 0; i < r.length; i++) {
    const a = r[i], b = r[(i + 1) % r.length]
    const L = Math.hypot(b[0] - a[0], b[1] - a[1])
    if (L < minLen || (o.keep && !o.keep(a, b))) continue
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

/** The centre and mean radius of a roughly round ring. */
export function roundOf(ring: XY[]): { c: XY; r: number } {
  const r = clean(ring, 0.3)
  const cx = r.reduce((s, p) => s + p[0], 0) / r.length, cy = r.reduce((s, p) => s + p[1], 0) / r.length
  return { c: [cx, cy], r: r.reduce((s, p) => s + Math.hypot(p[0] - cx, p[1] - cy), 0) / r.length }
}

/**
 * An open parking deck: a dark core set back from the edge, wrapped in
 * concrete spandrel bands at every floor, so it reads as horizontal bands of
 * concrete with dark open floors between. `floors` are the slab heights from
 * the ground up; the last is the roof deck, whose parapet band tops the deck.
 *
 * `wells` are the open centres of the helix ramps, as OSM maps them. The
 * outline round each helix is drawn as the ramp's solid concrete drum, with
 * a dark slot at each turn, its top turn standing `drumRise` above the
 * roof deck; the well itself is a dark disc in a parapet ring. With `bands:
 * false` the straight walls are left to the caller (the Hourly Deck's
 * shell). Returns the cleaned outline and the test for drum edges.
 */
export function parkingDeck(
  parts: { concrete: Part; gap: Part; deck: Part },
  ring: XY[],
  floors: number[],
  o: { band?: number; setback?: number; wells?: XY[][]; bands?: boolean; drumReach?: number; drumRise?: number } = {},
) {
  const band_ = o.band ?? 1.1, setback = o.setback ?? 0.9
  const r = clean(ring, 1.0)
  const core = inset(r, setback), ins = core
  const top = floors.at(-1)!
  const wells = (o.wells ?? []).map(roundOf), reach = o.drumReach ?? 8
  walls(parts.gap, core, 0, top)
  capHoles(parts.deck, core, wells.map((w) => circle(w.c[0], w.c[1], w.r, 14)), top)
  const isDrum = (a: XY, b: XY) => {
    const m: XY = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2]
    return wells.some((w) => Math.hypot(m[0] - w.c[0], m[1] - w.c[1]) < w.r + reach)
  }
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length, a = r[i], b = r[j]
    if (isDrum(a, b)) {
      const zt = top + (o.drumRise ?? 3)
      parts.concrete.quad([a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], zt], [a[0], a[1], zt])
      parts.concrete.quad([a[0], a[1], zt], [b[0], b[1], zt], [ins[j][0], ins[j][1], zt], [ins[i][0], ins[i][1], zt])
      const L = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1, n: XY = [((b[1] - a[1]) / L) * 0.05, (-(b[0] - a[0]) / L) * 0.05]
      for (const z of floors.slice(0, -1)) {
        const z0 = z + 1.2, z1 = z + 2.6
        parts.gap.quad([a[0] + n[0], a[1] + n[1], z0], [b[0] + n[0], b[1] + n[1], z0], [b[0] + n[0], b[1] + n[1], z1], [a[0] + n[0], a[1] + n[1], z1])
      }
    } else if (o.bands !== false) {
      for (const z of floors) ledgeEdge(parts.concrete, a, b, ins[i], ins[j], Math.max(0, z - 0.35), z + band_)
    }
  }
  for (const w of o.wells ?? []) helixWell(parts, w, top, o.drumRise ?? 3)
  return { r, isDrum }
}

/** One edge of a spandrel band: its outer face and its top ledge back to the core. */
export function ledgeEdge(p: Part, a: XY, b: XY, ai: XY, bi: XY, z0: number, z1: number) {
  p.quad([a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
  p.quad([a[0], a[1], z1], [b[0], b[1], z1], [bi[0], bi[1], z1], [ai[0], ai[1], z1])
}

/** A band's outer face and its top ledge back to the core (the underside is never seen). */
export function ledgeBand(p: Part, r: XY[], z0: number, z1: number, depth: number) {
  const ins = inset(r, depth)
  for (let i = 0; i < r.length; i++) ledgeEdge(p, r[i], r[(i + 1) % r.length], ins[i], ins[(i + 1) % r.length], z0, z1)
}

/**
 * The open well in the middle of a helix ramp: a round hole in the roof deck
 * (cut by `parkingDeck`) whose wall drops a turn to a dark floor, with a
 * parapet ring round it.
 */
export function helixWell(parts: { concrete: Part; gap: Part; deck: Part }, ring: XY[], top: number, rise = 1.1, seg = 14) {
  const { c: [cx, cy], r: rad } = roundOf(ring)
  const c = circle(cx, cy, rad, seg), inner = circle(cx, cy, rad - 0.5, seg)
  // the well, a turn down, its wall facing in, and the parapet round it
  const floor = top - 3.4
  cap(parts.gap, c, floor)
  for (let i = 0; i < seg; i++) {
    const j = (i + 1) % seg
    parts.concrete.quad([c[j][0], c[j][1], floor], [c[i][0], c[i][1], floor], [c[i][0], c[i][1], top + rise], [c[j][0], c[j][1], top + rise])
    parts.concrete.quad([c[i][0], c[i][1], top + rise], [inner[i][0], inner[i][1], top + rise], [inner[j][0], inner[j][1], top + rise], [c[j][0], c[j][1], top + rise])
  }
}

/** Piecewise-linear z(s) through (s, z) samples sorted by s; clamped at both ends. */
export function profile(pts: XY[]): (s: number) => number {
  return (s) => {
    if (s <= pts[0][0]) return pts[0][1]
    for (let i = 0; i < pts.length - 1; i++)
      if (s <= pts[i + 1][0]) return pts[i][1] + (pts[i + 1][1] - pts[i][1]) * (s - pts[i][0]) / (pts[i + 1][0] - pts[i][0])
    return pts.at(-1)![1]
  }
}

type Height = number | ((x: number, y: number) => number)
const at = (h: Height, q: XY) => (typeof h === 'number' ? h : h(q[0], q[1]))

/**
 * One wall face along a→b (facing right of travel, as on a counter-clockwise
 * ring) between two heights that may vary along it, split every `step`
 * metres so it follows a curved roof.
 */
export function edgeTo(p: Part, a: XY, b: XY, bot: Height, top: Height, step = 3) {
  const n = Math.max(1, Math.ceil(Math.hypot(b[0] - a[0], b[1] - a[1]) / step))
  const P = (t: number): XY => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
  for (let k = 0; k < n; k++) {
    const u = P(k / n), v = P((k + 1) / n)
    p.quad([u[0], u[1], at(bot, u)], [v[0], v[1], at(bot, v)], [v[0], v[1], at(top, v)], [u[0], u[1], at(top, u)])
  }
}

/** The walls of a counter-clockwise ring between two heights that may vary. */
export function bandTo(p: Part, r: XY[], bot: Height, top: Height, step = 3) {
  for (let i = 0; i < r.length; i++) edgeTo(p, r[i], r[(i + 1) % r.length], bot, top, step)
}

/**
 * A curved roof over a ring: the ring is cut into bands at `cuts` (values of
 * y) and each band is capped with its corners raised to `top(x, y)`. Faces
 * up, or down for a soffit.
 */
export function capTo(p: Part, r: XY[], cuts: number[], top: (x: number, y: number) => number, up = true) {
  for (let i = 0; i < cuts.length - 1; i++) {
    const band = clean(clip(clip(r, 0, -1, -cuts[i]), 0, 1, cuts[i + 1]), 0.05)
    if (band.length < 3 || Math.abs(area(band)) < 0.01) continue
    for (const [a, b, c] of triangulate(band)) {
      const V = (q: XY): [number, number, number] => [q[0], q[1], top(q[0], q[1])]
      if (up) p.tri(V(band[a]), V(band[b]), V(band[c]))
      else p.tri(V(band[a]), V(band[c]), V(band[b]))
    }
  }
}

/** Values from a to b in equal steps of about `step`, both ends included. */
export const span = (a: number, b: number, step: number) => {
  const n = Math.max(1, Math.round((b - a) / step))
  return Array.from({ length: n + 1 }, (_, i) => a + ((b - a) * i) / n)
}

/** A round column from z0 to z1, smooth-shaded. */
export function column(p: Part, x: number, y: number, r: number, z0: number, z1: number, seg = 8) {
  lathe(p, x, y, [[r, z0], [r, z1]], seg)
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
  const main = clean(MAIN)
  const onOutline = (ring: XY[]) => (a: XY, b: XY) => {
    // an edge is on the building's outside if its midpoint lies on the given outline
    const m: XY = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2]
    for (let i = 0; i < ring.length; i++) {
      const p = ring[i], q = ring[(i + 1) % ring.length], dx = q[0] - p[0], dy = q[1] - p[1], l = dx * dx + dy * dy || 1
      const t = Math.max(0, Math.min(1, ((m[0] - p[0]) * dx + (m[1] - p[1]) * dy) / l))
      if (Math.hypot(m[0] - p[0] - t * dx, m[1] - p[1] - t * dy) < 0.6) return true
    }
    return false
  }
  /** A flat block cut from `ring` by the half-planes in `cuts`, with windows on the real outline only. */
  const part = (ring: XY[], cuts: [number, number, number][], h: number, win0 = 3, win1 = h - 2.6) => {
    let r = ring
    for (const [a, b, c] of cuts) r = clip(r, a, b, c)
    if (r.length < 3 || Math.abs(area(r)) < 1) return
    const c = block(stone, roof, r, 0, h, 0.45)
    panels(win, c, win0, win1, { bay: 9, keep: onOutline(ring) })
  }

  // --- The 1982 terminal behind the lobby: flat roofs in three bands, with
  // a lower 10 m strip between the ticketing hall and baggage claim.
  const LB = -7, LX1 = -9.5
  const W: [number, number, number][] = [[0, 1, LB], [1, 0, LX1]] // y <= LB, x <= LX1
  part(main, [...W, [0, 1, -37]], 13.5)
  part(main, [...W, [0, -1, 37], [0, 1, -25]], 10, 3, 7.6)
  part(main, [...W, [0, -1, 25]], 13.1)

  // --- East wing and The Plaza: 17 m blocks with a 20.5 m upper storey set
  // back from every edge, and lower wings to the south and east.
  const E: [number, number, number][] = [[-1, 0, -LX1]] // x >= LX1
  part(main, [...E, [0, 1, -40]], 13)
  part(main, [...E, [0, -1, 40]], 17)
  const plaza = clean(PLAZA)
  part(plaza, [[0, 1, -37.8]], 13)
  part(plaza, [[0, -1, 37.8], [1, 0, 138]], 17)
  part(plaza, [[0, -1, 37.8], [-1, 0, -138]], 11, 3, 8.4)
  for (const r of [rect(-8, -31, 59, -19), rect(78, -31, 118, -15)]) {
    const c = block(stone, roof, r, 16, 20.5, 0.45)
    panels(win, c, 17.4, 19.6, { bay: 8, gap: 1.2 })
  }

  // --- The curbside lobby (2010s Terminal Lobby Expansion). Its roof rises
  // out of the old roofline towards the curb, from 14.6 m to 21 m, and stops
  // short of the front, where a tall glass wall carries on up to the canopy.
  // Heights checked against the Mapbox 3D buildings; the footprint is OSM's.
  const lobby = clean(clip(clip(main, 0, -1, -LB), 1, 0, LX1), 0.05)
  const zl = profile([[-7, 14.6], [-2, 15.0], [0, 15.6], [2, 16.6], [4, 17.6], [6, 18.6], [8, 19.5], [10, 20.3], [12, 20.7], [13.5, 20.9]])
  // The canopy: one long shell over the two-level roadway, highest just
  // off the glass and falling in a long curve to the row of columns at the
  // far curb.
  const T = 1.0
  const zc = profile([[13, 23.4], [22, 23.4], [26, 23.2], [30, 22.7], [34, 22.0], [38, 21.2], [42, 20.3], [46, 19.4], [50, 18.6], [54, 17.9], [58, 17.6], [64, 17.6]])
  const FRONT = 12.5 // lobby edges north of this are the glass front
  for (let i = 0; i < lobby.length; i++) {
    const a = lobby[i], b = lobby[(i + 1) % lobby.length]
    if (a[1] > FRONT && b[1] > FRONT) {
      // the glass front, up to the canopy's underside
      edgeTo(win, a, b, 0, (_x, y) => zc(y) - T, 12)
      continue
    }
    if (a[1] <= LB + 0.3 && b[1] <= LB + 0.3) {
      // back edge: a strip of clerestory glass between the old roof and the new
      win.quad([a[0], a[1] - 0.05, 13.1], [b[0], b[1] - 0.05, 13.1], [b[0], b[1] - 0.05, zl(b[1])], [a[0], a[1] - 0.05, zl(a[1])])
      continue
    }
    edgeTo(stone, a, b, 0, (_x, y) => zl(y), 2)
  }
  // a silver lip along the lobby roof's side edges
  for (let i = 0; i < lobby.length; i++) {
    const a = lobby[i], b = lobby[(i + 1) % lobby.length]
    if ((a[1] > FRONT && b[1] > FRONT) || (a[1] <= LB + 0.3 && b[1] <= LB + 0.3)) continue
    const n: XY = [(b[1] - a[1]), -(b[0] - a[0])], l = Math.hypot(n[0], n[1]) || 1
    const o: XY = [(n[0] / l) * 0.12, (n[1] / l) * 0.12]
    edgeTo(silver, [a[0] + o[0], a[1] + o[1]], [b[0] + o[0], b[1] + o[1]], (_x, y) => zl(y) - 0.7, (_x, y) => zl(y) + 0.2, 2)
  }
  capTo(silver, lobby, span(LB, 13.6, 1.5), (_x, y) => zl(y))

  // Two stair and lift cores stand up through the lobby roof's low back half.
  for (const r of [rect(-250, -7, -210, 8), rect(-80, -7, -40, 8)]) {
    const c = block(stone, roof, r, 12, 20, 0.45)
    panels(win, c, 15.6, 19, { bay: 8, gap: 1.4, keep: (a, b) => a[1] < -6.5 && b[1] < -6.5 })
  }

  // --- The roadway canopy (way/1414165678), on OSM's outline: a silver
  // shell 1 m thick with a pale soffit and edge, on one row of columns 24 m
  // apart along the outer curb.
  {
    const r = clean(CANOPY)
    const cuts = span(13, 63.6, 2.3)
    capTo(silver, r, cuts, (_x, y) => zc(y))
    capTo(trim, r, cuts, (_x, y) => zc(y) - T, false)
    bandTo(trim, r, (_x, y) => zc(y) - T, (_x, y) => zc(y), 2.3)
    for (let x = -268; x <= -18; x += 25) column(silver, x, 59, 0.75, 0, zc(59) - T)
  }

  // --- The Atrium: a long hall under a glazed barrel vault on white ribs,
  // between lower wings; and the square lantern block at its middle.
  {
    const atr = clean(ATRIUM)
    part(atr, [[1, 0, -227]], 14.5)
    part(atr, [[-1, 0, 227], [1, 0, -88]], 12.5, 2.6, 10.4)
    part(atr, [[-1, 0, 88]], 11, 2.6, 8.6)
    const X0 = -224, X1 = -91, Y0 = -84, Y1 = -56, RISE = 4.3, SEG = 10, Z0 = 12.5
    const arc: XY[] = span(0, 1, 1 / SEG).map((t) => [Y0 + (Y1 - Y0) * t, Z0 + RISE * Math.sin(Math.PI * t)])
    for (let i = 0; i < SEG; i++) {
      const [y0, z0] = arc[i], [y1, z1] = arc[i + 1]
      glass.quad([X0, y0, z0], [X1, y0, z0], [X1, y1, z1], [X0, y1, z1])
      if (i > 0) {
        glass.tri([X1, Y0, Z0], [X1, y0, z0], [X1, y1, z1])
        glass.tri([X0, Y0, Z0], [X0, y1, z1], [X0, y0, z0])
      }
    }
    for (let x = X0 + 9.5; x < X1 - 4; x += 19) {
      for (let i = 0; i < SEG; i++) {
        const [y0, z0] = arc[i], [y1, z1] = arc[i + 1]
        trim.quad([x - 0.5, y0, z0 + 0.2], [x + 0.5, y0, z0 + 0.2], [x + 0.5, y1, z1 + 0.2], [x - 0.5, y1, z1 + 0.2])
      }
    }
    const c = block(stone, roof, rect(-156, -70, -139, -54), 12, 25, 0.45)
    panels(win, c, 20, 23.6, { bay: 6, gap: 1, end: 1.2 })
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
