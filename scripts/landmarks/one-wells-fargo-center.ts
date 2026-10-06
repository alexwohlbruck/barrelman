/**
 * One Wells Fargo Center (1988, formerly First Union Center), Charlotte —
 * original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/one-wells-fargo-center.ts
 *
 * Map frame: x east, y north, z up, metres; origin at the centre of the tower
 * on the ground. Placed at bearing 5°, so the model's +x runs along the crown's
 * barrel vault (true bearing 95°, the diagonal of Uptown's 50° street grid)
 * and its arched glass ends face true east and west.
 *
 * The tower is a square on the street grid (≈53 m a side, so a diamond in this
 * frame) with its east and west corners cut off by flat end walls, |x| = E.
 * Each end wall carries a full-height glass bay with a dark slot that rises
 * into the vault's semicircular glass end, the "jukebox". The north and south
 * corners step back in grid-aligned sawtooth notches, more at each setback, so
 * the top floors narrow to a slab under the vault. Granite faces get broad
 * recessed window bands; there is no punched-window grid.
 *
 * OSM: way/380092606 is the tower outline (192 m, no parts). Its east wall is
 * the end wall at x = +19.6; its west tip runs 13 m past where the symmetric
 * end wall stands, and that sliver is left uncovered. The north and south
 * tips stop ≈3 m short of OSM's, where the base sawtooth notches them.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Rim = { points: XY[]; normals: V3[] }

const granite = new Part(), plinth = new Part(), windows = new Part()
const bay = new Part(), slot = new Part(), vault = new Part()
const terraces = new Part(), metal = new Part()

const R = 37.5        // half-diagonal of the grid square: the north/south tips
const E = 19.6        // end walls, from OSM's east wall and the vault's ends
const RV = 11         // barrel vault radius (≈22 m wide, from imagery)
const TOP = 179       // top of the vault
const SPRING = TOP - RV
const ATTIC = SPRING - 5
const BAY = RV - 1.2  // half-width of the end bays, inside the arch ring
const SLOT = 2.2      // half-width of the dark slot
const BEVEL = .5
const RECESS = .6
const ARC = 12        // segments per semicircle

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }
const up: V3 = [0, 0, 1]
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

/**
 * The tower's plan at a stage: the chamfered diamond, cut on the north and
 * south by a grid-aligned sawtooth whose valleys sit at |y| = cut. `teeth` is
 * how many valleys; 0 cuts straight across between the end walls.
 */
function plan(cut: number, teeth: number): XY[] {
  if (teeth === 0) return [[E, -cut], [E, cut], [-E, cut], [-E, -cut]]
  const t = (R - cut) / (teeth + 1)
  const ring: XY[] = [[E, -(R - E)], [E, R - E]]
  const peaks: XY[] = []
  for (let k = 0; k <= 2 * teeth; k++) {
    const x = R - cut - t - k * t
    peaks.push([x, k % 2 ? cut : cut + t])
  }
  ring.push(...peaks, [-E, R - E], [-E, -(R - E)])
  ring.push(...peaks.map(([x, y]): XY => [-x, -y]))
  return ring
}

/** Bevel each convex plan corner, with smooth analytic normals. */
function soften(ring: XY[], radius = BEVEL): Rim {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const l0 = Math.hypot(b[0] - a[0], b[1] - a[1]), l1 = Math.hypot(c[0] - b[0], c[1] - b[1])
    const u: XY = [(b[0] - a[0]) / l0, (b[1] - a[1]) / l0], v: XY = [(c[0] - b[0]) / l1, (c[1] - b[1]) / l1]
    const turn = u[0] * v[1] - u[1] * v[0]
    if (turn < 1e-5) {
      points.push(b); normals.push(unit([u[1] + v[1], -u[0] - v[0], 0])); continue
    }
    const r = Math.min(radius, l0 * .2, l1 * .2)
    points.push([b[0] - u[0] * r, b[1] - u[1] * r], [b[0] + v[0] * r, b[1] + v[1] * r])
    normals.push([u[1], -u[0], 0], [v[1], -v[0], 0])
  }
  return { points, normals }
}

function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

function cap(p: Part, ring: XY[], z: number, upward = true) {
  const c: V3 = [ring.reduce((s, v) => s + v[0], 0) / ring.length, ring.reduce((s, v) => s + v[1], 0) / ring.length, z]
  const pts = at(ring, z)
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length
    if (upward) p.tri(c, pts[i], pts[j]); else p.tri(c, pts[j], pts[i])
  }
}

function wall(p: Part, rim: Rim, z0: number, z1: number) {
  const a = at(rim.points, z0), b = at(rim.points, z1)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length
    quad(p, a[i], a[j], b[j], b[i], rim.normals[i], rim.normals[j])
  }
}

/**
 * Granite faces with broad recessed window bands between bevelled piers.
 * End walls keep their middle for the glass bay and take one band per flank.
 */
function facade(rim: Rim, bottom: number, top: number) {
  for (let i = 0; i < rim.points.length; i++) {
    const a = rim.points[i], b = rim.points[(i + 1) % rim.points.length]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1]), ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
    const n: V3 = [uy, -ux, 0]
    const v = (s: number, z: number, depth = 0): V3 => [a[0] + ux * s + uy * depth, a[1] + uy * s - ux * depth, z]
    const panel = (s0: number, s1: number, z0: number, z1: number) =>
      granite.quad(v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1))
    const end = Math.abs(n[0]) > .99 && Math.abs(Math.abs(a[0]) - E) < 1e-6
    let bands: [number, number][] = []
    if (end) {
      // Mid-line of the end wall is y = 0; the bay overlay covers |y| < BAY.
      const s0 = Math.abs(a[1]), flank = s0 - BAY
      if (flank > 4.5) {
        const w = flank * .5, c1 = (flank) / 2, c2 = length - flank / 2
        bands = [[c1 - w / 2, c1 + w / 2], [c2 - w / 2, c2 + w / 2]]
      }
    } else if (length >= 4.5) {
      const count = Math.max(1, Math.round(length / 7))
      const spacing = length / count, w = spacing * .56
      bands = Array.from({ length: count }, (_, j): [number, number] => [spacing * (j + .5) - w / 2, spacing * (j + .5) + w / 2])
    }
    if (!bands.length) {
      quad(granite, v(0, bottom), v(length, bottom), v(length, top), v(0, top), rim.normals[i], rim.normals[(i + 1) % rim.points.length])
      continue
    }
    const lo = bottom + .8, hi = top - .8
    panel(0, length, bottom, lo)
    panel(0, length, hi, top)
    let last = 0
    for (const [s0, s1] of bands) {
      panel(last, s0, lo, hi)
      const outer: XY[] = [[s0, lo], [s1, lo], [s1, hi], [s0, hi]]
      const inner = inset(outer, .35)
      const back = inner.map(([s, z]) => v(s, z, -RECESS))
      windows.quad(back[0], back[1], back[2], back[3])
      for (let k = 0; k < 4; k++) {
        const l = (k + 1) % 4
        const ds = outer[l][0] - outer[k][0], dz = outer[l][1] - outer[k][1], e = Math.hypot(ds, dz)
        const innerN = unit([n[0] * .4 - ux * dz / e, n[1] * .4 - uy * dz / e, ds / e])
        quad(granite, v(...outer[k]), v(...outer[l]), back[l], back[k], n, n, innerN, innerN)
      }
      last = s1
    }
    panel(last, length, lo, hi)
  }
}

/** A bevelled granite coping around a setback, with a terracotta terrace. */
function terrace(rim: Rim, bottom: number, top: number) {
  const middle = inset(rim.points, .45), inner = inset(rim.points, 1)
  const a = at(rim.points, bottom), b = at(middle, top), c = at(inner, top - .5)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length, ni = rim.normals[i], nj = rim.normals[j]
    quad(granite, a[i], a[j], b[j], b[i], ni, nj, up, up)
    quad(granite, b[i], b[j], c[j], c[i], up, up, [-nj[0], -nj[1], 0], [-ni[0], -ni[1], 0])
  }
  cap(terraces, inner, top - .5)
}

// Granite stages. The end walls run straight up; the north and south corners
// notch back at each setback, as on the real tower.
const stages: { z0: number; z1: number; cut: number; teeth: number }[] = [
  { z0: 0, z1: 118, cut: 31.5, teeth: 1 },
  { z0: 118, z1: 155, cut: 19, teeth: 3 },
  { z0: 155, z1: 159, cut: 17.4, teeth: 0 },
  { z0: 159, z1: ATTIC, cut: 14.2, teeth: 0 },
]
for (const s of stages) {
  const rim = soften(plan(s.cut, s.teeth))
  if (s.z0 === 0) {
    wall(plinth, rim, 0, 4)
    facade(rim, 4, s.z1 - .6)
  } else facade(rim, s.z0, s.z1 - .6)
  terrace(rim, s.z1 - .6, s.z1)
}

// The glass attic under the vault, inset from the top stage's long faces.
const attic: XY[] = [[E, -RV], [E, RV], [-E, RV], [-E, -RV]]
for (const i of [1, 3]) {
  const [a, b] = [attic[i], attic[(i + 1) % 4]]
  bay.quad([a[0], a[1], ATTIC - .5], [b[0], b[1], ATTIC - .5], [b[0], b[1], SPRING], [a[0], a[1], SPRING])
}

// The barrel vault: a true semicircle, smooth-shaded, dark blue glass.
const arc = Array.from({ length: ARC + 1 }, (_, k) => {
  const t = Math.PI * k / ARC
  return { y: RV * Math.cos(t), z: SPRING + RV * Math.sin(t), n: [0, Math.cos(t), Math.sin(t)] as V3 }
})
for (let k = 0; k < ARC; k++) {
  const p = arc[k], q = arc[k + 1]
  quad(vault, [E, p.y, p.z], [-E, p.y, p.z], [-E, q.y, q.z], [E, q.y, q.z], p.n, p.n, q.n, q.n)
}

// Each end: the glass bay up the wall, the arch's glass fan with a silver
// ring, and the dark slot rising into the arch with its own round top. All
// flush shapes laid just proud of the end wall.
for (const side of [1, -1]) {
  const x = (d: number) => side * (E + d)
  // Orient each face outward: on the west end, swap the y order.
  const face = (p: Part, d: number, y0: number, y1: number, z0: number, z1: number) => {
    const [ya, yb] = side > 0 ? [y0, y1] : [y1, y0]
    p.quad([x(d), ya, z0], [x(d), yb, z0], [x(d), yb, z1], [x(d), ya, z1])
  }
  const fan = (p: Part, d: number, r0: number, r1: number) => {
    for (let k = 0; k < ARC; k++) {
      const t0 = Math.PI * k / ARC, t1 = Math.PI * (k + 1) / ARC
      const P = (r: number, t: number): V3 => [x(d), r * Math.cos(t), SPRING + r * Math.sin(t)]
      const [a, b] = side > 0 ? [t0, t1] : [t1, t0]
      if (r0 === 0) p.tri([x(d), 0, SPRING], P(r1, a), P(r1, b))
      else p.quad(P(r0, a), P(r1, a), P(r1, b), P(r0, b))
    }
  }
  // Wall below the springline, outside the bay, up to the attic's edge.
  face(bay, 0, -RV, RV, ATTIC - .5, SPRING)
  face(bay, .08, -BAY, BAY, 4, ATTIC - .5)
  fan(bay, 0, 0, RV)
  fan(metal, .08, BAY, RV)
  face(metal, .08, BAY, RV, ATTIC - .5, SPRING)
  face(metal, .08, -RV, -BAY, ATTIC - .5, SPRING)
  // The slot: straight up from the plaza, a semicircle on top.
  const slotTop = SPRING + BAY - 2.6 - SLOT
  face(slot, .16, -SLOT, SLOT, 4, slotTop)
  for (let k = 0; k < ARC; k++) {
    const t0 = Math.PI * k / ARC, t1 = Math.PI * (k + 1) / ARC
    const P = (t: number): V3 => [x(.16), SLOT * Math.cos(t), slotTop + SLOT * Math.sin(t)]
    const [a, b] = side > 0 ? [t0, t1] : [t1, t0]
    slot.tri([x(.16), 0, slotTop], P(a), P(b))
  }
}

const parts = [
  { part: granite, material: { name: 'granite', color: 0xa87d72 } },
  { part: plinth, material: { name: 'granite-base', color: 0x8c685f } },
  { part: windows, material: { name: 'window', color: 0x617a92 } },
  { part: bay, material: { name: 'window-2', color: 0x8fa7bc, roughness: .6 } },
  { part: slot, material: { name: 'window-3', color: 0x2c3a4b } },
  { part: vault, material: { name: 'glass', color: 0x3a5274, roughness: .5 } },
  { part: metal, material: { name: 'arch-ring', color: 0xd3d8db, roughness: .6 } },
  { part: terraces, material: { name: 'terracotta-terraces', color: 0xc8968a } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('One Wells Fargo Center', parts, {
  license: 'CC0-1.0', bearing: 5, elevation: 0, height: TOP,
  frame: 'Y up, -Z north, +X east, metres; origin at the tower centre on the ground',
  replaces: ['way/380092606'],
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/one-wells-fargo-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
