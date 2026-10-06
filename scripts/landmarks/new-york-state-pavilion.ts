/**
 * New York State Pavilion, Flushing Meadows–Corona Park — original procedural
 * geometry, CC0-1.0. Philip Johnson, 1964 World's Fair.
 *
 *   bun scripts/landmarks/new-york-state-pavilion.ts
 *
 * x = across the tent, y = along its long axis, z = metres up. Anchor is the
 * centroid of the Tent of Tomorrow's OSM outline (relation 13317338),
 * 40.7435173, -73.8443487; bearing 336°, so +y runs NNW down the ellipse's
 * major axis. Every position below was measured from OSM in that frame.
 *
 * Three parts, all within 95 m of the anchor:
 * - the Tent of Tomorrow: sixteen concrete columns on a 74.2 × 97.2 m ellipse
 *   carrying the yellow steel crown ring. The roof panels are long gone, so the
 *   crown stays open; inside it, the striped mezzanine wall;
 * - the three observation towers, saucer decks on clusters of shafts, with a
 *   capsule elevator each;
 * - the Theaterama drum (now the Queens Theatre) with its later glass drum
 *   and lobby wing, extruded from its OSM outline.
 */
import { Part, encodePng, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const TAU = Math.PI * 2
const up: V3 = [0, 0, 1], down: V3 = [0, 0, -1]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }

const concrete = new Part(), yellow = new Part(), white = new Part(), grey = new Part()
const stripes = new Part(), paving = new Part(), drum = new Part(), glass = new Part(), roof = new Part()

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

/**
 * A smooth surface of revolution about (cx, cy): `profile` is [radius, z]
 * from bottom to top, walked counter-clockwise so faces point outward. A
 * zero radius closes the surface at the axis.
 */
function lathe(p: Part, cx: number, cy: number, profile: XY[], segs: number, inward = false) {
  const normals = profile.map((_, k) => {
    const a = profile[Math.max(0, k - 1)], b = profile[Math.min(profile.length - 1, k + 1)]
    const dr = b[0] - a[0], dz = b[1] - a[1]
    return [dz, -dr] as XY // outward normal of the profile, in (r, z)
  })
  for (let i = 0; i < segs; i++) {
    const t0 = i / segs * TAU, t1 = (i + 1) / segs * TAU
    for (let k = 0; k < profile.length - 1; k++) {
      const pt = (t: number, j: number): V3 => [cx + profile[j][0] * Math.cos(t), cy + profile[j][0] * Math.sin(t), profile[j][1]]
      const nm = (t: number, j: number): V3 => {
        const [nr, nz] = normals[j], s = inward ? -1 : 1
        return unit([s * nr * Math.cos(t), s * nr * Math.sin(t), s * nz])
      }
      const a = pt(t0, k), b = pt(t1, k), c = pt(t1, k + 1), d = pt(t0, k + 1)
      // A ring that closes on the axis is a fan, not a strip of slivers.
      if (profile[k + 1][0] === 0) { p.tri(a, b, c, undefined, undefined, undefined, [nm(t0, k), nm(t1, k), nm(t1, k + 1)]); continue }
      if (inward) quad(p, b, a, d, c, nm(t1, k), nm(t0, k), nm(t0, k + 1), nm(t1, k + 1))
      else quad(p, a, b, c, d, nm(t0, k), nm(t1, k), nm(t1, k + 1), nm(t0, k + 1))
    }
  }
}

/** A flat disc or annulus at height z. */
function disc(p: Part, cx: number, cy: number, r0: number, r1: number, z: number, segs: number, facing = up) {
  for (let i = 0; i < segs; i++) {
    const t0 = i / segs * TAU, t1 = (i + 1) / segs * TAU
    const at = (r: number, t: number): V3 => [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
    const q = [at(r0, t0), at(r1, t0), at(r1, t1), at(r0, t1)]
    if (facing[2] < 0) q.reverse()
    if (r0 <= 0) p.tri(q[facing[2] < 0 ? 3 : 0], q[facing[2] < 0 ? 1 : 1], q[2])
    else quad(p, q[0], q[1], q[2], q[3], facing, facing)
  }
}

/** A round column with a soft rounded top edge. */
function column(p: Part, cx: number, cy: number, r: number, z0: number, z1: number, segs = 12) {
  const b = Math.min(.45, r * .3)
  lathe(p, cx, cy, [[r, z0], [r, z1 - b], [r - b, z1], [0, z1]], segs)
}

// --------------------------------------------------------------------------
// Tent of Tomorrow

/** The column ellipse, fitted to the sixteen column bumps in the OSM outline. */
const A = 37.1, B = 48.6
const ell = (t: number, d: number): XY => {
  const n = unit([B * Math.cos(t), A * Math.sin(t), 0])
  return [A * Math.cos(t) + d * n[0], B * Math.sin(t) + d * n[1]]
}
const ellN = (t: number): V3 => unit([B * Math.cos(t), A * Math.sin(t), 0])

// Measured from OSM, mirrored to the building's own symmetry.
const COLUMN_R = 2.1, COLUMN_TOP = 30.4
const columnQuarter: XY[] = [[36.55, 8.3], [32.4, 24.75], [23.7, 39.3], [8.2, 47.1]]
for (const [x, y] of columnQuarter)
  for (const [sx, sy] of [[1, 1], [-1, 1], [-1, -1], [1, -1]]) column(concrete, x * sx, y * sy, COLUMN_R, 0, COLUMN_TOP)

// The crown ring: a box girder whose outer plate passes through the columns.
const RING_TOP = 29.6, RING_BOTTOM = 26.4, OUT = -0.9, IN = -4.5, CH = .35
const N = 96
for (let i = 0; i < N; i++) {
  const t0 = i / N * TAU, t1 = (i + 1) / N * TAU
  const P = (t: number, d: number, z: number): V3 => [...ell(t, d), z]
  const n0 = ellN(t0), n1 = ellN(t1)
  const m0 = n0.map(v => -v) as V3, m1 = n1.map(v => -v) as V3
  const c0 = unit([n0[0], n0[1], 1]), c1 = unit([n1[0], n1[1], 1])
  const ic0 = unit([-n0[0], -n0[1], 1]), ic1 = unit([-n1[0], -n1[1], 1])
  quad(yellow, P(t0, OUT, RING_BOTTOM), P(t1, OUT, RING_BOTTOM), P(t1, OUT, RING_TOP - CH), P(t0, OUT, RING_TOP - CH), n0, n1)
  quad(yellow, P(t0, OUT, RING_TOP - CH), P(t1, OUT, RING_TOP - CH), P(t1, OUT - CH, RING_TOP), P(t0, OUT - CH, RING_TOP), c0, c1)
  quad(yellow, P(t0, OUT - CH, RING_TOP), P(t1, OUT - CH, RING_TOP), P(t1, IN + CH, RING_TOP), P(t0, IN + CH, RING_TOP), up, up)
  quad(yellow, P(t0, IN + CH, RING_TOP), P(t1, IN + CH, RING_TOP), P(t1, IN, RING_TOP - CH), P(t0, IN, RING_TOP - CH), ic0, ic1)
  quad(yellow, P(t1, IN, RING_BOTTOM), P(t0, IN, RING_BOTTOM), P(t0, IN, RING_TOP - CH), P(t1, IN, RING_TOP - CH), m1, m0)
  quad(yellow, P(t1, OUT, RING_BOTTOM), P(t0, OUT, RING_BOTTOM), P(t0, IN, RING_BOTTOM), P(t1, IN, RING_BOTTOM), down, down)
}

// The scalloped skirt: arches between the girder's hanging points, the
// silhouette every photo of the tent shows. One plate down the middle of the
// girder, wound both ways so it reads from inside and out.
const ARCHES = 48, SKIRT_D = (OUT + IN) / 2, POINT = 22.4
const scallop = [POINT, 24.9, 25.9, 24.9]
for (let k = 0; k < ARCHES; k++) {
  for (let s = 0; s < 4; s++) {
    const t0 = (k * 4 + s) / (ARCHES * 4) * TAU, t1 = (k * 4 + s + 1) / (ARCHES * 4) * TAU
    const z0 = scallop[s], z1 = scallop[(s + 1) % 4]
    const n0 = ellN(t0), n1 = ellN(t1)
    const a: V3 = [...ell(t0, SKIRT_D), z0], b: V3 = [...ell(t1, SKIRT_D), z1]
    const c: V3 = [...ell(t1, SKIRT_D), RING_BOTTOM], d: V3 = [...ell(t0, SKIRT_D), RING_BOTTOM]
    quad(yellow, a, b, c, d, n0, n1)
    quad(yellow, b, a, d, c, n1.map(v => -v) as V3, n0.map(v => -v) as V3)
  }
}

// Teeth above the crown: the girder's stiffener plates rise past its top in
// sharp points. Kept inside the ring's own width.
const TEETH = 32
for (let k = 0; k < TEETH; k++) {
  const t = (k + .5) / TEETH * TAU, n = ellN(t), tan: V3 = [-n[1], n[0], 0], h = .25
  const at = (d: number, z: number, side: number): V3 => {
    const [x, y] = ell(t, d)
    return [x + tan[0] * h * side, y + tan[1] * h * side, z]
  }
  const base = [at(OUT - .2, RING_TOP, 1), at(IN + .6, RING_TOP, 1), at(OUT - .5, RING_TOP + 2.3, 1)]
  const back = [at(OUT - .2, RING_TOP, -1), at(IN + .6, RING_TOP, -1), at(OUT - .5, RING_TOP + 2.3, -1)]
  yellow.tri(base[0], base[1], base[2])
  yellow.tri(back[0], back[2], back[1])
  yellow.quad(base[1], back[1], back[2], base[2])
  yellow.quad(back[0], base[0], base[2], back[2])
}

// The mezzanine: a ring of stands inside the columns, red-and-white striped
// to the floor, with a yellow fascia under its railing.
const MEZZ_IN = -10.5, MEZZ_OUT = -4.6, MEZZ_TOP = 4.9, FASCIA = 4.0
const STRIPE_M = 3.2 // one red and one white stripe
const STRIPES = (() => {
  const w = 16, h = 4, data = new Uint8Array(w * h * 4)
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++)
    data.set(x < w / 2 ? [196, 52, 48, 255] : [236, 232, 224, 255], (y * w + x) * 4)
  return encodePng(w, h, data)
})()
{
  const M = 72
  let along = 0
  for (let i = 0; i < M; i++) {
    const t0 = i / M * TAU, t1 = (i + 1) / M * TAU
    const n0 = ellN(t0), n1 = ellN(t1), m0 = n0.map(v => -v) as V3, m1 = n1.map(v => -v) as V3
    const P = (t: number, d: number, z: number): V3 => [...ell(t, d), z]
    const seg = Math.hypot(ell(t1, MEZZ_IN)[0] - ell(t0, MEZZ_IN)[0], ell(t1, MEZZ_IN)[1] - ell(t0, MEZZ_IN)[1])
    const u0 = along / STRIPE_M, u1 = (along + seg) / STRIPE_M
    along += seg
    // Inner face, seen from the tent floor: stripes, then the fascia.
    stripes.quad(P(t1, MEZZ_IN, 0), P(t0, MEZZ_IN, 0), P(t0, MEZZ_IN, FASCIA), P(t1, MEZZ_IN, FASCIA),
      [[u1, 0], [u0, 0], [u0, 1], [u1, 1]])
    quad(yellow, P(t1, MEZZ_IN, FASCIA), P(t0, MEZZ_IN, FASCIA), P(t0, MEZZ_IN, MEZZ_TOP), P(t1, MEZZ_IN, MEZZ_TOP), m1, m0)
    quad(paving, P(t0, MEZZ_OUT, MEZZ_TOP), P(t1, MEZZ_OUT, MEZZ_TOP), P(t1, MEZZ_IN, MEZZ_TOP), P(t0, MEZZ_IN, MEZZ_TOP), up, up)
    quad(paving, P(t0, MEZZ_OUT, 0), P(t1, MEZZ_OUT, 0), P(t1, MEZZ_OUT, MEZZ_TOP), P(t0, MEZZ_OUT, MEZZ_TOP), n0, n1)
  }
}

// --------------------------------------------------------------------------
// Observation towers

type Tower = { c: XY; shafts: number; floors: number[]; lift: number }
// Deck centres are circles fitted to the OSM footprints; all three decks are
// the same 19.6 m saucer. Floors from the real 60, 150 and 226 ft towers.
const DECK_R = 9.8, SHAFT_R = 1.7
const towers: Tower[] = [
  { c: [-31.5, 61.0], shafts: 3, floors: [61.6, 67.4], lift: Math.PI },          // tallest, way/284856408
  { c: [-22.2, 66.8], shafts: 2, floors: [44.8], lift: Math.PI / 2 },            // middle, way/596614135
  { c: [-20.8, 58.8], shafts: 2, floors: [17.6], lift: -Math.PI / 2 },           // shortest, way/596614136
]
for (const tw of towers) {
  const [cx, cy] = tw.c, top = Math.max(...tw.floors)
  const spread = tw.shafts === 3 ? 2.3 : 1.9
  const shafts: XY[] = Array.from({ length: tw.shafts }, (_, k) => {
    const a = tw.lift + Math.PI + (k + .5 - tw.shafts / 2) * (TAU / tw.shafts)
    return [cx + spread * Math.cos(a), cy + spread * Math.sin(a)]
  })
  for (const [x, y] of shafts) column(concrete, x, y, SHAFT_R, 0, top + .1, 12)
  const hub = spread + SHAFT_R + .4
  for (const f of tw.floors) {
    // Saucer: a shallow concrete cone under a white rim and parapet.
    lathe(grey, cx, cy, [[hub, f - 4.2], [DECK_R - .6, f - 1.15], [DECK_R, f - .85]], 20)
    lathe(white, cx, cy, [[DECK_R, f - .85], [DECK_R, f + .75], [DECK_R - .2, f + .95], [DECK_R - .45, f + .95]], 20)
    lathe(white, cx, cy, [[DECK_R - .45, f + .95], [DECK_R - .45, f]], 20) // walks down, so faces in
    disc(grey, cx, cy, hub - .3, DECK_R - .45, f, 20)
    // Underside of the hub, where shafts enter the saucer.
    disc(grey, cx, cy, 0, hub, f - 4.2, 20, down)
  }
  // Capsule elevator: a guide strip up the outside of the cluster, with its
  // car parked part-way up.
  const lx = Math.cos(tw.lift), ly = Math.sin(tw.lift), px = -ly, py = lx
  const box = (p: Part, r0: number, r1: number, w: number, z0: number, z1: number) => {
    const c = (r: number, s: number, z: number): V3 => [cx + lx * r + px * s, cy + ly * r + py * s, z]
    p.quad(c(r1, -w, z0), c(r1, w, z0), c(r1, w, z1), c(r1, -w, z1))
    p.quad(c(r0, -w, z0), c(r1, -w, z0), c(r1, -w, z1), c(r0, -w, z1))
    p.quad(c(r1, w, z0), c(r0, w, z0), c(r0, w, z1), c(r1, w, z1))
    p.quad(c(r0, w, z0), c(r0, -w, z0), c(r0, -w, z1), c(r0, w, z1))
    p.quad(c(r0, -w, z1), c(r1, -w, z1), c(r1, w, z1), c(r0, w, z1))
  }
  const lowest = Math.min(...tw.floors) - 4.2
  box(concrete, spread * .5, hub + .5, .7, 0, lowest + 1)
  const carZ = Math.min(lowest - 4, Math.max(6, lowest * .45))
  box(white, hub + .5, hub + 2.3, 1.1, carZ, carZ + 3.2)
}

// --------------------------------------------------------------------------
// Theaterama, now the Queens Theatre

// OSM way/284856410 in the model frame, clockwise as mapped.
const theatre: XY[] = [[39.1,69.8],[40,67.7],[40.5,65.6],[40.7,63.4],[40.6,60.7],[40.1,58.6],[39.4,56.6],[38,54.2],[36.7,52.5],[35.5,51.4],[33.8,50],[32,49],[29.9,48.1],[27.8,47.6],[25.7,47.4],[23.5,47.5],[21.9,47.8],[19.8,48.4],[17.3,49.5],[15.5,50.7],[13.6,52.6],[12.5,53.8],[11.4,55.7],[10.7,57.2],[10.2,58.7],[9.8,60.3],[9.6,61.9],[9.6,63.5],[9.7,65.1],[7.4,66.4],[6.3,66.1],[5.3,66.3],[4.5,66.8],[4,67.6],[3.9,68.2],[3.9,68.8],[4,69.4],[4.4,70],[5,70.5],[5.7,70.8],[6.6,70.9],[7.3,70.6],[10.2,75.6],[9.6,76.5],[9.5,77.4],[8.8,76.5],[6.6,74.4],[3.6,73],[-.1,73],[-3.7,73.9],[-6.6,75.4],[-4.9,77.6],[-6.8,80.8],[-7.4,83.2],[-7,86.5],[-5.9,88.9],[-4.4,90.9],[-2.1,92.5],[.8,93.5],[4.4,93.7],[4.2,90.3],[7.4,88.7],[9.5,86],[10.6,83.8],[10.4,80.2],[10.1,78.9],[10.7,79.4],[11.3,79.6],[12,79.6],[12.8,79.4],[13.4,79],[13.8,78.4],[14,77.7],[14,77.1],[16.2,75.8],[17.8,76.7],[15.2,83.2],[18.1,84.3],[21.1,85.2],[24.6,85.6],[24.5,84.7],[27.7,84.4],[28,90.4],[31.6,89.6],[35.3,88.4],[39.1,86.6],[42.3,84.3],[44.9,81.9],[46.8,79.6],[44.8,77.5],[46.7,75.2]]

function triangulate(outline: XY[]): XY[][] {
  const area = outline.reduce((n, p, i) => { const q = outline[(i + 1) % outline.length]; return n + p[0] * q[1] - p[1] * q[0] }, 0)
  const pts = area > 0 ? [...outline] : [...outline].reverse()
  const cr = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const out: XY[][] = []
  while (pts.length > 3) {
    let found = false
    for (let i = 0; i < pts.length; i++) {
      const a = pts[(i + pts.length - 1) % pts.length], b = pts[i], c = pts[(i + 1) % pts.length]
      if (cr(a, b, c) <= 1e-9) continue
      if (pts.some(p => p !== a && p !== b && p !== c && cr(a, b, p) >= -1e-9 && cr(b, c, p) >= -1e-9 && cr(c, a, p) >= -1e-9)) continue
      out.push([a, b, c]); pts.splice(i, 1); found = true; break
    }
    if (!found) throw new Error('theatre outline does not triangulate')
  }
  out.push(pts)
  return out
}

/** The lobby wing: the whole outline at one storey and a half, flat-roofed. */
const WING = 6.5
{
  const area = theatre.reduce((n, p, i) => { const q = theatre[(i + 1) % theatre.length]; return n + p[0] * q[1] - p[1] * q[0] }, 0)
  const ccw = area > 0 ? theatre : [...theatre].reverse()
  for (let i = 0; i < ccw.length; i++) {
    const a = ccw[i], b = ccw[(i + 1) % ccw.length]
    drum.quad([a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], WING], [a[0], a[1], WING])
  }
  for (const [a, b, c] of triangulate(theatre)) roof.tri([a[0], a[1], WING], [b[0], b[1], WING], [c[0], c[1], WING])
}
// The original drum, just proud of its mapped outline, under a low white dome.
const DRUM: XY = [25.15, 62.97], DRUM_R = 15.75, DRUM_H = 13.4
lathe(drum, DRUM[0], DRUM[1], [[DRUM_R, 0], [DRUM_R, DRUM_H - .5], [DRUM_R - .15, DRUM_H - .15], [DRUM_R - .5, DRUM_H]], 24)
lathe(white, DRUM[0], DRUM[1], [[DRUM_R - .5, DRUM_H], [DRUM_R - 1.2, DRUM_H + .3], [12.5, 15.2], [9, 16.6], [5, 17.4], [0, 17.8]], 24)
// The glass lobby drum at the west end.
const GLASS: XY = [3.07, 82.86], GLASS_R = 11.1, GLASS_H = 9.2
lathe(glass, GLASS[0], GLASS[1], [[GLASS_R, 0], [GLASS_R, GLASS_H - .9]], 20)
lathe(paving, GLASS[0], GLASS[1], [[GLASS_R, GLASS_H - .9], [GLASS_R, GLASS_H - .3], [GLASS_R - .3, GLASS_H]], 20)
disc(roof, GLASS[0], GLASS[1], 0, GLASS_R - .3, GLASS_H, 20)

// --------------------------------------------------------------------------

const parts = [
  { part: concrete, material: { name: 'concrete', color: 0xb2ada0 } },
  { part: yellow, material: { name: 'crown-yellow', color: 0xe8b940 } },
  { part: white, material: { name: 'deck-white', color: 0xdcd7c9 } },
  { part: grey, material: { name: 'deck-grey', color: 0x8d8b85 } },
  { part: stripes, material: { name: 'mezzanine-stripes', color: 0xffffff, texture: { png: STRIPES } } },
  { part: paving, material: { name: 'mezzanine', color: 0xc6c0b3 } },
  { part: drum, material: { name: 'theatre-wall', color: 0xd4c9b0 } },
  { part: glass, material: { name: 'window', color: 0x7f93a0 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
]
const glb = writeGlb('new-york-state-pavilion', parts, {
  name: 'New York State Pavilion', license: 'CC0-1.0', author: 'Barrelman',
})
const path = new URL('../../landmarks/models/new-york-state-pavilion.glb', import.meta.url).pathname
await Bun.write(path, glb)
const tris = parts.reduce((n, p) => n + p.part.triangles, 0)
console.log(`${path}: ${tris} triangles, ${(glb.length / 1024).toFixed(1)} KB`)
for (const p of parts) console.log(`  ${p.material.name}: ${p.part.triangles}`)
