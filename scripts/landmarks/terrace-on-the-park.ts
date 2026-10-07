/**
 * Terrace on the Park (the 1964 World's Fair Port Authority heliport) —
 * original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/terrace-on-the-park.ts
 *
 * Authoring frame: x along the long side of the OSM outline (way/284860789),
 * y across it, z up, metres. Catalog bearing 68.5°, anchor at the outline's
 * centroid. The outline is a 64 × 49 m rounded rectangle and 43 m tall.
 *
 * Four pylons stand at the middle of the four sides, so each elevation reads
 * as a "T": a dark glazed slot runs up the face of each pylon into the box's
 * lower fascia. The glass-walled box sits on top, with a round lower level
 * hanging under its centre, a slightly projecting roof deck (the old
 * helipad) and a small mechanical penthouse.
 *
 * Everything is authored flat-shaded and then smoothed with a crease, so the
 * 45° chamfers and rounded corners shade softly while right angles stay crisp.
 */
import { Part, writeGlb, addGltfTriangles, type V3 } from './mesh'

type XY = [number, number]

const raw = { concrete: new Part(), glass: new Part(), slot: new Part(), roof: new Part() }

// Outline and heights.
const HX = 32, HY = 24.6, R = 2.6 // roof deck: matches the OSM outline
const LIP = .6 // how far the roof deck projects past the walls below it
const BX = HX - LIP, BY = HY - LIP, BR = R - LIP // the box walls
const Z_SOFFIT = 25, Z_BAND0 = 29.6, Z_BAND1 = 36.4, Z_DECK = 37.2, Z_ROOF = 40
const Z_PENT = 43
const CH = .5 // chamfer
const RECESS = .65
// Pylons: centred on each side, outer face flush with the box walls.
const PYLON = { w0: 13, w1: 15, d0: 7, d1: 9, slot: 2.6 }

const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

/**
 * A rounded rectangle, counter-clockwise from above. `faces[i]` names the
 * straight side that edge i lies on (0 south, 1 east, 2 north, 3 west), or
 * -1 on a corner arc. Every inset of it has the same vertex count, so lofts
 * between insets line up.
 */
function rrect(hx: number, hy: number, r: number, seg = 4) {
  const pts: XY[] = [], faces: number[] = []
  const corners: [number, number, number][] = [[hx - r, -hy + r, -90], [hx - r, hy - r, 0], [-hx + r, hy - r, 90], [-hx + r, -hy + r, 180]]
  corners.forEach(([cx, cy, a0], k) => {
    for (let i = 0; i <= seg; i++) {
      const a = (a0 + (90 * i) / seg) * Math.PI / 180
      pts.push([cx + r * Math.cos(a), cy + r * Math.sin(a)])
      faces.push(i === seg ? (k + 1) % 4 : -1)
    }
  })
  return { pts, faces }
}

function loft(p: Part, rings: V3[][]) {
  for (let k = 0; k < rings.length - 1; k++) {
    const a = rings[k], b = rings[k + 1]
    for (let i = 0; i < a.length; i++) {
      const j = (i + 1) % a.length
      p.quad(a[i], a[j], b[j], b[i])
    }
  }
}
function fan(p: Part, ring: V3[], up = true) {
  const c: V3 = [0, 0, 0]
  for (const v of ring) for (let k = 0; k < 3; k++) c[k] += v[k] / ring.length
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length
    if (up) p.tri(c, ring[i], ring[j]); else p.tri(c, ring[j], ring[i])
  }
}

/**
 * The walls of a rounded box between z0 and z1, with one continuous recessed
 * window band on each straight side, stopping short of the corners. The
 * lower fascia of each side carries the dark slot that continues the pylon
 * below it.
 */
function bandedWalls(ring: { pts: XY[]; faces: number[] }, z0: number, z1: number, b0: number, b1: number) {
  const { pts, faces } = ring
  for (let i = 0; i < pts.length; i++) {
    const a = pts[i], b = pts[(i + 1) % pts.length]
    const L = Math.hypot(b[0] - a[0], b[1] - a[1])
    if (faces[i] < 0) {
      raw.concrete.quad([a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
      continue
    }
    const ux = (b[0] - a[0]) / L, uy = (b[1] - a[1]) / L
    // s along the face, z up, d into the building.
    const v = (s: number, z: number, d = 0): V3 => [a[0] + ux * s - uy * d, a[1] + uy * s + ux * d, z]
    const panel = (p: Part, s0: number, s1: number, za: number, zb: number, d = 0) =>
      p.quad(v(s0, za, d), v(s1, za, d), v(s1, zb, d), v(s0, zb, d))
    const e = 1.6 // solid end beside each corner
    const mid = L / 2, half = PYLON.slot / 2
    // Lower fascia, split round the dark slot.
    panel(raw.concrete, 0, mid - half, z0, b0)
    panel(raw.concrete, mid + half, L, z0, b0)
    panel(raw.slot, mid - half, mid + half, z0, b0, RECESS)
    // Slot cheeks, facing into the slot. It opens into the window band above.
    raw.concrete.quad(v(mid - half, z0), v(mid - half, z0, RECESS), v(mid - half, b0, RECESS), v(mid - half, b0))
    raw.concrete.quad(v(mid + half, z0, RECESS), v(mid + half, z0), v(mid + half, b0), v(mid + half, b0, RECESS))
    // Upper fascia and the solid ends.
    panel(raw.concrete, 0, L, b1, z1)
    panel(raw.concrete, 0, e, b0, b1)
    panel(raw.concrete, L - e, L, b0, b1)
    // The recessed glazing and its reveals; the sill is split by the slot.
    panel(raw.glass, e, L - e, b0, b1, RECESS)
    raw.concrete.quad(v(e, b0), v(mid - half, b0), v(mid - half, b0, RECESS), v(e, b0, RECESS))
    raw.concrete.quad(v(mid + half, b0), v(L - e, b0), v(L - e, b0, RECESS), v(mid + half, b0, RECESS))
    raw.concrete.quad(v(L - e, b1), v(e, b1), v(e, b1, RECESS), v(L - e, b1, RECESS)) // head
    raw.concrete.quad(v(e, b0), v(e, b0, RECESS), v(e, b1, RECESS), v(e, b1))
    raw.concrete.quad(v(L - e, b0, RECESS), v(L - e, b0), v(L - e, b1), v(L - e, b1, RECESS))
  }
}

// ---- The box -------------------------------------------------------------

const box = rrect(BX, BY, BR)
const boxIn = rrect(BX - CH, BY - CH, BR - CH)
// Soffit with a chamfered lower edge.
fan(raw.concrete, at(boxIn.pts, Z_SOFFIT), false)
loft(raw.concrete, [at(boxIn.pts, Z_SOFFIT), at(box.pts, Z_SOFFIT + CH)])
bandedWalls(box, Z_SOFFIT + CH, Z_DECK, Z_BAND0, Z_BAND1)

// The roof deck projects a little: its underside, walls, chamfered coping,
// and a grey deck sunk just below the coping.
const deck = rrect(HX, HY, R)
const deckIn = (n: number) => rrect(HX - n, HY - n, R - n).pts
loft(raw.concrete, [at(box.pts, Z_DECK), at(deckIn(.25), Z_DECK), at(deck.pts, Z_DECK + .25),
  at(deck.pts, Z_ROOF - CH), at(deckIn(CH), Z_ROOF), at(deckIn(1.1), Z_ROOF), at(deckIn(1.4), Z_ROOF - .35)])
fan(raw.roof, at(deckIn(1.4), Z_ROOF - .35))

// ---- Penthouse -------------------------------------------------------------

const pent = rrect(13, 8.5, 1.4)
const pentTop = rrect(13 - CH, 8.5 - CH, 1.4 - CH)
loft(raw.concrete, [at(pent.pts, Z_ROOF - .35), at(pent.pts, Z_PENT - CH), at(pentTop.pts, Z_PENT)])
// A concrete top, so the penthouse reads against the grey deck.
fan(raw.concrete, at(pentTop.pts, Z_PENT))

// ---- The round lower level under the box ---------------------------------

const circle = (r: number, n = 16): XY[] => Array.from({ length: n }, (_, i) => {
  const a = (i + .5) * 2 * Math.PI / n
  return [r * Math.cos(a), r * Math.sin(a)]
})
const DR = 14.5, D0 = 20.4
loft(raw.concrete, [at(circle(DR - CH), D0), at(circle(DR), D0 + CH), at(circle(DR), D0 + 1.1), at(circle(DR - RECESS), D0 + 1.1)])
loft(raw.glass, [at(circle(DR - RECESS), D0 + 1.1), at(circle(DR - RECESS), Z_SOFFIT - .8)])
loft(raw.concrete, [at(circle(DR - RECESS), Z_SOFFIT - .8), at(circle(DR), Z_SOFFIT - .8), at(circle(DR), Z_SOFFIT)])
fan(raw.concrete, at(circle(DR - CH), D0), false)

// ---- Pylons ----------------------------------------------------------------

/**
 * One pylon on the south side (outer face at y = -BY), in plan, at a given
 * height: a slab with a glazed slot let into the middle of its outer face
 * and chamfered corners. It widens and deepens towards the top.
 */
function pylonRing(t: number) {
  const w = (PYLON.w0 + (PYLON.w1 - PYLON.w0) * t) / 2
  const d = PYLON.d0 + (PYLON.d1 - PYLON.d0) * t
  const s = PYLON.slot / 2, c = CH, y0 = -BY, y1 = -BY + d
  // Counter-clockwise from above, starting at the slot's west lip.
  const ring: XY[] = [
    [-s, y0], [-s, y0 + RECESS], [s, y0 + RECESS], [s, y0],
    [w - c, y0], [w, y0 + c], [w, y1 - c], [w - c, y1],
    [-w + c, y1], [-w, y1 - c], [-w, y0 + c], [-w + c, y0],
  ]
  return ring
}
const rot = ([x, y]: XY, k: number, sx: number, sy: number): XY => {
  // Turn the south pylon to the other sides, rescaling its offset to the
  // side's own half-width.
  const yy = y + BY
  if (k === 0) return [x, -sy + yy]
  if (k === 1) return [sx - yy, x]
  if (k === 2) return [-x, sy - yy]
  return [-sx + yy, -x]
}
const Z_PYLON = Z_SOFFIT + CH
for (let k = 0; k < 4; k++) {
  const lo = pylonRing(0).map((p) => rot(p, k, BX, BY))
  const hi = pylonRing(1).map((p) => rot(p, k, BX, BY))
  const a = at(lo, 0), b = at(hi, Z_PYLON)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length
    // The slot's back wall is the edge from index 1 to 2.
    const p = i === 1 ? raw.slot : raw.concrete
    p.quad(a[i], a[j], b[j], b[i])
  }
}

// ---- The low base building between the pylons -----------------------------

const base = rrect(15, 10, 2)
const baseIn = (n: number) => rrect(15 - n, 10 - n, 2 - n).pts
loft(raw.concrete, [at(base.pts, 0), at(base.pts, .9), at(baseIn(RECESS), .9)])
loft(raw.glass, [at(baseIn(RECESS), .9), at(baseIn(RECESS), 3.6)])
loft(raw.concrete, [at(baseIn(RECESS), 3.6), at(base.pts, 3.6), at(base.pts, 4.6), at(baseIn(CH), 5.1), at(baseIn(1), 5.1)])
fan(raw.roof, at(baseIn(1), 5.1))

// ---- Smooth and write ------------------------------------------------------

function smooth(src: Part, crease = 50) {
  const out = new Part()
  const index = Uint32Array.from({ length: src.pos.length / 3 }, (_, i) => i)
  addGltfTriangles(out, Float32Array.from(src.pos), index, { creaseDegrees: crease })
  return out
}

const parts = [
  { part: smooth(raw.concrete), material: { name: 'concrete', color: 0xd9d0bf } },
  { part: smooth(raw.glass, 30), material: { name: 'window', color: 0x5f6c78, roughness: .8 } },
  { part: smooth(raw.slot, 30), material: { name: 'window-2', color: 0x4b5661, roughness: .8 } },
  { part: smooth(raw.roof, 30), material: { name: 'roof', color: 0xbdb9b1 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Terrace on the Park', parts, {
  license: 'CC0-1.0', bearing: 68.5, elevation: 0, anchor: [40.7445364, -73.8507490], height: Z_PENT,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  footprint: { x: [-HX, HX], y: [-HY, HY] },
  note: 'OSM outline way/284860789; four mid-side pylons carry a glazed box with a projecting roof deck',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/terrace-on-the-park.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
