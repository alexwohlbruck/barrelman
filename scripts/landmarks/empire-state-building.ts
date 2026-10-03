/**
 * Empire State Building — original procedural geometry, CC0-1.0.
 *
 *   bun scripts/landmarks/empire-state-building.ts
 *
 * Map frame: x = v (across Fifth Avenue), y = u (uptown), z = metres up.
 * Place at 40.7485288, -73.9859714, bearing 29°, elevation 0.
 * OSM supplies tier envelopes, not polygon vertices: corner notches are
 * stylised within those envelopes. The western 23.8 m annex is excluded.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Envelope = [number, number, number, number]
const stone = new Part()
const glass = new Part()
const spandrel = new Part()
const trim = new Part()
const roof = new Part()
const steel = new Part()
const dark = new Part()
const CX = 28.2, CY = 1.85

function rectangle([x0, x1, y0, y1]: Envelope): XY[] {
  return [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
}

/** Manhattan's stair-step corner recesses, counter-clockwise from above. */
function shoulders([a, b, c, d]: Envelope, dx: number, dy: number): XY[] {
  return [[a + dx, c], [b - dx, c], [b - dx, c + dy * .45],
    [b - dx * .45, c + dy * .45], [b - dx * .45, c + dy], [b, c + dy],
    [b, d - dy], [b - dx * .45, d - dy], [b - dx * .45, d - dy * .45],
    [b - dx, d - dy * .45], [b - dx, d], [a + dx, d],
    [a + dx, d - dy * .45], [a + dx * .45, d - dy * .45],
    [a + dx * .45, d - dy], [a, d - dy], [a, c + dy],
    [a + dx * .45, c + dy], [a + dx * .45, c + dy * .45], [a + dx, c + dy * .45]]
}

function chamfer([a, b, c, d]: Envelope, n: number): XY[] {
  return [[a + n, c], [b - n, c], [b, c + n], [b, d - n],
    [b - n, d], [a + n, d], [a, d - n], [a, c + n]]
}

const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

/** These notched rings are star-shaped, so a centre fan handles concavity. */
function cap(p: Part, ring: XY[], z: number, up = true) {
  const c: V3 = [ring.reduce((s, v) => s + v[0], 0) / ring.length,
    ring.reduce((s, v) => s + v[1], 0) / ring.length, z]
  const v = at(ring, z)
  for (let i = 0; i < v.length; i++) {
    const j = (i + 1) % v.length
    if (up) p.tri(c, v[i], v[j])
    else p.tri(c, v[j], v[i])
  }
}

function solid(p: Part, ring: XY[], bottom: number, top: number) {
  p.loft([at(ring, bottom), at(ring, top)])
  cap(p, ring, top)
  cap(p, ring, bottom, false)
}

/** Real inset strips: piers, headers, reveals and glass share no coplanar faces. */
function facade(ring: XY[], bottom: number, top: number, pitch = 4.1, entrance = false) {
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1])
    const ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
    // Positive depth points outside a counter-clockwise ring.
    const v = (s: number, z: number, depth = 0): V3 =>
      [a[0] + ux * s + uy * depth, a[1] + uy * s - ux * depth, z]
    const panel = (p: Part, s0: number, s1: number, z0: number, z1: number, depth = 0) =>
      p.quad(v(s0, z0, depth), v(s1, z0, depth), v(s1, z1, depth), v(s0, z1, depth))
    if (entrance && i === 1) {
      // Fifth Avenue's centred Deco portal is set into the podium, within
      // the measured footprint. Tall side lights keep the street face calm.
      const middle = CY - a[1], left = middle - 6, right = middle + 6
      panel(stone, 0, left, bottom, top)
      panel(stone, right, length, bottom, top)
      panel(stone, left, right, 14.5, top)
      panel(dark, left, right, bottom, 14.5, -.28)
      stone.quad(v(left, bottom), v(left, bottom, -.28), v(left, 14.5, -.28), v(left, 14.5))
      stone.quad(v(right, bottom, -.28), v(right, bottom), v(right, 14.5), v(right, 14.5, -.28))
      for (const s of [middle - 2.15, middle + 2.15]) panel(trim, s - .22, s + .22, bottom, 14.5, -.02)
      for (const z of [7, 12.8, 14]) panel(spandrel, left, right, z, z + .22, -.12)
      for (let s = 2; s < length - 2; s += 4.1) {
        if (s > left - 2 && s < right + 1) continue
        panel(glass, s, s + 1.5, 4, 17.7, .002)
        panel(spandrel, s, s + 1.5, 9, 9.6, .004)
      }
      continue
    }
    const count = length < 3 ? 0 : Math.floor((length - .6) / pitch)
    if (!count) { panel(stone, 0, length, bottom, top); continue }
    const spacing = length / count
    const width = Math.min(1.85, spacing * .48)
    const lo = bottom + 1.3, hi = top - 1.4
    panel(stone, 0, length, bottom, lo)
    panel(stone, 0, length, hi, top)
    let previous = 0
    for (let j = 0; j < count; j++) {
      const start = spacing * (j + .5) - width / 2, end = start + width
      panel(stone, previous, start, lo, hi)
      panel(glass, start, end, lo, hi, -.22)
      stone.quad(v(start, lo), v(start, lo, -.22), v(start, hi, -.22), v(start, hi))
      stone.quad(v(end, lo, -.22), v(end, lo), v(end, hi), v(end, hi, -.22))
      stone.quad(v(start, lo, -.22), v(start, lo), v(end, lo), v(end, lo, -.22))
      stone.quad(v(start, hi), v(start, hi, -.22), v(end, hi, -.22), v(end, hi))
      // Pair floors at map scale so horizontal detail does not overpower piers.
      for (let z = Math.ceil((lo + 1) / 7.8) * 7.8; z < hi - .9; z += 7.8)
        panel(spandrel, start, end, z, z + .75, -.20)
      previous = end
    }
    panel(stone, previous, length, lo, hi)
  }
}

/** Raised stone coping surrounds a slightly lower, quiet grey terrace. */
function terrace(ring: XY[], bottom: number, top: number) {
  const cx = ring.reduce((s, p) => s + p[0], 0) / ring.length
  const cy = ring.reduce((s, p) => s + p[1], 0) / ring.length
  const inner: XY[] = ring.map(([x, y]) => [cx + (x - cx) * .972, cy + (y - cy) * .972])
  trim.loft([at(ring, bottom), at(ring, top)])
  trim.loft([at(inner, top - .45).reverse(), at(inner, top).reverse()])
  const a = at(ring, top), b = at(inner, top)
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length
    trim.quad(a[i], a[j], b[j], b[i])
  }
  cap(roof, inner, top - .45)
}

function tier(ring: XY[], bottom: number, top: number) {
  facade(ring, bottom, top - .65)
  terrace(ring, top - .65, top)
}

// The outline describes the low full-block podium, not a 443 m solid slab.
// Keep its full 129.6 × 59.4 m envelope while the tower sits east of the anchor.
const base: XY[] = [[-37.1, -25.3], [92.5, -25.3], [92.5, 30.5],
  [88.9, 34.1], [-31.8, 34.1], [-37.1, 28.8], [-37.1, 0]]
solid(stone, base, 0, 2)
facade(base, 2, 19.5, 4.5, true)
terrace(base, 19.5, 20.5)

const tiers = [
  { way: 137425131, z0: 20.5, z1: 75, bounds: [-19.2, 75, -23.4, 26.9] as Envelope, notch: [15, 8] },
  { way: 137425133, z0: 75, z1: 90, bounds: [-8, 63.3, -23.4, 26.9] as Envelope, notch: [8.5, 7] },
  { way: 137425128, z0: 90, z1: 115, bounds: [-6.4, 61.6, -18.7, 22.5] as Envelope, notch: [8, 6] },
  { way: 137425127, z0: 115, z1: 255, bounds: [-.4, 55.5, -18.7, 22.5] as Envelope, notch: [6.6, 5.3] },
  { way: 137425129, z0: 255, z1: 290, bounds: [1.6, 53.5, -16.7, 20.5] as Envelope, notch: [5.7, 4.6] },
  { way: 137425125, z0: 290, z1: 310, bounds: [5.2, 50.5, -12.5, 16.2] as Envelope, notch: [4, 3.2] },
]
for (const t of tiers) tier(shoulders(t.bounds, t.notch[0], t.notch[1]), t.z0, t.z1)
tier(chamfer([6.2, 49.5, -11.6, 15.2], 2.2), 310, 320)
tier(chamfer([9.2, 46.5, -8.6, 12.2], 2), 320, 330)

// Observatory promenade: a visible pale terrace and an inset lantern.
solid(dark, chamfer([17.4, 39, -7, 10.7], 2), 330, 335)
solid(trim, chamfer([16.8, 39.6, -7.6, 11.3], 2), 334.6, 335.5)

// A sparse open railing reads as the outdoor 86th-floor observatory,
// without a solid dark belt or texture around the crown.
const railing = chamfer([10, 45.7, -7.8, 11.4], 2)
for (let i = 0; i < railing.length; i++) {
  const a = railing[i], b = railing[(i + 1) % railing.length]
  const length = Math.hypot(b[0] - a[0], b[1] - a[1])
  const nx = -(b[1] - a[1]) / length * .075, ny = (b[0] - a[0]) / length * .075
  const bar: XY[] = [[a[0] - nx, a[1] - ny], [b[0] - nx, b[1] - ny],
    [b[0] + nx, b[1] + ny], [a[0] + nx, a[1] + ny]]
  solid(steel, bar, 331.4, 331.6)
  const count = Math.ceil(length / 5)
  for (let j = 0; j < count; j++) {
    const x = a[0] + (b[0] - a[0]) * j / count, y = a[1] + (b[1] - a[1]) * j / count
    solid(steel, rectangle([x - .085, x + .085, y - .085, y + .085]), 330, 331.5)
  }
}

// Faceted mooring mast: tall Art Deco ribs and gently narrowing shoulders.
const mastSections = [
  [335.5, 6.6, 6.55], [346, 6.6, 6.55], [352, 4.05, 4.05],
  [373.5, 4.05, 4.05], [377.5, 3.8, 3.8], [381, 2.8, 2.8],
  [386, 2.2, 2.2], [390, 1.9, 1.9],
] as const
const mastRings = mastSections.map(([z, rx, ry]) => {
  // The two overlapping OSM mast envelopes have centres 5 cm apart.
  const cx = z >= 352 ? 28.15 : CX
  return at(chamfer([cx-rx, cx+rx, CY-ry, CY+ry], rx * .29), z)
})
steel.loft(mastRings)
steel.cap(mastRings.at(-1)!, true)
// Long metal flutes break the mast into ribs, including its diagonal faces.
for (let k = 0; k < 3; k++) {
  const lower = mastRings[k], upper = mastRings[k + 1]
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    const mix = (a: V3, b: V3, t: number): V3 => {
      const x = a[0] + (b[0] - a[0]) * t, y = a[1] + (b[1] - a[1]) * t
      return [CX + (x - CX) * 1.002, CY + (y - CY) * 1.002, a[2]]
    }
    glass.quad(mix(lower[i], lower[j], .32), mix(lower[i], lower[j], .68),
      mix(upper[i], upper[j], .68), mix(upper[i], upper[j], .32))
  }
}

function octagon(z: number, r: number): V3[] {
  return Array.from({ length: 8 }, (_, i): V3 => {
    const a = i * Math.PI / 4 + Math.PI / 8
    return [28.15 + r * Math.cos(a), CY + r * Math.sin(a), z]
  })
}
function pole(p: Part, sections: [number, number][]) {
  const rings = sections.map(([z, r]) => octagon(z, r))
  p.loft(rings)
  p.cap(rings[0], false)
  p.cap(rings.at(-1)!, true)
}
pole(dark, [[373.5, 4.45], [376.5, 4.45]])
pole(steel, [[376.5, 4.5], [377.5, 4.5]])
pole(steel, [[387, 2.6], [390, 2.6], [391, 2.05], [409, 1.55], [422, 1.1], [435, .58], [443.2, .16]])
for (const z of [393, 401, 410, 419, 428]) pole(trim, [[z, z < 410 ? 2.2 : 1.35], [z + .65, z < 410 ? 2.2 : 1.35]])

const parts = [
  { part: stone, material: { name: 'stone', color: 0xd5cebc } },
  { part: glass, material: { name: 'glass', color: 0x82959c, roughness: .65 } },
  { part: spandrel, material: { name: 'aluminium-spandrels', color: 0xa5afb0 } },
  { part: trim, material: { name: 'limestone-trim', color: 0xe1daca } },
  { part: roof, material: { name: 'terrace', color: 0xb9b4a8 } },
  { part: steel, material: { name: 'steel', color: 0xb6c0c0, roughness: .55 } },
  { part: dark, material: { name: 'observatory-glass', color: 0x61727a } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 10_000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Empire State Building', parts, {
  license: 'CC0-1.0', bearing: 29, elevation: 0,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  anchor: [40.7485288, -73.9859714], height: 443.2,
  excludedAnnex: 'way/265260949',
  footprint: { x: [-37.1, 92.5], y: [-25.3, 34.1] },
  note: 'Measured OSM envelopes; stylised corner recesses and facade details',
})
const out = new URL('../../landmarks/models/empire-state-building.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
