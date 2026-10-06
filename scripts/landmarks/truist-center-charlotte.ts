/**
 * Truist Center (formerly Hearst Tower), Charlotte — original procedural
 * geometry, CC0-1.0.
 *   bun scripts/landmarks/truist-center-charlotte.ts
 *
 * Map frame: x and y are turned 48° clockwise to Uptown's grid, so +y runs
 * along the outline's long side (NE) and +x faces SE; z is metres up. Origin
 * is the centroid of OSM way/131139746, 35.2277067, -80.8405740.
 *
 * The building is a stone cross in plan whose four re-entrant corners are
 * filled with blue glass. The glass corners widen as they rise, so the shaft
 * flares to a full square near the top, where pointed metal fins cap them.
 * Above that the stone arms step in twice around a pale metal pyramid.
 * A 48 m stone podium fills the rest of the outline.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
/** A wall's material and band count; open ends let bands run on into the next stage. */
type Open = boolean | ((band: number) => boolean)
type Kind = { part: Part; bands: number; openBottom?: Open; openTop?: Open }

const stone = new Part(), bands = new Part(), glass = new Part()
const metal = new Part(), roof = new Part(), lantern = new Part()

const BEVEL = 0.45, RECESS = 0.6, CHAMFER = 0.5
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }
const cross = (a: V3, b: V3): V3 => [a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2], a[0]*b[1]-a[1]*b[0]]
const lerp = (a: number, b: number, t: number) => a + (b - a) * t

/** Tower centre: its SW arm stands at the outline's SW edge, centred across it. */
const TX = 0.2, TY = -35.6
const at = (p: XY, z: number): V3 => [p[0], p[1], z]

/**
 * One wall between two levels of a ring edge. The edge may lean (the shaft
 * flares), so points are bilinear in (s along the edge, t up the wall).
 * With `count` bands, the wall becomes stone piers around recessed glass
 * bands with bevelled reveals, each running the stage's full height.
 */
function wall(kind: Kind, a0: XY, b0: XY, a1: XY, b1: XY, z0: number, z1: number) {
  const P = (s: number, t: number, depth = 0): V3 => {
    const x = lerp(lerp(a0[0], b0[0], s), lerp(a1[0], b1[0], s), t)
    const y = lerp(lerp(a0[1], b0[1], s), lerp(a1[1], b1[1], s), t)
    return [x + n[0] * depth, y + n[1] * depth, lerp(z0, z1, t) + n[2] * depth]
  }
  const dir: V3 = [b0[0] - a0[0], b0[1] - a0[1], 0]
  const rise: V3 = [(a1[0] + b1[0] - a0[0] - b0[0]) / 2, (a1[1] + b1[1] - a0[1] - b0[1]) / 2, z1 - z0]
  const n = unit(cross(dir, rise))
  const length = (Math.hypot(...dir) + Math.hypot(b1[0] - a1[0], b1[1] - a1[1])) / 2
  const height = Math.hypot(...rise)
  const panel = (p: Part, s0: number, s1: number, t0: number, t1: number) =>
    p.quad(P(s0, t0), P(s1, t0), P(s1, t1), P(s0, t1))
  if (!kind.bands || length < 1.5) { panel(kind.part, 0, 1, 0, 1); return }

  const margin = Math.min(0.9 / height, 0.2)
  const spacing = 1 / kind.bands, width = spacing * 0.6
  const bs = BEVEL / length, bt = BEVEL / height
  const opens = (end: boolean | ((j: number) => boolean) | undefined, j: number) =>
    typeof end === 'function' ? end(j) : !!end
  let last = 0
  for (let j = 0; j < kind.bands; j++) {
    const s0 = spacing * (j + 0.5) - width / 2, s1 = s0 + width
    const openBottom = opens(kind.openBottom, j), openTop = opens(kind.openTop, j)
    const lo = openBottom ? 0 : margin, hi = openTop ? 1 : 1 - margin
    panel(stone, last, s0, 0, 1)
    if (lo > 0) panel(stone, s0, s1, 0, lo)
    if (hi < 1) panel(stone, s0, s1, hi, 1)
    const outer: [number, number][] = [[s0, lo], [s1, lo], [s1, hi], [s0, hi]]
    const inner: [number, number][] = [
      [s0 + bs, openBottom ? 0 : lo + bt], [s1 - bs, openBottom ? 0 : lo + bt],
      [s1 - bs, openTop ? 1 : hi - bt], [s0 + bs, openTop ? 1 : hi - bt],
    ]
    const back = inner.map(([s, t]) => P(s, t, -RECESS))
    bands.quad(back[0], back[1], back[2], back[3])
    for (let k = 0; k < 4; k++) {
      // An open end has no reveal: the band carries on into the next stage.
      if ((k === 0 && openBottom) || (k === 2 && openTop)) continue
      const l = (k + 1) % 4
      stone.quad(P(...outer[k]), P(...outer[l]), back[l], back[k])
    }
    last = s1
  }
  panel(stone, last, 1, 0, 1)
}

/** A star-shaped ring's walls between two levels, edge by edge. */
function walls(r0: XY[], r1: XY[], kinds: Kind[], z0: number, z1: number) {
  for (let i = 0; i < r0.length; i++) {
    const j = (i + 1) % r0.length
    wall(kinds[i], r0[i], r0[j], r1[i], r1[j], z0, z1)
  }
}

/** Fan cap over a ring that is star-shaped about `c`. */
function cap(p: Part, ring: XY[], z: number, c: XY, up = true) {
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length
    if (up) p.tri(at(c, z), at(ring[i], z), at(ring[j], z))
    else p.tri(at(c, z), at(ring[j], z), at(ring[i], z))
  }
}

/** Offset a ring inward along each corner's bisector. */
function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

/** A bevelled coping round the top of a stage, then its flat roof. */
function coping(ring: XY[], z: number, c: XY, surface: Part, rim = stone) {
  const inner = inset(ring, 0.6)
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length
    rim.quad(at(ring[i], z - 0.6), at(ring[j], z - 0.6), at(inner[j], z), at(inner[i], z))
  }
  cap(surface, inner, z, c)
}

const rot = ([x, y]: XY, k: number): XY => {
  const q = ((k % 4) + 4) % 4
  const r: XY = q === 0 ? [x, y] : q === 1 ? [-y, x] : q === 2 ? [-x, -y] : [y, -x]
  return [r[0] + TX, r[1] + TY]
}

/**
 * The shaft ring: a stone cross with arms 2a wide reaching ±h, its notches
 * filled with glass corners out to ±g. Eight points per quadrant, so every
 * level has the same topology and can be lofted.
 */
function shaftRing(h: number, a: number, g: number, faceBands: number, open: { openBottom?: boolean; openTop?: boolean } = {}): { ring: XY[]; kinds: Kind[] } {
  const c = CHAMFER
  const q: XY[] = [[h, -a + c], [h, a - c], [h - c, a], [g, a], [g, g - c], [g - c, g], [a, g], [a, h - c]]
  const k: Kind[] = [
    { part: stone, bands: faceBands, ...open }, { part: stone, bands: 0 }, { part: stone, bands: 0 },
    { part: glass, bands: 0 }, { part: metal, bands: 0 }, { part: glass, bands: 0 },
    { part: stone, bands: 0 }, { part: stone, bands: 0 },
  ]
  return { ring: [0, 1, 2, 3].flatMap(r => q.map(p => rot(p, r))), kinds: [0, 1, 2, 3].flatMap(() => k) }
}

/** A plain stone cross, for the crown stages above the glass corners. */
function crossRing(h: number, a: number, faceBands: number, returnBands: number, openBottom: Open, openTop: Open) {
  const c = CHAMFER
  const q: XY[] = [[h, -a + c], [h, a - c], [h - c, a], [a, a], [a, h - c]]
  const k: Kind[] = [
    { part: stone, bands: faceBands, openBottom, openTop }, { part: stone, bands: 0 },
    { part: stone, bands: returnBands }, { part: stone, bands: returnBands }, { part: stone, bands: 0 },
  ]
  return { ring: [0, 1, 2, 3].flatMap(r => q.map(p => rot(p, r))), kinds: [0, 1, 2, 3].flatMap(() => k) }
}

const centre: XY = [TX, TY]
const ARM = 13.5

// Shaft: buried in the podium below 48 m except on the SW front, where the
// tower comes down to the street between the podium's wings.
const PODIUM = 48, FINS = 165
const base = shaftRing(21, ARM, 17, 6, { openTop: true })
walls(base.ring, base.ring, base.kinds, 0, PODIUM)
// The flare: the stone faces lean out a little and the glass corners a lot,
// so the notched plan of the lower floors fills out to a square at the top.
const top = shaftRing(22.5, ARM, 21.4, 6, { openBottom: true, openTop: true })
walls(base.ring, top.ring, top.kinds, PODIUM, FINS)
cap(roof, top.ring, FINS, centre)

// Pointed metal fins at both ends of each glass corner, each rising to the
// corner's outer edge: a spike standing on the podium roof where the glass
// starts, and a cap over the glass where the flare stops.
function fin(x0: number, x1: number, z: number, rise: number) {
  for (let r = 0; r < 4; r++) {
    const p = [[x0, x0], [x1, x0], [x1, x1], [x0, x1]].map(v => at(rot(v as XY, r), z))
    const apex = at(rot([x1 - 0.4, x1 - 0.4], r), z + rise)
    for (let i = 0; i < 4; i++) metal.tri(p[i], p[(i + 1) % 4], apex)
  }
}
fin(15.5, 19.2, PODIUM, 6)
fin(ARM, 21.4, FINS, 6)

// The crown: three stepped stone stages, each a narrower and shallower
// cross. Each step drops the outermost bay on either side, so the middle
// bands carry on above the ones below and every face ends in a stepped
// gable; stepping the depth too keeps that gable in the side silhouette.
const BAY = (2 * ARM - 2 * CHAMFER) / 6
const armFor = (bays: number) => (bays * BAY + 2 * CHAMFER) / 2
const stages = [
  { z0: FINS, z1: 176, bays: 6, h: 22.5 },
  { z0: 176, z1: 183, bays: 4, h: 20.6 },
  { z0: 183, z1: 190, bays: 2, h: 18.6 },
]
for (const s of stages) {
  const { ring, kinds } = crossRing(s.h, armFor(s.bays), s.bays, 0, s.z0 === FINS, false)
  walls(ring, ring, kinds, s.z0, s.z1)
  coping(ring, s.z1, centre, roof)
}

// Pale metal pyramid between the stepped arms, topped by a glazed lantern.
const pyramid = [[176, 13.4], [198.6, 3.6]] as const
const rings = pyramid.map(([z, r]) => [[r, -r], [r, r], [-r, r], [-r, -r]].map(p => at([p[0] + TX, p[1] + TY], z)))
metal.loft(rings)
const lantern0 = [[3.6, -3.6], [3.6, 3.6], [-3.6, 3.6], [-3.6, -3.6]].map(p => [p[0] + TX, p[1] + TY] as XY)
walls(lantern0, lantern0, lantern0.map(() => ({ part: metal, bands: 0 })), 198.6, 201)
coping(lantern0, 201, centre, lantern, metal)

// Podium: OSM's outline, simplified, with the SW front pulled 1.8 m behind
// the tower's face so the two don't share a plane.
const podium: XY[] = [
  [-22.4, -54.8], [25.9, -54.8], [28.06, 48.86], [21.47, 56.68],
  [-21.95, 56.19], [-21.93, 50.34], [-27.57, 50.29], [-27.84, -49.87], [-24.12, -52.23],
]
const podiumKinds = podium.map((p, i) => {
  const q = podium[(i + 1) % podium.length]
  const length = Math.hypot(q[0] - p[0], q[1] - p[1])
  return { part: stone, bands: length < 8 ? 0 : Math.round(length / 6.5) }
})
walls(podium, podium, podiumKinds, 0, PODIUM)
coping(podium, PODIUM, [0, 0], roof)

const parts = [
  { part: stone, material: { name: 'stone', color: 0xdad4c8 } },
  { part: bands, material: { name: 'window-bands', color: 0x73879d } },
  { part: glass, material: { name: 'glass-corners', color: 0x566f8f } },
  { part: metal, material: { name: 'aluminium', color: 0xd6dadd, roughness: 0.6 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
  { part: lantern, material: { name: 'lantern', color: 0x5f7287 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Truist Center', parts, {
  license: 'CC0-1.0', bearing: 48, elevation: 0, anchor: [35.2277067, -80.840574], height: 201,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: 'way/131139746 and its 69 building:parts',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/truist-center-charlotte.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
