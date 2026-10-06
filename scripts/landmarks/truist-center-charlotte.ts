/**
 * Truist Center (formerly Hearst Tower), Charlotte — original procedural
 * geometry, CC0-1.0.
 *   bun scripts/landmarks/truist-center-charlotte.ts
 *
 * Map frame: x and y are turned 48° clockwise to Uptown's grid, so +y runs
 * along the outline's long side (NE, toward 6th St) and +x faces SE (College
 * St); z is metres up. Origin is the centroid of OSM way/131139746,
 * 35.2277067, -80.8405740.
 *
 * The tower is an inverted taper. Each face is a stone slab of window bays
 * between piers, running the full height. Blue glass wings fill the corners
 * between the slabs and flare outward as they rise, so the tower is about
 * 1.27× wider at the top than just above the podium. Each wing ends in a
 * pointed silver horn at the top and starts from one above the podium. The
 * slabs end just above the wings, each with a low peaked head over its
 * middle bays, around a flat grey roof. A 44 m stone podium with slit bays and horned
 * corner pavilions fills the rest of the outline, with a glass atrium on its
 * roof against the tower's NE face.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
/** How a wall is dressed: plain, or bands of recessed bays in rows. */
type Kind = { part: Part; bands?: number; rows?: number; width?: number; openTop?: boolean; openBottom?: boolean }

const stone = new Part(), bays = new Part(), glass = new Part()
const silver = new Part(), roof = new Part()

const BEVEL = 0.45, RECESS = 0.6
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }
const cross = (a: V3, b: V3): V3 => [a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2], a[0]*b[1]-a[1]*b[0]]
const lerp = (a: number, b: number, t: number) => a + (b - a) * t
const at = (p: XY, z: number): V3 => [p[0], p[1], z]

/**
 * One wall between two levels of a ring edge. The edge may lean (the wings
 * flare), so points are bilinear in (s along the edge, t up the wall). With
 * bands, the wall becomes stone piers around recessed bays with bevelled
 * reveals, broken by a stone spandrel every few floors (`rows`). An open end
 * leaves the bay running on into the next stage with no reveal.
 */
function wall(kind: Kind, a0: XY, b0: XY, a1: XY, b1: XY, z0: number, z1: number) {
  const dir: V3 = [b0[0] - a0[0], b0[1] - a0[1], 0]
  const rise: V3 = [(a1[0] + b1[0] - a0[0] - b0[0]) / 2, (a1[1] + b1[1] - a0[1] - b0[1]) / 2, z1 - z0]
  const n = unit(cross(dir, rise))
  const P = (s: number, t: number, depth = 0): V3 => {
    const x = lerp(lerp(a0[0], b0[0], s), lerp(a1[0], b1[0], s), t)
    const y = lerp(lerp(a0[1], b0[1], s), lerp(a1[1], b1[1], s), t)
    return [x + n[0] * depth, y + n[1] * depth, lerp(z0, z1, t) + n[2] * depth]
  }
  const panel = (p: Part, s0: number, s1: number, t0: number, t1: number) =>
    p.quad(P(s0, t0), P(s1, t0), P(s1, t1), P(s0, t1))
  const length = (Math.hypot(...dir) + Math.hypot(b1[0] - a1[0], b1[1] - a1[1])) / 2
  const height = Math.hypot(...rise)
  if (!kind.bands || length < 1.5) { panel(kind.part, 0, 1, 0, 1); return }

  const rows = kind.rows ?? 1
  const spacing = 1 / kind.bands, width = spacing * (kind.width ?? 0.72)
  const bs = BEVEL / length, bt = BEVEL / height
  // A spandrel is about a metre deep; half of one sits at each end of a row.
  const half = Math.min(0.55 / height, 0.2 / rows)
  let last = 0
  for (let j = 0; j < kind.bands; j++) {
    const s0 = spacing * (j + 0.5) - width / 2, s1 = s0 + width
    panel(stone, last, s0, 0, 1)
    // `below` is where the previous bay ended: one spandrel panel spans the
    // gap to the next bay rather than one per row end.
    let below = 0
    for (let r = 0; r < rows; r++) {
      const openBottom = r === 0 && kind.openBottom, openTop = r === rows - 1 && kind.openTop
      const t0 = r / rows, t1 = (r + 1) / rows
      const lo = openBottom ? t0 : t0 + half, hi = openTop ? t1 : t1 - half
      if (lo > below) panel(stone, s0, s1, below, lo)
      below = hi
      if (r === rows - 1 && hi < 1) panel(stone, s0, s1, hi, 1)
      const outer: XY[] = [[s0, lo], [s1, lo], [s1, hi], [s0, hi]]
      const inner: XY[] = [
        [s0 + bs, openBottom ? lo : lo + bt], [s1 - bs, openBottom ? lo : lo + bt],
        [s1 - bs, openTop ? hi : hi - bt], [s0 + bs, openTop ? hi : hi - bt],
      ]
      const back = inner.map(([s, t]) => P(s, t, -RECESS))
      bays.quad(back[0], back[1], back[2], back[3])
      for (let k = 0; k < 4; k++) {
        if ((k === 0 && openBottom) || (k === 2 && openTop)) continue
        const l = (k + 1) % 4
        stone.quad(P(...outer[k]), P(...outer[l]), back[l], back[k])
      }
    }
    last = s1
  }
  panel(stone, last, 1, 0, 1)
}

function walls(r0: XY[], r1: XY[], kinds: Kind[], z0: number, z1: number) {
  for (let i = 0; i < r0.length; i++) {
    const j = (i + 1) % r0.length
    wall(kinds[i], r0[i], r0[j], r1[i], r1[j], z0, z1)
  }
}

/** Fan cap over a ring that is star-shaped about `c`. */
function cap(p: Part, ring: XY[], z: number, c: XY) {
  for (let i = 0; i < ring.length; i++) p.tri(at(c, z), at(ring[i], z), at(ring[(i + 1) % ring.length], z))
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
function coping(ring: XY[], z: number, c: XY, surface: Part) {
  const inner = inset(ring, 0.6)
  for (let i = 0; i < ring.length; i++) {
    const j = (i + 1) % ring.length
    stone.quad(at(ring[i], z - 0.6), at(ring[j], z - 0.6), at(inner[j], z), at(inner[i], z))
  }
  cap(surface, inner, z, c)
}

/**
 * A pointed horn: a pyramid over `base` (counter-clockwise from above) whose
 * tip leans out past the corner, like an upturned wingtip.
 */
function horn(base: XY[], tip: XY, z: number, rise: number) {
  const apex = at(tip, z + rise)
  for (let i = 0; i < base.length; i++) silver.tri(at(base[i], z), at(base[(i + 1) % base.length], z), apex)
}

// ---------------------------------------------------------------- tower

/** Tower centre: its SW face stands on the outline's SW edge, centred across it. */
const TX = -0.9, TY = -40.2
/** Turn a first-quadrant point to quadrant k about the tower centre. */
const rot = ([x, y]: XY, k: number): XY => {
  const r: XY = k === 0 ? [x, y] : k === 1 ? [-y, x] : k === 2 ? [-x, -y] : [y, -x]
  return [r[0] + TX, r[1] + TY]
}
const quads = <T>(f: (k: number) => T[]) => [0, 1, 2, 3].flatMap(f)

const A = 12         // half-width of each face's stone slab
const SLAB = 1.2     // how far the slab stands proud of the glass wings
const EDGE = 1.2     // the pale stone strip on each wing's outer corner
const PODIUM = 44, WING0 = 56, WING1 = 187, EAVES = 189, PEAK = 191
const G0 = 15.6, G1 = 20.3  // wing half-width at its foot and its top: 1.27×

/**
 * The shaft at one level: a cross of stone slabs reaching ±(g + SLAB), the
 * notches between them filled by glass wings out to ±g, each with a stone
 * strip on its outer corner. Six points per quadrant, so levels loft; the
 * quadrant's last edge runs to the next quadrant's first point, (A, h).
 */
function shaft(g: number, slab: Kind, wing: Kind) {
  const h = g + SLAB
  const q: XY[] = [[h, -A], [h, A], [g, A], [g, g - EDGE], [g - EDGE, g], [A, g]]
  const kinds: Kind[] = [slab, { part: stone }, wing, { part: stone }, wing, { part: stone }]
  return { ring: quads(k => q.map(p => rot(p, k))), kinds: quads(() => kinds) }
}

// 0–44 m: stone, mostly buried in the podium; only the SW front shows, where
// the slab comes down to the street between the podium's wings.
const SLAB_BAYS = 6
// Quadrant 3's slab faces -y, the SW front; only it gets bays below the
// podium roof. The podium itself is drawn with OSM's full outline.
const SW = 3 * 6
const buried = shaft(G0, { part: stone }, { part: stone })
buried.kinds[SW] = { part: stone, bands: SLAB_BAYS, rows: 4, openTop: true }
walls(buried.ring, buried.ring, buried.kinds, 0, PODIUM)
const neck = shaft(G0, { part: stone, bands: SLAB_BAYS, openTop: true }, { part: stone })
neck.kinds[SW] = { ...neck.kinds[SW], openBottom: true }
walls(neck.ring, neck.ring, neck.kinds, PODIUM, WING0)
// 56–187 m: the flare. Slab bays carry straight on; the wings are glass.
const foot = shaft(G0, { part: stone }, { part: glass })
const head = shaft(G1, { part: stone, bands: SLAB_BAYS, rows: 9, openBottom: true, openTop: true }, { part: glass })
walls(foot.ring, head.ring, head.kinds, WING0, WING1)
const centre: XY = [TX, TY]
cap(roof, head.ring, WING1, centre)

// Horns at both ends of every wing: small brackets where the glass starts,
// tall upturned tips where it stops.
for (let k = 0; k < 4; k++) {
  for (const [g, z, size, rise, lean] of [[G0, WING0, 3.6, 6, 1.2], [G1, WING1, 5.5, 11, 2.2]]) {
    const base: XY[] = [[g - size, g - size], [g, g - size], [g, g - EDGE], [g - EDGE, g], [g - size, g]]
    horn(base.map(p => rot(p, k)), rot([g + lean, g + lean], k), z, rise)
  }
}

// 187–189 m: the slabs rise alone just past the wings.
const H1 = G1 + SLAB
const arm: XY[] = [[H1, -A], [H1, A], [A, A]]
const armKinds: Kind[] = [{ part: stone, bands: SLAB_BAYS, openBottom: true }, { part: stone }, { part: stone }]
walls(quads(k => arm.map(p => rot(p, k))), quads(k => arm.map(p => rot(p, k))), quads(() => armKinds), WING1, EAVES)

// The top is nearly flat; the horns dominate the roofline. Each slab ends
// two metres above the wings with a low peaked head, a metre thick, over its
// middle three bays. The bays under it end in flush pointed heads, which
// stand in for the real rib tracery.
const PW = 6  // half-width of the peaked head
for (let k = 0; k < 4; k++) {
  const P = (x: number, y: number, z: number): V3 => at(rot([x, y], k), z)
  const T = H1 - 1
  stone.tri(P(H1, -PW, EAVES), P(H1, PW, EAVES), P(H1, 0, PEAK))
  stone.tri(P(T, PW, EAVES), P(T, -PW, EAVES), P(T, 0, PEAK))
  stone.quad(P(H1, -PW, EAVES), P(H1, 0, PEAK), P(T, 0, PEAK), P(T, -PW, EAVES))
  stone.quad(P(H1, 0, PEAK), P(H1, PW, EAVES), P(T, PW, EAVES), P(T, 0, PEAK))
  const spacing = (2 * A) / SLAB_BAYS, width = spacing * 0.72 - 2 * BEVEL
  for (let j = 0; j < SLAB_BAYS; j++) {
    const c = -A + spacing * (j + 0.5)
    if (Math.abs(c) + width / 2 >= PW) continue
    const s0 = c - width / 2, s1 = c + width / 2
    // Each head stays inside the peak: 85% of its height above the bay's centre.
    const top = EAVES + (PEAK - EAVES) * (1 - Math.abs(c) / PW) * 0.85
    bays.tri(P(H1 + 0.03, s0, EAVES), P(H1 + 0.03, s1, EAVES), P(H1 + 0.03, c, top))
  }
}

cap(roof, quads(k => arm.map(p => rot(p, k))), EAVES, centre)

// ---------------------------------------------------------------- podium

// OSM's outline, simplified. The SW front is held 1.8 m behind the tower's
// base so the two never share a plane.
const podium: XY[] = [
  [-22.4, -54], [25.9, -54], [28.06, 48.86], [21.47, 56.68],
  [-21.95, 56.19], [-21.93, 50.34], [-27.57, 50.29], [-27.84, -49.87], [-24.12, -52.23],
]
// Art Deco slit bays, about 5.5 m apart, the height of the podium.
const podiumKinds = podium.map((p, i) => {
  const q = podium[(i + 1) % podium.length], length = Math.hypot(q[0] - p[0], q[1] - p[1])
  return { part: stone, bands: length < 8 ? 0 : Math.round(length / 5.5), width: 0.5 }
})
walls(podium, podium, podiumKinds, 0, PODIUM)
coping(podium, PODIUM, [0, 0], roof)

// Horned caps on the podium's corner pavilions, leaning out over each corner.
for (const [c, out] of [
  [[25.9, -54], [1, -1]], [[28.06, 48.86], [1, 1]], [[-27.57, 50.29], [-1, 1]], [[-24.12, -52.23], [-1, -1]],
] as [XY, XY][]) {
  const s = 4.5, x = c[0], y = c[1], ix = -out[0] * s, iy = -out[1] * s
  const base: XY[] = [[x, y], [x + ix, y], [x + ix, y + iy], [x, y + iy]]
  // Counter-clockwise from above whichever corner this is.
  horn(ix * iy > 0 ? base : base.reverse(), [x + out[0], y + out[1]], PODIUM, 6)
}

// Glass atrium on the podium roof against the tower's NE face, between the
// lower horns.
const atrium: XY[] = [[TX - 13, TY + G0], [TX + 13, TY + G0], [TX + 13, TY + G0 + 12], [TX - 13, TY + G0 + 12]]
walls(atrium, atrium, atrium.map(() => ({ part: glass })), PODIUM, WING0 - 2)
coping(atrium, WING0 - 2, [TX, TY + G0 + 6], roof)

const parts = [
  { part: stone, material: { name: 'stone', color: 0xcbbfb0 } },
  { part: bays, material: { name: 'window-bays', color: 0x6585aa } },
  { part: glass, material: { name: 'glass-wings', color: 0x5a7ea8, roughness: 0.6 } },
  { part: silver, material: { name: 'silver', color: 0xdfe2e3, roughness: 0.6 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Truist Center', parts, {
  license: 'CC0-1.0', bearing: 48, elevation: 0, anchor: [35.2277067, -80.840574], height: WING1 + 11,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: 'way/131139746 and its 69 building:parts',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/truist-center-charlotte.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
