/**
 * The rock spires of Black Spire Outpost, Star Wars: Galaxy's Edge, Disney's
 * Hollywood Studios — procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-batuu-spires.ts
 *
 * Map frame: x east, y north, z up, metres, bearing 0. The origin is the
 * mean of the five main spires' bases. One model holds the whole group,
 * since the spires are one piece of rockwork and read as a group from every
 * side.
 *
 * The spires are the petrified trees the outpost is named after: craggy
 * blue-grey stone needles, fluted with vertical clefts, narrowing unevenly
 * and leaning a little, with sharp lesser points clustered round their tops
 * and stepping down their flanks. They grow in fused clusters out of broad,
 * low mesas of layered tan and ochre rockwork. The tallest group stands
 * behind the Millennium Falcon, along the north and west face of Smugglers
 * Run's show building (way/688502132), which wraps round the Falcon's
 * courtyard. Every base falls inside that building's outline, so the model
 * replaces nothing: the show building is far larger and keeps its own
 * extrusion.
 *
 * Evidence
 * - Published: 135 ft (41 m) spires (Wikipedia, "Star Wars: Galaxy's Edge";
 *   fan sites give "more than 130 feet" for the tallest). The figure may be
 *   Disneyland's; Florida's tallest is taken to match.
 * - Photos from the courtyard: Commons, Jedi94 (CC BY-SA 4.0; "Black Spire,
 *   Walt Disney World" and "Star Wars, Galaxy's Edge at WDW"), Eden, Janine
 *   and Jim (CC BY 2.0), Steven Miller (CC BY 2.0), Ekynox mg (CC BY-SA 4.0);
 *   Mapillary, jonahadkins and Lanka6359 (CC BY-SA 4.0). They show two tall
 *   needles behind the Falcon with a tunnel between them, a shorter crown of
 *   points west of them and lower needles further round, all rising from a
 *   tan rock mass about 40% of their height.
 * - Positions: bearings to each spire from two Mapillary photos, their camera
 *   poses solved against the Falcon model's cockpit and mandible tips, and
 *   cross-checked with the long north-pointing spire shadows across the
 *   courtyard in the USGS NAIP orthophoto (public domain).
 * - OSM's natural=bare_rock areas in the land (way/720122310, 732497074–6,
 *   805583653, 805720820, 805936653–4, 823713027, 823729287, 589550174,
 *   823707702, 823709426, 770929187, 1090823872) are the low rockwork walls
 *   round the Resistance camp and the market, and the outcrops in the market;
 *   none of them is under a spire, so none is used.
 *
 * Doubts: the positions rest on two photo bearings that cross at a narrow
 * angle, so each spire may be off by 5 to 10 m along the line of sight. The
 * heights other than the tallest are scaled from the photos. The mesas'
 * outline and height are estimated from the photos; the needles' facets and
 * lesser points are a stylised stand-in for the sculpted rock.
 */
import { Part, cross, len, sub, writeGlb, type V3 } from './mesh'
import { finish } from './palette'

const slate = new Part()      // the blue-grey needles
const sand = new Part()       // the tan rock mesas they grow from
const ochre = new Part()      // the darker strata in the mesas

const TAU = Math.PI * 2
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A flat triangle facing away from `inside`. */
function tri(p: Part, a: V3, b: V3, c: V3, inside: V3) {
  const n = cross(sub(b, a), sub(c, a))
  if (len(n) < 1e-9) return
  const m: V3 = [(a[0] + b[0] + c[0]) / 3 - inside[0], (a[1] + b[1] + c[1]) / 3 - inside[1], (a[2] + b[2] + c[2]) / 3 - inside[2]]
  if (dot(n, m) >= 0) p.tri(a, b, c)
  else p.tri(a, c, b)
}

/** A small deterministic random source, so the rock comes out the same every run. */
let seed = 11
const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647 }
const between = (a: number, b: number) => a + (b - a) * rnd()

/** Loft closed rings (same corner count) with flat faces, then cap the top with an apex or a flat lid. */
function loft(p: Part | ((k: number) => Part), rings: V3[][], top: V3 | 'flat', bottom = false) {
  const pick = typeof p === 'function' ? p : () => p
  const n = rings[0].length
  const centre = (r: V3[]): V3 => [r.reduce((s, q) => s + q[0], 0) / n, r.reduce((s, q) => s + q[1], 0) / n, r.reduce((s, q) => s + q[2], 0) / n]
  for (let k = 0; k < rings.length - 1; k++) {
    const c0 = centre(rings[k]), c1 = centre(rings[k + 1])
    const inside: V3 = [(c0[0] + c1[0]) / 2, (c0[1] + c1[1]) / 2, (c0[2] + c1[2]) / 2]
    for (let i = 0; i < n; i++) {
      const j = (i + 1) % n
      tri(pick(k), rings[k][i], rings[k][j], rings[k + 1][j], inside)
      tri(pick(k), rings[k][i], rings[k + 1][j], rings[k + 1][i], inside)
    }
  }
  const last = rings[rings.length - 1], c = centre(last), q = pick(rings.length - 1)
  if (top === 'flat') {
    // A low dome over the lid, so a mesa top is not a dead-flat roof.
    const peak: V3 = [c[0], c[1], c[2] + 1.2]
    for (let i = 0; i < n; i++) tri(q, last[i], last[(i + 1) % n], peak, [c[0], c[1], c[2] - 5])
  } else {
    for (let i = 0; i < n; i++) tri(q, last[i], last[(i + 1) % n], top, [c[0], c[1], c[2] - 1])
  }
  if (bottom) {
    const b = rings[0], cb = centre(b)
    for (let i = 0; i < n; i++) tri(pick(0), b[i], b[(i + 1) % n], cb, [cb[0], cb[1], cb[2] + 1])
  }
}

/**
 * A needle: a fluted cross-section (alternate corners pulled in, so the
 * faces fold into vertical clefts) lofted up a wandering, leaning axis,
 * narrowing unevenly to a point. Returns the axis and radius at a height,
 * for hanging lesser points on its flanks.
 */
function needle(x: number, y: number, z0: number, h: number, r: number, lean: [number, number], sides = 10, levels = 6) {
  const base = Array.from({ length: sides }, (_, i) => (i % 2 ? between(0.62, 0.8) : between(0.95, 1.2)))
  const turn = rnd() * TAU
  const axis: Array<[number, number, number, number]> = [] // x, y, z, radius
  let wx = 0, wy = 0
  for (let k = 0; k <= levels; k++) {
    const t = k / (levels + 1)
    // Uneven taper: the profile thins as (1 - t), with each level a little
    // fatter or thinner than the last, and the axis drifting as it leans.
    const s = Math.pow(1 - t, 0.6) * between(0.86, 1.08)
    if (k) { wx += between(-0.2, 0.2) * r; wy += between(-0.2, 0.2) * r }
    axis.push([x + lean[0] * t + wx, y + lean[1] * t + wy, z0 + (h - z0) * t, r * s])
  }
  const rings = axis.map(([ax, ay, az, ar]) => base.map((f, i) => {
    const a = turn + (i / sides) * TAU
    const k = ar * f * between(0.9, 1.1)
    return [ax + Math.cos(a) * k, ay + Math.sin(a) * k, az] as V3
  }))
  const last = axis[axis.length - 1]
  loft(slate, rings, [last[0] + lean[0] * 0.15 + between(-0.3, 0.3), last[1] + lean[1] * 0.15 + between(-0.3, 0.3), h])
  return (z: number) => {
    let k = 0
    while (k < axis.length - 2 && axis[k + 1][2] < z) k++
    const a = axis[k], b = axis[k + 1], t = Math.min(1, Math.max(0, (z - a[2]) / (b[2] - a[2])))
    return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[3] + (b[3] - a[3]) * t] as [number, number, number]
  }
}

/**
 * A main spire: the needle itself, and `n` lesser points clustered near its
 * top and stepping down its flanks, each a small needle rooted inside the
 * main one and breaking out of its side.
 */
function spire(x: number, y: number, z0: number, h: number, r: number, lean: [number, number], n: number) {
  const at = needle(x, y, z0, h, r, lean)
  for (let j = 0; j < n; j++) {
    // Most of them high up, a few lower down the flank.
    const t = j < n * 0.6 ? between(0.62, 0.86) : between(0.35, 0.62)
    const z = z0 + (h - z0) * t
    const [ax, ay, ar] = at(z)
    const a = rnd() * TAU, d = ar * 0.55
    const top = z + (h - z0) * between(0.1, 0.2) * (1.15 - t * 0.4)
    needle(ax + Math.cos(a) * d, ay + Math.sin(a) * d, z - (h - z0) * 0.08, Math.min(top, h - 1.5), ar * 0.5,
      [Math.cos(a) * ar * 0.4, Math.sin(a) * ar * 0.4], 6, 2)
  }
}

/**
 * A mesa: a broad, low, irregular block of layered rock in stepped tiers,
 * each set back a little, with a thin ochre stratum between them.
 */
function mesa(x: number, y: number, rx: number, ry: number, rot: number, h: number) {
  const sides = 11
  const radii = Array.from({ length: sides }, () => between(0.75, 1.15))
  // The stratum wanders up and down round the mesa, so it reads as a bed
  // of rock rather than a painted belt.
  const ring = (z: number, s: number, dz = 0): V3[] => radii.map((f, i) => {
    const a = (i / sides) * TAU
    const px = Math.cos(a) * rx * f * s * between(0.95, 1.05), py = Math.sin(a) * ry * f * s * between(0.95, 1.05)
    return [x + px * Math.cos(rot) - py * Math.sin(rot), y + px * Math.sin(rot) + py * Math.cos(rot), z + between(-dz, dz)]
  })
  const z1 = h * between(0.45, 0.55)
  const rings = [ring(0, 1), ring(z1, 0.95, 1.2), ring(z1 + 1.1, 0.9, 1.2), ring(h, 0.72, 1.0)]
  loft((k) => (k === 1 ? ochre : sand), rings, 'flat')
}

// Positions in metres east and north of the Falcon OSM trace's centroid
// (-81.56183, 28.35378). The origin is the mean of the five main needles.
const MAIN: Array<[number, number]> = [[-12, -29], [-19, -7], [-31, -5], [8, -36], [-20, -20]]
const OX = MAIN.reduce((s, p) => s + p[0], 0) / MAIN.length
const OY = MAIN.reduce((s, p) => s + p[1], 0) / MAIN.length
const L = (x: number, y: number): [number, number] => [x - OX, y - OY]

// The mesas: one long mass along the show building's north-west face behind
// the Falcon, heaped higher round the two tallest needles.
for (const [x, y, rx, ry, rot, h] of [
  [-13, -27, 12, 9, 0.3, 18],   // under the tallest
  [-20, -10, 10, 8, -0.4, 16],  // under the second
  [-31, -6, 8, 7, 0.2, 14],     // under the western crown
  [-21, -19, 9, 7, 1.0, 14],    // the saddle with the tunnel between them
  [6, -34, 8, 6, 0.4, 11],       // the low spur south-east
  [-3, -32, 8, 5, 0.1, 11],      // linking it to the tallest
] as const) {
  const [lx, ly] = L(x, y)
  mesa(lx, ly, rx, ry, rot, h)
}

// The needles: main ones with their lesser points, and companions fused to
// them at the base, as the photos show them growing in clusters.
for (const [x, y, z0, h, r, lx, ly, n] of [
  [-12, -29, 9, 41, 6.6, 1.0, 1.4, 7],   // the tallest, left of the tunnel from the courtyard
  [-8, -27.5, 9, 26, 3.8, 0.6, 0.4, 3],  // fused companion on its east
  [-16, -25.5, 9, 21, 3.4, -0.6, 0.6, 2],
  [-19, -7, 8, 36, 5.8, -0.8, 0.9, 6],   // the second, right of the tunnel
  [-15.5, -10, 8, 22, 3.4, 0.5, -0.3, 2], // fused companion
  [-31, -5, 7, 32, 4.8, 0, 0.8, 5],      // the western crown
  [-28, -3, 7, 28, 3.6, 0.6, 0.6, 3],
  [-33.5, -8, 7, 25, 3.4, -0.5, -0.2, 2],
  [-20, -20, 6, 23, 3.8, 0.2, 0.3, 3],   // lesser points behind the tunnel
  [8, -36, 5, 18, 3.6, 0.4, -0.3, 3],    // the low spur south-east
] as const) {
  const [px, py] = L(x, y)
  spire(px, py, z0 + 1, h, r, [lx, ly], n)
}

const parts = [
  // Slate pulled light, as STYLE.md asks of a large dark surface, but kept
  // blue-grey enough to read as the dark rock against the tan.
  { part: slate, material: finish('spire-slate', 0x98a0a6) },
  { part: sand, material: finish('spire-sandstone', 0xd4bf9a) },
  { part: ochre, material: finish('spire-ochre', 0xbf9d72) },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Black Spire Outpost spires', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground', origin: [OX, OY],
})
await Bun.write(new URL('../../landmarks/models/wdw-batuu-spires.glb', import.meta.url), glb)
console.log(`wdw-batuu-spires.glb: ${triangles} triangles, ${glb.length} bytes; origin ${OX.toFixed(1)}, ${OY.toFixed(1)} m from the Falcon trace`)
