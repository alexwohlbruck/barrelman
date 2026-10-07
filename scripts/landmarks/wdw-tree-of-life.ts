/**
 * The Tree of Life, Disney's Animal Kingdom — procedural, CC0-1.0.
 *   bun scripts/landmarks/wdw-tree-of-life.ts
 *
 * Map frame: x east, y north, z up, metres; origin at the centroid of the OSM
 * outline (way/295879946, "Tree of Life", Q3538315), on the ground. Bearing 0:
 * the tree has no front.
 *
 * Evidence
 * - OSM (measured): the outline way/295879946, 2,280 m², about 60 × 60 m
 *   across its spurs, no height. The canopy's plan is this outline, smoothed
 *   (a low-order Fourier fit of its radius about the centroid) and pulled in
 *   a little, so the crown covers what it replaces without its spikes.
 * - Published: 145 ft (44 m) tall; trunk about 50 ft (15 m) across at the
 *   base (Wikipedia, "The Tree of Life (Disney)").
 * - Photos (Wikimedia Commons; credits in the placement report):
 *   - "Tree of Life, Disney's Animal Kingdom.jpg" (Jedi94, CC BY-SA 4.0),
 *     side-on from the south-west across the water: the proportions. The
 *     crown is an umbrella, much wider than it is deep, with a fairly flat
 *     top and a ragged edge; the trunk and its spreading limbs take the
 *     lower half of the height, the crown the upper; the bulbous trunk flares
 *     into buttress roots about one and a half times its waist.
 *   - "-ARBOL DE LA VIDA-…panoramio.jpg" (Ivan Curra, CC BY-SA 3.0), from
 *     below: thick twisting limbs under the crown, the carved bark.
 *   - "Tree of Life (49560198253).jpg", "The Tree of Life05/06.jpg", the
 *     ThrillZing 2006 set: other sides, the same squat dome.
 * - Estimated: the trunk's waist (about 15 m, from the photos against the
 *   published base), the limb count and spread (nine visible limbs), the
 *   crown's underside heights. The animal carvings are too fine to model;
 *   they are drawn as broad lighter bark masses swelling out of the trunk and
 *   roots, which is how they read from a distance.
 * - Not modelled: the rockwork and waterfalls at the foot, the garden paths
 *   (ground, per STYLE.md).
 */
import { Part, addGltfTriangles, cross, len, sub, writeGlb, type V3 } from './mesh'
import { finish } from './palette'

const TAU = Math.PI * 2
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const plus = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const scale = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const clamp = (x: number, a = 0, b = 1) => Math.min(b, Math.max(a, x))
const smoothstep = (a: number, b: number, x: number) => { const t = clamp((x - a) / (b - a)); return t * t * (3 - 2 * t) }
/** Deterministic noise in [0, 1). */
const hash = (n: number) => { const s = Math.sin(n * 127.1 + 311.7) * 43758.5453; return s - Math.floor(s) }

const bark = new Part(), carving = new Part()
const leafA = new Part(), leafB = new Part(), leafC = new Part()

/** Build in a scratch part, then smooth-shade into `target`, keeping creases sharper than `crease`. */
function smooth(target: Part, build: (p: Part) => void, crease = 70) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos), Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

// ---------------------------------------------------------------- the plan

// way/295879946 about its centroid (-81.5905973, 28.3579434), metres.
const OUTLINE: [number, number][] = [
  [-1.5, -25.4], [-4.5, -19.3], [-8.1, -16.3], [-14.3, -18.0], [-17.9, -16.0], [-18.8, -11.2], [-24.6, -9.6],
  [-27.8, -6.5], [-33.3, -6.3], [-34.9, -3.8], [-31.9, 2.5], [-30.3, 5.9], [-34.3, 7.9], [-32.7, 11.1],
  [-26.2, 9.7], [-20.9, 12.5], [-19.6, 19.2], [-19.6, 23.6], [-16.5, 24.7], [-13.5, 23.1], [-6.2, 20.0],
  [-0.1, 24.1], [5.6, 25.3], [11.1, 23.6], [17.4, 23.3], [19.9, 28.3], [23.4, 28.6], [21.8, 24.8],
  [21.7, 18.4], [24.8, 11.8], [23.2, 4.5], [25.0, -1.3], [26.2, -7.7], [21.2, -10.8], [19.9, -17.3],
  [22.9, -24.0], [23.1, -29.3], [18.7, -30.2], [14.3, -26.2], [10.6, -26.3], [4.3, -30.2], [1.0, -31.5],
  [0.2, -29.3], [2.3, -26.5], [1.8, -24.6],
]
/** Distance from the centroid to the outline along azimuth `a` (radians from +x). */
function rayOut(a: number) {
  const d = [Math.cos(a), Math.sin(a)]
  let best = 0
  for (let i = 0; i < OUTLINE.length; i++) {
    const [x0, y0] = OUTLINE[i], [x1, y1] = OUTLINE[(i + 1) % OUTLINE.length]
    const ex = x1 - x0, ey = y1 - y0, den = d[0] * ey - d[1] * ex
    if (Math.abs(den) < 1e-9) continue
    const t = (x0 * ey - y0 * ex) / den, u = (x0 * d[1] - y0 * d[0]) / den
    if (t > 0 && u >= 0 && u <= 1) best = Math.max(best, t)
  }
  return best
}
// A Fourier fit to order 3 of the outline's radius: the crown follows the
// footprint's broad shape but not its spurs and notches.
const FIT = (() => {
  const N = 180, r = Array.from({ length: N }, (_, i) => rayOut((i / N) * TAU))
  const c: [number, number][] = []
  for (let k = 0; k <= 3; k++) {
    let a = 0, b = 0
    for (let i = 0; i < N; i++) { const t = (i / N) * TAU; a += r[i] * Math.cos(k * t); b += r[i] * Math.sin(k * t) }
    c.push([(a * (k ? 2 : 1)) / N, (b * 2) / N])
  }
  return c
})()
const crownR = (a: number) => 1.0 * FIT.reduce((s, [ca, cb], k) => s + ca * Math.cos(k * a) + (k ? cb * Math.sin(k * a) : 0), 0)

// --------------------------------------------------------------- the trunk

// Trunk centre: a little west of the outline's centroid, under the crown's
// heaviest side (the outline reaches furthest west).
const TX = -3, TY = 0
/** Trunk radius by height: buttress flare, a waist, and a swell where the limbs part. */
const trunkProfile: [number, number][] = [
  [0, 15], [1.5, 13.2], [3.5, 11.5], [6, 10.3], [9, 9.6], [12, 9.6], [15, 10.1], [18, 10.9], [21, 11.4], [23, 11],
]
const trunkAt = (z: number) => {
  let i = 0
  while (i < trunkProfile.length - 2 && trunkProfile[i + 1][0] < z) i++
  const [z0, r0] = trunkProfile[i], [z1, r1] = trunkProfile[i + 1]
  return r0 + (r1 - r0) * clamp((z - z0) / (z1 - z0))
}
// Seven buttress lobes, strong at the foot and fading by mid-trunk, turning
// as they rise: the twisting, fluted trunk of the photos.
const LOBES = 7
const lobe = (a: number, z: number) => {
  const twist = z * 0.035
  const f = Math.cos(LOBES * (a + twist) + 0.6)
  // Rounded buttresses with broad hollows between, not spikes.
  const amp = 0.24 * (1 - smoothstep(1, 12, z)) + 0.06
  return 1 + amp * (Math.sign(f) * Math.pow(Math.abs(f), 0.7) * 0.8) + 0.04 * Math.cos(3 * a + z * 0.2)
}
smooth(bark, (p) => {
  const SEG = 42
  const zs = [0, 1.2, 2.6, 4.2, 6.2, 8.5, 11, 13.5, 16, 18.5, 21, 23]
  const rings = zs.map((z) => Array.from({ length: SEG }, (_, i): V3 => {
    const a = (i / SEG) * TAU, r = trunkAt(z) * lobe(a, z)
    return [TX + r * Math.cos(a), TY + r * Math.sin(a), z]
  }))
  p.loft(rings)
  // A domed cap the limbs grow out of; the crown hides it.
  const top = rings[rings.length - 1], c: V3 = [TX, TY, 30]
  for (let i = 0; i < SEG; i++) p.tri(top[i], top[(i + 1) % SEG], c)
}, 75)

/** A round, tapered tube along a path; ends left open sit inside other masses. */
function tube(p: Part, path: V3[], radii: number[], sides = 8) {
  const rings = path.map((c, i) => {
    const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
    const u = unit(cross(Math.abs(axis[2]) > 0.9 ? [1, 0, 0] : [0, 0, 1], axis))
    const v = cross(axis, u)
    return Array.from({ length: sides }, (_, j) => {
      const a = (j / sides) * TAU
      return plus(c, plus(scale(u, radii[i] * Math.cos(a)), scale(v, radii[i] * Math.sin(a))))
    })
  })
  p.loft(rings)
}

// The limbs: ten thick boughs leaving the top of the trunk and spreading
// almost level out to the crown's edge, under an open, lifted canopy: the
// spreading arms that show between trunk and leaves from every side.
const LIMBS = 10
for (let k = 0; k < LIMBS; k++) {
  const a = (k / LIMBS) * TAU + 0.25 + (hash(k) - 0.5) * 0.3
  const reach = crownR(a) * (0.82 + hash(k + 20) * 0.08)
  const z0 = 17 + hash(k + 40) * 4
  const path: V3[] = []
  const radii: number[] = []
  const STEPS = 6
  for (let s = 0; s <= STEPS; s++) {
    const t = s / STEPS
    const r = 8 + (reach - 8) * Math.pow(t, 0.85)
    const z = z0 + (26.5 - z0) * Math.pow(t, 0.9)
    const wob = (hash(k * 7 + s) - 0.5) * 0.25 * t
    path.push([TX * (1 - t) + r * Math.cos(a + wob), TY + r * Math.sin(a + wob), z])
    radii.push(3.6 - 2.5 * t)
  }
  smooth(bark, (p) => tube(p, path, radii, 7), 80)
}

/** An ellipsoid from a once-subdivided icosahedron (80 triangles), smooth-shaded. */
const SPHERE = (() => {
  const g = (1 + Math.sqrt(5)) / 2
  const v: V3[] = [[-1, g, 0], [1, g, 0], [-1, -g, 0], [1, -g, 0], [0, -1, g], [0, 1, g], [0, -1, -g], [0, 1, -g], [g, 0, -1], [g, 0, 1], [-g, 0, -1], [-g, 0, 1]].map((p) => unit(p as V3))
  const faces = [[0, 11, 5], [0, 5, 1], [0, 1, 7], [0, 7, 10], [0, 10, 11], [1, 5, 9], [5, 11, 4], [11, 10, 2], [10, 7, 6], [7, 1, 8],
    [3, 9, 4], [3, 4, 2], [3, 2, 6], [3, 6, 8], [3, 8, 9], [4, 9, 5], [2, 4, 11], [6, 2, 10], [8, 6, 7], [9, 8, 1]]
  // Wound so each face points out in the map frame.
  let tris: V3[][] = faces.map((f) => {
    const t = f.map((i) => v[i])
    const n = cross(sub(t[1], t[0]), sub(t[2], t[0]))
    return n[0] * t[0][0] + n[1] * t[0][1] + n[2] * t[0][2] > 0 ? t : [t[0], t[2], t[1]]
  })
  const next: V3[][] = []
  for (const [a, b, c] of tris) {
    const ab = unit(scale(plus(a, b), 0.5)), bc = unit(scale(plus(b, c), 0.5)), ca = unit(scale(plus(c, a), 0.5))
    next.push([a, ab, ca], [ab, b, bc], [ca, bc, c], [ab, bc, ca])
  }
  tris = next
  return tris
})()
function blob(p: Part, c: V3, r: V3, seed = 0, rough = 0.12) {
  // Lumpy, so a clump of leaves isn't a ball.
  const bump = (n: V3) => 1 + rough * (Math.sin(n[0] * 3.1 + seed) * Math.sin(n[1] * 2.7 + seed * 1.7) + 0.5 * Math.sin(n[2] * 3.7 + seed * 0.3))
  const P = (n: V3): V3 => { const b = bump(n); return [c[0] + n[0] * r[0] * b, c[1] + n[1] * r[1] * b, c[2] + n[2] * r[2] * b] }
  const N = (n: V3): V3 => unit([n[0] / r[0], n[1] / r[1], n[2] / r[2]])
  for (const [a, b, d] of SPHERE) p.tri(P(a), P(b), P(d), undefined, undefined, undefined, [N(a), N(b), N(d)])
}

// The carvings: broad bark masses swelling from the trunk and roots, lighter
// than the bark, like the reliefs of animals the trunk is made of.
for (let k = 0; k < 7; k++) {
  const a = (k / 7) * TAU + hash(k + 60) * 0.4
  const z = 2.5 + hash(k + 80) * 13
  const r = trunkAt(z) * lobe(a, z) - 1.2
  const size = 3.8 + hash(k + 100) * 1.4
  // Orient the ellipsoid in the trunk's tangent plane: long side along the trunk.
  const c: V3 = [TX + r * Math.cos(a), TY + r * Math.sin(a), z]
  const radial: V3 = [Math.cos(a), Math.sin(a), 0], tangent: V3 = [-Math.sin(a), Math.cos(a), 0]
  const local = new Part()
  blob(local, [0, 0, 0], [size * 1.1, size * 0.55, size * 1.4], k, 0.18)
  // Rotate scratch (x = tangent, y = radial, z = up) into place.
  for (let i = 0; i < local.pos.length; i += 9) {
    const corner = (j: number): V3 => {
      const gx = local.pos[i + j * 3], gy = local.pos[i + j * 3 + 1], gz = local.pos[i + j * 3 + 2]
      const m: V3 = [gx, -gz, gy] // glTF back to map frame
      return plus(c, plus(plus(scale(tangent, m[0]), scale(radial, m[1])), [0, 0, m[2]]))
    }
    const normal = (j: number): V3 => {
      const gx = local.nrm[i + j * 3], gy = local.nrm[i + j * 3 + 1], gz = local.nrm[i + j * 3 + 2]
      const m: V3 = [gx, -gz, gy]
      return unit(plus(plus(scale(tangent, m[0]), scale(radial, m[1])), [0, 0, m[2]]))
    }
    carving.tri(corner(0), corner(1), corner(2), undefined, undefined, undefined, [normal(0), normal(1), normal(2)])
  }
}

// --------------------------------------------------------------- the crown

// An umbrella, clearly wider than it is tall: a broad, fairly flat top at
// about 44 m, a ragged edge hanging to about 25 m, and an underside lifted
// over the limbs. Trunk and limbs take the lower half, the crown the upper,
// as in photo 01. From above, the edge is lobed rather than round.
const TOP = 44
const lobed = (a: number) => crownR(a) * (1 + 0.07 * Math.cos(6 * a + 0.8) + 0.04 * Math.cos(11 * a + 2.1))
const crownTop = (t: number) => TOP - 2.5 - 9 * Math.pow(t, 2.2)
const UNDER: [number, number][] = [[1, 25.5], [0.85, 26.5], [0.6, 28.5], [0.35, 30.5]]
const under = (t: number) => {
  let i = 0
  while (i < UNDER.length - 2 && UNDER[i + 1][0] > t) i++
  const [t0, z0] = UNDER[i], [t1, z1] = UNDER[i + 1]
  return z0 + (z1 - z0) * clamp((t - t0) / (t1 - t0))
}
// The canopy's body, hidden by the leaf clumps over it but closing it from below.
smooth(leafC, (p) => {
  const SEG = 36
  const ring = (t: number, z: (t: number) => number, k = 0.92) => Array.from({ length: SEG }, (_, i): V3 => {
    const a = (i / SEG) * TAU, r = lobed(a) * t * k
    return [r * Math.cos(a), r * Math.sin(a), z(t)]
  })
  const ts = [1, 0.8, 0.55, 0.3]
  p.loft(ts.map((t) => ring(t, (u) => crownTop(u) - 1.5)))
  const apex: V3 = [0, 0, crownTop(0) - 1.5], last = ring(0.3, (u) => crownTop(u) - 1.5)
  for (let i = 0; i < SEG; i++) p.tri(last[i], last[(i + 1) % SEG], apex)
  // Rim: from the top edge down to the underside edge.
  // The rim's lower edge is ragged, never a ruled line.
  const ragged = ring(1, () => 25.5).map((v, i): V3 => [v[0], v[1], 24 + 3.5 * hash(i * 5 + 1)])
  p.loft([ragged, ring(1, (u) => crownTop(u) - 1.5)])
  const us = UNDER.map(([t]) => t)
  p.loft(us.map((t) => ring(t, under).reverse()))
  const uc: V3 = [TX * 0.3, TY * 0.3, 32], ul = ring(us[us.length - 1], under).reverse()
  for (let i = 0; i < SEG; i++) p.tri(ul[i], ul[(i + 1) % SEG], uc)
}, 60)

/**
 * A clump of leaves: a flat, lumpy cushion, seven-sided, much wider than
 * deep. Many small flat clumps make a canopy; a few round ones make broccoli.
 */
function cushion(p: Part, c: V3, r: number, h: number, seed: number) {
  const N = 7
  const jag = (i: number, k: number) => 1 + 0.22 * (hash(seed * 11 + i * 3 + k) - 0.5)
  const ring = (k: number, z: number, rr: number) => Array.from({ length: N }, (_, i): V3 => {
    const a = (i / N) * TAU + seed
    return [c[0] + r * rr * jag(i, k) * Math.cos(a), c[1] + r * rr * jag(i, k) * Math.sin(a), c[2] + z]
  })
  const lo = ring(0, -0.5 * h, 0.7), mid = ring(1, 0, 1), hi = ring(2, 0.45 * h, 0.62)
  const top: V3 = [c[0], c[1], c[2] + 0.6 * h], bot: V3 = [c[0], c[1], c[2] - 0.6 * h]
  smooth(p, (q) => {
    q.loft([lo, mid, hi])
    for (let i = 0; i < N; i++) {
      q.tri(hi[i], hi[(i + 1) % N], top)
      q.tri(lo[(i + 1) % N], lo[i], bot)
    }
  }, 75)
}

// The leaves: cushions on a sunflower spiral over the plan, flat across the
// top and drooping at the edge, some hanging below it in wisps. Lighter
// greens on top where the sun catches, deeper ones at the edge.
const COUNT = 82
for (let i = 0; i < COUNT; i++) {
  const t = Math.sqrt((i + 0.5) / COUNT) * 1.0
  const a = i * 2.39996 + (hash(i) - 0.5) * 0.3
  const R = lobed(a)
  const edge = smoothstep(0.75, 1.0, t)
  const r = (3.6 + 1.6 * hash(i + 200)) * (1 - 0.15 * edge)
  const h = 2.4 + 1.0 * hash(i + 400)
  const droop = edge * (2 + 6 * hash(i + 500))
  const z = crownTop(Math.min(t, 1)) - droop + (hash(i + 300) - 0.5) * 1.6
  const pick = hash(i + 700) + 0.45 * edge - 0.35 * (1 - t)
  const part = pick < 0.35 ? leafA : pick < 0.8 ? leafB : leafC
  cushion(part, [R * t * Math.cos(a), R * t * Math.sin(a), z], r, h, i)
}

// ------------------------------------------------------------------ output

// Identity finishes from photo 01 (Jedi94), pulled to the palette's
// lightness (STYLE.md): the tan carved bark, its sunlit reliefs, and three
// leaf greens from shade to sun.
const parts = [
  { part: bark, material: finish('tol-bark', 0xb39a7e) },
  { part: carving, material: finish('tol-carving', 0xc4ab8a) },
  { part: leafA, material: finish('tol-leaf-light', 0x94c172) },
  { part: leafB, material: finish('tol-leaf', 0x7aac5e) },
  { part: leafC, material: finish('tol-leaf-deep', 0x689a54) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', '))
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('The Tree of Life', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: TOP,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-tree-of-life.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
