/**
 * The four Kaskey statues of Independence Square, Charlotte — procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/independence-square-statues.ts [out.glb]
 *
 * Map frame: x east, y north, z up, metres. The origin is the centre of the
 * Trade and Tryon crossing (OSM node 172358289), and the model is placed at
 * bearing 48°, so its +y runs up North Tryon Street and its ±x along Trade.
 *
 * Raymond Kaskey's four bronzes (1995) stand one on each corner, set back from
 * the kerb, all facing the middle of the crossing. Each is a half-length
 * figure, about 2.8× life, rising from the top of a tall square shaft of
 * pinkish granite like a ship's figurehead, with a tapering bronze pendant
 * hanging down the shaft's front face below it. Shaft top 4.5 m, overall about 7.6 m; The
 * Future's raised child reaches 8.9 m.
 *
 * Positions are the OSM artwork nodes (tourism=artwork, artist Raymond
 * Kaskey), measured from the junction node:
 *   Commerce        node/8415199021  ( 1.9, 14.4) m east/north — north corner
 *   The Future      node/8415199020  (15.4, -1.6)                — east corner
 *   Industry        node/8415199022  ( 0.0,-13.3)                — south corner
 *   Transportation  node/7986487900  (-14.6, 0.7)                — west corner
 *
 * Kept deliberately simple (landmarks/STYLE.md): a bevelled granite shaft on a
 * wider base, and a few smooth bronze masses per figure, each posed so its
 * silhouette differs at phone size — the miner's hat and pan, the mill worker
 * with a child on her hip, the railroad worker's arms akimbo, and the woman
 * holding a child high over her head. There is no building to replace.
 */
import { Part, addGltfTriangles, cross, sub, len, writeGlb, type V3 } from './mesh'

const granite = new Part()
const base = new Part()
const bronze = new Part()
const dark = new Part()
const TAU = Math.PI * 2
const unit = (v: V3): V3 => v.map(n => n / (len(v) || 1)) as V3
const plus = (a: V3, b: V3): V3 => a.map((n, i) => n + b[i]) as V3
const scale = (a: V3, s: number): V3 => a.map(n => n * s) as V3

// ------------------------------------------------------------------ helpers

type XY = [number, number]

/** A rectangle's outline with its corners cut by `b`, and per-corner normals. */
function rim(cx: number, cy: number, hx: number, hy: number, b: number): { pts: XY[]; nrm: XY[] } {
  const pts: XY[] = [
    [cx - hx + b, cy - hy], [cx + hx - b, cy - hy], [cx + hx, cy - hy + b], [cx + hx, cy + hy - b],
    [cx + hx - b, cy + hy], [cx - hx + b, cy + hy], [cx - hx, cy + hy - b], [cx - hx, cy - hy + b],
  ]
  const nrm: XY[] = [[0, -1], [0, -1], [1, 0], [1, 0], [0, 1], [0, 1], [-1, 0], [-1, 0]]
  return { pts, nrm }
}

/** A stone block with chamfered vertical and top edges, smooth-shaded. */
function block(p: Part, z0: number, z1: number, hx: number, hy = hx,
  { cx = 0, cy = 0, b = 0.12, top = [hx, hy] as XY } = {}) {
  const tb = Math.min(b, (z1 - z0) * 0.4)
  const lo = rim(cx, cy, hx, hy, b)
  const hi = rim(cx, cy, top[0], top[1], b)
  const lid = rim(cx, cy, top[0] - tb, top[1] - tb, Math.max(0.03, b - tb * 0.4))
  const tilt = (hx - top[0]) / (z1 - z0)
  const sideN = lo.nrm.map(n => unit([n[0], n[1], tilt]))
  const ring = (r: XY[], z: number): V3[] => r.map(([x, y]) => [x, y, z])
  const band = (a: V3[], na: V3[], c: V3[], nc: V3[]) => {
    for (let i = 0; i < 8; i++) {
      const j = (i + 1) % 8
      p.tri(a[i], a[j], c[j], undefined, undefined, undefined, [na[i], na[j], nc[j]])
      p.tri(a[i], c[j], c[i], undefined, undefined, undefined, [na[i], nc[j], nc[i]])
    }
  }
  const A = ring(lo.pts, z0), B = ring(hi.pts, z1 - tb)
  band(A, sideN, B, sideN)
  band(B, sideN, ring(lid.pts, z1), lid.pts.map(() => [0, 0, 1] as V3))
  const C = ring(lid.pts, z1)
  for (let i = 1; i < 7; i++) p.tri(C[0], C[i], C[i + 1])
}

/** Smooth each sculpted piece separately, keeping edges at joins and caps. */
function smooth(target: Part, build: (p: Part) => void, crease = 70) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos),
    Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

/**
 * A horizontal ring. `sq` > 2 squares it off (a superellipse), so the hips
 * can sit flush on the square shaft without its corners poking through.
 */
function oval(c: V3, rx: number, ry: number, n = 12, sq = 2): V3[] {
  const f = (t: number) => Math.sign(t) * Math.abs(t) ** (2 / sq)
  return Array.from({ length: n }, (_, i) => {
    const a = (i + 0.5) / n * TAU
    return [c[0] + rx * f(Math.cos(a)), c[1] + ry * f(Math.sin(a)), c[2]]
  })
}

/** A closed loft of horizontal rings, rows of [z, rx, ry, cx, cy, squareness]. */
function body(p: Part, rows: number[][], n = 12, crease = 70) {
  smooth(p, mesh => {
    const rings = rows.map(([z, rx, ry, cx = 0, cy = 0, sq = 2]) => oval([cx, cy, z], rx, ry, n, sq))
    mesh.loft(rings)
    mesh.cap(rings[0], false)
    mesh.cap(rings[rings.length - 1], true)
  }, crease)
}

function ellipsoid(p: Part, c: V3, r: V3, n = 10, rows = 6) {
  smooth(p, mesh => {
    const rings = Array.from({ length: rows + 1 }, (_, j) => {
      const a = -Math.PI / 2 + j / rows * Math.PI
      const k = j === 0 || j === rows ? 0 : Math.cos(a)
      return oval([c[0], c[1], c[2] + r[2] * Math.sin(a)], r[0] * k, r[1] * k, n)
    })
    mesh.loft(rings)
  }, 80)
}

/**
 * A rounded tapered limb along a path; each ring is square to the path. The
 * ring frame is carried along by parallel transport, so a bent elbow doesn't
 * twist the limb flat.
 */
function tube(p: Part, path: V3[], widths: number[], depths = widths, sides = 8) {
  const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
  smooth(p, mesh => {
    let u: V3 | undefined
    const rings = path.map((c, i) => {
      const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
      const ref: V3 = u ?? (Math.abs(axis[2]) > 0.9 ? [1, 0, 0] : [0, 0, 1])
      u = unit(sub(ref, scale(axis, dot(ref, axis))))
      const v = cross(axis, u)
      return Array.from({ length: sides }, (_, j) => {
        const a = j / sides * TAU
        return plus(c, plus(scale(u!, widths[i] * Math.cos(a)), scale(v, depths[i] * Math.sin(a))))
      })
    })
    mesh.loft(rings)
    mesh.cap(rings[0], false)
    mesh.cap(rings[rings.length - 1], true)
  })
}

/** A flat disc of radius r, thickness t, centred at c and tilted forward by `tilt` rad. */
function disc(p: Part, c: V3, r: number, t: number, tilt: number, n = 14) {
  const rot = ([x, y, z]: V3): V3 => [c[0] + x, c[1] + y * Math.cos(tilt) - z * Math.sin(tilt), c[2] + y * Math.sin(tilt) + z * Math.cos(tilt)]
  const lo = oval([0, 0, -t / 2], r, r, n).map(rot), hi = oval([0, 0, t / 2], r * 0.92, r * 0.92, n).map(rot)
  smooth(p, mesh => { mesh.loft([lo, hi]); mesh.cap(lo, false); mesh.cap(hi, true) }, 50)
}

// ------------------------------------------------------------- one statue
//
// Each statue is authored in its own frame — the shaft centred on the
// origin, the figure facing +y — then turned to face the crossing.

const SHAFT_TOP = 4.5
type Kit = { granite: Part; base: Part; bronze: Part; dark: Part }

function pedestal(k: Kit) {
  // A wider plinth, a chamfered step, and the tall shaft (1.0 m square in the
  // photos, about a fifth of its height).
  block(k.base, 0, 0.9, 0.68, 0.68, { b: 0.1 })
  block(k.base, 0.9, 1.05, 0.6, 0.6, { b: 0.08, top: [0.52, 0.52] })
  block(k.granite, 1.05, SHAFT_TOP, 0.5, 0.5, { b: 0.08 })
}

/**
 * The bronze shared by all four: hips merging into the shaft top, a torso
 * leaning a little out over the front face, neck and head, and the pendant
 * hanging down the shaft below. `lean` pushes the torso forward.
 */
function torso(k: Kit, { broad = 0.62, lean = 0.2, pendant = 1.5, pendantW = 0.46 } = {}) {
  // Hips start square on the shaft top (its corners at ±0.5 m), then round
  // off into the waist and chest.
  body(k.bronze, [
    [SHAFT_TOP - 0.05, 0.5, 0.5, 0, 0, 5],
    [SHAFT_TOP + 0.2, 0.53, 0.52, 0, 0.06, 4],
    // Thighs and lap, as in the photos: the figure is cut off at the knee.
    [SHAFT_TOP + 0.6, 0.54, 0.5, 0, 0.12, 3],
    [5.5, 0.5, 0.42, 0, lean, 2.4],
    [5.95, broad * 0.92, 0.44, 0, lean + 0.04],
    [6.45, broad, 0.44, 0, lean + 0.02],
    [6.72, broad * 0.9, 0.36, 0, lean],
    [6.9, 0.34, 0.25, 0, lean],
  ])
  tube(k.bronze, [[0, lean, 6.8], [0, lean + 0.04, 7.08]], [0.2, 0.18], [0.2, 0.18], 8)
  // The pendant: a broad bronze tongue down the front face of the shaft,
  // narrowing to a blunt tip.
  body(k.dark, [
    [SHAFT_TOP - pendant, 0.1, 0.05, 0, 0.53],
    [SHAFT_TOP - pendant * 0.65, pendantW * 0.6, 0.11, 0, 0.58],
    [SHAFT_TOP - pendant * 0.3, pendantW * 0.9, 0.17, 0, 0.62],
    [SHAFT_TOP + 0.05, pendantW, 0.2, 0, 0.63],
    [SHAFT_TOP + 0.35, pendantW * 0.7, 0.12, 0, 0.58],
  ], 10)
}

/** Head centred over the neck; returns its centre. */
function head(k: Kit, lean: number, r: V3 = [0.25, 0.28, 0.33]): V3 {
  const c: V3 = [0, lean + 0.05, 7.3]
  ellipsoid(k.bronze, c, r, 10, 6)
  return c
}

/**
 * An arm: shoulder → elbow → wrist. Upper arm and forearm are separate
 * tubes joined by a rounded elbow, so a sharp bend stays round.
 */
function arm(k: Kit, [s, e, w]: V3[], [a, b, c] = [0.2, 0.17, 0.14]) {
  tube(k.bronze, [s, e], [a, b])
  ellipsoid(k.bronze, e, [b, b, b], 8, 4)
  tube(k.bronze, [e, w], [b, c])
  ellipsoid(k.bronze, w, [c + 0.02, c + 0.02, c + 0.02], 8, 4)
}

const statues: { name: string; at: XY; build: (k: Kit) => void }[] = [
  {
    // Commerce: the gold miner, in a broad-brimmed hat, holding his pan.
    name: 'Commerce', at: [1.9, 14.4],
    build: k => {
      const L = 0.2
      torso(k, { lean: L })
      const h = head(k, L)
      disc(k.dark, [h[0], h[1], h[2] + 0.2], 0.52, 0.06, 0.15)
      ellipsoid(k.dark, [h[0], h[1] - 0.02, h[2] + 0.3], [0.27, 0.29, 0.2], 10, 4)
      for (const s of [-1, 1]) arm(k, [[s * 0.6, L, 6.6], [s * 0.82, L + 0.3, 6.0], [s * 0.42, L + 0.75, 5.9]])
      disc(k.dark, [0, L + 0.92, 5.85], 0.44, 0.1, -0.55)
    },
  },
  {
    // The Future: a woman lifting a child high over her head, her robe
    // sweeping forward into the pendant.
    name: 'The Future', at: [15.4, -1.6],
    build: k => {
      const L = 0.28
      torso(k, { lean: L, broad: 0.56, pendant: 1.9, pendantW: 0.5 })
      const h = head(k, L, [0.23, 0.26, 0.31])
      ellipsoid(k.dark, [h[0], h[1] - 0.12, h[2] + 0.02], [0.25, 0.24, 0.3], 10, 5)
      arm(k, [[0.52, L, 6.65], [0.72, L + 0.15, 7.35], [0.42, L + 0.3, 8.05]], [0.18, 0.15, 0.13])
      arm(k, [[-0.52, L, 6.65], [-0.62, L + 0.25, 7.3], [-0.3, L + 0.32, 8.0]], [0.18, 0.15, 0.13])
      // The child, held aloft between her hands.
      ellipsoid(k.bronze, [0.06, L + 0.32, 8.4], [0.32, 0.24, 0.45], 10, 6)
      ellipsoid(k.bronze, [0.06, L + 0.34, 9.0], [0.21, 0.21, 0.22], 8, 5)
      // Robe billowing out in front of her hips.
      ellipsoid(k.dark, [0, L + 0.5, 5.15], [0.55, 0.4, 0.42], 10, 5)
    },
  },
  {
    // Industry: the mill worker, a child held against her left hip.
    name: 'Industry', at: [0, -13.3],
    build: k => {
      const L = 0.2
      torso(k, { lean: L, broad: 0.58 })
      const h = head(k, L, [0.24, 0.27, 0.32])
      ellipsoid(k.dark, [h[0], h[1] - 0.16, h[2] + 0.08], [0.22, 0.2, 0.22], 10, 5)
      // Her right hand to her breast, elbow out; her left arm round the child.
      arm(k, [[0.56, L, 6.6], [0.95, L + 0.15, 6.05], [0.18, L + 0.55, 6.25]])
      const c: V3 = [-0.62, L + 0.42, 5.6]
      ellipsoid(k.bronze, c, [0.26, 0.24, 0.4], 10, 6)
      ellipsoid(k.bronze, [c[0] - 0.03, c[1] + 0.02, c[2] + 0.55], [0.2, 0.2, 0.21], 8, 5)
      arm(k, [[-0.56, L, 6.6], [-0.92, L + 0.1, 5.95], [-0.45, L + 0.65, 5.45]])
    },
  },
  {
    // Transportation: the railroad worker, bare-chested, arms akimbo.
    name: 'Transportation', at: [-14.6, 0.7],
    build: k => {
      const L = 0.15
      torso(k, { lean: L, broad: 0.72 })
      head(k, L, [0.25, 0.28, 0.32])
      for (const s of [-1, 1]) arm(k, [[s * 0.7, L, 6.6], [s * 1.12, L - 0.05, 5.95], [s * 0.6, L + 0.1, 5.4]], [0.2, 0.17, 0.14])
    },
  },
]

// --------------------------------------------------------------- placement

const BEARING = 48 // degrees: model +y up North Tryon Street (OSM 50.3°), ±x along Trade (136.4°)

/** Copy `src` into `dst`, turned by `a` (map radians, counter-clockwise) and moved to (x, y). */
function place(src: Part, dst: Part, a: number, x: number, y: number) {
  const c = Math.cos(a), s = Math.sin(a)
  // Parts store glTF: X = map x, Y = up, Z = −map y.
  for (let i = 0; i < src.pos.length; i += 3) {
    const X = src.pos[i], Y = src.pos[i + 1], Z = src.pos[i + 2]
    dst.pos.push(X * c + Z * s + x, Y, -X * s + Z * c - y)
    const nX = src.nrm[i], nY = src.nrm[i + 1], nZ = src.nrm[i + 2]
    dst.nrm.push(nX * c + nZ * s, nY, -nX * s + nZ * c)
  }
  dst.uv.push(...src.uv)
}

const b = BEARING * Math.PI / 180
for (const st of statues) {
  // OSM east/north offset → model frame (rotate by −bearing about up).
  const [e, n] = st.at
  const x = e * Math.cos(b) - n * Math.sin(b), y = e * Math.sin(b) + n * Math.cos(b)
  const k: Kit = { granite: new Part(), base: new Part(), bronze: new Part(), dark: new Part() }
  pedestal(k)
  st.build(k)
  // Face the centre of the crossing: local +y → direction (−x, −y).
  const a = Math.atan2(x, -y)
  place(k.granite, granite, a, x, y)
  place(k.base, base, a, x, y)
  place(k.bronze, bronze, a, x, y)
  place(k.dark, dark, a, x, y)
}

// Colours: sRGB medians from the reference photos (/tmp/nyc-work/
// independence-square-statues/photos), sunlit where the photos allow.
const parts = [
  // Shaft granite, pink-grey; p1.jpg [1185, 470, 20, 30] (sunlit, Commerce).
  { part: granite, material: { name: 'granite', color: 0xa69a96 } },
  // The plinth, a shade darker; p2.jpg [530, 515, 25, 20] lifted to sun.
  { part: base, material: { name: 'granite-base', color: 0x958985 } },
  // Bronze figures; p2.jpg [518, 268, 10, 10] and [535, 270, 25, 20].
  { part: bronze, material: { name: 'bronze', color: 0x564d47 } },
  // Darker patina: hair, hats, pans and the pendants.
  { part: dark, material: { name: 'bronze-dark', color: 0x3b3531 } },
]
const glb = writeGlb('Independence Square statues', parts, {
  license: 'CC0-1.0',
  frame: 'Y up, -Z north, +X east, metres; origin at the centre of the Trade and Tryon crossing',
  bearing: BEARING, statues: statues.map(s => s.name),
})
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000 || glb.length > 250_000) throw new Error(`Landmark exceeds budget: ${triangles} triangles`)
const out = process.argv[2] ?? new URL('../../landmarks/models/independence-square-statues.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
