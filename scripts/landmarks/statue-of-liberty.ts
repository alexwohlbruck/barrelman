/**
 * Lady Liberty, authored in metres: east +x, north +y, up +z.
 *
 *   bun scripts/landmarks/statue-of-liberty.ts [out.glb]
 *
 * The heel is at 36.9 m and the flame tip at 83 m. The catalog raises the
 * pedestal onto Fort Wood; the fort remains map geometry, not part of this
 * asset. She faces south, with her right (west) arm holding the torch.
 * All geometry is procedural and CC0-1.0; no source mesh or textures.
 */
import { Part, addGltfTriangles, cross, sub, len, square, writeGlb, type V3 } from './mesh'

const pedestal = new Part()
const stoneTrim = new Part()
const loggiaRecess = new Part()
const copper = new Part()
const copperFolds = new Part()
const torch = new Part()
const flame = new Part()
const TAU = Math.PI * 2
const unit = (v: V3): V3 => v.map(n => n / (len(v) || 1)) as V3
const plus = (a: V3, b: V3): V3 => a.map((n, i) => n + b[i]) as V3
const scale = (a: V3, s: number): V3 => a.map(n => n * s) as V3

// Smooth each sculpted piece separately, preserving edges at joins and caps.
function smooth(target: Part, build: (p: Part) => void, crease = 65) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos),
    Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

function solid(p: Part, rings: V3[][]) {
  p.loft(rings)
  p.cap(rings[0], false)
  p.cap(rings[rings.length - 1], true)
}

function oval(c: V3, rx: number, ry: number, n = 20): V3[] {
  return Array.from({ length: n }, (_, i) => {
    const a = i / n * TAU
    return [c[0] + rx * Math.cos(a), c[1] + ry * Math.sin(a), c[2]]
  })
}

function ellipsoid(p: Part, c: V3, radii: V3, n = 16, rows = 8) {
  smooth(p, mesh => {
    const rings = Array.from({ length: rows + 1 }, (_, j) => {
      const a = -Math.PI / 2 + j / rows * Math.PI
      return oval([c[0], c[1], c[2] + radii[2] * Math.sin(a)],
        radii[0] * (j === 0 || j === rows ? 0 : Math.cos(a)),
        radii[1] * (j === 0 || j === rows ? 0 : Math.cos(a)), n)
    })
    mesh.loft(rings)
  }, 80)
}

/** Rounded, tapered limbs, cloth ridges and crown rays; ring axis follows path. */
function tube(p: Part, path: V3[], widths: number[], depths = widths, sides = 12) {
  smooth(p, mesh => {
    const rings = path.map((c, i) => {
      const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
      const u = unit(cross([0, 1, 0], axis))
      const v = cross(axis, u)
      return Array.from({ length: sides }, (_, j) => {
        const a = j / sides * TAU
        return plus(c, plus(scale(u, widths[i] * Math.cos(a)), scale(v, depths[i] * Math.sin(a))))
      })
    })
    solid(mesh, rings)
  })
}

function box(p: Part, c: V3, half: V3, rotate = 0) {
  const rings = [-1, 1].map(s => [
    [-half[0], -half[1]], [half[0], -half[1]], [half[0], half[1]], [-half[0], half[1]],
  ].map(([x, y]): V3 => [c[0] + x * Math.cos(rotate) - y * Math.sin(rotate),
    c[1] + x * Math.sin(rotate) + y * Math.cos(rotate), c[2] + s * half[2]]))
  solid(p, rings)
}

// Five OSM envelopes, with shallow relief carved into their outer surfaces.
pedestal.slab(0, 4, 20)
pedestal.slab(4, 8, 12.75)
for (let row = 0; row < 4; row++) {
  const z = 8 + row * 2
  pedestal.slab(z, z + 2, 9.02)
  // Alternating large ashlar courses keep the base legible at map scale.
  if (row % 2) {
    pedestal.slab(z, z + 1.85, 9.2)
    continue
  }
  for (let face = 0; face < 4; face++) {
    const a = face * Math.PI / 2
    const edges = [-9.2, row === 0 ? -3.05 : 3.05, 9.2]
    for (let j = 0; j < edges.length - 1; j++) {
      const along = (edges[j] + edges[j + 1]) / 2
      box(pedestal, [along * Math.cos(a) - 9.1 * Math.sin(a),
        along * Math.sin(a) + 9.1 * Math.cos(a), z + 0.94],
      [(edges[j + 1] - edges[j]) / 2 - 0.055, 0.1, 0.9], a)
    }
  }
}
solid(pedestal, [square(7.75, 13), square(6.95, 29.5)])
stoneTrim.slab(16, 16.65, 7.85)
stoneTrim.slab(28.6, 29.5, 7.45)
// Inset core and real open recesses between the loggia piers, on all faces.
// Recess colour records the deep bays in the photo; it is not window glass.
loggiaRecess.slab(29.5, 34, 6.05)
for (let face = 0; face < 4; face++) {
  const a = face * Math.PI / 2
  for (let i = -2; i <= 2; i++) {
    const along = i * 3.2
    box(stoneTrim, [along * Math.cos(a) - 6.8 * Math.sin(a),
      along * Math.sin(a) + 6.8 * Math.cos(a), 31.75], [0.42, 0.55, 2.25], a)
  }
}
stoneTrim.slab(34, 35, 7.75)
stoneTrim.slab(35, 35.65, 8.45)
stoneTrim.slab(35.65, 36.2, 8.05)
stoneTrim.slab(36.2, 36.9, 8.45, 7.5)
stoneTrim.slab(36.2, 36.9, 5.6)

// Sandals and the forward left foot: distinguish the stance below the hem.
ellipsoid(copper, [-1.65, 0.1, 37.65], [1.35, 2.05, 0.75])
ellipsoid(copper, [2.05, -2.35, 37.65], [1.25, 2.4, 0.75])

// Broad cloth folds are part of the silhouette, not separate thin decoration.
// z, centre x/y, half-width/depth, fold amplitude, fold phase.
const robe = [
  [37.5, 0, 0.4, 4.6, 3.05, 0.32, 0],
  [40, 0, 0.5, 4.8, 3.25, 0.48, 0.05],
  [45, -0.2, 0.5, 4.25, 2.9, 0.48, 0.12],
  [50, -0.35, 0.2, 3.65, 2.55, 0.42, 0.2],
  [55, 0, 0, 3.2, 2.15, 0.25, 0.35],
  [59, 0, 0, 3.7, 2.15, 0.22, 0.5],
  [62, 0, 0.15, 3.8, 1.95, 0.18, 0.6],
  [63.5, 0, 0.2, 2.7, 1.65, 0.1, 0.7],
  [64.5, 0, 0.2, 1.15, 1.05, 0.04, 0.8],
]
const robeSurface = new Part()
smooth(robeSurface, p => solid(p, robe.map(([z, x, y, rx, ry, fold, phase]) =>
  Array.from({ length: 40 }, (_, i): V3 => {
    const a = i / 40 * TAU
    const f = fold * (Math.cos(a * 9 + phase) + 0.25 * Math.cos(a * 5 - phase))
    return [x + (rx + f) * Math.cos(a), y + (ry + f) * Math.sin(a), z]
  }))), 72)

// Keep the original mesh and its smooth normals. Colour whole loft quads,
// following the recessed folds and the sheltered east flank in photo 01;
// this preserves broad, continuous regions instead of isolated dark triangles.
const robeLevels = robe.length - 1
const robeSideTriangles = 40 * robeLevels * 2
for (let t = 0; t < robeSurface.triangles; t++) {
  const column = Math.floor(t / (robeLevels * 2))
  const level = Math.floor(t / 2) % robeLevels
  const a = (column + 0.5) / 40 * TAU
  const phase = (robe[level][6] + robe[level + 1][6]) / 2
  // Only the deep lower folds receive the dark patina; shallow chest
  // ripples keep the main copper colour rather than forming painted stripes.
  const foldDepth = (robe[level][5] + robe[level + 1][5]) / 2
  const valley = foldDepth >= 0.3 && Math.cos(a * 9 + phase) < -0.5
  const shelteredFlank = Math.cos(a) > 0.65 && Math.sin(a) > -0.55
  const target = t < robeSideTriangles && (valley || shelteredFlank) ? copperFolds : copper
  target.pos.push(...robeSurface.pos.slice(t * 9, t * 9 + 9))
  target.nrm.push(...robeSurface.nrm.slice(t * 9, t * 9 + 9))
  target.uv.push(...robeSurface.uv.slice(t * 6, t * 6 + 6))
}

// The palla crosses the chest and gathers at the left elbow. Flattened
// ridges share the robe's colour and read as a few heavy folds of cloth.
for (let i = 0; i < 2; i++) {
  tube(copper, [[-3.05, -0.8, 62 - i * 1.9], [-1.7, -2.15, 60 - i * 2],
    [0.5, -2.55, 58 - i * 1.9], [3.65, -1.2, 57.7 - i * 1.6]],
  [0.22, 0.55, 0.55, 0.3], [0.12, 0.18, 0.18, 0.12], 10)
}

// A broad descending fold breaks the long, otherwise tubular skirt and
// carries the mantle from the left hip across the front of the knees.
tube(copper, [[3.1, -1.5, 57.5], [3.35, -1.65, 55.5], [2.6, -2.65, 51.5], [0.6, -3.0, 46.5], [-1.6, -2.7, 41]],
  [0.08, 0.45, 0.85, 0.8, 0.16], [0.06, 0.14, 0.24, 0.22, 0.1], 12)

// Raised right arm: generous sleeve at the shoulder, bare forearm above.
tube(copper, [[-2.7, 0.1, 61.2], [-3.65, 0, 62.9], [-4.1, -0.1, 64.6], [-4.7, -0.3, 66.4]],
  [1.85, 1.7, 1.45, 1.05], [1.8, 1.6, 1.3, 1.05])
tube(copper, [[-4.65, -0.4, 65.8], [-5.15, -1.25, 68.5], [-5.75, -2.4, 71.8], [-6.1, -3.2, 74.5]],
  [1.02, 0.93, 0.76, 0.64], [0.95, 0.85, 0.7, 0.62])
ellipsoid(copper, [-6.1, -3.2, 74.5], [0.85, 0.85, 1.1])

// Left upper arm and bent forearm cradle the tablet against the torso.
tube(copper, [[3.05, 0, 61.3], [4.2, 0, 58.5], [4.65, -0.75, 55.5]],
  [1.4, 1.55, 1.1], [1.4, 1.45, 1.0])
tube(copper, [[4.65, -0.75, 55.5], [4.1, -2.4, 55.8], [3.2, -3.1, 57.2]],
  [0.95, 0.85, 0.65])
// Tablet tilts toward the body at its top and a little out to the east.
{
  const p = new Part()
  box(p, [0, 0, 0], [1.9, 0.44, 3.6])
  const points = new Float32Array(p.pos)
  for (let i = 0; i < points.length; i += 3) {
    const x = points[i], y = -points[i + 2], z = points[i + 1]
    points[i] = 3.35 + x + z * 0.13
    points[i + 1] = 59 + z
    points[i + 2] = -(-2.75 + y + z * 0.2)
  }
  addGltfTriangles(copper, points, Uint32Array.from({ length: points.length / 3 }, (_, i) => i))
}
ellipsoid(copper, [3.1, -3.35, 56.9], [0.85, 0.55, 0.65], 12, 6)

// Neck, hair behind the face, then a long, calm classical face.
ellipsoid(copper, [0, 0.2, 65], [1.1, 1.0, 1.35])
ellipsoid(copper, [0, 0.5, 68.25], [1.95, 1.6, 2.55])
smooth(copper, p => solid(p, [
  oval([0, -0.55, 65.5], 0.8, 0.7, 20),
  oval([0, -0.6, 66.2], 1.25, 1.05, 20),
  oval([0, -0.5, 67.5], 1.6, 1.2, 20),
  oval([0, -0.35, 69], 1.55, 1.2, 20),
  oval([0, 0, 70.15], 1.3, 1.1, 20),
]))
for (const side of [-1, 1]) {
  tube(copper, [[side * 1.45, 0.8, 69.7], [side * 1.6, 1.1, 68.2],
    [side * 1.4, 1.2, 66.6], [side * 0.8, 1.05, 65.7]],
  [0.3, 0.36, 0.36, 0.2], [0.24, 0.25, 0.25, 0.2], 8)
}
// Nose and brow are relief in the same copper, never painted facial features.
tube(copper, [[0, -1.6, 68.6], [0, -2.0, 67.5], [0, -1.8, 67.25]],
  [0.23, 0.32, 0.3], [0.16, 0.28, 0.18], 8)
for (const side of [-1, 1]) {
  tube(copper, [[side * 0.3, -1.57, 68.5], [side * 0.85, -1.57, 68.6], [side * 1.3, -1.22, 68.4]],
    [0.12, 0.15, 0.08], [0.12, 0.16, 0.08], 8)
}
tube(copper, [[-0.5, -1.51, 66.65], [0, -1.68, 66.6], [0.5, -1.51, 66.65]],
  [0.06, 0.1, 0.06], [0.06, 0.1, 0.06], 8)

// Diadem with seven separate rays; their widened roots survive map distances.
smooth(copper, p => solid(p, [oval([0, 0.2, 69.6], 1.92, 1.65, 28),
  oval([0, 0.2, 70.35], 2.03, 1.75, 28), oval([0, 0.2, 70.8], 1.85, 1.55, 28)]))
for (let i = 0; i < 7; i++) {
  const a = (15 + i * 25) * Math.PI / 180
  const root: V3 = [1.75 * Math.cos(a), 0.2 - 1.35 * Math.sin(a), 69.8 + 0.8 * Math.sin(a)]
  const tip: V3 = [5.05 * Math.cos(a), 0.2 - 3.6 * Math.sin(a), 69.8 + 5.0 * Math.sin(a)]
  tube(copper, [root, plus(root, scale(sub(tip, root), 0.25)), tip], [0.43, 0.34, 0.015], [0.35, 0.25, 0.015], 8)
}

// Torch handle, flared bowl and gold flame. The tip is exactly z = 83.
smooth(torch, p => solid(p, [
  oval([-6.1, -3.2, 73.3], 0.4, 0.4, 16),
  oval([-6.1, -3.2, 76.7], 0.55, 0.55, 16),
  oval([-6.1, -3.2, 77.5], 1.35, 1.35, 16),
  oval([-6.1, -3.2, 78.1], 1.5, 1.5, 16),
  oval([-6.1, -3.2, 78.5], 1.4, 1.4, 16),
]))
smooth(flame, p => solid(p, [
  oval([-6.1, -3.2, 78.45], 0.85, 0.75, 16),
  oval([-6.3, -3.2, 79.3], 1.1, 0.9, 16),
  oval([-6.15, -3.2, 80.3], 0.85, 0.7, 16),
  oval([-5.8, -3.2, 81.4], 0.55, 0.45, 16),
  oval([-5.9, -3.2, 82.3], 0.27, 0.25, 16),
  oval([-6.25, -3.2, 83], 0, 0, 16),
]), 80)

// Reference photos: 01.jpg (1280 × 960) and 02.jpg (1280 × 3171).
// Colours are sRGB patch medians (copper excludes pale highlights);
// the GLB writer converts them to linear baseColorFactor. Do not compensate
// for the map palette. Dark verdigris follows the robe valleys and sheltered
// flank; the loggia also has a broad dark region for its deep openings.
// Sample rectangles are [left, top, right, bottom], with exclusive upper bounds.
const parts = [
  // Granite shaft, rustication, plinth and steps; photo 02 [638, 2736, 740, 2783].
  { part: pedestal, material: { name: 'pedestal', color: 0xada89e } },
  // Lighter granite piers, cornices, parapet and horizontal bands; photo 02 [348, 2410, 375, 2489].
  { part: stoneTrim, material: { name: 'stone-trim', color: 0xb9aea6 } },
  // Deep inset walls behind the loggia piers; photo 02 [565, 2385, 611, 2458].
  { part: loggiaRecess, material: { name: 'loggia-recess', color: 0x302e29 } },
  // Coloured sunlit robe: photo 01 [608, 265, 665, 387], median of the
  // brightest quartile with HSV saturation >= 0.4 (excluding pale glints).
  // Tablet [685, 251, 707, 294] gives a corroborating #50a59c.
  { part: copper, material: { name: 'copper', color: 0x59a69f } },
  // Deep green folds: photo 01 [643, 397, 651, 433], unfiltered median.
  { part: copperFolds, material: { name: 'copper-folds', color: 0x076e61 } },
  // Weathered copper torch handle and cup; photo 02 [345, 192, 356, 197].
  { part: torch, material: { name: 'torch', color: 0x577477 } },
  // Sunlit gold leaf on the flame; photo 02 [364, 137, 368, 140].
  { part: flame, material: { name: 'flame', color: 0xf2dc84 } },
]
const glb = writeGlb('Statue of Liberty', parts, {
  license: 'CC0-1.0',
  frame: 'Y up, -Z north, +X east, metres; origin at pedestal base, placed 10 m above ground',
  facing: 'south (-y map frame)', heel: 36.9, flameTip: 83, crownRays: 7,
})
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 8000 || glb.length > 400_000) throw new Error('Landmark exceeds geometry budget')
const out = process.argv[2] ?? new URL('../../landmarks/models/statue-of-liberty.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
