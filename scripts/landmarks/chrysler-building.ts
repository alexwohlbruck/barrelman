/**
 * Chrysler Building — original procedural geometry, CC0-1.0.
 * Run: bun scripts/landmarks/chrysler-building.ts
 * Map frame: x = v, y = u, z up, metres. Placement: bearing 29°, elevation 0.
 * The supplied OSM envelopes determine the setbacks; small footprint notches
 * are stylised because the measurements supply bounds rather than vertices.
 */
import { Part, addGltfTriangles, writeGlb, type V3 } from './mesh'

const brick = new Part(), stone = new Part(), glass = new Part(), windows = new Part()
const steel = new Part(), seams = new Part(), highlights = new Part()
const podium = new Part(), darkBrick = new Part(), steelFolds = new Part()
const crownSteel = new Part()
const roof = new Part()
const CX = -8.4, CY = 7
type XY = [number, number]
const rect = (x0: number, y0: number, x1: number, y1: number): XY[] =>
  [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
const ring = (xy: XY[], z: number): V3[] => xy.map(([x, y]) => [x, y, z])

function solid(p: Part, xy: XY[], lo: number, hi: number) {
  p.loft([ring(xy, lo), ring(xy, hi)])
  // All footprints are star-shaped about their centroid (including the
  // recessed tower corners); a centre fan also caps their concave notches.
  const c = xy.reduce((s, v) => [s[0] + v[0] / xy.length, s[1] + v[1] / xy.length], [0, 0])
  for (let i = 0; i < xy.length; i++) {
    const a = xy[i], b = xy[(i + 1) % xy.length]
    p.tri([c[0], c[1], hi], [a[0], a[1], hi], [b[0], b[1], hi])
    p.tri([c[0], c[1], lo], [b[0], b[1], lo], [a[0], a[1], lo])
  }
}

/** Flat, outward-wound facade patches, just clear of the masonry. */
function facade(xy: XY[], lo: number, hi: number, spacing = 6.5, width = 2.3) {
  for (let e = 0; e < xy.length; e++) {
    const a = xy[e], b = xy[(e + 1) % xy.length]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1])
    const dx = (b[0] - a[0]) / length, dy = (b[1] - a[1]) / length
    const point = (t: number, z: number): V3 => [a[0] + dx * t + dy * .035, a[1] + dy * t - dx * .035, z]
    const count = Math.floor((length - 1.2) / spacing)
    for (let i = 0; i < count; i++) {
      const mid = length * (i + 1) / (count + 1)
      const l = mid - width / 2, r = mid + width / 2
      // Group adjacent real windows into broad, quiet recesses. Individual
      // floors and mullions alias badly at phone-scale map distances.
      windows.quad(point(l, lo), point(r, lo), point(r, hi), point(l, hi))
    }
  }
}

function block(x0: number, y0: number, x1: number, y1: number, lo: number, hi: number) {
  const xy = rect(x0, y0, x1, y1)
  solid(podium, xy, lo, hi)
  solid(stone, xy, hi - .65, hi)
  roof.cap(ring(rect(x0 + .5, y0 + .5, x1 - .5, y1 - .5), hi + .012), true)
  facade(xy, lo + (lo === 0 ? 5 : .9), hi - 1.4)
  brickBands(xy, [hi - 4.5], 1.8, false)
}

/** Broad summaries of the dark brick courses, preserving the pale central
 * piers visible in the reference. They sit behind the existing glazing. */
function brickBands(xy: XY[], heights: number[], height: number, split: boolean) {
  for (let i = 0; i < xy.length; i++) {
    const a = xy[i], b = xy[(i + 1) % xy.length]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1])
    const dx = (b[0] - a[0]) / length, dy = (b[1] - a[1]) / length
    const p = (t: number, z: number): V3 => [a[0] + dx * t + dy * .018, a[1] + dy * t - dx * .018, z]
    const spans = split ? [[.03, .29], [.71, .97]] : [[.025, .975]]
    for (const z of heights) for (const [lo, hi] of spans) {
      darkBrick.quad(p(lo * length, z), p(hi * length, z), p(hi * length, z + height), p(lo * length, z + height))
    }
  }
}

// Three interlocking podium wings preserve the asymmetric street envelope.
block(-40.5, -6.6, 20.5, 19, 0, 15)
block(-40.5, 18.8, 23.3, 37.5, 0, 50)
block(-40.5, -25.3, 12.6, -6.6, 0, 50)
block(-34.8, -19, 12.6, -6.6, 50, 70)
block(-34.8, 18.8, 17.4, 31.7, 50, 70)
block(-32, -16.5, 14.1, -6.4, 70, 80)
block(-32, 18.8, 14.7, 29, 70, 80)
block(-22.7, -13.4, 5.8, -10.7, 80, 90)
block(-22.6, 24.2, 6, 26, 80, 90)

const shaft = rect(-22.7, -10.7, 6, 24.2)
solid(brick, shaft, 0, 185)
facade(shaft, 16, 184, 6, 2.1)
brickBands(shaft, [105, 121, 137, 153, 169, 180], 2.2, true)
solid(stone, shaft, 183.8, 185.5)

// Four recessed corners above the 185 m shoulder; the central bays continue
// through the 199 m eagle terrace, as in the individual OSM corner parts.
const upper: XY[] = [[-17.8, -10.7], [1, -10.7], [1, -2.9], [6, -2.9],
  [6, 16.4], [.9, 16.4], [.9, 24.2], [-18.1, 24.2],
  [-18.1, 16.6], [-22.7, 16.6], [-22.7, -2.5], [-17.8, -2.5]]
solid(brick, upper, 185, 199)
facade(upper, 185.8, 197.9, 6, 2.1)
solid(steel, upper, 198.1, 200)

// Storefronts and restrained stone entrance frames; all sit within the OSM
// outer bounds, so detail never expands the building onto the pavement.
for (const y of [-25.3, 37.5]) {
  const inward = y < 0 ? 1 : -1
  solid(stone, rect(-15.8, y + Math.min(0, inward * .3), -1, y + Math.max(0, inward * .3)), 0, 9)
}

function smooth(p: Part, build: (s: Part) => void, crease = 42) {
  const s = new Part(); build(s)
  addGltfTriangles(p, new Float32Array(s.pos),
    Uint32Array.from({ length: s.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

/** Crown face coordinate: t left/right, d outward, z up, rotated CCW. */
function facePoint(side: number, t: number, d: number, z: number): V3 {
  const a = side * Math.PI / 2
  return [CX + t * Math.cos(a) + d * Math.sin(a), CY + t * Math.sin(a) - d * Math.cos(a), z]
}

// Seven intersecting barrel-vault terraces, not cones. Each has four
// arched faces and closed groined roofs. Larger lower terraces remain
// visible around the smaller ones. All four faces carry the sunburst glazing.
const tiers = [
  // half-width, spring, arch rise, underside, triangular lights per face
  [14.35, 200, 29, 199, 9],
  [11.1, 218, 24, 212, 7],
  [8.65, 233, 16, 226, 7],
  [6.95, 243, 13, 235, 5],
  [4.95, 253, 9, 246, 5],
  [4, 259, 8, 253, 3],
  [2.95, 265, 7, 260, 3],
]
for (const [half, spring, rise, bottom, lights] of tiers) {
  const segments = 16
  // A modest backward lean softens the side silhouette while keeping each
  // sunburst in front of the smaller terrace behind it.
  const depth = half
  for (let side = 0; side < 4; side++) {
    const p = (x: number, z: number, d = depth): V3 => {
      const taper = 1 - .18 * Math.max(0, Math.min(1, (z - spring) / rise))
      return facePoint(side, x * taper, d * taper, z)
    }
    smooth(steelFolds, roof => {
      for (let j = 0; j < segments; j++) {
        const a = Math.PI * j / segments, b = Math.PI * (j + 1) / segments
        const x0 = -half * Math.cos(a), x1 = -half * Math.cos(b)
        const z0 = spring + rise * Math.sin(a), z1 = spring + rise * Math.sin(b)
        crownSteel.quad(p(x0, bottom), p(x1, bottom), p(x1, z1), p(x0, z0))
        roof.quad(p(x0, z0), p(x1, z1), p(x1, z1, Math.abs(x1)), p(x0, z0, Math.abs(x0)))
        // Rolled arch rims and a narrow shadow line make the scallops legible.
        seams.quad(p(x0 * .976, spring + rise * .976 * Math.sin(a), depth + .045),
          p(x1 * .976, spring + rise * .976 * Math.sin(b), depth + .045),
          p(x1 * .992, spring + rise * .992 * Math.sin(b), depth + .045),
          p(x0 * .992, spring + rise * .992 * Math.sin(a), depth + .045))
      }
    })
    // The lowest sunburst frames pale masonry, not a solid silver plate.
    // This is a colour-region overlay on the existing crown face.
    if (half === tiers[0][0]) {
      for (let j = 0; j < segments; j++) {
        const a = Math.PI * j / segments, b = Math.PI * (j + 1) / segments
        const x0 = -half * .6 * Math.cos(a), x1 = -half * .6 * Math.cos(b)
        brick.quad(p(x0, bottom, depth + .12), p(x1, bottom, depth + .12),
          p(x1, spring + rise * .6 * Math.sin(b), depth + .12),
          p(x0, spring + rise * .6 * Math.sin(a), depth + .12))
      }
      for (const x of [-4.8, 0, 4.8]) {
        const top = spring + rise * .53 * Math.sqrt(1 - (x / (half * .6)) ** 2)
        windows.quad(p(x - .65, bottom + .6, depth + .16), p(x + .65, bottom + .6, depth + .16),
          p(x + .65, top, depth + .16), p(x - .65, top, depth + .16))
      }
    }
    // One broad rim per arch survives map scale. Fine radial ribs and inner
    // rings only produce aliasing; the triangular lights supply the sunburst.
    for (let j = 0; j < segments; j++) {
      const a = Math.PI * j / segments, b = Math.PI * (j + 1) / segments
      const arc = (r: number, angle: number): V3 => p(-half * r * Math.cos(angle), spring + rise * r * Math.sin(angle), depth + .085)
      const r = .975
      highlights.quad(arc(r - .035, a), arc(r - .035, b), arc(r, b), arc(r, a))
    }
    for (let j = 0; j < lights; j++) {
      const angle = .22 + (Math.PI - .44) * (j + .5) / lights
      const spread = Math.min(.10, 1 / lights)
      const polar = (r: number, a: number): V3 => p(-half * r * Math.cos(a), spring + rise * r * Math.sin(a), depth + .07)
      // The triangles point outward along a fan, rather than straight up.
      glass.tri(polar(.67, angle - spread), polar(.67, angle + spread), polar(.94, angle))
    }
  }
}

// A stainless needle, 1.5 m across at the 272 m crown junction: roughly a
// quarter of the upper terrace's 5.9 m width, tapering to the 319 m tip.
// A pixel-width minimum must be handled by the renderer, not a fat cylinder.
smooth(crownSteel, p => {
  const profile = [[270, 1.25], [272, .75], [282, .55], [300, .27], [313, .10]]
  const rings = profile.map(([z, r]) => Array.from({ length: 12 }, (_, i): V3 =>
    [CX + r * Math.cos(i * Math.PI / 6), CY + r * Math.sin(i * Math.PI / 6), z]))
  p.loft(rings)
  const last = rings.at(-1)!
  for (let i = 0; i < 12; i++) p.tri(last[i], last[(i + 1) % 12], [CX, CY, 319])
})

// Eight projecting eagle heads (two at each stepped corner). Beaks are
// hooked, with a heavy brow and a swept-back neck, rather than generic spikes.
function eagle(x: number, y: number, angle: number) {
  const place = (s: number, w: number, z: number): V3 =>
    [x + Math.cos(angle) * s - Math.sin(angle) * w, y + Math.sin(angle) * s + Math.cos(angle) * w, z]
  const sections = [[-1.3, 1.15, 198.3, 201], [1.4, .85, 199.1, 202],
    [2.6, .62, 199.3, 201.6], [3.7, .08, 198.9, 200.5]]
  const rings = sections.map(([s, w, lo, hi]) =>
    [place(s, -w, lo), place(s, w, lo), place(s, w, hi), place(s, -w, hi)])
  steel.loft(rings)
  steel.cap(rings[0], false); steel.cap(rings.at(-1)!, true)
  for (const sign of [-1, 1]) {
    const eye = [place(2.05, sign * .7, 201.2), place(2.65, sign * .6, 201.12), place(2.35, sign * .66, 200.88)]
    if (sign > 0) eye.reverse()
    glass.tri(eye[0], eye[1], eye[2])
  }
}
for (const sx of [-1, 1]) for (const sy of [-1, 1]) {
  eagle(CX + sx * 14.3, CY + sy * 10.1, sx > 0 ? 0 : Math.PI)
  eagle(CX + sx * 10.1, CY + sy * 17.2, sy > 0 ? Math.PI / 2 : -Math.PI / 2)
}

// The 31st-floor radiator caps: shallow round discs and swept metal wings.
for (const x of [-23.4, 6.65]) for (const y of [-14, 26.8]) {
  solid(brick, rect(x - 2.7, y - 2.5, x + 2.7, y + 2.5), 80, 89.8)
  solid(stone, rect(x - 2.9, y - 2.7, x + 2.9, y + 2.7), 87.9, 89.4)
  const n = 12
  const circle = (z: number, r: number): V3[] => Array.from({ length: n }, (_, i) =>
    [x + r * Math.cos(i * Math.PI * 2 / n), y + r * Math.sin(i * Math.PI * 2 / n), z])
  steel.loft([circle(89, 2), circle(90.6, 2.6), circle(91, 1.6)])
  steel.cap(circle(91, 1.6), true)
  solid(steel, rect(x - 3.05, y - 1.15, x + 3.05, y + 1.15), 89.5, 90.2)
}

// Parchment displays baseColorFactor directly as sRGB. Invert the shared
// writer's linearisation locally so these swatches describe map appearance.
function mapColor(hex: number): number {
  return [16, 8, 0].reduce((value, shift) =>
    value | Math.round(255 * Math.pow(((hex >> shift) & 255) / 255, 1 / 2.2)) << shift, 0)
}

// Shared visible materials stay one primitive each even when their geometry
// was authored separately (rolled steel rims and flat masonry roof caps).
for (const [target, source] of [[steel, highlights], [podium, roof]]) {
  target.pos.push(...source.pos)
  target.nrm.push(...source.nrm)
  target.uv.push(...source.uv)
}

const parts = [
  // Median sRGB samples: 04.jpg supplies masonry, glazing and steel folds;
  // 01.jpg supplies sunlit silver without its clipped specular highlights.
  // The podium uses the lower sunlit masonry in 04: the ground-level podium
  // itself is obscured in these references. Roofs share that masonry grey.
  { part: brick, material: { name: 'shaft-brick', color: mapColor(0xc6bfb4) } },
  { part: podium, material: { name: 'podium-masonry', color: mapColor(0xcecdc6) } },
  { part: stone, material: { name: 'stone-trim', color: mapColor(0xd6d0c8) } },
  { part: windows, material: { name: 'window-glazing', color: mapColor(0x343434) } },
  { part: darkBrick, material: { name: 'dark-brick', color: mapColor(0x323230) } },
  { part: glass, material: { name: 'crown-glass', color: mapColor(0x454746), roughness: .4, doubleSided: true } },
  { part: crownSteel, material: { name: 'brushed-steel', color: mapColor(0xbdbdbe), roughness: .4, doubleSided: true } },
  { part: steel, material: { name: 'polished-steel', color: mapColor(0xedeeee), roughness: .32, doubleSided: true } },
  { part: steelFolds, material: { name: 'steel-folds', color: mapColor(0x9fa1a3), roughness: .32, doubleSided: true } },
  { part: seams, material: { name: 'steel-seams', color: mapColor(0x6e6b62) } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 12_000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Chrysler Building', parts, {
  license: 'CC0-1.0', anchor: [40.75151, -73.9752851], bearing: 29, elevation: 0,
  frame: 'Y up, -Z north, +X east, metres; origin at the anchor on the ground',
  mapBounds: { x: [-40.5, 23.3], y: [-25.3, 37.5], z: [0, 319] },
  crownArches: 7, source: 'Procedural geometry from supplied OSM measurements; stylised Art Deco details',
})
const output = new URL('../../landmarks/models/chrysler-building.glb', import.meta.url).pathname
await Bun.write(output, glb)
console.log(`${output}: ${triangles} triangles, ${glb.length} bytes`)
