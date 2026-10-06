/**
 * Ascendus, Ed Carpenter's gateway sculpture at the Charlotte Douglas airport
 * turn-off on Billy Graham Parkway — procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/ascendus.ts [out.glb]
 *
 * Map frame: x east, y north, z up, metres. The origin is the sculpture's
 * foot (OSM node 12329988037), and the catalog places it at bearing 0.
 *
 * Steel, aluminium and laminated glass, 60 × 16 × 16 ft (18.3 × 4.9 × 4.9 m)
 * by the airport's own description. Two tall leaf-shaped trusses — pointed at
 * both ends, widest a little above the middle — rise from one foot and lean
 * east, the second turned a little from the first so they open into a narrow
 * V. The lean is kept within the published 16 ft square footprint. A few
 * bays of amber glass sit in their middles. Each leaf is two swept chords with
 * rungs and a zig-zag brace between them. The only photos available are
 * street-level views from the parkway to the south, so the turn between the
 * leaves and their lean are read from those and may be off by some degrees.
 */
import { Part, type V3, writeGlb } from './mesh'
import { finish } from './palette'

const STEEL = finish('steel-white', 0xdde0e2, 0.5)
const GLASS = finish('dichroic-glass', 0xd8bd6a, 0.3)

const steel = new Part()
const glass = new Part()

const H = 18.3
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const norm = (a: V3): V3 => mul(a, 1 / Math.hypot(...a))
const crossV = (a: V3, b: V3): V3 => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]

/** A rectangular bar from a to b, `w` across and `d` deep, `side` fixing its roll. */
function bar(path: V3[], w: number, d: number, side: V3) {
  const sections = path.map((p, i) => {
    const t = norm(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
    const n = norm(crossV(t, side)), m = crossV(n, t)
    return [add(add(p, mul(n, w / 2)), mul(m, d / 2)), add(add(p, mul(n, -w / 2)), mul(m, d / 2)),
      add(add(p, mul(n, -w / 2)), mul(m, -d / 2)), add(add(p, mul(n, w / 2)), mul(m, -d / 2))]
  })
  steel.sweep(sections)
}
const sub = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]

/**
 * One leaf. `azimuth` is the compass direction its width runs along, `lean`
 * how far its long axis tilts from vertical towards that direction (degrees),
 * `bow` how far its centre line curves out of plane.
 */
function leaf(azimuth: number, lean: number, length: number, width: number, bow: number, panes: number[]) {
  const az = (azimuth * Math.PI) / 180, ln = (lean * Math.PI) / 180
  const across: V3 = [Math.sin(az), Math.cos(az), 0] // horizontal, in the leaf's plane
  const axis: V3 = norm(add(mul(across, Math.sin(ln)), [0, 0, Math.cos(ln)]))
  const v = norm(crossV(crossV(axis, across), axis)) // across, square to the axis
  const out = norm(crossV(axis, v)) // the leaf's face normal
  const N = 16
  // Widest a little above the middle, pointed at both ends.
  const half = (s: number) => (width / 2) * Math.sin(Math.PI * s ** 0.85) ** 0.9
  const centre = (s: number): V3 => add(mul(axis, length * s), mul(out, bow * Math.sin(Math.PI * s)))
  const chord = (k: number) => Array.from({ length: N + 1 }, (_, i) => add(centre(i / N), mul(v, k * half(i / N))))
  const left = chord(-1), right = chord(1)
  bar(left, 0.32, 0.45, out)
  bar(right, 0.32, 0.45, out)
  // Rungs and a zig-zag brace between the chords.
  for (let i = 2; i <= N - 2; i += 2) bar([left[i], right[i]], 0.16, 0.3, out)
  for (let i = 2; i < N - 2; i += 2) bar(i % 4 ? [left[i], right[i + 2]] : [right[i], left[i + 2]], 0.12, 0.22, out)
  // Amber glass in the middle bays, set in the leaf's plane, both faces.
  for (const i of panes) {
    const q = [left[i], right[i], right[i + 2], left[i + 2]].map((p) => add(p, mul(out, 0.02)))
    glass.quad(q[0], q[1], q[2], q[3])
    glass.quad(q[3], q[2], q[1], q[0])
  }
}

// The east leaf leans further; the west one stands straighter and is turned
// about 25° from it, so the pair read as two leaves from the parkway.
leaf(85, 13, H / Math.cos((13 * Math.PI) / 180), 2.7, 0.4, [6, 8])
leaf(110, 6, H / Math.cos((6 * Math.PI) / 180), 2.4, -0.4, [8])

// The shared foot: a low steel shoe the two leaves spring from.
{
  const k = 0.9, z = 0.6
  const pts: V3[] = [[-k, -k, 0], [k, -k, 0], [k, k, 0], [-k, k, 0]]
  const top = pts.map(([x, y]): V3 => [x * 0.7, y * 0.7, z])
  steel.loft([pts, top])
  steel.cap(top, true)
}

const parts = [{ part: steel, material: STEEL }, { part: glass, material: { ...GLASS, doubleSided: false } }]
const tris = parts.reduce((n, { part }) => n + part.triangles, 0)
if (tris > 3000) throw new Error(`over budget: ${tris} triangles`)
const glb = writeGlb('Ascendus', parts, {
  title: 'Ascendus',
  artist: 'Ed Carpenter',
  license: 'CC0-1.0',
  source: 'scripts/landmarks/ascendus.ts',
})
const outPath = process.argv[2] ?? new URL('../../landmarks/models/ascendus.glb', import.meta.url).pathname
await Bun.write(outPath, glb)
console.log(`${outPath}: ${tris} triangles, ${glb.length} bytes`)
