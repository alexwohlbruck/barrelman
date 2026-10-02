/**
 * The Statue of Liberty, from "Lady Liberty" by Anna M (Poly Pizza, CC-BY 3.0).
 *
 *   bun scripts/landmarks/statue-of-liberty.ts [out.glb]
 *
 * Downloads the source model (pinned by hash) and normalises it into the frame
 * contract in `landmarks/README.md`. The source is a nicely stylised 2.5k
 * triangle statue, but it arrives in arbitrary units, centred on its own
 * middle, with no normals and a pedestal drawn at half its real height. So:
 *
 *   - the pedestal is stretched to the real one, 10 m (the top of Fort Wood,
 *     which OSM maps and the map keeps drawing) to 46.9 m, and the statue
 *     above it is scaled to her real 46 m, base to torch — 93 m overall
 *     above the ground, with the placement's `elevation` lifting her onto
 *     the fort;
 *   - the origin moves to the centre of the pedestal's base;
 *   - the materials are recoloured to the map's palette: weathered copper,
 *     a gold flame, and a pedestal in the granite colour OSM gives the fort;
 *   - normals are recomputed flat, and every material is double-sided,
 *     because a model from elsewhere makes no promise about its winding.
 *
 * She already faces +Z, the model's south; the catalog's `bearing` turns her
 * to face the Narrows.
 */
import { Part, readGlb, addGltfTriangles, writeGlb } from './mesh'

const SOURCE = 'https://static.poly.pizza/934e3216-3a6e-4e3d-b8fa-b1dd041bbb11.glb'
/** A changed upstream file is a different model; refuse it rather than ship it unseen. */
const SOURCE_SHA256 = '7c15d584f1cbd6b48d2d9b20c0827d153f55545841735b5c001a19de31e25547'

/** Real heights above the top of Fort Wood's walls (10 m above the ground). */
const PEDESTAL_TOP = 36.9 // 46.9 m above ground
const STATUE_HEIGHT = 46.1 // base of the statue to the tip of the torch

const response = await fetch(SOURCE)
if (!response.ok) throw new Error(`${SOURCE}: ${response.status}`)
const bytes = new Uint8Array(await response.arrayBuffer())
const sha = new Bun.CryptoHasher('sha256').update(bytes).digest('hex')
if (sha !== SOURCE_SHA256) throw new Error(`source model changed: sha256 ${sha}, expected ${SOURCE_SHA256}`)

const primitives = readGlb(bytes)

// The source's materials, by what they are: mat19 is the pedestal, mat12 the
// flame, mat11 everything else (the copper figure).
const role = (name: string) => (name === 'mat19' ? 'pedestal' : name === 'mat12' ? 'flame' : 'copper')

// Measure in source units: the pedestal's plan centre and its top, and the
// figure's extent above it.
const extent = (filter: (r: string) => boolean) => {
  const min = [Infinity, Infinity, Infinity], max = [-Infinity, -Infinity, -Infinity]
  for (const p of primitives) {
    if (!filter(role(p.material.name))) continue
    for (let i = 0; i < p.position.length; i += 3)
      for (let k = 0; k < 3; k++) {
        min[k] = Math.min(min[k], p.position[i + k])
        max[k] = Math.max(max[k], p.position[i + k])
      }
  }
  return { min, max }
}
const pedestal = extent((r) => r === 'pedestal')
const figure = extent((r) => r !== 'pedestal')
const cx = (pedestal.min[0] + pedestal.max[0]) / 2
const cz = (pedestal.min[2] + pedestal.max[2]) / 2
const base = pedestal.min[1]
const joint = pedestal.max[1]

// One horizontal scale for everything, from the figure; the pedestal alone is
// also stretched vertically, piecewise so the two meet without a seam.
const scale = STATUE_HEIGHT / (figure.max[1] - joint)
const pedestalStretch = PEDESTAL_TOP / (joint - base)
const height = (y: number) =>
  y <= joint ? (y - base) * pedestalStretch : PEDESTAL_TOP + (y - joint) * scale

const parts = { copper: new Part(), flame: new Part(), pedestal: new Part() }
for (const p of primitives) {
  const out = new Float32Array(p.position.length)
  for (let i = 0; i < p.position.length; i += 3) {
    out[i] = (p.position[i] - cx) * scale
    out[i + 1] = height(p.position[i + 1])
    out[i + 2] = (p.position[i + 2] - cz) * scale
  }
  addGltfTriangles(parts[role(p.material.name)], out, p.index)
}

const glb = writeGlb(
  'Statue of Liberty',
  [
    { part: parts.copper, material: { name: 'copper', color: 0x7cb5a1, doubleSided: true } },
    { part: parts.flame, material: { name: 'flame', color: 0xf0c45a, doubleSided: true } },
    { part: parts.pedestal, material: { name: 'pedestal', color: 0xc9bfa6, doubleSided: true } },
  ],
  {
    frame: 'Y up, -Z north, +X east, metres, origin at the anchor on the ground',
    source: SOURCE,
    attribution: '"Lady Liberty" by Anna M, CC-BY 3.0, via Poly Pizza',
  },
)

const out = process.argv[2] ?? new URL('../../landmarks/models/statue-of-liberty.glb', import.meta.url).pathname
await Bun.write(out, glb)
const triangles = parts.copper.triangles + parts.flame.triangles + parts.pedestal.triangles
console.log(`${out}: ${triangles} triangles, ${(glb.length / 1024).toFixed(1)} KiB`)
console.log(`scale ${scale.toFixed(2)} m/unit, pedestal stretched ${(pedestalStretch / scale).toFixed(2)}x`)
