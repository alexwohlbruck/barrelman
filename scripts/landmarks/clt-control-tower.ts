/**
 * Charlotte Douglas International Airport (CLT): the FAA air traffic control
 * tower — procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/clt-control-tower.ts
 *
 * The 2015 tower south of the terminal (OSM way/589068976, height 113 m;
 * Wikipedia gives 367 ft). A plain white round shaft that flares like a
 * trumpet into a broad two-storey drum with a dark window band round its
 * middle, topped by the narrower glazed cab under a white roof and a short
 * mast (Mapillary images 2019 from the runway, m240/m270 in the work folder).
 *
 * Proportions are measured off those photos against the OSM ring, which is
 * the drum's 22 m diameter: the shaft about half as wide, the flare from 64 to
 * 77 m, the drum to 98.5 m, the cab to 106 m, the mast to 113 m.
 *
 * Unlike the terminal models it is round, so it is placed at bearing 0 with
 * its origin at the centre of the ring.
 */
import { Part, writeGlb } from './mesh'
import { PALETTE } from './palette'
import { lathe } from './clt-terminal'

const stone = new Part(), trim = new Part(), win = new Part(), metal = new Part()
const SEG = 16

// Shaft and flare, smooth.
lathe(stone, 0, 0, [[5.4, 0], [5.3, 40], [5.25, 64], [5.6, 68.5], [6.6, 72.2], [8.2, 74.7], [9.8, 76.3], [11.0, 77.4]], SEG)
// The drum: two plain storeys either side of the window band, with lips.
lathe(trim, 0, 0, [[11.0, 77.4], [11.25, 77.6], [11.25, 78.4], [11.0, 78.6]], SEG)
lathe(stone, 0, 0, [[11.0, 78.6], [11.0, 89.8]], SEG)
lathe(win, 0, 0, [[11.0, 89.8], [10.85, 90.0], [10.85, 91.4], [11.0, 91.6]], SEG)
lathe(stone, 0, 0, [[11.0, 91.6], [11.0, 97.4]], SEG)
lathe(trim, 0, 0, [[11.0, 97.4], [11.3, 97.6], [11.3, 98.3], [10.9, 98.5], [7.0, 98.5]], SEG)
// The cab: glass leaning out, a white roof with a deep edge.
lathe(win, 0, 0, [[6.6, 98.5], [7.3, 104.2]], SEG)
lathe(trim, 0, 0, [[7.3, 104.2], [7.8, 104.4], [7.8, 105.3], [6.4, 105.8], [0, 106.1]], SEG)
// Mast.
lathe(metal, 0, 0, [[0.5, 105.9], [0.35, 110], [0.2, 113], [0, 113]], 8)

const parts = [
  { part: stone, material: PALETTE.stone },
  { part: trim, material: PALETTE.trim },
  { part: win, material: PALETTE.window },
  { part: metal, material: PALETTE.metal },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Charlotte Douglas Air Traffic Control Tower', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor', bearing: 0, elevation: 0, height: 113,
})
if (glb.length > 250_000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/clt-control-tower.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
