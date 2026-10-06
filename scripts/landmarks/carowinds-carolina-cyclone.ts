/**
 * Carolina Cyclone, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-carolina-cyclone.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The origin is the
 * middle of the track's footprint (see anchor), at the lowest ground under it.
 * Built with ./coaster-kit.ts, using its loops and corkscrews.
 *
 * Plan: the OSM circuit (way/650270619, way/670967409, way/670967410), from
 * where the track leaves the station building (way/241199374). OSM draws each
 * loop as a back-and-forth spike and the double corkscrew as an S; those
 * nodes are dropped (the kit draws both), so positions are metres along the
 * edited polyline, 559 m round.
 *
 * Profile: an Arrow custom looper (RCDB 81: 2,100 ft, 95 ft lift, 65 ft drop,
 * four inversions; Wikipedia for the order). Loop heights and shapes from
 * photos: the loops are tall teardrops whose tops sit about 21 to 23 m up,
 * below Wikipedia's 85 ft, which the energy from a 95 ft lift could not reach.
 *
 *   station, then a left hairpin    s 0–33
 *   lift, 29 m                      s 36–122 (6–22%)
 *   pre-drop and level turn, 26.5 m s 136–154, on the lattice tower
 *   drop to the loops               foot at s 185 (33%)
 *   loop 1, 19 m                    s 189–211 (34–38%)
 *   loop 2, 17.5 m                  s 212–234 (38–42%)
 *   banked turn over a small hill   s 272 (49%)
 *   double corkscrew, radius 5 m    s 302–348 (54–62%), over the walkway
 *   low helix, 1.5 turns            s 380–500
 *   brakes and station              from s 512 (92%)
 *
 * Photos (Wikimedia Commons): "Carolina Cyclone loops and corkscrew"
 * (2012, Martin Lewison, CC BY-SA 2.0) matched by hand-placed camera; "Carolina
 * Cyclone's twin vertical loops and a train crossing the structure above the
 * trees", ThrillZing, October 2024, CC BY 4.0, matched with the camera moved
 * from the phone GPS to fit the loops' bearings.
 *
 * Style: one swept teal ribbon (track and spine, as painted since 2021) on
 * white tubular columns; the station is its building, walls and roof.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit (way/650270619, way/670967409, way/670967410, oneway, in
// the direction of travel), node by node in the park frame (see coaster-kit),
// starting where the track leaves the station building (way/241199374).
const CHAIN: [number, number][] = [
  [-49.8, 148.3], [-44.1, 160.0], [-43.6, 161.8], [-43.3, 163.5], [-43.6, 164.9], [-44.3, 166.3], [-45.9, 167.4], [-48.1, 168.2],
  [-50.2, 168.1], [-52.6, 167.3], [-54.6, 165.9], [-55.7, 164.4], [-94.6, 79.9], [-96.3, 73.1], [-92.8, 68.9], [-89.6, 66.4],
  [-84.7, 66.4], [-81.1, 69.5], [-77.7, 74.6], [-67.4, 93.2], [-50.4, 125.4], [-44.0, 140.1], [-33.1, 161.3], [-27.2, 165.7],
  [-22.1, 166.5], [-15.7, 165.3], [-11.4, 161.9], [-8.5, 156.4], [-8.3, 149.7], [-10.8, 144.6], [-15.5, 141.2], [-23.1, 138.2],
  [-36.1, 105.5], [-35.5, 97.3], [-37.0, 88.0], [-40.4, 79.3], [-45.0, 73.5], [-51.6, 70.2], [-57.8, 69.4], [-62.9, 70.7],
  [-67.5, 75.0], [-69.7, 80.4], [-67.8, 87.2], [-62.7, 91.4], [-56.7, 92.1], [-51.5, 90.2], [-47.6, 85.1], [-47.3, 78.2],
  [-48.6, 75.2], [-53.0, 71.5], [-59.9, 70.7], [-66.8, 73.0], [-72.3, 78.2], [-74.8, 88.4], [-74.3, 94.7], [-72.1, 101.0],
  [-59.2, 128.7],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (-120, 40); rows
// south to north, columns west to east; metres above 189.0 m.
const GROUND = [
  [1.3, 3.8, 4.6, 4.4, 4.6, 4.8, 5.0, 5.3],
  [1.5, 2.6, 3.4, 3.6, 4.6, 4.8, 4.6, 4.9],
  [1.7, 1.9, 2.8, 1.8, 3.7, 4.3, 4.7, 5.0],
  [1.8, 1.9, 1.6, 3.3, 3.7, 4.5, 4.6, 4.8],
  [1.5, 1.9, 1.4, 2.4, 3.7, 4.0, 4.2, 4.4],
  [0.7, 1.8, 1.5, 2.1, 3.4, 3.3, 4.0, 4.2],
  [0.1, 0.9, 1.3, 1.4, 2.7, 4.0, 3.9, 4.3],
  [0.6, 0.3, 1.3, 2.5, 3.2, 3.6, 3.6, 3.7],
  [0.0, 1.1, 1.6, 2.1, 2.7, 2.8, 2.8, 3.1],
]
// The station building (way/241199374).
const STATION: [number, number][] = [
  [-58.8, 152.1], [-49.8, 148.3], [-48.7, 147.8], [-48.0, 147.5], [-46.8, 147.0], [-45.2, 146.1], [-47.4, 141.1], [-48.0, 139.7], [-49.2, 137.2],
  [-54.4, 126.0], [-59.2, 128.7], [-65.5, 132.2], [-64.6, 134.6], [-62.4, 139.2], [-63.4, 139.7], [-60.5, 146.1], [-61.4, 146.4],
]

await buildCoaster({
  name: 'Carolina Cyclone',
  out: '../../landmarks/models/carowinds-carolina-cyclone.glb',
  chain: CHAIN,
  anchor: [-52, 117],
  ground: { x0: -120, y0: 40, step: 20, rows: GROUND, base: 189 },
  // Heights above the local ground; s along the OSM polyline (610 m round).
  profile: [
    { name: 'station', s: 0, z: 2.5 },
    { name: 'foot of the lift', s: 36, z: 2.5, r: 16 },
    { name: 'lift crest (95 ft)', s: 122, z: 29, r: 14 },
    { name: 'pre-drop', s: 136, z: 26.5, r: 14 },
    { name: 'turn, top of the drop', s: 154, z: 26.5, r: 16 },
    { name: 'drop, foot', s: 185, z: 3.5, r: 20 },
    { name: 'loop 1, in', s: 189, z: 3.5 },
    { name: 'loop 2, out', s: 234, z: 3.5, r: 16 },
    { name: 'banked turn hill', s: 272, z: 10 },
    { name: 'valley', s: 295, z: 4 },
    { name: 'corkscrews, in', s: 302, z: 4 },
    { name: 'corkscrews, out', s: 348, z: 4 },
    { name: 'helix, low', s: 410, z: 2 },
    { name: 'helix, low', s: 470, z: 2 },
    { name: 'brakes', s: 512, z: 2.5, r: 15 },
  ],
  lift: [36, 122],
  brakes: [{ from: 512, to: 36, z: 2.5, head: 0.5 }],
  inversions: [
    { kind: 'loop', name: 'loop 1', from: 189, to: 211.5, height: 19, rTop: 3.3, offset: 4 },
    { kind: 'loop', name: 'loop 2', from: 212.5, to: 234, height: 17.5, rTop: 3.3, offset: 4 },
    { kind: 'roll', name: 'double corkscrew', from: 302, to: 348, radius: 5, turns: 2, dir: 'right' },
  ],
  losses: [0.012, 0.0005],
  track: { W: 2.2, RAIL: 0.45, DEPTH: 1.4, SPINE: 1.1 },
  station: { ring: STATION, posts: [], roofZ: 6 },
  stationWalls: true,
  colours: { deck: ['cyclone-teal', 0x3fb8b4], spine: ['cyclone-teal-2', 0x34a19e], supports: 'trim' },
  supportR: [0.75, 1.0, 1.1],
  meta: { height: 29, trackLength: 640 },
})
