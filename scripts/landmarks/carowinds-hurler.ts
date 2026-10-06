/**
 * Hurler, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-hurler.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The origin is the
 * middle of the track's footprint (see anchor), at the lowest ground under it.
 * Built with ./coaster-kit.ts, with its wooden trestle.
 *
 * Plan: the OSM circuit (way/650268718, way/650268717, way/239949108,
 * way/650268716, way/890803169), 930 m in plan, from the station exit.
 *
 * Profile: an International Coasters wooden triple out-and-back (RCDB 85:
 * 3,157 ft, 83 ft, 50 mph; Wikipedia for the ride in order: right turn out
 * of the station, lift, a small right turn on top, the first drop, a wide
 * banked turn, airtime hills, a second wide turn, more hills, an elevated
 * turn, a small hill, a last wide turn, a final hill, brakes). Heights of
 * the hills are set from the energy left and checked against photos.
 *
 *   lift, 25.3 m                    s 36–98 (4–11%)
 *   turn on top, then 50° drop      s 100–170 (11–18%)
 *   banked turn, south-west         s 225 (24%)
 *   airtime hills, 15 m and 12 m    s 320, 392 (34%, 42%)
 *   banked turn, north-east         s 485 (52%)
 *   airtime hill, 9.5 m             s 575 (62%)
 *   elevated banked turn            s 662 (71%)
 *   hill, last turn, final hill     s 722, 775, 845
 *   brakes                          from s 872 (94%)
 *
 * Photos (Wikimedia Commons), matched by hand-placed camera: "Hurler
 * (Carowinds) 1" (from the Skytower, 2023) and "Carowinds 014" (from the
 * midway, 2008), both Jeremy Thompson, CC BY 2.0.
 *
 * Style: a wooden deck ribbon on a trestle drawn as broad walls of bents and
 * ledgers with large openings, as coney-island-cyclone.ts does, in the
 * weathered silver-grey to grey-brown timber the photos show.
 * The station is the box building beside the brake run.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit (way/650268718, way/650268717, way/239949108,
// way/650268716, way/890803169, all oneway, in the direction of travel),
// node by node in the park frame (see coaster-kit), from the station exit.
const CHAIN: [number, number][] = [
  [-76.8, 244.4], [-79.6, 243.6], [-81.5, 243.6], [-83.9, 244.0], [-85.7, 244.9], [-87.9, 246.1], [-89.8, 248.0], [-90.9, 250.4],
  [-91.3, 253.3], [-91.2, 256.3], [-90.7, 258.6], [-89.6, 260.8], [-87.9, 262.6], [-85.5, 264.1], [-33.3, 283.0], [-22.2, 287.0],
  [-17.7, 287.8], [-12.9, 287.0], [-9.7, 285.0], [-7.1, 281.1], [-6.6, 278.1], [-6.8, 274.8], [-8.9, 270.8], [-22.7, 262.1],
  [-31.6, 256.1], [-51.8, 242.6], [-63.6, 236.9], [-73.4, 234.7], [-82.2, 235.2], [-88.8, 238.2], [-93.0, 243.9], [-95.3, 250.4],
  [-95.3, 256.6], [-92.8, 263.1], [-86.5, 271.1], [-76.7, 278.1], [73.2, 360.4], [85.3, 365.4], [93.1, 365.6], [101.4, 364.1],
  [107.9, 360.4], [111.4, 354.9], [113.2, 347.9], [111.4, 341.2], [107.9, 333.4], [103.1, 327.5], [90.1, 319.7], [44.2, 300.1],
  [34.1, 295.8], [-1.6, 280.1], [-6.6, 278.1], [-13.7, 275.3], [-21.2, 274.3], [-27.7, 275.8], [-32.2, 279.8], [-33.3, 283.0],
  [-34.5, 287.3], [-34.0, 293.8], [-31.2, 299.0], [-24.2, 304.5], [-2.1, 316.0], [16.0, 325.2], [22.5, 327.7], [30.3, 329.9],
  [40.1, 329.2], [45.6, 325.2], [48.9, 320.7], [50.6, 315.5], [49.6, 309.5], [45.9, 302.5], [44.2, 300.1], [42.1, 297.5],
  [30.3, 290.8], [-6.8, 274.8], [-34.7, 262.7], [-57.8, 252.2],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (-120, 220); rows
// south to north, columns west to east; metres above 184.6 m.
const GROUND = [
  [4.2, 5.7, 6.1, 6.7, 6.8, 7.1, 7.2, 7.3, 8.0, 7.9, 8.3, 9.5, 8.9, 6.5],
  [4.8, 5.9, 6.7, 6.6, 6.7, 7.3, 7.3, 7.1, 7.2, 7.1, 7.3, 6.8, 8.9, 7.2],
  [4.7, 5.4, 6.1, 6.4, 6.6, 6.4, 7.0, 7.2, 7.0, 7.1, 6.8, 6.8, 8.0, 7.4],
  [3.9, 4.8, 5.5, 5.4, 7.1, 7.1, 6.5, 7.2, 7.2, 6.9, 6.9, 6.6, 6.2, 8.6],
  [4.0, 4.7, 4.3, 3.9, 6.2, 7.3, 6.6, 6.6, 6.5, 6.7, 6.6, 6.5, 6.3, 6.0],
  [4.2, 4.1, 3.5, 3.7, 5.2, 6.0, 5.4, 5.3, 6.3, 6.5, 6.6, 6.5, 6.0, 5.4],
  [3.3, 2.6, 1.7, 2.6, 4.4, 4.8, 4.2, 3.3, 3.3, 6.3, 6.5, 6.3, 6.0, 5.1],
  [2.0, 1.4, 0.7, 1.8, 3.3, 3.7, 3.1, 2.1, 1.4, 1.5, 3.9, 6.2, 5.8, 3.2],
  [0.9, 0.2, 0.1, 1.1, 2.0, 2.1, 1.8, 1.2, 0.5, 0.4, 0.3, 1.5, 4.2, 4.6],
]
// The station building (way/668700368).
const STATION: [number, number][] = [
  [-81.3, 255.3], [-76.8, 244.4], [-74.7, 239.4], [-55.7, 247.2], [-57.8, 252.2], [-62.3, 263.1], [-71.1, 259.5],
]

await buildCoaster({
  name: 'Hurler',
  out: '../../landmarks/models/carowinds-hurler.glb',
  chain: CHAIN,
  anchor: [8.5, 301],
  ground: { x0: -120, y0: 220, step: 20, rows: GROUND, base: 184.6 },
  // Heights above the local ground; s along the OSM polyline (930 m round).
  profile: [
    { name: 'station', s: 0, z: 1.5 },
    { name: 'foot of the lift', s: 36, z: 1.5, r: 18 },
    { name: 'lift crest (83 ft)', s: 98, z: 25.3, r: 14 },
    { name: 'turn before the drop', s: 128, z: 23, r: 18, pitch: 50 },
    { name: 'first drop, foot', s: 170, z: 2, r: 22 },
    { name: 'banked turn (south-west)', s: 225, z: 8 },
    { name: 'valley', s: 282, z: 2 },
    { name: 'airtime hill', s: 320, z: 15 },
    { name: 'valley', s: 356, z: 2.5 },
    { name: 'airtime hill', s: 392, z: 12 },
    { name: 'valley', s: 428, z: 3 },
    { name: 'banked turn (north-east)', s: 485, z: 10 },
    { name: 'valley', s: 540, z: 2.5, r: 26 },
    { name: 'airtime hill', s: 575, z: 9.5 },
    { name: 'valley', s: 610, z: 2.5, r: 24 },
    { name: 'elevated banked turn', s: 662, z: 8 },
    { name: 'valley', s: 697, z: 2.5, r: 22 },
    { name: 'airtime hill', s: 722, z: 6.5 },
    { name: 'banked turn (east)', s: 775, z: 5 },
    { name: 'valley', s: 815, z: 2 },
    { name: 'final hill', s: 845, z: 5 },
    { name: 'brakes', s: 872, z: 1.5, r: 15 },
  ],
  lift: [36, 98],
  brakes: [{ from: 872, to: 36, z: 1.5, head: 0.5 }],
  losses: [0.012, 0.0005],
  track: { W: 3.0, RAIL: 0.5, DEPTH: 1.1, SPINE: 2.6 },
  station: { ring: STATION, posts: [], roofZ: 8 },
  stationWalls: true,
  trestle: { every: 4.2, along: 2.2, levelStep: 3.4, ledgerH: 1.2 },
  colours: { deck: ['hurler-wood', 0x8e8579], spine: ['hurler-wood-2', 0x7f776c], supports: ['hurler-timber', 0x9a9184] },
  supportR: [0.5, 0.6, 0.7],
  meta: { height: 25.3, trackLength: 962 },
})
