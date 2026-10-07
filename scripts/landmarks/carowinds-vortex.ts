/**
 * Vortex, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-vortex.ts   (REPORT=1 for the element table)
 *
 * Built with ./coaster-kit.ts. Map frame: x east, y north, z up, metres;
 * bearing 0. The origin is the middle of the track's footprint (ANCHOR), at
 * the lowest ground under it.
 *
 * Plan: the chain of OSM roller_coaster=track ways from the west end of the
 * station (way/669164155), all oneway, in the direction of travel:
 * way/669205939, 894782920, 894782923, 894782917, 894782358, 894782912,
 * 894782914, 894782918, 894782924, 894782925, 894782922, 894782919,
 * 894782916, 894782915, 894782921, 894782913, 888642929 (609 m round).
 *
 * Profile: a B&M stand-up of 622 m (RCDB 86, Wikipedia, Coasterpedia): a
 * 90 ft chain lift and pre-drop, the 80 ft curved drop to the right, a
 * vertical loop, a rising right turn and a drop into a left upward helix, a
 * smaller drop and a corkscrew to the right, a figure-eight turn, and a left
 * turn into the brakes.
 *
 * Photos matched (Wikimedia Commons): "Vortex (Carowinds) 2023 1", Jeremy
 * Thompson, CC BY 2.0 (from the Skytower; loop top measured at about 22 m);
 * "Vortex's red vertical loop and black supports from the path", ThrillZing,
 * October 2024, CC BY 4.0; "Carowinds 023 (3145061684)", Jeremy Thompson,
 * CC BY 2.0 (helix and figure eight). Colours from the 2024 photos: red
 * track, black supports since the 2023 refurbishment.
 */
import { buildCoaster } from './coaster-kit'

const CHAIN: [number, number][] = [
  [114.8, 80.5], [107.9, 77.7], [95.5, 72.6], [77.2, 65.1], [67.9, 61.3], [59.1, 57.2], [56.9, 56.0], [53.9, 54.9],
  [50.5, 54.3], [47.9, 54.4], [46.1, 55.3], [44.8, 56.9], [43.9, 59.1], [43.7, 61.5], [44.1, 63.9], [44.9, 66.1],
  [45.9, 67.7], [48.8, 70.8], [52.5, 73.0], [58.5, 74.0], [64.5, 75.0], [70.5, 76.0], [76.5, 77.0], [82.5, 77.9],
  [88.5, 78.9], [94.5, 79.9], [97.2, 81.1], [102.2, 82.2], [104.8, 82.0], [108.0, 80.8], [110.1, 79.1], [111.8, 76.7],
  [112.6, 74.1], [112.2, 70.9], [111.3, 67.9], [109.5, 62.5], [108.6, 58.7], [108.4, 54.9], [108.4, 50.9], [109.2, 45.8],
  [110.7, 41.6], [113.2, 37.9], [116.1, 35.2], [119.2, 33.8], [122.0, 33.0], [124.8, 32.9], [127.5, 33.6], [129.5, 34.9],
  [131.3, 36.6], [132.7, 39.1], [133.5, 41.0], [133.7, 43.6], [133.4, 46.3], [132.3, 48.7], [130.9, 50.7], [128.7, 52.2],
  [126.0, 53.6], [124.2, 54.1], [121.7, 54.1], [119.3, 53.6], [116.6, 52.6], [114.3, 50.5], [112.7, 47.4], [112.3, 43.8],
  [112.6, 40.8], [113.5, 38.3], [115.7, 36.2], [118.3, 35.1], [120.9, 34.4], [124.1, 34.5], [127.1, 35.3], [129.5, 36.6],
  [132.7, 39.1], [134.9, 41.5], [139.7, 47.4], [145.2, 55.7], [151.0, 65.5], [152.4, 67.0], [154.1, 67.7], [156.0, 67.4],
  [158.1, 65.5], [160.4, 63.1], [161.9, 61.7], [164.3, 60.6], [166.6, 60.6], [168.5, 60.9], [170.4, 61.8], [172.1, 63.3],
  [173.4, 65.0], [175.1, 67.4], [179.9, 74.3], [181.6, 76.2], [183.8, 78.2], [185.4, 79.3], [187.7, 80.1], [189.9, 80.2],
  [191.9, 79.8], [193.8, 79.0], [195.5, 77.7], [196.8, 75.6], [197.4, 73.0], [197.0, 70.7], [195.4, 67.1], [193.0, 63.9],
  [188.9, 59.6], [184.6, 55.9], [180.9, 53.4], [177.3, 51.5], [174.7, 50.4], [171.9, 50.0], [168.9, 50.1], [165.6, 51.0],
  [163.6, 52.4], [161.9, 54.4], [160.8, 56.7], [160.2, 59.6], [160.2, 62.3], [160.4, 63.1], [160.9, 64.6], [161.8, 66.8],
  [163.2, 68.7], [165.4, 70.3], [168.2, 71.5], [171.5, 71.8], [174.3, 71.1], [180.1, 68.2], [183.9, 66.2], [187.7, 65.2],
  [190.4, 64.9], [193.0, 65.5], [194.7, 66.4], [195.4, 67.1], [196.2, 67.9], [197.6, 70.5], [198.0, 73.2], [197.8, 75.8],
  [197.2, 78.6], [195.8, 81.3], [193.9, 83.3], [191.9, 84.8], [189.4, 85.7], [186.2, 86.2], [180.8, 86.1], [133.5, 87.3],
  [130.9, 87.4], [128.9, 86.9],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (20, 20); rows
// south to north, columns west to east; metres above 193.1 m.
const GROUND = [
  [3.1, 0.9, 1.4, 1.9, 1.7, 1.1, 0.1, 0.3, 0.4, 0.0, 2.3],
  [1.2, 0.6, 0.8, 0.9, 1.4, 1.8, 1.8, 0.8, 0.0, 2.2, 2.4],
  [0.8, 0.6, 0.5, 0.4, 1.1, 1.5, 2.0, 1.7, 2.3, 2.2, 2.4],
  [0.9, 1.1, 0.4, 0.6, 1.3, 1.8, 1.2, 1.5, 1.7, 2.0, 2.9],
  [0.7, 0.7, 1.0, 1.4, 1.4, 1.7, 1.6, 1.5, 1.7, 2.3, 2.3],
  [0.3, 0.8, 0.8, 0.9, 1.4, 1.3, 1.2, 1.3, 2.1, 1.7, 2.6],
]
// The station canopy (way/669164155).
const STATION: [number, number][] = [
  [112.4, 86.6], [116.6, 88.5], [115.9, 90.1], [117.1, 90.7], [119.8, 91.9], [120.9, 92.4], [121.5, 91.1], [126.0, 93.1], [127.7, 89.4], [128.9, 86.9], [130.4, 83.7],
  [122.0, 79.9], [121.1, 79.5], [122.1, 77.2], [120.8, 76.7], [118.0, 75.3], [116.0, 79.6], [115.3, 79.3], [114.8, 80.5], [113.5, 83.2], [113.9, 83.3],
]

await buildCoaster({
  name: 'Vortex',
  out: '../../landmarks/models/carowinds-vortex.glb',
  chain: CHAIN,
  anchor: [120, 62],
  ground: { x0: 20, y0: 20, step: 20, rows: GROUND, base: 193.1 },
  profile: [
    { name: 'station', s: 0, z: 1.5 },
    { name: 'foot of the lift', s: 8, z: 1.5, r: 12 },
    { name: 'lift crest (90 ft)', s: 50, z: 27.4, r: 14 },
    { name: 'lift crest, out', s: 54, z: 27.4, r: 14 },
    { name: 'pre-drop', s: 65, z: 24.8, r: 14 },
    { name: 'pre-drop, out', s: 72, z: 24.8, r: 16 },
    { name: 'curved drop, foot (80 ft)', s: 103, z: 2.5, r: 22 },
    { name: 'loop, out', s: 132, z: 2.5, r: 22 },
    { name: 'rising right turn', s: 156, z: 12 },
    { name: 'valley', s: 186, z: 2.5 },
    { name: 'upward helix, low lap', s: 218, z: 3.5 },
    { name: 'upward helix, high lap', s: 285, z: 10 },
    { name: 'corkscrew, in (after a smaller drop)', s: 316, z: 6 },
    { name: 'corkscrew, out', s: 352, z: 6 },
    { name: 'figure eight, top', s: 385, z: 10 },
    { name: 'figure eight, low', s: 448, z: 1.2 },
    { name: 'brakes', s: 492, z: 3.5, r: 20 },
    { name: 'brakes, out', s: 557, z: 2.5 },
  ],
  lift: [8, 52],
  brakes: [{ from: 487, to: 8, z: 3, head: 0.5 }],
  inversions: [
    { kind: 'loop', name: 'vertical loop', from: 103.5, to: 131.5, height: 20, rTop: 3.5 },
    { kind: 'roll', name: 'corkscrew', from: 316, to: 352, radius: 4.5, dir: 'right' },
  ],
  losses: [0.008, 0.0004],
  track: { W: 2.8, RAIL: 0.6, DEPTH: 1.6, SPINE: 1.2 },
  // An open canopy over the platform (the 2008 and 2023 aerials), on posts clear of the track.
  station: { ring: STATION, posts: [[113, 85], [119, 90], [126, 91.5], [121, 77.5], [128.5, 82]], roofZ: 7 },
  supportEvery: 9,
  // Red track, black supports (repainted for the 2023 refurbishment), from the 2024 photos.
  colours: { deck: ['vortex-red', 0xc94a40], spine: ['vortex-red-2', 0xb03d36], supports: ['vortex-black', 0x4a4f57] },
  supportR: [0.55, 0.65, 0.75],
  meta: { height: 27.4, trackLength: 622 },
})
