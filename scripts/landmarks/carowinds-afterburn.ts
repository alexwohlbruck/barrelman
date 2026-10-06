/**
 * Afterburn (formerly Top Gun: The Jet Coaster), Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-afterburn.ts   (REPORT=1 for the element table)
 *
 * Built with ./coaster-kit.ts, as an inverted coaster (`inverted`): the
 * train hangs under the rails, the box spine sits above them, and the box
 * supports reach over the track. Map frame: x east, y north, z up, metres;
 * bearing 0. The origin is the middle of the footprint (ANCHOR), at the
 * lowest ground under the track.
 *
 * Plan: the OSM roller_coaster=track ways from the station exit, in the
 * direction of travel: way/669986344, 894788185, 894788183, 894788196,
 * 894788184, 894788195, 894788190, 894788189, 894788180, 894788191,
 * 894788193, 894788186, 894788188, 894788182, 894788194, 894788181,
 * 888529618. The transfer spur (way/894788187, 894788192) is left out. Two
 * places are redrawn: the loop's zigzag (way/894788184, 894788195,
 * 894788190) is replaced by the straight it stands on, as the kit draws the
 * loop itself; and a hairpin cusp before the tunnel (way/894788191) is
 * eased into a bend.
 *
 * Profile: a B&M inverted coaster of 901 m (RCDB 527, Wikipedia,
 * Coasterpedia): a 113 ft chain lift (113 ft above the station; the ground
 * falls about 6 m to the lift's top, so the 125 ft drop reaches the bottom
 * of the dip), a small pre-drop and a drop turning right, a 90 ft vertical
 * loop, a ravine below ground level, an Immelmann, a zero-g roll under the
 * lift hill, a dive into the tunnel under the rear entrance and the batwing,
 * a camelback beside the station, a corkscrew to the right, a 200° climbing
 * left turn and the brakes.
 *
 * Photos matched (Wikimedia Commons), both from the Carolina Skytower: "Top
 * Gun (Layout)", Coasterman1234 at en.wikipedia, CC BY-SA 3.0, and
 * "Carowinds 025 (3145067126)", Jeremy Thompson, CC BY 2.0 (loop top about
 * 28 m, Immelmann about 26 m above the ground). Colours from "Afterburn's
 * gray inverted track and blue supports stretching across a grassy lawn",
 * ThrillZing, October 2024, CC BY 4.0.
 */
import { buildCoaster } from './coaster-kit'

const CHAIN: [number, number][] = [
  [181.5, -311.4], [179.3, -325.1], [169.7, -385.8], [168.3, -390.0], [166.4, -392.0], [164.1, -393.9], [161.4, -394.8], [158.5, -394.2],
  [155.7, -392.7], [153.1, -389.3], [151.3, -386.6], [150.4, -384.7], [149.3, -380.7], [148.4, -378.2], [146.8, -368.5], [146.7, -362.0],
  [146.5, -355.0], [146.4, -348.0], [146.3, -341.0], [146.1, -334.0], [146.0, -327.0], [145.8, -317.6], [144.2, -306.6], [142.9, -300.7],
  [141.4, -295.3], [139.5, -288.8], [137.6, -284.0], [135.3, -278.2], [133.4, -274.9], [131.5, -274.0], [130.2, -274.3], [129.7, -275.3],
  [129.4, -277.0], [129.7, -280.0], [131.5, -284.4], [134.8, -291.0], [138.1, -294.9], [141.1, -297.6], [143.4, -298.8], [146.7, -301.8],
  [149.5, -303.6], [152.3, -306.4], [153.9, -308.5], [155.8, -312.0], [157.0, -315.3], [157.8, -318.0], [158.8, -322.0], [159.7, -324.7],
  [161.0, -327.4], [163.1, -330.6], [166.7, -335.1], [168.1, -337.4], [169.7, -342.3], [171.8, -345.4], [176.7, -349.6], [178.4, -353.2],
  [184.8, -361.0], [187.3, -365.5], [189.2, -369.4], [190.2, -373.4], [190.6, -376.6], [190.6, -379.3], [190.2, -381.5], [191.5, -384.9],
  [194.5, -387.3], [198.0, -388.0], [201.5, -387.0], [204.5, -384.5], [212.3, -372.7], [217.4, -368.8], [221.5, -365.8], [221.6, -365.0],
  [220.9, -365.0], [219.1, -365.9], [215.9, -366.9], [213.0, -367.2], [209.7, -366.9], [206.6, -365.6], [204.6, -363.3], [202.9, -360.5],
  [198.8, -351.1], [195.0, -343.9], [189.9, -334.2], [175.7, -309.7], [171.3, -303.5], [162.1, -291.7], [158.6, -285.9], [156.9, -282.1],
  [156.7, -279.5], [157.5, -276.8], [158.9, -273.4], [161.3, -267.6], [162.1, -264.4], [162.0, -261.2], [160.9, -258.3], [159.1, -255.1],
  [154.9, -250.2], [150.7, -246.4], [147.2, -243.2], [144.2, -241.3], [141.7, -240.2], [139.1, -239.2], [135.7, -239.1], [133.2, -239.8],
  [130.9, -240.9], [129.4, -242.4], [128.5, -244.7], [128.5, -247.7], [129.6, -250.0], [131.9, -252.3], [134.3, -253.5], [138.1, -254.3],
  [142.1, -254.4], [146.4, -253.7], [150.5, -252.2], [165.1, -246.0], [176.4, -241.2], [180.0, -240.4], [183.8, -240.6], [186.8, -241.5],
  [189.1, -243.0], [190.9, -245.2], [191.9, -248.0], [192.5, -251.0], [192.1, -254.7], [191.5, -259.8], [185.1, -290.8], [183.9, -296.4],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (100, -420); rows
// south to north, columns west to east; metres above 192.1 m.
const GROUND = [
  [0.0, 0.3, 0.8, 1.1, 1.3, 1.2, 1.9, 1.6],
  [0.6, 0.8, 1.3, 1.6, 2.3, 2.3, 2.4, 2.3],
  [1.2, 1.5, 2.2, 3.4, 5.0, 3.6, 3.6, 3.0],
  [3.4, 3.8, 4.5, 4.8, 6.4, 4.8, 6.2, 5.1],
  [3.9, 5.2, 5.5, 5.8, 6.3, 8.1, 8.1, 6.7],
  [4.0, 4.6, 5.1, 7.4, 7.9, 8.4, 8.0, 7.5],
  [4.3, 4.5, 2.3, 7.9, 8.7, 9.0, 8.7, 7.6],
  [4.2, 4.2, 4.4, 8.4, 9.5, 9.8, 9.3, 8.2],
  [4.3, 4.3, 5.6, 8.2, 10.2, 10.4, 10.3, 8.8],
  [4.2, 4.2, 4.8, 5.9, 9.7, 10.4, 10.0, 9.3],
  [4.2, 4.3, 4.4, 5.1, 6.9, 8.9, 8.7, 8.7],
]
// The station building (way/888529381).
const STATION: [number, number][] = [
  [186.1, -296.8], [183.9, -296.4], [181.2, -295.6], [179.7, -295.6], [179.1, -298.8], [178.8, -300.3], [176.9, -300.0], [175.2, -308.2], [174.9, -310.1], [181.5, -311.4],
  [189.8, -312.9], [191.6, -313.2], [192.0, -310.7], [192.6, -307.1], [196.8, -307.7], [197.3, -304.2], [193.6, -303.6], [194.5, -298.1], [192.3, -297.7], [190.9, -297.5],
]

await buildCoaster({
  name: 'Afterburn',
  out: '../../landmarks/models/carowinds-afterburn.glb',
  chain: CHAIN,
  anchor: [165, -315],
  ground: { x0: 100, y0: -420, step: 20, rows: GROUND, base: 192.1 },
  // Rail heights above the local ground; the riders hang about 3 m below.
  profile: [
    { name: 'station', s: 0, z: 4.5 },
    { name: 'foot of the lift', s: 16, z: 4.5, r: 14 },
    { name: 'lift crest (113 ft)', s: 70, z: 40, r: 12 },
    { name: 'lift crest, out', s: 74, z: 40, r: 12 },
    { name: 'pre-drop', s: 85, z: 37.5, r: 12 },
    { name: 'pre-drop, out', s: 89, z: 37.5, r: 16 },
    { name: 'drop to the right, foot', s: 127, z: 4.5, r: 24 },
    { name: 'loop, out', s: 162, z: 4.5, r: 28 },
    { name: 'ravine', s: 182, z: 1.5 },
    { name: 'Immelmann, in', s: 203, z: 1.5 },
    { name: 'Immelmann, out', s: 250, z: 25.8 },
    { name: 'valley', s: 273, z: 4.2 },
    { name: 'under the lift', s: 290, z: 5.5 },
    { name: 'zero-g roll, in', s: 317, z: 15 },
    { name: 'zero-g roll, out', s: 343, z: 15 },
    { name: 'tunnel', s: 372, z: 1.2 },
    { name: 'batwing, in', s: 381, z: 1.2 },
    { name: 'batwing, out', s: 420, z: 5 },
    { name: 'camelback beside the station', s: 458, z: 15 },
    { name: 'valley', s: 478, z: 5, r: 16 },
    { name: 'corkscrew, in', s: 495, z: 8 },
    { name: 'corkscrew, out', s: 533, z: 8 },
    { name: 'climbing turn, foot', s: 548, z: 6 },
    { name: 'climbing turn, top', s: 618, z: 12 },
    { name: 'brakes', s: 640, z: 11, r: 20 },
    { name: 'brakes, out', s: 690, z: 5.5, r: 20 },
  ],
  lift: [16, 72],
  brakes: [{ from: 630, to: 16, z: 5, head: 0.5 }],
  belowGrade: [[160, 222], [350, 428]],
  inversions: [
    { kind: 'loop', name: 'vertical loop (90 ft)', from: 128, to: 161, height: 24, rTop: 4.5 },
    // A half loop of 11.5 m radius in the vertical plane of the entry, then the half roll out over the top.
    { kind: 'path', name: 'Immelmann', from: 203, to: 250, points: [[136.5, -281.1, 3.0], [134.9, -277.1, 7.5], [134.3, -275.7, 13.6], [134.9, -277.1, 19.6], [136.5, -281.1, 24.1], [138.6, -286.4, 25.7], [140.6, -292.5, 25.9]] },
    { kind: 'roll', name: 'zero-g roll', from: 317, to: 343, radius: -2.5, dir: 'left' },
    { kind: 'path', name: 'batwing', from: 381, to: 420, points: [[219.0, -367.6, 2.8], [222.4, -364.4, 8.5], [224.6, -360.0, 15.5], [223.6, -355.6, 20.5], [219.5, -353.4, 22], [214.8, -354.8, 19.5], [210.4, -357.8, 12.5], [205.5, -359.6, 6.5]] },
    { kind: 'roll', name: 'corkscrew', from: 495, to: 533, radius: -4.5, dir: 'right' },
  ],
  losses: [0.008, 0.0004],
  inverted: { clear: 4 },
  supportEvery: 12,
  track: { W: 2.8, RAIL: 0.6, DEPTH: 1.7, SPINE: 1.3 },
  station: { ring: STATION, posts: [], roofZ: 8 },
  stationWalls: true,
  // Light grey track over blue box supports, from the 2024 photos (repainted 2014).
  colours: { deck: ['afterburn-grey', 0xcdd1d5], spine: ['afterburn-grey-2', 0xb9bec4], supports: ['afterburn-blue', 0x4f68b8] },
  supportR: [0.6, 0.75, 0.9],
  meta: { height: 40, trackLength: 901 },
})
