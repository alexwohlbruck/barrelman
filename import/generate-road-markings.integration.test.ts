/**
 * Runs generate-road-markings.sql over a synthetic junction in a throwaway
 * schema: a divided avenue with a planted median, crossed by a signalled street.
 * Then rebuilds part of it in place, queues it as a replication diff would and
 * plans the rebuild, and builds one box into a database that has no road tables.
 *
 * Point it at a scratch database: the queue test uses the osm_replay schema
 * that replicate-extract.sh uses.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/scratch \
 *      bun test import/generate-road-markings.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { readFileSync } from 'fs'
import { join } from 'path'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'road_markings_test'

const SB_X = -74.0
const NB_X = -73.9997
const STREET_Y = 40.71

// Two pieces with a gap between them: (-74.100, 40.713) to (-74.102, 40.716).
const RELATION = 'MULTILINESTRING((-74.100 40.712, -74.100 40.713, -74.1005 40.7135), (-74.102 40.716, -74.102 40.717, -74.1025 40.7175))'

const line = (...pts: [number, number][]) => `LINESTRING(${pts.map(p => p.join(' ')).join(', ')})`
const ways: [number, string, Record<string, string>][] = [
  [1, line([SB_X, 40.711], [SB_X, STREET_Y]), { highway: 'trunk', oneway: 'yes', lanes: '3', 'turn:lanes': 'left|through|through' }],
  [2, line([SB_X, STREET_Y], [SB_X, 40.709]), { highway: 'trunk', oneway: 'yes', lanes: '3' }],
  [3, line([NB_X, 40.709], [NB_X, STREET_Y]), { highway: 'trunk', oneway: 'yes', lanes: '3', 'bus:lanes': '||designated' }],
  [4, line([NB_X, STREET_Y], [NB_X, 40.711]), { highway: 'trunk', oneway: 'yes', lanes: '3', 'bus:lanes': '||designated' }],
  [5, line([-74.001, STREET_Y], [SB_X, STREET_Y]), { highway: 'residential', lanes: '2', 'cycleway:right': 'lane' }],
  [6, line([SB_X, STREET_Y], [NB_X, STREET_Y]), { highway: 'residential', lanes: '2' }],
  [7, line([NB_X, STREET_Y], [-73.999, STREET_Y]), { highway: 'residential', lanes: '2' }],
  [10, line([-74.002, 40.712], [-74.002, 40.714]), { highway: 'tertiary', 'overtaking': 'yes' }],
  [11, line([-74.003, 40.712], [-74.003, 40.714]), { highway: 'service' }],
  [12, line([-74.004, 40.712], [-74.004, 40.714]), { highway: 'service', lanes: '1' }],
  // A one-way that gains a lane, and one drawn along its right kerb.
  [20, line([-74.010, 40.712], [-74.010, 40.7125]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [21, line([-74.010, 40.7125], [-74.010, 40.7135]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [22, line([-74.012, 40.712], [-74.012, 40.713]), { highway: 'residential', oneway: 'yes', lanes: '2', placement: 'right_of:2' }],
  [27, line([-74.010, 40.7135], [-74.010, 40.7145]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // A one-way that gains a lane well south of a cell boundary at 40.718.
  [28, line([-74.020, 40.712], [-74.020, 40.7125]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [29, line([-74.020, 40.7125], [-74.020, 40.722]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // A one-way that gains a lane, then runs on as nine more ways.
  [40, line([-74.040, 40.712], [-74.040, 40.7125]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [41, line([-74.040, 40.7125], [-74.040, 40.7134]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [42, line([-74.040, 40.7134], [-74.040, 40.7143]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [43, line([-74.040, 40.7143], [-74.040, 40.7152]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [44, line([-74.040, 40.7152], [-74.040, 40.7161]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [45, line([-74.040, 40.7161], [-74.040, 40.7170]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [46, line([-74.040, 40.7170], [-74.040, 40.7179]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [47, line([-74.040, 40.7179], [-74.040, 40.7188]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [48, line([-74.040, 40.7188], [-74.040, 40.7197]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [49, line([-74.040, 40.7197], [-74.040, 40.7206]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // The same across a cell boundary at 40.72, its first way changed below.
  [50, line([-74.050, 40.718], [-74.050, 40.7195]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [51, line([-74.050, 40.7195], [-74.050, 40.7215]), { highway: 'residential', oneway: 'yes', lanes: '3', maxspeed: '45 mph' }],
  [52, line([-74.050, 40.7215], [-74.050, 40.723]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // A left-bay approach that runs on as two lanes.
  [60, line([-74.070, 40.712], [-74.070, 40.713]), { highway: 'residential', oneway: 'yes', lanes: '3', 'turn:lanes': 'left|through|through' }],
  [61, line([-74.070, 40.713], [-74.070, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  // One unsplit two-way street with bike lanes, through a crossroads.
  [62, line([-74.080, 40.7145], [-74.079, 40.7145], [-74.078, 40.7145]), { highway: 'residential', lanes: '2', 'cycleway:both': 'lane' }],
  [63, line([-74.079, 40.714], [-74.079, 40.7145], [-74.079, 40.715]), { highway: 'residential', lanes: '2' }],
  // A crossing that only clips the kerb of road 12, at 25° to it.
  [30, line([-74.004 + 0.9 / 84300, 40.7125], [-74.004 + (0.9 + 4.226) / 84300, 40.7125 + 9.063 / 111000]),
    { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  // A street with bike lanes on both sides of a crossroads.
  [23, line([-74.008, 40.7145], [-74.007, 40.7145]), { highway: 'residential', lanes: '2', 'cycleway:both': 'lane' }],
  [24, line([-74.007, 40.7145], [-74.006, 40.7145]), { highway: 'residential', lanes: '2', 'cycleway:both': 'lane' }],
  [25, line([-74.007, 40.714], [-74.007, 40.7145]), { highway: 'residential', lanes: '2' }],
  [26, line([-74.007, 40.7145], [-74.007, 40.715]), { highway: 'residential', lanes: '2' }],
  // A street mapped as two straight legs meeting at a right angle.
  [70, line([-74.0907, 40.7140], [-74.0900, 40.7140], [-74.0900, 40.7145]), { highway: 'residential' }],
  // Cul-de-sacs ending in a turning circle, one with a tagged diameter, a
  // turning loop, and one ending on a cell boundary at 40.718.
  [80, line([-74.120, 40.712], [-74.120, 40.713]), { highway: 'residential', lanes: '2' }],
  [81, line([-74.125, 40.712], [-74.125, 40.713]), { highway: 'residential', lanes: '2' }],
  [82, line([-74.130, 40.712], [-74.130, 40.713]), { highway: 'residential' }],
  [84, line([-74.135, 40.712], [-74.135, 40.713]), { highway: 'residential' }],
  [85, line([-74.140, 40.712], [-74.140, 40.713]), { highway: 'residential' }],
  [83, line([-74.026, 40.717], [-74.026, 40.718]), { highway: 'residential' }],
  // West St southbound at Murray St, mirrored north: three lanes into four
  // with a right bay, and a driveway meeting the join.
  [100, line([-74.160, 40.712], [-74.160, 40.7125]), { highway: 'trunk', oneway: 'yes', lanes: '3' }],
  [101, line([-74.160, 40.7125], [-74.160, 40.714]), { highway: 'trunk', oneway: 'yes', lanes: '4', 'turn:lanes': 'through|through|through|right' }],
  [102, line([-74.1605, 40.7125], [-74.160, 40.7125]), { highway: 'service', lanes: '1' }],
  // Bedford Ave and Rogers Ave, one lane and three, merging into four.
  [110, line([-74.1703, 40.7120], [-74.170, 40.713]), { highway: 'secondary', oneway: 'yes', lanes: '1' }],
  [111, line([-74.1697, 40.7120], [-74.170, 40.713]), { highway: 'secondary', oneway: 'yes', lanes: '3', 'turn:lanes': 'through|through|through' }],
  [112, line([-74.170, 40.713], [-74.170, 40.7145]), { highway: 'secondary', oneway: 'yes', lanes: '4' }],
  // The FDR losing a lane to an exit ramp that peels off to the right.
  [120, line([-74.180, 40.7115], [-74.180, 40.713]), { highway: 'motorway', oneway: 'yes', lanes: '4' }],
  [121, line([-74.180, 40.713], [-74.180, 40.715]), { highway: 'motorway', oneway: 'yes', lanes: '3' }],
  [122, line([-74.180, 40.713], [-74.17965, 40.715]), { highway: 'motorway_link', oneway: 'yes', lanes: '1' }],
  // A two-way street dividing into a pair of one-ways.
  [130, line([-74.190, 40.7115], [-74.190, 40.713]), { highway: 'primary', lanes: '4' }],
  [131, line([-74.190, 40.713], [-74.1898, 40.715]), { highway: 'primary', oneway: 'yes', lanes: '2' }],
  [132, line([-74.1902, 40.715], [-74.190, 40.713]), { highway: 'primary', oneway: 'yes', lanes: '2' }],
  // Grand Army Plaza: four lanes splitting three ways.
  [140, line([-74.200, 40.7115], [-74.200, 40.713]), { highway: 'primary', oneway: 'yes', lanes: '4' }],
  [141, line([-74.200, 40.713], [-74.2004, 40.715]), { highway: 'primary_link', oneway: 'yes', lanes: '1' }],
  [142, line([-74.200, 40.713], [-74.200, 40.715]), { highway: 'primary', oneway: 'yes', lanes: '3' }],
  [143, line([-74.200, 40.713], [-74.1996, 40.715]), { highway: 'secondary_link', oneway: 'yes', lanes: '1' }],
  // West St at Chambers St: a short way between a junction and the crosswalk
  // past it, all inside the junction.
  [150, line([-74.210, 40.714], [-74.210, 40.713]), { highway: 'trunk', oneway: 'yes', lanes: '3' }],
  [151, line([-74.210, 40.713], [-74.210, 40.71285]), { highway: 'trunk', oneway: 'yes', lanes: '3' }],
  [152, line([-74.210, 40.71285], [-74.210, 40.711]), { highway: 'trunk', oneway: 'yes', lanes: '3' }],
  [153, line([-74.2105, 40.713], [-74.210, 40.713], [-74.2095, 40.713]), { highway: 'secondary', lanes: '2' }],
  [154, line([-74.2102, 40.71295], [-74.2098, 40.71295]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  [155, line([-74.2102, 40.71282], [-74.2098, 40.71282]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  // A motorway bridge passing over that junction.
  [156, line([-74.2115, 40.7129], [-74.2085, 40.7129]), { highway: 'motorway', oneway: 'yes', lanes: '3', bridge: 'yes', layer: '1' }],
  // An exit ramp gaining a lane at each of four ways, three of them short,
  // across a cell boundary at 40.718.
  [170, line([-74.015, 40.7160], [-74.015, 40.7172]), { highway: 'motorway_link', oneway: 'yes', lanes: '1' }],
  [171, line([-74.015, 40.7172], [-74.015, 40.71748]), { highway: 'motorway_link', oneway: 'yes', lanes: '2' }],
  [172, line([-74.015, 40.71748], [-74.015, 40.71780]), { highway: 'motorway_link', oneway: 'yes', lanes: '3', 'turn:lanes': 'none|none|right' }],
  [173, line([-74.015, 40.71780], [-74.015, 40.71803]), { highway: 'motorway_link', oneway: 'yes', lanes: '4' }],
  [174, line([-74.015, 40.71803], [-74.015, 40.7190]), { highway: 'motorway_link', oneway: 'yes', lanes: '5' }],
  // A way whose two nodes were mapped at one spot.
  [175, line([-74.015, 40.7195], [-74.015, 40.7195]), { highway: 'residential', lanes: '2' }],
  // A street crossed twice at one spot, the crossings a few degrees apart.
  [160, line([-74.2205, 40.713], [-74.2195, 40.713]), { highway: 'residential', lanes: '2' }],
  [161, line([-74.2200, 40.71285], [-74.2200, 40.71315]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  [162, line([-74.22003, 40.71285], [-74.21997, 40.71315]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  // A crossing drawn well past both kerbs, and askew to the street.
  [9, line([-74.0006, 40.70985], [-74.0004, 40.71015]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  // One-ways between sidewalks mapped 7 m (9 m) either side of them: plain,
  // with three lanes, with a tagged width, and with a cycle track between the
  // street and its sidewalk on one side.
  [400, line([-74.400, 40.712], [-74.400, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '1' }],
  [401, line([-74.400 + 7 / 84300, 40.712], [-74.400 + 7 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  [402, line([-74.400 - 7 / 84300, 40.712], [-74.400 - 7 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  [403, line([-74.403, 40.712], [-74.403, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [404, line([-74.403 + 9 / 84300, 40.712], [-74.403 + 9 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  [405, line([-74.403 - 9 / 84300, 40.712], [-74.403 - 9 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  [406, line([-74.406, 40.712], [-74.406, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '1', width: '6' }],
  [407, line([-74.406 + 7 / 84300, 40.712], [-74.406 + 7 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  [408, line([-74.406 - 7 / 84300, 40.712], [-74.406 - 7 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  [409, line([-74.409, 40.712], [-74.409, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '1' }],
  [410, line([-74.409 - 7 / 84300, 40.712], [-74.409 - 7 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  [411, line([-74.409 + 5 / 84300, 40.712], [-74.409 + 5 / 84300, 40.714]), { highway: 'cycleway' }],
  [412, line([-74.409 + 7 / 84300, 40.712], [-74.409 + 7 / 84300, 40.714]), { highway: 'footway', footway: 'sidewalk' }],
  // A divided avenue with a 4 m median, crossed by a street.
  [420, line([-74.420, 40.713], [-74.420, 40.712]), { highway: 'primary', oneway: 'yes', lanes: '2' }],
  [421, line([-74.420, 40.712], [-74.420, 40.711]), { highway: 'primary', oneway: 'yes', lanes: '2' }],
  [422, line([-74.420 + 10.8 / 84300, 40.711], [-74.420 + 10.8 / 84300, 40.712]), { highway: 'primary', oneway: 'yes', lanes: '2' }],
  [423, line([-74.420 + 10.8 / 84300, 40.712], [-74.420 + 10.8 / 84300, 40.713]), { highway: 'primary', oneway: 'yes', lanes: '2' }],
  [424, line([-74.4207, 40.712], [-74.420, 40.712], [-74.420 + 10.8 / 84300, 40.712], [-74.4192, 40.712]), { highway: 'residential', lanes: '2' }],
  // A signalled approach whose crosswalk runs 30° askew, its south end
  // furthest from the junction.
  [430, line([-74.441, 40.712], [-74.440, 40.712]), { highway: 'residential', lanes: '2' }],
  [431, line([-74.440, 40.712], [-74.439, 40.712]), { highway: 'residential', lanes: '2' }],
  [432, line([-74.440, 40.711], [-74.440, 40.712], [-74.440, 40.713]), { highway: 'residential', lanes: '2' }],
  [434, line([-74.440 - 12.3 / 84300, 40.712 - 4 / 111050], [-74.440 - 7.7 / 84300, 40.712 + 4 / 111050]),
    { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  // Four lanes north into a signalled crossroads over a crosswalk 20° askew,
  // over one 50° askew, and giving way over one 20° askew.
  ...([[440, -74.460, 20, 12], [450, -74.470, 50, 20], [460, -74.480, 20, 12]] as const).flatMap(([id, x, skew, back]) => {
    const t = Math.tan((skew * Math.PI) / 180)
    return [
      [id, line([x, 40.711], [x, 40.712]), { highway: 'secondary', oneway: 'yes', lanes: '4' }],
      [id + 1, line([x, 40.712], [x, 40.713]), { highway: 'secondary', oneway: 'yes', lanes: '4' }],
      [id + 2, line([x - 0.001, 40.712], [x, 40.712], [x + 0.001, 40.712]), { highway: 'residential', lanes: '2' }],
      [id + 3, line([x - 10 / 84300, 40.712 - (back + 10 * t) / 111050], [x + 10 / 84300, 40.712 - (back - 10 * t) / 111050]),
        { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
    ] as [number, string, Record<string, string>][]
  }),
]

// Painted areas, nodes on roads, and the roads they lie on (OSM wiki schemes).
const rect = (x: number, y: number, west: number, east: number, south: number, north: number) => {
  const [x0, x1, y0, y1] = [x + west / 84300, x + east / 84300, y + south / 111050, y + north / 111050]
  return `POLYGON((${x0} ${y0}, ${x1} ${y0}, ${x1} ${y1}, ${x0} ${y1}, ${x0} ${y0}))`
}
const hdWays: [number, string, Record<string, string>][] = [
  // A two-way left-turn lane.
  [200, line([-74.300, 40.712], [-74.300, 40.715]), { highway: 'secondary', lanes: '3', 'lanes:forward': '1', 'lanes:backward': '1',
    'lanes:both_ways': '1', 'turn:lanes:both_ways': 'left' }],
  // Lane changes barred: out of the left lane only, and across the left divider entirely.
  [210, line([-74.305, 40.712], [-74.305, 40.714]), { highway: 'primary', oneway: 'yes', lanes: '3', 'change:lanes': 'not_right|yes|yes' }],
  [211, line([-74.307, 40.712], [-74.307, 40.714]), { highway: 'primary', oneway: 'yes', lanes: '3', 'change:lanes': 'no|no|yes' }],
  // A buffered bike lane.
  [220, line([-74.310, 40.712], [-74.310, 40.714]), { highway: 'residential', lanes: '2', 'cycleway:right': 'lane', 'cycleway:right:buffer': 'yes' }],
  // E 7th St, Charlotte: a painted median down a two-way street.
  [230, line([-74.315, 40.712], [-74.315, 40.715]), { highway: 'secondary', lanes: '2' }],
  // A gore beside a one-way, and a painted island on a two-way street.
  [240, line([-74.320, 40.712], [-74.320, 40.715]), { highway: 'trunk', oneway: 'yes', lanes: '2' }],
  [250, line([-74.325, 40.712], [-74.325, 40.715]), { highway: 'tertiary', lanes: '2' }],
  [252, line([-74.3252, 40.7145], [-74.3248, 40.7145]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'dashes' }],
  // A minor road giving way at a T junction.
  [260, line([-74.330, 40.712], [-74.330, 40.7135]), { highway: 'tertiary', lanes: '2' }],
  [261, line([-74.331, 40.7135], [-74.330, 40.7135], [-74.329, 40.7135]), { highway: 'secondary', lanes: '2' }],
  // A speed hump.
  [270, line([-74.335, 40.712], [-74.335, 40.714]), { highway: 'residential' }],
  // Angled parking on the right, marked parallel parking on the left.
  [290, line([-74.340, 40.712], [-74.340, 40.714]), { highway: 'residential', lanes: '2', 'parking:right': 'lane',
    'parking:right:orientation': 'diagonal', 'parking:left': 'lane', 'parking:left:orientation': 'parallel', 'parking:left:markings': 'yes' }],
  // Barred areas beside left-kerb bike lanes: on a two-way street and in a
  // contraflow lane the traffic is on the rider's left; beside a with-flow lane
  // on a one-way, the rider's left is the kerb.
  [310, line([-74.345, 40.712], [-74.345, 40.714]), { highway: 'residential', lanes: '2', 'cycleway:left': 'lane', 'cycleway:left:marking:left': 'barred_area' }],
  [311, line([-74.347, 40.712], [-74.347, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '1', 'cycleway:left': 'opposite_lane', 'cycleway:left:marking:left': 'barred_area' }],
  [312, line([-74.349, 40.712], [-74.349, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '1', 'cycleway:left': 'lane', 'cycleway:left:marking:left': 'barred_area' }],
  // A long way with a buffered bike lane and angled parking across the cell
  // boundary at 40.718, crossed far south of it.
  [300, line([-74.0175, 40.705], [-74.0175, 40.728]), { highway: 'residential', lanes: '2', 'cycleway:right': 'lane',
    'cycleway:right:buffer': 'yes', 'parking:left': 'lane', 'parking:left:orientation': 'diagonal' }],
  [301, line([-74.0178, 40.710], [-74.0172, 40.710]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  // A painted median across the boundary of two cells at 40.718.
  [280, line([-74.0225, 40.716], [-74.0225, 40.720]), { highway: 'secondary', lanes: '2' }],
]
const hdFeatures: [string, string, Record<string, string>, string][] = [
  ['W231', rect(-74.315, 40.7130, -1.5, 1.5, 0, 66), { road_marking: 'restriction' }, 'area'],
  ['W241', rect(-74.320, 40.7130, 4, 9, 0, 66), { road_marking: 'restriction', pattern: 'chevron' }, 'area'],
  ['W251', rect(-74.325, 40.7130, -1.2, 1.2, 0, 40), { traffic_calming: 'painted_island' }, 'area'],
  ['W281', rect(-74.0225, 40.7175, -1.5, 1.5, 0, 111), { road_marking: 'restriction', pattern: 'stripes', colour: 'yellow' }, 'area'],
  ['N262', `POINT(-74.330 ${40.7135 - 15 / 111050})`, { highway: 'give_way', direction: 'forward' }, 'point'],
  ['N271', 'POINT(-74.335 40.713)', { traffic_calming: 'hump' }, 'point'],
  ['N272', 'POINT(-74.335 40.7135)', { highway: 'crossing', crossing: 'marked', 'crossing:markings': 'zebra', traffic_calming: 'table' }, 'point'],
]

// Every build drops the *_next tables by bare name too, scoped ones included.
const GUARD_NEXT = 'CREATE TABLE road_surfaces_next (); CREATE TABLE road_markings_next (); CREATE TABLE road_glyphs_next ();'

const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql

run('generate-road-markings.sql', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {} })
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA}; SET search_path TO ${SCHEMA}, public;
      CREATE TABLE geo_places (id text, osm_id bigint, tags jsonb NOT NULL, geom geometry(Geometry, 4326) NOT NULL, geom_type text NOT NULL,
        osm_type char(1) NOT NULL DEFAULT 'W');
      -- The script drops these by bare name; without them here the drop would reach public's.
      CREATE TABLE road_surfaces (); CREATE TABLE road_markings (); CREATE TABLE road_glyphs (); ${GUARD_NEXT}`)
    for (const [id, wkt, tags] of [...ways, ...hdWays]) {
      await sql`INSERT INTO geo_places VALUES (${'W' + id}, ${id}, ${sql.json(tags)}, ST_GeomFromText(${wkt}, 4326), 'line')`
    }
    for (const [id, wkt, tags, type] of hdFeatures) {
      await sql`INSERT INTO geo_places VALUES (${id}, ${Number(id.slice(1))}, ${sql.json(tags)}, ST_GeomFromText(${wkt}, 4326), ${type}, ${id[0]})`
    }
    await sql`INSERT INTO geo_places VALUES ('N8', 8, ${sql.json({ highway: 'traffic_signals' })}, ST_SetSRID(ST_MakePoint(${SB_X}, ${STREET_Y}), 4326), 'point', 'N')`
    await sql`INSERT INTO geo_places VALUES ('N433', 433, ${sql.json({ highway: 'traffic_signals' })}, ST_SetSRID(ST_MakePoint(-74.440, 40.712), 4326), 'point', 'N')`
    for (const [id, x] of [[444, -74.460], [454, -74.470]]) {
      await sql`INSERT INTO geo_places VALUES (${'N' + id}, ${id}, ${sql.json({ highway: 'traffic_signals' })}, ST_SetSRID(ST_MakePoint(${x}, 40.712), 4326), 'point', 'N')`
    }
    await sql`INSERT INTO geo_places VALUES ('N464', 464, ${sql.json({ highway: 'give_way', direction: 'forward' })},
      ST_SetSRID(ST_MakePoint(-74.480, ${40.712 - 25 / 111050}), 4326), 'point', 'N')`
    const turns: [string, number, number, Record<string, string>][] = [
      ['N80', -74.120, 40.713, { highway: 'turning_circle' }],
      ['N81', -74.125, 40.713, { highway: 'turning_circle', diameter: '30' }],
      ['N82', -74.130, 40.713, { highway: 'turning_loop' }],
      ['N83', -74.026, 40.718, { highway: 'turning_circle' }],
      ['N84', -74.135, 40.713, { highway: 'turning_loop' }],
      ['N85', -74.140, 40.713, { highway: 'turning_loop' }],
    ]
    for (const [id, x, y, tags] of turns) {
      await sql`INSERT INTO geo_places VALUES (${id}, ${Number(id.slice(1))}, ${sql.json(tags)}, ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326), 'point', 'N')`
    }
    // Under N84 a grass strip 3 m wide and 300 m long; under N85 a ring that crosses itself.
    const [sx, sy] = [-74.135, 40.713]
    await sql`INSERT INTO geo_places VALUES ('W86', 86, ${sql.json({ landuse: 'grass' })},
      ST_MakeEnvelope(${sx - 1.5 / 84300}, ${sy - 150 / 111050}, ${sx + 1.5 / 84300}, ${sy + 150 / 111050}, 4326), 'area')`
    const pt = (dx: number, dy: number) => `${east(-74.140, dx)} ${north(40.713, dy)}`
    await sql`INSERT INTO geo_places VALUES ('W87', 87, ${sql.json({ landuse: 'grass' })},
      ST_GeomFromText(${`POLYGON((${[[-6, -6], [6, -6], [6, 6], [-6, 6], [-6, 0], [-3, -9], [-6, -6]].map(([x, y]) => pt(x, y)).join(', ')}))`}, 4326), 'area')`
    // A route relation carrying a highway tag, stored as one line of its members.
    await sql`INSERT INTO geo_places VALUES ('R90', 90, ${sql.json({ type: 'route', route: 'road', highway: 'secondary' })},
      ST_GeomFromText(${RELATION}, 4326), 'line', 'R')`
    await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
  }, 60_000)

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; DROP SCHEMA IF EXISTS osm_replay CASCADE`)
    await sql?.end()
  })

  const covered = async (x: number, y: number) =>
    (await sql`SELECT EXISTS (SELECT 1 FROM road_surfaces WHERE kind IS NULL AND ST_Intersects(geom, ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326))) as hit`)[0].hit

  test('paves each carriageway but leaves the median between them open', async () => {
    expect(await covered(SB_X, 40.7105)).toBe(true)
    expect(await covered(NB_X, 40.7105)).toBe(true)
    expect(await covered((SB_X + NB_X) / 2, 40.7105)).toBe(false)
  })

  test('paves a service road only where its lanes are mapped', async () => {
    expect(await covered(-74.003, 40.713)).toBe(false)
    expect(await covered(-74.004, 40.713)).toBe(true)
  })

  test('paves the crossing street through the median', async () => {
    expect(await covered((SB_X + NB_X) / 2, STREET_Y)).toBe(true)
  })

  test('puts a stop line on the signalled approach only', async () => {
    const [{ sb, nb }] = await sql`
      SELECT count(*) FILTER (WHERE ST_Y(ST_Centroid(geom)) > ${STREET_Y + 0.00003} AND abs(ST_X(ST_Centroid(geom)) - ${SB_X}) < 0.0001)::int as sb,
             count(*) FILTER (WHERE ST_Y(ST_Centroid(geom)) < ${STREET_Y - 0.00003} AND abs(ST_X(ST_Centroid(geom)) - ${NB_X}) < 0.0001)::int as nb
      FROM road_markings WHERE kind = 'stop'`
    expect(sb).toBe(1)
    expect(nb).toBe(0)
  })

  test('draws the left-turn arrow in the lane on the driver\'s left', async () => {
    const arrows = await sql`SELECT ST_X(geom) as x FROM road_glyphs WHERE glyph = 'road-arrow-left' AND ST_X(geom) > -74.01`
    expect(arrows.length).toBeGreaterThan(0)
    // Southbound, the driver's left is east of the centreline.
    for (const a of arrows) expect(a.x).toBeGreaterThan(SB_X)
  })

  test('paints the tagged bus lane red at the kerb side', async () => {
    const bus = await sql`SELECT ST_X(ST_Centroid(geom)) as x, color FROM road_markings WHERE kind = 'bus_lane'`
    expect(bus.length).toBeGreaterThan(0)
    // Northbound, the third lane from the left is the eastmost.
    for (const b of bus) {
      expect(b.color).toBe('red')
      expect(b.x).toBeGreaterThan(NB_X)
    }
  })

  test('paints the bike lane green', async () => {
    const [{ n }] = await sql`SELECT count(*)::int as n FROM road_markings WHERE kind = 'bike_lane' AND color = 'green'`
    expect(n).toBeGreaterThan(0)
  })

  test('splits two-way streets with a double yellow unless passing is tagged', async () => {
    const rows = await sql`
      SELECT pattern, color, ST_X(ST_Centroid(geom)) < -74.0015 as passing FROM road_markings WHERE kind = 'centre'`
    expect(rows.filter(r => !r.passing).every(r => r.pattern === 'double' && r.color === 'yellow')).toBe(true)
    expect(rows.filter(r => r.passing).map(r => r.pattern)).toContain('dashed')
  })

  test('dashes lanes long on trunk roads and short on streets', async () => {
    const [{ trunk }] = await sql`SELECT count(*) FILTER (WHERE pattern = 'dashed_long')::int as trunk FROM road_markings WHERE kind = 'lane'`
    expect(trunk).toBeGreaterThan(0)
  })

  test('keeps every crosswalk bar and stop line on the carriageway', async () => {
    const [{ n, outside }] = await sql`
      SELECT count(*)::int as n,
             COALESCE(max(ST_Area(ST_Difference(m.geom, (SELECT ST_Union(geom) FROM road_surfaces))::geography)), 0) as outside
      FROM road_markings m WHERE m.kind IN ('crosswalk', 'stop')`
    expect(n).toBeGreaterThan(1)
    expect(outside).toBeLessThan(0.01)
  })

  // Metres east of a longitude near 40.71°N.
  const east = (x: number, m: number) => x + m / 84300

  test('widens a one-way for an added lane by easing both kerbs out from the join', async () => {
    expect(await covered(east(-74.010, -2.5), 40.7122)).toBe(true)
    expect(await covered(east(-74.010, -3.5), 40.7122)).toBe(false)
    // 5 m past the join the kerbs have barely moved; 90 m past, three lanes
    // of 3 m sit either side of the way.
    expect(await covered(east(-74.010, -2.9), 40.71255)).toBe(true)
    expect(await covered(east(-74.010, -3.4), 40.71255)).toBe(false)
    for (const m of [-4.2, 4.2]) expect(await covered(east(-74.010, m), 40.7133)).toBe(true)
    for (const m of [-4.8, 4.8]) expect(await covered(east(-74.010, m), 40.7133)).toBe(false)
  })

  test('runs the way after it down the middle of the road', async () => {
    expect(await covered(east(-74.010, -4), 40.7142)).toBe(true)
    expect(await covered(east(-74.010, -5), 40.7142)).toBe(false)
    expect(await covered(east(-74.010, 4), 40.7142)).toBe(true)
    expect(await covered(east(-74.010, 5), 40.7142)).toBe(false)
  })

  test('runs on without a jog at any join down a long chain', async () => {
    // Where the lane opens the kerbs have yet to move.
    for (const y of [40.71245, 40.71255]) {
      expect(await covered(east(-74.040, -2.5), y)).toBe(true)
      expect(await covered(east(-74.040, -3.5), y)).toBe(false)
    }
    for (let k = 1; k < 8; k++) {
      const join = 40.7125 + (k + 1) * 0.0009
      for (const y of [join - 0.00005, join + 0.00005]) {
        expect(await covered(east(-74.040, -4), y)).toBe(true)
        expect(await covered(east(-74.040, -5), y)).toBe(false)
        expect(await covered(east(-74.040, 4), y)).toBe(true)
        expect(await covered(east(-74.040, 5), y)).toBe(false)
      }
    }
  })

  // Metres north of a latitude.
  const ahead = (y: number, m: number) => y + m / 111050

  // The ramp's kerbs across a probe square to it at `y`, in metres east of its way.
  const RAMP_X = -74.015
  const rampKerbs = async (y: number) => (await sql`
    SELECT (ST_XMin(c) - ${RAMP_X}) * 84300 as l, (ST_XMax(c) - ${RAMP_X}) * 84300 as r
    FROM (SELECT ST_Intersection(ST_SetSRID(ST_MakeLine(ST_MakePoint(${east(RAMP_X, -20)}, ${y}), ST_MakePoint(${east(RAMP_X, 20)}, ${y})), 4326),
                                 ST_Union(geom)) as c
          FROM road_surfaces
          WHERE kind IS NULL AND geom && ST_MakeEnvelope(${east(RAMP_X, -20)}, ${y - 1e-6}, ${east(RAMP_X, 20)}, ${y + 1e-6}, 4326)) s`)[0]
  const rampProfile = async () => {
    const out: { l: number; r: number }[] = []
    for (let m = -10; m <= 180; m++) out.push(await rampKerbs(ahead(40.7172, m)))
    return out
  }

  describe('a ramp gaining a lane at each of several short ways', () => {
    let profile: { l: number; r: number }[] = []
    beforeAll(async () => {
      profile = await rampProfile()
    })

    test('widens all the way to its five lanes, each kerb only ever moving out', async () => {
      for (let i = 1; i < profile.length; i++) {
        expect(profile[i].l).toBeLessThan(profile[i - 1].l + 0.03)
        expect(profile[i].r).toBeGreaterThan(profile[i - 1].r - 0.03)
      }
      expect(profile[0].r - profile[0].l).toBeCloseTo(3.4, 0)
      expect(profile.at(-1)!.r - profile.at(-1)!.l).toBeCloseTo(17, 0)
    })

    test('draws each kerb as one smooth flare, not a step at each way', async () => {
      // A kerb whose lane adds each ease on their own steps out and levels off
      // at every join; as one flare it bends a little at a time.
      for (const side of ['l', 'r'] as const) {
        for (let i = 1; i < profile.length - 1; i++) {
          expect(Math.abs(profile[i + 1][side] - 2 * profile[i][side] + profile[i - 1][side])).toBeLessThan(0.06)
        }
      }
      const [{ sharpest }] = await kerbTurns(RAMP_X, ahead(40.7172, 60), 70)
      expect(sharpest).toBeLessThan(8)
    })

    test('runs its edge lines on unbroken where one way\'s taper carries into the next', async () => {
      const [{ worst, joins }] = await sql`
        WITH e AS (SELECT ST_StartPoint(geom) as s, ST_EndPoint(geom) as e, color FROM road_markings
                   WHERE kind = 'edge' AND geom && ${sql`ST_MakeEnvelope(${east(RAMP_X, -12)}, 40.7170, ${east(RAMP_X, 12)}, 40.7192, 4326)`})
        SELECT max(gap) as worst, count(*)::int as joins
        FROM (SELECT (SELECT min(ST_Distance(a.e::geography, b.s::geography)) FROM e b WHERE b.color = a.color AND b.s <> a.s) as gap
              FROM e a WHERE ST_Y(a.e) BETWEEN 40.7171 AND 40.7181) g`
      expect(joins).toBeGreaterThanOrEqual(6)
      expect(Number(worst)).toBeLessThan(0.05)
    })

    test('starts the line of each lane that opens where its taper ends', async () => {
      const dashes = await sql`
        SELECT ST_YMin(d.geom) as y0, ST_YMax(d.geom) as y1, (ST_X(ST_Centroid(d.geom)) - ${RAMP_X}) * 84300 as x
        FROM road_markings m, ST_Dump(m.geom) d
        WHERE m.kind = 'lane' AND m.geom && ST_MakeEnvelope(${east(RAMP_X, -12)}, 40.7172, ${east(RAMP_X, 12)}, 40.7190, 4326)`
      expect(dashes.length).toBeGreaterThan(20)
      for (const d of dashes) {
        expect((d.y1 - d.y0) * 111050).toBeGreaterThan(2.5)
        expect((d.y1 - d.y0) * 111050).toBeLessThan(3.5)
      }
      // No dash where the second lane is still opening, just past the first add.
      expect(dashes.filter(d => d.y0 < ahead(40.7172, 25)).length).toBe(0)
    })
  })

  test('runs the dashes on across a join without starting over', async () => {
    // The left divider of the chain of three-lane ways, 100 m each, past where it widened.
    const dashes = await sql`
      SELECT ST_YMin(d.geom) as y0, ST_YMax(d.geom) as y1
      FROM road_markings m, ST_Dump(m.geom) d
      WHERE m.kind = 'lane' AND d.geom && ST_MakeEnvelope(${east(-74.040, -2)}, 40.7136, ${east(-74.040, -1)}, 40.7204, 4326)
        AND ST_YMin(d.geom) > 40.7136 AND ST_YMax(d.geom) < 40.7204
      ORDER BY 1`
    expect(dashes.length).toBeGreaterThan(60)
    for (const [i, d] of dashes.entries()) {
      expect((d.y1 - d.y0) * 111050).toBeGreaterThan(2.9)
      expect((d.y1 - d.y0) * 111050).toBeLessThan(3.1)
      if (i > 0) {
        expect((d.y0 - dashes[i - 1].y1) * 111050).toBeGreaterThan(5.5)
        expect((d.y0 - dashes[i - 1].y1) * 111050).toBeLessThan(6.6)
      }
    }
  })
  // The lines of `kinds` crossing a probe from `w` to `e` metres east of `x`, at
  // `y`; or, given `along`, a box that far either side of it, which no dash
  // gap falls through.
  const crossing = async (x: number, y: number, w: number, e: number, kinds: string[], color?: string, along = 0) =>
    (await sql`SELECT count(*)::int as n FROM road_markings
      WHERE kind = ANY (${kinds}) AND (${color ?? null}::text IS NULL OR color = ${color ?? null})
        AND ST_Intersects(geom, CASE WHEN ${along}::float8 > 0 THEN ST_MakeEnvelope(${east(x, w)}, ${ahead(y, -along)}, ${east(x, e)}, ${ahead(y, along)}, 4326)
                                ELSE ST_SetSRID(ST_MakeLine(ST_MakePoint(${east(x, w)}, ${y}), ST_MakePoint(${east(x, e)}, ${y})), 4326) END)`)[0].n

  test('opens a bay with a taper even where a driveway meets the join', async () => {
    // Three lanes of 3.6 m end 5.4 m right of the way; four with a right bay end at 9.
    expect(await covered(east(-74.160, 7), ahead(40.7125, 3))).toBe(false)
    expect(await covered(east(-74.160, 8.5), ahead(40.7125, 120))).toBe(true)
  })

  test('lines two merging ways up with the kerbs of the road they become', async () => {
    // The four lanes run 6.6 m either side; 3 m short of the merge the one-lane
    // way holds the left kerb and the three-lane way the right.
    expect(await covered(east(-74.170, -6), ahead(40.713, -3))).toBe(true)
    expect(await covered(east(-74.170, 6.2), ahead(40.713, -3))).toBe(true)
    // Nothing paints green across a merge, as it would across a junction.
    const [{ g }] = await sql`SELECT count(*)::int as g FROM road_markings WHERE kind = 'bike_lane'
      AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-74.170, 40.713), 4326)::geography, 20)`
    expect(g).toBe(0)
  })

  test('peels an exit ramp off the right kerb, not across the through lanes', async () => {
    // Just past the split the ramp's yellow left edge stays out of the middle of the road.
    expect(await crossing(-74.180, ahead(40.713, 5), -3, 3, ['edge'], 'yellow')).toBe(0)
    // and the through lanes keep the left kerb: no yellow edge jumps inward.
    expect(await crossing(-74.180, ahead(40.713, 5), -7.4, -6.4, ['edge'], 'yellow')).toBe(1)
  })

  test('divides a two-way street into one-ways that each keep their own kerb', async () => {
    // Four lanes of 3.4 m: 6.8 m either side of the way at the split.
    expect(await covered(east(-74.190, -6.3), ahead(40.713, 4))).toBe(true)
    expect(await covered(east(-74.190, 6.3), ahead(40.713, 4))).toBe(true)
  })

  test('spreads a three-way split across the road it leaves', async () => {
    // Four lanes of 3.4 m, 6.8 m either side: the outer links take the kerbs.
    expect(await covered(east(-74.200, -6.4), ahead(40.713, 3))).toBe(true)
    expect(await covered(east(-74.200, 6.4), ahead(40.713, 3))).toBe(true)
  })

  test('ends lane lines at the crosswalks round a junction', async () => {
    // Between the crossing street and the crosswalk past it, inside the junction.
    const [{ n }] = await sql`SELECT count(*)::int as n FROM road_markings WHERE kind = 'lane' AND NOT bridge
      AND ST_Intersects(geom, ST_MakeEnvelope(${east(-74.210, -5)}, ${ahead(40.713, -14.6)}, ${east(-74.210, 5)}, ${ahead(40.713, -5.4)}, 4326))`
    expect(n).toBe(0)
    // and past the crosswalk they run again.
    expect(await crossing(-74.210, ahead(40.713, -35), -5, 5, ['lane'], undefined, 4.6)).toBe(2)
  })

  test('runs a bridge\'s lines on over a junction below it', async () => {
    const [{ n }] = await sql`SELECT count(*)::int as n FROM road_markings WHERE kind = 'lane' AND bridge
      AND ST_Intersects(geom, ST_MakeEnvelope(-74.2102, 40.7128, -74.2100, 40.7130, 4326))`
    expect(n).toBe(2)
  })

  test('keeps the arrows of a merging way in its lanes as they ease over', async () => {
    const arrows = await sql`
      SELECT (SELECT min(ST_Distance(g.geom::geography, m.geom::geography)) FROM road_markings m
              WHERE m.kind = 'lane' AND m.geom && ST_Expand(g.geom, 0.0002)) as gap
      FROM road_glyphs g WHERE g.glyph LIKE 'road-arrow%' AND g.geom && ST_MakeEnvelope(-74.1705, 40.7115, -74.1695, 40.713, 4326)`
    expect(arrows.length).toBe(6)
    for (const a of arrows) expect(Number(a.gap)).toBeGreaterThan(1)
  })

  test('ends lane lines at the stop line where the approach has no crosswalk', async () => {
    // The widest street crossing the southbound avenue is 7.6 m: its stop line
    // stands 5.3 m back, past the crossing carriageway's own 4.8 m.
    expect(await crossing(SB_X, ahead(STREET_Y, 5), -6, 6, ['lane'])).toBe(0)
    expect(await crossing(SB_X, ahead(STREET_Y, 12), -6, 6, ['lane'], undefined, 4.6)).toBeGreaterThan(0)
  })

  test('does not stack two crosswalks mapped over each other', async () => {
    const [{ overlap }] = await sql`
      SELECT COALESCE(sum(ST_Area(ST_Intersection(a.geom, b.geom)::geography)), 0) as overlap
      FROM road_markings a JOIN road_markings b ON a.fid < b.fid AND a.geom && b.geom
      WHERE a.kind = 'crosswalk' AND b.kind = 'crosswalk'
        AND ST_DWithin(a.geom::geography, ST_SetSRID(ST_MakePoint(-74.220, 40.713), 4326)::geography, 15)`
    expect(Number(overlap)).toBeLessThan(0.05)
  })

  test('builds with a skewed crossing too short for one bar', async () => {
    const [{ n }] = await sql`SELECT count(*)::int as n FROM road_surfaces`
    expect(n).toBeGreaterThan(0)
  })

  test('gives back no band, rather than failing, for a line it cannot taper', async () => {
    const [{ band }] = await sql`SELECT road_profile_band('POINT(0 0)'::geometry, 1, 10, 1, 0, 3, 2, 10, 0, 0, 0, 0, 1, 5) as band`
    expect(band).toBeNull()
  })

  test('lines a way up with the through lanes of a left-bay approach before it', async () => {
    for (const [m, hit] of [[2.5, true], [3.5, false], [-2.5, true], [-3.5, false]] as const) {
      expect(await covered(east(-74.070, m), 40.7135)).toBe(hit)
    }
  })

  test('paints bike lanes across a crossroads in both directions of one street', async () => {
    for (const side of [1, -1]) {
      const [{ n }] = await sql`
        SELECT count(*)::int as n FROM road_markings
        WHERE kind IN ('bike_lane', 'bike') AND ST_DWithin(geom::geography,
          ST_SetSRID(ST_MakePoint(-74.079, ${40.7145 + (side * 3.8) / 111000}), 4326)::geography, 3)`
      expect(n).toBeGreaterThan(0)
    }
  })

  test('lays a way tagged placement=right_of:2 along its right kerb', async () => {
    expect(await covered(east(-74.012, -5), 40.7125)).toBe(true)
    expect(await covered(east(-74.012, 1), 40.7125)).toBe(false)
  })

  test('carries a bike lane across a junction to the lane beyond it', async () => {
    // Eastbound, the lane centre is 3.8 m right of the way.
    const [{ fill, edges }] = await sql`
      SELECT count(*) FILTER (WHERE kind = 'bike_lane')::int as fill,
             count(*) FILTER (WHERE kind = 'bike' AND pattern = 'dashed')::int as edges
      FROM road_markings
      WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-74.007, 40.7145 - 3.8 / 111000), 4326)::geography, 3)`
    expect(fill).toBeGreaterThan(0)
    expect(edges).toBeGreaterThan(0)
  })

  test('runs crosswalk bars with the traffic on the road they cross', async () => {
    const bars = await sql`
      SELECT (ST_XMax(b.geom) - ST_XMin(b.geom)) * 84300 as dx, (ST_YMax(b.geom) - ST_YMin(b.geom)) * 111000 as dy
      FROM road_markings m, ST_Dump(m.geom) b
      WHERE m.kind = 'crosswalk' AND ST_DWithin(m.geom::geography, ST_SetSRID(ST_MakePoint(-74.0005, ${STREET_Y}), 4326)::geography, 8)`
    expect(bars.length).toBeGreaterThan(3)
    // The street runs east-west and the crossing is drawn askew across it.
    for (const b of bars) expect(b.dx).toBeGreaterThan(2 * b.dy)
  })

  test('runs a street out to the kerbs its mapped sidewalks stand back from', async () => {
    // Sidewalks 7 m out, their middles 2.25 m from the kerb.
    for (const m of [-4.5, 4.5]) expect(await covered(east(-74.400, m), 40.713)).toBe(true)
    for (const m of [-5.0, 5.0]) expect(await covered(east(-74.400, m), 40.713)).toBe(false)
    // A tagged width wins, and a cycle track between street and sidewalk
    // leaves that side unread, so the lane keeps its own width.
    expect(await covered(east(-74.406, 3.3), 40.713)).toBe(false)
    expect(await covered(east(-74.409, 2), 40.713)).toBe(false)
    expect(await covered(east(-74.409, -2), 40.713)).toBe(false)
  })

  test('keeps its lanes their width in a street widened to its kerbs', async () => {
    // Three 3 m lanes down the middle of 13.5 m: dividers 1.5 m either side.
    const lines = await sql`
      SELECT (ST_X(ST_Centroid(geom)) - -74.403) * 84300 as dx FROM road_markings
      WHERE kind = 'lane' AND ST_Intersects(geom, ST_MakeEnvelope(-74.4032, 40.7125, -74.4028, 40.7135, 4326))`
    expect(lines.length).toBeGreaterThan(1)
    for (const l of lines) expect(Math.abs(Math.abs(l.dx) - 1.5)).toBeLessThan(0.2)
  })

  test('rounds the nose of a narrow median rather than drawing it to a point', async () => {
    // Cross street kerb 3 m north of its line; the median runs 3.4 to 7.4 m
    // east of the southbound way.
    const x = (m: number) => east(-74.420, m)
    expect(await covered(x(5.4), north(40.712, 3.3))).toBe(false)
    expect(await covered(x(3.9), north(40.712, 4.0))).toBe(false)
    expect(await covered(x(6.9), north(40.712, 4.0))).toBe(false)
    expect(await covered(x(5.4), north(40.712, 10))).toBe(false)
    // The corners on the far kerbs keep their full radius.
    expect(await covered(x(-3.9), north(40.712, 3.4))).toBe(true)
  })

  test('stands a stop line clear of an askew crosswalk across all its lanes', async () => {
    const [{ stops, gap }] = await sql`
      SELECT count(*)::int as stops, min(ST_Distance(s.geom::geography, c.geom::geography)) as gap
      FROM road_markings s, road_markings c
      WHERE s.kind = 'stop' AND c.kind = 'crosswalk'
        AND ST_Intersects(s.geom, ST_MakeEnvelope(-74.4405, 40.7118, -74.4400, 40.7122, 4326))
        AND ST_Intersects(c.geom, ST_MakeEnvelope(-74.4405, 40.7118, -74.4400, 40.7122, 4326))`
    expect(stops).toBeGreaterThan(0)
    expect(gap).toBeGreaterThan(0.3)
  })

  // The stop line south of the crossroads at `x`, its parts, the crosswalk
  // past it, and what lies between the two.
  const skewedStop = async (x: number) => (await sql`
    WITH box AS (SELECT ST_MakeEnvelope(${east(x, -12)}, ${north(40.712, -45)}, ${east(x, 12)}, ${north(40.712, -4)}, 4326) as b),
    stop AS (SELECT geom FROM road_markings, box WHERE kind = 'stop' AND ST_Intersects(geom, box.b)),
    walk AS (SELECT ST_Union(geom) as geom FROM road_markings, box WHERE kind = 'crosswalk' AND ST_Intersects(geom, box.b))
    SELECT (SELECT count(*)::int FROM stop) as stops,
           (SELECT ST_NumGeometries(geom) FROM stop LIMIT 1) as parts,
           (SELECT bool_or(ST_Intersects(s.geom, w.geom)) FROM stop s, walk w) as overlaps,
           (SELECT min(ST_Distance(p.geom::geography, w.geom::geography)) FROM stop s, walk w, ST_Dump(s.geom) p) as gap,
           (SELECT max(ST_Distance(p.geom::geography, w.geom::geography)) FROM stop s, walk w, ST_Dump(s.geom) p) as widest,
           -- Each part's long side, in degrees east of north.
           (SELECT array_agg(degrees(ST_Azimuth(ST_PointN(r, 1), ST_PointN(r, 2)))::numeric(6, 1) ORDER BY p.path)
            FROM stop s, ST_Dump(s.geom) p,
                 LATERAL (SELECT ST_ExteriorRing(ST_OrientedEnvelope(ST_Transform(p.geom, 3857))) as e) o,
                 LATERAL (SELECT CASE WHEN ST_Length(ST_MakeLine(ST_PointN(o.e, 1), ST_PointN(o.e, 2))) > ST_Length(ST_MakeLine(ST_PointN(o.e, 2), ST_PointN(o.e, 3)))
                                      THEN ST_MakeLine(ST_PointN(o.e, 1), ST_PointN(o.e, 2)) ELSE ST_MakeLine(ST_PointN(o.e, 2), ST_PointN(o.e, 3)) END as r) l) as sides,
           (SELECT count(*)::int FROM road_markings m, stop s, walk w
            WHERE m.kind IN ('lane', 'centre', 'edge') AND m.geom && ST_Expand(w.geom, 0.0003)
              AND ST_Intersects(m.geom, ST_Difference(ST_ConvexHull(ST_Collect(s.geom, w.geom)), ST_Buffer(s.geom::geography, 0.05)::geometry))) as lines_past,
           (SELECT min(ST_Distance(m.geom::geography, w.geom::geography)) FROM road_markings m, walk w
            WHERE m.kind IN ('lane', 'centre', 'edge') AND m.geom && ST_Expand(w.geom, 0.0003)) as lines_gap
  `)[0]
  // Azimuth of a line, folded into 0..180.
  const fold = (deg: number) => ((deg % 180) + 180) % 180

  test('slants a stop line parallel to a crosswalk 20° askew, 1.2 m short of it', async () => {
    const r = await skewedStop(-74.460)
    expect(r.stops).toBe(1)
    expect(r.parts).toBe(1)
    expect(r.overlaps).toBe(false)
    expect(r.gap).toBeGreaterThan(1.0)
    expect(r.gap).toBeLessThan(1.4)
    // Parallel to the crosswalk all the way across: as close at both ends.
    expect(r.widest - r.gap).toBeLessThan(0.3)
    // The crosswalk runs 20° north of east.
    expect(Math.abs(fold(Number(r.sides[0])) - 70)).toBeLessThan(2)
    expect(r.lines_past).toBe(0)
  })

  test('steps square stop lines lane by lane along a crosswalk 50° askew', async () => {
    const r = await skewedStop(-74.470)
    expect(r.stops).toBe(1)
    expect(r.parts).toBe(4)
    expect(r.overlaps).toBe(false)
    expect(r.gap).toBeGreaterThan(1.0)
    expect(r.gap).toBeLessThan(1.4)
    for (const side of r.sides) expect(Math.abs(fold(Number(side)) - 90)).toBeLessThan(2)
    // A line between two lanes runs on to the stop line further forward.
    expect(r.lines_gap).toBeGreaterThan(1.0)
  })

  test('lines yield triangles up along a crosswalk 20° askew, clear of it', async () => {
    const [r] = await sql`
      WITH t AS (SELECT p.geom FROM road_markings m, ST_Dump(m.geom) p
                 WHERE m.kind = 'yield' AND m.geom && ST_MakeEnvelope(${east(-74.480, -12)}, 40.7116, ${east(-74.480, 12)}, 40.7120, 4326)),
      walk AS (SELECT ST_Union(geom) as geom FROM road_markings WHERE kind = 'crosswalk' AND geom && ST_MakeEnvelope(${east(-74.480, -12)}, 40.7116, ${east(-74.480, 12)}, 40.7120, 4326))
      SELECT count(*)::int as n, bool_or(ST_Intersects(t.geom, w.geom)) as overlaps, min(ST_Distance(t.geom::geography, w.geom::geography)) as gap,
             max(ST_Distance(t.geom::geography, w.geom::geography)) as widest
      FROM t, walk w`
    expect(r.n).toBeGreaterThanOrEqual(12)
    expect(r.overlaps).toBe(false)
    expect(r.gap).toBeGreaterThan(1.0)
    expect(r.widest - r.gap).toBeLessThan(0.5)
  })

  test('keeps lane lines out of the crossing carriageway', async () => {
    const [{ n }] = await sql`
      SELECT count(*)::int as n FROM road_markings
      WHERE kind IN ('lane', 'centre') AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${SB_X}, ${STREET_Y}), 4326)::geography, 3)`
    expect(n).toBe(0)
  })

  test('rounds a bend mapped as a sharp corner', async () => {
    // Just outside the corner the two legs make, and inside it.
    expect(await covered(east(-74.0900, 1.5), 40.7140 - 1.5 / 111000)).toBe(false)
    expect(await covered(east(-74.0900, -4), 40.7140 + 4 / 111000)).toBe(true)
  })

  test('smooths a way between the vertices it is pinned at, and keeps those', async () => {
    const [{ ends, pinned, corner, bend }] = await sql`
      WITH l AS (SELECT 'LINESTRING(0 0, 100 0, 100 100, 200 100)'::geometry as g),
           s AS (SELECT road_smooth(g, 'MULTIPOINT((100 100))'::geometry, 20) as g FROM l)
      SELECT ST_Equals(ST_StartPoint(s.g), 'POINT(0 0)') AND ST_Equals(ST_EndPoint(s.g), 'POINT(200 100)') as ends,
             ST_Intersects(s.g, 'POINT(100 100)'::geometry) as pinned,
             ST_Distance(s.g, 'POINT(100 0)'::geometry) as corner,
             ST_Distance(s.g, 'POINT(50 0)'::geometry) as bend
      FROM s`
    expect(ends).toBe(true)
    expect(pinned).toBe(true)
    expect(corner).toBeGreaterThan(2)
    expect(corner).toBeLessThan(5)
    expect(bend).toBeLessThan(0.01)
  })

  test('leaves a smoothed line as it is unless it is one line with room to bend', async () => {
    const [{ multi, tiny }] = await sql`
      SELECT ST_Equals(road_smooth(${RELATION}::geometry, NULL, 20), ${RELATION}::geometry) as multi,
             ST_NPoints(road_smooth('LINESTRING(0 0, 0.001 0, 0 0)'::geometry, NULL, 20)) >= 2 as tiny`
    expect(multi).toBe(true)
    expect(tiny).toBe(true)
  })

  test('draws ways only, not a route relation over them', async () => {
    expect(await covered(-74.100, 40.7125)).toBe(false)
    expect(await covered(-74.101, 40.7145)).toBe(false)
  })

  test('draws kerb corners as arcs, not a few facets', async () => {
    // Turning angles along the kerbs within 12 m of a crossroads.
    const [{ sharpest }] = await sql`
      WITH ring AS (
        SELECT fid, d.geom as p, d.path
        FROM road_surfaces s, ST_DumpPoints(ST_Boundary(ST_Transform(s.geom, 3857))) d
        WHERE ST_DWithin(s.geom::geography, ST_SetSRID(ST_MakePoint(-74.079, 40.7145), 4326)::geography, 40)
      ), turns AS (
        SELECT p, degrees(abs(atan2(sin(a2 - a1), cos(a2 - a1)))) as turn
        FROM (SELECT p, ST_Azimuth(lag(p) OVER w, p) as a1, ST_Azimuth(p, lead(p) OVER w) as a2 FROM ring
              WINDOW w AS (PARTITION BY fid, path[1:array_length(path, 1) - 1] ORDER BY path[array_length(path, 1)])) t
        WHERE a1 IS NOT NULL AND a2 IS NOT NULL
      )
      SELECT max(turn) as sharpest FROM turns
      WHERE ST_DWithin(ST_Transform(p, 4326)::geography, ST_SetSRID(ST_MakePoint(-74.079, 40.7145), 4326)::geography, 12)`
    expect(sharpest).toBeLessThan(18)
  })

  // Metres north of a latitude near 40.71°N.
  const north = (y: number, m: number) => y + m / 111050
  const kerbTurns = (x: number, y: number, within: number) => sql`
    WITH ring AS (
      SELECT d.geom as p, d.path
      FROM (SELECT ST_Union(ST_Transform(geom, 3857)) as g FROM road_surfaces
            WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326)::geography, ${within + 20})) s,
           ST_DumpPoints(ST_Boundary(s.g)) d
    ), turns AS (
      SELECT p, degrees(abs(atan2(sin(a2 - a1), cos(a2 - a1)))) as turn
      FROM (SELECT p, ST_Azimuth(lag(p) OVER w, p) as a1, ST_Azimuth(p, lead(p) OVER w) as a2 FROM ring
            WINDOW w AS (PARTITION BY path[1:array_length(path, 1) - 1] ORDER BY path[array_length(path, 1)])) t
      WHERE a1 IS NOT NULL AND a2 IS NOT NULL
    )
    SELECT max(turn) as sharpest, count(*)::int as n FROM turns
    WHERE ST_DWithin(ST_Transform(p, 4326)::geography, ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326)::geography, ${within})`

  describe('a turning circle', () => {
    const [X, Y] = [-74.120, 40.713]

    test('paves a round bulb 11 m across the end of the street', async () => {
      for (const [dx, dy] of [[10.5, 0], [-10.5, 0], [0, 10.5], [7.3, 7.3], [-7.3, 7.3]]) {
        expect(await covered(east(X, dx), north(Y, dy))).toBe(true)
      }
      for (const [dx, dy] of [[11.6, 0], [-11.6, 0], [0, 11.6], [8.3, 8.3]]) {
        expect(await covered(east(X, dx), north(Y, dy))).toBe(false)
      }
    })

    test('draws its kerb as one smooth curve, flaring into the street without a notch', async () => {
      // Where the street's kerb meets the circle, and just outside both.
      expect(await covered(east(X, 3.5), north(Y, -11))).toBe(true)
      expect(await covered(east(X, -3.5), north(Y, -11))).toBe(true)
      expect(await covered(east(X, 3.5), north(Y, -30))).toBe(false)
      const [{ sharpest, n }] = await kerbTurns(X, Y, 25)
      expect(n).toBeGreaterThan(40)
      expect(sharpest).toBeLessThan(12)
    })

    test('takes its size from a tagged diameter', async () => {
      expect(await covered(east(-74.125, 14.5), 40.713)).toBe(true)
      expect(await covered(east(-74.125, 15.5), 40.713)).toBe(false)
    })

    test('ends the lane lines before it, and puts no stop line or arrow in it', async () => {
      const [{ inside, before }] = await sql`
        SELECT count(*) FILTER (WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${X}, ${Y}), 4326)::geography, 12))::int as inside,
               count(*) FILTER (WHERE kind = 'centre' AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${X}, ${north(Y, -40)}), 4326)::geography, 1))::int as before
        FROM road_markings WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${X}, ${Y}), 4326)::geography, 60)`
      expect(before).toBe(1)
      expect(inside).toBe(0)
      const [{ glyphs }] = await sql`
        SELECT count(*)::int as glyphs FROM road_glyphs
        WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${X}, ${Y}), 4326)::geography, 12)`
      expect(glyphs).toBe(0)
    })

    test('leaves a turning loop\'s island unpaved, and marks it to be drawn over the road running into it', async () => {
      const [{ island }] = await sql`
        SELECT ST_Area(geom::geography) as island FROM road_surfaces
        WHERE kind = 'island' AND ST_Intersects(geom, ST_SetSRID(ST_MakePoint(-74.130, 40.713), 4326))`
      expect(island).toBeGreaterThan(Math.PI * 8 * 8)
      expect(island).toBeLessThan(Math.PI * 9 * 9)
      expect(await covered(-74.130, 40.713)).toBe(false)
      expect(await covered(east(-74.130, 6), 40.713)).toBe(false)
      expect(await covered(east(-74.130, 11), 40.713)).toBe(true)
      expect(await covered(-74.130, north(40.713, 11))).toBe(true)
      expect(await covered(east(-74.130, 15), 40.713)).toBe(false)
    })
  })

  test('reads a long strip under a turning loop as no island, rather than paving round it', async () => {
    const [{ island }] = await sql`
      SELECT ST_Area(geom::geography) as island FROM road_surfaces
      WHERE kind = 'island' AND ST_Intersects(geom, ST_SetSRID(ST_MakePoint(-74.135, 40.713), 4326))`
    expect(island).toBeLessThan(Math.PI * 9 * 9)
    expect(await covered(-74.135, north(40.713, 40))).toBe(false)
  })

  test('builds round an island mapped as an invalid polygon', async () => {
    expect(await covered(east(-74.140, 11), 40.713)).toBe(true)
  })

  describe('paint from the OSM wiki\'s lane and marking tags', () => {
    // Rows within `m` metres of a point.
    const by = (x: number, y: number, m: number) => sql`ST_Expand(ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326), ${m / 84300})`

    test('edges a two-way left-turn lane with solid yellow outside and dashed yellow inside', async () => {
      const rows = await sql`
        SELECT pattern, color, abs(ST_X(ST_Centroid(geom)) + 74.300) * 84300 as off
        FROM road_markings WHERE kind = 'centre' AND geom && ${by(-74.300, 40.7135, 50)}`
      expect(rows.every(r => r.color === 'yellow')).toBe(true)
      expect(rows.some(r => r.pattern === 'double')).toBe(false)
      const solid = rows.filter(r => r.pattern === 'solid').map(r => r.off)
      const dashed = rows.filter(r => r.pattern === 'dashed').map(r => r.off)
      expect(solid.length).toBeGreaterThanOrEqual(2)
      expect(dashed.length).toBeGreaterThanOrEqual(2)
      expect(Math.min(...solid)).toBeGreaterThan(Math.max(...dashed))
    })

    test('puts left arrows for both directions down its middle', async () => {
      const arrows = await sql`
        SELECT direction FROM road_glyphs WHERE glyph = 'road-arrow-left'
          AND geom && ST_MakeEnvelope(${-74.300 - 1 / 84300}, 40.712, ${-74.300 + 1 / 84300}, 40.715, 4326)`
      expect(arrows.map(a => a.direction)).toContain(0)
      expect(arrows.map(a => a.direction)).toContain(180)
    })

    const lanes = (x: number) => sql`
      SELECT pattern, (ST_X(ST_Centroid(geom)) - ${x}) * 84300 as off
      FROM road_markings WHERE kind = 'lane' AND geom && ${by(x, 40.713, 20)} ORDER BY off`

    test('draws a solid line beside the dashed one where only the left lane may not change', async () => {
      const rows = await lanes(-74.305)
      expect(rows.map(r => r.pattern)).toEqual(['solid', 'dashed', 'dashed'])
      // Northbound, the left lane is west: its solid line lies west of the dashed one beside it.
      expect(rows[1].off - rows[0].off).toBeCloseTo(0.2, 1)
    })

    test('makes a lane line solid where neither lane may change across it', async () => {
      expect((await lanes(-74.307)).map(r => r.pattern)).toEqual(['solid', 'solid', 'dashed'])
    })

    test('hatches a bike lane\'s buffer', async () => {
      const [{ n, x }] = await sql`
        SELECT sum(ST_NumGeometries(geom))::int as n, avg(ST_X(ST_Centroid(geom))) as x
        FROM road_markings WHERE kind = 'hatch' AND geom && ${by(-74.310, 40.713, 20)}`
      expect(n).toBeGreaterThan(30)
      expect(x).toBeGreaterThan(-74.310)
    })

    test('hatches a painted median on a two-way street in yellow, and ends the centre line at it', async () => {
      const [{ hatch, outline }] = await sql`
        SELECT count(*) FILTER (WHERE kind = 'hatch' AND color = 'yellow')::int as hatch,
               count(*) FILTER (WHERE kind = 'restriction' AND color = 'yellow' AND pattern = 'solid')::int as outline
        FROM road_markings WHERE geom && ${by(-74.315, 40.7133, 40)}`
      expect(hatch).toBeGreaterThan(0)
      expect(outline).toBeGreaterThan(0)
      const [{ inside }] = await sql`
        SELECT COALESCE(sum(ST_Length(ST_Intersection(m.geom, ST_Buffer(a.geom, -0.00001))::geography)), 0) as inside
        FROM road_markings m, geo_places a WHERE a.id = 'W231' AND m.kind = 'centre' AND m.geom && a.geom`
      expect(inside).toBeLessThan(0.5)
    })

    test('paves a gore beside a one-way and fills it with white chevrons pointing at the traffic', async () => {
      expect(await covered(-74.320 + 7 / 84300, 40.7133)).toBe(true)
      const bars = await sql`
        SELECT color, (d).geom as g FROM (SELECT color, ST_Dump(geom) as d FROM road_markings
          WHERE kind = 'hatch' AND geom && ${by(-74.320 + 6.5 / 84300, 40.7133, 10)}) t`
      expect(bars.length).toBeGreaterThan(0)
      expect(bars.every(b => b.color === 'white')).toBe(true)
      // A whole chevron is a V: its middle vertex is its southernmost, toward the northbound traffic.
      const [{ apex }] = await sql`
        SELECT count(*) FILTER (WHERE ST_NPoints(g) = 3 AND ST_Y(ST_PointN(g, 2)) < least(ST_Y(ST_PointN(g, 1)), ST_Y(ST_PointN(g, 3))))::int as apex
        FROM (SELECT (ST_Dump(geom)).geom as g FROM road_markings WHERE kind = 'hatch' AND geom && ${by(-74.320 + 6.5 / 84300, 40.7133, 10)}) t`
      expect(apex).toBeGreaterThan(0)
    })

    test('reads a traffic_calming=painted_island area as a painted median', async () => {
      const [{ n }] = await sql`
        SELECT count(*)::int as n FROM road_markings WHERE kind = 'hatch' AND color = 'yellow' AND geom && ${by(-74.325, 40.7132, 25)}`
      expect(n).toBeGreaterThan(0)
    })

    test('paints a yield line of triangles, not a stop line, where the minor road gives way', async () => {
      const rows = await sql`
        SELECT kind, ST_NumGeometries(geom) as n FROM road_markings
        WHERE kind IN ('yield', 'stop') AND geom && ${by(-74.330, 40.7134, 15)}`
      expect(rows.map(r => r.kind)).toEqual(['yield'])
      expect(rows[0].n).toBeGreaterThanOrEqual(4)
    })

    test('reads a barred area as a buffer only on the side of the rider\'s traffic', async () => {
      const hatched = async (x: number) => (await sql`
        SELECT count(*)::int as n FROM road_markings WHERE kind = 'hatch' AND geom && ${by(x, 40.713, 8)}`)[0].n > 0
      expect(await hatched(-74.345)).toBe(true)
      expect(await hatched(-74.347)).toBe(true)
      expect(await hatched(-74.349)).toBe(false)
    })

    test('angles parking stalls so a car pulls in forward', async () => {
      const bars = await sql`
        SELECT ST_X(ST_StartPoint(g)) as x0, ST_Y(ST_StartPoint(g)) as y0, ST_X(ST_EndPoint(g)) as x1, ST_Y(ST_EndPoint(g)) as y1
        FROM (SELECT (ST_Dump(geom)).geom as g FROM road_markings WHERE kind = 'parking' AND geom && ${by(-74.340, 40.713, 20)}) t
        WHERE ST_X(ST_Centroid(g)) > -74.340`
      expect(bars.length).toBeGreaterThan(30)
      // Northbound on the right: each stall's traffic end (west) lies south of its kerb end.
      for (const b of bars) expect(b.x0 < b.x1 ? b.y0 < b.y1 : b.y1 < b.y0).toBe(true)
    })

    test('lines a marked parallel parking lane along the traffic side', async () => {
      const [{ longest }] = await sql`
        SELECT max(ST_Length(g::geography)) as longest
        FROM (SELECT (ST_Dump(geom)).geom as g FROM road_markings WHERE kind = 'parking' AND geom && ${by(-74.340, 40.713, 20)}) t
        WHERE ST_X(ST_Centroid(g)) < -74.340`
      expect(longest).toBeGreaterThan(100)
    })

    test('breaks the edge lines of a crossing:markings=dashes crosswalk into dashes', async () => {
      const rows = await sql`
        SELECT style, ST_NumGeometries(geom) as n FROM road_markings WHERE kind = 'crosswalk' AND geom && ${by(-74.325, 40.7145, 6)}`
      expect(rows.map(r => r.style)).toEqual(['dashes'])
      // Two edges of a 7 m road, a 0.6 m dash every 1.2 m.
      expect(rows[0].n).toBeGreaterThanOrEqual(8)
    })

    test('leaves a raised crosswalk its crosswalk, without hump chevrons', async () => {
      const kinds = (await sql`SELECT DISTINCT kind FROM road_markings WHERE geom && ${by(-74.335, 40.7135, 4)}`).map(r => r.kind)
      expect(kinds).toContain('crosswalk')
      expect(kinds).not.toContain('hump')
    })

    test('marks a speed hump with a chevron in each lane, on the carriageway', async () => {
      const rows = await sql`
        SELECT ST_NumGeometries(geom) as n,
               ST_Area(ST_Difference(geom, (SELECT ST_Union(geom) FROM road_surfaces WHERE geom && ${by(-74.335, 40.713, 20)}))::geography) as outside
        FROM road_markings WHERE kind = 'hump' AND geom && ${by(-74.335, 40.713, 5)}`
      expect(rows).toHaveLength(1)
      expect(rows[0].n).toBe(2)
      expect(rows[0].outside).toBeLessThan(0.01)
    })
  })

  describe('rebuilding one box', () => {
    const BOX = (sql: postgres.Sql) => sql`ST_MakeEnvelope(-74.0003, 40.7095, -73.9985, 40.7105, 4326)`
    const footprint = async () => (await sql`
      SELECT ST_Area(ST_Union(geom)::geography) as union_m2, sum(ST_Area(geom::geography)) as sum_m2 FROM road_surfaces`)[0]
    let before: any

    beforeAll(async () => {
      before = await footprint()
      // As on a database last built before islands.
      await sql`ALTER TABLE road_surfaces DROP COLUMN kind`
      await sql.unsafe(GUARD_NEXT)
      await sql`TRUNCATE _rm_scope`
      await sql`INSERT INTO _rm_scope VALUES (${BOX(sql)})`
      await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
    }, 60_000)

    test('leaves the road surface whole, with nothing doubled at the box edge', async () => {
      const after = await footprint()
      expect(Math.abs(after.union_m2 - before.union_m2) / before.union_m2).toBeLessThan(0.01)
      expect(Math.abs(after.sum_m2 - after.union_m2) / after.union_m2).toBeLessThan(0.01)
    })

    test('gives live tables built before islands their column', async () => {
      const [{ ok }] = await sql`SELECT to_regclass('road_surfaces') IS NOT NULL AND EXISTS (
        SELECT 1 FROM pg_attribute WHERE attrelid = 'road_surfaces'::regclass AND attname = 'kind') as ok`
      expect(ok).toBe(true)
    })

    test('keeps the paint inside and outside the box', async () => {
      const [{ inside, outside }] = await sql`
        SELECT count(*) FILTER (WHERE ST_CoveredBy(geom, ${BOX(sql)}))::int as inside,
               count(*) FILTER (WHERE NOT ST_Intersects(geom, ${BOX(sql)}))::int as outside
        FROM road_markings`
      expect(inside).toBeGreaterThan(0)
      expect(outside).toBeGreaterThan(0)
    })
  })

  describe('a diff upstream of a cell boundary', () => {
    let queued = 0
    let ownTest = false
    beforeAll(async () => {
      await sql`UPDATE geo_places SET tags = tags || '{"lanes": "1"}' WHERE id = 'W50'`
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'W'::char(1) as osm_type, osm_id, NULL::text as name, '{highway/residential}'::text[] as categories, geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'W50';
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id = 'W50';`)
      await sql.unsafe(readFileSync(join(import.meta.dir, 'detail-queue-table.sql'), 'utf8'))
      await sql`DELETE FROM detail_dirty`
      // As on a database last built before the queue's road test existed.
      await sql`DROP FUNCTION road_is_marked_way(jsonb)`
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
      queued = (await sql`SELECT count(*)::int as n FROM detail_dirty`)[0].n
      ownTest = (await sql`SELECT to_regprocedure(${SCHEMA + '.road_is_marked_way(jsonb)'}) IS NOT NULL as ok`)[0].ok
      const out = await sql.unsafe(readFileSync(join(import.meta.dir, 'road-markings-dirty-cells.sql'), 'utf8')
        .replaceAll(':max_cells', '100').replaceAll(':max_attempts', '3').replaceAll(':cell', '0.02'))
      const rows = (Array.isArray(out.at(-1)) ? out.at(-1) : out) as { kind: string; a: string; b: string }[]
      for (const c of rows.filter(r => r.kind === 'cell')) {
        const [cx, cy] = [Number(c.a), Number(c.b)]
        await sql`TRUNCATE _rm_scope`
        await sql`INSERT INTO _rm_scope VALUES (ST_MakeEnvelope(${cx * 0.02}, ${cy * 0.02}, ${(cx + 1) * 0.02}, ${(cy + 1) * 0.02}, 4326))`
        await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
      }
    }, 180_000)

    afterAll(async () => {
      await sql`DELETE FROM detail_dirty`
    })

    test('queues it on a database whose last build had no road test of its own', async () => {
      expect(ownTest).toBe(true)
      // The way, its old outline, and the two ways carrying on from it.
      expect(queued).toBe(4)
    })

    test('rebuilds the ways downstream as well, on both sides of the boundary', async () => {
      // One lane before, not two: the next way's long taper is still 2.6 m
      // either side at the boundary, and both cells draw it so.
      for (const y of [40.71995, 40.72005]) {
        for (const m of [-2.3, 2.3]) expect(await covered(east(-74.050, m), y)).toBe(true)
        for (const m of [-3, 3]) expect(await covered(east(-74.050, m), y)).toBe(false)
      }
      for (const m of [-4.2, 4.2]) expect(await covered(east(-74.050, m), 40.7212)).toBe(true)
    })
  })

  test('keeps the queue\'s road test the same as the build\'s', () => {
    const body = (file: string) => {
      const text = readFileSync(join(import.meta.dir, file), 'utf8')
      const start = text.indexOf('CREATE OR REPLACE FUNCTION road_is_marked_way')
      return text.slice(start, text.indexOf('$$;', start))
    }
    expect(body('queue-road-markings.sql')).toBe(body('generate-road-markings.sql'))
  })

  describe('two neighbouring cells', () => {
    const cell = (y0: number, y1: number) => sql`ST_MakeEnvelope(-74.03, ${y0}, -74.01, ${y1}, 4326)`
    const median = () => sql`
      SELECT ST_Length(ST_Union(geom)::geography) as union_m, sum(ST_Length(geom::geography)) as sum_m
      FROM road_markings WHERE kind = 'hatch' AND geom && ST_MakeEnvelope(-74.0227, 40.7174, -74.0223, 40.7186, 4326)`
    let medianBefore: any
    // Bars along the long bike lane and parking near the boundary.
    const bars = () => sql`
      SELECT ST_AsText(ST_SnapToGrid(ST_Union(geom), 0.0000001)) as g, ST_Length(ST_Union(geom)::geography) as m
      FROM road_markings WHERE kind IN ('hatch', 'parking') AND geom && ST_MakeEnvelope(-74.0180, 40.7176, -74.0170, 40.7184, 4326)`
    let barsBefore: any
    const RAMP_BOX = (sql: postgres.Sql) => sql`ST_MakeEnvelope(${east(RAMP_X, -12)}, 40.7170, ${east(RAMP_X, 12)}, 40.7192, 4326)`
    let rampRef: { l: number; r: number }[] = []
    beforeAll(async () => {
      ;[medianBefore] = await median()
      ;[barsBefore] = await bars()
      // The ramp as the full build drew it.
      rampRef = await rampProfile()
      await sql`CREATE TABLE ramp_paint_ref AS SELECT geom FROM road_markings WHERE kind = 'lane' AND geom && ${RAMP_BOX(sql)}`
      for (const [y0, y1] of [[40.70, 40.718], [40.718, 40.73]]) {
        await sql`TRUNCATE _rm_scope`
        await sql`INSERT INTO _rm_scope VALUES (${cell(y0, y1)})`
        await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
      }
    }, 120_000)

    test('place a way the same on both sides of the boundary, far from where it widened', async () => {
      for (const y of [40.7179, 40.7181, 40.720]) {
        expect(await covered(east(-74.020, -4.2), y)).toBe(true)
        expect(await covered(east(-74.020, -4.8), y)).toBe(false)
        expect(await covered(east(-74.020, 4.2), y)).toBe(true)
      }
    })

    test('flare a ramp across their boundary as one build does', async () => {
      const after = await rampProfile()
      for (const [i, k] of after.entries()) {
        expect(Math.abs(k.l - rampRef[i].l)).toBeLessThan(0.05)
        expect(Math.abs(k.r - rampRef[i].r)).toBeLessThan(0.05)
      }
      const [{ moved, n, ref }] = await sql`
        SELECT ST_HausdorffDistance(ST_Union(m.geom), (SELECT ST_Union(geom) FROM ramp_paint_ref)) * 111050 as moved,
               sum(ST_NumGeometries(m.geom))::int as n, (SELECT sum(ST_NumGeometries(geom))::int FROM ramp_paint_ref) as ref
        FROM road_markings m WHERE m.kind = 'lane' AND m.geom && ${RAMP_BOX(sql)}`
      expect(Number(moved)).toBeLessThan(0.05)
      // A dash on the boundary is cut in two there, and nowhere else.
      expect(n - ref).toBeGreaterThanOrEqual(0)
      expect(n - ref).toBeLessThanOrEqual(4)
    })

    test('build a turning circle on their boundary whole, once', async () => {
      for (const [dx, dy] of [[10.5, 0], [-10.5, 0], [0, 10.5], [0, -5]]) {
        expect(await covered(east(-74.026, dx), north(40.718, dy))).toBe(true)
      }
      const [{ union_m2, sum_m2 }] = await sql`
        SELECT ST_Area(ST_Union(geom)::geography) as union_m2, sum(ST_Area(geom::geography)) as sum_m2
        FROM road_surfaces WHERE geom && ST_MakeEnvelope(-74.0265, 40.7175, -74.0255, 40.7185, 4326)`
      expect(Math.abs(sum_m2 - union_m2) / union_m2).toBeLessThan(0.01)
      const [{ sharpest }] = await kerbTurns(-74.026, 40.718, 25)
      expect(sharpest).toBeLessThan(12)
    })

    test('hatch a painted area across their boundary as one build would', async () => {
      const [after] = await median()
      expect(medianBefore.union_m).toBeGreaterThan(20)
      expect(Math.abs(after.union_m - medianBefore.union_m)).toBeLessThan(0.05)
      expect(Math.abs(after.sum_m - after.union_m)).toBeLessThan(0.05)
    })

    test('space a long way\'s hatching and stalls as one build would, whatever each box cuts', async () => {
      const [after] = await bars()
      expect(barsBefore.m).toBeGreaterThan(20)
      expect(Math.abs(after.m - barsBefore.m)).toBeLessThan(0.05)
      const [{ moved }] = await sql`SELECT ST_HausdorffDistance(${after.g}::geometry, ${barsBefore.g}::geometry) * 84300 as moved`
      expect(moved).toBeLessThan(0.05)
    })

    test('meet without a gap or an overlap', async () => {
      const [{ union_m2, sum_m2 }] = await sql`
        SELECT ST_Area(ST_Union(geom)::geography) as union_m2, sum(ST_Area(geom::geography)) as sum_m2
        FROM road_surfaces WHERE geom && ST_MakeEnvelope(-74.021, 40.717, -74.019, 40.719, 4326)`
      expect(Math.abs(sum_m2 - union_m2) / union_m2).toBeLessThan(0.01)
      expect(union_m2).toBeGreaterThan(9 * 200)
    })
  })

  describe('queueing a replication diff', () => {
    const plan = async (maxCells: number) =>
      sql.unsafe(readFileSync(join(import.meta.dir, 'road-markings-dirty-cells.sql'), 'utf8')
        .replaceAll(':max_cells', String(maxCells)).replaceAll(':max_attempts', '3').replaceAll(':cell', '0.02'))
    const rows = (result: any) => (Array.isArray(result.at(-1)) ? result.at(-1) : result) as { kind: string; a: string; b: string }[]

    beforeAll(async () => {
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'W'::char(1) as osm_type, osm_id, NULL::text as name, NULL::text[] as categories, ST_Translate(geom, 0.05, 0) as geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'W5';
        -- A crossing deleted where its crosswalk is painted, a sidewalk away
        -- from any, and a building: only the first matters to road markings.
        INSERT INTO osm_replay.old_places (id, osm_type, osm_id, categories, geom, geom_type) VALUES
          ('W98', 'W', 98, '{highway/footway}', ST_GeomFromText('LINESTRING(-74.0006 40.70985, -74.0004 40.71015)', 4326), 'line'),
          ('W99', 'W', 99, '{highway/footway}', ST_GeomFromText('LINESTRING(-74.0035 40.7125, -74.0035 40.7135)', 4326), 'line'),
          ('W97', 'W', 97, '{building/house}', ST_GeomFromText('POLYGON((-74.0005 40.7101, -74.0004 40.7101, -74.0004 40.7102, -74.0005 40.7101))', 4326), 'area');
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id IN ('W5', 'N8');`)
      await sql.unsafe(readFileSync(join(import.meta.dir, 'detail-queue-table.sql'), 'utf8'))
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
    })

    test('queues the old and new outlines of what road markings are drawn from, and nothing else', async () => {
      // W5 old and new, the signal N8, and the crossing W98; then the ways
      // meeting W5's end (W1, W2, W6) and those meeting theirs (W3, W4, W7).
      const [{ n }] = await sql`SELECT count(*)::int as n FROM detail_dirty WHERE layer = 'road_markings'`
      expect(n).toBe(10)
    })

    test('drops what lies where nothing was built, and plans the cells of the rest', async () => {
      const out = rows(await plan(100))
      const dropped = out.find(r => r.kind === 'dropped')!
      // The old outline lies 0.05° east, where nothing was built.
      expect(dropped.a).toBe('1')
      expect(out.find(r => r.kind === 'entries')!.a.split(',')).toHaveLength(9)
      const cells = out.filter(r => r.kind === 'cell')
      expect(cells.length).toBeGreaterThan(0)
      for (const c of cells) expect(Number(c.a) * 0.02).toBeLessThan(-73.98)
      const [{ n }] = await sql`SELECT count(*)::int as n FROM detail_dirty WHERE layer = 'road_markings'`
      expect(n).toBe(9)
    })

    test('always takes the oldest entry, however many cells it spans', async () => {
      const entries = rows(await plan(1)).find(r => r.kind === 'entries')!.a.split(',')
      const [{ oldest }] = await sql`SELECT min(id)::text as oldest FROM detail_dirty WHERE layer = 'road_markings'`
      expect(entries).toContain(oldest)
    })

    test('takes a failed entry last', async () => {
      const [{ oldest }] = await sql`SELECT min(id)::text as oldest FROM detail_dirty WHERE layer = 'road_markings'`
      await sql`UPDATE detail_dirty SET attempts = 1 WHERE id = ${oldest}`
      const [{ next }] = await sql`SELECT min(id)::text as next FROM detail_dirty WHERE layer = 'road_markings' AND attempts = 0`
      const entries = rows(await plan(1)).find(r => r.kind === 'entries')!.a.split(',')
      expect(entries).toContain(next)
      await sql`UPDATE detail_dirty SET attempts = 3 WHERE id = ${oldest}`
      expect(rows(await plan(100)).find(r => r.kind === 'dropped')!.b).toBe('1')
    })
  })

  describe('a turning circle moved', () => {
    beforeAll(async () => {
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'N'::char(1) as osm_type, osm_id, NULL::text as name, '{highway/turning_circle}'::text[] as categories,
                 ST_Translate(geom, 0.004, 0) as geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'N80';
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id = 'N80';`)
      await sql`DELETE FROM detail_dirty`
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
    })

    afterAll(async () => {
      await sql`DELETE FROM detail_dirty`
    })

    test('queues where it was and where it is', async () => {
      const [{ was, is }] = await sql`
        SELECT count(*) FILTER (WHERE ST_Intersects(box, ST_SetSRID(ST_MakePoint(-74.116, 40.713), 4326)))::int as was,
               count(*) FILTER (WHERE ST_Intersects(box, ST_SetSRID(ST_MakePoint(-74.120, 40.713), 4326)))::int as is
        FROM detail_dirty WHERE layer = 'road_markings'`
      expect(was).toBe(1)
      expect(is).toBe(1)
    })
  })

  describe('a painted area deleted, and one added', () => {
    beforeAll(async () => {
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'W'::char(1) as osm_type, osm_id, NULL::text as name, '{}'::text[] as categories, geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'W231';
        -- An untagged area nowhere near paint.
        INSERT INTO osm_replay.old_places (id, osm_type, osm_id, categories, geom, geom_type) VALUES
          ('W96', 'W', 96, '{}', ST_GeomFromText('POLYGON((-74.36 40.712, -74.3599 40.712, -74.3599 40.7121, -74.36 40.712))', 4326), 'area');
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id = 'W251';`)
      await sql`DELETE FROM detail_dirty`
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
    })

    afterAll(async () => {
      await sql`DELETE FROM detail_dirty`
    })

    test('queues where each lies', async () => {
      const at = async (x: number, y: number) => (await sql`
        SELECT count(*)::int as n FROM detail_dirty WHERE layer = 'road_markings' AND ST_Intersects(box, ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326))`)[0].n
      expect(await at(-74.315, 40.7133)).toBe(1)
      expect(await at(-74.325, 40.7132)).toBe(1)
      expect(await at(-74.36, 40.712)).toBe(0)
    })
  })

  describe('a sidewalk moved, and footways deleted', () => {
    beforeAll(async () => {
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'W'::char(1) as osm_type, osm_id, NULL::text as name, '{highway/footway}'::text[] as categories,
                 ST_Translate(geom, 0.00004, 0) as geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'W401';
        -- Deleted: one beside a paved street, one nowhere near a street.
        INSERT INTO osm_replay.old_places (id, osm_type, osm_id, categories, geom, geom_type) VALUES
          ('W497', 'W', 497, '{highway/footway}', ST_GeomFromText('LINESTRING(-74.40310 40.712, -74.40310 40.714)', 4326), 'line'),
          ('W498', 'W', 498, '{highway/footway}', ST_GeomFromText('LINESTRING(-74.38 40.712, -74.38 40.714)', 4326), 'line');
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id = 'W401';`)
      await sql`DELETE FROM detail_dirty`
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
    })

    afterAll(async () => {
      await sql`DELETE FROM detail_dirty`
    })

    test('queues the streets beside them', async () => {
      const at = async (x: number, y: number) => (await sql`
        SELECT count(*)::int as n FROM detail_dirty WHERE layer = 'road_markings' AND ST_Intersects(box, ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326))`)[0].n
      expect(await at(-74.400, 40.7139)).toBeGreaterThan(0)
      expect(await at(-74.403, 40.7139)).toBeGreaterThan(0)
      expect(await at(-74.38, 40.713)).toBe(0)
    })
  })

  describe('a first scoped build', () => {
    const BOX = 'ST_MakeEnvelope(-74.0003, 40.7095, -73.9985, 40.7105, 4326)'

    beforeAll(async () => {
      await sql.unsafe(`DROP TABLE road_surfaces, road_markings, road_glyphs; ${GUARD_NEXT}
        TRUNCATE _rm_scope; INSERT INTO _rm_scope VALUES (${BOX});`)
      await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
    }, 60_000)

    test('creates the live tables and fills only the box', async () => {
      const [{ inside, outside }] = await sql.unsafe(`
        SELECT count(*) FILTER (WHERE ST_CoveredBy(geom, ${BOX}))::int as inside,
               count(*) FILTER (WHERE NOT ST_CoveredBy(geom, ${BOX}))::int as outside
        FROM road_surfaces`)
      expect(inside).toBeGreaterThan(0)
      expect(outside).toBe(0)
      const [{ n }] = await sql`SELECT count(*)::int as n FROM road_markings`
      expect(n).toBeGreaterThan(0)
    })
  })
})
