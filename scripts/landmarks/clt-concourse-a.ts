/**
 * Charlotte Douglas International Airport (CLT): Concourse A — procedural,
 * CC0-1.0.
 *
 *   bun scripts/landmarks/clt-concourse-a.ts
 *
 * One OSM outline (way/1414163869) holding three generations: the 1986 pier
 * running west from the terminal, the long connector north along runway
 * 18C/36C, and the two Concourse A North gate piers (2018 and its 2020s
 * extension) at the connector's end.
 *
 * The older parts are plain 10 m blocks with gate-level window panels and a
 * pale roof. The A North piers are taller glass halls under a broad flat roof
 * whose thick silver fascia overhangs the walls and curls round the airside
 * corners (Commons, "CLT Concourse A North with United Aircraft (2020)").
 *
 * Site frame and helpers: see clt-terminal.ts.
 */
import { Part } from './mesh'
import { CLT, block, cap, clean, inset, panels, roundRect, save, walls, type XY } from './clt-terminal'

const A: XY[] = [[-266.4,-44.5],[-314.1,-44.1],[-313.9,-41.2],[-355.5,31.9],[-355.2,188.0],[-351.6,188.0],[-349.3,233.9],[-355.0,234.0],[-354.5,450.6],[-350.5,450.6],[-347.8,496.6],[-497.5,496.9],[-497.0,450.6],[-367.8,450.8],[-368.1,373.4],[-372.6,373.4],[-372.7,365.0],[-371.2,365.0],[-371.2,360.8],[-382.7,360.5],[-383.5,322.4],[-372.5,322.6],[-371.6,298.6],[-367.5,298.6],[-368.0,234.9],[-368.0,233.5],[-376.1,233.7],[-421.4,233.8],[-466.6,234.0],[-496.2,234.4],[-503.1,234.5],[-503.7,221.2],[-503.3,195.2],[-503.4,188.8],[-488.8,188.5],[-457.3,188.0],[-393.9,187.7],[-387.0,187.7],[-367.4,187.8],[-368.7,115.7],[-384.2,116.0],[-384.7,71.9],[-368.6,71.6],[-368.8,27.5],[-357.7,8.7],[-357.2,-32.8],[-363.3,-32.8],[-381.2,-32.6],[-381.2,-36.1],[-383.9,-36.1],[-383.8,-32.7],[-385.6,-32.7],[-391.3,-32.7],[-391.3,-35.9],[-409.7,-35.9],[-409.8,-32.8],[-415.8,-32.7],[-415.8,-35.9],[-419.2,-35.9],[-431.0,-35.9],[-431.0,-32.3],[-435.9,-32.3],[-436.0,-30.6],[-453.3,-30.7],[-482.2,-30.5],[-482.3,-32.4],[-510.8,-32.4],[-518.4,-32.4],[-518.3,-29.0],[-524.0,-29.0],[-524.1,-32.3],[-542.3,-32.2],[-552.8,-32.2],[-552.8,-34.6],[-552.8,-44.1],[-556.6,-44.1],[-556.6,-51.0],[-556.6,-54.1],[-552.8,-54.1],[-552.8,-57.0],[-552.9,-60.5],[-535.2,-60.5],[-535.2,-64.1],[-533.3,-64.1],[-526.8,-64.2],[-526.7,-60.7],[-511.3,-60.7],[-511.3,-64.5],[-509.2,-64.5],[-502.5,-64.6],[-502.4,-60.8],[-492.8,-61.0],[-492.8,-64.1],[-480.1,-64.1],[-480.0,-60.6],[-475.5,-60.6],[-464.6,-60.6],[-464.6,-63.9],[-455.7,-64.0],[-455.7,-65.5],[-420.9,-65.6],[-394.1,-65.7],[-394.0,-63.9],[-385.3,-63.9],[-383.5,-64.0],[-383.6,-60.6],[-381.2,-60.6],[-381.3,-52.8],[-363.1,-52.8],[-329.2,-52.8],[-329.2,-57.7],[-266.5,-57.7],[-266.4,-53.5],[-266.4,-44.5]]
const CENTRE: XY = [-411.51, 215.57]

const stone = new Part(), roof = new Part(), win = new Part(), silver = new Part(), trim = new Part()

const H = 10
const c = block(stone, roof, A, 0, H, 0.45)
panels(win, c, 4, 8.6, { bay: 8, gap: 1.2, minLen: 4 })

// The A North piers: glass walls, set just outside the outline so they cover
// the older block's walls, under an overhanging roof.
const PIERS: [number, number, number, number][] = [
  [-503.8, 187.4, -348.9, 234.6],
  [-497.9, 450.3, -347.5, 497.2],
]
const ZW = 15.5, ZR = 18
for (const [x0, y0, x1, y1] of PIERS) {
  // walls: glass between silver corner piers, rounded at the airside (west) end
  const body = clean(roundRect(x0 - 0.3, y0 - 0.3, x1 + 0.3, y1 + 0.3, [8, 0, 0, 8], 4))
  walls(win, body, 0, ZW)
  // a white band at the gate level floor, where the jet bridges dock
  walls(trim, inset(body, -0.06), 5.2, 6.4)
  // the roof slab: fascia all round, flat top, soffit under the overhang
  const slab = clean(roundRect(x0 - 3.2, y0 - 3.2, x1 + 1.0, y1 + 3.2, [11, 0, 0, 11], 5))
  const top = inset(slab, 0.8)
  walls(silver, slab, ZW, ZR - 0.8)
  walls(silver, slab, ZR - 0.8, ZR, top)
  cap(roof, top, ZR)
  cap(silver, slab, ZW, false)
}

await save('clt-concourse-a', 'Charlotte Douglas International Airport Concourse A', CENTRE, [
  { part: stone, material: CLT.stone },
  { part: win, material: CLT.window },
  { part: trim, material: CLT.trim },
  { part: roof, material: CLT.roof },
  { part: silver, material: CLT.silver },
])
