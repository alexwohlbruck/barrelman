/**
 * Charlotte Douglas International Airport (CLT): Daily Deck West —
 * procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/clt-daily-deck-west.ts
 *
 * One of the pair of daily parking decks north of the Hourly Deck
 * (relation/5936266, 3,000 spaces), a plain open deck: open floors at 3.4 m behind
 * concrete spandrel bands, a pale roof deck, and two helix ramps whose open
 * wells OSM maps as holes. Unlike the Hourly Deck it has no metal shell. The covered walkway between the two decks (relation/5936267) is drawn here too.
 *
 * OSM records no height for either daily deck. The roof deck is at 13.5 m
 * (four levels and the roof, at 3.4 m), and the helices are solid concrete
 * drums whose top turn stands a parapet above it, both checked against
 * Mapbox's 3D buildings (tileset mapbox.mapbox-3dbuildings-v1), used only as
 * a visual reference; the geometry is OSM's outline and our own modelling.
 *
 * Site frame and helpers: see clt-terminal.ts; colours match
 * clt-hourly-deck.ts.
 */
import { Part } from './mesh'
import { finish } from './palette'
import { CLT, cap, clean, parkingDeck, save, walls, type XY } from './clt-terminal'

const OUTER: XY[] = [[-154.7,452.5],[-155.2,397.0],[-155.2,395.0],[-155.2,393.3],[-154.2,393.3],[-154.3,382.7],[-160.8,382.8],[-160.8,384.4],[-194.9,384.8],[-195.0,375.5],[-192.6,375.6],[-189.5,376.5],[-186.7,378.3],[-184.6,380.3],[-182.1,381.8],[-179.4,382.9],[-176.5,383.4],[-173.6,383.3],[-170.7,382.6],[-168.0,381.5],[-165.6,379.8],[-163.6,377.7],[-162.0,375.3],[-160.9,372.6],[-160.4,369.7],[-160.4,366.8],[-161.0,364.0],[-162.2,361.3],[-163.8,358.9],[-165.9,356.9],[-168.3,355.3],[-171.0,354.2],[-173.9,353.6],[-176.8,353.6],[-179.7,354.1],[-182.4,355.2],[-184.8,356.8],[-186.9,358.8],[-188.6,361.2],[-190.8,362.9],[-192.8,363.7],[-195.1,364.2],[-195.2,359.0],[-205.3,359.0],[-216.1,359.0],[-269.4,359.3],[-269.4,364.6],[-274.0,364.6],[-273.9,370.9],[-274.9,370.9],[-274.6,438.2],[-269.3,438.2],[-269.3,440.9],[-274.0,441.0],[-274.0,448.0],[-269.4,448.0],[-269.4,450.1],[-273.9,450.1],[-273.7,517.3],[-272.7,517.3],[-272.6,524.3],[-268.5,524.2],[-268.5,527.7],[-216.8,527.5],[-214.7,527.5],[-212.7,527.5],[-208.5,527.5],[-206.4,527.4],[-204.4,527.4],[-193.9,527.4],[-193.9,523.2],[-191.6,523.2],[-189.2,524.0],[-187.7,527.1],[-186.0,529.4],[-183.8,531.3],[-181.3,532.8],[-178.6,533.8],[-175.7,534.2],[-172.8,534.1],[-169.9,533.4],[-167.3,532.2],[-164.9,530.5],[-162.9,528.4],[-161.4,525.9],[-160.3,523.2],[-159.8,520.3],[-159.9,517.4],[-160.6,514.6],[-161.8,512.0],[-163.4,509.6],[-165.5,507.6],[-168.0,506.0],[-170.7,504.9],[-173.6,504.4],[-176.5,504.5],[-179.4,505.1],[-182.1,506.2],[-184.5,507.8],[-186.5,509.9],[-189.1,511.2],[-191.1,511.7],[-194.2,512.0],[-194.3,508.0],[-186.4,505.3],[-177.9,503.1],[-170.3,501.6],[-162.0,500.5],[-162.0,502.8],[-155.3,502.7],[-155.4,498.6],[-154.3,498.6],[-154.3,497.1],[-154.3,495.6],[-154.4,485.8],[-154.5,470.8],[-153.6,470.8],[-153.7,457.4],[-154.6,457.4],[-154.6,456.6],[-154.7,452.5]]
const WELLS: XY[][] = [
  [[-180.8,516.5],[-181.4,518.9],[-181.1,521.4],[-180.0,523.6],[-178.2,525.3],[-175.9,526.3],[-173.3,526.4],[-170.8,525.5],[-168.8,523.8],[-167.6,521.5],[-167.3,518.9],[-168.2,516.0],[-170.3,513.7],[-173.1,512.5],[-176.2,512.7],[-178.9,514.1],[-180.8,516.5]],
  [[-181.7,366.7],[-181.9,369.2],[-181.2,371.5],[-179.7,373.5],[-177.7,374.9],[-175.2,375.5],[-172.6,375.1],[-170.3,373.8],[-168.7,371.8],[-167.9,369.3],[-168.0,366.7],[-169.4,364.0],[-171.9,362.1],[-174.9,361.4],[-177.9,362.1],[-180.3,364.0],[-181.7,366.7]],
]
const CENTRE: XY = [-201.39, 443.87]
const FLOORS = [0, 3.4, 6.8, 10.15, 13.5]

const parts = { concrete: new Part(), gap: new Part(), deck: new Part() }
const canopy = new Part()
parkingDeck(parts, OUTER, FLOORS, { wells: WELLS })

// The covered walkway at the deck's south-east corner, towards the terminal
// (relation/5936267, building=roof): a thin roof on the second floor line.
const WALK: XY[] = [[-154.2,393.3],[-146.9,393.2],[-147.0,385.7],[-147.0,384.5],[-139.8,384.4],[-136.1,384.3],[-136.1,385.6],[-135.9,392.8],[-133.0,392.9],[-128.0,392.7],[-128.0,394.5],[-127.9,396.7],[-135.8,396.8],[-135.7,400.1],[-137.5,400.2],[-140.5,400.1],[-143.5,400.3],[-146.6,400.3],[-146.7,396.8],[-155.2,397.0],[-155.2,395.0],[-155.2,393.3],[-154.2,393.3]]
const walk = clean(WALK, 0.3)
walls(canopy, walk, 6.2, 6.9)
cap(canopy, walk, 6.9)
cap(canopy, walk, 6.2, false)

await save('clt-daily-deck-west', 'Charlotte Douglas International Airport Daily Deck West', CENTRE, [
  { part: parts.concrete, material: finish('concrete', 0xdcd9d2) },
  { part: parts.gap, material: finish('open-floor', 0x62676b) },
  { part: parts.deck, material: CLT.roof },
  { part: canopy, material: CLT.silver },
])
