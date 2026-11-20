/**
 * How high a bridge's deck stands, where OSM records it on the bridge's
 * `man_made=bridge` outline, its ways and nodes and the seamarks under it, and
 * where a suspended deck hangs free of piers.
 *
 * What each tag measures:
 *
 *   ele (on a node of a bridge's highway or railway way)  The deck's surface
 *       there, in metres above the EGM96 geoid, as OSM defines `ele` on any
 *       node. That is not the terrain's datum everywhere (NAVD88 in the
 *       conterminous US, 0.4–1 m under EGM96 in the east), so it is
 *       converted to the terrain's datum on reading (`terrainHeight` in
 *       datum.ts, which says where it can). The deck is pinned to it
 *       exactly (see `pinned` in profile.ts), once `trustedPins` believes it.
 *       This is the tag to add where nothing else gives a deck its height.
 *   ele (on the outline, or on a bridge way itself)  Above EGM96 too, and
 *       converted the same way, and
 *       taken as the deck's top at its crest (a way's at its middle), but only
 *       as a least height, never above MAX_DECK over the ground: an `ele`
 *       mapped as the ground's, as imports have, falls at or under the deck
 *       and changes little.
 *   seamark:bridge:clearance_height  Clearance under the bridge above the
 *       water, as charted. The deck's top is DECK_DEPTH above it. For an
 *       opening bridge, seamark:bridge:clearance_height_closed is the one the
 *       deck stands at. On the outline it sets the deck's crest; on a seamark
 *       (seamark:type=bridge, a node or way under the bridge or the bridge way
 *       itself) the deck clears it there (`clearanceNeeds`).
 *   height (on the outline)  Key:height is the feature's bottom to its top.
 *       For a bridge whose structure stays below its roadway (beam, truss,
 *       viaduct) that top is the deck. For one hung from towers it may be the
 *       towers' top instead, so it is taken as the deck only when it falls
 *       clearly short of the towers' own `height`.
 *   bridge:support=pylon / abutment  The towers and anchorages, as areas. A
 *       suspended deck needs no piers between them. Where towers give their
 *       `height` (above the water) and the outline no deck height, the deck
 *       hangs a cable's sag below their tops (`hungBelow`); a support taller
 *       than TOWER is a tower whatever its `bridge:support`.
 *   layer  Drawing order only: not a height.
 *
 * Heights are measured from the lowest ground under the deck's span, and
 * never from below sea level: the terrain lays tidal water a metre or two
 * under it.
 *
 * Where OSM has none of these, `config/bridge-elevations.json` may give a
 * deck height by the bridge's Wikidata item; see that file.
 */
import { readFileSync } from 'node:fs'
import { along, beside, heightAt, metresPerUnit, type Need, type Pin, type Point } from './profile'

/** Metres from a bridge's charted clearance to its roadway: the depth of its deck. */
export const DECK_DEPTH = 2
/** A bridge `height` past this share of its towers' is the towers' top, not its deck's. */
const TOWER_SHARE = 0.9
/** Highest deck a tag is believed for, in metres above the ground under it. */
export const MAX_DECK = 150
/** Metres a pin may lie below the ground under it and still be believed: the terrain and a surveyed height differ by about this much. */
export const PIN_BELOW = 1
/** A suspension span's towers stand this share of the distance between them above its deck: the sag of its main cables. */
const SAG = 1 / 9.5
/** Tallest a support stands, in metres, and still be a pier rather than a tower. */
export const TOWER = 60
/** Structures whose towers carry no deck hung below their tops. */
const UNHUNG = new Set(['beam', 'truss', 'arch', 'cantilever', 'viaduct', 'aqueduct', 'humpback'])

/** A suspended span's crest rises this share of the distance between its towers above the deck at them. */
export const CAMBER = 0.01

export type Support = { kind: 'pylon' | 'abutment'; ring: Point[]; height: number | null }

/** What the deck generator knows of an outline beyond its shape. */
export type OutlineLevel = {
  /** Metres from the lowest ground under the span to the top of the deck at its crest. */
  deck?: number
  /** The outline's `ele`: the least height of the deck's top at its crest, in metres in the terrain's datum. */
  top?: number
  /** Where the deck height came from. */
  source?: 'override' | 'seamark' | 'height' | 'towers'
  /** Hung from towers: no piers between its outermost supports. */
  suspended?: boolean
  supports?: Support[]
}

type Overrides = Record<string, { deck: number; note: string }>

const OVERRIDES: Overrides = JSON.parse(readFileSync(new URL('../../../config/bridge-elevations.json', import.meta.url), 'utf8')).bridges

export const metres = (value: string | null | undefined): number | null => {
  const m = value?.match(/^\s*(-?[0-9]+(?:\.[0-9]+)?)\s*(m|ft|')?\s*$/)
  if (!m) return null
  return Number(m[1]) * (m[2] === 'ft' || m[2] === "'" ? 0.3048 : 1)
}

/** The clearance a seamark charts under a bridge, in metres, where it gives one to trust: closed, for an opening bridge. */
export function clearance(tags: Record<string, string>): number | null {
  const value = metres(tags['seamark:bridge:clearance_height_closed']) ?? metres(tags['seamark:bridge:clearance_height'])
  return value !== null && value > 0 && value + DECK_DEPTH <= MAX_DECK ? value : null
}

const SUSPENDED = new Set(['suspension', 'cable-stayed', 'simple-suspension'])

export const isSuspended = (tags: Record<string, string>) => SUSPENDED.has(tags.bridge) || SUSPENDED.has(tags['bridge:structure'])

/**
 * The deck height an outline's tags give, measured from the ground under its
 * span; null where they give none to trust. `gap` is how far EGM96 stands
 * above the terrain's datum at the outline (`geoidGap`), 0 where unknown.
 */
export function deckLevel(tags: Record<string, string>, supports: Support[], overrides: Overrides = OVERRIDES, gap = 0): OutlineLevel {
  const suspended = isSuspended(tags)
  const level: OutlineLevel = { suspended, supports }
  const ele = metres(tags.ele)
  // Measured from sea level, not the ground: whether it is believed is only known once the ground is (`levelNeeds`).
  // `gap` takes it from EGM96 to the terrain's datum (see datum.ts).
  if (ele !== null) level.top = ele - gap
  const override = tags.wikidata ? overrides[tags.wikidata] : undefined
  if (override) return { ...level, deck: override.deck, source: 'override' }
  const charted = clearance(tags)
  if (charted !== null) return { ...level, deck: charted + DECK_DEPTH, source: 'seamark' }
  const hung = hungBelow(tags, supports, level)
  const height = metres(tags.height)
  if (height === null || height <= 0 || height > MAX_DECK) return hung ?? level
  const towers = supports.flatMap(s => (s.kind === 'pylon' && s.height ? [s.height] : []))
  const towering = suspended || ['arch', 'cable-stayed', 'suspension'].includes(tags['bridge:structure'])
  if (towering && (!towers.length || height >= TOWER_SHARE * Math.max(...towers))) return hung ?? level
  // A suspension bridge's `height` is often its charted clearance, under its deck: its towers' tell the deck higher.
  return hung && hung.deck > height ? hung : { ...level, deck: height, source: 'height' }
}

/**
 * The deck of a span hung from towers OSM gives a `height`, which a suspension
 * bridge's cables reach the top of: SAG of the span between the two farthest
 * apart above it. Taken for any bridge with two such towers not known to
 * stand on them, as most are suspension bridges.
 */
function hungBelow(tags: Record<string, string>, supports: Support[], level: OutlineLevel): (OutlineLevel & { deck: number }) | null {
  const structure = tags['bridge:structure'] ?? tags.bridge
  if (UNHUNG.has(structure) || structure === 'cable-stayed') return null
  const towers = supports.filter(s => s.height !== null && s.height > TOWER)
  let span = 0
  for (const a of towers) for (const b of towers) {
    const [p, q] = [centre(a.ring), centre(b.ring)]
    span = Math.max(span, Math.hypot(q[0] - p[0], q[1] - p[1]) * metresPerUnit((p[1] + q[1]) / 2))
  }
  if (span < 2 * TOWER) return null
  const deck = Math.max(...towers.map(t => t.height!)) - span * SAG
  return deck > DECK_DEPTH ? { ...level, suspended: true, supports: supports.filter(s => s.kind === 'abutment' || towers.includes(s)), deck, source: 'towers' } : null
}

/** Farthest a support may stand to the side of a deck and still carry it, in metres: a tower's legs flank the roadway. */
const SUPPORT_REACH = 40

/** Distances along a deck level with the supports of a kind beside or over it, in order. */
function supportsAlong(points: Point[], d: number[], supports: Support[], kind: Support['kind']): number[] {
  return supports.filter(s => s.kind === kind).flatMap(s => {
    const near = beside(points, centre(s.ring))
    if (!near.alongside || near.distance > SUPPORT_REACH) return []
    return [d[near.segment - 1] + (d[near.segment] - d[near.segment - 1]) * near.t]
  }).sort((a, b) => a - b)
}

const centre = (ring: Point[]): Point => {
  const n = ring.length > 1 && ring[0][0] === ring[ring.length - 1][0] && ring[0][1] === ring[ring.length - 1][1] ? ring.length - 1 : ring.length
  const pts = ring.slice(0, Math.max(1, n))
  return [pts.reduce((a, p) => a + p[0], 0) / pts.length, pts.reduce((a, p) => a + p[1], 0) / pts.length]
}

/**
 * The heights a deck must reach to stand at its outline's recorded level,
 * `base` being the lowest ground under the outline. Between the two towers
 * farthest apart, every deck in the outline meets the crest midway and, for a
 * suspended span, falls by CAMBER of the span to each tower along a parabola.
 * Without towers the crest is the middle of the deck's run inside.
 */
export function levelNeeds(points: Point[], within: boolean[], level: OutlineLevel, base: number): Need[] {
  if (!within.some(Boolean)) return []
  const d = along(points)
  const mid = (d[within.indexOf(true)] + d[within.lastIndexOf(true)]) / 2
  // An `ele` above the ground under the span, and no farther above it than a deck stands, is a least height at the crest.
  const top: Need[] = level.top !== undefined && level.top > base && level.top <= base + MAX_DECK ? [{ at: mid, height: level.top }] : []
  if (level.deck === undefined) return top
  return [...top, ...deckNeeds(points, d, within, level as OutlineLevel & { deck: number }, base, mid)]
}

/** The two towers farthest apart, which a span hangs between; null with fewer than two apart. */
function towerAxis(level: OutlineLevel): [Point, Point] | null {
  const towers = (level.supports ?? []).filter(s => s.kind === 'pylon').map(s => centre(s.ring))
  let axis: [Point, Point] | null = null
  for (const a of towers) for (const b of towers)
    if (!axis || Math.hypot(b[0] - a[0], b[1] - a[1]) > Math.hypot(axis[1][0] - axis[0][0], axis[1][1] - axis[0][1])) axis = [a, b]
  return axis && Math.hypot(axis[1][0] - axis[0][0], axis[1][1] - axis[0][1]) * metresPerUnit(axis[0][1]) >= 1 ? axis : null
}

/** How far between its towers a point lies, 0 to 1, where it lies on the span between them; null off it. */
export function alongSpan(level: OutlineLevel, p: Point): number | null {
  const axis = towerAxis(level)
  if (!axis) return null
  const [a, b] = axis
  const v: Point = [b[0] - a[0], b[1] - a[1]]
  const u = ((p[0] - a[0]) * v[0] + (p[1] - a[1]) * v[1]) / (v[0] * v[0] + v[1] * v[1])
  const off = Math.abs((p[0] - a[0]) * v[1] - (p[1] - a[1]) * v[0]) / Math.hypot(v[0], v[1])
  return u >= 0 && u <= 1 && off * metresPerUnit(p[1]) < SUPPORT_REACH ? u : null
}

function deckNeeds(points: Point[], d: number[], within: boolean[], level: OutlineLevel & { deck: number }, base: number, mid: number): Need[] {
  const axis = towerAxis(level)
  if (!axis) return [{ at: mid, height: base + level.deck }]
  const span = Math.hypot(axis[1][0] - axis[0][0], axis[1][1] - axis[0][1]) * metresPerUnit((axis[0][1] + axis[1][1]) / 2)
  const sag = level.suspended ? span * CAMBER : 0
  return points.flatMap((p, i) => {
    const u = within[i] ? alongSpan(level, p) : null
    return u !== null ? [{ at: d[i], height: base + level.deck - sag * (2 * u - 1) ** 2, exact: level.suspended }] : []
  })
}

/**
 * The stretch of a deck hung from its outline's towers, where it stands on no
 * piers: out from its outer towers to the anchorage just beyond each, or
 * where there is none, to the end of its run inside the outline there.
 */
export function hungStretch(points: Point[], within: boolean[], level: OutlineLevel): [number, number] | null {
  if (!level.suspended) return null
  const d = along(points)
  const towers = supportsAlong(points, d, level.supports ?? [], 'pylon')
  if (!towers.length) return null
  const anchorages = supportsAlong(points, d, level.supports ?? [], 'abutment')
  const before = anchorages.filter(a => a < towers[0])
  const after = anchorages.filter(a => a > towers[towers.length - 1])
  const first = within.indexOf(true), last = within.lastIndexOf(true)
  return [before.length ? before[before.length - 1] : d[Math.max(0, first)], after.length ? after[0] : d[last < 0 ? d.length - 1 : last]]
}

/**
 * The pins to believe of a deck's nodes tagged `ele`, `ground` being the
 * ground under it at each of `d`. A pin below the ground (by more than
 * PIN_BELOW) cannot be the deck's surface, unless it is `under` a roof the
 * terrain takes for ground, as a covered way's is; and one more than MAX_DECK
 * above it is a typo or another datum. Either is left out, and the deck takes
 * its height by rule there, as if the tag were not there.
 */
export function trustedPins(pins: Pin[], d: number[], ground: number[]): Pin[] {
  return pins.filter(p => {
    const g = heightAt(d, ground, p.at)
    return (p.under || p.height >= g - PIN_BELOW) && p.height <= g + MAX_DECK
  })
}

/** Farthest a seamark may lie from a deck's centreline and still chart the clearance under it, in metres. */
export const SEAMARK_REACH = 30

/** A seamark's charted clearance under a bridge, where it lies. */
export type Seamark = { point: Point; clearance: number }

/**
 * Heights a deck must reach over the seamarks charting a bridge's clearance
 * under it: the clearance plus DECK_DEPTH over the ground (the water) there,
 * at the seamark's place along the deck. A seamark beside the deck, within
 * SEAMARK_REACH of it, charts the channel under every deck it lies under.
 */
export function clearanceNeeds(points: Point[], d: number[], ground: number[], seamarks: Seamark[]): Need[] {
  return seamarks.flatMap(m => {
    const near = beside(points, m.point)
    if (!near.alongside || near.distance > SEAMARK_REACH) return []
    const at = d[near.segment - 1] + (d[near.segment] - d[near.segment - 1]) * near.t
    return [{ at, height: heightAt(d, ground, at) + m.clearance + DECK_DEPTH }]
  })
}
