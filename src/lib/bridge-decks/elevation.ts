/**
 * How high a bridge's deck stands, where OSM records it on the bridge's
 * `man_made=bridge` outline, its ways and nodes and the seamarks under it, and
 * where a suspended deck hangs free of piers.
 *
 * What each tag measures:
 *
 *   ele (on a node of a bridge's highway or railway way)  The deck's surface
 *       there, in metres above sea level, as `ele` is on any node: the same
 *       datum as the terrain (NAVD88 in the US). The deck is pinned to it
 *       exactly (see `pinned` in profile.ts), once `trustedPins` believes it.
 *       This is the tag to add where nothing else gives a deck its height.
 *   ele (on the outline, or on a bridge way itself)  Above sea level too, and
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
 *       suspended deck needs no piers between them.
 *   layer  Drawing order only: not a height.
 *
 * Heights are measured from the lowest ground under the deck's span, which
 * is the water surface on the terrain model for a river crossing.
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
/** A suspended span's crest rises this share of the distance between its towers above the deck at them. */
export const CAMBER = 0.01

export type Support = { kind: 'pylon' | 'abutment'; ring: Point[]; height: number | null }

/** What the deck generator knows of an outline beyond its shape. */
export type OutlineLevel = {
  /** Metres from the lowest ground under the span to the top of the deck at its crest. */
  deck?: number
  /** The outline's `ele`: the least height of the deck's top at its crest, in metres above sea level. */
  top?: number
  /** Where the deck height came from. */
  source?: 'override' | 'seamark' | 'height'
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

/** The deck height an outline's tags give, measured from the ground under its span; null where they give none to trust. */
export function deckLevel(tags: Record<string, string>, supports: Support[], overrides: Overrides = OVERRIDES): OutlineLevel {
  const suspended = isSuspended(tags)
  const level: OutlineLevel = { suspended, supports }
  const ele = metres(tags.ele)
  // Measured from sea level, not the ground: whether it is believed is only known once the ground is (`levelNeeds`).
  if (ele !== null) level.top = ele
  const override = tags.wikidata ? overrides[tags.wikidata] : undefined
  if (override) return { ...level, deck: override.deck, source: 'override' }
  const charted = clearance(tags)
  if (charted !== null) return { ...level, deck: charted + DECK_DEPTH, source: 'seamark' }
  const height = metres(tags.height)
  if (height === null || height <= 0 || height > MAX_DECK) return level
  const towers = supports.flatMap(s => (s.kind === 'pylon' && s.height ? [s.height] : []))
  const towering = suspended || ['arch', 'cable-stayed', 'suspension'].includes(tags['bridge:structure'])
  if (towering && (!towers.length || height >= TOWER_SHARE * Math.max(...towers))) return level
  return { ...level, deck: height, source: 'height' }
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

function deckNeeds(points: Point[], d: number[], within: boolean[], level: OutlineLevel & { deck: number }, base: number, mid: number): Need[] {
  const towers = (level.supports ?? []).filter(s => s.kind === 'pylon').map(s => centre(s.ring))
  let axis: [Point, Point] | null = null
  for (const a of towers) for (const b of towers)
    if (!axis || Math.hypot(b[0] - a[0], b[1] - a[1]) > Math.hypot(axis[1][0] - axis[0][0], axis[1][1] - axis[0][1])) axis = [a, b]
  if (!axis || Math.hypot(axis[1][0] - axis[0][0], axis[1][1] - axis[0][1]) * metresPerUnit(axis[0][1]) < 1) return [{ at: mid, height: base + level.deck }]
  const [a, b] = axis
  const v: Point = [b[0] - a[0], b[1] - a[1]]
  const span = Math.hypot(v[0], v[1]) * metresPerUnit((a[1] + b[1]) / 2)
  const sag = level.suspended ? span * CAMBER : 0
  return points.flatMap((p, i) => {
    if (!within[i]) return []
    const u = ((p[0] - a[0]) * v[0] + (p[1] - a[1]) * v[1]) / (v[0] * v[0] + v[1] * v[1])
    return u >= 0 && u <= 1 ? [{ at: d[i], height: base + level.deck - sag * (2 * u - 1) ** 2, exact: level.suspended }] : []
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
 * PIN_BELOW) cannot be the deck's surface, and one more than MAX_DECK above it
 * is a typo or another datum: either is left out, and the deck takes its
 * height by rule there, as if the tag were not there.
 */
export function trustedPins(pins: Pin[], d: number[], ground: number[]): Pin[] {
  return pins.filter(p => {
    const g = heightAt(d, ground, p.at)
    return p.height >= g - PIN_BELOW && p.height <= g + MAX_DECK
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
