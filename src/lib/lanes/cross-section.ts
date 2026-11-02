// A way's lanes across its carriageway, from its OSM tags, by the rules
// import/generate-road-markings.sql builds road_markings with (its "Roads and
// their lanes" and "Where the way runs" sections). Offsets are metres to the
// left of the way's traffic direction: the OSM direction, reversed for
// oneway=-1, as the markings build turns such a way.

export type Tags = Record<string, string | undefined>
export type DrivingSide = 'right' | 'left'

export interface CrossSectionLane {
  /** 1-based, left to right as the lane's own traffic sees it. */
  index: number
  /** Metres left of the traffic direction to the lane's middle. */
  offset: number
  width: number
  /** turn:lanes values for this lane; empty where none are tagged. */
  turns: string[]
  bus: boolean
  bike: boolean
}

export interface CrossSection {
  /** The traffic direction runs against the way's nodes (oneway=-1). */
  flip: boolean
  oneway: boolean
  width: number
  laneWidth: number
  /** Lanes running with the traffic direction, then those against it. */
  forward: CrossSectionLane[]
  backward: CrossSectionLane[]
  /** Kerb offsets, left and right. */
  kerbLeft: number
  kerbRight: number
}

const MOTORWAY_TRUNK = new Set(['motorway', 'motorway_link', 'trunk', 'trunk_link'])

/** road_lane_width */
export function laneWidthFor(highway: string | undefined): number {
  switch (highway) {
    case 'motorway': case 'trunk': return 3.6
    case 'motorway_link': case 'trunk_link': case 'primary': return 3.4
    case 'primary_link': case 'secondary': case 'secondary_link': case 'tertiary': case 'tertiary_link': return 3.3
    case 'service': return 2.8
    case 'cycleway': case 'path': case 'footway': return 1.5
    default: return 3.0
  }
}

const carFree = (highway: string | undefined) => ['pedestrian', 'cycleway', 'path', 'footway'].includes(highway ?? '')

/** road_int */
export function tagInt(value: string | undefined): number | null {
  const m = value?.match(/^\s*([0-9]{1,2})/)
  return m ? Number(m[1]) : null
}

/** road_metres */
export function tagMetres(value: string | undefined): number | null {
  if (value == null) return null
  if (/^\s*[0-9]+(\.[0-9]+)?\s*m?\s*$/.test(value)) return Number(value.match(/[0-9]+(?:\.[0-9]+)?/)![0])
  if (/^\s*[0-9]+(\.[0-9]+)?\s*(ft|')\s*$/.test(value)) return Number(value.match(/[0-9]+(?:\.[0-9]+)?/)![0]) * 0.3048
  return null
}

const first = (...values: (string | undefined)[]) => values.find(v => v != null)

/** road_parking_width */
export function parkingWidth(tags: Tags, side: 'left' | 'right'): number {
  const v = first(tags[`parking:${side}`], tags['parking:both'], tags[`parking:lane:${side}`], tags['parking:lane:both'])
  const o = first(tags[`parking:${side}:orientation`], tags['parking:both:orientation'])
  if (v && ['lane', 'parallel', 'diagonal', 'perpendicular', 'marked'].includes(v)) {
    const kind = o ?? v
    return kind === 'diagonal' ? 4.5 : kind === 'perpendicular' ? 5.0 : 2.4
  }
  return v === 'half_on_kerb' ? 1.2 : 0
}

interface Bike { width: number; buffer: number; track: boolean }

/** road_bike_barred */
function barredBuffer(tags: Tags, side: 'left' | 'right', oneway: boolean): Tags {
  const marks = [`cycleway:${side}:marking:left`, `cycleway:${side}:marking:right`, 'cycleway:both:marking:left', 'cycleway:both:marking:right']
  const buffers = [`cycleway:${side}:buffer`, 'cycleway:both:buffer', 'cycleway:buffer']
  if (!marks.some(k => k in tags) || buffers.some(k => k in tags)) return tags
  const value = first(tags[`cycleway:${side}`], tags['cycleway:both'], tags.cycleway)
  const against = (value != null && ['opposite_lane', 'opposite_track'].includes(value))
    || tags[`cycleway:${side}:oneway`] === '-1'
    || (side === 'left' && !oneway && (tags['cycleway:left:oneway'] ?? '-1') === '-1')
  const kerbOnLeft = (side === 'left') === !against
  const traffic = kerbOnLeft ? 'right' : 'left'
  const barred = [tags[`cycleway:${side}:marking:${traffic}`], tags[`cycleway:both:marking:${traffic}`]].includes('barred_area')
  return barred ? { ...tags, [`cycleway:${side}:buffer`]: 'yes' } : tags
}

/** road_bike */
function bikeLane(tags: Tags, side: 'left' | 'right', oneway: boolean, americas: boolean): Bike {
  const c = tags.cycleway
  const v = first(tags[`cycleway:${side}`], tags['cycleway:both'],
    side === 'right' && c != null && !['opposite_lane', 'opposite_track'].includes(c) ? c : undefined,
    side === 'left' && (!oneway || (c != null && ['opposite_lane', 'opposite_track'].includes(c))) ? c : undefined)
  const w = tagMetres(first(tags[`cycleway:${side}:width`], tags['cycleway:both:width']))
  const bv = first(tags[`cycleway:${side}:buffer`], tags['cycleway:both:buffer'], tags['cycleway:buffer'])
  const b = tagMetres(bv) ?? (bv === 'yes' ? 0.9 : 0)
  if (v && ['lane', 'exclusive_lane', 'opposite_lane'].includes(v)) return { width: (w ?? 1.6) + b, buffer: b, track: false }
  if (v && ['track', 'opposite_track'].includes(v) && carFree(tags.highway)) return { width: (w ?? 1.8) + b, buffer: b, track: false }
  if (v && ['track', 'opposite_track'].includes(v) && americas) {
    const buffer = Math.max(b, 0.9)
    return { width: (w ?? 1.8) + buffer, buffer, track: true }
  }
  return { width: 0, buffer: 0, track: false }
}

/** road_bus_lanes: 1-based indices, left to right as traffic sees them. */
export function busLanes(perLane: string | undefined, kerbCount: number | null, n: number, side: DrivingSide): number[] {
  if (n > 0 && perLane != null) {
    const list = perLane.split('|')
    if (list.length === n) return list.flatMap((v, i) => (v.trim() === 'designated' ? [i + 1] : []))
  }
  if (kerbCount != null && kerbCount >= 1 && kerbCount <= n) {
    return Array.from({ length: kerbCount }, (_, i) => (side === 'right' ? n - kerbCount + 1 + i : i + 1))
  }
  return []
}

/** road_turn_bays: lanes at the left that only turn left, and at the right that only turn right. */
export function turnBays(turns: string | undefined, n: number): [number, number] {
  if (turns == null || n < 2) return [0, 0]
  const lanes = turns.split('|')
  if (lanes.length !== n) return [0, 0]
  const leftOnly = lanes.map(v => /(^|;)\s*(left|sharp_left|slight_left|reverse)\s*(;|$)/.test(v) && !/through|right/.test(v))
  const rightOnly = lanes.map(v => /(^|;)\s*(right|sharp_right|slight_right)\s*(;|$)/.test(v) && !/through|left|reverse/.test(v))
  const firstNotLeft = leftOnly.findIndex(x => !x)
  const lastNotRight = rightOnly.map((x, i) => (x ? -1 : i)).reduce((a, b) => Math.max(a, b), -1)
  const lb = firstNotLeft < 0 ? n : firstNotLeft
  const rb = n - (lastNotRight + 1)
  return lb + rb >= n ? [0, 0] : [lb, rb]
}

/** road_place: the way's offset a placement value puts it at. */
function place(value: string | undefined, edge: number, laneW: number, dir: number, lanes: number): number | null {
  const m = value?.trim().match(/^(left_of|right_of|middle_of):([0-9]+)$/)
  if (!m) return null
  const n = tagInt(m[2])
  if (n == null || n < 1 || n > lanes) return null
  const k = m[1] === 'left_of' ? n - 1 : m[1] === 'right_of' ? n : n - 0.5
  return edge + dir * laneW * k
}

/** A turn:lanes list split per lane, or null when it does not name `n` lanes. */
export function laneTurns(turns: string | undefined, n: number): string[][] | null {
  if (turns == null || n < 1) return null
  const lanes = turns.split('|')
  if (lanes.length !== n) return null
  return lanes.map(v => v.split(';').map(t => t.trim()).filter(t => t && t !== 'none'))
}

export function isOneway(tags: Tags): boolean {
  const o = tags.oneway
  const yes = (o != null && ['yes', 'true', '1', '-1'].includes(o))
    || ['roundabout', 'circular'].includes(tags.junction ?? '')
    || (['motorway', 'motorway_link'].includes(tags.highway ?? '') && o !== 'no')
  return yes && !(carFree(tags.highway) && tags['oneway:bicycle'] === 'no')
}

/**
 * The lanes of a way. The sidewalk-derived room beside the lanes and the
 * tapers between ways need the ways around it, so they are left out: they
 * move the kerbs, not the lanes.
 */
export function crossSection(tags: Tags, opts: { americas: boolean; side: DrivingSide }): CrossSection {
  const highway = tags.highway
  const flip = tags.oneway === '-1'
  const oneway = isOneway(tags)
  // OSM's left and right, as the traffic direction sees them.
  const osmRight = flip ? 'left' : 'right'
  const osmLeft = flip ? 'right' : 'left'

  const lanesTag = tagInt(tags.lanes)
  const bothWays = tagInt(tags['lanes:both_ways']) ?? 0
  const widthTag = tagMetres(tags['width:carriageway']) ?? tagMetres(tags.width)
  const bikeR = bikeLane(barredBuffer(tags, osmRight, oneway), osmRight, oneway, opts.americas)
  const bikeL = bikeLane(barredBuffer(tags, osmLeft, oneway), osmLeft, oneway, opts.americas)
  let parkR = parkingWidth(tags, osmRight)
  let parkL = parkingWidth(tags, osmLeft)
  const buswayR = first(tags[`busway:${osmRight}`], tags['busway:both'], tags.busway) === 'lane'
  const buswayL = first(tags[`busway:${osmLeft}`], tags['busway:both'], !oneway ? tags.busway : undefined) === 'lane'

  const lanes = lanesTag ?? (
    (highway === 'motorway' || highway === 'trunk') && oneway ? 2
      : highway === 'pedestrian' ? 2
        : oneway ? 1
          : highway === 'service' ? 1
            : 2)
  const forwardTag = tagInt(tags['lanes:forward'])
  const half = Math.ceil((lanes - bothWays) / 2)
  const fwd = Math.max(oneway ? lanes - bothWays : forwardTag ?? half, 0)
  const bwd = Math.max(oneway ? 0 : tagInt(tags['lanes:backward']) ?? lanes - bothWays - (forwardTag ?? half), 0)
  const n = fwd + bwd + bothWays

  const width = widthTag != null && widthTag >= 2.5 && widthTag <= 60
    ? widthTag
    : n * laneWidthFor(highway) + bikeR.width + bikeL.width + parkR + parkL
  if (parkR + parkL > 0 && (width - bikeR.width - bikeL.width - parkR - parkL) / Math.max(n, 1) < 2.6) {
    parkR = 0
    parkL = 0
  }
  const laneW = (width - bikeR.width - bikeL.width - parkR - parkL) / Math.max(n, 1)

  const turnForward = tags['turn:lanes:forward'] ?? (oneway ? tags['turn:lanes'] : undefined)
  const turnBackward = tags['turn:lanes:backward']

  const busCountF = first(tags['lanes:bus:forward'], tags['lanes:psv:forward'])
  const busCountB = first(tags['lanes:bus:backward'], tags['lanes:psv:backward'])
  const busF = busLanes(
    first(tags['bus:lanes:forward'], tags['psv:lanes:forward'], oneway ? first(tags['bus:lanes'], tags['psv:lanes']) : undefined),
    tagInt(busCountF) ?? (oneway ? tagInt(first(tags['lanes:bus'], tags['lanes:psv'])) : halfOf(tagInt(tags['lanes:bus']))),
    fwd, opts.side)
  const busB = busLanes(
    first(tags['bus:lanes:backward'], tags['psv:lanes:backward']),
    tagInt(busCountB) ?? (!oneway ? halfOf(tagInt(tags['lanes:bus'])) : null),
    bwd, opts.side)
  const kerbLane = (count: number) => (opts.side === 'right' ? count : 1)
  const busForward = busF.length ? busF : [buswayR ? kerbLane(fwd) : 0, buswayL && oneway ? (opts.side === 'right' ? 1 : fwd) : 0].filter(x => x > 0)
  const busBackward = busB.length ? busB : [buswayL && !oneway ? kerbLane(bwd) : 0].filter(x => x > 0)

  const mirror = opts.side === 'left' ? -1 : 1
  let leftEdge = width / 2 - bikeL.width - parkL
  let rightEdge = -width / 2 + bikeR.width + parkR

  // Where the lanes stand: placement, or a turn bay hanging off its own side.
  const bf = turnBays(turnForward, fwd)
  const bb = turnBays(turnBackward, bwd)
  const splitB0 = leftEdge - bwd * laneW
  const splitF0 = splitB0 - bothWays * laneW
  const placed = flip
    ? place(tags['placement:backward'], leftEdge, laneW, -1, fwd) ?? place(tags.placement, leftEdge, laneW, -1, fwd)
    : place(tags['placement:forward'], oneway ? leftEdge : splitF0, laneW, -1, fwd)
      ?? place(tags['placement:backward'], splitB0, laneW, 1, bwd)
      ?? place(tags.placement, leftEdge, laneW, -1, n)
  const base = placed != null
    ? -placed
    : (oneway ? (bf[0] - bf[1]) * laneW / 2 : (bf[0] - bf[1] - bb[0] + bb[1]) * laneW / 2) * mirror
  // With no room read beside the lanes, only placement and highways move them.
  const shift = placed != null || MOTORWAY_TRUNK.has(highway ?? '') ? base : 0
  leftEdge += shift
  rightEdge += shift

  const forwardTurns = laneTurns(turnForward, fwd)
  const backwardTurns = laneTurns(turnBackward, bwd)
  const motor = (count: number, at: (i: number) => number, turns: string[][] | null, bus: number[]): CrossSectionLane[] =>
    Array.from({ length: count }, (_, k) => ({
      index: k + 1,
      offset: at(k + 1),
      width: laneW,
      turns: turns?.[k] ?? [],
      bus: bus.includes(k + 1),
      bike: false,
    }))

  // Right-hand traffic: backward lanes from the left edge, then any centre
  // turn lane, then forward lanes; left-hand traffic the other way round.
  const forward = opts.side === 'right'
    ? motor(fwd, i => leftEdge - (bwd + bothWays) * laneW - (i - 0.5) * laneW, forwardTurns, busForward)
    : motor(fwd, i => leftEdge - (i - 0.5) * laneW, forwardTurns, busForward)
  const backward = opts.side === 'right'
    ? motor(bwd, i => leftEdge - bwd * laneW + (i - 0.5) * laneW, backwardTurns, busBackward)
    : motor(bwd, i => rightEdge + (i - 0.5) * laneW, backwardTurns, busBackward)

  // The kerbs take a turn bay's step where the lanes do not.
  const kshift = base
  const bikeBand = (bike: Bike, onRight: boolean): CrossSectionLane | null => {
    if (bike.width <= 0) return null
    const kerbSide = onRight ? -width / 2 + (bike.track ? 0 : parkR) : width / 2 - (bike.track ? 0 : parkL)
    const paint = bike.width - bike.buffer
    const offset = kerbSide + (bike.track ? kshift : shift) + (onRight ? paint / 2 : -paint / 2)
    return { index: 0, offset, width: paint, turns: [], bus: false, bike: true }
  }
  const contraflowLeft = ['opposite_lane', 'opposite_track'].includes(first(tags[`cycleway:${osmLeft}`], tags.cycleway) ?? '')
  const bikeRight = bikeBand(bikeR, true)
  const bikeLeft = bikeBand(bikeL, false)
  const rightBelongsForward = opts.side === 'right' || oneway
  const leftBelongsForward = opts.side === 'left' || (oneway && !contraflowLeft)
  const withBikes = (lanes: CrossSectionLane[], extra: (CrossSectionLane | null)[], forwardDir: boolean) => {
    const all = [...lanes, ...extra.filter((x): x is CrossSectionLane => x != null)]
    // Left to right as the lane's traffic sees it: descending offset going forward.
    all.sort((a, b) => (forwardDir ? b.offset - a.offset : a.offset - b.offset))
    return all.map((lane, i) => ({ ...lane, index: i + 1 }))
  }

  return {
    flip,
    oneway,
    width,
    laneWidth: laneW,
    forward: withBikes(forward, [rightBelongsForward ? bikeRight : null, leftBelongsForward ? bikeLeft : null], true),
    backward: withBikes(backward, [rightBelongsForward ? null : bikeRight, leftBelongsForward ? null : bikeLeft], false),
    kerbLeft: width / 2 + kshift,
    kerbRight: -width / 2 + kshift,
  }
}

function halfOf(n: number | null): number | null {
  return n == null ? null : Math.trunc(n / 2)
}
