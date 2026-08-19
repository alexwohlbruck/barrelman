/**
 * Vendor extensions on GTFS-RT ServiceAlerts.
 *
 * The spec gives an alert `cause`, `effect` and `severity_level`, and plenty of
 * agencies set none of them. Every MTA alert sampled — all 179 on the subway
 * feed, all 82 on the bus feed — arrives as UNKNOWN_CAUSE / UNKNOWN_EFFECT /
 * UNKNOWN_SEVERITY, because MTA puts the real category in a protobuf extension
 * instead: "Planned - Detour", "Planned - Suspended", "Delays". Without reading
 * it, a full line suspension is indistinguishable from a poster about an
 * escalator, and every alert in New York renders as an unlabelled grey note.
 *
 * Extensions live on the `Alert` message at agency-assigned field numbers that
 * the stock `gtfs-realtime-bindings` decoder doesn't know and drops. So this
 * module walks the raw feed itself, hands each registered extension its own
 * field's bytes, and returns whatever enrichment they yield. Adding an agency
 * is one entry in `ALERT_EXTENSIONS` — a field number and a `read()` — with no
 * change to the alerts service.
 *
 * Enrichment never overrides the feed: the alerts service only reaches for it
 * where the spec fields were left unset.
 */

// ── Minimal protobuf wire reader ────────────────────────────────────
//
// Only what walking to an extension's bytes needs: varints, length-delimited
// fields, and enough of the fixed widths to skip past them. Hand-rolled rather
// than pulled from `protobufjs`, which reaches us only as a transitive
// dependency of the bindings and could vanish under a minor bump of theirs.

class WireReader {
  private pos = 0

  constructor(private readonly buf: Uint8Array) {}

  get done(): boolean {
    return this.pos >= this.buf.length
  }

  /** Base-128 varint. Values beyond 2^53 are not something a feed carries. */
  varint(): number {
    let result = 0
    let shift = 0
    while (this.pos < this.buf.length) {
      const byte = this.buf[this.pos++]
      result += (byte & 0x7f) * Math.pow(2, shift)
      if ((byte & 0x80) === 0) break
      shift += 7
    }
    return result
  }

  /** Field number and wire type from the next tag. */
  tag(): { field: number; wire: number } {
    const tag = this.varint()
    return { field: tag >>> 3, wire: tag & 7 }
  }

  bytes(): Uint8Array {
    const length = this.varint()
    const start = this.pos
    this.pos = Math.min(this.pos + length, this.buf.length)
    return this.buf.subarray(start, this.pos)
  }

  /** Advance past a field whose value we don't want. */
  skip(wire: number): void {
    switch (wire) {
      case 0: this.varint(); break
      case 1: this.pos += 8; break
      case 2: this.bytes(); break
      case 5: this.pos += 4; break
      // Groups (3/4) were removed from proto3 and no transit feed emits them.
      default: this.pos = this.buf.length
    }
  }
}

const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

// ── Types ───────────────────────────────────────────────────────────

/** What an extension can tell us that the spec fields didn't. */
export interface AlertEnrichment {
  /** The agency's own category, verbatim — e.g. "Planned - Detour". */
  category?: string
  /** The agency scheduled this rather than reacting to it. */
  planned?: boolean
  /** GTFS-RT effect the category implies. */
  effect?: string
  /** GTFS-RT severity the category implies. */
  severity?: string
  /** When the agency published it — the "Posted on" line riders recognise. */
  postedAt?: string
  /** When they last revised it. */
  updatedAt?: string
}

export interface AlertExtension {
  /** For diagnostics; not used in matching. */
  name: string
  /** Field number this extension occupies on the `Alert` message. */
  field: number
  /** Read one alert's extension payload. Return null if it says nothing useful. */
  read(bytes: Uint8Array): AlertEnrichment | null
}

// ── MTA / Mercury ───────────────────────────────────────────────────
//
// MTA's feeds are produced by Cubic/Mercury, which attaches a `MercuryAlert`
// at field 1001. The fields below are the ones that survive into anything a
// rider sees; the message carries more (display-before-active windows, station
// alternatives) that we have no use for.

const MERCURY_FIELD = 1001
const MERCURY_CREATED_AT = 1
const MERCURY_UPDATED_AT = 2
const MERCURY_ALERT_TYPE = 3

/**
 * Mercury's `alert_type` vocabulary, mapped onto the GTFS-RT effect it means.
 *
 * Keys are normalised: the "Planned - " prefix is stripped (it's tracked
 * separately, since it's the difference between "the agency chose this" and
 * "something has gone wrong") and the rest lowercased. An unrecognised type
 * still yields its category and timestamps — we just don't claim to know the
 * effect.
 */
const MERCURY_EFFECTS: Record<string, string> = {
  'suspended': 'NO_SERVICE',
  'part suspended': 'REDUCED_SERVICE',
  'reduced service': 'REDUCED_SERVICE',
  'detour': 'DETOUR',
  'reroute': 'DETOUR',
  // No GTFS-RT effect means "skipping your stop". Detour is what it costs a
  // rider — the service runs, but not where they're standing.
  'stops skipped': 'DETOUR',
  'express to local': 'MODIFIED_SERVICE',
  'local to express': 'MODIFIED_SERVICE',
  'extra transfer': 'MODIFIED_SERVICE',
  'special schedule': 'MODIFIED_SERVICE',
  'slow speeds': 'SIGNIFICANT_DELAYS',
  'delays': 'SIGNIFICANT_DELAYS',
  'expect delays': 'SIGNIFICANT_DELAYS',
  'some delays': 'SIGNIFICANT_DELAYS',
  'boarding change': 'STOP_MOVED',
  'extra service': 'ADDITIONAL_SERVICE',
  'station notice': 'OTHER_EFFECT',
  'special notice': 'OTHER_EFFECT',
  'service change': 'MODIFIED_SERVICE',
  'escalator': 'ACCESSIBILITY_ISSUE',
  'elevator': 'ACCESSIBILITY_ISSUE',
}

/**
 * How loud an effect should be, given whether the agency planned it.
 *
 * Planned work is never the top rung: a closure the agency scheduled and
 * posted weeks ago is worth an amber warning, not the red reserved for a line
 * that has just stopped running under you.
 */
export function severityForEffect(effect: string, planned: boolean): string {
  switch (effect) {
    case 'NO_SERVICE':
      return planned ? 'WARNING' : 'SEVERE'
    case 'SIGNIFICANT_DELAYS':
    case 'REDUCED_SERVICE':
    case 'DETOUR':
    case 'STOP_MOVED':
      return 'WARNING'
    default:
      return 'INFO'
  }
}

/** Strip the planned marker and normalise for lookup. */
export function normaliseCategory(category: string): { key: string; planned: boolean } {
  const trimmed = category.trim()
  // \b so a category that merely starts with the letters — "Plannedish" — is
  // not read as a planned-work marker with a nonsense category behind it.
  const match = /^planned\b\s*[-–—:]?\s*/i.exec(trimmed)
  return {
    key: (match ? trimmed.slice(match[0].length) : trimmed).toLowerCase().trim(),
    planned: Boolean(match),
  }
}

export const mercuryExtension: AlertExtension = {
  name: 'mercury',
  field: MERCURY_FIELD,

  read(bytes) {
    const reader = new WireReader(bytes)
    let category: string | undefined
    let createdAt: number | undefined
    let updatedAt: number | undefined

    while (!reader.done) {
      const { field, wire } = reader.tag()
      if (field === MERCURY_ALERT_TYPE && wire === 2) category = decodeUtf8(reader.bytes())
      else if (field === MERCURY_CREATED_AT && wire === 0) createdAt = reader.varint()
      else if (field === MERCURY_UPDATED_AT && wire === 0) updatedAt = reader.varint()
      else reader.skip(wire)
    }

    if (!category && !createdAt && !updatedAt) return null

    const enrichment: AlertEnrichment = {
      postedAt: createdAt ? new Date(createdAt * 1000).toISOString() : undefined,
      updatedAt: updatedAt ? new Date(updatedAt * 1000).toISOString() : undefined,
    }

    if (category) {
      const { key, planned } = normaliseCategory(category)
      const effect = MERCURY_EFFECTS[key]
      enrichment.category = category
      enrichment.planned = planned
      if (effect) {
        enrichment.effect = effect
        enrichment.severity = severityForEffect(effect, planned)
      }
    }

    return enrichment
  },
}

// ── Registry ────────────────────────────────────────────────────────

/**
 * Every extension we know how to read. Add an agency here and the alerts
 * service picks it up — nothing else needs to change.
 */
export const ALERT_EXTENSIONS: AlertExtension[] = [mercuryExtension]

// GTFS-RT field numbers on the messages we walk through.
const FEED_ENTITY = 2
const ENTITY_ALERT = 5

/**
 * Walk a raw feed and pull out each alert's extension enrichment.
 *
 * Keyed by the entity's position in the feed, which is the same order the
 * bindings' decoded `entity` array uses — so the caller can pair enrichment to
 * alert by index without depending on entity ids, which not every feed sets.
 *
 * Feeds with no registered extension cost one linear scan that matches nothing.
 * Anything malformed yields an empty map rather than throwing: enrichment is an
 * improvement on the alert, never a precondition for showing it.
 */
export function readAlertExtensions(
  feed: Uint8Array,
  extensions: AlertExtension[] = ALERT_EXTENSIONS,
): Map<number, AlertEnrichment> {
  const found = new Map<number, AlertEnrichment>()
  if (extensions.length === 0) return found

  const byField = new Map(extensions.map(e => [e.field, e]))

  try {
    const reader = new WireReader(feed)
    let entityIndex = 0

    while (!reader.done) {
      const { field, wire } = reader.tag()
      if (field !== FEED_ENTITY || wire !== 2) {
        reader.skip(wire)
        continue
      }

      const index = entityIndex++
      const entity = new WireReader(reader.bytes())

      while (!entity.done) {
        const { field: entityField, wire: entityWire } = entity.tag()
        if (entityField !== ENTITY_ALERT || entityWire !== 2) {
          entity.skip(entityWire)
          continue
        }

        const alert = new WireReader(entity.bytes())
        while (!alert.done) {
          const { field: alertField, wire: alertWire } = alert.tag()
          const extension = byField.get(alertField)
          if (extension && alertWire === 2) {
            const enrichment = extension.read(alert.bytes())
            if (enrichment) found.set(index, { ...found.get(index), ...enrichment })
          } else {
            alert.skip(alertWire)
          }
        }
      }
    }
  } catch {
    return new Map()
  }

  return found
}
