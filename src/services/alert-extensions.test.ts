import { describe, test, expect } from 'bun:test'
import {
  readAlertExtensions,
  mercuryExtension,
  normaliseCategory,
  severityForEffect,
  type AlertExtension,
} from './alert-extensions'

/**
 * Reading what the stock decoder throws away.
 *
 * The fixtures here are hand-encoded protobuf rather than a recorded feed,
 * because the thing under test *is* the wire walk — a captured `.pb` would
 * make the test pass or fail for reasons hidden inside the bytes. Building
 * them here keeps every field number and wire type visible.
 */

// ── Wire encoding helpers ───────────────────────────────────────────

function varint(value: number): number[] {
  const out: number[] = []
  let v = value
  while (v > 0x7f) {
    out.push((v & 0x7f) | 0x80)
    v = Math.floor(v / 128)
  }
  out.push(v)
  return out
}

const tag = (field: number, wire: number) => varint(field * 8 + wire)

/** Length-delimited field: tag, length, payload. */
function delimited(field: number, payload: number[]): number[] {
  return [...tag(field, 2), ...varint(payload.length), ...payload]
}

function str(field: number, value: string): number[] {
  return delimited(field, [...new TextEncoder().encode(value)])
}

function uint(field: number, value: number): number[] {
  return [...tag(field, 0), ...varint(value)]
}

// ── Fixture builders ────────────────────────────────────────────────

const MERCURY_FIELD = 1001

function mercuryPayload(opts: { type?: string; createdAt?: number; updatedAt?: number }): number[] {
  return [
    ...(opts.createdAt ? uint(1, opts.createdAt) : []),
    ...(opts.updatedAt ? uint(2, opts.updatedAt) : []),
    ...(opts.type ? str(3, opts.type) : []),
  ]
}

/** An Alert message: header text at field 10, extension at its own field. */
function alertMessage(header: string, extension?: { field: number; payload: number[] }): number[] {
  return [
    ...delimited(10, str(1, header)), // header_text: TranslatedString{translation}
    ...(extension ? delimited(extension.field, extension.payload) : []),
  ]
}

/** A FeedMessage carrying the given entities (field 2), each wrapping an alert. */
function feedMessage(entities: Array<number[] | null>): Uint8Array {
  const bytes: number[] = []
  entities.forEach((alert, i) => {
    const entity = [
      ...str(1, `entity-${i}`),
      ...(alert ? delimited(5, alert) : []),
    ]
    bytes.push(...delimited(2, entity))
  })
  return new Uint8Array(bytes)
}

// ── Mercury mapping ─────────────────────────────────────────────────

describe('normaliseCategory', () => {
  test('separates the planned marker from the category itself', () => {
    expect(normaliseCategory('Planned - Detour')).toEqual({ key: 'detour', planned: true })
    expect(normaliseCategory('Detour')).toEqual({ key: 'detour', planned: false })
  })

  test('tolerates the punctuation agencies vary on', () => {
    expect(normaliseCategory('Planned – Stops Skipped').key).toBe('stops skipped')
    expect(normaliseCategory('Planned: Suspended')).toEqual({ key: 'suspended', planned: true })
    expect(normaliseCategory('planned  reroute')).toEqual({ key: 'reroute', planned: true })
  })

  test('does not mistake a category that merely starts with the word', () => {
    expect(normaliseCategory('Plannedish Thing')).toEqual({
      key: 'plannedish thing',
      planned: false,
    })
  })
})

describe('severityForEffect', () => {
  test('a suspension the agency chose is quieter than one that just happened', () => {
    expect(severityForEffect('NO_SERVICE', true)).toBe('WARNING')
    expect(severityForEffect('NO_SERVICE', false)).toBe('SEVERE')
  })

  test('disruptions warn, conveniences inform', () => {
    expect(severityForEffect('DETOUR', true)).toBe('WARNING')
    expect(severityForEffect('SIGNIFICANT_DELAYS', false)).toBe('WARNING')
    expect(severityForEffect('ADDITIONAL_SERVICE', false)).toBe('INFO')
    expect(severityForEffect('OTHER_EFFECT', false)).toBe('INFO')
  })
})

describe('mercuryExtension.read', () => {
  test('maps an alert type onto an effect and a severity', () => {
    const result = mercuryExtension.read(
      new Uint8Array(mercuryPayload({ type: 'Planned - Detour', createdAt: 1_787_111_937 })),
    )

    expect(result).toMatchObject({
      category: 'Planned - Detour',
      planned: true,
      effect: 'DETOUR',
      severity: 'WARNING',
      postedAt: new Date(1_787_111_937_000).toISOString(),
    })
  })

  test('an unplanned suspension is the loudest thing the mapping produces', () => {
    const result = mercuryExtension.read(new Uint8Array(mercuryPayload({ type: 'Suspended' })))

    expect(result).toMatchObject({ effect: 'NO_SERVICE', severity: 'SEVERE', planned: false })
  })

  test('keeps the category of a type it has no mapping for, and claims no effect', () => {
    const result = mercuryExtension.read(
      new Uint8Array(mercuryPayload({ type: 'Fare Machine Outage' })),
    )

    expect(result?.category).toBe('Fare Machine Outage')
    expect(result?.effect).toBeUndefined()
    expect(result?.severity).toBeUndefined()
  })

  test('a payload with nothing we use yields nothing', () => {
    expect(mercuryExtension.read(new Uint8Array(mercuryPayload({})))).toBeNull()
  })
})

// ── Walking the feed ────────────────────────────────────────────────

describe('readAlertExtensions', () => {
  test('pairs enrichment to the entity it came from', () => {
    const feed = feedMessage([
      alertMessage('First', {
        field: MERCURY_FIELD,
        payload: mercuryPayload({ type: 'Delays' }),
      }),
      alertMessage('Second', {
        field: MERCURY_FIELD,
        payload: mercuryPayload({ type: 'Planned - Suspended' }),
      }),
    ])

    const found = readAlertExtensions(feed)

    expect(found.get(0)).toMatchObject({ effect: 'SIGNIFICANT_DELAYS' })
    expect(found.get(1)).toMatchObject({ effect: 'NO_SERVICE', planned: true })
  })

  test('counts every entity, so indexes still line up past one with no extension', () => {
    const feed = feedMessage([
      alertMessage('Bare'),
      alertMessage('Enriched', {
        field: MERCURY_FIELD,
        payload: mercuryPayload({ type: 'Detour' }),
      }),
    ])

    const found = readAlertExtensions(feed)

    expect(found.has(0)).toBe(false)
    expect(found.get(1)).toMatchObject({ effect: 'DETOUR' })
  })

  test('indexes count non-alert entities too — a combined feed must not shift', () => {
    // Entity 0 carries no alert at all (a vehicle position in a combined feed).
    const feed = feedMessage([
      null,
      alertMessage('Enriched', {
        field: MERCURY_FIELD,
        payload: mercuryPayload({ type: 'Detour' }),
      }),
    ])

    const found = readAlertExtensions(feed)

    expect(found.get(1)).toMatchObject({ effect: 'DETOUR' })
  })

  test('ignores extension fields no registered reader claims', () => {
    const feed = feedMessage([
      alertMessage('Other vendor', { field: 2001, payload: mercuryPayload({ type: 'Detour' }) }),
    ])

    expect(readAlertExtensions(feed).size).toBe(0)
  })

  test('a new agency is one registry entry — nothing else changes', () => {
    const acme: AlertExtension = {
      name: 'acme',
      field: 2001,
      read: () => ({ category: 'Acme Disruption', effect: 'REDUCED_SERVICE' }),
    }
    const feed = feedMessage([
      alertMessage('Other vendor', { field: 2001, payload: mercuryPayload({ type: 'whatever' }) }),
    ])

    const found = readAlertExtensions(feed, [acme])

    expect(found.get(0)).toMatchObject({ category: 'Acme Disruption', effect: 'REDUCED_SERVICE' })
  })

  test('a feed with no extensions at all costs a scan and returns nothing', () => {
    const feed = feedMessage([alertMessage('Plain'), alertMessage('Also plain')])

    expect(readAlertExtensions(feed).size).toBe(0)
  })

  test('truncated bytes yield no enrichment rather than throwing', () => {
    const feed = feedMessage([
      alertMessage('First', {
        field: MERCURY_FIELD,
        payload: mercuryPayload({ type: 'Delays' }),
      }),
    ])

    expect(() => readAlertExtensions(feed.subarray(0, feed.length - 4))).not.toThrow()
  })
})
