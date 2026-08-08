import { describe, expect, test } from 'bun:test'
import { describeCron, isValidCron, nextRun, parseCron } from './cron'

/** Fixed instant helper — all expectations are anchored, never "now". */
const at = (iso: string) => new Date(iso)

describe('parseCron', () => {
  test('expands wildcards, ranges, steps and lists', () => {
    const f = parseCron('0,30 9-17/4 * * mon-fri')
    expect([...f.minute].sort((a, b) => a - b)).toEqual([0, 30])
    expect([...f.hour].sort((a, b) => a - b)).toEqual([9, 13, 17])
    expect(f.dayOfMonth.size).toBe(31)
    expect([...f.dayOfWeek].sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5])
    expect(f.dowRestricted).toBe(true)
    expect(f.domRestricted).toBe(false)
  })

  test('treats weekday 7 as Sunday', () => {
    expect([...parseCron('0 0 * * 7').dayOfWeek]).toEqual([0])
  })

  test('accepts nicknames', () => {
    expect([...parseCron('@daily').hour]).toEqual([0])
    expect([...parseCron('@hourly').minute]).toEqual([0])
  })

  test('rejects malformed expressions', () => {
    for (const bad of ['', '0 3 * *', '60 3 * * *', '0 24 * * *', '0 3 * * 8', 'nonsense', '0 3 * * mon-']) {
      expect(isValidCron(bad)).toBe(false)
    }
  })
})

describe('nextRun', () => {
  test('finds the next daily occurrence in UTC', () => {
    expect(nextRun('0 3 * * *', at('2026-08-08T01:00:00Z'))?.toISOString()).toBe('2026-08-08T03:00:00.000Z')
    expect(nextRun('0 3 * * *', at('2026-08-08T05:00:00Z'))?.toISOString()).toBe('2026-08-09T03:00:00.000Z')
  })

  test('is strictly after the given instant', () => {
    // Exactly on a firing minute must advance, or a schedule would re-fire in a
    // tight loop for the remainder of that minute.
    expect(nextRun('0 3 * * *', at('2026-08-08T03:00:00Z'))?.toISOString()).toBe('2026-08-09T03:00:00.000Z')
  })

  test('resolves wall-clock time in the schedule timezone', () => {
    // 03:00 America/New_York is 07:00 UTC in August (EDT, UTC-4).
    expect(nextRun('0 3 * * *', at('2026-08-08T01:00:00Z'), 'America/New_York')?.toISOString()).toBe(
      '2026-08-08T07:00:00.000Z',
    )
    // …and 08:00 UTC in January (EST, UTC-5). Same expression, DST handled.
    expect(nextRun('0 3 * * *', at('2026-01-08T01:00:00Z'), 'America/New_York')?.toISOString()).toBe(
      '2026-01-08T08:00:00.000Z',
    )
  })

  test('skips the hour that does not exist on spring-forward', () => {
    // US DST 2026 begins 2026-03-08; 02:30 local never happens that day, so a
    // 02:30 schedule must roll to the next day rather than fire twice or hang.
    const next = nextRun('30 2 * * *', at('2026-03-08T05:00:00Z'), 'America/New_York')
    expect(next?.toISOString()).toBe('2026-03-09T06:30:00.000Z')
  })

  test('applies the POSIX either-or rule when both day fields are restricted', () => {
    // "1st of the month OR any Monday". 2026-09-01 is a Tuesday, so it matches
    // on day-of-month alone…
    expect(nextRun('0 0 1 * mon', at('2026-08-31T12:00:00Z'))?.toISOString()).toBe('2026-09-01T00:00:00.000Z')
    // …while 2026-08-31 is a Monday, matching on day-of-week alone. An AND
    // reading would have skipped both and jumped to a Monday the 1st.
    expect(nextRun('0 0 1 * mon', at('2026-08-29T12:00:00Z'))?.toISOString()).toBe('2026-08-31T00:00:00.000Z')
  })

  test('crosses a year boundary for a sparse schedule', () => {
    expect(nextRun('0 0 1 1 *', at('2026-08-08T00:00:00Z'))?.toISOString()).toBe('2027-01-01T00:00:00.000Z')
  })

  test('returns null when nothing can ever match', () => {
    expect(nextRun('0 0 30 2 *', at('2026-08-08T00:00:00Z'))).toBeNull()
  })
})

describe('describeCron', () => {
  test('summarises the common shapes', () => {
    expect(describeCron('0 3 * * *')).toBe('Daily at 03:00 UTC')
    expect(describeCron('0 3 * * *', 'America/New_York')).toBe('Daily at 03:00 America/New_York')
    expect(describeCron('30 4 * * 0')).toBe('Sunday at 04:30 UTC')
    expect(describeCron('0 * * * *')).toBe('Hourly, on the hour')
  })

  test('falls back to the raw expression', () => {
    expect(describeCron('*/7 2,5 * * *')).toBe('*/7 2,5 * * *')
    expect(describeCron('not a cron')).toBe('not a cron')
  })
})
