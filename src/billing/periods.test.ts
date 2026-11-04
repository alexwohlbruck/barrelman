/**
 * Billing periods decide when an allowance resets. A subscribed account's
 * period has to land on the day Polar bills it, or a mid-month subscriber gets
 * a second allowance two weeks after paying for the first.
 */
import { describe, test, expect } from 'bun:test'
import { addMonths, anchoredPeriod, billingPeriod, calendarPeriod, periodsBetween, previousPeriod } from './periods'
import { getPlan } from './plans'

const free = getPlan('free')
const developer = getPlan('developer')
const enterprise = getPlan('enterprise')

describe('calendarPeriod', () => {
  test('is the UTC month containing the date', () => {
    expect(calendarPeriod(new Date('2026-11-04T12:00:00Z'))).toEqual({ start: '2026-11-01', end: '2026-12-01' })
    expect(calendarPeriod('2026-12-31')).toEqual({ start: '2026-12-01', end: '2027-01-01' })
  })
})

describe('addMonths', () => {
  test('keeps the day of month', () => {
    expect(addMonths('2026-01-14', 1)).toBe('2026-02-14')
    expect(addMonths('2026-11-14', 2)).toBe('2027-01-14')
    expect(addMonths('2026-03-14', -3)).toBe('2025-12-14')
  })

  test('clamps to the end of a shorter month, and returns to the day after', () => {
    expect(addMonths('2026-01-31', 1)).toBe('2026-02-28')
    expect(addMonths('2026-01-31', 2)).toBe('2026-03-31')
    expect(addMonths('2028-01-31', 1)).toBe('2028-02-29')
  })
})

describe('anchoredPeriod', () => {
  test('runs from the anchor day to the same day next month', () => {
    expect(anchoredPeriod('2026-10-14T15:30:00Z', '2026-11-04')).toEqual({ start: '2026-10-14', end: '2026-11-14' })
  })

  test('the renewal day belongs to the new period', () => {
    expect(anchoredPeriod('2026-10-14', '2026-11-13')).toEqual({ start: '2026-10-14', end: '2026-11-14' })
    expect(anchoredPeriod('2026-10-14', '2026-11-14')).toEqual({ start: '2026-11-14', end: '2026-12-14' })
  })

  test('steps forward from an old anchor when renewals were never recorded', () => {
    expect(anchoredPeriod('2025-03-20', '2026-11-04')).toEqual({ start: '2026-10-20', end: '2026-11-20' })
  })

  test('steps backward from an anchor in the future', () => {
    // syncSubscription reports the period's end, which is a boundary too.
    expect(anchoredPeriod('2026-11-14', '2026-11-04')).toEqual({ start: '2026-10-14', end: '2026-11-14' })
  })

  test('an anchor on the 31st clamps in short months without drifting', () => {
    expect(anchoredPeriod('2026-01-31', '2026-02-27')).toEqual({ start: '2026-01-31', end: '2026-02-28' })
    expect(anchoredPeriod('2026-01-31', '2026-03-30')).toEqual({ start: '2026-02-28', end: '2026-03-31' })
    expect(anchoredPeriod('2026-01-31', '2026-04-15')).toEqual({ start: '2026-03-31', end: '2026-04-30' })
  })
})

describe('billingPeriod', () => {
  const anchor = new Date('2026-10-14T15:30:00Z')
  const now = new Date('2026-11-04T09:00:00Z')

  test('a subscribed plan follows its anchor', () => {
    expect(billingPeriod(developer, anchor, now)).toEqual({ start: '2026-10-14', end: '2026-11-14' })
  })

  test('free stays on calendar months even with an anchor left from a past subscription', () => {
    expect(billingPeriod(free, anchor, now)).toEqual({ start: '2026-11-01', end: '2026-12-01' })
  })

  test('plans without a Polar product stay on calendar months', () => {
    expect(billingPeriod(enterprise, anchor, now)).toEqual({ start: '2026-11-01', end: '2026-12-01' })
  })

  test('a subscribed plan with no anchor yet falls back to the calendar month', () => {
    expect(billingPeriod(developer, null, now)).toEqual({ start: '2026-11-01', end: '2026-12-01' })
  })
})

describe('previousPeriod and periodsBetween', () => {
  test('the previous period ends where the current one starts', () => {
    const current = billingPeriod(developer, '2026-10-14', '2026-11-04')
    expect(previousPeriod(developer, '2026-10-14', current)).toEqual({ start: '2026-09-14', end: '2026-10-14' })
    expect(previousPeriod(free, null, calendarPeriod('2026-11-04'))).toEqual({ start: '2026-10-01', end: '2026-11-01' })
  })

  test('lists every period up to the one given, contiguous', () => {
    expect(periodsBetween(free, null, '2026-08-20', '2026-11-01')).toEqual([
      { start: '2026-08-01', end: '2026-09-01' },
      { start: '2026-09-01', end: '2026-10-01' },
      { start: '2026-10-01', end: '2026-11-01' },
    ])
    expect(periodsBetween(free, null, '2026-11-02', '2026-11-01')).toEqual([])
  })
})
