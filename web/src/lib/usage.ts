/**
 * Shaping metered usage for display.
 *
 * `/usage` (a customer looking at their own cycle) and the accounts page (an
 * operator looking at someone else's) render the same numbers from the same
 * `UsageBucket[]`, so the folding lives here rather than twice in two views.
 */
import type { UsageBucket } from './types'

export interface DailyCredits {
  day: string
  credits: number
}

export interface EndpointTotals {
  endpoint: string
  requests: number
  credits: number
  rejected: number
}

/** Credits per day, oldest first — the bar chart's series. */
export function creditsPerDay(daily: UsageBucket[]): DailyCredits[] {
  const totals = new Map<string, number>()
  for (const bucket of daily) {
    totals.set(bucket.day, (totals.get(bucket.day) ?? 0) + bucket.credits)
  }
  return [...totals.entries()]
    .map(([day, credits]) => ({ day, credits }))
    .sort((a, b) => a.day.localeCompare(b.day))
}

/** Endpoint groups, heaviest spender first. */
export function totalsByEndpoint(daily: UsageBucket[]): EndpointTotals[] {
  const totals = new Map<string, { requests: number; credits: number; rejected: number }>()
  for (const bucket of daily) {
    const current = totals.get(bucket.endpoint) ?? { requests: 0, credits: 0, rejected: 0 }
    current.requests += bucket.requests
    current.credits += bucket.credits
    current.rejected += bucket.rejected
    totals.set(bucket.endpoint, current)
  }
  return [...totals.entries()]
    .map(([endpoint, value]) => ({ endpoint, ...value }))
    .sort((a, b) => b.credits - a.credits)
}

/** '-' is the sentinel the metering layer uses for usage with no key. */
export function keyLabel(id: string, names: Map<string, string>): string {
  if (id === '-') return 'No key'
  return names.get(id) ?? 'Deleted key'
}

export function shortDay(day: string): string {
  return day.slice(5)
}

/**
 * Billing cycles are defined in UTC, so cycle dates must be rendered in UTC
 * too. Formatting the UTC-midnight boundary in local time shows the previous
 * day to everyone west of Greenwich — "resets Aug 31" for a cycle that
 * actually rolls over on Sep 1.
 */
export function formatCycleDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, { dateStyle: 'medium', timeZone: 'UTC' })
}
