/**
 * Formatting for the request metrics. Shared by the Traffic page and the
 * account pane, which render the same `TrafficReport` at two scopes.
 */
import type { CallerTraffic, TrafficStats } from './types'

export const RANGES = [
  { value: '24h', label: '24 hours' },
  { value: '7d', label: '7 days' },
  { value: '30d', label: '30 days' },
] as const

/** Callers with no account, recorded under sentinel ids by the server. */
const SUBJECT_LABELS: Record<string, string> = {
  '@service': 'Service key',
  '@anonymous': 'Anonymous',
  '@unidentified': 'Unidentified',
}

export function isAccount(subject: string): boolean {
  return !subject.startsWith('@')
}

export function callerLabel(row: CallerTraffic): string {
  return SUBJECT_LABELS[row.subject] ?? row.email ?? row.subject
}

export function keyLabel(row: CallerTraffic): string {
  if (!row.apiKeyId) return '—'
  return row.keyName ?? row.apiKeyId
}

export function formatMs(ms: number | null, top = 10_000): string {
  if (ms === null) return '—'
  const prefix = ms >= top ? '≥ ' : ''
  return ms >= 1_000 ? `${prefix}${(ms / 1_000).toFixed(ms >= 10_000 ? 0 : 1)} s` : `${prefix}${ms} ms`
}

export function percent(part: number, whole: number): string {
  if (!whole || !part) return '0%'
  const value = (part / whole) * 100
  return value < 0.1 ? '<0.1%' : `${value < 10 ? value.toFixed(1) : Math.round(value)}%`
}

/** Refusals that are not the throttle: quota and credentials. */
export function otherRefused(stats: TrafficStats): number {
  return stats.quotaRefused + stats.denied
}

const LAYER_LABELS: Record<keyof TrafficStats['limitedBy'], string> = {
  key: 'per key',
  account: 'per account',
  ip: 'per address',
  concurrency: 'concurrency',
  penalty: 'penalty box',
}

/** The layers that refused, largest first, e.g. "per key 120 · concurrency 4". */
export function limitedBreakdown(stats: TrafficStats): { layer: string; count: number }[] {
  return (Object.entries(stats.limitedBy) as [keyof TrafficStats['limitedBy'], number][])
    .filter(([, count]) => count > 0)
    .sort((a, b) => b[1] - a[1])
    .map(([layer, count]) => ({ layer: LAYER_LABELS[layer], count }))
}
