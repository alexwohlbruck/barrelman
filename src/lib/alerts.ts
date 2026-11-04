/**
 * Operator alerts: one webhook, three payload shapes.
 *
 * Everything that wants to tell an operator something has gone wrong (a
 * scheduled job failing, a dependency going down, the data disk filling up)
 * sends through here, so the delivery format, the rate limit and the failure
 * handling live in one place. Nothing here is configured unless
 * ALERT_WEBHOOK_URL is set; with it unset every call is a no-op.
 *
 * Formats:
 *   - `ntfy`  — plain-text body with Title/Priority/Tags headers, which is what
 *               an ntfy topic URL expects (https://docs.ntfy.sh/publish/).
 *   - `slack` — `{ text }`, accepted by Slack incoming webhooks and by the
 *               Slack-compatible endpoints of Discord (`/slack`) and Mattermost.
 *   - `json`  — the alert object itself, for anything else.
 *
 * Sending never throws. An alert that cannot be delivered is logged and
 * dropped: the thing that triggered it is already failing, and an exception
 * from the reporting path must not add a second failure on top.
 */
import { envRaw, envString } from '../config/env'
import { scrubText } from './error-tracking'

export type AlertFormat = 'ntfy' | 'slack' | 'json'
export const ALERT_FORMATS: readonly AlertFormat[] = ['ntfy', 'slack', 'json']

/** `resolved` is a recovery: the condition behind an earlier alert has cleared. */
export type AlertSeverity = 'critical' | 'warning' | 'resolved'

export interface Alert {
  /**
   * Identifies the condition, e.g. `dependency:motis` or `disk:/data`. Repeats
   * of the same key and severity inside the rate-limit window are dropped.
   */
  key: string
  severity: AlertSeverity
  title: string
  message: string
}

export interface AlerterOptions {
  url: string
  format?: AlertFormat
  fetchFn?: typeof fetch
  now?: () => number
  /** Drop a repeat of the same key and severity sent within this many ms. */
  dedupeMs?: number
}

export interface SendOptions {
  /** Override the window for this alert, e.g. six hours for the disk check. */
  dedupeMs?: number
}

/**
 * `deduped` means the same alert was delivered inside the window, so the
 * operator already has it. `failed` means it was not delivered and nothing was
 * recorded: sending it again next cycle goes straight out.
 */
export type SendResult = 'sent' | 'deduped' | 'failed'

export interface Alerter {
  send(alert: Alert, options?: SendOptions): Promise<SendResult>
}

const DEFAULT_DEDUPE_MS = 10 * 60_000
const SEND_TIMEOUT_MS = 10_000

const NTFY_PRIORITY: Record<AlertSeverity, string> = { critical: 'urgent', warning: 'high', resolved: 'default' }
const NTFY_TAGS: Record<AlertSeverity, string> = {
  critical: 'rotating_light',
  warning: 'warning',
  resolved: 'white_check_mark',
}

/**
 * HTTP header values must be ASCII, and a script name or path can carry
 * anything. ntfy decodes RFC 2047 encoded words, so non-ASCII titles go out
 * that way instead of failing the request.
 */
function headerSafe(value: string): string {
  // eslint-disable-next-line no-control-regex
  if (/^[\x20-\x7e]*$/.test(value)) return value
  return `=?UTF-8?B?${Buffer.from(value, 'utf8').toString('base64')}?=`
}

/** The request a given format sends. Exported for tests. */
export function formatAlert(
  alert: Alert,
  format: AlertFormat,
  timestamp: number,
): { headers: Record<string, string>; body: string } {
  const { message } = alert

  if (format === 'ntfy') {
    return {
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        Title: headerSafe(alert.title),
        Priority: NTFY_PRIORITY[alert.severity],
        Tags: NTFY_TAGS[alert.severity],
      },
      body: message,
    }
  }

  if (format === 'slack') {
    return {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: `*${alert.title}*\n${message}` }),
    }
  }

  return {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      source: 'barrelman',
      key: alert.key,
      severity: alert.severity,
      title: alert.title,
      message,
      timestamp: new Date(timestamp).toISOString(),
    }),
  }
}

export function createAlerter(options: AlerterOptions): Alerter {
  const format = options.format ?? 'json'
  const fetchFn = options.fetchFn ?? globalThis.fetch
  const now = options.now ?? Date.now
  const defaultDedupe = options.dedupeMs ?? DEFAULT_DEDUPE_MS
  const lastSent = new Map<string, number>()

  return {
    async send(alert, sendOptions = {}) {
      const dedupeKey = `${alert.key}\u0000${alert.severity}`
      const at = now()
      const window = sendOptions.dedupeMs ?? defaultDedupe
      const previous = lastSent.get(dedupeKey)
      if (previous !== undefined && at - previous < window) return 'deduped'

      const { headers, body } = formatAlert(alert, format, at)
      try {
        const res = await fetchFn(options.url, {
          method: 'POST',
          headers,
          body,
          signal: AbortSignal.timeout(SEND_TIMEOUT_MS),
        })
        if (!res.ok) {
          console.warn(`[alerts] webhook answered HTTP ${res.status} for ${alert.key}`)
          return 'failed'
        }
      } catch (err) {
        console.warn(`[alerts] webhook failed for ${alert.key}:`, err instanceof Error ? err.message : err)
        return 'failed'
      }

      // Recorded only once delivered, so a webhook blip is retried by the next
      // check instead of silencing the alert for the whole window.
      lastSent.set(dedupeKey, at)
      // A recovery clears the way for the next failure to alert immediately,
      // and the other way round: "down, up, down" inside ten minutes is three
      // real events, and dropping the second "down" would leave the operator
      // believing it recovered.
      const opposite = alert.severity === 'resolved' ? ['critical', 'warning'] : ['resolved']
      for (const severity of opposite) lastSent.delete(`${alert.key}\u0000${severity}`)
      return 'sent'
    },
  }
}

/**
 * Process-wide alerter from the environment, or null when alerts are off.
 * Held on globalThis so `bun --hot` re-evaluating this module keeps the
 * rate-limit history instead of starting a fresh one per edit.
 */
const ALERTER_KEY = Symbol.for('barrelman.alerts.alerter')

export function alertsEnabled(): boolean {
  return envRaw('ALERT_WEBHOOK_URL') !== undefined
}

export function getAlerter(): Alerter | null {
  const url = envRaw('ALERT_WEBHOOK_URL')
  if (!url) return null
  const globals = globalThis as Record<symbol, unknown>
  const cached = globals[ALERTER_KEY] as { url: string; alerter: Alerter } | undefined
  if (cached && cached.url === url) return cached.alerter

  const alerter = createAlerter({
    url,
    format: envString<AlertFormat>('ALERT_WEBHOOK_FORMAT', 'json', ALERT_FORMATS),
  })
  globals[ALERTER_KEY] = { url, alerter }
  return alerter
}

/** Send through the configured webhook. Reports `failed` when ALERT_WEBHOOK_URL is unset. */
export async function sendAlert(alert: Alert, options?: SendOptions): Promise<SendResult> {
  const alerter = getAlerter()
  if (!alerter) return 'failed'
  return alerter.send(alert, options)
}

/** True when the operator has this alert, from this send or an earlier one. */
export const delivered = (result: SendResult) => result !== 'failed'

/**
 * Make upstream error text fit for a webhook. An ntfy topic is readable by
 * anyone who knows its name, so URLs, addresses and container host:port pairs
 * go, as do the emails and credentials error tracking strips.
 */
export function scrubForAlert(text: string): string {
  return scrubText(text)
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/\S+/gi, '[url]')
    .replace(/\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b/g, '[host]')
    .replace(/\b[a-z0-9][a-z0-9_.-]*:\d{2,5}\b/gi, '[host]')
}
