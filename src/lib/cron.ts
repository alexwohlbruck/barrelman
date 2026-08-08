/**
 * Minimal 5-field cron parser + "next occurrence" solver, timezone-aware.
 *
 * Used by the scheduler to decide when a scheduled job is next due. Written
 * here rather than pulled in as a dependency (or taken from `Bun.cron`, which
 * only exists from Bun 1.3.14 and would break `bun test` on older local
 * toolchains) because we need one specific thing the built-ins don't give us:
 * the next occurrence *after an arbitrary instant, in an arbitrary IANA
 * timezone*. Schedules are stored in the DB with their own timezone, so an
 * operator writing "0 3 * * *" gets 3am where they live, not 3am UTC.
 *
 * Supported: `*`, `a`, `a-b`, `a-b/n`, `*\/n`, comma lists, three-letter month
 * and weekday names, and the usual `@daily`-style nicknames. Sunday is both 0
 * and 7.
 */

export interface CronFields {
  minute: Set<number>
  hour: Set<number>
  dayOfMonth: Set<number>
  month: Set<number>
  dayOfWeek: Set<number>
  /** POSIX: when BOTH day fields are restricted, a day matches if EITHER does. */
  domRestricted: boolean
  dowRestricted: boolean
}

const NICKNAMES: Record<string, string> = {
  '@yearly': '0 0 1 1 *',
  '@annually': '0 0 1 1 *',
  '@monthly': '0 0 1 * *',
  '@weekly': '0 0 * * 0',
  '@daily': '0 0 * * *',
  '@midnight': '0 0 * * *',
  '@hourly': '0 * * * *',
}

const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
const DAYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat']

export class CronParseError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'CronParseError'
  }
}

/** Expand one field (`0-30/15`, `1-5`, `MON,WED`) into the set of values it matches. */
function parseField(raw: string, min: number, max: number, names: string[], label: string): Set<number> {
  const out = new Set<number>()
  for (const part of raw.split(',')) {
    const piece = part.trim()
    if (!piece) throw new CronParseError(`Empty ${label} value in "${raw}"`)

    const [rangePart, stepPart] = piece.split('/')
    let step = 1
    if (stepPart !== undefined) {
      step = Number(stepPart)
      if (!Number.isInteger(step) || step < 1) throw new CronParseError(`Invalid ${label} step "${stepPart}"`)
    }

    let lo: number
    let hi: number
    if (rangePart === '*') {
      lo = min
      hi = max
    } else {
      const bounds = rangePart.split('-')
      if (bounds.length > 2) throw new CronParseError(`Invalid ${label} range "${rangePart}"`)
      lo = toNumber(bounds[0], names, label)
      hi = bounds.length === 2 ? toNumber(bounds[1], names, label) : lo
      // A bare value with a step means "from here to the end" — `5/10` is the
      // same as `5-59/10` for minutes. Standard cron behaviour.
      if (bounds.length === 1 && stepPart !== undefined) hi = max
    }

    if (lo < min || hi > max || lo > hi) {
      throw new CronParseError(`${label} value out of range in "${piece}" (expected ${min}-${max})`)
    }
    for (let v = lo; v <= hi; v += step) out.add(v)
  }
  if (out.size === 0) throw new CronParseError(`No ${label} values matched "${raw}"`)
  return out
}

function toNumber(token: string | undefined, names: string[], label: string): number {
  const t = (token ?? '').trim().toLowerCase()
  if (!t) throw new CronParseError(`Missing ${label} value`)
  const named = names.indexOf(t.slice(0, 3))
  if (names.length > 0 && named >= 0) return label === 'month' ? named + 1 : named
  const n = Number(t)
  if (!Number.isInteger(n)) throw new CronParseError(`Invalid ${label} value "${token}"`)
  return n
}

/** Parse a 5-field expression (or nickname). Throws CronParseError on bad input. */
export function parseCron(expression: string): CronFields {
  const trimmed = expression.trim().toLowerCase()
  if (!trimmed) throw new CronParseError('Empty cron expression')

  const expanded = NICKNAMES[trimmed] ?? trimmed
  const fields = expanded.split(/\s+/)
  if (fields.length !== 5) {
    throw new CronParseError(`Expected 5 fields (minute hour day month weekday), got ${fields.length}`)
  }

  const [min, hr, dom, mon, dow] = fields
  const dayOfWeek = parseField(dow, 0, 7, DAYS, 'weekday')
  // 7 and 0 are both Sunday; normalise so lookups only ever ask about 0-6.
  if (dayOfWeek.delete(7)) dayOfWeek.add(0)

  return {
    minute: parseField(min, 0, 59, [], 'minute'),
    hour: parseField(hr, 0, 23, [], 'hour'),
    dayOfMonth: parseField(dom, 1, 31, [], 'day-of-month'),
    month: parseField(mon, 1, 12, MONTHS, 'month'),
    dayOfWeek,
    domRestricted: dom !== '*',
    dowRestricted: dow !== '*',
  }
}

/** True if the expression parses. Used to validate operator input before saving. */
export function isValidCron(expression: string): boolean {
  try {
    parseCron(expression)
    return true
  } catch {
    return false
  }
}

interface WallClock {
  hour: number
  minute: number
  day: number
  month: number
  dayOfWeek: number
}

const formatters = new Map<string, Intl.DateTimeFormat>()
function formatterFor(timeZone: string): Intl.DateTimeFormat {
  let f = formatters.get(timeZone)
  if (!f) {
    f = new Intl.DateTimeFormat('en-US', {
      timeZone,
      hourCycle: 'h23',
      weekday: 'short',
      month: 'numeric',
      day: 'numeric',
      hour: 'numeric',
      minute: 'numeric',
    })
    formatters.set(timeZone, f)
  }
  return f
}

/** Read an instant's wall-clock fields as seen in `timeZone`. */
function wallClock(at: Date, timeZone: string): WallClock {
  const parts = formatterFor(timeZone).formatToParts(at)
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? '0'
  return {
    hour: Number(get('hour')) % 24,
    minute: Number(get('minute')),
    day: Number(get('day')),
    month: Number(get('month')),
    dayOfWeek: Math.max(0, DAYS.indexOf(get('weekday').slice(0, 3).toLowerCase())),
  }
}

/** True if `expression` is a valid IANA zone this runtime knows. */
export function isValidTimeZone(timeZone: string): boolean {
  try {
    new Intl.DateTimeFormat('en-US', { timeZone })
    return true
  } catch {
    return false
  }
}

function matchesDay(f: CronFields, wc: WallClock): boolean {
  if (!f.month.has(wc.month)) return false
  const dom = f.dayOfMonth.has(wc.day)
  const dow = f.dayOfWeek.has(wc.dayOfWeek)
  if (f.domRestricted && f.dowRestricted) return dom || dow
  if (f.domRestricted) return dom
  if (f.dowRestricted) return dow
  return true
}

const MINUTE_MS = 60_000
/** Day-stepping means the worst realistic case is Feb-29-only: ~1500 iterations. */
const MAX_STEPS = 100_000

/**
 * The first instant strictly after `after` that matches `expression`, evaluated
 * against wall-clock time in `timeZone`. Returns null if nothing matches within
 * a few years (e.g. `0 0 30 2 *`).
 *
 * Walks forward a minute at a time but skips whole days and hours that can't
 * match, so a yearly schedule resolves in ~400 steps rather than ~500k. Working
 * in real instants and re-reading the wall clock each step means DST is handled
 * by construction: a spring-forward hour simply never appears.
 */
export function nextRun(expression: string, after: Date, timeZone = 'UTC'): Date | null {
  const f = parseCron(expression)
  const tz = isValidTimeZone(timeZone) ? timeZone : 'UTC'

  // Start at the next whole minute — a schedule never fires twice in one minute.
  let t = Math.floor(after.getTime() / MINUTE_MS) * MINUTE_MS + MINUTE_MS

  for (let steps = 0; steps < MAX_STEPS; steps++) {
    const wc = wallClock(new Date(t), tz)
    if (!matchesDay(f, wc)) {
      // Jump to 00:00 of the next local day.
      t += (24 * 60 - (wc.hour * 60 + wc.minute)) * MINUTE_MS
      continue
    }
    if (!f.hour.has(wc.hour)) {
      t += (60 - wc.minute) * MINUTE_MS
      continue
    }
    if (!f.minute.has(wc.minute)) {
      t += MINUTE_MS
      continue
    }
    return new Date(t)
  }
  return null
}

/** Human summary of a cron expression, for the console. Falls back to the raw text. */
export function describeCron(expression: string, timeZone = 'UTC'): string {
  let f: CronFields
  try {
    f = parseCron(expression)
  } catch {
    return expression
  }

  const zone = timeZone && timeZone !== 'UTC' ? ` ${timeZone}` : ' UTC'
  const at = (h: number, m: number) => `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`
  const single = (s: Set<number>) => (s.size === 1 ? [...s][0] : null)

  const h = single(f.hour)
  const m = single(f.minute)

  if (h !== null && m !== null) {
    const dowNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    if (!f.domRestricted && !f.dowRestricted) return `Daily at ${at(h, m)}${zone}`
    if (f.dowRestricted && !f.domRestricted) {
      const days = [...f.dayOfWeek].sort().map((d) => dowNames[d])
      return `${days.join(', ')} at ${at(h, m)}${zone}`
    }
    const dom = single(f.dayOfMonth)
    if (dom !== null && !f.dowRestricted) return `Monthly on day ${dom} at ${at(h, m)}${zone}`
  }

  if (f.minute.size === 60 && f.hour.size === 24) return 'Every minute'
  if (m !== null && f.hour.size === 24 && !f.domRestricted && !f.dowRestricted) {
    return m === 0 ? 'Hourly, on the hour' : `Hourly, at :${String(m).padStart(2, '0')}`
  }
  return expression
}
