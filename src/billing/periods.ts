/**
 * Billing periods: the window a monthly allowance covers.
 *
 * A free account's period is the calendar month in UTC. A subscribed account's
 * period follows its subscription instead, because that is what Polar bills: a
 * customer who subscribed on the 14th pays on the 14th, so their allowance
 * resets on the 14th too. Resetting on the 1st would hand a mid-month
 * subscriber a second allowance a fortnight after they paid for the first.
 *
 * The subscription's period is stored as one anchor, `billing_period_start`,
 * and every other period is derived from it by stepping whole months. Any
 * boundary works as the anchor, so a renewal webhook that never arrives costs
 * nothing: the next period is still where Polar will put it. A day of the month
 * the target month lacks (the 31st in February) clamps to that month's last
 * day, and the following month returns to the anchor's day.
 *
 * Periods are whole UTC days, because usage is counted per UTC day. A
 * subscription renewing at 15:00 on the 14th starts its new allowance at
 * 00:00 on the 14th. Every day belongs to exactly one period, so nothing is
 * counted twice or dropped at a boundary.
 */
import type { Plan } from './plans'

export interface BillingPeriod {
  /** First day of the period, `YYYY-MM-DD`, inclusive. */
  start: string
  /** First day of the next period, `YYYY-MM-DD`, exclusive. */
  end: string
}

const DAY_MS = 86_400_000

export function toDay(date: Date | string): string {
  return typeof date === 'string' ? date.slice(0, 10) : date.toISOString().slice(0, 10)
}

function parts(day: string): [number, number, number] {
  const [y, m, d] = day.split('-').map(Number) as [number, number, number]
  return [y, m - 1, d]
}

function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month + 1, 0)).getUTCDate()
}

/** `day` moved by `months`, keeping its day of month where the target month has it. */
export function addMonths(day: string, months: number): string {
  const [y, m, d] = parts(day)
  const total = y * 12 + m + months
  const year = Math.floor(total / 12)
  const month = total - year * 12
  const date = Math.min(d, daysInMonth(year, month))
  return new Date(Date.UTC(year, month, date)).toISOString().slice(0, 10)
}

export function addDays(day: string, days: number): string {
  const [y, m, d] = parts(day)
  return new Date(Date.UTC(y, m, d) + days * DAY_MS).toISOString().slice(0, 10)
}

/** Midnight UTC at the start of `day`, as an instant. */
export function dayStart(day: string): Date {
  return new Date(`${day}T00:00:00.000Z`)
}

export function calendarPeriod(date: Date | string): BillingPeriod {
  const start = `${toDay(date).slice(0, 7)}-01`
  return { start, end: addMonths(start, 1) }
}

/** The period, stepped in whole months from `anchor`, that contains `date`. */
export function anchoredPeriod(anchor: Date | string, date: Date | string): BillingPeriod {
  const anchorDay = toDay(anchor)
  const day = toDay(date)
  const [ay, am] = parts(anchorDay)
  const [y, m] = parts(day)

  // The month difference lands on the right period or the one after it; a
  // clamped or later day of month means the boundary may not have come yet.
  let k = (y - ay) * 12 + (m - am)
  if (addMonths(anchorDay, k) > day) k -= 1
  return { start: addMonths(anchorDay, k), end: addMonths(anchorDay, k + 1) }
}

/**
 * Whether a plan's periods follow its subscription. Only plans sold through a
 * Polar product have one; free, enterprise and the operator-assigned plans
 * stay on calendar months.
 */
export function followsSubscription(plan: Plan): boolean {
  return Boolean(plan.polarProductEnv)
}

/**
 * The period containing `date` for an account on `plan`.
 *
 * A subscribed plan with no anchor recorded yet (a subscription that predates
 * the column, or one whose webhook has not arrived) falls back to the calendar
 * month, which is what every account used before.
 */
export function billingPeriod(
  plan: Plan,
  anchor: Date | string | null | undefined,
  date: Date | string = new Date(),
): BillingPeriod {
  if (anchor && followsSubscription(plan)) return anchoredPeriod(anchor, date)
  return calendarPeriod(date)
}

/** The period before `period`, under the same plan and anchor. */
export function previousPeriod(
  plan: Plan,
  anchor: Date | string | null | undefined,
  period: BillingPeriod,
): BillingPeriod {
  return billingPeriod(plan, anchor, addDays(period.start, -1))
}

/**
 * The part of `period` that is metered as one stretch: from the later of its
 * start and `meteredFrom`, to the earlier of its end and `until`. Null when
 * nothing is left.
 *
 * `meteredFrom` is the day an account last changed plan or subscription. The
 * days before it were judged, settled and reported under the old plan and
 * anchor when the change happened, so the new ones never see them. A segment
 * is keyed by its start, which is why a moved anchor cannot find, and so
 * cannot double-bill or wrongly subtract, a report from before the change.
 */
export function segmentOf(
  period: BillingPeriod,
  meteredFrom: string | null | undefined,
  until?: string,
): BillingPeriod | null {
  const start = meteredFrom && meteredFrom > period.start ? meteredFrom : period.start
  const end = until && until < period.end ? until : period.end
  return start < end ? { start, end } : null
}

function lengthInDays(range: BillingPeriod): number {
  return Math.round((dayStart(range.end).getTime() - dayStart(range.start).getTime()) / DAY_MS)
}

export interface SegmentEdges {
  /** The segment was opened by a new subscription, which paid from the period's start. */
  fromPeriodStart?: boolean
  /** The segment was ended by its subscription ending, which had paid to the period's end. */
  toPeriodEnd?: boolean
}

/**
 * The allowance included in one segment of `period`.
 *
 * A plan change inside a subscription splits the period, and each part gets
 * its plan's allowance in proportion to the days it covers, the way Polar
 * prorates the price. A whole allowance per part would let a customer flip
 * Developer → Business → Developer in a minute, pay almost nothing once the
 * proration nets out, and collect a whole Business allowance each time.
 * Prorated, the parts of a period add up to at most one allowance of the
 * biggest plan held in it.
 *
 * A subscription that starts or ends is not a plan change: it paid for its
 * whole period, so `edges` extends the share to the period's start or end.
 * Plans without a subscription (Free, Enterprise, operator plans) stay on
 * calendar months and are never prorated.
 */
export function segmentAllowance(
  plan: Plan,
  period: BillingPeriod,
  segment: BillingPeriod,
  edges: SegmentEdges = {},
): number {
  if (!followsSubscription(plan)) return plan.monthlyCredits
  const share = {
    start: edges.fromPeriodStart ? period.start : segment.start,
    end: edges.toPeriodEnd ? period.end : segment.end,
  }
  return Math.round((plan.monthlyCredits * lengthInDays(share)) / lengthInDays(period))
}

/** Every period from the one containing `from` up to, not including, the one starting at `until`. */
export function periodsBetween(
  plan: Plan,
  anchor: Date | string | null | undefined,
  from: string,
  until: string,
): BillingPeriod[] {
  const periods: BillingPeriod[] = []
  let period = billingPeriod(plan, anchor, from)
  while (period.start < until) {
    periods.push(period)
    period = billingPeriod(plan, anchor, period.end)
  }
  return periods
}
