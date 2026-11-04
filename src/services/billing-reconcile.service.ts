/**
 * Billing reconciliation sweep.
 *
 * Webhooks are the normal path for plan changes, and a missed one leaves an
 * account on the wrong plan until someone presses "refresh" in the console.
 * This walks every account linked to a Polar customer, re-reads its
 * subscription, and records the subscription's period so allowance resets
 * stay on the renewal day.
 *
 * It also compares what the overage reporter has sent for the open period
 * against the quantity on the customer's Polar meter, and logs any difference.
 * It changes nothing on that side: a mismatch can be a pass still in flight, a
 * meter that resets on a different day, or a real loss, and telling those
 * apart needs a person.
 *
 * A no-op unless billing is enabled.
 */
import { connection as sql } from '../db'
import { getPlan } from '../billing/plans'
import { billingPeriod } from '../billing/periods'
import { billing } from '../config/billing.config'
import { getMeteredCredits, syncSubscription } from './billing.service'
import { recordBillingPeriod } from './credits.service'

export interface ReconcileResult {
  accounts: number
  synced: number
  failed: number
  /** Accounts whose Polar meter disagrees with what was reported. */
  drift: number
}

/** Pause between accounts, to stay well inside Polar's API rate limit. */
const ACCOUNT_PAUSE_MS = 250

export async function reconcileBilling(log: (text: string) => void = console.log): Promise<ReconcileResult> {
  const result: ReconcileResult = { accounts: 0, synced: 0, failed: 0, drift: 0 }
  if (!billing.enabled) {
    log('Billing is not enabled on this instance; nothing to reconcile.')
    return result
  }

  const accounts = await sql<Array<{ id: string; email: string; plan: string }>>`
    SELECT id, email, plan FROM accounts_users
    WHERE polar_customer_id IS NOT NULL
    ORDER BY id`
  result.accounts = accounts.length
  log(`Reconciling ${accounts.length} account(s) linked to Polar`)

  for (const [i, account] of accounts.entries()) {
    if (i > 0) await Bun.sleep(ACCOUNT_PAUSE_MS)

    let planId = account.plan
    try {
      const summary = await syncSubscription(account.id, account.email)
      planId = summary.planId
      // The period's end is the next period's start, and any boundary anchors
      // the account's periods equally well.
      await recordBillingPeriod(account.id, summary.currentPeriodEnd)
      result.synced += 1
      if (planId !== account.plan) log(`  ${account.id}: plan ${account.plan} → ${planId}`)
    } catch (err) {
      result.failed += 1
      log(`  ${account.id}: subscription sync failed: ${err instanceof Error ? err.message : String(err)}`)
      continue
    }

    const plan = getPlan(planId)
    if (!billing.meterUsage || !plan.overageAllowed) continue

    const metered = await getMeteredCredits(account.id)
    if (metered === null) continue

    const [user] = await sql<Array<{ anchor: Date | null }>>`
      SELECT billing_period_start AS anchor FROM accounts_users WHERE id = ${account.id}`
    const period = billingPeriod(plan, user?.anchor ?? null)
    const [report] = await sql<Array<{ reported: number }>>`
      SELECT GREATEST(reported_credits, COALESCE(pending_credits, 0))::int AS reported
      FROM accounts_overage_reports
      WHERE user_id = ${account.id} AND cycle = ${period.start}`
    const reported = report?.reported ?? 0

    if (metered !== reported) {
      result.drift += 1
      log(`  ${account.id}: reported ${reported} overage credits since ${period.start}, Polar meter shows ${metered}`)
    }
  }

  log(`Done: ${result.synced} synced, ${result.failed} failed, ${result.drift} with meter drift`)
  return result
}
