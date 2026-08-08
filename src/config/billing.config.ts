/**
 * Polar billing configuration.
 *
 * Billing is off unless BOTH are true: Polar is configured, and a valid license
 * grants the `billing` feature. With either missing the whole subscription
 * surface is inert — every account sits on the free plan, the console hides its
 * billing pages, and the Polar SDK is never constructed.
 *
 * That is the correct state for a self-hosted barrelman. The Commons Clause in
 * LICENSE removes the right to sell the software, so charging third parties for
 * access is not something a self-hoster may do; metering exists there only to
 * show an operator their own usage. See src/lib/license.ts and LICENSING.md.
 */
import { verifyLicense, hasFeature } from '../lib/license'

/** Credit packs, as `productId:credits` pairs. Bought once, never expiring. */
function parseCreditPacks(raw: string | undefined): Record<string, number> {
  if (!raw) return {}

  const packs: Record<string, number> = {}
  for (const entry of raw.split(',')) {
    const [productId, credits] = entry.split(':').map((part) => part.trim())
    if (!productId || !credits) continue
    const amount = Number(credits)
    if (!Number.isFinite(amount) || amount <= 0) {
      console.warn(`[billing] ignoring credit pack "${entry}" — credits must be a positive number`)
      continue
    }
    packs[productId] = Math.floor(amount)
  }
  return packs
}

const licenseToken = process.env.BARRELMAN_LICENSE ?? ''
export const license = licenseToken ? await verifyLicense(licenseToken) : null

if (licenseToken && !license) {
  console.warn('[license] BARRELMAN_LICENSE is set but invalid or expired — billing stays disabled')
}

const polarConfigured = Boolean(process.env.POLAR_ACCESS_TOKEN)
const billingLicensed = hasFeature(license, 'billing')

if (polarConfigured && !billingLicensed) {
  console.warn(
    '[license] POLAR_ACCESS_TOKEN is set but no license grants the "billing" feature — ' +
      'billing is disabled. Selling access to barrelman requires a commercial license; see LICENSING.md.',
  )
}

export const billing = {
  enabled: polarConfigured && billingLicensed,
  sandbox: process.env.POLAR_SANDBOX === 'true',
  accessToken: process.env.POLAR_ACCESS_TOKEN ?? '',
  webhookSecret: process.env.POLAR_WEBHOOK_SECRET ?? '',
  organizationId: process.env.POLAR_ORGANIZATION_ID ?? '',
  creditPacks: parseCreditPacks(process.env.POLAR_CREDIT_PACKS),
  /**
   * Meter slug that overage events are ingested under. Must match the meter
   * configured in Polar, or the events are accepted and then billed by nothing.
   */
  usageEventName: process.env.POLAR_USAGE_EVENT_NAME || 'barrelman_credits',
  /** Whether to report metered overage at all. */
  meterUsage: process.env.POLAR_METER_USAGE !== 'false',
}

// A misconfigured webhook secret is worse than none: unsigned callbacks would
// be trusted, and anyone who guesses the URL could grant themselves credits.
if (billing.enabled && !billing.webhookSecret) {
  throw new Error(
    'POLAR_WEBHOOK_SECRET must be set when billing is enabled. ' +
      'Copy the signing secret from your Polar dashboard, or unset POLAR_ACCESS_TOKEN to disable billing.',
  )
}

if (billing.enabled) {
  console.log(
    `[billing] Polar billing enabled (${billing.sandbox ? 'sandbox' : 'production'})` +
      `${Object.keys(billing.creditPacks).length ? `, ${Object.keys(billing.creditPacks).length} credit pack(s)` : ''}`,
  )
}
