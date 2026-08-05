/**
 * Polar billing configuration.
 *
 * Billing is entirely optional. With no `POLAR_ACCESS_TOKEN` the whole
 * subscription surface is inert: every account sits on the free plan, the
 * console hides its billing pages, and the Polar SDK is never constructed. That
 * is the correct default for a self-hosted barrelman, where the operator owns
 * the hardware and metering exists only to show them their own usage.
 */

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

export const billing = {
  enabled: Boolean(process.env.POLAR_ACCESS_TOKEN),
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
