/**
 * Email address handling for sign-up: canonicalisation and throwaway-domain
 * detection.
 *
 * The point is to make repeat free-tier sign-ups mildly inconvenient, not to
 * build an identity-verification system. Someone determined will get a second
 * account — they can also just use a second real inbox. What this stops is the
 * cheap version: `me+1@gmail.com`, `me+2@gmail.com`, and ten-minute mailboxes.
 */

/**
 * Providers that treat a `+suffix` as the same mailbox. Folding tags for
 * everyone would be wrong — plenty of self-hosted and corporate mail servers
 * deliver `a+b@host` to a genuinely different account than `a@host`.
 */
const PLUS_ALIASING_DOMAINS = new Set([
  'gmail.com',
  'googlemail.com',
  'outlook.com',
  'hotmail.com',
  'live.com',
  'msn.com',
  'fastmail.com',
  'proton.me',
  'protonmail.com',
  'pm.me',
  'icloud.com',
  'me.com',
  'mac.com',
  'yahoo.com',
  'zoho.com',
  'hey.com',
])

/** Domains where dots in the local part are insignificant. */
const DOT_INSENSITIVE_DOMAINS = new Set(['gmail.com', 'googlemail.com'])

/** Domains that are the same mailbox under a different name. */
const DOMAIN_ALIASES: Record<string, string> = {
  googlemail: 'gmail.com',
  'googlemail.com': 'gmail.com',
  'protonmail.com': 'proton.me',
  'pm.me': 'proton.me',
}

/**
 * A deliberately short list of the largest disposable-mail providers. An
 * exhaustive list is a losing race — new domains appear daily — so this is a
 * speed bump, and the real defence is the per-IP sign-up limit plus the fact
 * that free credits are capped anyway.
 */
const DISPOSABLE_DOMAINS = new Set([
  '10minutemail.com',
  'guerrillamail.com',
  'guerrillamail.info',
  'sharklasers.com',
  'mailinator.com',
  'tempmail.com',
  'temp-mail.org',
  'throwawaymail.com',
  'yopmail.com',
  'trashmail.com',
  'getnada.com',
  'dispostable.com',
  'maildrop.cc',
  'fakeinbox.com',
  'mohmal.com',
  'emailondeck.com',
  'moakt.com',
  'tempr.email',
  'spamgourmet.com',
  'mailnesia.com',
  'discard.email',
  'inboxkitten.com',
  'burnermail.io',
  'mintemail.com',
  'tempmailo.com',
  'minuteinbox.com',
  'luxusmail.org',
  'byom.de',
])

const EMAIL_RE = /^[^\s@]+@[^\s@.]+(\.[^\s@.]+)+$/

export function isValidEmail(email: string): boolean {
  return EMAIL_RE.test(email.trim()) && email.trim().length <= 254
}

/** Domain part, lowercased and de-aliased. Empty string if malformed. */
export function emailDomain(email: string): string {
  const at = email.lastIndexOf('@')
  if (at < 0) return ''
  const domain = email.slice(at + 1).trim().toLowerCase()
  return DOMAIN_ALIASES[domain] ?? domain
}

/**
 * Reduce an address to the mailbox it actually reaches, for uniqueness checks.
 * `Me.Long+barrelman@googlemail.com` and `melong@gmail.com` collapse to the
 * same string; `alex+work@self-hosted.dev` is left alone.
 *
 * Always store the user's original input for display and delivery — this form
 * is only ever a lookup key.
 */
export function normalizeEmail(email: string): string {
  const trimmed = email.trim().toLowerCase()
  const at = trimmed.lastIndexOf('@')
  if (at < 0) return trimmed

  let local = trimmed.slice(0, at)
  const domain = emailDomain(trimmed)

  if (PLUS_ALIASING_DOMAINS.has(domain)) {
    const plus = local.indexOf('+')
    if (plus > 0) local = local.slice(0, plus)
  }
  if (DOT_INSENSITIVE_DOMAINS.has(domain)) {
    local = local.replaceAll('.', '')
  }

  return `${local}@${domain}`
}

export function isDisposableEmail(email: string): boolean {
  return DISPOSABLE_DOMAINS.has(emailDomain(email))
}
