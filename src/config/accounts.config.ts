/**
 * Configuration for the public-API account system (sign-in, sessions, WebAuthn).
 *
 * Barrelman serves its console from the same origin as the API (`/console`), so
 * one origin covers both by default. `BARRELMAN_CONSOLE_ORIGIN` exists for the
 * dev setup, where Vite serves the console on :5199 and proxies API calls back
 * here — the browser origin is then the Vite one, and WebAuthn + CSRF checks
 * have to agree with it.
 */

import { envBoolean, envNumber, envRaw, envString } from './env'
import { parseCidrList, type Cidr } from '../lib/cidr'

export const appName = 'Barrelman'

function trimSlash(url: string): string {
  return url.replace(/\/+$/, '')
}

/** Public origin of this API — used in emails and OAuth redirect URIs. */
export const serverOrigin = trimSlash(
  process.env.BARRELMAN_SERVER_ORIGIN || `http://localhost:${Number(process.env.PORT) || 5001}`,
)

/** Origin the console is served from. Same as the API unless split in dev. */
export const consoleOrigin = trimSlash(process.env.BARRELMAN_CONSOLE_ORIGIN || serverOrigin)

/**
 * Every browser origin allowed to present a session cookie on a state-changing
 * request. Extra origins (a separate dashboard deployment, a staging host) can
 * be added with a comma-separated `BARRELMAN_ALLOWED_ORIGINS`.
 */
export const allowedOrigins = Array.from(
  new Set(
    [
      serverOrigin,
      consoleOrigin,
      // The Vite dev server, so `./start.sh dev` works without extra config.
      ...(process.env.NODE_ENV === 'production' ? [] : ['http://localhost:5199', 'http://127.0.0.1:5199']),
      ...(process.env.BARRELMAN_ALLOWED_ORIGINS || '')
        .split(',')
        .map((o) => trimSlash(o.trim()))
        .filter(Boolean),
    ].filter(Boolean),
  ),
)

/**
 * WebAuthn relying-party ID: the registrable domain of the origin the console
 * runs on, without port. A credential registered under one rpID cannot be used
 * under another, so this must stay stable across deployments of the same host.
 */
export const rpID = (() => {
  try {
    return new URL(consoleOrigin).hostname
  } catch {
    return 'localhost'
  }
})()

/** How long a browser session stays valid without re-authenticating. */
export const sessionTtlMs = envNumber('BARRELMAN_SESSION_TTL_DAYS', 30) * 24 * 60 * 60 * 1000

/**
 * Sessions past half their lifetime are slid forward on use, so an active user
 * is never signed out mid-session while an abandoned one still ages out.
 */
export const sessionRenewThresholdMs = sessionTtlMs / 2

/** One-time email codes are short-lived and single-use. */
export const otpTtlMs = envNumber('BARRELMAN_OTP_TTL_MINUTES', 15) * 60 * 1000

export const sessionCookieName = 'barrelman_session'

/**
 * `open` (default) lets anyone sign up — this is a public API. `invite` limits
 * sign-in to accounts an admin has already created, for private deployments.
 */
export const registrationMode = envString('BARRELMAN_REGISTRATION_MODE', 'open', ['open', 'invite'] as const)

/** Whether accounts/auth are available at all. Off = legacy shared-key mode. */
export const accountsEnabled = process.env.BARRELMAN_ACCOUNTS_ENABLED !== 'false'

export const isProduction = process.env.NODE_ENV === 'production'

/**
 * Whether cookies carry `Secure`. NODE_ENV alone missed a deployment served
 * over HTTPS without NODE_ENV=production, which then sent its session cookie
 * in a form a browser would also attach to a plain-HTTP request. An https
 * origin is a statement that the browser reaches us over TLS, so it is enough.
 */
export function cookiesSecure(env: { production: boolean; origins: readonly string[] }): boolean {
  return env.production || env.origins.some((origin) => origin.toLowerCase().startsWith('https://'))
}

export const secureCookies = cookiesSecure({ production: isProduction, origins: [serverOrigin, consoleOrigin] })

/**
 * Terms of service.
 *
 * The version is a plain string an operator bumps when the terms materially
 * change; accounts that accepted an older one are asked again. Leave
 * `BARRELMAN_TOS_URL` unset on a private or self-hosted instance and the whole
 * mechanism goes quiet — there is nothing to agree to.
 */
export const tosVersion = process.env.BARRELMAN_TOS_VERSION || '1'
export const privacyUrl = process.env.BARRELMAN_PRIVACY_URL || ''

/**
 * A mutable object rather than separate consts so tests can exercise both the
 * configured and unconfigured worlds without re-importing the module — the
 * same shape `billing` uses, for the same reason.
 */
export const terms = {
  version: tosVersion,
  url: process.env.BARRELMAN_TOS_URL || '',
  privacyUrl,
  /** Acceptance is required only once an operator has terms to point at. */
  get required(): boolean {
    return Boolean(this.url)
  },
}

export const tosUrl = terms.url

/**
 * Require administrators to have signed in with a passkey before an admin-role
 * session reaches `/admin/*`. Off by default so an upgrade never locks anyone
 * out. Admin-scoped API keys are unaffected. See `services/admin-access.service.ts`
 * for how it avoids locking out the last administrator.
 */
export const adminRequirePasskey = envBoolean('BARRELMAN_ADMIN_REQUIRE_PASSKEY', false)

export interface AdminIpAllowlist {
  /** False when the variable is unset: every address is allowed. */
  configured: boolean
  ranges: Cidr[]
}

/**
 * `BARRELMAN_ADMIN_ALLOWED_IPS`: comma-separated CIDRs (IPv4 and IPv6) allowed
 * to reach `/admin/*`. Unset allows any address.
 *
 * An entry that doesn't parse is dropped with an error in the log. If every
 * entry is bad the list is empty and refuses everyone: the operator asked for a
 * restriction, and a typo should not quietly lift it.
 */
export function readAdminIpAllowlist(
  raw: string | undefined = envRaw('BARRELMAN_ADMIN_ALLOWED_IPS'),
): AdminIpAllowlist {
  if (raw === undefined) return { configured: false, ranges: [] }
  const { ranges, invalid } = parseCidrList(raw)
  if (invalid.length > 0) {
    console.error(`[config] BARRELMAN_ADMIN_ALLOWED_IPS: ignoring unparseable entries: ${invalid.join(', ')}`)
  }
  if (ranges.length === 0) {
    console.error('[config] BARRELMAN_ADMIN_ALLOWED_IPS has no valid ranges, so /admin/* refuses every address')
  }
  return { configured: true, ranges }
}

export const adminIpAllowlist = readAdminIpAllowlist()
