/**
 * Configuration for the public-API account system (sign-in, sessions, WebAuthn).
 *
 * Barrelman serves its console from the same origin as the API (`/console`), so
 * one origin covers both by default. `BARRELMAN_CONSOLE_ORIGIN` exists for the
 * dev setup, where Vite serves the console on :5199 and proxies API calls back
 * here — the browser origin is then the Vite one, and WebAuthn + CSRF checks
 * have to agree with it.
 */

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
export const sessionTtlMs = Number(process.env.BARRELMAN_SESSION_TTL_DAYS ?? 30) * 24 * 60 * 60 * 1000

/**
 * Sessions past half their lifetime are slid forward on use, so an active user
 * is never signed out mid-session while an abandoned one still ages out.
 */
export const sessionRenewThresholdMs = sessionTtlMs / 2

/** One-time email codes are short-lived and single-use. */
export const otpTtlMs = Number(process.env.BARRELMAN_OTP_TTL_MINUTES ?? 15) * 60 * 1000

export const sessionCookieName = 'barrelman_session'

/**
 * Emails that are granted the `admin` role on sign-up. The very first account
 * created on a fresh instance also becomes an admin, so a new deployment is
 * never locked out of its own console.
 */
export const adminEmails = new Set(
  (process.env.BARRELMAN_ADMIN_EMAILS || '')
    .split(',')
    .map((e) => e.trim().toLowerCase())
    .filter(Boolean),
)

/**
 * `open` (default) lets anyone sign up — this is a public API. `invite` limits
 * sign-in to accounts an admin has already created, for private deployments.
 */
export const registrationMode = (process.env.BARRELMAN_REGISTRATION_MODE ?? 'open') as 'open' | 'invite'

/** Whether accounts/auth are available at all. Off = legacy shared-key mode. */
export const accountsEnabled = process.env.BARRELMAN_ACCOUNTS_ENABLED !== 'false'

export const isProduction = process.env.NODE_ENV === 'production'

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
