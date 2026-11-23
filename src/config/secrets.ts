import { dbUrl as defaultDbUrl } from '../db'

/** Placeholders shipped in `.env.example` and `docker-compose.yml`. Public, so never a real secret. */
export const DEFAULT_API_KEY = 'brm_dev_changeme'
export const DEFAULT_DB_PASSWORD = 'barrelman'

function dbPassword(url: string): string | undefined {
  try {
    return decodeURIComponent(new URL(url).password)
  } catch {
    return undefined
  }
}

/**
 * Refuse to start a production instance on a published default secret, and
 * warn when the service key is unset (every data endpoint is then open).
 * Outside production an unset key is the local-development path, so it only logs.
 */
export function assertSecretsConfigured(
  env: Record<string, string | undefined> = process.env,
  dbUrl: string = defaultDbUrl,
): void {
  const apiKey = env.BARRELMAN_API_KEY
  if (env.NODE_ENV !== 'production') {
    if (!apiKey) console.log('[auth] No BARRELMAN_API_KEY set — data endpoints are open (development mode)')
    return
  }

  const defaults: string[] = []
  if (apiKey === DEFAULT_API_KEY) defaults.push('BARRELMAN_API_KEY')
  if (dbPassword(dbUrl) === DEFAULT_DB_PASSWORD) defaults.push('BARRELMAN_DB_PASSWORD')
  if (defaults.length > 0) {
    throw new Error(
      `[auth] ${defaults.join(' and ')} still set to the published default. ` +
        'Set a long random value before running with NODE_ENV=production.',
    )
  }

  if (!apiKey) {
    console.warn(
      '[auth] BARRELMAN_API_KEY is not set — every data endpoint is OPEN and unmetered. ' +
        'Set it before exposing this instance.',
    )
  }
}
