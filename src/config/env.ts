/**
 * Environment readers that treat a blank variable as unset.
 *
 * `docker-compose.yml` forwards every optional setting as `${VAR:-}`, which
 * defines the variable as an empty string when the operator has not configured
 * it — inside the container it is *present*, just blank. `process.env.VAR ??
 * fallback` therefore never reaches its fallback, and `Number('')` is 0. That
 * turned the defaults of every numeric knob into zero: sign-in codes expired in
 * the same millisecond they were issued, Lucia minted sessions that were
 * already dead, the account sweep ran in a tight loop, and the per-IP rate
 * limiter allowed one request per minute.
 *
 * A `.env` line written as `BARRELMAN_OTP_TTL_MINUTES=` does the same thing, so
 * this is not only a Compose problem.
 */

/** A configured value, or `undefined` when the variable is absent or blank. */
export function envRaw(name: string): string | undefined {
  const raw = process.env[name]
  if (raw === undefined) return undefined
  const trimmed = raw.trim()
  return trimmed === '' ? undefined : trimmed
}

/**
 * String setting. Pass `allowed` for anything with a fixed set of values: a
 * value outside it falls back with a warning rather than being handed on as a
 * `T` it is not.
 *
 * Without that check the type is a lie with consequences. `BARRELMAN_
 * REGISTRATION_MODE=Invite` typechecks as `'open' | 'invite'`, matches neither
 * in `registrationMode === 'invite'`, and so opens sign-ups on the deployment
 * that was trying to close them — a fail-open produced by a capital letter.
 */
export function envString<T extends string>(name: string, fallback: T, allowed?: readonly T[]): T {
  const raw = envRaw(name) as T | undefined
  if (raw === undefined) return fallback

  if (allowed && !allowed.includes(raw)) {
    console.warn(`[config] ${name}="${raw}" is not one of ${allowed.join(', ')} — using ${fallback}`)
    return fallback
  }
  return raw
}

/**
 * Numeric setting. An explicit `0` is honoured — several knobs use it to mean
 * "disabled" — but a blank or unparseable value falls back, loudly, because a
 * silent 0 is indistinguishable from a deliberate one.
 */
export function envNumber(name: string, fallback: number): number {
  const raw = envRaw(name)
  if (raw === undefined) return fallback

  const value = Number(raw)
  if (!Number.isFinite(value)) {
    console.warn(`[config] ${name}="${raw}" is not a number — using ${fallback}`)
    return fallback
  }
  return value
}
