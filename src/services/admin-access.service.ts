/**
 * Extra conditions on reaching `/admin/*`, on top of holding an admin
 * credential: the client address allowlist and the passkey requirement.
 *
 * Both the admin guard (`middleware/auth.ts`) and `GET /auth/session` read
 * from here. The guard refuses; the session endpoint reports the same refusal
 * to the console, so an administrator who is turned away sees why and what to
 * do, instead of a console whose operator pages quietly fail.
 *
 * ## The passkey requirement and lockout
 *
 * `BARRELMAN_ADMIN_REQUIRE_PASSKEY=true` turns an admin-role session away from
 * `/admin/*` unless it was created by a passkey sign-in. It is only enforced
 * once at least one administrator has a passkey registered. Until then it logs
 * a warning at startup and does nothing, because enforcing it would refuse
 * every administrator and leave nobody able to reach the console to fix it.
 *
 * The check is a one-way latch. While the requirement is waiting, it looks
 * again every minute, so registering the first passkey starts enforcement
 * without a restart. Once enforced, it stays enforced until the process
 * restarts. Re-evaluating on every request would let anyone holding an
 * administrator's email-code session switch the requirement off by deleting
 * that administrator's passkeys from the account page.
 *
 * An administrator without a passkey is never stuck: the account page and
 * passkey registration only need a signed-in session, so they register one
 * there and sign in with it. If every administrator's passkey is lost, a
 * restart re-evaluates, finds none, and stops enforcing.
 */
import type { Session } from 'lucia'
import { accountsConnection as sql } from '../db'
import { adminIpAllowlist, adminRequirePasskey, type AdminIpAllowlist } from '../config/accounts.config'
import { clientIp } from '../lib/rate-limit'
import { ipAllowed } from '../lib/cidr'

export type AdminRefusalCode = 'ip_not_allowed' | 'passkey_required'

export interface AdminRefusal {
  status: 403
  code: AdminRefusalCode
  error: string
}

export const PASSKEY_REQUIRED_MESSAGE =
  'This instance requires administrators to sign in with a passkey. ' +
  'Register a passkey on your account page, then sign out and sign back in with it.'

export const IP_NOT_ALLOWED_MESSAGE = 'Admin access is not allowed from this network address.'

/** Refusal for a client address outside `BARRELMAN_ADMIN_ALLOWED_IPS`, or null. */
export function ipRefusal(ip: string, allowlist: AdminIpAllowlist): AdminRefusal | null {
  if (!allowlist.configured) return null
  if (ipAllowed(ip, allowlist.ranges)) return null
  return { status: 403, code: 'ip_not_allowed', error: IP_NOT_ALLOWED_MESSAGE }
}

export interface PasskeyPolicyDeps {
  required: boolean
  /** Passkeys registered to active administrators. */
  countAdminPasskeys: () => Promise<number>
  recheckMs?: number
  now?: () => number
  log?: Pick<Console, 'warn' | 'info' | 'error'>
}

export interface PasskeyPolicy {
  /** Whether admin sessions must be passkey sessions right now. */
  enforced(): Promise<boolean>
  /** Run once at startup: logs which state the requirement is in. */
  startupCheck(): Promise<void>
}

export function createPasskeyPolicy(deps: PasskeyPolicyDeps): PasskeyPolicy {
  const recheckMs = deps.recheckMs ?? 60_000
  const now = deps.now ?? Date.now
  const log = deps.log ?? console

  let latched = false
  let checkedAt = Number.NEGATIVE_INFINITY
  let inflight: Promise<boolean> | null = null

  async function check(): Promise<boolean> {
    try {
      const count = await deps.countAdminPasskeys()
      if (count > 0 && !latched) {
        latched = true
        log.info('[admin] BARRELMAN_ADMIN_REQUIRE_PASSKEY is now enforced: admin sessions must be passkey sign-ins')
      }
    } catch (err) {
      // Stays in whatever state it was in. Before the latch that is "not yet
      // enforced", the same answer the startup check would give with no data.
      log.error('[admin] could not count administrator passkeys:', err)
    }
    checkedAt = now()
    return latched
  }

  return {
    async enforced() {
      if (!deps.required) return false
      if (latched) return true
      if (now() - checkedAt < recheckMs) return false
      inflight ??= check().finally(() => {
        inflight = null
      })
      return inflight
    },

    async startupCheck() {
      if (!deps.required) return
      if (await check()) return
      log.warn(
        [
          '',
          '!!! BARRELMAN_ADMIN_REQUIRE_PASSKEY is set, but no administrator has a passkey registered.',
          '!!! It is NOT being enforced, because enforcing it now would lock every administrator out.',
          '!!! Register a passkey on the console account page; enforcement starts within a minute of the first one.',
          '',
        ].join('\n'),
      )
    },
  }
}

async function countAdminPasskeys(): Promise<number> {
  const [row] = await sql`
    SELECT count(*)::int AS n
    FROM accounts_passkeys p
    JOIN accounts_users u ON u.id = p.user_id
    WHERE u.role = 'admin' AND u.suspended_at IS NULL`
  return Number(row?.n ?? 0)
}

export const passkeyPolicy = createPasskeyPolicy({ required: adminRequirePasskey, countAdminPasskeys })

export interface AdminAccessDeps {
  allowlist: AdminIpAllowlist
  passkeyPolicy: PasskeyPolicy
  clientIp: (request: Request) => string
}

export const defaultAdminAccessDeps: AdminAccessDeps = {
  allowlist: adminIpAllowlist,
  passkeyPolicy,
  clientIp,
}

/** The address check alone, for routes that run it before any credential. */
export function requestIpRefusal(request: Request, deps: AdminAccessDeps = defaultAdminAccessDeps) {
  return ipRefusal(deps.clientIp(request), deps.allowlist)
}

/**
 * Why an admin-role session may not use `/admin/*` from this request, or null
 * when it may. The caller has already established the account is an admin.
 */
export async function adminSessionRefusal(
  request: Request,
  session: Pick<Session, 'authMethod'> | null | undefined,
  deps: AdminAccessDeps = defaultAdminAccessDeps,
): Promise<AdminRefusal | null> {
  const ip = requestIpRefusal(request, deps)
  if (ip) return ip
  if ((await deps.passkeyPolicy.enforced()) && session?.authMethod !== 'passkey') {
    return { status: 403, code: 'passkey_required', error: PASSKEY_REQUIRED_MESSAGE }
  }
  return null
}
