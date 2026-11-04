/**
 * Console authentication state.
 *
 * The console authenticates with a session cookie and nothing else, so there is
 * no credential in `localStorage` and nothing for a stray script on the page to
 * read. `bootstrap()` asks the server who we are on load; every other export
 * drives one of the sign-in methods.
 *
 * Authorisation is the account's role: `admin` unlocks the operator views, and
 * the server enforces the same thing on `/admin/*`. Automation authenticates
 * separately, with an account API key carrying the `admin` scope — that is a
 * header on a script's request, never something the browser holds.
 */
import { computed, ref } from 'vue'
import { startAuthentication, startRegistration, browserSupportsWebAuthn } from '@simplewebauthn/browser'
import type { AdminAccess, AuthConfig, PublicUser } from './types'

/**
 * The console used to stash an admin bearer here. A leftover value actively
 * broke sign-in: it was sent on every admin request and, being neither a
 * session nor a valid key, came back 403 while a perfectly good session cookie
 * rode along unread. Purge it on load so an old browser heals itself.
 */
localStorage.removeItem('barrelman_admin_key')

export const user = ref<PublicUser | null>(null)
export const authConfig = ref<AuthConfig | null>(null)
/** False until `bootstrap()` has answered — the UI waits rather than flashing. */
export const ready = ref(false)

/**
 * Set for an administrator whose session the admin API refuses: a sign-in that
 * wasn't a passkey under BARRELMAN_ADMIN_REQUIRE_PASSKEY, or an address outside
 * BARRELMAN_ADMIN_ALLOWED_IPS. The operator views are hidden while it is set,
 * and the shell shows the reason with what to do about it.
 */
export const adminAccess = ref<AdminAccess | null>(null)

export const isSignedIn = computed(() => user.value !== null)
export const hasAdminRole = computed(() => user.value?.role === 'admin')
export const adminBlocked = computed(() => hasAdminRole.value && adminAccess.value?.ok === false)
/** An administrator the admin API will actually serve from this session. */
export const isAdmin = computed(() => hasAdminRole.value && !adminBlocked.value)

export const passkeysSupported = browserSupportsWebAuthn()

// ── Requests ────────────────────────────────────────────────────────────

export class AuthError extends Error {
  constructor(
    message: string,
    public status: number,
    public reason?: string,
  ) {
    super(message)
    this.name = 'AuthError'
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(path, {
    ...init,
    // Cookies must ride along, and the API's CSRF check reads the Origin.
    credentials: 'same-origin',
    headers: {
      ...(init.body ? { 'content-type': 'application/json' } : {}),
      ...(init.headers || {}),
    },
  })

  if (res.status === 204) return null as T

  const payload = res.headers.get('content-type')?.includes('application/json')
    ? await res.json()
    : await res.text()

  if (!res.ok) {
    const message =
      typeof payload === 'object' && payload && 'error' in payload ? payload.error : res.statusText
    throw new AuthError(String(message), res.status, (payload as { reason?: string })?.reason)
  }

  return payload as T
}

// ── Session lifecycle ───────────────────────────────────────────────────

interface SessionResponse {
  user: PublicUser
  adminAccess?: AdminAccess
}

function applySession(session: SessionResponse | null) {
  user.value = session?.user ?? null
  adminAccess.value = session?.adminAccess ?? null
}

/** Load the sign-in configuration and the current session. Call once on boot. */
export async function bootstrap(): Promise<void> {
  try {
    const [config, session] = await Promise.all([
      request<AuthConfig>('/auth/config'),
      request<SessionResponse | null>('/auth/session'),
    ])
    authConfig.value = config
    applySession(session)
  } catch {
    // A failed bootstrap must not wedge the console on a blank screen — fall
    // through to the sign-in page and let the user retry from there.
    applySession(null)
  } finally {
    ready.value = true
  }
}

export async function requestCode(email: string): Promise<void> {
  await request('/auth/request-code', { method: 'POST', body: JSON.stringify({ email }) })
}

export async function verifyCode(email: string, code: string): Promise<PublicUser> {
  const result = await request<{ user: PublicUser }>('/auth/verify-code', {
    method: 'POST',
    body: JSON.stringify({ email, code }),
  })
  user.value = result.user
  // The sign-in response doesn't say whether the admin API accepts this
  // session; the session endpoint does.
  await refreshUser().catch(() => {})
  return result.user
}

export async function signInWithPasskey(): Promise<PublicUser> {
  const options = await request<Record<string, unknown>>('/auth/passkeys/authenticate/options', {
    method: 'POST',
  })
  const assertion = await startAuthentication(options as never)
  const result = await request<{ user: PublicUser }>('/auth/passkeys/authenticate/verify', {
    method: 'POST',
    body: JSON.stringify(assertion),
  })
  user.value = result.user
  await refreshUser().catch(() => {})
  return result.user
}

export async function registerPasskey(name?: string): Promise<void> {
  const options = await request<Record<string, unknown>>('/auth/passkeys/register/options', {
    method: 'POST',
  })
  const attestation = await startRegistration(options as never)
  await request('/auth/passkeys/register/verify', {
    method: 'POST',
    body: JSON.stringify({ ...attestation, name }),
  })
}

/** Full-page navigation — the provider redirects the browser back to us. */
export function signInWithOAuth(provider: string, options: { link?: boolean; next?: string } = {}) {
  const params = new URLSearchParams()
  if (options.link) params.set('link', '1')
  if (options.next) params.set('next', options.next)
  const query = params.toString()
  window.location.href = `/auth/oauth/${provider}${query ? `?${query}` : ''}`
}

export async function signOut(): Promise<void> {
  try {
    await request('/auth/session', { method: 'DELETE' })
  } finally {
    applySession(null)
  }
}

// ── Account deletion ────────────────────────────────────────────────────

/** Email a code that confirms deleting this account. */
export async function requestDeletionCode(): Promise<void> {
  await request('/account/delete/request', { method: 'POST' })
}

/**
 * Delete the signed-in account, confirmed by the emailed code or, when `code`
 * is omitted, by a passkey prompt. The server ends every session; the local
 * user is cleared so the router sends the console back to sign-in.
 */
export async function deleteOwnAccount(email: string, code?: string): Promise<void> {
  let passkey: unknown
  if (!code) {
    const options = await request<Record<string, unknown>>('/account/delete/passkey-options', { method: 'POST' })
    passkey = await startAuthentication(options as never)
  }
  await request('/account', { method: 'DELETE', body: JSON.stringify({ email, code, passkey }) })
  user.value = null
}

export async function refreshUser(): Promise<void> {
  const session = await request<SessionResponse | null>('/auth/session')
  applySession(session)
}
