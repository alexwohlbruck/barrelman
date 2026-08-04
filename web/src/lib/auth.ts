/**
 * Console authentication state.
 *
 * The console authenticates with a session cookie, not a bearer token, so
 * there is nothing to keep in `localStorage` and nothing for a stray script on
 * the page to read. `bootstrap()` asks the server who we are on load; every
 * other export drives one of the sign-in methods.
 *
 * `adminKey` remains for the legacy shared-secret path, which stays available
 * so an operator who has not created an account can still reach the console.
 */
import { computed, ref } from 'vue'
import { startAuthentication, startRegistration, browserSupportsWebAuthn } from '@simplewebauthn/browser'
import type { AuthConfig, PublicUser } from './types'

const ADMIN_KEY_STORAGE = 'barrelman_admin_key'

export const user = ref<PublicUser | null>(null)
export const authConfig = ref<AuthConfig | null>(null)
/** False until `bootstrap()` has answered — the UI waits rather than flashing. */
export const ready = ref(false)

/** Legacy shared-secret access, for operators without an account. */
export const adminKey = ref<string>(localStorage.getItem(ADMIN_KEY_STORAGE) || '')
/** Whether the server requires any admin credential at all (false in open dev mode). */
export const authRequired = ref<boolean>(true)

export const isSignedIn = computed(() => user.value !== null)
export const isAdmin = computed(
  () => user.value?.role === 'admin' || Boolean(adminKey.value) || !authRequired.value,
)
export const isAuthenticated = computed(
  () => isSignedIn.value || Boolean(adminKey.value) || !authRequired.value,
)

export const passkeysSupported = browserSupportsWebAuthn()

// ── Legacy admin key ────────────────────────────────────────────────────

export function setAdminKey(key: string) {
  adminKey.value = key
  localStorage.setItem(ADMIN_KEY_STORAGE, key)
}

export function clearAdminKey() {
  adminKey.value = ''
  localStorage.removeItem(ADMIN_KEY_STORAGE)
}

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

/** Load the sign-in configuration and the current session. Call once on boot. */
export async function bootstrap(): Promise<void> {
  try {
    const [config, session, consoleConfig] = await Promise.all([
      request<AuthConfig>('/auth/config'),
      request<{ user: PublicUser } | null>('/auth/session'),
      request<{ authRequired: boolean }>('/admin/config').catch(() => ({ authRequired: true })),
    ])
    authConfig.value = config
    user.value = session?.user ?? null
    authRequired.value = consoleConfig.authRequired
  } catch {
    // A failed bootstrap must not wedge the console on a blank screen — fall
    // through to the sign-in page and let the user retry from there.
    user.value = null
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
    user.value = null
    clearAdminKey()
  }
}

export async function refreshUser(): Promise<void> {
  const session = await request<{ user: PublicUser } | null>('/auth/session')
  user.value = session?.user ?? null
}
