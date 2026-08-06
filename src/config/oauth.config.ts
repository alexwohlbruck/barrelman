/**
 * Third-party sign-in providers.
 *
 * Each is enabled purely by the presence of its client id and secret, so an
 * operator turns one on by setting two environment variables and restarting —
 * there is no separate feature flag to keep in sync. The console asks
 * `/auth/config` which ones are live and renders only those buttons.
 */
import type { OAuthProvider } from '../schema/accounts'
import { serverOrigin } from './accounts.config'

export interface OAuthProviderConfig {
  id: OAuthProvider
  label: string
  clientId: string
  clientSecret: string
  /** Where the provider sends the user back. Must match the provider's console. */
  redirectUri: string
  scopes: string[]
  /** Self-hosted GitLab instances point elsewhere than gitlab.com. */
  baseUrl?: string
}

function provider(
  id: OAuthProvider,
  label: string,
  clientId: string | undefined,
  clientSecret: string | undefined,
  scopes: string[],
  baseUrl?: string,
): OAuthProviderConfig | null {
  if (!clientId || !clientSecret) return null
  return {
    id,
    label,
    clientId,
    clientSecret,
    redirectUri: `${serverOrigin}/auth/oauth/${id}/callback`,
    scopes,
    baseUrl,
  }
}

export const oauthProviders: Record<OAuthProvider, OAuthProviderConfig | null> = {
  google: provider('google', 'Google', process.env.GOOGLE_CLIENT_ID, process.env.GOOGLE_CLIENT_SECRET, [
    'openid',
    'profile',
    'email',
  ]),
  github: provider('github', 'GitHub', process.env.GITHUB_CLIENT_ID, process.env.GITHUB_CLIENT_SECRET, [
    'read:user',
    'user:email',
  ]),
  gitlab: provider(
    'gitlab',
    'GitLab',
    process.env.GITLAB_CLIENT_ID,
    process.env.GITLAB_CLIENT_SECRET,
    ['read_user', 'openid', 'profile', 'email'],
    (process.env.GITLAB_BASE_URL || 'https://gitlab.com').replace(/\/+$/, ''),
  ),
}

export function getOAuthProvider(id: string): OAuthProviderConfig | null {
  return oauthProviders[id as OAuthProvider] ?? null
}

/** The providers an operator has actually configured, for the sign-in screen. */
export function enabledOAuthProviders(): Array<{ id: OAuthProvider; label: string }> {
  return Object.values(oauthProviders)
    .filter((p): p is OAuthProviderConfig => p !== null)
    .map(({ id, label }) => ({ id, label }))
}

/** How long a pending authorization may sit before its state cookie expires. */
export const oauthStateTtlMs = 10 * 60 * 1000
