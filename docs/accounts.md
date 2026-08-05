# Accounts & API keys

Barrelman serves two kinds of caller: developers with their own accounts, and
Parchment's server. They authenticate differently on purpose.

| Credential | Looks like | Metered | For |
|---|---|---|---|
| Account API key | `brm_live_…` | Yes | Customers |
| Service key | `BARRELMAN_API_KEY` | **No** | Parchment → barrelman, internal jobs |
| Session cookie | — | n/a | The console. Never accepted on data endpoints |

The service key is not a customer key and is never billed. Existing deployments
that only ever set `BARRELMAN_API_KEY` keep working unchanged.

## Signing in

Three ways, all landing on the same account:

- **Email code.** Eight digits, single-use, fifteen minutes. Always available.
- **Passkeys** (WebAuthn). Registered against an already-signed-in account, so
  the address behind a credential is always verified first. Sign-in is
  usernameless — `residentKey: 'required'`, so the browser offers the credential
  without anyone typing an address.
- **OAuth** — Google, GitHub, GitLab. Each turns on when its client ID and
  secret are set; the console asks `/auth/config` which to render.

Passkeys and OAuth are **additive**. Email codes always work, so removing your
last passkey or unlinking every provider cannot lock an account out.

### Why OAuth matches on subject, not email

Identities are linked by the provider's stable subject ID, never the email
address — providers let people change addresses, and matching on a mutable field
would hand an account to whoever inherits an old one.

An identity is only linked to an *existing* barrelman account when the provider
asserts the same address **and says it has verified it**. GitHub will happily
report an unverified address, so barrelman asks for the verified primary
specifically.

### Administrators

The **first account created on a fresh instance becomes an administrator**, so a
new deployment is never locked out of its own console. After that, promote by
listing addresses in `BARRELMAN_ADMIN_EMAILS`.

Admin routes accept either an admin-role session or the shared
`BARRELMAN_ADMIN_KEY` — humans get real accounts, scripts and CI keep the key.

## API keys

Created in the console at `/console/keys`.

- **Shown once.** Only a SHA-256 digest is stored. A lost key is rolled, not
  recovered.
- **Revocation is immediate** — it evicts the verification cache directly rather
  than waiting out its TTL, because a key that keeps working for another minute
  is not revoked in any sense a customer would accept.
- **Revoked rows are kept**, so usage history stays attributable.

### Scopes

A key names the endpoint groups it may call. A key embedded in a web map can be
limited to `tiles` and `search`, and is then worthless for running up a routing
bill if it leaks.

```
tiles  places  spatial  geocode  search  routing  transit  isochrone
```

`['*']` means every group, and is the default. A key holding `*` alongside
narrower scopes is stored as just `*` — keeping both would suggest the narrow
ones constrain something.

### Presenting a key

```bash
curl -H "Authorization: Bearer brm_live_..." \
  "https://api.barrelman.dev/contains?lat=35.77&lng=-78.63"
```

Tile URLs also accept `?api_key=` (or the older `?token=`), because a map
library fetches tiles itself and cannot set a header. A key in a URL ends up in
logs and browser history, so this is documented for tiles specifically, where
there is no alternative.

## Sessions

The console authenticates with a session cookie, not a bearer token in
`localStorage` — there is nothing on the page for a stray script to read.

Sessions are managed by [Lucia](https://v3.lucia-auth.com) over the
`accounts_sessions` table. Two things follow from that:

- **Lucia stores session IDs in the clear** — the cookie value *is* the row's
  primary key. Treat that table as credential material.
- Lucia validates expiry on read but never deletes, so a periodic sweep removes
  expired rows (see `services/account-maintenance.service.ts`).

Cookie-authenticated state-changing requests are origin-checked. Bearer-token
requests are not, because a bearer token is not sent automatically and so
carries no CSRF risk.

Users can see their active sessions in the console and revoke any of them, or
sign out everywhere at once.

## Endpoints

| Route | Purpose |
|---|---|
| `GET /auth/config` | Which sign-in methods this instance offers |
| `POST /auth/request-code` | Email a sign-in code |
| `POST /auth/verify-code` | Exchange a code for a session |
| `POST /auth/passkeys/*` | WebAuthn registration and sign-in |
| `GET /auth/oauth/:provider` | Start an OAuth flow |
| `GET /auth/session` | Current account, or 204 |
| `GET /account/keys` | List keys |
| `POST /account/keys` | Create one — returns the only copy |
| `DELETE /account/keys/:id` | Revoke |
| `GET /account/usage` | Usage for a date range |
| `GET /account/credits` | Balance for the cycle |

`/account/*` is session-authenticated only. A leaked API key must not be able to
mint more keys or read the billing history of the account it belongs to.

## Sign-up abuse

Deliberately light — see [abuse controls](abuse-controls.md) for the rest.

- **Email normalisation.** Uniqueness is enforced on a canonical form, folding
  gmail dots and `+tags`. `a.b+x@gmail.com` and `ab@gmail.com` are one account,
  so one inbox cannot farm free monthly grants.
- **Disposable domains** are refused from a short built-in list.
- **Per-IP sign-up budget** — five new accounts per address per day by default.
  Only spent when an account is genuinely created, so an office behind one NAT
  can sign in all day.

None of this stops someone determined; they can use a second real inbox. It
stops the cheap version.
