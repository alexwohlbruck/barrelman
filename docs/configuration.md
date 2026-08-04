# Configuration

Every environment variable, grouped by what it affects. `.env.example` carries
the same list with inline commentary; this is the reference.

Nothing here is required to run barrelman locally. The defaults give you a
working engine with an open API and no accounts — which is the right shape for
development and for a private self-hosted instance.

## Core

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://barrelman:barrelman@localhost:5434/barrelman` | PostGIS connection string |
| `BARRELMAN_DB_PASSWORD` | `barrelman` | Used by compose for the DB container |
| `PORT` | `5001` | HTTP port |
| `REGIONS` | `north-carolina,nyc-metro` | Which geographies the importers pull. `global` for everything |
| `OLLAMA_HOST` | `http://localhost:11434` | Embeddings for semantic search (optional) |
| `BARRELMAN_STATEMENT_TIMEOUT_MS` | `10000` | Query timeout on the API pool. `0` disables. DDL is exempt |

## Authentication

| Variable | Default | Description |
|---|---|---|
| `BARRELMAN_API_KEY` | — | Shared **service** credential (Parchment → barrelman). Unmetered, never billed. **Unset means the data API is open** |
| `BARRELMAN_ADMIN_KEY` | falls back to `BARRELMAN_API_KEY` | Shared key for `/admin/*`. An admin-role session works too |
| `BARRELMAN_ACCOUNTS_ENABLED` | `true` | `false` disables accounts entirely |

> Leaving `BARRELMAN_API_KEY` unset is how every guard in this codebase spells
> "local development", and a fresh clone has to be usable with no configuration.
> A production instance sets it; the server logs a warning at startup if
> `NODE_ENV=production` and it is missing.

## Accounts

| Variable | Default | Description |
|---|---|---|
| `BARRELMAN_SERVER_ORIGIN` | `http://localhost:$PORT` | Public origin. Used in emails and as the OAuth redirect base |
| `BARRELMAN_CONSOLE_ORIGIN` | server origin | Only if the console is hosted separately. **Also sets the WebAuthn relying-party ID** |
| `BARRELMAN_ALLOWED_ORIGINS` | — | Extra browser origins allowed to present a session cookie |
| `BARRELMAN_ADMIN_EMAILS` | — | Addresses granted admin on sign-up. The first account is always an admin |
| `BARRELMAN_REGISTRATION_MODE` | `open` | `invite` restricts sign-in to accounts an admin created |
| `BARRELMAN_SESSION_TTL_DAYS` | `30` | Session lifetime |
| `BARRELMAN_OTP_TTL_MINUTES` | `15` | Sign-in code lifetime |
| `BARRELMAN_SIGNUPS_PER_IP_PER_DAY` | `5` | New accounts per address per day. `0` disables |
| `BARRELMAN_TEST_ACCOUNT_EMAIL` | — | Fixed code `00000000` for this address, for E2E tests. **Unset in production** |

Changing `BARRELMAN_CONSOLE_ORIGIN` after passkeys exist invalidates them — a
credential registered under one relying-party ID cannot be used under another.

## Email

| Variable | Default | Description |
|---|---|---|
| `SMTP_HOST` | — | Without it, sign-in codes are printed to the log instead of sent |
| `SMTP_PORT` | `465` | |
| `SMTP_SECURE` | `true` | |
| `SMTP_USER` / `SMTP_PASS` | — | Omit both for an unauthenticated relay |
| `SMTP_FROM` | `Barrelman <noreply@barrelman.dev>` | |

## OAuth

Each provider turns on when both of its variables are set. Register the redirect
URI as `<BARRELMAN_SERVER_ORIGIN>/auth/oauth/<provider>/callback`.

| Variable | Description |
|---|---|
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | |
| `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | |
| `GITLAB_CLIENT_ID` / `GITLAB_CLIENT_SECRET` | |
| `GITLAB_BASE_URL` | `https://gitlab.com`. Set for a self-hosted instance |

## Billing

See [polar-setup.md](polar-setup.md) for the walkthrough.

| Variable | Description |
|---|---|
| `POLAR_ACCESS_TOKEN` | **Enables billing.** Without it every account stays on free |
| `POLAR_WEBHOOK_SECRET` | Required when billing is on — the server refuses to start otherwise |
| `POLAR_ORGANIZATION_ID` | |
| `POLAR_SANDBOX` | `true` for sandbox.polar.sh |
| `POLAR_DEVELOPER_PRODUCT_ID` | |
| `POLAR_BUSINESS_PRODUCT_ID` | |
| `POLAR_SCALE_PRODUCT_ID` | |
| `POLAR_CREDIT_PACKS` | `productId:credits` pairs, comma-separated |
| `POLAR_USAGE_EVENT_NAME` | `barrelman_credits`. Must match the Polar meter |
| `POLAR_METER_USAGE` | `false` runs paid plans flat-rate with soft limits |

Refusing to start without a webhook secret is deliberate: an unsigned webhook
endpoint would let anyone who guesses the URL grant themselves a plan.

## Metering

| Variable | Default | Description |
|---|---|---|
| `BARRELMAN_USAGE_FLUSH_MS` | `5000` | Buffer flush interval. A crash loses at most this much, in the customer's favour |
| `BARRELMAN_BALANCE_CACHE_MS` | `15000` | Balance cache TTL. Bounds how far an account can overshoot |
| `BARRELMAN_OVERAGE_REPORT_MS` | `900000` | How often overage is batched to the provider |
| `BARRELMAN_ACCOUNT_SWEEP_MS` | `3600000` | Expired sessions, lifted suspensions, abuse detection |

## Abuse controls

See [abuse-controls.md](abuse-controls.md) for what these mean.

| Variable | Default | Description |
|---|---|---|
| `BARRELMAN_ANON_RPM` | `120` | Per address when nobody is authenticated |
| `BARRELMAN_IP_RPM` | `3000` | Ceiling on any address. Generous — NAT puts real users behind one |
| `BARRELMAN_PER_KEY_SHARE` | `0.8` | Share of the account budget one key may spend |
| `BARRELMAN_ISOCHRONE_CONCURRENCY_PER_ACCOUNT` | `2` | |
| `BARRELMAN_TRANSIT_CONCURRENCY_PER_ACCOUNT` | `4` | |
| `BARRELMAN_ROUTING_CONCURRENCY_PER_ACCOUNT` | `8` | |
| `BARRELMAN_ABUSE_STRIKES` | `25` | Rejections before the penalty box |
| `BARRELMAN_ABUSE_SIGNAL_STRIKES` | `100` | Strikes before a signal is raised |
| `BARRELMAN_BURN_RATE_MULTIPLE` | `3` | Daily spend vs monthly allowance before flagging |
| `BARRELMAN_BURN_RATE_SUSPEND_MULTIPLE` | `25` | Multiple triggering an automatic hold |
| `BARRELMAN_AUTO_SUSPEND_HOURS` | `6` | How long that hold lasts |
| `BARRELMAN_MULTI_ACCOUNT_THRESHOLD` | `6` | Sign-ups from one address before flagging |

These defaults are **not calibrated against real traffic**.

## Terms of service

| Variable | Default | Description |
|---|---|---|
| `BARRELMAN_TOS_URL` | — | **Setting this turns on acceptance**, gating API-key creation |
| `BARRELMAN_TOS_VERSION` | `1` | Bump to re-prompt everyone. Existing keys keep working |
| `BARRELMAN_PRIVACY_URL` | — | Linked from the sign-in screen |

> **All of these must be listed in the `barrelman` service's `environment:`
> block in `docker-compose.yml`.** Compose does not forward the host
> environment; a variable that is set in `.env` but not named there is silently
> absent inside the container, and the symptom is a setting that appears
> configured and has no effect. When you add a variable, add it in three places:
> here, in `.env.example`, and in the compose file.

## Engines

| Variable | Default | Description |
|---|---|---|
| `GRAPHHOPPER_URL` | `http://localhost:8989` | |
| `GRAPHHOPPER_JAVA_OPTS` | `-Xmx5g -Xms1g` | Heap must fit the whole routing graph |
| `MOTIS_URL` | `http://localhost:8080` | |
| `MOTIS_RT_UPDATE_INTERVAL` | MOTIS default (60s) | Raise in dev to cut realtime polling |
| `MARTIN_URL` | `http://barrelman-martin:3000` | |
| `BARRELMAN_TILE_KEY` | — | Dedicated unmetered tile credential. **Setting it means tiles are not public** |
| `ISOCHRONE_CONCURRENCY` | `8` | Parallel GraphHopper calls per isochrone |
| `TRANSITLAND_API_KEY` | — | For GTFS feed discovery |

## Database tuning

Compose defaults are sized for a dev laptop sharing RAM with MOTIS,
Elasticsearch and GraphHopper. **Production should give the database the box.**

| Variable | Dev default | 32GB host |
|---|---|---|
| `BARRELMAN_DB_SHARED_BUFFERS` | `2GB` | `8GB` |
| `BARRELMAN_DB_CACHE_SIZE` | `4GB` | `24GB` |
| `BARRELMAN_DB_WORK_MEM` | `64MB` | `128MB` |
| `BARRELMAN_DB_MAINTENANCE_WORK_MEM` | `1GB` | `2GB` |
| `BARRELMAN_DB_MEM_LIMIT` | `4g` | `28g` |
| `BARRELMAN_DB_RANDOM_PAGE_COST` | `1.1` | `1.1` (SSD); raise toward `4` on spinning disks |

Search latency is dominated by whether the working set stays resident, so these
matter more than they look.
