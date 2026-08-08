# Configuration

Every environment variable, grouped by what it affects. `.env.example` carries
the same list with inline commentary; this is the reference.

Nothing here is required to run barrelman locally. The defaults give you a
working engine with an open API and no accounts — which is the right shape for
development and for a private self-hosted instance.

A variable set to nothing (`BARRELMAN_OTP_TTL_MINUTES=` in `.env`, or unset
under compose, which forwards it as an empty string) counts as *not configured*
and takes the default below — the same as omitting the line. Only an actual
value overrides one, so a knob you mean to turn off needs an explicit `0`.

## Core

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://barrelman:barrelman@localhost:5434/barrelman` | PostGIS connection string |
| `BARRELMAN_DB_PASSWORD` | `barrelman` | Used by compose for the DB container |
| `PORT` | `5001` | HTTP port |
| `REGIONS` | `north-carolina,nyc-metro` | Which geographies the importers pull. `global` for everything |
| `OLLAMA_HOST` | `http://localhost:11434` | Embeddings for semantic search (optional) |
| `BARRELMAN_STATEMENT_TIMEOUT_MS` | `10000` | Query timeout on the API pool. `0` disables. DDL is exempt |
| `PUBLIC_BASE_URL` | — | Origin `/docs` advertises as its "Production" server. Unset offers only localhost |
| `NODE_ENV` | — | `production` makes the server warn at startup about missing secrets |
| `COMPOSE_FILE` | — | Read by Compose, not by barrelman. `.env.example` sets it so a bare `docker compose up` picks up the dev overrides |

`REGIONS` is consumed by the **importers**, which run in `barrelman-ops` — not
by the API. It is named in that service's `environment:` block, not the API's.

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

## Licensing

| Variable | Description |
|---|---|
| `BARRELMAN_LICENSE` | Signed token unlocking gated features. Only `billing` today |
| `BARRELMAN_LICENSE_PUBLIC_KEY` | Overrides the key licenses verify against. For tests and commercial licensees |

The Commons Clause in [LICENSE](../LICENSE) removes the right to sell
Barrelman, so a self-hosted instance may not charge third parties for access —
see [LICENSING.md](../LICENSING.md). Subscription billing is gated on a license
granting the `billing` feature, which only the official deployment holds.
Everything else — search, tiles, routing, transit, accounts, keys, metering —
needs no license and never will.

## Billing

**Requires a license granting `billing` (above).** Setting these without one
logs a warning and leaves billing disabled.

| Variable | Description |
|---|---|
| `POLAR_ACCESS_TOKEN` | Polar API token. Billing needs this *and* a license |
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
| `BARRELMAN_TRUSTED_PROXY_HOPS` | `1` | Proxies in front of barrelman. **Read this one** — see below |
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

### `BARRELMAN_TRUSTED_PROXY_HOPS`

Every per-address limit above depends on the address being one the caller
cannot choose, and `X-Forwarded-For` is a list whose first entry the caller
writes. The address is therefore counted in from the **right**, so it lands on
the entry written by the outermost proxy you run.

| Set it to | When |
|---|---|
| `0` | barrelman is exposed directly. `X-Forwarded-For` is ignored; the peer address is used |
| `1` *(default)* | One reverse proxy in front — Traefik, nginx, Caddy |
| `2` | A CDN in front of that proxy |

Both mistakes are silent. Too high and a caller spoofs past every per-address
limit by prepending entries; too low and every request looks like it came from
your own proxy, so all of them share one bucket and legitimate traffic throttles
itself.

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
| `GRAPHHOPPER_URL` | `http://localhost:8989` | Compose sets `http://barrelman-graphhopper:8989` |
| `GRAPHHOPPER_JAVA_OPTS` | `-Xmx6g -Xms1g` | Heap must fit the whole routing graph — see below |
| `MOTIS_URL` | `http://localhost:8080` | Compose sets `http://barrelman-motis:8080` |
| `MOTIS_RT_UPDATE_INTERVAL` | MOTIS default (60s) | GTFS-RT poll interval. Baked into the MOTIS config at import time, so it takes effect on the next `rebuild-motis.sh`, not on restart |
| `MARTIN_URL` | `http://barrelman-martin:3000` | Set to point at a Martin outside this stack |
| `PELIAS_URL` | `http://pelias_api:4000` | Address geocoder. Only used when the `pelias` profile is up |
| `ISOCHRONE_CONCURRENCY` | `8` | Parallel GraphHopper calls per isochrone. Raise only if GraphHopper is dedicated to isochrone work |
| `TRANSITLAND_API_KEY` | — | For GTFS feed discovery. Free at [transit.land](https://transit.land/users/sign_up) |

`GRAPHHOPPER_JAVA_OPTS` is the one to watch as `REGIONS` grows. GraphHopper
loads its graph into JVM **heap** rather than memory-mapping it, so an
undersized `-Xmx` dies before the process logs its version — the failure looks
like a crash, not an out-of-memory. Check `du -sh` on the `graph-cache`
directory and keep `-Xmx` comfortably above it.

## Import pipeline

Read by the scripts running in `barrelman-ops` (and, for the osm2pgsql step, in
`barrelman-db`). None of them affect a running API.

| Variable | Default | Description |
|---|---|---|
| `GEOFABRIK_URL` | NC extract | Single-PBF fallback, used only when `REGIONS` resolves to nothing |
| `GEOFABRIK_REPLICATION_URL` | NC updates feed | Replication feed for `UPDATE_MODE=replication`. **Change this with `REGIONS`** — the default is North Carolina's, so an unchanged value applies the wrong region's diffs |
| `IMPORT_PBF` | — | Path to a local PBF inside the container, overriding the download |
| `FORCE_DOWNLOAD` | `0` | `1` re-downloads even when the PBF is already present |
| `UPDATE_MODE` | `replication` | `full` re-downloads and re-imports instead, for extracts with no replication feed |
| `BARRELMAN_DATA_DIR` | `/data` | Where PBFs and derived extracts live. Set by the DB image |
| `OSM_DATA_DIR` | `./data` | Where `prepare-motis-osm.sh` looks for `region.osm.pbf` |
| `GTFS_DATA_DIR` | `./data/gtfs` | GTFS ZIP output. Compose sets `/gtfs-zips` |
| `GTFS_REGION` | — | Overrides a single GTFS search area, bypassing `REGIONS` |
| `GITHUB_TOKEN` | — | Raises the GitHub API limit when resolving a region's OpenAddresses file list (60 req/hour unauthenticated). A rate-limited lookup yields an empty address list and a warning, not a failure |
| `DB_CONTAINER` | `barrelman-db` | Container the scripts `docker exec` into for osm2pgsql |

## Landing-site demo

Read by the `barrelman-landing` service in `docker-compose.dev.yml` (the
`landing` profile), not by the API. Both are ordinary keys on an account an
administrator has moved onto the `demo` plan — see
[accounts.md](accounts.md#unmetered-keys-and-why-nobody-can-mint-one).

| Variable | Default | Description |
|---|---|---|
| `BARRELMAN_DEMO_KEY` | — | Server-side, for `/api/demo/*`. Never reaches the browser |
| `BARRELMAN_TILE_DEMO_KEY` | — | Browser-side, for map tiles. **Public by necessity** — scope it to `tiles` alone |

Two keys rather than one so either can be revoked on its own, and so the key
that ships in the page cannot run searches. Neither should be
`BARRELMAN_API_KEY`: that is the service credential Parchment uses, and
revoking it to stop a demo would take Parchment down with it.

## Database tuning

Compose defaults are sized for a dev laptop sharing RAM with MOTIS,
Elasticsearch and GraphHopper. **Production should give the database the box.**

| Variable | Compose default | 32GB host |
|---|---|---|
| `BARRELMAN_DB_SHARED_BUFFERS` | `1GB` | `8GB` |
| `BARRELMAN_DB_CACHE_SIZE` | `3GB` | `24GB` |
| `BARRELMAN_DB_WORK_MEM` | `64MB` | `128MB` |
| `BARRELMAN_DB_MAINTENANCE_WORK_MEM` | `1GB` | `2GB` |
| `BARRELMAN_DB_SHM_SIZE` | `1gb` | `2gb` |
| `BARRELMAN_DB_MEM_LIMIT` | `2g` | `28g` |
| `BARRELMAN_DB_RANDOM_PAGE_COST` | `1.1` | `1.1` (SSD); raise toward `4` on spinning disks |

Rules of thumb: ~25% of host RAM in `shared_buffers`, 50–75% in
`effective_cache_size`.

### The one that bites

`mem_limit` has to cover **`shared_buffers` + `shm_size` + `maintenance_work_mem`**,
plus headroom for `work_mem` and per-backend overhead.

`shared_buffers` and `/dev/shm` are both shared memory charged to the same
container cgroup, so they add up. Get it wrong and the kernel OOM-kills Postgres
partway through an index build, which surfaces at the client as a bare:

```
psql:/app/import/post-import.sql:114: error: connection to server was lost
```

Nothing in that message points at memory. Confirm it with
`docker inspect barrelman-db --format '{{.State.OOMKilled}}'`.

`BARRELMAN_DB_SHM_SIZE` exists because Docker's 64 MB default is far too small
for parallel index builds — those fail the other way, with
`could not resize shared memory segment ... No space left on device`, which
reads like a full disk. Its requirement scales with `maintenance_work_mem` and
the parallel worker count, so raise the two together.

Search latency is dominated by whether the working set stays resident, so these
matter more than they look.
