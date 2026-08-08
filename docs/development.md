# Development

Everything runs under Docker Compose. You should not need to start a server by
hand.

This page is about working *on* barrelman from a clone. If you want to stand an
instance *up*, see [the self-hosting guide](https://docs.barrelman.dev/self-hosting).

## The stack

```bash
./start.sh dev            # everything, with hot reload
./start.sh dev --build    # rebuild images first
./start.sh dev --down     # stop
```

| Service | URL | What it is |
|---|---|---|
| `barrelman` | http://localhost:5001 | The API (Bun, hot-reloaded from `src/`) |
| `barrelman-console` | http://localhost:5199/console | Console dev server (Vite, HMR) |
| `barrelman-landing` | http://localhost:5200 | Marketing site (Nuxt, HMR) |
| `barrelman-db` | localhost:5434 | PostgreSQL + PostGIS + pgvector |
| `martin` | localhost:5002 | Vector tiles, normally reached through the API |
| `graphhopper` | localhost:5003 | Street routing |
| `motis` | localhost:5004 | Transit routing |
| `barrelman-ops` | — | Worker that runs import jobs from the console |

Also worth knowing:

- **API docs** at http://localhost:5001/docs — the OpenAPI surface.
- In production the API serves the pre-built console itself at `/console`; the
  separate dev server exists only for HMR.
- The console dev server is Vite with `base: '/console/'`, so the **trailing
  slash is required** in dev. Production (served by Elysia) accepts both.

## Where jobs actually run

The API container is deliberately lean — no docker CLI, no osmium, no python.
It only *enqueues* `process` jobs into Postgres. **`barrelman-ops`** mounts the
docker socket, carries that tooling, and claims them one at a time under a
Postgres advisory lock (single-flight, so two imports can never overlap).
`internal` jobs — SQL and migrations — still run in-process in the API, which
already has the DB client.

So anything that shells out runs there:

```bash
docker compose exec -d barrelman-ops bash scripts/run-import.sh
docker compose logs -f barrelman-ops
```

Running the same script in `barrelman` fails on a missing `docker` or `osmium`.
Job state — status, exit code, logs — lives in the DB, so the console renders
one unified list either way.

`barrelman-ops` is **not** source-mounted, unlike the API. Changes to
`scripts/` or `import/` need `./start.sh dev --build` to reach it.

## Deploying a branch to a server

`docker-compose.yml` gives `barrelman` and `barrelman-db` an `image:` and no
`build:` — the `build:` stanzas live in `docker-compose.dev.yml`. So on a server
that only has the base file:

```bash
docker compose build barrelman     # → "No services to build", exit 0
```

It is a silent no-op, and the container keeps running the published image from
`main` while you believe your fix is deployed. Build those two explicitly:

```bash
docker build -t alexwohlbruck/barrelman:latest .
docker build -f Dockerfile.db -t alexwohlbruck/barrelman-db:latest .
docker compose up -d --force-recreate barrelman barrelman-db
```

`barrelman-ops` does have a `build:` in the base file, so `docker compose build
barrelman-ops` works — but it still needs an explicit rebuild for any change to
`scripts/` or `import/`, since it is not source-mounted.

## The marketing site

It lives in its own repository, checked out alongside this one — the same layout
`parchment` and `parchment-landing` use:

```
code/
├── barrelman/
└── barrelman-landing/
```

`start.sh dev` detects the sibling directory and enables the `landing` compose
profile. Without it the service is skipped and everything else still comes up,
so cloning barrelman on its own works fine.

To run only that service:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  --profile landing up barrelman-landing
```

## Direct compose

`start.sh` is a thin wrapper. The equivalent:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile landing up -d
```

`.env` sets `COMPOSE_FILE` to both files, so a bare `docker compose up` in this
directory already picks up the dev overrides — the `--profile landing` flag is
the only part you have to remember.

## Hot reload, and where it stops

The API runs under `bun --hot` with `src/` bind-mounted, so most edits apply
instantly. Two things it does **not** pick up:

- **New dependencies.** `package.json` is baked into the image, not mounted, so
  adding one means rebuilding:

  ```bash
  ./start.sh dev --build
  ```

  That also renews the anonymous `node_modules` volumes, which matters more than
  it sounds. Those volumes exist so the container's Linux installs never fight
  the host's macOS ones — but Compose **reuses them across recreation**, so a
  plain `docker compose build` leaves a stale `node_modules` masking the freshly
  built image. The symptom is an API that crash-loops on a module which is
  demonstrably present in the image:

  ```
  error: Cannot find module '@polar-sh/sdk/webhooks'
  ```

  If you ever see that, the volume is stale, not the image. `--build` handles it;
  by hand it is `docker compose ... up -d --build --renew-anon-volumes`.

  For a quick one-off without a rebuild:

  ```bash
  docker cp package.json barrelman:/app/package.json
  docker cp bun.lock barrelman:/app/bun.lock
  docker exec barrelman bun install
  docker restart barrelman
  ```

  This patches the running container only; the next recreation loses it.

- **Module-level singletons.** `bun --hot` re-evaluates a module's body but
  keeps the old instance for things constructed at import time — the Lucia
  client, the Polar client, the metering timers. If a change to one of those
  seems not to apply, `docker restart barrelman` before you go looking for the
  bug.

Anything started on an interval is guarded on `globalThis` for the same reason:
without the guard, each hot reload stacks another copy of the timer.

## Tests

```bash
bun test                  # unit and route tests — no database needed
bun run test:integration  # hits the real DB; opt-in
```

Integration tests are gated behind `BARRELMAN_INTEGRATION_TESTS=1`, which the
script sets. They need a database with imported data on `DATABASE_URL`, and
some of them assert against North Carolina content specifically.

For the console:

```bash
cd web && bun run typecheck && bun run build
```

## Database

```bash
docker exec -it barrelman-db psql -U barrelman -d barrelman
```

Account tables are prefixed `accounts_`. All schema is created by idempotent
`ensure*Schema()` calls at startup rather than a migration tool, so a fresh
database needs no migration step — see `src/services/accounts.service.ts` for
the accounts DDL, which mirrors `src/schema/accounts.ts`.

## Signing in locally

With no SMTP configured, sign-in codes are printed to the server log instead of
emailed:

```bash
docker logs barrelman --tail 20 | grep "sign-in code"
```

The first account created on a fresh database becomes an administrator, so a
new local instance is never locked out of its own console.
