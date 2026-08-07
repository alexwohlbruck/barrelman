# Pricing & credits

> This describes the **hosted API** at `api.barrelman.dev`. A self-hosted
> instance meters usage the same way but has nothing to charge — subscription
> billing is gated on a license only the official deployment holds, because the
> Commons Clause forbids selling barrelman. See [LICENSING.md](../LICENSING.md).

Usage is metered in **credits**, not requests, because barrelman's endpoints are
wildly unequal. A vector tile is one indexed read; an isochrone fans out to
hundreds of GraphHopper calls. Charging both as "one request" would either give
tiles away at a loss or price routing as if it were free.

Credits are integers with the cheapest operation as the unit, so a balance stays
exact across millions of small charges — no floating-point drift.

## What things cost

| Group | Credits | Endpoints |
|---|---|---|
| `tiles` | 1 | `/tiles/*` |
| `places` | 3 | `/place/*`, `/brands` |
| `spatial` | 3 | `/contains`, `/children` |
| `geocode` | 5 | `/geocode/*` |
| `search` | 6 | `/search`, `/autocomplete` |
| `routing` | 12 | `/route`, `/graphhopper/*` |
| `transit` | 20 | `/transit/*`, `/gbfs/*` |
| `isochrone` | 40 | `/isochrone` |

The ratios are calibrated against the market rather than invented. Taking a tile
as 1, [Mapbox](https://www.mapbox.com/pricing) prices geocoding at roughly 3×
and directions at 8×; [Stadia Maps](https://stadiamaps.com/pricing/) prices both
at 20×. Barrelman sits between them, which is roughly where the real cost sits
too — a geocode is more than a tile read but nowhere near a routing solve.

A test pins that ratio inside the market band, so a future edit cannot quietly
drift out of it.

## Plans

| Plan | Price | Credits / month | Rate limit | Past the allowance |
|---|---|---|---|---|
| **Free** | $0 | 100,000 | 300 / min | **Stops with `402`** |
| **Developer** | $19 | 1,000,000 | 900 / min | $0.030 / 1k |
| **Business** | $99 | 10,000,000 | 1,800 / min | $0.018 / 1k |
| **Scale** | $299 | 40,000,000 | 6,000 / min | $0.012 / 1k |
| **Enterprise** | Custom | Negotiated | Negotiated | $0.008 / 1k |

At Developer, $19 buys 200,000 geocodes, 166,000 searches or 83,000 routes. The
same geocoding volume on Mapbox is roughly $150.

Free is **evaluation and non-commercial**; every paid plan includes commercial
use. That matches how MapTiler and Stadia license theirs.

Enterprise is `contactOnly` — checkout refuses it with a "get in touch" rather
than sending anyone to a payment page.

### The plan that is not on the ladder

There is a sixth, `demo`, which does not appear here, in `/account/plans`, or in
the console's plan picker, because it cannot be bought. It is `internal: true`
and an administrator assigns it. It serves the API **unmetered**, and is what
runs the interactive demo on the landing page.

A demo cannot be a normal account: metered, it eventually stops, and it stops on
whichever afternoon the page gets attention. What bounds it instead is a
per-visitor rate limit, 240/min per address inside a 3,000/min account ceiling.
That is a better fit for the actual risk — one account, thousands of strangers,
where an account-wide limit means the first scraper decides everyone else's
experience.

Unmetered is not unbilled-and-unseen: usage is still recorded at zero credits,
so demo traffic shows up in the dashboards and in abuse detection like anyone
else's. See
[accounts.md](accounts.md#unmetered-keys-and-why-nobody-can-mint-one) for why
this is a plan rather than a kind of key.

### Two deliberate properties

**The free tier stops rather than billing.** At zero credits the API answers
`402` with the reset date until the next cycle:

```json
{
  "error": "Credit allowance exhausted for this billing period. …",
  "remaining": 0,
  "resetsAt": "2026-09-01T00:00:00.000Z"
}
```

Nobody can run up a bill on a plan they did not pay for.

**Overage is capped.** Paid plans keep serving past the allowance, but only up
to a multiple of it (3x on Developer and Business, 2x on Scale); past that,
requests are refused with a `402` carrying `reason: "overage-cap-reached"`. A
leaked key would otherwise accrue charges with no ceiling — a Developer key at
its own rate limit against `/isochrone` is roughly $1/minute — and the
burn-rate detector only runs on the hourly sweep. Enterprise is uncapped
because its volume is contractual.

**Overage stays close to the included rate** — 1.6–1.8×, not the 10× an earlier
draft charged. The customer hitting overage is the one about to buy the next
plan up; punishing them for it is backwards. A test asserts this stays within 2×,
and that a bigger plan is never more expensive per credit.

## Cycles and balances

Cycles are **calendar months in UTC**. The monthly allowance resets on the 1st
and does not roll over.

A balance has two parts:

1. The plan's monthly allowance.
2. Purchased credits from the ledger, which never expire.

The allowance is spent **first**, so a prepaid pack survives an idle month
rather than being silently consumed by usage the plan already covered.

## Headers

| Header | Meaning |
|---|---|
| `X-Barrelman-Credits-Charged` | What this request cost |
| `X-Barrelman-Overage` | Present when the charge fell past the allowance |
| `Retry-After` | On a `429`, seconds until the window rolls |

A request that fails with a **5xx is refunded** — customers should not pay for
our outages.

## How metering works

Barrelman serves autocomplete: one request per keystroke. A database write per
request would cost more than the read it is measuring. So counters accumulate in
memory and flush on a timer as a single multi-row upsert, keyed by
(user, key, day, endpoint group).

The trade-off is explicit: a crash loses at most one flush interval of counters,
always in the customer's favour. **Credit grants are never buffered** — those go
straight to the ledger.

Quota decisions read a short-lived cache rather than the database, so an account
can overshoot by at most the traffic it lands inside one TTL. Bounded, small,
and much cheaper than a round trip per request.

Neither of those applies to money coming *in*: purchases are written
synchronously and keyed on the provider's order ID.

## Where the numbers live

`src/billing/plans.ts` is the source of truth. `GET /account/plans` serves it.

The marketing site duplicates the figures so it can build statically with no API
dependency — if the two disagree, `/account/plans` wins, and the site needs
updating. This is a known duplication, recorded in the landing repo's README.

## Changing prices

1. Edit `CREDIT_COSTS` or `PLANS` in `src/billing/plans.ts`.
2. Run `bun test src/billing/plans.test.ts` — the invariants (monotonic
   allowances, overage within 2× of included, free never accrues) are asserted,
   not assumed.
3. Update the Polar products to match — see [polar-setup.md](polar-setup.md).
   Polar only knows the price; the allowance lives in the code.
4. Update the landing site.

Existing subscribers keep the Polar price they signed up at until you change it
there; the *allowance* changes immediately for everyone, because it is read from
the code on every request.
