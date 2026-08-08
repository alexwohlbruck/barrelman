# Setting up Polar billing

> **Official deployment only.** The Commons Clause in [LICENSE](../LICENSE)
> removes the right to sell Barrelman, so charging third parties for access is
> not something a self-hosted instance may do — see
> [LICENSING.md](../LICENSING.md). Billing is gated on a signed license granting
> the `billing` feature (`src/lib/license.ts`), and `POLAR_ACCESS_TOKEN` alone
> does nothing without one. This document is operational notes for whoever runs
> the official API, not a self-hosting guide.

With no valid license the whole subscription surface is inert — every account
sits on the free plan, `/billing/*` returns 404, and the Polar SDK is never
constructed.

Work through it in the **sandbox** first (`POLAR_SANDBOX=true`,
[sandbox.polar.sh](https://sandbox.polar.sh)). It mirrors production with test
payments. The product IDs differ between the two environments, so you will
repeat steps 2–4 when you go live.

---

## 0. Issue the license

Nothing below has any effect until the server holds a license granting the
`billing` feature. Do this first.

`scripts/generate-license.ts` has no imports — only the `crypto.subtle` and
`Buffer` globals — so `bun` can run it by absolute path from any directory,
including a machine that has no checkout of this repo.

### Where the private key lives

The OS keychain, under **`dev.barrelman` / `license-signing-key`** — the same
place parchment keeps its master seed (`web/src-tauri/src/keychain.rs`), named
after the app the same way. The script reads it from there, so the seed never
appears in shell history, in `ps` output, or in a file anyone has to remember to
delete.

Back up the keychain. Losing the key means generating a new keypair and
reissuing every license; leaking it means anyone can mint themselves the
`billing` feature.

### Once, to create the keypair

```bash
bun ~/Documents/code/barrelman/scripts/generate-license.ts --keygen
```

This has already been done. It prints **only** the public key — the private seed
goes straight to the keychain and is never displayed. The public key is
committed as `DEFAULT_LICENSE_PUBLIC_KEY` in
[`src/lib/license.ts`](../src/lib/license.ts); while it was empty, no token
verified and billing was off everywhere, including production.

The command refuses to run if a key is already in the keychain, since a second
keypair would silently invalidate every license issued under the first. Delete
the entry in Keychain Access if a re-key is genuinely what you want.

### Per license

```bash
bun ~/Documents/code/barrelman/scripts/generate-license.ts --exp 2027-01-01
```

The seed comes from the keychain. `LICENSE_PRIVATE_KEY=<hex-seed>` still works
and takes precedence — for CI, for Linux hosts with no Secret Service, and for
anyone holding the seed elsewhere.

| Flag | Default | Meaning |
|---|---|---|
| `--exp` | none (perpetual) | Any date `Date.parse` accepts |
| `--org` | `barrelman` | Who the license is issued to |

Set the printed token as `BARRELMAN_LICENSE` on the server and recreate the
container — Compose bakes environment at creation, so a restart is not enough:

```bash
docker compose up -d --force-recreate barrelman
```

Startup logs which state it landed in:

```
[billing] Polar billing enabled (production)                      # licensed
[license] BARRELMAN_LICENSE is set but invalid or expired …       # bad token
[license] POLAR_ACCESS_TOKEN is set but no license grants …       # no token
```

**Renew before it lapses.** An expiring license does not cut customers off — it
stops you charging them. Quota is resolved from the `users.plan` column
(`credits.service.ts`), which never consults `billing.enabled`, so everyone
keeps serving at their paid allowance. What stops is the commercial machinery:

| Still works | Stops |
|---|---|
| Every data endpoint | New checkouts and the billing portal |
| Existing plans and their allowances | Overage reporting to Polar |
| Metering and credit accounting | Polar webhooks — `/billing/webhook` returns 404 |

The webhook is the one to watch. With it dark, subscription lifecycle events
stop being applied, so a customer who cancels or downgrades keeps their old plan
until a valid license lets the events flow again.

The license is deliberately not runnable from the admin console. It takes the
private key as input, and a web UI is the wrong place for that.

---

## 1. Organization and access token

1. Create an organization at [polar.sh](https://polar.sh).
2. **Settings → General** — copy the organization ID → `POLAR_ORGANIZATION_ID`.
3. **Settings → Developers → New Token**. Barrelman needs:

   | Scope | Used by |
   |---|---|
   | `products:read` | Pricing page — reads live prices |
   | `checkouts:write` | `POST /billing/checkout` |
   | `customers:read` | `POST /billing/sync` |
   | `customer_sessions:write` | `GET /billing/portal` |
   | `subscriptions:read` | `POST /billing/sync` |
   | `orders:read` | Credit-pack reconciliation |
   | `events:write` | Metered overage ingestion |

   Copy the token → `POLAR_ACCESS_TOKEN`. It is shown once.

---

## 2. Subscription products

Create one **recurring monthly** product per paid plan. The names are yours; the
IDs are what barrelman matches on.

| Plan | Price | Env var |
|---|---|---|
| Developer | $19 / month | `POLAR_DEVELOPER_PRODUCT_ID` |
| Business | $99 / month | `POLAR_BUSINESS_PRODUCT_ID` |
| Scale | $299 / month | `POLAR_SCALE_PRODUCT_ID` |

For each: **Products → New Product** → recurring, monthly, fixed price. Open the
product and copy the ID from the URL.

Do **not** create a product for Free or Enterprise. Free has no product by
definition, and Enterprise is `contactOnly` — checkout refuses it with a
"get in touch" message rather than sending anyone to a payment page.

> The allowances (1M / 10M / 40M credits) live in `src/billing/plans.ts`, not in
> Polar. Polar only knows the price. If you change an allowance, change it there.

---

## 3. Usage meter for overage

Paid plans keep serving past their allowance and bill the excess. That needs a
meter Polar can price.

1. **Products → Meters → New Meter**.
2. Name it, and set the event name to match `POLAR_USAGE_EVENT_NAME`
   (default `barrelman_credits`).
3. Aggregate over the `credits` metadata property, summed.
4. On each paid product, add a **metered price** billed against this meter:

   | Plan | Per 1,000 credits |
   |---|---|
   | Developer | $0.030 |
   | Business | $0.018 |
   | Scale | $0.012 |

Barrelman reports **only the credits beyond the included allowance** — the
allowance is already paid for in the subscription price, so metering all usage
would bill it twice. Reporting is idempotent: each pass computes the cycle's
total overage, compares it with what was already reported, and ingests the
difference.

Set `POLAR_METER_USAGE=false` to disable reporting entirely and run the paid
plans as flat-rate with soft limits.

---

## 4. Credit packs (optional)

One-off, non-expiring credits, spent only after the monthly allowance runs out.

1. Create a **one-time** product per pack (e.g. "500,000 credits" at $15).
2. Map product IDs to credit amounts:

   ```dotenv
   POLAR_CREDIT_PACKS=prod_abc123:500000,prod_def456:5000000
   ```

Grants are keyed on the Polar order ID, which is unique in the ledger — the
retries Polar performs cannot grant a pack twice.

---

## 5. Webhook

1. **Settings → Developers → Webhooks → Add Endpoint**.
2. URL: `https://<your-server-origin>/billing/webhook`.
3. Subscribe to:

   | Event | Effect |
   |---|---|
   | `subscription.active` | Links the customer, applies the plan |
   | `subscription.updated` | Re-applies the plan on an upgrade or downgrade |
   | `subscription.canceled` | Returns the account to free |
   | `subscription.revoked` | Returns the account to free |
   | `order.paid` | Grants a credit pack, if the product is one |

4. Copy the signing secret → `POLAR_WEBHOOK_SECRET`.

The endpoint is authenticated by Polar's signature over the raw body, and
**barrelman refuses to start if billing is enabled without the secret** — an
unsigned webhook endpoint would let anyone who guesses the URL grant themselves
a plan.

For local development the URL must be reachable from the internet:

```bash
ngrok http 5001
```

Then use `https://<subdomain>.ngrok.io/billing/webhook` as the endpoint.

---

## 6. Environment

```dotenv
POLAR_ACCESS_TOKEN=polar_oat_...
POLAR_WEBHOOK_SECRET=whsec_...
POLAR_ORGANIZATION_ID=...
POLAR_SANDBOX=true              # false in production

POLAR_DEVELOPER_PRODUCT_ID=...
POLAR_BUSINESS_PRODUCT_ID=...
POLAR_SCALE_PRODUCT_ID=...

# Optional
POLAR_CREDIT_PACKS=prod_abc:500000,prod_def:5000000
POLAR_USAGE_EVENT_NAME=barrelman_credits
POLAR_METER_USAGE=true
```

Restart the API. It logs `[billing] Polar billing enabled (sandbox)` on boot.

---

## 7. Verify

```bash
curl -s http://localhost:5001/billing/config | jq '.billingEnabled, .products'
```

`billingEnabled` should be `true` and `products` should carry live prices — if
the array is empty, the token is missing `products:read` or the product IDs are
from the wrong environment (sandbox IDs do not resolve in production).

Then, end to end:

1. Sign in to `/console`, open **Billing**, click **Upgrade** on Developer.
2. Complete the sandbox checkout (Polar's test card: `4242 4242 4242 4242`).
3. You land back on `/console/billing?checkout=success`, which calls
   `POST /billing/sync` — so the plan is correct even if the webhook is slow.
4. Confirm the plan changed:

   ```bash
   docker exec barrelman-db psql -U barrelman -d barrelman \
     -c "SELECT email, plan FROM accounts_users;"
   ```

5. Check the webhook delivered: **Settings → Developers → Webhooks →** your
   endpoint → Deliveries. A `403` means the signing secret does not match.

If a webhook is ever missed, **Refresh** on the billing page re-reads Polar and
wins over the local plan — that is the repair path.

---

## Going live

1. Repeat steps 2–4 in the production organization; the IDs are different.
2. Swap the token, secret, organization and product IDs.
3. `POLAR_SANDBOX=false`.
4. Point the webhook at the production origin.
5. Re-run the verification above with a real card, then refund it in Polar.
