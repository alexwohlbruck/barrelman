<script setup lang="ts">
/**
 * Plans, credit packs and the link out to the payment provider.
 *
 * When billing is not configured on the instance this page still renders the
 * plan table — a self-hosted operator should be able to see what the tiers mean
 * for their own metering — but every purchase control is hidden, because there
 * is nothing behind it.
 */
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { Check, ExternalLink, RefreshCw } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import Badge from '@/components/ui/Badge.vue'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import CardContent from '@/components/ui/CardContent.vue'
import CardHeader from '@/components/ui/CardHeader.vue'
import CardTitle from '@/components/ui/CardTitle.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { getBillingConfig, getBillingStatus, getLedger, getPortalUrl, startCheckout, syncBilling } from '@/lib/api'
import { toast } from '@/lib/toast'
import { formatNumber } from '@/lib/utils'
import { refreshUser } from '@/lib/auth'
import type { BillingConfig, BillingStatus, LedgerEntry, Plan } from '@/lib/types'

const route = useRoute()

const config = ref<BillingConfig | null>(null)
const status = ref<BillingStatus | null>(null)
const ledger = ref<LedgerEntry[]>([])
const loading = ref(true)
const busy = ref('')

const currentPlanId = computed(() => status.value?.plan.id ?? 'free')

function planRank(id: string) {
  return config.value?.plans.find((p) => p.id === id)?.rank ?? 0
}
const billingEnabled = computed(() => config.value?.billingEnabled === true)

/**
 * Live price from the payment provider when it is configured, falling back to
 * the plan's list price. A pricing page that renders nothing because Polar is
 * unreachable is worse than one showing the list price.
 */
function priceFor(plan: Plan) {
  const product = config.value?.products.find((p) => p.planId === plan.id)
  const cents = product?.priceAmount ?? plan.priceCents
  const currency = (product?.priceCurrency ?? 'usd').toUpperCase()

  if (plan.contactOnly) return { amount: 'Custom', interval: null }
  if (cents === 0) return { amount: 'Free', interval: null }

  return {
    amount: (cents / 100).toLocaleString(undefined, {
      style: 'currency',
      currency,
      maximumFractionDigits: 0,
    }),
    interval: product?.interval ?? 'month',
  }
}

/** Dollars per 1,000 overage credits, formatted for the plan card. */
function overageLabel(plan: Plan) {
  const perThousand = plan.overagePerThousand ?? plan.overageMicrosPerCredit / 1000
  return `$${perThousand.toFixed(perThousand < 0.01 ? 4 : 3).replace(/0+$/, '').replace(/\.$/, '')}`
}

function fail(err: unknown, title: string) {
  toast({ title, description: err instanceof Error ? err.message : undefined, variant: 'error' })
}

async function load() {
  loading.value = true
  try {
    const [cfg, st, led] = await Promise.all([
      getBillingConfig(),
      getBillingStatus(),
      getLedger().catch(() => ({ entries: [] })),
    ])
    config.value = cfg
    status.value = st
    ledger.value = led.entries
  } catch (err) {
    fail(err, 'Could not load billing')
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  await load()
  // Returning from a successful checkout, the webhook may not have landed yet;
  // ask the provider directly rather than showing a stale plan.
  if (route.query.checkout === 'success') await sync()
})

async function subscribe(planId: string) {
  busy.value = planId
  try {
    const { checkoutUrl } = await startCheckout({ plan: planId })
    window.location.href = checkoutUrl
  } catch (err) {
    fail(err, 'Could not start checkout')
    busy.value = ''
  }
}

async function buyCredits(productId: string) {
  busy.value = productId
  try {
    const { checkoutUrl } = await startCheckout({ creditPack: productId })
    window.location.href = checkoutUrl
  } catch (err) {
    fail(err, 'Could not start checkout')
    busy.value = ''
  }
}

async function openPortal() {
  busy.value = 'portal'
  try {
    const { portalUrl } = await getPortalUrl()
    window.location.href = portalUrl
  } catch (err) {
    fail(err, 'Could not open the billing portal')
  } finally {
    busy.value = ''
  }
}

async function sync() {
  busy.value = 'sync'
  try {
    await syncBilling()
    await Promise.all([load(), refreshUser()])
    toast({ title: 'Subscription refreshed', variant: 'success' })
  } catch (err) {
    fail(err, 'Could not refresh the subscription')
  } finally {
    busy.value = ''
  }
}
</script>

<template>
  <PageHeader title="Billing" subtitle="Your plan, credits and invoices">
    <template #actions>
      <Button v-if="billingEnabled" variant="outline" size="sm" :disabled="busy === 'sync'" @click="sync">
        <RefreshCw :class="['size-4', busy === 'sync' && 'animate-spin']" />
        Refresh
      </Button>
      <Button
        v-if="billingEnabled && status?.hasSubscription"
        variant="outline"
        size="sm"
        :disabled="busy === 'portal'"
        @click="openPortal"
      >
        <ExternalLink class="size-4" />
        Manage payment
      </Button>
    </template>
  </PageHeader>

  <div class="mx-auto max-w-5xl space-y-6 p-8">
    <div v-if="loading" class="flex justify-center py-16"><Spinner class="size-6" /></div>

    <template v-else>
      <div v-if="!billingEnabled" class="rounded-lg border border-border bg-muted/40 px-4 py-3 text-sm text-muted-foreground">
        Billing is not configured on this instance, so every account stays on the free plan. The tiers
        below describe what metering would apply if it were.
      </div>

      <!-- Exhausted free quota: the single most useful thing to say here -->
      <div
        v-if="status && !status.balance.overageAllowed && status.balance.remaining <= 0"
        class="rounded-lg border border-destructive bg-destructive/5 px-4 py-3 text-sm"
      >
        <p class="font-medium text-destructive">Your free credits for this period are used up</p>
        <p class="mt-1 text-muted-foreground">
          API requests are being refused with <code>402</code> until
          {{ new Date(status.balance.cycleResetsAt).toLocaleDateString(undefined, { dateStyle: 'medium', timeZone: 'UTC' }) }}
          (UTC). Upgrade below to resume immediately — the free plan deliberately stops rather than
          billing you for overage.
        </p>
      </div>

      <!-- Plans -->
      <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <Card
          v-for="plan in config?.plans ?? []"
          :key="plan.id"
          :class="plan.id === currentPlanId ? 'border-primary ring-1 ring-primary' : ''"
        >
          <CardHeader class="flex-row items-center justify-between">
            <CardTitle>{{ plan.name }}</CardTitle>
            <Badge v-if="plan.id === currentPlanId" variant="success">Current</Badge>
          </CardHeader>
          <CardContent class="flex h-full flex-col gap-4">
            <div>
              <span class="text-2xl font-semibold">{{ priceFor(plan).amount }}</span>
              <span v-if="priceFor(plan).interval" class="text-sm text-muted-foreground">
                /{{ priceFor(plan).interval }}
              </span>
            </div>
            <p class="text-sm text-muted-foreground">{{ plan.description }}</p>
            <ul class="flex flex-col gap-1.5 text-sm">
              <li class="flex items-center gap-2">
                <Check class="size-3.5 text-[var(--success)]" />
                <!-- The stored figures for a negotiated plan are placeholders;
                     printing them would read as a real quoted allowance. -->
                <span v-if="plan.contactOnly">Custom volume and rate limits</span>
                <span v-else>{{ formatNumber(plan.monthlyCredits) }} credits / month</span>
              </li>
              <li v-if="!plan.contactOnly" class="flex items-center gap-2">
                <Check class="size-3.5 text-[var(--success)]" />
                {{ formatNumber(plan.requestsPerMinute) }} requests / minute
              </li>
              <li v-if="!plan.contactOnly" class="flex items-center gap-2 text-muted-foreground">
                <Check class="size-3.5" :class="plan.overageAllowed ? 'text-[var(--success)]' : 'opacity-30'" />
                <span v-if="plan.overageAllowed">Then {{ overageLabel(plan) }} / 1k credits</span>
                <span v-else>Stops at the allowance — never billed for overage</span>
              </li>
              <li class="flex items-center gap-2 text-muted-foreground">
                <Check class="size-3.5" :class="plan.commercialUse ? 'text-[var(--success)]' : 'opacity-30'" />
                <span>{{ plan.commercialUse ? 'Commercial use' : 'Evaluation and non-commercial use' }}</span>
              </li>
            </ul>
            <div class="mt-auto pt-2">
              <Button
                v-if="plan.contactOnly && plan.id !== currentPlanId"
                variant="outline"
                class="w-full"
                as="a"
                href="mailto:sales@barrelman.dev?subject=Enterprise%20plan"
              >
                Contact us
              </Button>
              <Button
                v-else-if="billingEnabled && plan.id !== currentPlanId && plan.priceCents > 0"
                class="w-full"
                :disabled="busy === plan.id"
                @click="subscribe(plan.id)"
              >
                <Spinner v-if="busy === plan.id" class="size-4" />
                <template v-else>{{ planRank(plan.id) > planRank(currentPlanId) ? 'Upgrade' : 'Switch' }}</template>
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      <!-- Credit packs -->
      <Card v-if="billingEnabled && config?.creditPacks.length">
        <CardHeader><CardTitle>Credit packs</CardTitle></CardHeader>
        <CardContent class="flex flex-col gap-3">
          <p class="text-sm text-muted-foreground">
            One-off credits that never expire. They are spent only after your monthly allowance runs out.
          </p>
          <div class="flex flex-wrap gap-2">
            <Button
              v-for="pack in config.creditPacks"
              :key="pack.productId"
              variant="outline"
              :disabled="busy === pack.productId"
              @click="buyCredits(pack.productId)"
            >
              <Spinner v-if="busy === pack.productId" class="size-4" />
              <template v-else>{{ formatNumber(pack.credits) }} credits</template>
            </Button>
          </div>
        </CardContent>
      </Card>

      <!-- Ledger -->
      <Card v-if="ledger.length">
        <CardHeader><CardTitle>Credit history</CardTitle></CardHeader>
        <CardContent>
          <table class="w-full text-sm">
            <tbody>
              <tr v-for="entry in ledger" :key="entry.id" class="border-b border-border/50 last:border-0">
                <td class="py-2">
                  {{ entry.description || entry.kind }}
                  <span class="ml-1.5 text-xs text-muted-foreground">
                    {{ new Date(entry.createdAt).toLocaleDateString(undefined, { dateStyle: 'medium' }) }}
                  </span>
                </td>
                <td
                  class="py-2 text-right tabular-nums"
                  :class="entry.amount >= 0 ? 'text-[var(--success)]' : 'text-destructive'"
                >
                  {{ entry.amount >= 0 ? '+' : '' }}{{ formatNumber(entry.amount) }}
                </td>
              </tr>
            </tbody>
          </table>
        </CardContent>
      </Card>
    </template>
  </div>
</template>
