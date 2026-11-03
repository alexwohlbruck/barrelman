<script setup lang="ts">
/**
 * Request outcomes, rate limiting and latency, for the whole instance or one
 * account. The Traffic page and the account pane both render this, so what an
 * operator sees about one customer is the same view filtered, not a second
 * set of numbers that could disagree with the first.
 */
import { computed, ref, watch } from 'vue'
import { Gauge, ShieldOff } from 'lucide-vue-next'
import Badge from '@/components/ui/Badge.vue'
import Spinner from '@/components/ui/Spinner.vue'
import Switch from '@/components/ui/Switch.vue'
import StatTile from '@/components/StatTile.vue'
import TrafficChart from '@/components/traffic/TrafficChart.vue'
import { getTraffic, updateAdminKey } from '@/lib/api'
import { toast } from '@/lib/toast'
import {
  RANGES,
  callerLabel,
  formatMs,
  isAccount,
  keyLabel,
  limitedBreakdown,
  percent,
} from '@/lib/traffic'
import { formatNumber, cn } from '@/lib/utils'
import type { CallerTraffic, TrafficRange, TrafficReport } from '@/lib/types'

const props = defineProps<{ userId?: string; refreshKey?: number }>()
const emit = defineEmits<{ openAccount: [subject: string]; keysChanged: [] }>()

const range = ref<TrafficRange>(props.userId ? '7d' : '24h')
const report = ref<TrafficReport | null>(null)
const loading = ref(false)
const busyKey = ref<string | null>(null)

async function load() {
  loading.value = true
  try {
    report.value = await getTraffic(range.value, props.userId)
  } catch (err) {
    toast({
      title: 'Could not load traffic',
      description: err instanceof Error ? err.message : undefined,
      variant: 'error',
    })
  } finally {
    loading.value = false
  }
}

watch([range, () => props.userId, () => props.refreshKey], load, { immediate: true })

const totals = computed(() => report.value?.totals)
const top = computed(() => report.value?.latencyBoundsMs.at(-1) ?? 10_000)
const ms = (value: number | null) => formatMs(value, top.value)

/** "Throttled per key 120 · concurrency 4. 15 refused for credits." */
const refusalSummary = computed(() => {
  const stats = totals.value
  if (!stats) return ''
  const parts: string[] = []
  const layers = limitedBreakdown(stats)
  if (layers.length) parts.push(`Throttled ${layers.map((e) => `${e.layer} ${formatNumber(e.count)}`).join(' · ')}.`)
  if (stats.quotaRefused) parts.push(`${formatNumber(stats.quotaRefused)} refused for credits.`)
  if (stats.denied) parts.push(`${formatNumber(stats.denied)} refused for keys or scopes.`)
  return parts.join(' ')
})

/** In the account pane every row is the same account, so the column is noise. */
const showAccount = computed(() => !props.userId)

async function toggleExempt(row: CallerTraffic, exempt: boolean) {
  if (!row.apiKeyId) return
  busyKey.value = row.apiKeyId
  try {
    await updateAdminKey(row.subject, row.apiKeyId, { rateLimitExempt: exempt })
    row.rateLimitExempt = exempt
    toast({
      title: exempt ? 'Rate limits disabled for this key' : 'Rate limits restored for this key',
      variant: 'success',
    })
    emit('keysChanged')
  } catch (err) {
    toast({ title: 'Could not change the key', description: err instanceof Error ? err.message : undefined, variant: 'error' })
  } finally {
    busyKey.value = null
  }
}
</script>

<template>
  <div class="flex flex-col gap-5">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="inline-flex h-9 items-center rounded-lg bg-muted p-1 text-sm">
        <button
          v-for="option in RANGES"
          :key="option.value"
          type="button"
          :class="cn(
            'rounded-md px-3 py-1 font-medium transition-all',
            range === option.value ? 'bg-background text-foreground shadow' : 'text-muted-foreground hover:text-foreground',
          )"
          @click="range = option.value"
        >
          {{ option.label }}
        </button>
      </div>
      <Spinner v-if="loading && report" class="size-4 text-muted-foreground" />
    </div>

    <div v-if="loading && !report" class="flex justify-center py-12"><Spinner class="size-6" /></div>

    <template v-else-if="report && totals">
      <div class="grid gap-3 sm:grid-cols-4">
        <StatTile label="Requests" :value="formatNumber(totals.requests)" :sub="`${formatNumber(totals.served)} served`" />
        <StatTile
          label="Rate-limited"
          :value="percent(totals.rateLimited, totals.requests)"
          :sub="`${formatNumber(totals.rateLimited)} refused`"
        />
        <StatTile label="Latency p95" :value="ms(totals.p95Ms)" :sub="`p50 ${ms(totals.p50Ms)} · p99 ${ms(totals.p99Ms)}`" />
        <StatTile
          label="Server errors"
          :value="percent(totals.serverErrors, totals.served)"
          :sub="`${formatNumber(totals.serverErrors)} of ${formatNumber(totals.served)}`"
        />
      </div>

      <p v-if="refusalSummary" class="-mt-2 text-xs text-muted-foreground">{{ refusalSummary }}</p>

      <section class="rounded-xl border border-border p-4">
        <TrafficChart v-if="totals.requests" :series="report.series" :bucket="report.bucket" />
        <p v-else class="py-10 text-center text-sm text-muted-foreground">No metered requests in this window.</p>
      </section>

      <!-- By endpoint -->
      <section v-if="report.byEndpoint.length">
        <h3 class="mb-2 text-sm font-medium">By endpoint</h3>
        <div class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead class="text-xs text-muted-foreground">
              <tr class="border-b border-border">
                <th class="pb-2 text-left font-medium">Group</th>
                <th class="pb-2 text-right font-medium">Requests</th>
                <th class="pb-2 text-right font-medium">Limited</th>
                <th class="pb-2 text-right font-medium">p50</th>
                <th class="pb-2 text-right font-medium">p95</th>
                <th class="pb-2 text-right font-medium">p99</th>
                <th class="pb-2 text-right font-medium">5xx</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in report.byEndpoint" :key="row.endpoint" class="border-b border-border/50 last:border-0">
                <td class="py-1.5">{{ row.endpoint }}</td>
                <td class="py-1.5 text-right tabular-nums">{{ formatNumber(row.requests) }}</td>
                <td class="py-1.5 text-right tabular-nums" :class="row.rateLimited ? 'text-[var(--warning)]' : 'text-muted-foreground'">
                  {{ percent(row.rateLimited, row.requests) }}
                </td>
                <td class="py-1.5 text-right tabular-nums">{{ ms(row.p50Ms) }}</td>
                <td class="py-1.5 text-right tabular-nums">{{ ms(row.p95Ms) }}</td>
                <td class="py-1.5 text-right tabular-nums">{{ ms(row.p99Ms) }}</td>
                <td class="py-1.5 text-right tabular-nums" :class="row.serverErrors ? 'text-destructive' : 'text-muted-foreground'">
                  {{ percent(row.serverErrors, row.served) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <!-- By caller -->
      <section v-if="report.byCaller.length">
        <div class="mb-2 flex items-baseline justify-between gap-3">
          <h3 class="text-sm font-medium">{{ showAccount ? 'By account and key' : 'By key' }}</h3>
          <span class="text-xs text-muted-foreground">Most rate-limited first</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead class="text-xs text-muted-foreground">
              <tr class="border-b border-border">
                <th v-if="showAccount" class="pb-2 text-left font-medium">Account</th>
                <th class="pb-2 text-left font-medium">Key</th>
                <th class="pb-2 text-right font-medium">Requests</th>
                <th class="pb-2 text-right font-medium">Limited</th>
                <th class="pb-2 text-right font-medium">p95</th>
                <th class="pb-2 text-right font-medium" title="Rate limits apply to this key">Limits</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="row in report.byCaller"
                :key="`${row.subject}:${row.apiKeyId}`"
                class="border-b border-border/50 last:border-0"
              >
                <td v-if="showAccount" class="max-w-[14rem] py-2">
                  <button
                    v-if="isAccount(row.subject)"
                    type="button"
                    class="block max-w-full truncate text-left hover:underline"
                    @click="emit('openAccount', row.subject)"
                  >
                    {{ callerLabel(row) }}
                  </button>
                  <span v-else class="text-muted-foreground">{{ callerLabel(row) }}</span>
                  <span v-if="row.plan" class="text-xs text-muted-foreground">{{ row.plan }}</span>
                </td>
                <td class="max-w-[12rem] py-2">
                  <div class="flex items-center gap-1.5">
                    <span class="truncate">{{ keyLabel(row) }}</span>
                    <Badge v-if="row.revoked" variant="muted">revoked</Badge>
                  </div>
                  <code v-if="row.keyPrefix" class="text-[11px] text-muted-foreground">{{ row.keyPrefix }}…</code>
                </td>
                <td class="py-2 text-right tabular-nums">{{ formatNumber(row.requests) }}</td>
                <td class="py-2 text-right tabular-nums">
                  <span :class="row.rateLimited ? 'text-[var(--warning)]' : 'text-muted-foreground'">
                    {{ formatNumber(row.rateLimited) }}
                    <span class="text-xs">({{ percent(row.rateLimited, row.requests) }})</span>
                  </span>
                  <div v-if="limitedBreakdown(row).length" class="text-[11px] text-muted-foreground">
                    {{ limitedBreakdown(row).map((e) => `${e.layer} ${formatNumber(e.count)}`).join(' · ') }}
                  </div>
                </td>
                <td class="py-2 text-right tabular-nums">{{ ms(row.p95Ms) }}</td>
                <td class="py-2 text-right">
                  <div v-if="row.apiKeyId && isAccount(row.subject) && !row.revoked" class="flex items-center justify-end gap-2">
                    <ShieldOff v-if="row.rateLimitExempt" class="size-3.5 text-muted-foreground" />
                    <Switch
                      :model-value="!row.rateLimitExempt"
                      :disabled="busyKey === row.apiKeyId"
                      :title="row.rateLimitExempt ? 'Rate limits disabled — click to restore' : 'Rate limits on — click to disable'"
                      @update:model-value="toggleExempt(row, !$event)"
                    />
                  </div>
                  <span v-else class="text-muted-foreground">—</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <p v-if="!userId" class="flex items-center gap-1.5 text-xs text-muted-foreground">
        <Gauge class="size-3.5" />
        Live on this replica: {{ formatNumber(report.throttle.inFlight) }} in flight,
        {{ formatNumber(report.throttle.penalised) }} in the penalty box,
        {{ formatNumber(report.throttle.trackedKeys) }} keys tracked.
      </p>
    </template>
  </div>
</template>
