<script setup lang="ts">
/**
 * Usage and credit consumption for the current billing cycle.
 *
 * The bar chart is plain flexbox divs rather than a charting dependency: it
 * plots one series over at most 31 days, and the console already ships enough
 * JavaScript.
 */
import { computed, onMounted, ref } from 'vue'
import { RefreshCw } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import StatTile from '@/components/StatTile.vue'
import Badge from '@/components/ui/Badge.vue'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import CardContent from '@/components/ui/CardContent.vue'
import CardHeader from '@/components/ui/CardHeader.vue'
import CardTitle from '@/components/ui/CardTitle.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { getApiKeys, getCredits, getUsage } from '@/lib/api'
import { toast } from '@/lib/toast'
import { creditsPerDay, formatCycleDate, keyLabel as labelForKey, shortDay, totalsByEndpoint } from '@/lib/usage'
import { formatNumber } from '@/lib/utils'
import type { ApiKeySummary, CreditBalance, UsageReport } from '@/lib/types'

const balance = ref<CreditBalance | null>(null)
const usage = ref<UsageReport | null>(null)
const keys = ref<ApiKeySummary[]>([])
const loading = ref(true)

const byDay = computed(() => creditsPerDay(usage.value?.daily ?? []))
const byEndpoint = computed(() => totalsByEndpoint(usage.value?.daily ?? []))

const totalRequests = computed(() => byEndpoint.value.reduce((sum, row) => sum + row.requests, 0))
const totalRejected = computed(() => byEndpoint.value.reduce((sum, row) => sum + row.rejected, 0))
const peakDay = computed(() => Math.max(1, ...byDay.value.map((d) => d.credits)))

const usedPercent = computed(() => {
  if (!balance.value || balance.value.monthlyCredits === 0) return 0
  return Math.min(100, Math.round((balance.value.used / balance.value.monthlyCredits) * 100))
})

const keyNames = computed(() => new Map(keys.value.map((key) => [key.id, key.name])))

function keyLabel(id: string) {
  return labelForKey(id, keyNames.value)
}

async function load() {
  loading.value = true
  try {
    const [credits, report, keyList] = await Promise.all([getCredits(), getUsage(), getApiKeys(true)])
    balance.value = credits
    usage.value = report
    keys.value = keyList.keys
  } catch (err) {
    toast({
      title: 'Could not load usage',
      description: err instanceof Error ? err.message : undefined,
      variant: 'error',
    })
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>

<template>
  <PageHeader title="Usage" subtitle="Credits consumed in the current billing cycle">
    <template #actions>
      <Button variant="outline" size="sm" :disabled="loading" @click="load">
        <RefreshCw :class="['size-4', loading && 'animate-spin']" />
        Refresh
      </Button>
    </template>
  </PageHeader>

  <div class="mx-auto max-w-5xl space-y-6 p-8">
    <div v-if="loading" class="flex justify-center py-16"><Spinner class="size-6" /></div>

    <template v-else>
      <!-- Headline numbers -->
      <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatTile label="Credits used" :value="formatNumber(balance?.used ?? 0)" />
        <StatTile label="Credits remaining" :value="formatNumber(balance?.remaining ?? 0)" />
        <StatTile label="Requests" :value="formatNumber(totalRequests)" />
        <StatTile label="Refused" :value="formatNumber(totalRejected)" />
      </div>

      <!-- Allowance -->
      <Card v-if="balance">
        <CardContent class="py-5">
          <div class="mb-2 flex items-baseline justify-between">
            <span class="text-sm font-medium">{{ balance.plan.name }} allowance</span>
            <span class="text-sm text-muted-foreground">
              {{ formatNumber(balance.used) }} / {{ formatNumber(balance.monthlyCredits) }}
            </span>
          </div>
          <div class="h-2 overflow-hidden rounded-full bg-muted">
            <div
              class="h-full rounded-full transition-all"
              :class="usedPercent >= 90 ? 'bg-destructive' : usedPercent >= 70 ? 'bg-[var(--warning)]' : 'bg-primary'"
              :style="{ width: `${usedPercent}%` }"
            />
          </div>
          <div class="mt-2 flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
            <span>Resets {{ formatCycleDate(balance.cycleResetsAt) }} (UTC)</span>
            <span v-if="balance.purchased > 0">
              + {{ formatNumber(balance.purchased) }} purchased credits (never expire)
            </span>
            <Badge v-if="balance.overage > 0" variant="warning">
              {{ formatNumber(balance.overage) }} credits of overage
            </Badge>
          </div>
        </CardContent>
      </Card>

      <!-- Daily credits -->
      <Card>
        <CardHeader><CardTitle>Credits per day</CardTitle></CardHeader>
        <CardContent>
          <p v-if="!byDay.length" class="py-8 text-center text-sm text-muted-foreground">
            No usage yet this cycle.
          </p>
          <div v-else class="flex h-40 items-end gap-1">
            <div
              v-for="bar in byDay"
              :key="bar.day"
              class="group relative flex flex-1 flex-col items-center justify-end"
              :title="`${bar.day}: ${bar.credits} credits`"
            >
              <div
                class="w-full rounded-t bg-primary/80 transition-colors group-hover:bg-primary"
                :style="{ height: `${Math.max(2, (bar.credits / peakDay) * 100)}%` }"
              />
              <span class="mt-1.5 text-[10px] text-muted-foreground">{{ shortDay(bar.day) }}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      <div class="grid gap-4 lg:grid-cols-2">
        <!-- By endpoint group -->
        <Card>
          <CardHeader><CardTitle>By endpoint</CardTitle></CardHeader>
          <CardContent>
            <p v-if="!byEndpoint.length" class="py-6 text-center text-sm text-muted-foreground">Nothing yet.</p>
            <table v-else class="w-full text-sm">
              <thead class="text-xs text-muted-foreground">
                <tr class="border-b border-border">
                  <th class="pb-2 text-left font-medium">Group</th>
                  <th class="pb-2 text-right font-medium">Requests</th>
                  <th class="pb-2 text-right font-medium">Credits</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in byEndpoint" :key="row.endpoint" class="border-b border-border/50 last:border-0">
                  <td class="py-2">
                    {{ row.endpoint }}
                    <Badge v-if="row.rejected > 0" variant="warning" class="ml-1.5">
                      {{ row.rejected }} refused
                    </Badge>
                  </td>
                  <td class="py-2 text-right tabular-nums">{{ formatNumber(row.requests) }}</td>
                  <td class="py-2 text-right tabular-nums">{{ formatNumber(row.credits) }}</td>
                </tr>
              </tbody>
            </table>
          </CardContent>
        </Card>

        <!-- By key -->
        <Card>
          <CardHeader><CardTitle>By key</CardTitle></CardHeader>
          <CardContent>
            <p v-if="!usage?.byKey.length" class="py-6 text-center text-sm text-muted-foreground">Nothing yet.</p>
            <table v-else class="w-full text-sm">
              <thead class="text-xs text-muted-foreground">
                <tr class="border-b border-border">
                  <th class="pb-2 text-left font-medium">Key</th>
                  <th class="pb-2 text-right font-medium">Requests</th>
                  <th class="pb-2 text-right font-medium">Credits</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="row in usage.byKey"
                  :key="row.apiKeyId"
                  class="border-b border-border/50 last:border-0"
                >
                  <td class="py-2 truncate">{{ keyLabel(row.apiKeyId) }}</td>
                  <td class="py-2 text-right tabular-nums">{{ formatNumber(row.requests) }}</td>
                  <td class="py-2 text-right tabular-nums">{{ formatNumber(row.credits) }}</td>
                </tr>
              </tbody>
            </table>
          </CardContent>
        </Card>
      </div>
    </template>
  </div>
</template>
