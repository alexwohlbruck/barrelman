<script setup lang="ts">
/**
 * One account, as an operator sees it: what it has spent, what keys it holds
 * and what has been decided about it.
 *
 * The same numbers a customer gets on `/console/usage`, plus a lifetime total —
 * an account that was abusive last month reads as spotless through a cycle
 * window, and the cycle is the only window the customer-facing view has.
 */
import { computed, ref, watch } from 'vue'
import { Ban, KeyRound, ShieldCheck } from 'lucide-vue-next'
import Badge from '@/components/ui/Badge.vue'
import Dialog from '@/components/ui/Dialog.vue'
import Spinner from '@/components/ui/Spinner.vue'
import StatTile from '@/components/StatTile.vue'
import { getAdminUser } from '@/lib/api'
import { toast } from '@/lib/toast'
import { creditsPerDay, formatCycleDate, keyLabel, shortDay, totalsByEndpoint } from '@/lib/usage'
import { formatNumber } from '@/lib/utils'
import type { AdminUser, AdminUserDetail } from '@/lib/types'

const props = defineProps<{ account: AdminUser | null }>()
const emit = defineEmits<{ close: [] }>()

const detail = ref<AdminUserDetail | null>(null)
const loading = ref(false)

const byDay = computed(() => creditsPerDay(detail.value?.usage.daily ?? []))
const byEndpoint = computed(() => totalsByEndpoint(detail.value?.usage.daily ?? []))
const peakDay = computed(() => Math.max(1, ...byDay.value.map((d) => d.credits)))
const cycleRequests = computed(() => byEndpoint.value.reduce((sum, row) => sum + row.requests, 0))
const cycleRejected = computed(() => byEndpoint.value.reduce((sum, row) => sum + row.rejected, 0))

const keyNames = computed(() => new Map((detail.value?.keys ?? []).map((key) => [key.id, key.name])))
const liveKeys = computed(() => (detail.value?.keys ?? []).filter((key) => !key.revokedAt))

const usedPercent = computed(() => {
  const balance = detail.value?.balance
  if (!balance || balance.monthlyCredits === 0) return 0
  return Math.min(100, Math.round((balance.used / balance.monthlyCredits) * 100))
})

// Refetch whenever a different row is opened; the dialog is reused, so the
// previous account's numbers would otherwise flash up under the new heading.
watch(
  () => props.account?.id,
  async (id) => {
    detail.value = null
    if (!id) return
    loading.value = true
    try {
      detail.value = await getAdminUser(id)
    } catch (err) {
      toast({
        title: 'Could not load the account',
        description: err instanceof Error ? err.message : undefined,
        variant: 'error',
      })
      emit('close')
    } finally {
      loading.value = false
    }
  },
  { immediate: true },
)

function formatDateTime(value: string | null) {
  return value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : '—'
}

type BadgeVariant = 'destructive' | 'warning' | 'success' | 'secondary'

function actionVariant(action: string): BadgeVariant {
  if (action === 'suspend' || action === 'flag') return 'destructive'
  if (action === 'warn') return 'warning'
  if (action === 'unsuspend') return 'success'
  return 'secondary'
}
</script>

<template>
  <Dialog
    :open="account !== null"
    :title="account?.email"
    :description="account?.name || undefined"
    class="max-w-3xl max-h-[85vh] overflow-y-auto"
    @update:open="emit('close')"
  >
    <div v-if="loading" class="flex justify-center py-16"><Spinner class="size-6" /></div>

    <div v-else-if="detail" class="flex flex-col gap-5">
      <div class="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{{ detail.user.plan.name }}</Badge>
        <Badge v-if="detail.user.role === 'admin'" variant="info">admin</Badge>
        <Badge v-if="!detail.user.plan.metered" variant="warning">unmetered</Badge>
        <Badge v-if="detail.suspension.suspended" variant="destructive">
          {{ detail.suspension.kind || 'suspended' }}
        </Badge>
        <span class="text-xs text-muted-foreground">Joined {{ formatDateTime(detail.user.createdAt) }}</span>
      </div>

      <p v-if="detail.suspension.suspended" class="rounded-md bg-destructive/10 px-3 py-2 text-xs text-destructive">
        {{ detail.suspension.reason }}
        <span v-if="detail.suspension.until"> · lifts {{ formatDateTime(detail.suspension.until) }}</span>
      </p>

      <!-- This cycle -->
      <section class="flex flex-col gap-3">
        <h3 class="text-sm font-medium">This billing cycle</h3>
        <div class="grid gap-3 sm:grid-cols-4">
          <StatTile label="Credits used" :value="formatNumber(detail.balance?.used ?? 0)" />
          <StatTile label="Remaining" :value="formatNumber(detail.balance?.remaining ?? 0)" />
          <StatTile label="Requests" :value="formatNumber(cycleRequests)" />
          <StatTile label="Refused" :value="formatNumber(cycleRejected)" />
        </div>

        <div v-if="detail.balance">
          <div class="h-2 overflow-hidden rounded-full bg-muted">
            <div
              class="h-full rounded-full transition-all"
              :class="usedPercent >= 90 ? 'bg-destructive' : usedPercent >= 70 ? 'bg-[var(--warning)]' : 'bg-primary'"
              :style="{ width: `${usedPercent}%` }"
            />
          </div>
          <div class="mt-1.5 flex flex-wrap justify-between gap-2 text-xs text-muted-foreground">
            <span>
              {{ formatNumber(detail.balance.used) }} / {{ formatNumber(detail.balance.monthlyCredits) }} ·
              resets {{ formatCycleDate(detail.balance.cycleResetsAt) }} (UTC)
            </span>
            <span v-if="detail.balance.overage > 0" class="text-[var(--warning)]">
              {{ formatNumber(detail.balance.overage) }} credits of overage
            </span>
          </div>
        </div>

        <div v-if="byDay.length" class="flex h-28 items-end gap-1">
          <div
            v-for="bar in byDay"
            :key="bar.day"
            class="group flex flex-1 flex-col items-center justify-end"
            :title="`${bar.day}: ${bar.credits} credits`"
          >
            <div
              class="w-full rounded-t bg-primary/80 transition-colors group-hover:bg-primary"
              :style="{ height: `${Math.max(2, (bar.credits / peakDay) * 100)}%` }"
            />
            <span class="mt-1.5 text-[10px] text-muted-foreground">{{ shortDay(bar.day) }}</span>
          </div>
        </div>
        <p v-else class="text-sm text-muted-foreground">No usage yet this cycle.</p>
      </section>

      <!-- Lifetime -->
      <section v-if="detail.lifetime" class="rounded-lg border border-border px-4 py-3 text-sm">
        <div class="flex flex-wrap items-baseline gap-x-5 gap-y-1">
          <span class="font-medium">All time</span>
          <span class="tabular-nums">{{ formatNumber(detail.lifetime.requests) }} requests</span>
          <span class="tabular-nums">{{ formatNumber(detail.lifetime.credits) }} credits</span>
          <span v-if="detail.lifetime.rejected > 0" class="tabular-nums text-muted-foreground">
            {{ formatNumber(detail.lifetime.rejected) }} refused
          </span>
          <span v-if="detail.lifetime.firstDay" class="text-xs text-muted-foreground">
            {{ detail.lifetime.firstDay }} → {{ detail.lifetime.lastDay }}
          </span>
        </div>
      </section>

      <!-- Breakdowns -->
      <section class="grid gap-5 sm:grid-cols-2">
        <div>
          <h3 class="mb-2 text-sm font-medium">By endpoint</h3>
          <p v-if="!byEndpoint.length" class="text-sm text-muted-foreground">Nothing yet.</p>
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
                <td class="py-1.5">
                  {{ row.endpoint }}
                  <Badge v-if="row.rejected > 0" variant="warning" class="ml-1.5">
                    {{ row.rejected }} refused
                  </Badge>
                </td>
                <td class="py-1.5 text-right tabular-nums">{{ formatNumber(row.requests) }}</td>
                <td class="py-1.5 text-right tabular-nums">{{ formatNumber(row.credits) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div>
          <h3 class="mb-2 text-sm font-medium">By key</h3>
          <p v-if="!detail.usage.byKey.length" class="text-sm text-muted-foreground">Nothing yet.</p>
          <table v-else class="w-full text-sm">
            <thead class="text-xs text-muted-foreground">
              <tr class="border-b border-border">
                <th class="pb-2 text-left font-medium">Key</th>
                <th class="pb-2 text-right font-medium">Requests</th>
                <th class="pb-2 text-right font-medium">Credits</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in detail.usage.byKey" :key="row.apiKeyId" class="border-b border-border/50 last:border-0">
                <td class="truncate py-1.5">{{ keyLabel(row.apiKeyId, keyNames) }}</td>
                <td class="py-1.5 text-right tabular-nums">{{ formatNumber(row.requests) }}</td>
                <td class="py-1.5 text-right tabular-nums">{{ formatNumber(row.credits) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <!-- Keys -->
      <section>
        <h3 class="mb-2 text-sm font-medium">
          API keys
          <span class="text-muted-foreground">({{ liveKeys.length }} active of {{ detail.keys.length }})</span>
        </h3>
        <p v-if="!detail.keys.length" class="text-sm text-muted-foreground">No keys.</p>
        <ul v-else class="flex flex-col gap-1.5">
          <li
            v-for="key in detail.keys"
            :key="key.id"
            class="flex items-center gap-3 rounded-lg border border-border px-3 py-2 text-sm"
          >
            <KeyRound class="size-4 shrink-0 text-muted-foreground" />
            <div class="min-w-0 flex-1">
              <div class="flex flex-wrap items-center gap-2">
                <span class="truncate font-medium">{{ key.name }}</span>
                <code class="text-xs text-muted-foreground">{{ key.prefix }}…{{ key.last4 }}</code>
                <Badge v-if="key.revokedAt" variant="muted">revoked</Badge>
                <Badge v-for="scope in key.scopes" :key="scope" variant="secondary">{{ scope }}</Badge>
              </div>
              <p class="mt-0.5 text-xs text-muted-foreground">
                Last used {{ formatDateTime(key.lastUsedAt) }}
              </p>
            </div>
          </li>
        </ul>
      </section>

      <!-- Audit trail -->
      <section>
        <h3 class="mb-2 text-sm font-medium">Moderation history</h3>
        <p v-if="!detail.history.length" class="text-sm text-muted-foreground">
          Nothing has been recorded against this account.
        </p>
        <ul v-else class="flex flex-col gap-1.5">
          <li
            v-for="entry in detail.history"
            :key="entry.id"
            class="flex items-start gap-3 rounded-lg border border-border px-3 py-2 text-sm"
          >
            <Ban v-if="entry.action === 'suspend'" class="mt-0.5 size-4 shrink-0 text-destructive" />
            <ShieldCheck v-else class="mt-0.5 size-4 shrink-0 text-muted-foreground" />
            <div class="min-w-0 flex-1">
              <div class="flex flex-wrap items-center gap-2">
                <Badge :variant="actionVariant(entry.action)">{{ entry.action }}</Badge>
                <span v-if="entry.kind" class="text-xs text-muted-foreground">{{ entry.kind }}</span>
              </div>
              <p v-if="entry.reason" class="mt-1">{{ entry.reason }}</p>
              <p class="mt-0.5 text-xs text-muted-foreground">
                {{ formatDateTime(entry.createdAt) }} · by {{ entry.actorId }}
              </p>
            </div>
          </li>
        </ul>
      </section>
    </div>
  </Dialog>
</template>
