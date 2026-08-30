<script setup lang="ts">
/**
 * Administrator view: accounts and the abuse queue.
 *
 * Suspending someone is destructive and hard to undo from their side — it kills
 * their sessions and every key at once — so the dialog makes the operator type
 * a reason, and shows them that the reason is what the user will read.
 *
 * Deleting is worse: it cascades away the keys, the usage, the ledger and the
 * audit trail, and cannot be undone from either side. That dialog asks for the
 * address typed back rather than a reason, since there is nothing left to
 * write a reason on.
 */
import { computed, onMounted, ref } from 'vue'
import {
  AlertTriangle,
  Ban,
  BarChart3,
  CreditCard,
  RefreshCw,
  Search,
  ShieldCheck,
  ShieldOff,
  Trash2,
  User,
} from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import UserDetailDialog from '@/components/UserDetailDialog.vue'
import Badge from '@/components/ui/Badge.vue'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import CardContent from '@/components/ui/CardContent.vue'
import CardHeader from '@/components/ui/CardHeader.vue'
import CardTitle from '@/components/ui/CardTitle.vue'
import Dialog from '@/components/ui/Dialog.vue'
import Input from '@/components/ui/Input.vue'
import Label from '@/components/ui/Label.vue'
import Select from '@/components/ui/Select.vue'
import Spinner from '@/components/ui/Spinner.vue'
import Tabs from '@/components/ui/Tabs.vue'
import Textarea from '@/components/ui/Textarea.vue'
import {
  deleteAdminUser,
  getAbuseSignals,
  getAdminUsers,
  resolveAbuseSignal,
  setUserPlan,
  setUserRole,
  suspendUser,
  unsuspendUser,
} from '@/lib/api'
import { toast } from '@/lib/toast'
import { formatNumber } from '@/lib/utils'
import type { AbuseSignal, AdminUser, Plan, SuspensionKind } from '@/lib/types'

const users = ref<AdminUser[]>([])
const total = ref(0)
const signals = ref<AbuseSignal[]>([])
const openSignals = ref(0)
const loading = ref(true)
const busy = ref('')

const search = ref('')
const statusFilter = ref<'all' | 'suspended' | 'paid'>('all')
const tab = ref<'accounts' | 'abuse'>('accounts')

const target = ref<AdminUser | null>(null)
const reason = ref('')
const kind = ref<SuspensionKind>('tos-violation')
const durationHours = ref('')

/** The row whose usage, keys and audit trail are being read. */
const detailTarget = ref<AdminUser | null>(null)

const deleteTarget = ref<AdminUser | null>(null)
const deleteConfirm = ref('')
/**
 * Deletion is irreversible and cascades — the operator types the address back
 * rather than confirming a dialog they have stopped reading. The server checks
 * this too; it is not only a UI courtesy.
 */
const deleteConfirmed = computed(() => {
  const email = deleteTarget.value?.email.trim().toLowerCase()
  return !!email && deleteConfirm.value.trim().toLowerCase() === email
})

const plans = ref<Plan[]>([])
const planTarget = ref<AdminUser | null>(null)
const nextPlan = ref('')
const planReason = ref('')

const planOptions = computed(() => plans.value.map((p) => ({ label: p.name, value: p.id })))
/** The plan being assigned, for the warning about what it grants. */
const chosenPlan = computed(() => plans.value.find((p) => p.id === nextPlan.value) ?? null)

const kindOptions: { label: string; value: SuspensionKind }[] = [
  { label: 'Terms of service violation', value: 'tos-violation' },
  { label: 'Abuse', value: 'abuse' },
  { label: 'Spam', value: 'spam' },
  { label: 'Billing', value: 'billing' },
  { label: 'Operator request', value: 'operator-request' },
]

const suspendedCount = computed(() => users.value.filter((u) => u.suspension.suspended).length)

function fail(err: unknown, title: string) {
  toast({ title, description: err instanceof Error ? err.message : undefined, variant: 'error' })
}

async function load() {
  loading.value = true
  try {
    const [userPage, abuse] = await Promise.all([
      getAdminUsers({ search: search.value.trim() || undefined, status: statusFilter.value }),
      getAbuseSignals(),
    ])
    users.value = userPage.users
    total.value = userPage.total
    plans.value = userPage.plans
    signals.value = abuse.signals
    openSignals.value = abuse.open
  } catch (err) {
    fail(err, 'Could not load accounts')
  } finally {
    loading.value = false
  }
}

onMounted(load)

function openSuspend(user: AdminUser) {
  target.value = user
  reason.value = ''
  kind.value = 'tos-violation'
  durationHours.value = ''
}

async function confirmSuspend() {
  if (!target.value || !reason.value.trim()) return
  busy.value = target.value.id
  try {
    await suspendUser(target.value.id, {
      reason: reason.value.trim(),
      kind: kind.value,
      hours: durationHours.value ? Number(durationHours.value) : undefined,
    })
    toast({ title: `Suspended ${target.value.email}`, variant: 'success' })
    target.value = null
    await load()
  } catch (err) {
    fail(err, 'Could not suspend the account')
  } finally {
    busy.value = ''
  }
}

async function toggleRole(account: AdminUser) {
  const promoting = account.role !== 'admin'
  const verb = promoting ? 'Make' : 'Remove'
  if (!confirm(`${verb} ${account.email} ${promoting ? 'an administrator?' : ' as an administrator?'}`)) return

  busy.value = account.id
  try {
    const r = await setUserRole(account.id, promoting ? 'admin' : 'user')
    account.role = r.user.role as typeof account.role
    toast({ title: promoting ? 'Now an administrator' : 'Administrator access removed', variant: 'success' })
  } catch (err) {
    // 409 is the last-administrator guard, and it is the interesting case.
    toast({
      title: 'Could not change the role',
      description: err instanceof Error ? err.message : undefined,
      variant: 'error',
    })
  } finally {
    busy.value = ''
  }
}

async function reinstate(user: AdminUser) {
  if (!confirm(`Reinstate ${user.email}?`)) return
  busy.value = user.id
  try {
    await unsuspendUser(user.id)
    toast({ title: `Reinstated ${user.email}`, variant: 'success' })
    await load()
  } catch (err) {
    fail(err, 'Could not reinstate the account')
  } finally {
    busy.value = ''
  }
}

function openPlan(user: AdminUser) {
  planTarget.value = user
  nextPlan.value = user.plan
  planReason.value = ''
}

async function confirmPlan() {
  const user = planTarget.value
  if (!user || nextPlan.value === user.plan) return
  busy.value = user.id
  try {
    await setUserPlan(user.id, nextPlan.value, planReason.value.trim() || undefined)
    toast({ title: `${user.email} moved to ${nextPlan.value}`, variant: 'success' })
    planTarget.value = null
    await load()
  } catch (err) {
    fail(err, 'Could not change the plan')
  } finally {
    busy.value = ''
  }
}

function openDelete(user: AdminUser) {
  deleteTarget.value = user
  deleteConfirm.value = ''
}

async function confirmDelete() {
  const user = deleteTarget.value
  if (!user || !deleteConfirmed.value) return
  busy.value = user.id
  try {
    await deleteAdminUser(user.id, deleteConfirm.value.trim())
    toast({ title: `Deleted ${user.email}`, variant: 'success' })
    deleteTarget.value = null
    await load()
  } catch (err) {
    fail(err, 'Could not delete the account')
  } finally {
    busy.value = ''
  }
}

async function dismiss(signal: AbuseSignal) {
  busy.value = signal.id
  try {
    await resolveAbuseSignal(signal.id)
    await load()
  } catch (err) {
    fail(err, 'Could not dismiss the signal')
  } finally {
    busy.value = ''
  }
}

function severityVariant(severity: string) {
  return severity === 'high' ? 'destructive' : severity === 'medium' ? 'warning' : 'muted'
}

function formatDate(value: string | null) {
  return value ? new Date(value).toLocaleDateString(undefined, { dateStyle: 'medium' }) : '—'
}

function describeDetail(detail: Record<string, unknown> | null) {
  if (!detail) return ''
  return Object.entries(detail)
    .map(([key, value]) => `${key}: ${typeof value === 'number' ? formatNumber(value) : value}`)
    .join(' · ')
}
</script>

<template>
  <PageHeader title="Accounts" subtitle="Moderate developer accounts and review automated abuse signals">
    <template #actions>
      <Button variant="outline" size="sm" :disabled="loading" @click="load">
        <RefreshCw :class="['size-4', loading && 'animate-spin']" />
        Refresh
      </Button>
    </template>
  </PageHeader>

  <div class="mx-auto max-w-5xl space-y-4 p-8">
    <Tabs
      v-model="tab"
      :tabs="[
        { value: 'accounts', label: `Accounts (${total})` },
        { value: 'abuse', label: `Abuse queue (${openSignals})` },
      ]"
    />

    <div v-if="loading" class="flex justify-center py-16"><Spinner class="size-6" /></div>

    <!-- Accounts -->
    <template v-else-if="tab === 'accounts'">
      <div class="flex flex-wrap items-center gap-2">
        <div class="relative min-w-56 flex-1">
          <Search class="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input v-model="search" placeholder="Search by email or name" class="pl-9" @keyup.enter="load" />
        </div>
        <Select
          v-model="statusFilter"
          :options="[
            { label: 'All accounts', value: 'all' },
            { label: 'Suspended', value: 'suspended' },
            { label: 'Paid plans', value: 'paid' },
          ]"
          class="w-44"
          @update:model-value="load"
        />
        <Button variant="outline" @click="load">Apply</Button>
      </div>

      <p v-if="suspendedCount" class="text-sm text-muted-foreground">
        {{ suspendedCount }} of the accounts shown {{ suspendedCount === 1 ? 'is' : 'are' }} suspended.
      </p>

      <Card v-if="!users.length">
        <CardContent class="py-14 text-center text-sm text-muted-foreground">No accounts match.</CardContent>
      </Card>

      <div v-else class="flex flex-col gap-2">
        <Card v-for="account in users" :key="account.id">
          <CardContent class="flex items-center gap-4 py-4">
            <div
              class="flex size-9 shrink-0 items-center justify-center rounded-lg"
              :class="account.suspension.suspended ? 'bg-destructive/15 text-destructive' : 'bg-muted text-muted-foreground'"
            >
              <Ban v-if="account.suspension.suspended" class="size-4" />
              <User v-else class="size-4" />
            </div>

            <div class="min-w-0 flex-1">
              <div class="flex flex-wrap items-center gap-2">
                <span class="truncate font-medium">{{ account.email }}</span>
                <Badge variant="secondary">{{ account.plan }}</Badge>
                <Badge v-if="account.role === 'admin'" variant="info">admin</Badge>
                <Badge v-if="account.suspension.suspended" variant="destructive">
                  {{ account.suspension.kind || 'suspended' }}
                </Badge>
                <Badge v-if="account.terms.required && account.terms.outstanding" variant="warning">
                  terms not accepted
                </Badge>
              </div>
              <p v-if="account.suspension.suspended" class="mt-1 text-xs text-destructive">
                {{ account.suspension.reason }}
                <span v-if="account.suspension.until" class="text-muted-foreground">
                  · lifts {{ formatDate(account.suspension.until) }}
                </span>
              </p>
              <p v-else class="mt-1 text-xs text-muted-foreground">Joined {{ formatDate(account.createdAt) }}</p>
            </div>

            <Button variant="ghost" size="sm" @click="detailTarget = account">
              <BarChart3 class="size-4" />
              Usage
            </Button>

            <Button variant="ghost" size="sm" :disabled="busy === account.id" @click="openPlan(account)">
              <CreditCard class="size-4" />
              Plan
            </Button>

            <Button
              variant="ghost"
              size="sm"
              :disabled="busy === account.id"
              :title="account.role === 'admin' ? 'Remove administrator access' : 'Make administrator'"
              @click="toggleRole(account)"
            >
              <ShieldCheck v-if="account.role !== 'admin'" class="size-4" />
              <ShieldOff v-else class="size-4 text-muted-foreground" />
            </Button>

            <Button
              v-if="account.suspension.suspended"
              variant="outline"
              size="sm"
              :disabled="busy === account.id"
              @click="reinstate(account)"
            >
              <ShieldCheck class="size-4" />
              Reinstate
            </Button>
            <Button
              v-else-if="account.role !== 'admin'"
              variant="ghost"
              size="sm"
              :disabled="busy === account.id"
              title="Suspend"
              @click="openSuspend(account)"
            >
              <Ban class="size-4 text-destructive" />
            </Button>

            <!-- Demote an administrator before deleting them, as with suspend. -->
            <Button
              v-if="account.role !== 'admin'"
              variant="ghost"
              size="sm"
              :disabled="busy === account.id"
              title="Delete permanently"
              @click="openDelete(account)"
            >
              <Trash2 class="size-4 text-destructive" />
            </Button>
          </CardContent>
        </Card>
      </div>
    </template>

    <!-- Abuse queue -->
    <template v-else>
      <Card v-if="!signals.length">
        <CardContent class="flex flex-col items-center gap-2 py-14 text-center">
          <ShieldCheck class="size-8 text-[var(--success)]" />
          <p class="font-medium">Nothing to review</p>
          <p class="text-sm text-muted-foreground">
            Automated detections appear here. A signal is an observation, not a judgement.
          </p>
        </CardContent>
      </Card>

      <div v-else class="flex flex-col gap-2">
        <Card v-for="signal in signals" :key="signal.id">
          <CardContent class="flex items-center gap-4 py-4">
            <AlertTriangle class="size-4 shrink-0 text-[var(--warning)]" />
            <div class="min-w-0 flex-1">
              <div class="flex flex-wrap items-center gap-2">
                <span class="font-medium">{{ signal.kind }}</span>
                <Badge :variant="severityVariant(signal.severity)">{{ signal.severity }}</Badge>
                <span v-if="signal.email" class="truncate text-sm text-muted-foreground">{{ signal.email }}</span>
                <Badge v-if="signal.suspendedAt" variant="destructive">already suspended</Badge>
              </div>
              <p class="mt-1 text-xs text-muted-foreground">
                {{ describeDetail(signal.detail) }} · {{ formatDate(signal.createdAt) }}
              </p>
            </div>
            <Button variant="ghost" size="sm" :disabled="busy === signal.id" @click="dismiss(signal)">
              Dismiss
            </Button>
          </CardContent>
        </Card>
      </div>
    </template>

    <UserDetailDialog :account="detailTarget" @close="detailTarget = null" />

    <!-- Delete -->
    <Dialog :open="deleteTarget !== null" title="Delete account" @update:open="deleteTarget = null">
      <div class="flex flex-col gap-4">
        <p class="text-sm text-muted-foreground">
          This erases <strong>{{ deleteTarget?.email }}</strong> and everything attached to it — API keys,
          usage, the credit ledger and its own moderation history. There is no undo, and no record left of
          the account having existed. Suspending is the reversible option.
        </p>

        <div class="flex flex-col gap-1.5">
          <Label for="delete-confirm">Type the email address to confirm</Label>
          <Input id="delete-confirm" v-model="deleteConfirm" :placeholder="deleteTarget?.email" autocomplete="off" />
        </div>
      </div>

      <div class="mt-2 flex justify-end gap-2">
        <Button variant="outline" @click="deleteTarget = null">Cancel</Button>
        <Button
          variant="destructive"
          :disabled="!deleteConfirmed || busy === deleteTarget?.id"
          @click="confirmDelete"
        >
          <Spinner v-if="busy === deleteTarget?.id" class="size-4" />
          <template v-else>Delete permanently</template>
        </Button>
      </div>
    </Dialog>

    <!-- Plan -->
    <Dialog :open="planTarget !== null" title="Change plan" @update:open="planTarget = null">
      <div class="flex flex-col gap-4">
        <p class="text-sm text-muted-foreground">
          An operator override for <strong>{{ planTarget?.email }}</strong>, independent of what they have
          paid for. A billing sync will not undo it.
        </p>

        <div class="flex flex-col gap-1.5">
          <Label for="plan-id">Plan</Label>
          <Select id="plan-id" v-model="nextPlan" :options="planOptions" />
        </div>

        <!--
          Unmetered access is the one thing on this dialog that cannot be
          undone by the next billing cycle, so it says out loud what it grants.
        -->
        <p
          v-if="chosenPlan && !chosenPlan.metered"
          class="rounded-md bg-warning/10 px-3 py-2 text-xs text-warning-foreground"
        >
          <strong>{{ chosenPlan.name }} is unmetered.</strong>
          Requests are served without spending credits, bounded only by
          {{ formatNumber(chosenPlan.requestsPerMinutePerIp ?? chosenPlan.requestsPerMinute) }} per minute
          per visitor. Use it for demos you run yourself, not for customers.
        </p>

        <div class="flex flex-col gap-1.5">
          <Label for="plan-reason">Reason</Label>
          <Input id="plan-reason" v-model="planReason" placeholder="Why (recorded in the audit log)" />
        </div>
      </div>

      <div class="mt-2 flex justify-end gap-2">
        <Button variant="outline" @click="planTarget = null">Cancel</Button>
        <Button
          :disabled="nextPlan === planTarget?.plan || busy === planTarget?.id"
          @click="confirmPlan"
        >
          <Spinner v-if="busy === planTarget?.id" class="size-4" />
          <template v-else>Change plan</template>
        </Button>
      </div>
    </Dialog>

    <!-- Suspend -->
    <Dialog :open="target !== null" title="Suspend account" @update:open="target = null">
      <div class="flex flex-col gap-4">
        <p class="text-sm text-muted-foreground">
          This ends every session and disables every API key for
          <strong>{{ target?.email }}</strong> immediately.
        </p>

        <div class="flex flex-col gap-1.5">
          <Label for="suspend-kind">Category</Label>
          <Select id="suspend-kind" v-model="kind" :options="kindOptions" />
        </div>

        <div class="flex flex-col gap-1.5">
          <Label for="suspend-reason">Reason</Label>
          <Textarea id="suspend-reason" v-model="reason" :rows="3" placeholder="What did they do?" />
          <p class="text-xs text-muted-foreground">
            Shown to the user verbatim — at sign-in and on every API response. Write it for them to read.
          </p>
        </div>

        <div class="flex flex-col gap-1.5">
          <Label for="suspend-hours">Duration (hours)</Label>
          <Input id="suspend-hours" v-model="durationHours" inputmode="numeric" placeholder="Leave empty for indefinite" />
        </div>
      </div>

      <div class="mt-2 flex justify-end gap-2">
        <Button variant="outline" @click="target = null">Cancel</Button>
        <Button variant="destructive" :disabled="!reason.trim() || busy === target?.id" @click="confirmSuspend">
          <Spinner v-if="busy === target?.id" class="size-4" />
          <template v-else>Suspend</template>
        </Button>
      </div>
    </Dialog>
  </div>
</template>
