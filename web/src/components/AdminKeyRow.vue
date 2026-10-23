<script setup lang="ts">
/**
 * One of a customer's keys, editable by an operator: name, scopes, origins,
 * the rate-limit exemption, and revocation. Every change is logged to the
 * account's moderation history server-side.
 */
import { computed, ref } from 'vue'
import { KeyRound, Pencil, ShieldOff } from 'lucide-vue-next'
import Badge from '@/components/ui/Badge.vue'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Label from '@/components/ui/Label.vue'
import Switch from '@/components/ui/Switch.vue'
import Textarea from '@/components/ui/Textarea.vue'
import { revokeAdminKey, updateAdminKey, type AdminKeyPatch } from '@/lib/api'
import { toast } from '@/lib/toast'
import type { ApiKeySummary } from '@/lib/types'

const props = defineProps<{
  apiKey: ApiKeySummary
  userId: string
  /** Every grantable scope, `*` and `admin` included. */
  scopes: string[]
  /** Only an administrator's own keys may hold `admin`; the server enforces it too. */
  ownerIsAdmin: boolean
}>()
const emit = defineEmits<{ changed: [key?: ApiKeySummary] }>()

const editing = ref(false)
const busy = ref(false)
const confirmingRevoke = ref(false)

const name = ref('')
const picked = ref<Set<string>>(new Set())
const origins = ref('')

const groupScopes = computed(() => props.scopes.filter((s) => s !== '*' && s !== 'admin'))
const revoked = computed(() => Boolean(props.apiKey.revokedAt))

function startEditing() {
  name.value = props.apiKey.name
  picked.value = new Set(props.apiKey.scopes.filter((s) => s !== '*'))
  origins.value = props.apiKey.allowedOrigins.join('\n')
  editing.value = true
  confirmingRevoke.value = false
}

function toggle(scope: string) {
  const next = new Set(picked.value)
  if (next.has(scope)) next.delete(scope)
  else next.add(scope)
  picked.value = next
}

async function save(patch: AdminKeyPatch, done: string) {
  busy.value = true
  try {
    const { key } = await updateAdminKey(props.userId, props.apiKey.id, patch)
    toast({ title: done, variant: 'success' })
    emit('changed', key)
    return true
  } catch (err) {
    toast({ title: 'Could not change the key', description: err instanceof Error ? err.message : undefined, variant: 'error' })
    return false
  } finally {
    busy.value = false
  }
}

async function saveEdits() {
  const groups = [...picked.value].filter((s) => s !== 'admin')
  const scopes = groups.length ? groups : ['*']
  if (picked.value.has('admin')) scopes.push('admin')
  const ok = await save(
    {
      name: name.value.trim() || props.apiKey.name,
      scopes,
      allowedOrigins: origins.value
        .split(/[\n,]/)
        .map((o) => o.trim())
        .filter(Boolean),
    },
    'Key updated',
  )
  if (ok) editing.value = false
}

function setExempt(exempt: boolean) {
  void save({ rateLimitExempt: exempt }, exempt ? 'Rate limits disabled for this key' : 'Rate limits restored for this key')
}

async function revoke() {
  busy.value = true
  try {
    await revokeAdminKey(props.userId, props.apiKey.id)
    toast({ title: 'Key revoked', variant: 'success' })
    emit('changed')
  } catch (err) {
    toast({ title: 'Could not revoke the key', description: err instanceof Error ? err.message : undefined, variant: 'error' })
  } finally {
    busy.value = false
    confirmingRevoke.value = false
  }
}

function formatDateTime(value: string | null) {
  return value ? new Date(value).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' }) : 'never'
}
</script>

<template>
  <li class="rounded-lg border border-border text-sm" :class="revoked && 'opacity-60'">
    <div class="flex items-start gap-3 px-3 py-2.5">
      <KeyRound class="mt-0.5 size-4 shrink-0 text-muted-foreground" />
      <div class="min-w-0 flex-1">
        <div class="flex flex-wrap items-center gap-2">
          <span class="truncate font-medium">{{ apiKey.name }}</span>
          <code class="text-xs text-muted-foreground">{{ apiKey.prefix }}…{{ apiKey.last4 }}</code>
          <Badge v-if="revoked" variant="muted">revoked</Badge>
          <Badge v-if="apiKey.rateLimitExempt && !revoked" variant="warning">
            <ShieldOff class="size-3" />
            no rate limits
          </Badge>
        </div>
        <div class="mt-1.5 flex flex-wrap gap-1">
          <Badge v-for="scope in apiKey.scopes" :key="scope" :variant="scope === 'admin' ? 'info' : 'secondary'">
            {{ scope === '*' ? 'all endpoints' : scope }}
          </Badge>
        </div>
        <p class="mt-1.5 text-xs text-muted-foreground">
          <template v-if="apiKey.allowedOrigins.length">Origins {{ apiKey.allowedOrigins.join(', ') }} · </template>
          Last used {{ formatDateTime(apiKey.lastUsedAt) }}
          <template v-if="revoked"> · revoked {{ formatDateTime(apiKey.revokedAt) }}</template>
        </p>
      </div>

      <div v-if="!revoked" class="flex shrink-0 items-center gap-3">
        <label class="flex items-center gap-2 text-xs text-muted-foreground">
          Rate limits
          <Switch :model-value="!apiKey.rateLimitExempt" :disabled="busy" @update:model-value="setExempt(!$event)" />
        </label>
        <Button v-if="!editing" variant="ghost" size="sm" title="Edit" @click="startEditing">
          <Pencil class="size-4" />
        </Button>
      </div>
    </div>

    <div v-if="editing" class="flex flex-col gap-4 border-t border-border px-3 py-3">
      <div class="flex flex-col gap-1.5">
        <Label>Name</Label>
        <Input v-model="name" />
      </div>

      <div class="flex flex-col gap-1.5">
        <Label>Scopes</Label>
        <p class="text-xs text-muted-foreground">None picked means every endpoint group.</p>
        <div class="flex flex-wrap gap-1.5">
          <button
            v-for="scope in [...groupScopes, ...(ownerIsAdmin ? ['admin'] : [])]"
            :key="scope"
            type="button"
            :class="[
              'rounded-md border px-2.5 py-1 text-xs transition-colors',
              picked.has(scope)
                ? 'border-primary bg-primary text-primary-foreground'
                : 'border-border text-muted-foreground hover:border-foreground/30 hover:text-foreground',
            ]"
            @click="toggle(scope)"
          >
            {{ scope }}
          </button>
        </div>
      </div>

      <div class="flex flex-col gap-1.5">
        <Label>Allowed origins</Label>
        <Textarea v-model="origins" :rows="2" placeholder="One per line. Empty means unrestricted." />
      </div>

      <div class="flex items-center justify-between gap-2">
        <div>
          <Button v-if="!confirmingRevoke" variant="ghost" size="sm" class="text-destructive" :disabled="busy" @click="confirmingRevoke = true">
            Revoke key
          </Button>
          <div v-else class="flex items-center gap-2">
            <span class="text-xs text-muted-foreground">Stops working immediately.</span>
            <Button variant="destructive" size="sm" :disabled="busy" @click="revoke">Revoke</Button>
            <Button variant="ghost" size="sm" @click="confirmingRevoke = false">Keep</Button>
          </div>
        </div>
        <div class="flex gap-2">
          <Button variant="outline" size="sm" :disabled="busy" @click="editing = false">Cancel</Button>
          <Button size="sm" :disabled="busy" @click="saveEdits">Save</Button>
        </div>
      </div>
    </div>
  </li>
</template>
