<script setup lang="ts">
/**
 * One API key with its controls: rename, scopes, origins, revoke, and — for
 * administrators only — the rate-limit exemption.
 *
 * Shared by the API keys page (the signed-in account's own keys) and the
 * account pane (an operator looking at a customer's), so both offer the same
 * options. `mode` picks the routes: `self` saves through `/account/keys`,
 * which only ever reaches the caller's own keys; `admin` saves through
 * `/admin/users/:id/keys`, which logs every change to that account.
 *
 * The exemption is never settable through `/account/keys`. Wherever the switch
 * appears it saves through the admin route, so the server's admin guard — not
 * this component hiding a control — is what decides who may flip it.
 */
import { computed, ref } from 'vue'
import { Globe, KeyRound, Pencil, ShieldOff } from 'lucide-vue-next'
import Badge from '@/components/ui/Badge.vue'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Label from '@/components/ui/Label.vue'
import Switch from '@/components/ui/Switch.vue'
import Textarea from '@/components/ui/Textarea.vue'
import { revokeAdminKey, revokeApiKey, updateAdminKey, updateApiKey, type AdminKeyPatch } from '@/lib/api'
import { toast } from '@/lib/toast'
import type { ApiKeySummary } from '@/lib/types'

const props = defineProps<{
  apiKey: ApiKeySummary
  mode: 'self' | 'admin'
  /** Every scope the server knows, `*` and `admin` included; filtered here. */
  scopes: string[]
  /** Only an administrator's own keys may hold `admin`. The server enforces it too. */
  ownerIsAdmin: boolean
  /** Whether the person looking is an administrator, which is what shows the rate-limit switch. */
  viewerIsAdmin: boolean
  /** Shown on the scope chips where known. */
  creditCosts?: Record<string, number>
}>()
const emit = defineEmits<{ changed: [] }>()

const editing = ref(false)
const busy = ref(false)
const confirmingRevoke = ref(false)

const name = ref('')
const picked = ref<Set<string>>(new Set())
const origins = ref('')

const revoked = computed(() => Boolean(props.apiKey.revokedAt))
/** `admin` is offered only on an administrator's own keys; `*` is "none picked". */
const pickable = computed(() =>
  props.scopes.filter((s) => s !== '*' && (s !== 'admin' || props.ownerIsAdmin)),
)

function message(err: unknown) {
  return err instanceof Error ? err.message : undefined
}

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

/** One origin per line, though a person typing a list will use commas anyway. */
function parseOrigins(text: string): string[] {
  return text
    .split(/[\n,]/)
    .map((o) => o.trim())
    .filter(Boolean)
}

function update(patch: AdminKeyPatch) {
  return props.mode === 'admin'
    ? updateAdminKey(props.apiKey.userId, props.apiKey.id, patch)
    : updateApiKey(props.apiKey.id, patch)
}

async function run(action: () => Promise<unknown>, done: string, failed: string): Promise<boolean> {
  busy.value = true
  try {
    await action()
    toast({ title: done, variant: 'success' })
    emit('changed')
    return true
  } catch (err) {
    toast({ title: failed, description: message(err), variant: 'error' })
    return false
  } finally {
    busy.value = false
  }
}

async function saveEdits() {
  const groups = [...picked.value].filter((s) => s !== 'admin')
  const scopes = groups.length ? groups : ['*']
  if (picked.value.has('admin')) scopes.push('admin')
  const ok = await run(
    () =>
      update({
        name: name.value.trim() || props.apiKey.name,
        scopes,
        allowedOrigins: parseOrigins(origins.value),
      }),
    'Key updated',
    'Could not update the key',
  )
  if (ok) editing.value = false
}

function setExempt(exempt: boolean) {
  void run(
    () => updateAdminKey(props.apiKey.userId, props.apiKey.id, { rateLimitExempt: exempt }),
    exempt ? 'Rate limits disabled for this key' : 'Rate limits restored for this key',
    'Could not change the key',
  )
}

async function revoke() {
  await run(
    () => (props.mode === 'admin' ? revokeAdminKey(props.apiKey.userId, props.apiKey.id) : revokeApiKey(props.apiKey.id)),
    `Revoked ${props.apiKey.name}`,
    'Could not revoke the key',
  )
  confirmingRevoke.value = false
}

function formatDate(value: string | null) {
  return value ? new Date(value).toLocaleDateString(undefined, { dateStyle: 'medium' }) : 'never'
}
</script>

<template>
  <li class="list-none rounded-xl border border-border bg-card text-sm" :class="revoked && 'opacity-60'">
    <div class="flex items-start gap-3 px-4 py-3">
      <KeyRound class="mt-0.5 size-4 shrink-0 text-muted-foreground" />
      <div class="min-w-0 flex-1">
        <div class="flex flex-wrap items-center gap-2">
          <span class="truncate font-medium">{{ apiKey.name }}</span>
          <Badge v-if="revoked" variant="destructive">revoked</Badge>
          <Badge v-if="apiKey.rateLimitExempt && !revoked" variant="warning">
            <ShieldOff class="size-3" />
            no rate limits
          </Badge>
        </div>
        <div class="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          <code class="font-mono">{{ apiKey.prefix }}…{{ apiKey.last4 }}</code>
          <span>Created {{ formatDate(apiKey.createdAt) }}</span>
          <span>Last used {{ formatDate(apiKey.lastUsedAt) }}</span>
          <span v-if="revoked">Revoked {{ formatDate(apiKey.revokedAt) }}</span>
        </div>
        <div class="mt-2 flex flex-wrap gap-1">
          <Badge v-for="scope in apiKey.scopes" :key="scope" :variant="scope === 'admin' ? 'info' : 'secondary'">
            {{ scope === '*' ? 'all endpoints' : scope }}
          </Badge>
        </div>
        <div v-if="apiKey.allowedOrigins.length" class="mt-2 flex flex-wrap items-center gap-1">
          <Globe class="size-3 text-muted-foreground" />
          <Badge v-for="origin in apiKey.allowedOrigins" :key="origin" variant="outline">{{ origin }}</Badge>
        </div>
      </div>

      <div v-if="!revoked" class="flex shrink-0 items-center gap-3">
        <label v-if="viewerIsAdmin" class="flex items-center gap-2 text-xs text-muted-foreground">
          Rate limits
          <Switch :model-value="!apiKey.rateLimitExempt" :disabled="busy" @update:model-value="setExempt(!$event)" />
        </label>
        <Button v-if="!editing" variant="ghost" size="sm" title="Edit" @click="startEditing">
          <Pencil class="size-4" />
        </Button>
      </div>
    </div>

    <div v-if="editing" class="flex flex-col gap-4 border-t border-border px-4 py-3">
      <div class="flex flex-col gap-1.5">
        <Label>Name</Label>
        <Input v-model="name" />
      </div>

      <div class="flex flex-col gap-1.5">
        <Label>Scopes</Label>
        <p class="text-xs text-muted-foreground">
          None picked means every endpoint group. Narrowing a key limits the damage if it leaks.
        </p>
        <div class="flex flex-wrap gap-1.5">
          <button
            v-for="scope in pickable"
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
            <span v-if="creditCosts?.[scope] !== undefined" class="opacity-60">· {{ creditCosts[scope] }}cr</span>
          </button>
        </div>
      </div>

      <div class="flex flex-col gap-1.5">
        <Label>Allowed origins</Label>
        <Textarea
          v-model="origins"
          :rows="2"
          placeholder="https://example.com&#10;https://*.netlify.app"
          class="font-mono text-xs"
        />
        <p class="text-xs text-muted-foreground">
          One per line; empty allows any origin. A restricted key only works from a browser on a matching site —
          requests from curl or a server send no <code>Origin</code> and are refused.
        </p>
      </div>

      <div class="flex flex-wrap items-center justify-between gap-2">
        <div>
          <Button
            v-if="!confirmingRevoke"
            variant="ghost"
            size="sm"
            class="text-destructive"
            :disabled="busy"
            @click="confirmingRevoke = true"
          >
            Revoke key
          </Button>
          <div v-else class="flex items-center gap-2">
            <span class="text-xs text-muted-foreground">Anything using it fails immediately.</span>
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
