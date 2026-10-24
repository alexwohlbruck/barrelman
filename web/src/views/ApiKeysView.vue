<script setup lang="ts">
/**
 * API key management.
 *
 * A created key's plaintext exists only in the response that created it, so the
 * UI shows it once in a dialog that makes copying the obvious action, and says
 * plainly that it will not be shown again.
 */
import { computed, onMounted, ref } from 'vue'
import { Check, Copy, KeyRound, Plus } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import ApiKeyRow from '@/components/ApiKeyRow.vue'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import CardContent from '@/components/ui/CardContent.vue'
import Dialog from '@/components/ui/Dialog.vue'
import Input from '@/components/ui/Input.vue'
import Label from '@/components/ui/Label.vue'
import Spinner from '@/components/ui/Spinner.vue'
import Textarea from '@/components/ui/Textarea.vue'
import { createApiKey, getApiKeys, getPlans } from '@/lib/api'
import { isAdmin } from '@/lib/auth'
import { toast } from '@/lib/toast'
import type { ApiKeySummary, CreatedApiKey, Scope } from '@/lib/types'

function message(err: unknown) {
  return err instanceof Error ? err.message : undefined
}

const keys = ref<ApiKeySummary[]>([])
const scopes = ref<Scope[]>([])
const creditCosts = ref<Record<string, number>>({})
const loading = ref(true)

const showCreate = ref(false)
const creating = ref(false)
const newName = ref('')
const newScopes = ref<Set<string>>(new Set())
const newOrigins = ref('')

/**
 * One origin per line is the only sane input here: origins contain commas
 * nowhere but a person typing a list will use them anyway, so splitting on both
 * and discarding blanks avoids a class of silent mistake.
 */
function parseOrigins(text: string): string[] {
  return text
    .split(/[\n,]/)
    .map((line) => line.trim())
    .filter(Boolean)
}

const created = ref<CreatedApiKey | null>(null)
const copied = ref(false)

/**
 * The server lists `admin` among the scopes, but only an administrator may
 * grant it (and is refused otherwise), so nobody else is offered it.
 */
const groupScopes = computed(() => scopes.value.filter((s) => s !== '*' && (s !== 'admin' || isAdmin.value)))
/** No explicit selection means full access, which matches the server default. */
const scopeSummary = computed(() => (newScopes.value.size === 0 ? 'Full access' : `${newScopes.value.size} selected`))

async function load() {
  loading.value = true
  try {
    const [keyList, plans] = await Promise.all([getApiKeys(), getPlans()])
    keys.value = keyList.keys
    scopes.value = plans.scopes
    creditCosts.value = plans.creditCosts
  } catch (err) {
    toast({ title: 'Could not load API keys', description: message(err), variant: 'error' })
  } finally {
    loading.value = false
  }
}

onMounted(load)

function toggleScope(scope: string) {
  const next = new Set(newScopes.value)
  if (next.has(scope)) next.delete(scope)
  else next.add(scope)
  newScopes.value = next
}

function openCreate() {
  newName.value = ''
  newScopes.value = new Set()
  newOrigins.value = ''
  showCreate.value = true
}

async function submitCreate() {
  if (!newName.value.trim() || creating.value) return
  creating.value = true
  try {
    const origins = parseOrigins(newOrigins.value)
    const result = await createApiKey({
      name: newName.value.trim(),
      scopes: newScopes.value.size ? [...newScopes.value] : undefined,
      allowedOrigins: origins.length ? origins : undefined,
    })
    showCreate.value = false
    created.value = result
    copied.value = false
    await load()
  } catch (err) {
    toast({ title: 'Could not create the key', description: message(err), variant: 'error' })
  } finally {
    creating.value = false
  }
}

async function copyKey() {
  if (!created.value) return
  try {
    await navigator.clipboard.writeText(created.value.key)
    copied.value = true
  } catch {
    toast({ title: 'Could not copy', description: 'Select the key and copy it manually.', variant: 'error' })
  }
}

</script>

<template>
  <PageHeader title="API keys" subtitle="Keys authenticate your requests and attribute usage to your account">
    <template #actions>
      <Button size="sm" @click="openCreate">
        <Plus class="size-4" />
        Create key
      </Button>
    </template>
  </PageHeader>

  <div class="mx-auto max-w-4xl space-y-4 p-8">
    <div v-if="loading" class="flex justify-center py-16"><Spinner class="size-6" /></div>

    <Card v-else-if="!keys.length">
      <CardContent class="flex flex-col items-center gap-3 py-14 text-center">
        <div class="flex size-12 items-center justify-center rounded-xl bg-muted text-muted-foreground">
          <KeyRound class="size-6" />
        </div>
        <div>
          <p class="font-medium">No API keys yet</p>
          <p class="mt-1 text-sm text-muted-foreground">Create one to start calling the API.</p>
        </div>
        <Button @click="openCreate">
          <Plus class="size-4" />
          Create your first key
        </Button>
      </CardContent>
    </Card>

    <ul v-else class="flex flex-col gap-3">
      <ApiKeyRow
        v-for="key in keys"
        :key="key.id"
        :api-key="key"
        mode="self"
        :scopes="scopes"
        :owner-is-admin="isAdmin"
        :viewer-is-admin="isAdmin"
        :credit-costs="creditCosts"
        @changed="load"
      />
    </ul>

    <!-- Create -->
    <Dialog v-model:open="showCreate" title="Create an API key">
      <div class="flex flex-col gap-4">
        <div class="flex flex-col gap-1.5">
          <Label for="key-name">Name</Label>
          <Input id="key-name" v-model="newName" placeholder="Production web app" />
          <p class="text-xs text-muted-foreground">So you can tell your keys apart later.</p>
        </div>

        <div class="flex flex-col gap-2">
          <div class="flex items-center justify-between">
            <Label>Scopes</Label>
            <span class="text-xs text-muted-foreground">{{ scopeSummary }}</span>
          </div>
          <p class="text-xs text-muted-foreground">
            Leave empty for full access. Narrowing a key limits the damage if it leaks — a tiles-only
            key embedded in a web map cannot run up a routing bill.
          </p>
          <div class="flex flex-wrap gap-1.5">
            <button
              v-for="scope in groupScopes"
              :key="scope"
              type="button"
              :class="[
                'rounded-md border px-2.5 py-1 text-xs transition-colors',
                newScopes.has(scope)
                  ? 'border-primary bg-primary text-primary-foreground'
                  : 'border-border text-muted-foreground hover:border-foreground/30 hover:text-foreground',
              ]"
              @click="toggleScope(scope)"
            >
              {{ scope }}
              <span v-if="creditCosts[scope] !== undefined" class="opacity-60">· {{ creditCosts[scope] }}cr</span>
            </button>
          </div>
        </div>

        <div class="flex flex-col gap-1.5">
          <Label for="key-origins">Allowed origins</Label>
          <Textarea
            id="key-origins"
            v-model="newOrigins"
            :rows="3"
            placeholder="https://example.com&#10;https://*.netlify.app"
            class="font-mono text-xs"
          />
          <p class="text-xs text-muted-foreground">
            One per line. Leave empty to allow any origin. Restricting a key means the browser must send a
            matching <code>Origin</code> or <code>Referer</code> — use it for a key that ships in a web page,
            not one your server calls with.
          </p>
        </div>
      </div>

      <div class="mt-2 flex justify-end gap-2">
        <Button variant="outline" @click="showCreate = false">Cancel</Button>
        <Button :disabled="creating || !newName.trim()" @click="submitCreate">
          <Spinner v-if="creating" class="size-4" />
          <template v-else>Create key</template>
        </Button>
      </div>
    </Dialog>

    <!-- Show once -->
    <Dialog :open="created !== null" title="Copy your API key" @update:open="created = null">
      <div class="flex flex-col gap-3">
        <p class="text-sm text-muted-foreground">
          This is the only time this key will be shown. Store it somewhere safe — if you lose it, roll a
          new one.
        </p>
        <div class="flex items-center gap-2 rounded-lg border border-border bg-muted/40 p-3">
          <code class="min-w-0 flex-1 break-all font-mono text-xs">{{ created?.key }}</code>
          <Button variant="outline" size="sm" @click="copyKey">
            <Check v-if="copied" class="size-4 text-[var(--success)]" />
            <Copy v-else class="size-4" />
          </Button>
        </div>
      </div>
      <div class="mt-2 flex justify-end">
        <Button @click="created = null">Done</Button>
      </div>
    </Dialog>
  </div>
</template>
