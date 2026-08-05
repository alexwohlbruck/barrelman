<script setup lang="ts">
/**
 * Account settings: profile, sign-in methods and active sessions.
 */
import { computed, onMounted, ref } from 'vue'
import { Fingerprint, Link2, Link2Off, LogOut, Monitor, Plus, ShieldAlert, Trash2 } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import Badge from '@/components/ui/Badge.vue'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import CardContent from '@/components/ui/CardContent.vue'
import CardHeader from '@/components/ui/CardHeader.vue'
import CardTitle from '@/components/ui/CardTitle.vue'
import Input from '@/components/ui/Input.vue'
import Label from '@/components/ui/Label.vue'
import Spinner from '@/components/ui/Spinner.vue'
import {
  acceptTerms,
  deletePasskey,
  getAccount,
  getLinkedProviders,
  getPasskeys,
  getSessions,
  revokeSession,
  signOutOthers,
  unlinkProvider,
  updateAccount,
} from '@/lib/api'
import { authConfig, passkeysSupported, refreshUser, registerPasskey, signInWithOAuth, user } from '@/lib/auth'
import { toast } from '@/lib/toast'
import type { PasskeySummary, SessionSummary, SuspensionInfo, TermsState } from '@/lib/types'

const passkeys = ref<PasskeySummary[]>([])
const sessions = ref<SessionSummary[]>([])
const linked = ref<{ provider: string; createdAt: string }[]>([])
const terms = ref<TermsState | null>(null)
const suspension = ref<SuspensionInfo | null>(null)
const loading = ref(true)
const busy = ref('')

const name = ref('')

const linkedIds = computed(() => new Set(linked.value.map((l) => l.provider)))
const availableProviders = computed(() => authConfig.value?.methods.oauth ?? [])

function fail(err: unknown, title: string) {
  toast({ title, description: err instanceof Error ? err.message : undefined, variant: 'error' })
}

async function load() {
  loading.value = true
  try {
    const [account, passkeyList, sessionList, providerList] = await Promise.all([
      getAccount(),
      getPasskeys().catch(() => []),
      getSessions().catch(() => []),
      getLinkedProviders().catch(() => []),
    ])
    terms.value = account.terms
    suspension.value = account.suspension
    passkeys.value = passkeyList
    sessions.value = sessionList
    linked.value = providerList
    name.value = user.value?.name ?? ''
  } catch (err) {
    fail(err, 'Could not load account settings')
  } finally {
    loading.value = false
  }
}

onMounted(load)

async function agreeToTerms() {
  if (!terms.value) return
  busy.value = 'terms'
  try {
    const result = await acceptTerms(terms.value.version)
    terms.value = result.terms
    toast({ title: 'Terms accepted', variant: 'success' })
  } catch (err) {
    fail(err, 'Could not record your acceptance')
  } finally {
    busy.value = ''
  }
}

async function saveProfile() {
  busy.value = 'profile'
  try {
    await updateAccount(name.value.trim())
    await refreshUser()
    toast({ title: 'Profile saved', variant: 'success' })
  } catch (err) {
    fail(err, 'Could not save your profile')
  } finally {
    busy.value = ''
  }
}

async function addPasskey() {
  busy.value = 'passkey'
  try {
    await registerPasskey()
    await load()
    toast({ title: 'Passkey added', variant: 'success' })
  } catch (err) {
    // A cancelled prompt is not a failure worth reporting.
    if (err instanceof Error && /abort|cancel|NotAllowed/i.test(err.message)) return
    fail(err, 'Could not add a passkey')
  } finally {
    busy.value = ''
  }
}

async function removePasskey(passkey: PasskeySummary) {
  if (!confirm(`Remove "${passkey.name}"?`)) return
  try {
    await deletePasskey(passkey.id)
    await load()
  } catch (err) {
    fail(err, 'Could not remove the passkey')
  }
}

async function unlink(provider: string) {
  if (!confirm(`Unlink ${provider}? You can still sign in with an email code.`)) return
  try {
    await unlinkProvider(provider)
    await load()
  } catch (err) {
    fail(err, 'Could not unlink the provider')
  }
}

async function revoke(session: SessionSummary) {
  try {
    await revokeSession(session.id)
    await load()
  } catch (err) {
    fail(err, 'Could not revoke the session')
  }
}

async function endOthers() {
  if (!confirm('Sign out of every other device?')) return
  busy.value = 'sessions'
  try {
    await signOutOthers()
    await load()
    toast({ title: 'Signed out everywhere else', variant: 'success' })
  } catch (err) {
    fail(err, 'Could not sign out other devices')
  } finally {
    busy.value = ''
  }
}

function describeDevice(userAgent: string | null) {
  if (!userAgent) return 'Unknown device'
  if (/curl|wget|python|node/i.test(userAgent)) return userAgent.split('/')[0]
  const browser = /Edg\//.test(userAgent)
    ? 'Edge'
    : /Firefox\//.test(userAgent)
      ? 'Firefox'
      : /Chrome\//.test(userAgent)
        ? 'Chrome'
        : /Safari\//.test(userAgent)
          ? 'Safari'
          : 'Browser'
  const os = /iPhone|iPad/.test(userAgent)
    ? 'iOS'
    : /Android/.test(userAgent)
      ? 'Android'
      : /Mac OS X/.test(userAgent)
        ? 'macOS'
        : /Windows/.test(userAgent)
          ? 'Windows'
          : /Linux/.test(userAgent)
            ? 'Linux'
            : ''
  return os ? `${browser} on ${os}` : browser
}

function formatDate(value: string | null) {
  return value ? new Date(value).toLocaleDateString(undefined, { dateStyle: 'medium' }) : '—'
}
</script>

<template>
  <PageHeader title="Account" subtitle="Your profile, sign-in methods and devices" />

  <div class="mx-auto max-w-3xl space-y-6 p-8">
    <div v-if="loading" class="flex justify-center py-16"><Spinner class="size-6" /></div>

    <template v-else>
      <!-- Suspension notice: the most important thing on the page when it applies -->
      <Card v-if="suspension?.suspended" class="border-destructive">
        <CardContent class="flex flex-col gap-2 py-5">
          <div class="flex items-center gap-2">
            <ShieldAlert class="size-4 text-destructive" />
            <span class="font-medium text-destructive">This account is suspended</span>
          </div>
          <p class="text-sm">{{ suspension.reason }}</p>
          <p v-if="suspension.until" class="text-xs text-muted-foreground">
            The suspension lifts automatically on
            {{ new Date(suspension.until).toLocaleString() }}.
          </p>
          <p v-else-if="suspension.appealable" class="text-xs text-muted-foreground">
            If you believe this is a mistake, reply to any email from us to appeal.
          </p>
        </CardContent>
      </Card>

      <!-- Outstanding terms: blocks key creation, so say so plainly -->
      <Card v-if="terms?.required && terms.outstanding" class="border-[var(--warning)]">
        <CardContent class="flex flex-wrap items-center justify-between gap-3 py-5">
          <div>
            <p class="font-medium">
              {{ terms.acceptedVersion ? 'The terms of service have changed' : 'Accept the terms of service' }}
            </p>
            <p class="mt-1 text-sm text-muted-foreground">
              You need to accept version {{ terms.version }} before creating new API keys.
              <a v-if="terms.url" :href="terms.url" target="_blank" rel="noopener" class="underline">
                Read the terms
              </a>
            </p>
          </div>
          <Button :disabled="busy === 'terms'" @click="agreeToTerms">
            <Spinner v-if="busy === 'terms'" class="size-4" />
            <template v-else>Accept</template>
          </Button>
        </CardContent>
      </Card>

      <!-- Profile -->
      <Card>
        <CardHeader><CardTitle>Profile</CardTitle></CardHeader>
        <CardContent class="flex flex-col gap-4">
          <div class="flex flex-col gap-1.5">
            <Label>Email</Label>
            <p class="text-sm text-muted-foreground">{{ user?.email }}</p>
          </div>
          <div class="flex flex-col gap-1.5">
            <Label for="account-name">Name</Label>
            <div class="flex gap-2">
              <Input id="account-name" v-model="name" placeholder="Your name" class="max-w-xs" />
              <Button variant="outline" :disabled="busy === 'profile'" @click="saveProfile">
                <Spinner v-if="busy === 'profile'" class="size-4" />
                <template v-else>Save</template>
              </Button>
            </div>
          </div>
          <div class="flex items-center gap-2 text-sm">
            <span class="text-muted-foreground">Plan</span>
            <Badge variant="secondary">{{ user?.plan }}</Badge>
            <Badge v-if="user?.role === 'admin'" variant="info">administrator</Badge>
          </div>
        </CardContent>
      </Card>

      <!-- Passkeys -->
      <Card v-if="passkeysSupported">
        <CardHeader class="flex-row items-center justify-between">
          <CardTitle>Passkeys</CardTitle>
          <Button variant="outline" size="sm" :disabled="busy === 'passkey'" @click="addPasskey">
            <Spinner v-if="busy === 'passkey'" class="size-4" />
            <Plus v-else class="size-4" />
            Add passkey
          </Button>
        </CardHeader>
        <CardContent>
          <p v-if="!passkeys.length" class="py-4 text-sm text-muted-foreground">
            No passkeys yet. Adding one lets you sign in with a fingerprint or face instead of an
            emailed code.
          </p>
          <div v-else class="flex flex-col divide-y divide-border">
            <div v-for="passkey in passkeys" :key="passkey.id" class="flex items-center gap-3 py-3 first:pt-0">
              <Fingerprint class="size-4 shrink-0 text-muted-foreground" />
              <div class="min-w-0 flex-1">
                <div class="flex items-center gap-2">
                  <span class="truncate text-sm font-medium">{{ passkey.name }}</span>
                  <Badge v-if="passkey.backedUp" variant="muted">synced</Badge>
                </div>
                <p class="text-xs text-muted-foreground">
                  Added {{ formatDate(passkey.createdAt) }} · Last used {{ formatDate(passkey.lastUsedAt) }}
                </p>
              </div>
              <Button variant="ghost" size="sm" @click="removePasskey(passkey)">
                <Trash2 class="size-4 text-destructive" />
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <!-- Connected accounts -->
      <Card v-if="availableProviders.length">
        <CardHeader><CardTitle>Connected accounts</CardTitle></CardHeader>
        <CardContent class="flex flex-col divide-y divide-border">
          <div
            v-for="provider in availableProviders"
            :key="provider.id"
            class="flex items-center gap-3 py-3 first:pt-0"
          >
            <Link2 class="size-4 shrink-0 text-muted-foreground" />
            <span class="flex-1 text-sm font-medium">{{ provider.label }}</span>
            <Button
              v-if="linkedIds.has(provider.id)"
              variant="ghost"
              size="sm"
              @click="unlink(provider.id)"
            >
              <Link2Off class="size-4" />
              Unlink
            </Button>
            <Button
              v-else
              variant="outline"
              size="sm"
              @click="signInWithOAuth(provider.id, { link: true, next: '/console/account' })"
            >
              Connect
            </Button>
          </div>
        </CardContent>
      </Card>

      <!-- Sessions -->
      <Card>
        <CardHeader class="flex-row items-center justify-between">
          <CardTitle>Active sessions</CardTitle>
          <Button
            v-if="sessions.length > 1"
            variant="outline"
            size="sm"
            :disabled="busy === 'sessions'"
            @click="endOthers"
          >
            <LogOut class="size-4" />
            Sign out others
          </Button>
        </CardHeader>
        <CardContent class="flex flex-col divide-y divide-border">
          <div v-for="session in sessions" :key="session.id" class="flex items-center gap-3 py-3 first:pt-0">
            <Monitor class="size-4 shrink-0 text-muted-foreground" />
            <div class="min-w-0 flex-1">
              <div class="flex items-center gap-2">
                <span class="truncate text-sm font-medium">{{ describeDevice(session.userAgent) }}</span>
                <Badge v-if="session.current" variant="success">this device</Badge>
              </div>
              <p class="text-xs text-muted-foreground">Signed in {{ formatDate(session.createdAt) }}</p>
            </div>
            <Button v-if="!session.current" variant="ghost" size="sm" @click="revoke(session)">
              <Trash2 class="size-4 text-destructive" />
            </Button>
          </div>
        </CardContent>
      </Card>
    </template>
  </div>
</template>
