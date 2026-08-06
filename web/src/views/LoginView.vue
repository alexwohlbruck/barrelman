<script setup lang="ts">
/**
 * Sign-in. Three ways in: a one-time email code, a passkey, or an OAuth
 * provider. Which appear depends on what the server reports at /auth/config,
 * so an instance with no OAuth configured simply shows fewer buttons.
 *
 * The legacy shared admin key stays reachable behind a disclosure, so an
 * operator who has not created an account is never locked out of their own
 * console.
 */
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, ArrowRight, Compass, Fingerprint, KeyRound, Mail } from 'lucide-vue-next'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { verifyKey } from '@/lib/api'
import {
  authConfig,
  authRequired,
  isSignedIn,
  passkeysSupported,
  requestCode,
  setAdminKey,
  signInWithOAuth,
  signInWithPasskey,
  verifyCode,
} from '@/lib/auth'

const route = useRoute()
const router = useRouter()

const step = ref<'identify' | 'code'>('identify')

const email = ref('')
const code = ref('')
const error = ref('')
const notice = ref('')
const loading = ref(false)

const showAdminKey = ref(false)
const adminKeyInput = ref('')

const providers = computed(() => authConfig.value?.methods.oauth ?? [])
const canUsePasskeys = computed(() => passkeysSupported && authConfig.value?.methods.passkey !== false)
// Default to the router root, which sends everyone to their own keys —
// including admins, who reach the operator views from the sidebar.
const redirect = computed(() => (route.query.redirect as string) || '/')

onMounted(() => {
  // An OAuth failure comes back as a query parameter on the console URL.
  const oauthError = route.query.error as string | undefined
  if (oauthError) error.value = oauthError

  if (isSignedIn.value) router.replace(redirect.value)
  else if (!authRequired.value) router.replace('/')
})

function fail(err: unknown, fallback: string) {
  error.value = err instanceof Error ? err.message : fallback
}

async function submitEmail() {
  if (!email.value.trim() || loading.value) return
  loading.value = true
  error.value = ''
  try {
    await requestCode(email.value.trim())
    step.value = 'code'
    notice.value = `We sent a sign-in code to ${email.value.trim()}.`
  } catch (err) {
    fail(err, 'Could not send a sign-in code')
  } finally {
    loading.value = false
  }
}

async function submitCode() {
  if (!code.value.trim() || loading.value) return
  loading.value = true
  error.value = ''
  try {
    await verifyCode(email.value.trim(), code.value.trim())
    router.replace(redirect.value)
  } catch (err) {
    fail(err, 'That code is not valid')
  } finally {
    loading.value = false
  }
}

async function usePasskey() {
  loading.value = true
  error.value = ''
  try {
    await signInWithPasskey()
    router.replace(redirect.value)
  } catch (err) {
    // A cancelled WebAuthn prompt throws as well; that is not worth an error.
    if (err instanceof Error && /abort|cancel|NotAllowed/i.test(err.message)) error.value = ''
    else fail(err, 'Passkey sign-in failed')
  } finally {
    loading.value = false
  }
}

async function submitAdminKey() {
  if (!adminKeyInput.value.trim() || loading.value) return
  loading.value = true
  error.value = ''
  try {
    const ok = await verifyKey(adminKeyInput.value.trim())
    if (!ok) {
      error.value = 'That admin key was not accepted.'
      return
    }
    setAdminKey(adminKeyInput.value.trim())
    router.replace(redirect.value)
  } catch (err) {
    fail(err, 'Verification failed')
  } finally {
    loading.value = false
  }
}

function back() {
  step.value = 'identify'
  code.value = ''
  error.value = ''
  notice.value = ''
}
</script>

<template>
  <div class="flex min-h-screen items-center justify-center app-grid-bg px-4 py-10">
    <div class="w-full max-w-sm">
      <div class="mb-8 flex flex-col items-center text-center">
        <div
          class="mb-4 flex size-14 items-center justify-center rounded-2xl bg-primary text-primary-foreground shadow-lg"
        >
          <Compass class="size-7" />
        </div>
        <h1 class="text-2xl font-semibold tracking-tight">Barrelman</h1>
        <p class="mt-1 text-sm text-muted-foreground">
          {{ step === 'code' ? 'Enter your sign-in code' : 'Sign in to manage your API keys' }}
        </p>
      </div>

      <!-- Step 1: identify -->
      <div v-if="step === 'identify'" class="flex flex-col gap-3">
        <form class="flex flex-col gap-3" @submit.prevent="submitEmail">
          <div class="relative">
            <Mail
              class="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
            />
            <Input
              v-model="email"
              type="email"
              autocomplete="email webauthn"
              placeholder="you@example.com"
              class="h-11 pl-9"
              :disabled="loading"
            />
          </div>
          <Button type="submit" class="h-11 w-full" :disabled="loading || !email.trim()" as="button">
            <Spinner v-if="loading" class="size-4" />
            <template v-else>
              Continue with email
              <ArrowRight class="size-4" />
            </template>
          </Button>
        </form>

        <div v-if="canUsePasskeys || providers.length" class="relative py-2">
          <div class="absolute inset-0 flex items-center">
            <div class="w-full border-t border-border" />
          </div>
          <div class="relative flex justify-center">
            <span class="bg-background px-2 text-xs text-muted-foreground">or</span>
          </div>
        </div>

        <Button
          v-if="canUsePasskeys"
          variant="outline"
          class="h-11 w-full"
          :disabled="loading"
          as="button"
          @click="usePasskey"
        >
          <Fingerprint class="size-4" />
          Sign in with a passkey
        </Button>

        <Button
          v-for="provider in providers"
          :key="provider.id"
          variant="outline"
          class="h-11 w-full"
          :disabled="loading"
          as="button"
          @click="signInWithOAuth(provider.id, { next: `/console${redirect}` })"
        >
          Continue with {{ provider.label }}
        </Button>

        <p v-if="error" class="text-sm text-destructive">{{ error }}</p>

        <p
          v-if="authConfig?.registrationMode === 'open'"
          class="mt-2 text-center text-xs text-muted-foreground"
        >
          No account yet? Entering your email creates one — the free plan needs no card.
        </p>
      </div>

      <!-- Step 2: the code -->
      <form v-else class="flex flex-col gap-3" @submit.prevent="submitCode">
        <p v-if="notice" class="text-sm text-muted-foreground">{{ notice }}</p>
        <Input
          v-model="code"
          inputmode="numeric"
          autocomplete="one-time-code"
          placeholder="00000000"
          class="h-11 text-center font-mono text-lg tracking-[0.3em]"
          :disabled="loading"
        />
        <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
        <Button type="submit" class="h-11 w-full" :disabled="loading || !code.trim()" as="button">
          <Spinner v-if="loading" class="size-4" />
          <template v-else>
            Sign in
            <ArrowRight class="size-4" />
          </template>
        </Button>
        <button
          type="button"
          class="mx-auto flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground"
          @click="back"
        >
          <ArrowLeft class="size-3" />
          Use a different email
        </button>
      </form>

      <!-- Operator escape hatch -->
      <div class="mt-8 border-t border-border pt-4">
        <button
          v-if="!showAdminKey"
          class="mx-auto flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground"
          @click="showAdminKey = true"
        >
          <KeyRound class="size-3" />
          Sign in with an admin key
        </button>
        <form v-else class="flex flex-col gap-2" @submit.prevent="submitAdminKey">
          <Input
            v-model="adminKeyInput"
            type="password"
            placeholder="BARRELMAN_ADMIN_KEY"
            class="h-10"
            :disabled="loading"
          />
          <Button type="submit" variant="outline" class="h-10 w-full" :disabled="loading" as="button">
            Enter console
          </Button>
          <p class="text-center text-xs text-muted-foreground">
            For operators without an account. Grants full administrator access.
          </p>
        </form>
      </div>
    </div>
  </div>
</template>
