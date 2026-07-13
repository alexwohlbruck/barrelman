<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Compass, KeyRound, ArrowRight } from 'lucide-vue-next'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { verifyKey } from '@/lib/api'
import { setKey, authRequired } from '@/lib/auth'

const route = useRoute()
const router = useRouter()
const key = ref('')
const error = ref('')
const loading = ref(false)

onMounted(() => {
  // If the server doesn't require auth, skip straight in.
  if (!authRequired.value) router.replace('/dashboard')
})

async function submit() {
  if (!key.value.trim()) return
  loading.value = true
  error.value = ''
  try {
    const ok = await verifyKey(key.value.trim())
    if (!ok) {
      error.value = 'Invalid admin key. Check BARRELMAN_ADMIN_KEY (or BARRELMAN_API_KEY).'
      return
    }
    setKey(key.value.trim())
    const redirect = (route.query.redirect as string) || '/dashboard'
    router.replace(redirect)
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Verification failed'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="flex min-h-screen items-center justify-center app-grid-bg px-4">
    <div class="w-full max-w-sm">
      <div class="mb-8 flex flex-col items-center text-center">
        <div class="mb-4 flex size-14 items-center justify-center rounded-2xl bg-primary text-primary-foreground shadow-lg">
          <Compass class="size-7" />
        </div>
        <h1 class="text-2xl font-semibold tracking-tight">Barrelman Console</h1>
        <p class="mt-1 text-sm text-muted-foreground">Admin access required</p>
      </div>

      <form class="flex flex-col gap-3" @submit.prevent="submit">
        <div class="relative">
          <KeyRound class="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            v-model="key"
            type="password"
            placeholder="Admin key"
            class="pl-9 h-11"
            :disabled="loading"
          />
        </div>
        <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
        <Button type="submit" class="h-11 w-full" :disabled="loading || !key.trim()" as="button">
          <Spinner v-if="loading" class="size-4" />
          <template v-else>
            Enter console
            <ArrowRight class="size-4" />
          </template>
        </Button>
      </form>

      <p class="mt-6 text-center text-xs text-muted-foreground">
        The key gates every operator action — imports, migrations, graph rebuilds.
      </p>
    </div>
  </div>
</template>
