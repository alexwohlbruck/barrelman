<script setup lang="ts">
import { ref, computed } from 'vue'
import { Send, Zap } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import Card from '@/components/ui/Card.vue'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Textarea from '@/components/ui/Textarea.vue'
import Select from '@/components/ui/Select.vue'
import Label from '@/components/ui/Label.vue'
import Badge from '@/components/ui/Badge.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { testEndpoint } from '@/lib/api'
import type { TestResult } from '@/lib/types'

const method = ref('GET')
const path = ref('/health')
const query = ref('')
const body = ref('')
const auth = ref<'api' | 'admin' | 'none'>('api')
const loading = ref(false)
const result = ref<TestResult | null>(null)

const methodOptions = ['GET', 'POST', 'PUT', 'DELETE'].map((m) => ({ label: m, value: m }))
const authOptions = [
  { label: 'API key', value: 'api' },
  { label: 'Admin key', value: 'admin' },
  { label: 'No auth', value: 'none' },
]

interface Preset {
  label: string
  method: string
  path: string
  query?: string
  body?: string
  auth?: 'api' | 'admin' | 'none'
}
const presets: Preset[] = [
  { label: 'Health', method: 'GET', path: '/health', auth: 'none' },
  { label: 'Auth health', method: 'GET', path: '/health/auth', auth: 'api' },
  { label: 'Search', method: 'POST', path: '/search', body: '{\n  "query": "starbucks",\n  "limit": 5\n}', auth: 'api' },
  { label: 'Reverse geocode', method: 'GET', path: '/geocode', query: 'lat=40.7484&lng=-73.9857', auth: 'api' },
  { label: 'Contains', method: 'GET', path: '/contains', query: 'lat=40.7484&lng=-73.9857', auth: 'api' },
  { label: 'Migration status', method: 'GET', path: '/admin/migration/status', auth: 'api' },
]

function applyPreset(p: Preset) {
  method.value = p.method
  path.value = p.path
  query.value = p.query || ''
  auth.value = p.auth || 'api'
  body.value = p.body || ''
}

const prettyBody = computed(() => {
  if (!result.value) return ''
  const b = result.value.body
  if (b === undefined) return result.value.error || ''
  return typeof b === 'string' ? b : JSON.stringify(b, null, 2)
})

const statusVariant = computed(() => {
  if (!result.value) return 'muted'
  if (result.value.status === 0) return 'destructive'
  if (result.value.status < 300) return 'success'
  if (result.value.status < 400) return 'info'
  if (result.value.status < 500) return 'warning'
  return 'destructive'
})

async function send() {
  loading.value = true
  result.value = null
  try {
    result.value = await testEndpoint({
      method: method.value,
      path: path.value,
      query: query.value || undefined,
      body: method.value !== 'GET' && body.value ? body.value : undefined,
      auth: auth.value,
    })
  } catch (err) {
    result.value = {
      ok: false,
      status: 0,
      statusText: 'Client error',
      durationMs: 0,
      error: err instanceof Error ? err.message : 'Request failed',
    }
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <PageHeader title="API Tester" subtitle="Send requests to the running Barrelman API" />

  <div class="p-8">
    <!-- Presets -->
    <div class="mb-5 flex flex-wrap items-center gap-2">
      <span class="flex items-center gap-1 text-xs font-medium text-muted-foreground"><Zap class="size-3.5" /> Presets:</span>
      <button
        v-for="p in presets"
        :key="p.label"
        class="rounded-full border border-border px-3 py-1 text-xs text-muted-foreground transition-colors hover:border-muted-foreground/40 hover:text-foreground"
        @click="applyPreset(p)"
      >
        {{ p.label }}
      </button>
    </div>

    <div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <!-- Request -->
      <Card class="p-5">
        <div class="flex flex-col gap-4">
          <div class="flex gap-2">
            <div class="w-28 shrink-0">
              <Select v-model="method" :options="methodOptions" />
            </div>
            <Input v-model="path" placeholder="/search" class="flex-1 font-mono" />
          </div>

          <div class="flex flex-col gap-1.5">
            <Label>Query string</Label>
            <Input v-model="query" placeholder="q=starbucks&limit=5" class="font-mono" />
          </div>

          <div class="flex flex-col gap-1.5">
            <Label>Auth</Label>
            <Select v-model="auth" :options="authOptions" />
            <p class="text-xs text-muted-foreground">Injected server-side from the configured keys — you don't paste secrets here.</p>
          </div>

          <div v-if="method !== 'GET'" class="flex flex-col gap-1.5">
            <Label>Body (JSON)</Label>
            <Textarea v-model="body" placeholder='{ "key": "value" }' :rows="6" />
          </div>

          <Button @click="send" :disabled="loading || !path" class="w-full">
            <Spinner v-if="loading" class="size-4" />
            <Send v-else class="size-4" />
            Send request
          </Button>
        </div>
      </Card>

      <!-- Response -->
      <Card class="flex flex-col p-5">
        <div class="mb-3 flex items-center gap-3">
          <span class="text-sm font-medium">Response</span>
          <template v-if="result">
            <Badge :variant="statusVariant as any">{{ result.status || 'ERR' }} {{ result.statusText }}</Badge>
            <span class="text-xs text-muted-foreground tabular-nums">{{ result.durationMs }} ms</span>
            <span v-if="result.bytes !== undefined" class="text-xs text-muted-foreground tabular-nums">{{ result.bytes }} B</span>
          </template>
        </div>

        <div
          v-if="result"
          class="max-h-[calc(100vh-20rem)] min-h-64 flex-1 overflow-auto rounded-lg border border-border bg-[#0b0b0c] p-3"
        >
          <pre class="whitespace-pre-wrap break-all font-mono text-xs text-foreground/90">{{ prettyBody }}</pre>
        </div>
        <div v-else class="flex min-h-64 flex-1 items-center justify-center rounded-lg border border-dashed border-border text-sm text-muted-foreground">
          Send a request to see the response
        </div>
      </Card>
    </div>
  </div>
</template>
