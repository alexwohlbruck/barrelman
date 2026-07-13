<script setup lang="ts">
import { ref, watch, computed } from 'vue'
import { Play, AlertTriangle, Flame, Terminal, Info } from 'lucide-vue-next'
import Dialog from '@/components/ui/Dialog.vue'
import Button from '@/components/ui/Button.vue'
import Switch from '@/components/ui/Switch.vue'
import Spinner from '@/components/ui/Spinner.vue'
import ParamField from './ParamField.vue'
import DangerBadge from '@/components/DangerBadge.vue'
import { runScript, ApiError } from '@/lib/api'
import { toast } from '@/lib/toast'
import { refreshJobs } from '@/lib/store'
import type { ScriptDef, Job } from '@/lib/types'

const props = defineProps<{ script: ScriptDef | null; open: boolean }>()
const emit = defineEmits<{ 'update:open': [value: boolean]; started: [job: Job] }>()

const params = ref<Record<string, unknown>>({})
const confirmed = ref(false)
const submitting = ref(false)

watch(
  () => [props.open, props.script?.id],
  () => {
    if (!props.open || !props.script) return
    const init: Record<string, unknown> = {}
    for (const p of props.script.params || []) {
      init[p.name] = p.default ?? (p.type === 'boolean' ? false : '')
    }
    params.value = init
    confirmed.value = false
  },
  { immediate: true },
)

const missingRequired = computed(() => {
  if (!props.script?.params) return false
  return props.script.params.some((p) => p.required && !String(params.value[p.name] ?? '').trim())
})

const canRun = computed(() => {
  if (missingRequired.value) return false
  if (props.script?.danger === 'destructive' && !confirmed.value) return false
  return true
})

// Live preview of the resolved invocation (mirrors the server's builder).
const preview = computed(() => {
  const s = props.script
  if (!s) return ''
  if (s.exec.kind === 'internal') return `internal: ${s.exec.handler}`
  const args = [...s.exec.args]
  const positional: string[] = []
  const envParts: string[] = []
  for (const p of s.params || []) {
    let val = params.value[p.name]
    const empty = val === undefined || val === null || val === ''
    if (empty && p.type !== 'boolean') continue
    if (p.type === 'boolean') val = Boolean(val)
    const shown = p.secret && val ? '••••••' : String(val)
    if (p.apply === 'env') {
      if (p.type === 'boolean') { if (val) envParts.push(`${p.envVar || p.name}=1`) }
      else envParts.push(`${p.envVar || p.name}=${shown}`)
    } else if (p.apply === 'flag') {
      const flag = p.flag || `--${p.name}`
      if (p.type === 'boolean') { if (val) args.push(flag) }
      else args.push(flag, shown)
    } else if (p.apply === 'positional' && typeof val === 'string' && val.trim()) {
      positional.push(...val.trim().split(/\s+/))
    }
  }
  const cmd = [...args, ...positional].join(' ')
  return `${envParts.join(' ')}${envParts.length ? ' ' : ''}${s.exec.command} ${cmd}`.trim()
})

async function run() {
  if (!props.script || !canRun.value) return
  submitting.value = true
  try {
    const { job } = await runScript(props.script.id, params.value)
    toast({ title: 'Job started', description: props.script.name, variant: 'success' })
    emit('started', job)
    emit('update:open', false)
    refreshJobs()
  } catch (err) {
    if (err instanceof ApiError && err.status === 409) {
      toast({ title: 'Already running', description: err.message, variant: 'warning' })
    } else {
      toast({ title: 'Failed to start', description: err instanceof Error ? err.message : 'Unknown error', variant: 'error' })
    }
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <Dialog :open="open" @update:open="emit('update:open', $event)" class="max-w-xl">
    <template v-if="script">
      <div class="flex flex-col gap-1.5 pr-6">
        <div class="flex items-center gap-2">
          <h2 class="text-lg font-semibold leading-none tracking-tight">{{ script.name }}</h2>
          <DangerBadge :danger="script.danger" />
        </div>
        <p class="text-sm text-muted-foreground">{{ script.description }}</p>
      </div>

      <!-- Params -->
      <div v-if="script.params?.length" class="flex flex-col gap-4">
        <ParamField
          v-for="p in script.params"
          :key="p.name"
          :param="p"
          :model-value="params[p.name]"
          @update:model-value="params[p.name] = $event"
        />
      </div>

      <!-- Notes -->
      <div v-if="script.notes" class="flex gap-2 rounded-lg border border-border bg-muted/40 p-3 text-xs text-muted-foreground">
        <Info class="mt-0.5 size-3.5 shrink-0" />
        <span>{{ script.notes }}</span>
      </div>

      <!-- Command preview -->
      <div class="rounded-lg border border-border bg-background/60 p-3">
        <div class="mb-1.5 flex items-center gap-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
          <Terminal class="size-3.5" /> Will run
        </div>
        <code class="block whitespace-pre-wrap break-all font-mono text-xs text-foreground">{{ preview }}</code>
      </div>

      <!-- Destructive confirmation -->
      <label
        v-if="script.danger === 'destructive'"
        class="flex items-start gap-3 rounded-lg border border-destructive/40 bg-destructive/10 p-3"
      >
        <Switch :model-value="confirmed" @update:model-value="confirmed = $event" class="mt-0.5" />
        <div class="flex items-start gap-2 text-xs">
          <Flame class="mt-0.5 size-3.5 shrink-0 text-destructive" />
          <span class="text-destructive">This is a destructive operation and may drop or rebuild data. I understand and want to proceed.</span>
        </div>
      </label>
      <div
        v-else-if="script.confirm"
        class="flex gap-2 rounded-lg border border-[var(--warning)]/40 bg-[var(--warning)]/10 p-3 text-xs text-[var(--warning)]"
      >
        <AlertTriangle class="mt-0.5 size-3.5 shrink-0" />
        <span>Double-check parameters before running — this task modifies data or services.</span>
      </div>

      <!-- Actions -->
      <div class="flex justify-end gap-2">
        <Button variant="ghost" @click="emit('update:open', false)" :disabled="submitting">Cancel</Button>
        <Button
          :variant="script.danger === 'destructive' ? 'destructive' : 'default'"
          :disabled="!canRun || submitting"
          @click="run"
        >
          <Spinner v-if="submitting" class="size-4" />
          <Play v-else class="size-4" />
          Run script
        </Button>
      </div>
    </template>
  </Dialog>
</template>
