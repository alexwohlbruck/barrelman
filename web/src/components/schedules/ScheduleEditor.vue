<script setup lang="ts">
import { ref, watch, computed, nextTick } from 'vue'
import { CalendarClock, Info, Save, Terminal } from 'lucide-vue-next'
import Dialog from '@/components/ui/Dialog.vue'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Label from '@/components/ui/Label.vue'
import Select from '@/components/ui/Select.vue'
import Switch from '@/components/ui/Switch.vue'
import Spinner from '@/components/ui/Spinner.vue'
import ParamField from '@/components/scripts/ParamField.vue'
import DangerBadge from '@/components/DangerBadge.vue'
import { createSchedule, previewCron, updateSchedule, ApiError } from '@/lib/api'
import { toast } from '@/lib/toast'
import type { CronPreview, Schedule, ScriptDef } from '@/lib/types'

const props = defineProps<{
  schedule: Schedule | null
  scripts: ScriptDef[]
  defaultTimezone: string
  open: boolean
}>()
const emit = defineEmits<{ 'update:open': [value: boolean]; saved: [] }>()

const scriptId = ref('')
const cron = ref('0 3 * * *')
const timezone = ref('UTC')
const enabled = ref(true)
const params = ref<Record<string, unknown>>({})
const saving = ref(false)

const preview = ref<CronPreview | null>(null)
const previewError = ref<string | null>(null)

/** Common starting points — an operator can still type any 5-field expression. */
const PRESETS = [
  { label: 'Daily at 03:00', value: '0 3 * * *' },
  { label: 'Daily at 04:00', value: '0 4 * * *' },
  { label: 'Every 6 hours', value: '0 0-23/6 * * *' },
  { label: 'Weekly, Sunday 05:30', value: '30 5 * * 0' },
  { label: 'Monthly, 1st at 02:00', value: '0 2 1 * *' },
]

const selected = computed(() => props.scripts.find((s) => s.id === scriptId.value) ?? null)

const scriptOptions = computed(() =>
  [...props.scripts]
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((s) => ({ label: s.name, value: s.id })),
)

// Timezones the runtime knows, so an operator can't save one the server rejects.
const zoneOptions = computed(() => {
  const supported: string[] =
    typeof Intl.supportedValuesOf === 'function' ? Intl.supportedValuesOf('timeZone') : []
  const browser = Intl.DateTimeFormat().resolvedOptions().timeZone
  const zones = new Set<string>(['UTC', props.defaultTimezone, browser, ...supported].filter(Boolean))
  return [...zones].sort().map((z) => ({ label: z, value: z }))
})

/**
 * Set while the form is being populated from props. Both watchers below run in
 * the same flush, so without this the script watcher fires on the assignment
 * made here and wipes the saved params it just loaded — opening an existing
 * schedule for editing would silently blank its parameters.
 */
let loading = false

watch(
  () => [props.open, props.schedule?.id],
  () => {
    if (!props.open) return
    loading = true
    const s = props.schedule
    // No default script for a new schedule: the manifest's first entry is the
    // destructive full OSM import, which is the last thing to preselect.
    scriptId.value = s?.scriptId ?? ''
    cron.value = s?.cron ?? '0 3 * * *'
    timezone.value = s?.timezone ?? props.defaultTimezone ?? 'UTC'
    enabled.value = s?.enabled ?? true
    params.value = { ...(s?.params ?? {}) }
    void refreshPreview()
    // Cleared once the queue drains rather than inside the script watcher —
    // reopening on the same script never fires that watcher, and the flag would
    // stay set and swallow the operator's next real change.
    void nextTick(() => {
      loading = false
    })
  },
  { immediate: true },
)

// Reset params when the operator picks a different script — the previous
// script's flags mean nothing to the new one and would be passed through.
watch(scriptId, () => {
  if (loading) return
  const defaults: Record<string, unknown> = {}
  for (const p of selected.value?.params ?? []) defaults[p.name] = p.default ?? (p.type === 'boolean' ? false : '')
  params.value = defaults
})

watch([cron, timezone], () => void refreshPreview())

/** Ask the server what this expression means — same parser that will run it. */
async function refreshPreview() {
  previewError.value = null
  try {
    preview.value = await previewCron(cron.value, timezone.value)
  } catch (err) {
    preview.value = null
    previewError.value = err instanceof Error ? err.message : 'Invalid expression'
  }
}

const canSave = computed(() => Boolean(scriptId.value && preview.value && !saving.value))

const fmt = (iso: string) =>
  new Date(iso).toLocaleString(undefined, {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })

async function save() {
  if (!canSave.value) return
  saving.value = true
  // Drop blanks so an untouched optional field doesn't become an empty env var.
  const cleaned = Object.fromEntries(
    Object.entries(params.value).filter(([, v]) => v !== '' && v !== null && v !== undefined),
  )
  const payload = { scriptId: scriptId.value, cron: cron.value, timezone: timezone.value, params: cleaned, enabled: enabled.value }
  try {
    if (props.schedule) await updateSchedule(props.schedule.id, payload)
    else await createSchedule(payload)
    toast({ title: props.schedule ? 'Schedule updated' : 'Schedule created', description: selected.value?.name, variant: 'success' })
    emit('saved')
    emit('update:open', false)
  } catch (err) {
    const msg = err instanceof ApiError || err instanceof Error ? err.message : 'Unknown error'
    toast({ title: 'Save failed', description: msg, variant: 'error' })
  } finally {
    saving.value = false
  }
}
</script>

<template>
  <Dialog :open="open" @update:open="emit('update:open', $event)" class="max-w-xl">
    <div class="flex flex-col gap-1.5 pr-6">
      <h2 class="text-lg font-semibold leading-none tracking-tight">
        {{ schedule ? 'Edit schedule' : 'New schedule' }}
      </h2>
      <p class="text-sm text-muted-foreground">
        Run a script on a recurring schedule. Each run becomes a normal job you can watch and cancel.
      </p>
    </div>

    <div class="flex flex-col gap-4">
      <!-- Script -->
      <div class="flex flex-col gap-1.5">
        <Label>Script</Label>
        <Select v-model="scriptId" :options="scriptOptions" placeholder="Choose a script…" />
        <p v-if="selected" class="flex items-start gap-2 text-xs text-muted-foreground">
          <DangerBadge :danger="selected.danger" />
          <span>{{ selected.description }}</span>
        </p>
      </div>

      <!-- Cron -->
      <div class="flex flex-col gap-1.5">
        <Label for="cron">Schedule</Label>
        <Input id="cron" v-model="cron" class="font-mono" placeholder="minute hour day month weekday" />
        <div class="flex flex-wrap gap-1.5">
          <button
            v-for="p in PRESETS"
            :key="p.value"
            type="button"
            class="rounded-full border border-border px-2.5 py-0.5 text-[11px] text-muted-foreground transition-colors hover:text-foreground"
            @click="cron = p.value"
          >
            {{ p.label }}
          </button>
        </div>
      </div>

      <!-- Timezone -->
      <div class="flex flex-col gap-1.5">
        <Label>Timezone</Label>
        <Select v-model="timezone" :options="zoneOptions" />
      </div>

      <!-- Resolved preview -->
      <div class="rounded-lg border border-border bg-background/60 p-3">
        <div class="mb-1.5 flex items-center gap-1.5 text-[11px] font-medium text-muted-foreground">
          <CalendarClock class="size-3.5" /> Next runs
        </div>
        <p v-if="previewError" class="text-xs text-destructive">{{ previewError }}</p>
        <template v-else-if="preview">
          <p class="text-xs font-medium">{{ preview.description }}</p>
          <ul class="mt-1.5 flex flex-col gap-0.5">
            <li v-for="t in preview.upcoming.slice(0, 3)" :key="t" class="font-mono text-xs text-muted-foreground">
              {{ fmt(t) }}
            </li>
          </ul>
          <p class="mt-1.5 text-[11px] text-muted-foreground/70">Shown in your local time.</p>
        </template>
      </div>

      <!-- Script params -->
      <div v-if="selected?.params?.length" class="flex flex-col gap-4 border-t border-border pt-4">
        <div class="flex items-center gap-1.5 text-[11px] font-medium text-muted-foreground">
          <Terminal class="size-3.5" /> Parameters
        </div>
        <ParamField
          v-for="p in selected.params"
          :key="p.name"
          :param="p"
          :model-value="params[p.name]"
          @update:model-value="params[p.name] = $event"
        />
      </div>

      <div v-if="selected?.notes" class="flex gap-2 rounded-lg border border-border bg-muted/40 p-3 text-xs text-muted-foreground">
        <Info class="mt-0.5 size-3.5 shrink-0" />
        <span>{{ selected.notes }}</span>
      </div>

      <!-- Enabled -->
      <label class="flex items-center justify-between gap-3 rounded-lg border border-border p-3">
        <div>
          <div class="text-sm font-medium">Enabled</div>
          <div class="text-xs text-muted-foreground">Disabled schedules are kept but never fire.</div>
        </div>
        <Switch :model-value="enabled" @update:model-value="enabled = $event" />
      </label>
    </div>

    <div class="flex justify-end gap-2">
      <Button variant="ghost" :disabled="saving" @click="emit('update:open', false)">Cancel</Button>
      <Button :disabled="!canSave" @click="save">
        <Spinner v-if="saving" class="size-4" />
        <Save v-else class="size-4" />
        {{ schedule ? 'Save changes' : 'Create schedule' }}
      </Button>
    </div>
  </Dialog>
</template>
