<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { RouterLink, useRouter } from 'vue-router'
import { CalendarClock, Layers, Loader2, Pencil, Play, Plus, Trash2 } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import Button from '@/components/ui/Button.vue'
import Badge from '@/components/ui/Badge.vue'
import Switch from '@/components/ui/Switch.vue'
import Spinner from '@/components/ui/Spinner.vue'
import Dialog from '@/components/ui/Dialog.vue'
import ScheduleEditor from '@/components/schedules/ScheduleEditor.vue'
import { getSchedules, getScripts, setScheduleEnabled, deleteSchedule, runSchedule, ApiError } from '@/lib/api'
import { toast } from '@/lib/toast'
import { refreshJobs } from '@/lib/store'
import { timeAgo } from '@/lib/utils'
import type { Schedule, ScriptDef } from '@/lib/types'

const schedules = ref<Schedule[]>([])
const scripts = ref<ScriptDef[]>([])
const defaultTimezone = ref('UTC')
const loading = ref(true)

const editorOpen = ref(false)
const editing = ref<Schedule | null>(null)

const deleteTarget = ref<Schedule | null>(null)
const deleting = ref(false)
const running = ref<string | null>(null)

const router = useRouter()
const enabledCount = computed(() => schedules.value.filter((s) => s.enabled).length)

const scriptById = computed(() => new Map(scripts.value.map((s) => [s.id, s])))

async function load() {
  loading.value = true
  try {
    const [sched, scriptsRes] = await Promise.all([getSchedules(), getScripts()])
    schedules.value = sched.schedules
    defaultTimezone.value = sched.defaultTimezone
    scripts.value = scriptsRes.scripts
  } catch (err) {
    toast({ title: 'Failed to load schedules', description: err instanceof Error ? err.message : '', variant: 'error' })
  } finally {
    loading.value = false
  }
}

function openNew() {
  editing.value = null
  editorOpen.value = true
}

function openEdit(s: Schedule) {
  editing.value = s
  editorOpen.value = true
}

async function toggle(s: Schedule) {
  try {
    const { schedule } = await setScheduleEnabled(s.id, !s.enabled)
    // Patch in place so the row doesn't jump while the operator is looking at it.
    schedules.value = schedules.value.map((x) => (x.id === schedule.id ? schedule : x))
  } catch (err) {
    toast({ title: 'Update failed', description: err instanceof Error ? err.message : '', variant: 'error' })
  }
}

async function runNow(s: Schedule) {
  running.value = s.id
  try {
    await runSchedule(s.id)
    toast({ title: 'Job started', description: s.scriptName, variant: 'success' })
    refreshJobs()
    await load()
  } catch (err) {
    if (err instanceof ApiError && err.status === 409) {
      const activeJobId = err.body?.activeJobId as string | undefined
      toast({
        title: 'Already in flight',
        description: activeJobId ? `${s.scriptName} is already queued or running — opening it.` : err.message,
        variant: 'warning',
      })
      if (activeJobId) router.push(`/jobs/${activeJobId}`)
    } else {
      toast({ title: 'Failed to start', description: err instanceof Error ? err.message : '', variant: 'error' })
    }
  } finally {
    running.value = null
  }
}

async function doDelete() {
  if (!deleteTarget.value) return
  deleting.value = true
  try {
    await deleteSchedule(deleteTarget.value.id)
    toast({ title: 'Schedule deleted', description: deleteTarget.value.scriptName, variant: 'success' })
    deleteTarget.value = null
    await load()
  } catch (err) {
    toast({ title: 'Delete failed', description: err instanceof Error ? err.message : '', variant: 'error' })
  } finally {
    deleting.value = false
  }
}

const fmtNext = (ts?: number) =>
  ts
    ? new Date(ts).toLocaleString(undefined, { weekday: 'short', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
    : '—'

onMounted(load)
</script>

<template>
  <div>
    <PageHeader title="Schedules" subtitle="Recurring imports and refreshes, run as tracked jobs">
      <template #actions>
        <Button @click="openNew"><Plus class="size-4" /> New schedule</Button>
      </template>
    </PageHeader>

    <div class="p-8">
      <div v-if="loading" class="flex justify-center py-16">
        <Spinner class="size-6" />
      </div>

      <div v-else-if="!schedules.length" class="rounded-xl border border-dashed border-border py-16 text-center">
        <CalendarClock class="mx-auto size-8 text-muted-foreground" />
        <p class="mt-3 text-sm font-medium">No schedules yet</p>
        <p class="mt-1 text-sm text-muted-foreground">
          Add one to keep OSM, GTFS or GBFS data refreshing on its own.
        </p>
        <Button class="mt-4" @click="openNew"><Plus class="size-4" /> New schedule</Button>
      </div>

      <template v-else>
        <p v-if="!enabledCount" class="mb-4 rounded-lg border border-[var(--warning)]/40 bg-[var(--warning)]/10 p-3 text-xs text-[var(--warning)]">
          Every schedule is currently disabled — nothing will run automatically. Toggle one on to start it.
        </p>

        <div class="grid gap-3 md:grid-cols-2">
          <div
            v-for="s in schedules"
            :key="s.id"
            class="flex flex-col gap-3 rounded-xl border border-border bg-card/40 p-4"
            :class="!s.enabled && 'opacity-60'"
          >
            <div class="flex items-start justify-between gap-3">
              <div class="min-w-0">
                <h3 class="truncate font-semibold">{{ s.scriptName }}</h3>
                <div class="text-xs text-muted-foreground">{{ s.description || s.cron }}</div>
              </div>
              <Switch :model-value="s.enabled" @update:model-value="toggle(s)" />
            </div>

            <div class="flex flex-wrap gap-1.5">
              <Badge variant="secondary" class="font-mono text-[11px]">{{ s.cron }}</Badge>
              <Badge variant="outline">{{ s.timezone }}</Badge>
              <Badge v-for="(v, k) in s.params" :key="k" variant="outline" class="font-mono text-[11px]">
                {{ k }}={{ v }}
              </Badge>
            </div>

            <dl class="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
              <dt class="text-muted-foreground">Next run</dt>
              <dd class="text-right tabular-nums">{{ s.enabled ? fmtNext(s.nextRunAt) : 'Disabled' }}</dd>
              <dt class="text-muted-foreground">Last run</dt>
              <dd class="text-right">
                <RouterLink v-if="s.lastJobId" :to="`/jobs/${s.lastJobId}`" class="underline">
                  {{ timeAgo(s.lastRunAt) }}
                </RouterLink>
                <span v-else class="text-muted-foreground">{{ s.lastRunAt ? timeAgo(s.lastRunAt) : 'Never' }}</span>
              </dd>
            </dl>

            <p v-if="s.lastSkipReason" class="rounded-md bg-[var(--warning)]/10 px-2 py-1 text-[11px] text-[var(--warning)]">
              Last fire skipped: {{ s.lastSkipReason }}
            </p>

            <!-- The question every operator asks before enabling a nightly
                 import: what happens if the last one is still going? -->
            <p class="flex items-start gap-1.5 text-[11px] text-muted-foreground">
              <Layers class="mt-0.5 size-3 shrink-0" />
              <span v-if="scriptById.get(s.scriptId)?.exclusive">
                If the previous run is still going when this fires, the occurrence is skipped — two runs of
                {{ s.scriptName }} never overlap.
              </span>
              <span v-else>
                Fires queue behind whatever the ops worker is running; they never execute alongside it.
              </span>
            </p>

            <div class="mt-auto flex justify-end gap-1.5">
              <Button variant="ghost" size="sm" :disabled="running === s.id" @click="runNow(s)">
                <Spinner v-if="running === s.id" class="size-3.5" />
                <Play v-else class="size-3.5" />
                Run now
              </Button>
              <Button variant="ghost" size="sm" @click="openEdit(s)"><Pencil class="size-3.5" /> Edit</Button>
              <Button variant="ghost" size="sm" class="text-destructive hover:text-destructive" @click="deleteTarget = s">
                <Trash2 class="size-3.5" /> Delete
              </Button>
            </div>
          </div>
        </div>
      </template>

      <p class="mt-6 flex items-center gap-1.5 text-xs text-muted-foreground">
        <CalendarClock class="size-3.5" />
        Every scheduled run appears in
        <RouterLink to="/jobs" class="underline">Jobs</RouterLink>
        with its full log, marked <span class="font-medium">Scheduled</span>.
      </p>
      <p class="mt-2 flex items-start gap-1.5 text-xs text-muted-foreground">
        <Layers class="mt-0.5 size-3.5 shrink-0" />
        A schedule can't cause two imports at once. The ops worker runs one job at a time, and a script marked
        <span class="font-medium">One at a time</span> refuses a second run outright — a fire that lands mid-run is
        recorded as skipped rather than stacking up.
      </p>
    </div>

    <ScheduleEditor
      :schedule="editing"
      :scripts="scripts"
      :default-timezone="defaultTimezone"
      :open="editorOpen"
      @update:open="editorOpen = $event"
      @saved="load"
    />

    <Dialog :open="deleteTarget !== null" @update:open="deleteTarget = $event ? deleteTarget : null" class="max-w-md">
      <template v-if="deleteTarget">
        <div class="flex flex-col gap-1.5 pr-6">
          <h2 class="text-lg font-semibold leading-none">Delete schedule?</h2>
          <p class="text-sm text-muted-foreground">
            <span class="font-medium text-foreground">{{ deleteTarget.scriptName }}</span> will stop running
            automatically. Past job history is kept, and you can still run the script by hand.
          </p>
        </div>
        <div class="flex justify-end gap-2">
          <Button variant="ghost" :disabled="deleting" @click="deleteTarget = null">Cancel</Button>
          <Button variant="destructive" :disabled="deleting" @click="doDelete">
            <Loader2 v-if="deleting" class="size-4 animate-spin" />
            <Trash2 v-else class="size-4" />
            Delete
          </Button>
        </div>
      </template>
    </Dialog>
  </div>
</template>
