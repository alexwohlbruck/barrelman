<script setup lang="ts">
/**
 * First-run setup checklist.
 *
 * A fresh instance boots with an empty database, so every data endpoint answers
 * correctly and returns nothing. The console already knows enough to say why —
 * no region, no import, no key — so this surfaces the remaining steps instead of
 * leaving them to the docs.
 *
 * Entirely derived from `/admin/metrics`, `/admin/regions` and `/admin/services`,
 * which the dashboard already loads. No new endpoint, and nothing to keep in
 * sync beyond this file.
 *
 * Required steps gate whether the card shows at all. Optional ones (transit,
 * bikeshare) are listed but never block completion — otherwise the checklist
 * would live forever on the many instances that legitimately skip them.
 */
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Check, ArrowRight, X, Loader2, Circle } from 'lucide-vue-next'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import { metrics, regions, refreshMetrics } from '@/lib/store'
import { refreshBoundaryCatalog } from '@/lib/api'
import { toast } from '@/lib/toast'

const router = useRouter()

const DISMISS_KEY = 'barrelman.setup.dismissed'
const dismissed = ref(localStorage.getItem(DISMISS_KEY) === '1')
const catalogBusy = ref(false)

interface Step {
  key: string
  title: string
  detail: string
  done: boolean
  to?: string
  cta?: string
  action?: () => void | Promise<void>
  busy?: boolean
}

const loaded = computed(() => metrics.value !== null && regions.value !== null)

const required = computed<Step[]>(() => {
  const m = metrics.value
  const r = regions.value ?? []
  return [
    {
      key: 'catalog',
      title: 'Fetch the boundary catalog',
      detail: 'Downloads the index of every importable region. No API key needed.',
      done: (m?.boundaries.count ?? 0) > 0,
      cta: 'Fetch',
      busy: catalogBusy.value,
      action: fetchCatalog,
    },
    {
      key: 'region',
      title: 'Choose a region',
      detail: 'Barrelman imports named regions, not the whole planet. Add one by name.',
      done: r.length > 0,
      to: '/regions',
      cta: 'Add region',
    },
    {
      key: 'osm',
      title: 'Import OpenStreetMap data',
      detail: 'The long one — about 15 minutes for a US state. Search stays empty until it finishes.',
      done: (m?.geoPlaces.total ?? 0) > 0,
      to: '/scripts',
      cta: 'Run import',
    },
    {
      key: 'key',
      title: 'Create an API key',
      detail: 'Scoped, shown once. This is what your apps authenticate with.',
      done: (m?.accounts.activeKeys ?? 0) > 0,
      to: '/keys',
      cta: 'Create key',
    },
  ]
})

const optional = computed<Step[]>(() => {
  const m = metrics.value
  return [
    {
      key: 'transit',
      title: 'Transit routing',
      detail: 'GTFS feeds and the MOTIS timetable. Three scripts, in order.',
      done: (m?.gtfs.feeds ?? 0) > 0,
      to: '/scripts',
      cta: 'Set up',
    },
    {
      key: 'gbfs',
      title: 'Bikeshare',
      detail: 'GBFS systems and live station availability. Slow — it scans the global catalog.',
      done: (m?.gbfs.systems ?? 0) > 0,
      to: '/scripts',
      cta: 'Set up',
    },
  ]
})

const doneCount = computed(() => required.value.filter((s) => s.done).length)
const complete = computed(() => doneCount.value === required.value.length)
const visible = computed(() => loaded.value && !complete.value && !dismissed.value)

async function fetchCatalog() {
  catalogBusy.value = true
  try {
    const r = await refreshBoundaryCatalog()
    toast({ title: `Cached ${r.count.toLocaleString()} importable regions`, variant: 'success' })
    await refreshMetrics()
  } catch (err) {
    toast({
      title: 'Could not fetch the boundary catalog',
      description: err instanceof Error ? err.message : undefined,
      variant: 'error',
    })
  } finally {
    catalogBusy.value = false
  }
}

function dismiss() {
  dismissed.value = true
  localStorage.setItem(DISMISS_KEY, '1')
}
</script>

<template>
  <Card v-if="visible" class="border-primary/30 bg-primary/[0.03] p-5">
    <div class="flex items-start justify-between gap-4">
      <div>
        <h2 class="text-sm font-medium">Finish setting up</h2>
        <p class="mt-0.5 text-xs text-muted-foreground">
          This instance has no data yet. These steps get it answering real queries.
        </p>
      </div>
      <div class="flex items-center gap-3">
        <span class="text-xs tabular-nums text-muted-foreground">
          {{ doneCount }} of {{ required.length }}
        </span>
        <button
          class="rounded-md p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          title="Hide until next time"
          @click="dismiss"
        >
          <X class="size-4" />
        </button>
      </div>
    </div>

    <!-- Progress -->
    <div class="mt-3 h-1 overflow-hidden rounded-full bg-muted">
      <div
        class="h-full rounded-full bg-primary transition-[width] duration-500"
        :style="{ width: `${(doneCount / required.length) * 100}%` }"
      />
    </div>

    <ul class="mt-4 space-y-1">
      <li
        v-for="step in required"
        :key="step.key"
        class="flex items-center gap-3 rounded-lg px-2 py-2"
        :class="step.done ? 'opacity-55' : 'hover:bg-muted/40'"
      >
        <span
          class="flex size-5 shrink-0 items-center justify-center rounded-full border"
          :class="step.done ? 'border-[var(--success)] bg-[var(--success)]/15' : 'border-border'"
        >
          <Check v-if="step.done" class="size-3 text-[var(--success)]" />
        </span>
        <div class="min-w-0 flex-1">
          <div class="text-sm" :class="step.done && 'line-through decoration-muted-foreground/50'">
            {{ step.title }}
          </div>
          <div v-if="!step.done" class="mt-0.5 text-xs text-muted-foreground">{{ step.detail }}</div>
        </div>
        <template v-if="!step.done">
          <Button v-if="step.action" size="sm" variant="outline" :disabled="step.busy" @click="step.action">
            <Loader2 v-if="step.busy" class="size-3.5 animate-spin" />
            {{ step.cta }}
          </Button>
          <Button v-else-if="step.to" size="sm" variant="outline" @click="router.push(step.to)">
            {{ step.cta }}
            <ArrowRight class="size-3.5" />
          </Button>
        </template>
      </li>
    </ul>

    <div class="mt-4 border-t border-border pt-3">
      <div class="px-2 text-xs text-muted-foreground">Optional</div>
      <ul class="mt-1 space-y-1">
        <li
          v-for="step in optional"
          :key="step.key"
          class="flex items-center gap-3 rounded-lg px-2 py-1.5"
          :class="step.done ? 'opacity-55' : 'hover:bg-muted/40'"
        >
          <span class="flex size-5 shrink-0 items-center justify-center">
            <Check v-if="step.done" class="size-3 text-[var(--success)]" />
            <Circle v-else class="size-2 text-muted-foreground/40" />
          </span>
          <div class="min-w-0 flex-1">
            <div class="text-sm text-muted-foreground">{{ step.title }}</div>
            <div v-if="!step.done" class="mt-0.5 text-xs text-muted-foreground/70">{{ step.detail }}</div>
          </div>
          <Button v-if="!step.done && step.to" size="sm" variant="ghost" @click="router.push(step.to)">
            {{ step.cta }}
          </Button>
        </li>
      </ul>
    </div>
  </Card>
</template>
