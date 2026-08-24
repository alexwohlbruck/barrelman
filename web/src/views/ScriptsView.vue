<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { useRouter, RouterLink } from 'vue-router'
import { Map as MapIcon, TrainFront, Bike, Search, Route, Database, FileCog, LayoutGrid, Layers } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import ScriptCard from '@/components/scripts/ScriptCard.vue'
import RunScriptDialog from '@/components/scripts/RunScriptDialog.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { getScripts } from '@/lib/api'
import { jobs } from '@/lib/store'
import { toast } from '@/lib/toast'
import type { ScriptsResponse, ScriptDef, ScriptCategory, Job } from '@/lib/types'

const router = useRouter()
const data = ref<ScriptsResponse | null>(null)
const loading = ref(true)
const activeCategory = ref<ScriptCategory | 'all'>('all')

const dialogOpen = ref(false)
const selectedScript = ref<ScriptDef | null>(null)

const categoryIcons: Record<ScriptCategory, any> = {
  osm: MapIcon,
  transit: TrainFront,
  gbfs: Bike,
  search: Search,
  routing: Route,
  database: Database,
  config: FileCog,
}

// A script's own in-flight job — queued counts, since that is exactly the state
// an operator needs to see before clicking Run again.
const activeByScript = computed(() => {
  const m = new Map<string, Job>()
  for (const j of jobs.value) {
    if (j.status === 'running' || j.status === 'queued') if (!m.has(j.scriptId)) m.set(j.scriptId, j)
  }
  return m
})

const queuedCount = computed(() => jobs.value.filter((j) => j.status === 'queued').length)
const runningJob = computed(() => jobs.value.find((j) => j.status === 'running'))

const visibleCategories = computed(() => {
  if (!data.value) return []
  if (activeCategory.value === 'all') return data.value.categories
  return data.value.categories.filter((c) => c.key === activeCategory.value)
})

function openRun(script: ScriptDef) {
  selectedScript.value = script
  dialogOpen.value = true
}

function onStarted(job: Job) {
  router.push(`/jobs/${job.id}`)
}

onMounted(async () => {
  try {
    data.value = await getScripts()
  } catch (err) {
    toast({ title: 'Failed to load scripts', description: err instanceof Error ? err.message : '', variant: 'error' })
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <PageHeader title="Scripts" subtitle="Run and manage every barrelman data task" />

  <div class="p-8">
    <!-- The single most confusing thing about this page is that Run doesn't
         always mean start now. Say so once, at the top. -->
    <div class="mb-6 flex items-start gap-2 rounded-lg border border-border bg-muted/40 p-3 text-xs text-muted-foreground">
      <Layers class="mt-0.5 size-3.5 shrink-0" />
      <span>
        Scripts run one at a time on the ops worker. Anything started while it's busy waits in the queue and begins on
        its own — it is never run alongside another script. Scripts marked
        <span class="font-medium text-foreground">One at a time</span> go further: a second run is refused outright
        while one is queued or running, which is why a nightly schedule can never stack up behind a slow import.
        <template v-if="runningJob || queuedCount">
          <RouterLink to="/jobs" class="underline underline-offset-4 hover:text-foreground">
            {{ runningJob ? '1 running' : 'Nothing running' }}<template v-if="queuedCount">, {{ queuedCount }} queued</template>
          </RouterLink>
          right now.
        </template>
      </span>
    </div>

    <div v-if="loading" class="flex items-center justify-center py-20 text-muted-foreground">
      <Spinner class="mr-2 size-5" /> Loading scripts…
    </div>

    <template v-else-if="data">
      <!-- Category filter -->
      <div class="mb-6 flex flex-wrap gap-2">
        <button
          :class="[
            'flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs font-medium transition-colors',
            activeCategory === 'all' ? 'border-primary bg-primary text-primary-foreground' : 'border-border text-muted-foreground hover:text-foreground',
          ]"
          @click="activeCategory = 'all'"
        >
          <LayoutGrid class="size-3.5" /> All
          <span class="opacity-60">{{ data.scripts.length }}</span>
        </button>
        <button
          v-for="cat in data.categories"
          :key="cat.key"
          :class="[
            'flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs font-medium transition-colors',
            activeCategory === cat.key ? 'border-primary bg-primary text-primary-foreground' : 'border-border text-muted-foreground hover:text-foreground',
          ]"
          @click="activeCategory = cat.key"
        >
          <component :is="categoryIcons[cat.key]" class="size-3.5" />
          {{ cat.label }}
          <span class="opacity-60">{{ cat.scripts.length }}</span>
        </button>
      </div>

      <!-- Grouped scripts -->
      <div class="space-y-10">
        <section v-for="cat in visibleCategories" :key="cat.key">
          <h2 class="mb-4 flex items-center gap-2 text-sm font-semibold">
            <component :is="categoryIcons[cat.key]" class="size-4 text-muted-foreground" />
            {{ cat.label }}
            <span class="text-xs font-normal text-muted-foreground">{{ cat.scripts.length }}</span>
          </h2>
          <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            <ScriptCard
              v-for="script in cat.scripts"
              :key="script.id"
              :script="script"
              :active-job="activeByScript.get(script.id)"
              @run="openRun(script)"
            />
          </div>
        </section>
      </div>
    </template>
  </div>

  <RunScriptDialog
    v-model:open="dialogOpen"
    :script="selectedScript"
    @started="onStarted"
    @conflict="(id: string) => router.push(`/jobs/${id}`)"
  />
</template>
