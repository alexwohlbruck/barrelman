<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import { Map as MapIcon, TrainFront, Bike, Search, Route, Database, FileCog, LayoutGrid } from 'lucide-vue-next'
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

const runningByScript = computed(() => {
  const m = new Map<string, Job>()
  for (const j of jobs.value) if (j.status === 'running') m.set(j.scriptId, j)
  return m
})

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
              :running-job="runningByScript.get(script.id)"
              @run="openRun(script)"
            />
          </div>
        </section>
      </div>
    </template>
  </div>

  <RunScriptDialog v-model:open="dialogOpen" :script="selectedScript" @started="onStarted" />
</template>
