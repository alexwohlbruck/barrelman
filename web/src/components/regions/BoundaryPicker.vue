<script setup lang="ts">
/**
 * "Type a place name, get a region."
 *
 * Searches the cached boundary catalog (Geofabrik's extract index) and resolves
 * the pick into a fully populated region definition — PBF + replication URLs,
 * the real boundary's bounding box, a GTFS search box, TIGER FIPS codes and the
 * OpenAddresses file list. The result is handed to the region editor for review
 * rather than saved directly, so nothing is written without a look.
 */
import { ref, watch } from 'vue'
import { Search, Loader2, RefreshCw, MapPin, Download } from 'lucide-vue-next'
import Dialog from '@/components/ui/Dialog.vue'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Spinner from '@/components/ui/Spinner.vue'
import Badge from '@/components/ui/Badge.vue'
import { searchBoundaries, refreshBoundaryCatalog, resolveBoundary, ApiError } from '@/lib/api'
import { toast } from '@/lib/toast'
import type { Boundary, DerivedRegion } from '@/lib/types'

defineProps<{ open: boolean }>()
const emit = defineEmits<{ 'update:open': [value: boolean]; resolved: [value: DerivedRegion] }>()

const query = ref('')
const results = ref<Boundary[]>([])
const searching = ref(false)
const refreshing = ref(false)
const resolvingId = ref<string | null>(null)
const catalogCount = ref<number | null>(null)
const searched = ref(false)

let seq = 0

async function runSearch() {
  const q = query.value.trim()
  searched.value = Boolean(q)
  if (!q) {
    results.value = []
    return
  }
  // Keystrokes race: only the newest response may write to `results`.
  const mine = ++seq
  searching.value = true
  try {
    const res = await searchBoundaries(q)
    if (mine !== seq) return
    results.value = res.boundaries
    catalogCount.value = res.catalog.count
  } catch (err) {
    if (mine !== seq) return
    toast({ title: 'Search failed', description: err instanceof Error ? err.message : '', variant: 'error' })
  } finally {
    if (mine === seq) searching.value = false
  }
}

let debounce: ReturnType<typeof setTimeout> | undefined
watch(query, () => {
  clearTimeout(debounce)
  debounce = setTimeout(runSearch, 250)
})

async function refresh() {
  refreshing.value = true
  try {
    const { count } = await refreshBoundaryCatalog()
    catalogCount.value = count
    toast({ title: 'Boundary catalog updated', description: `${count} regions available`, variant: 'success' })
    await runSearch()
  } catch (err) {
    toast({ title: 'Refresh failed', description: err instanceof Error ? err.message : '', variant: 'error' })
  } finally {
    refreshing.value = false
  }
}

async function pick(b: Boundary) {
  resolvingId.value = b.id
  try {
    const derived = await resolveBoundary(b.id)
    emit('resolved', derived)
    emit('update:open', false)
    query.value = ''
    results.value = []
    searched.value = false
  } catch (err) {
    const msg = err instanceof ApiError || err instanceof Error ? err.message : 'Unknown error'
    toast({ title: 'Could not resolve region', description: msg, variant: 'error' })
  } finally {
    resolvingId.value = null
  }
}

const fmtBbox = (b: number[]) => b.map((n) => n.toFixed(2)).join(', ')
</script>

<template>
  <Dialog :open="open" @update:open="emit('update:open', $event)" class="max-w-2xl">
    <div class="flex max-h-[80vh] flex-col gap-4">
      <div class="flex flex-col gap-1 pr-6">
        <h2 class="text-lg font-semibold leading-none">Add a region by name</h2>
        <p class="text-sm text-muted-foreground">
          Search published OSM extracts. Picking one fills in the download URLs, bounding box,
          transit search area and address sources automatically.
        </p>
      </div>

      <div class="flex items-center gap-2">
        <div class="relative flex-1">
          <Search class="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            :model-value="query"
            class="pl-9"
            placeholder="colorado, germany, bayern, US-CO…"
            @update:model-value="query = $event"
            @keydown.enter="runSearch"
          />
        </div>
        <Button variant="outline" :disabled="refreshing" title="Re-download the catalog from Geofabrik" @click="refresh">
          <Loader2 v-if="refreshing" class="size-4 animate-spin" />
          <RefreshCw v-else class="size-4" />
        </Button>
      </div>

      <div class="min-h-[8rem] flex-1 overflow-y-auto">
        <div v-if="searching" class="flex justify-center py-10">
          <Spinner class="size-5" />
        </div>

        <div v-else-if="catalogCount === 0" class="rounded-lg border border-dashed border-border p-6 text-center">
          <Download class="mx-auto size-7 text-muted-foreground" />
          <p class="mt-2 text-sm font-medium">The boundary catalog is empty</p>
          <p class="mt-1 text-sm text-muted-foreground">
            Fetch it once from Geofabrik — no API key needed.
          </p>
          <Button class="mt-3" :disabled="refreshing" @click="refresh">
            <Loader2 v-if="refreshing" class="size-4 animate-spin" />
            <Download v-else class="size-4" />
            Fetch catalog
          </Button>
        </div>

        <div v-else-if="searched && !results.length" class="py-10 text-center text-sm text-muted-foreground">
          No matching extract. Try a country, state or province name.
        </div>

        <div v-else-if="!searched" class="py-10 text-center text-sm text-muted-foreground">
          Start typing to search
          <template v-if="catalogCount"> {{ catalogCount }} available regions</template>.
        </div>

        <ul v-else class="flex flex-col gap-1.5">
          <li v-for="b in results" :key="b.id">
            <button
              class="flex w-full items-center gap-3 rounded-lg border border-border bg-card/40 p-3 text-left transition hover:border-primary/50 hover:bg-accent/40 disabled:opacity-60"
              :disabled="resolvingId !== null"
              @click="pick(b)"
            >
              <MapPin class="size-4 shrink-0 text-muted-foreground" />
              <div class="min-w-0 flex-1">
                <div class="flex items-center gap-2">
                  <span class="truncate font-medium">{{ b.label }}</span>
                  <Badge v-for="c in [...b.iso3166_1, ...b.iso3166_2]" :key="c" variant="secondary" class="text-[10px]">
                    {{ c }}
                  </Badge>
                </div>
                <div class="truncate text-xs text-muted-foreground">
                  <code>{{ b.id }}</code>
                  <span v-if="b.parent"> · in {{ b.parent }}</span>
                  <span class="font-mono"> · [{{ fmtBbox(b.bbox) }}]</span>
                </div>
              </div>
              <Loader2 v-if="resolvingId === b.id" class="size-4 shrink-0 animate-spin" />
            </button>
          </li>
        </ul>
      </div>

      <div class="flex justify-end">
        <Button variant="ghost" @click="emit('update:open', false)">Cancel</Button>
      </div>
    </div>
  </Dialog>
</template>
