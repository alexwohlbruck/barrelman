<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { Plus, Pencil, Trash2, Globe, MapPin, Loader2, Database, Search } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import Button from '@/components/ui/Button.vue'
import Badge from '@/components/ui/Badge.vue'
import Switch from '@/components/ui/Switch.vue'
import Spinner from '@/components/ui/Spinner.vue'
import Dialog from '@/components/ui/Dialog.vue'
import RegionEditor from '@/components/regions/RegionEditor.vue'
import BoundaryPicker from '@/components/regions/BoundaryPicker.vue'
import { getRegions, updateRegion, deleteRegion, ApiError } from '@/lib/api'
import { toast } from '@/lib/toast'
import type { ImportRegion, DerivedRegion } from '@/lib/types'

const regions = ref<ImportRegion[]>([])
const loading = ref(true)

const editorOpen = ref(false)
const editing = ref<ImportRegion | null>(null)
const prefill = ref<DerivedRegion | null>(null)

const pickerOpen = ref(false)

const deleteTarget = ref<ImportRegion | null>(null)
const deleting = ref(false)

async function load() {
  loading.value = true
  try {
    regions.value = (await getRegions()).regions
  } catch (err) {
    toast({ title: 'Failed to load regions', description: err instanceof Error ? err.message : '', variant: 'error' })
  } finally {
    loading.value = false
  }
}

function openNew() {
  editing.value = null
  prefill.value = null
  editorOpen.value = true
}

function openEdit(r: ImportRegion) {
  editing.value = r
  prefill.value = null
  editorOpen.value = true
}

/** Catalog pick resolved — review the auto-filled definition before saving. */
function onResolved(derived: DerivedRegion) {
  editing.value = null
  prefill.value = derived
  editorOpen.value = true
}

async function toggleEnabled(r: ImportRegion) {
  try {
    const { isGlobal, key, ...rest } = r
    await updateRegion(r.key, { ...rest, enabled: !r.enabled })
    await load()
  } catch (err) {
    toast({ title: 'Update failed', description: err instanceof Error ? err.message : '', variant: 'error' })
  }
}

async function doDelete() {
  if (!deleteTarget.value) return
  deleting.value = true
  try {
    await deleteRegion(deleteTarget.value.key)
    toast({ title: 'Region deleted', description: deleteTarget.value.label, variant: 'success' })
    deleteTarget.value = null
    await load()
  } catch (err) {
    const msg = err instanceof ApiError || err instanceof Error ? err.message : 'Unknown error'
    toast({ title: 'Delete failed', description: msg, variant: 'error' })
  } finally {
    deleting.value = false
  }
}

const fmtBbox = (b: number[]) => `[${b.map((n) => n.toFixed(2)).join(', ')}]`

onMounted(load)
</script>

<template>
  <div>
    <PageHeader title="Regions" subtitle="Geographic areas the OSM, transit, and address importers pull in">
      <template #actions>
        <Button variant="outline" @click="pickerOpen = true">
          <Search class="size-4" /> Add by name
        </Button>
        <Button @click="openNew">
          <Plus class="size-4" /> New region
        </Button>
      </template>
    </PageHeader>

    <div class="p-8">
      <div v-if="loading" class="flex justify-center py-16">
        <Spinner class="size-6" />
      </div>

      <div v-else-if="!regions.length" class="rounded-xl border border-dashed border-border py-16 text-center">
        <MapPin class="mx-auto size-8 text-muted-foreground" />
        <p class="mt-3 text-sm font-medium">No regions yet</p>
        <p class="mt-1 text-sm text-muted-foreground">Define an area to import, then run an import against it.</p>
        <div class="mt-4 flex justify-center gap-2">
          <Button @click="pickerOpen = true"><Search class="size-4" /> Add by name</Button>
          <Button variant="outline" @click="openNew"><Plus class="size-4" /> Blank region</Button>
        </div>
      </div>

      <div v-else class="grid gap-3 md:grid-cols-2">
        <div
          v-for="r in regions"
          :key="r.key"
          class="flex flex-col gap-3 rounded-xl border border-border bg-card/40 p-4"
        >
          <div class="flex items-start justify-between gap-3">
            <div class="min-w-0">
              <div class="flex items-center gap-2">
                <Globe v-if="r.isGlobal" class="size-4 shrink-0 text-muted-foreground" />
                <MapPin v-else class="size-4 shrink-0 text-muted-foreground" />
                <h3 class="truncate font-semibold">{{ r.label }}</h3>
              </div>
              <code class="text-xs text-muted-foreground">{{ r.key }}</code>
            </div>
            <Switch :model-value="r.enabled" @update:model-value="toggleEnabled(r)" />
          </div>

          <div class="flex flex-wrap gap-1.5">
            <Badge variant="secondary" class="font-mono text-[11px]">{{ fmtBbox(r.bbox) }}</Badge>
            <Badge v-if="r.osmExtracts.length" variant="outline">{{ r.osmExtracts.length }} OSM extract{{ r.osmExtracts.length === 1 ? '' : 's' }}</Badge>
            <Badge v-if="r.gtfsRegion" variant="outline">GTFS: {{ r.gtfsRegion }}</Badge>
            <Badge v-if="r.pelias.openaddresses.length" variant="outline">{{ r.pelias.openaddresses.length }} address file{{ r.pelias.openaddresses.length === 1 ? '' : 's' }}</Badge>
          </div>

          <div class="mt-auto flex justify-end gap-1.5">
            <Button variant="ghost" size="sm" @click="openEdit(r)"><Pencil class="size-3.5" /> Edit</Button>
            <Button
              v-if="!r.isGlobal"
              variant="ghost"
              size="sm"
              class="text-destructive hover:text-destructive"
              @click="deleteTarget = r"
            >
              <Trash2 class="size-3.5" /> Delete
            </Button>
          </div>
        </div>
      </div>

      <p class="mt-6 flex items-center gap-1.5 text-xs text-muted-foreground">
        <Database class="size-3.5" />
        Changes take effect on the next import. Run one from
        <RouterLink to="/scripts" class="underline">Scripts</RouterLink>
        with the region selected.
      </p>
    </div>

    <BoundaryPicker :open="pickerOpen" @update:open="pickerOpen = $event" @resolved="onResolved" />

    <RegionEditor
      :region="editing"
      :prefill="prefill"
      :open="editorOpen"
      @update:open="editorOpen = $event"
      @saved="load"
    />

    <!-- Delete confirmation -->
    <Dialog :open="deleteTarget !== null" @update:open="deleteTarget = $event ? deleteTarget : null" class="max-w-md">
      <template v-if="deleteTarget">
        <div class="flex flex-col gap-1.5 pr-6">
          <h2 class="text-lg font-semibold leading-none">Delete region?</h2>
          <p class="text-sm text-muted-foreground">
            Remove <span class="font-medium text-foreground">{{ deleteTarget.label }}</span> from the import config. This does not delete any already-imported data.
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
