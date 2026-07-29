<script setup lang="ts">
import { ref, watch, computed } from 'vue'
import { Save, Loader2 } from 'lucide-vue-next'
import Dialog from '@/components/ui/Dialog.vue'
import Button from '@/components/ui/Button.vue'
import Input from '@/components/ui/Input.vue'
import Label from '@/components/ui/Label.vue'
import Textarea from '@/components/ui/Textarea.vue'
import Switch from '@/components/ui/Switch.vue'
import BboxMap from './BboxMap.vue'
import { createRegion, updateRegion, ApiError } from '@/lib/api'
import { toast } from '@/lib/toast'
import type { Bbox, ImportRegion } from '@/lib/types'

const props = defineProps<{ region: ImportRegion | null; open: boolean }>()
const emit = defineEmits<{ 'update:open': [value: boolean]; saved: [] }>()

const isEdit = computed(() => props.region !== null)

// Continental-US default so a fresh map shows land; the operator pans/zooms and
// clicks "Use current view" (or drags the corners) to set the real box.
const DEFAULT_BBOX: Bbox = [-125, 24, -66, 50]

interface Form {
  key: string
  label: string
  bbox: Bbox
  osmExtracts: string
  osmReplication: string
  gtfsRegion: string
  openaddresses: string
  wofIds: string
  tigerStates: string
  enabled: boolean
}

const form = ref<Form>(blank())
const saving = ref(false)

function blank(): Form {
  return {
    key: '',
    label: '',
    bbox: [...DEFAULT_BBOX],
    osmExtracts: '',
    osmReplication: '',
    gtfsRegion: '',
    openaddresses: '',
    wofIds: '',
    tigerStates: '',
    enabled: true,
  }
}

const lines = (arr: string[]) => arr.join('\n')
const toLines = (s: string) => s.split('\n').map((x) => x.trim()).filter(Boolean)
const toList = (s: string) => s.split(/[\s,]+/).map((x) => x.trim()).filter(Boolean)
const toNums = (s: string) => toList(s).map(Number).filter((n) => Number.isFinite(n))

watch(
  () => [props.open, props.region?.key],
  () => {
    if (!props.open) return
    const r = props.region
    form.value = r
      ? {
          key: r.key,
          label: r.label,
          bbox: [...r.bbox],
          osmExtracts: lines(r.osmExtracts),
          osmReplication: lines(r.osmReplication),
          gtfsRegion: r.gtfsRegion,
          openaddresses: lines(r.pelias.openaddresses),
          wofIds: (r.pelias.wofIds || []).join(', '),
          tigerStates: (r.pelias.tigerStates || []).join(', '),
          enabled: r.enabled,
        }
      : blank()
  },
  { immediate: true },
)

const keyValid = computed(() => isEdit.value || /^[a-z0-9][a-z0-9-]*$/.test(form.value.key))
const canSave = computed(() => form.value.label.trim() && keyValid.value && (isEdit.value || form.value.key.trim()))

function setCoord(i: number, raw: string) {
  const n = parseFloat(raw)
  if (!Number.isFinite(n)) return
  const next = [...form.value.bbox] as Bbox
  next[i] = n
  form.value.bbox = next
}

async function save() {
  if (!canSave.value || saving.value) return
  saving.value = true
  const payload = {
    key: form.value.key.trim(),
    label: form.value.label.trim(),
    bbox: form.value.bbox,
    osmExtracts: toLines(form.value.osmExtracts),
    osmReplication: toLines(form.value.osmReplication),
    gtfsRegion: form.value.gtfsRegion.trim(),
    enabled: form.value.enabled,
    pelias: {
      openaddresses: toLines(form.value.openaddresses),
      wofIds: toList(form.value.wofIds),
      tigerStates: toNums(form.value.tigerStates),
    },
  }
  try {
    if (isEdit.value) {
      const { key, ...rest } = payload
      await updateRegion(props.region!.key, rest)
      toast({ title: 'Region saved', description: payload.label, variant: 'success' })
    } else {
      await createRegion(payload)
      toast({ title: 'Region created', description: payload.label, variant: 'success' })
    }
    emit('saved')
    emit('update:open', false)
  } catch (err) {
    const msg = err instanceof ApiError || err instanceof Error ? err.message : 'Unknown error'
    toast({ title: 'Save failed', description: msg, variant: 'error' })
  } finally {
    saving.value = false
  }
}

const coordLabels = ['West', 'South', 'East', 'North']
</script>

<template>
  <Dialog :open="open" @update:open="emit('update:open', $event)" class="max-w-2xl">
    <div class="flex max-h-[85vh] flex-col gap-4 overflow-y-auto pr-1">
      <div class="flex flex-col gap-1 pr-6">
        <h2 class="text-lg font-semibold leading-none">{{ isEdit ? `Edit ${region?.label}` : 'New region' }}</h2>
        <p class="text-sm text-muted-foreground">
          Defines the area an import pulls in. Run an import with <code class="font-mono text-xs">REGIONS={{ form.key || 'key' }}</code> to fetch it.
        </p>
      </div>

      <!-- Identity -->
      <div class="grid grid-cols-2 gap-3">
        <div class="flex flex-col gap-1.5">
          <Label>Key</Label>
          <Input
            :model-value="form.key"
            :disabled="isEdit"
            placeholder="e.g. north-carolina"
            @update:model-value="form.key = $event"
          />
          <p v-if="!keyValid" class="text-xs text-destructive">Lowercase letters, numbers and dashes only.</p>
        </div>
        <div class="flex flex-col gap-1.5">
          <Label>Label</Label>
          <Input :model-value="form.label" placeholder="North Carolina" @update:model-value="form.label = $event" />
        </div>
      </div>

      <!-- Bounding box -->
      <div class="flex flex-col gap-2">
        <Label>Bounding box</Label>
        <BboxMap :model-value="form.bbox" @update:model-value="form.bbox = $event" />
        <div class="grid grid-cols-4 gap-2">
          <div v-for="(lbl, i) in coordLabels" :key="lbl" class="flex flex-col gap-1">
            <span class="text-xs text-muted-foreground">{{ lbl }}</span>
            <Input type="number" :model-value="form.bbox[i]" @update:model-value="setCoord(i, $event)" />
          </div>
        </div>
      </div>

      <!-- OSM sources -->
      <div class="flex flex-col gap-1.5">
        <Label>OSM extract URLs</Label>
        <Textarea
          :model-value="form.osmExtracts"
          :rows="2"
          placeholder="https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf"
          @update:model-value="form.osmExtracts = $event"
        />
        <p class="text-xs text-muted-foreground">One Geofabrik/planet PBF URL per line. Leave blank to carve from a planet extract by bbox.</p>
      </div>
      <div class="flex flex-col gap-1.5">
        <Label>OSM replication URLs <span class="text-muted-foreground">(optional)</span></Label>
        <Textarea
          :model-value="form.osmReplication"
          :rows="2"
          placeholder="https://download.geofabrik.de/north-america/us/north-carolina-updates/"
          @update:model-value="form.osmReplication = $event"
        />
      </div>

      <!-- Transit -->
      <div class="flex flex-col gap-1.5">
        <Label>GTFS region token <span class="text-muted-foreground">(optional)</span></Label>
        <Input :model-value="form.gtfsRegion" placeholder="nc" @update:model-value="form.gtfsRegion = $event" />
        <p class="text-xs text-muted-foreground">Transitland operator/region token used by the GTFS importer.</p>
      </div>

      <!-- Addresses -->
      <div class="rounded-lg border border-border bg-muted/30 p-3">
        <p class="mb-2 text-sm font-medium">Addresses (Pelias) <span class="text-muted-foreground">— optional</span></p>
        <div class="flex flex-col gap-3">
          <div class="flex flex-col gap-1.5">
            <Label>OpenAddresses files</Label>
            <Textarea
              :model-value="form.openaddresses"
              :rows="2"
              placeholder="us/nc/mecklenburg.csv"
              @update:model-value="form.openaddresses = $event"
            />
            <p class="text-xs text-muted-foreground">One OpenAddresses CSV path per line.</p>
          </div>
          <div class="grid grid-cols-2 gap-3">
            <div class="flex flex-col gap-1.5">
              <Label>Who's-on-First ids</Label>
              <Input :model-value="form.wofIds" placeholder="85688773" @update:model-value="form.wofIds = $event" />
            </div>
            <div class="flex flex-col gap-1.5">
              <Label>TIGER state FIPS</Label>
              <Input :model-value="form.tigerStates" placeholder="37" @update:model-value="form.tigerStates = $event" />
            </div>
          </div>
        </div>
      </div>

      <!-- Enabled -->
      <label class="flex items-center gap-3">
        <Switch :model-value="form.enabled" @update:model-value="form.enabled = $event" />
        <span class="text-sm">Enabled</span>
      </label>

      <!-- Actions -->
      <div class="flex justify-end gap-2 pt-1">
        <Button variant="ghost" :disabled="saving" @click="emit('update:open', false)">Cancel</Button>
        <Button :disabled="!canSave || saving" @click="save">
          <Loader2 v-if="saving" class="size-4 animate-spin" />
          <Save v-else class="size-4" />
          {{ isEdit ? 'Save changes' : 'Create region' }}
        </Button>
      </div>
    </div>
  </Dialog>
</template>
