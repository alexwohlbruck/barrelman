<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, watch } from 'vue'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { Crop } from 'lucide-vue-next'
import Button from '@/components/ui/Button.vue'
import type { Bbox } from '@/lib/types'

// Draw/adjust an import region's bounding box on a map. The box is defined by two
// draggable corner markers (SW + NE); "Use current view" snaps it to whatever the
// map is currently showing — the fastest way to pick an area: pan/zoom, then click.
const props = defineProps<{ modelValue: Bbox }>()
const emit = defineEmits<{ 'update:modelValue': [value: Bbox] }>()

const container = ref<HTMLDivElement | null>(null)
let map: maplibregl.Map | null = null
let swMarker: maplibregl.Marker | null = null
let neMarker: maplibregl.Marker | null = null
// Guards the modelValue watcher from fighting our own internal marker updates.
let syncing = false

// Raster OSM basemap — an internal admin tool, so public tiles are fine and let
// an operator pick anywhere on earth (including areas not yet imported).
const OSM_STYLE: maplibregl.StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '© OpenStreetMap contributors',
    },
  },
  layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
}

function bboxPolygon(b: Bbox): GeoJSON.Feature<GeoJSON.Polygon> {
  const [w, s, e, n] = b
  return {
    type: 'Feature',
    properties: {},
    geometry: { type: 'Polygon', coordinates: [[[w, s], [e, s], [e, n], [w, n], [w, s]]] },
  }
}

// Normalized bbox from the two corner markers (min/max so a crossed drag stays valid).
function currentBbox(): Bbox {
  const sw = swMarker!.getLngLat()
  const ne = neMarker!.getLngLat()
  return [
    Math.min(sw.lng, ne.lng),
    Math.min(sw.lat, ne.lat),
    Math.max(sw.lng, ne.lng),
    Math.max(sw.lat, ne.lat),
  ]
}

function renderBbox(b: Bbox) {
  const src = map?.getSource('bbox') as maplibregl.GeoJSONSource | undefined
  src?.setData(bboxPolygon(b))
}

function commit(b: Bbox) {
  syncing = true
  swMarker!.setLngLat([b[0], b[1]])
  neMarker!.setLngLat([b[2], b[3]])
  syncing = false
  renderBbox(b)
  emit('update:modelValue', b)
}

function useCurrentView() {
  if (!map) return
  const bounds = map.getBounds()
  commit([bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()])
}

onMounted(() => {
  if (!container.value) return
  const b = props.modelValue
  map = new maplibregl.Map({
    container: container.value,
    style: OSM_STYLE,
    bounds: [
      [b[0], b[1]],
      [b[2], b[3]],
    ],
    fitBoundsOptions: { padding: 32 },
    attributionControl: { compact: true },
  })
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right')
  map.on('load', () => {
    if (!map) return
    map.addSource('bbox', { type: 'geojson', data: bboxPolygon(props.modelValue) })
    map.addLayer({ id: 'bbox-fill', type: 'fill', source: 'bbox', paint: { 'fill-color': '#6366f1', 'fill-opacity': 0.12 } })
    map.addLayer({ id: 'bbox-line', type: 'line', source: 'bbox', paint: { 'line-color': '#6366f1', 'line-width': 2 } })

    const b0 = props.modelValue
    swMarker = new maplibregl.Marker({ draggable: true, color: '#6366f1' }).setLngLat([b0[0], b0[1]]).addTo(map)
    neMarker = new maplibregl.Marker({ draggable: true, color: '#6366f1' }).setLngLat([b0[2], b0[3]]).addTo(map)
    for (const m of [swMarker, neMarker]) {
      m.on('drag', () => renderBbox(currentBbox()))
      m.on('dragend', () => commit(currentBbox()))
    }
  })
})

watch(
  () => props.modelValue,
  (b) => {
    if (syncing || !map || !swMarker || !neMarker) return
    swMarker.setLngLat([b[0], b[1]])
    neMarker.setLngLat([b[2], b[3]])
    renderBbox(b)
  },
)

onBeforeUnmount(() => {
  map?.remove()
  map = null
})
</script>

<template>
  <div class="relative overflow-hidden rounded-lg border border-border">
    <div ref="container" class="h-64 w-full" />
    <Button type="button" variant="secondary" size="sm" class="absolute left-2 top-2 z-10 shadow" @click="useCurrentView">
      <Crop class="size-3.5" /> Use current view
    </Button>
  </div>
</template>
