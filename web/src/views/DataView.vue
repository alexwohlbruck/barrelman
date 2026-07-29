<script setup lang="ts">
import { onMounted, h, type Component } from 'vue'
import { RouterLink } from 'vue-router'
import { RefreshCw, Database, MapPin, TrainFront, Bike, Layers } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import Card from '@/components/ui/Card.vue'
import CardHeader from '@/components/ui/CardHeader.vue'
import CardTitle from '@/components/ui/CardTitle.vue'
import CardContent from '@/components/ui/CardContent.vue'
import Button from '@/components/ui/Button.vue'
import Badge from '@/components/ui/Badge.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { metrics, metricsLoading, metricsError, refreshMetrics } from '@/lib/store'
import { formatNumber, timeAgo } from '@/lib/utils'

onMounted(() => {
  if (!metrics.value) refreshMetrics()
})

// A small inline row renderer to keep the template tidy.
const Row = (props: { label: string; value: string; hint?: string }) =>
  h('div', { class: 'flex items-center justify-between gap-4 border-b border-border/60 py-2 last:border-0' }, [
    h('span', { class: 'text-sm text-muted-foreground' }, props.label),
    h('span', { class: 'flex items-baseline gap-1.5' }, [
      h('span', { class: 'text-sm font-medium tabular-nums' }, props.value),
      props.hint ? h('span', { class: 'text-xs text-muted-foreground' }, props.hint) : null,
    ]),
  ]) as unknown as Component
</script>

<template>
  <PageHeader title="Data" subtitle="Table sizes, coverage, and freshness">
    <template #actions>
      <Button variant="outline" size="sm" @click="refreshMetrics" :disabled="metricsLoading">
        <RefreshCw :class="['size-4', metricsLoading && 'animate-spin']" /> Refresh
      </Button>
    </template>
  </PageHeader>

  <div class="p-8">
    <div v-if="!metrics && metricsLoading" class="flex items-center justify-center py-20 text-muted-foreground">
      <Spinner class="mr-2 size-5" /> Loading metrics…
    </div>
    <div v-else-if="metricsError" class="py-20 text-center text-destructive">{{ metricsError }}</div>

    <div v-else-if="metrics" class="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <!-- geo_places -->
      <Card class="lg:col-span-2">
        <CardHeader class="flex-row items-center justify-between">
          <CardTitle class="flex items-center gap-2"><MapPin class="size-4" /> geo_places (OSM)</CardTitle>
          <Badge v-if="metrics.geoPlaces.approx" variant="muted" class="text-[10px]">≈ sampled estimates</Badge>
        </CardHeader>
        <CardContent class="grid gap-x-10 gap-y-0 sm:grid-cols-2">
          <Row label="Total rows" :value="formatNumber(metrics.geoPlaces.total)" />
          <Row label="Named places" :value="formatNumber(metrics.geoPlaces.named)" />
          <Row label="Intersections" :value="formatNumber(metrics.geoPlaces.intersections)" />
          <Row label="With codes" :value="formatNumber(metrics.geoPlaces.withCodes)" />
          <Row
            label="Parent context"
            :value="formatNumber(metrics.geoPlaces.withParentContext)"
            :hint="metrics.geoPlaces.parentContextCoverage !== null ? metrics.geoPlaces.parentContextCoverage + '%' : undefined"
          />
          <Row
            label="Embeddings"
            :value="formatNumber(metrics.geoPlaces.withEmbedding)"
            :hint="metrics.geoPlaces.embeddingCoverage !== null ? metrics.geoPlaces.embeddingCoverage + '%' : undefined"
          />
        </CardContent>
      </Card>

      <!-- GTFS -->
      <Card>
        <CardHeader>
          <CardTitle class="flex items-center gap-2"><TrainFront class="size-4" /> GTFS / Transit</CardTitle>
        </CardHeader>
        <CardContent>
          <Row label="Feeds" :value="formatNumber(metrics.gtfs.feeds)" :hint="`${formatNumber(metrics.gtfs.feedsWithRt)} realtime`" />
          <Row label="Stops" :value="formatNumber(metrics.gtfs.stops)" />
          <Row label="Routes" :value="formatNumber(metrics.gtfs.routes)" />
          <Row label="Transfers" :value="formatNumber(metrics.gtfs.transfers)" />
          <Row label="Trip patterns" :value="formatNumber(metrics.gtfs.tripPatterns)" />
          <Row label="Shapes" :value="formatNumber(metrics.gtfs.shapes)" />
          <Row label="Stop-area members" :value="formatNumber(metrics.transit.stopAreaMembers)" />
          <Row label="Last feed import" :value="metrics.gtfs.lastImport ? timeAgo(metrics.gtfs.lastImport) : '—'" />
        </CardContent>
      </Card>

      <!-- GBFS + DB -->
      <div class="flex flex-col gap-4">
        <Card>
          <CardHeader>
            <CardTitle class="flex items-center gap-2"><Bike class="size-4" /> GBFS / Micromobility</CardTitle>
          </CardHeader>
          <CardContent>
            <Row label="Systems" :value="formatNumber(metrics.gbfs.systems)" />
            <Row label="Stations" :value="formatNumber(metrics.gbfs.stations)" />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle class="flex items-center gap-2"><Database class="size-4" /> Database</CardTitle>
          </CardHeader>
          <CardContent>
            <Row label="Total size" :value="metrics.database.sizePretty || '—'" />
          </CardContent>
        </Card>
      </div>

      <!-- Quick actions -->
      <Card class="lg:col-span-2">
        <CardHeader>
          <CardTitle class="flex items-center gap-2"><Layers class="size-4" /> Related tasks</CardTitle>
        </CardHeader>
        <CardContent class="flex flex-wrap gap-2">
          <RouterLink to="/scripts">
            <Button variant="outline" size="sm">Enrichment & migration →</Button>
          </RouterLink>
          <RouterLink to="/scripts">
            <Button variant="outline" size="sm">Imports →</Button>
          </RouterLink>
        </CardContent>
      </Card>
    </div>
  </div>
</template>
