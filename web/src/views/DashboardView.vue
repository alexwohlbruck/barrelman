<script setup lang="ts">
import { onMounted, computed } from 'vue'
import { RouterLink } from 'vue-router'
import { RefreshCw, Database, MapPin, TrainFront, Bike, HardDrive, Activity, ArrowRight } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import StatTile from '@/components/StatTile.vue'
import JobStatusBadge from '@/components/JobStatusBadge.vue'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import Spinner from '@/components/ui/Spinner.vue'
import {
  services, servicesLoading, refreshServices,
  metrics, metricsLoading, refreshMetrics,
  jobs,
} from '@/lib/store'
import { formatNumber, timeAgo, formatDuration } from '@/lib/utils'

const recentJobs = computed(() => jobs.value.slice(0, 5))

function refreshAll() {
  refreshServices()
  refreshMetrics()
}

onMounted(refreshAll)
</script>

<template>
  <PageHeader title="Dashboard" subtitle="Service health and data at a glance">
    <template #actions>
      <Button variant="outline" size="sm" @click="refreshAll" :disabled="servicesLoading || metricsLoading">
        <RefreshCw :class="['size-4', (servicesLoading || metricsLoading) && 'animate-spin']" />
        Refresh
      </Button>
    </template>
  </PageHeader>

  <div class="space-y-8 p-8">
    <!-- Service health -->
    <section>
      <h2 class="mb-3 flex items-center gap-2 text-sm font-medium text-muted-foreground">
        <Activity class="size-4" /> Services
      </h2>
      <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Card v-for="svc in services" :key="svc.key" class="p-4">
          <div class="flex items-start justify-between gap-2">
            <div class="min-w-0">
              <div class="truncate text-sm font-medium">{{ svc.name }}</div>
              <div v-if="svc.url" class="mt-0.5 truncate text-xs text-muted-foreground">{{ svc.url }}</div>
            </div>
            <span
              :class="[
                'mt-1 size-2.5 shrink-0 rounded-full',
                svc.status === 'ok' ? 'bg-[var(--success)] shadow-[0_0_8px_var(--success)]' : 'bg-destructive shadow-[0_0_8px_var(--destructive)]',
              ]"
            />
          </div>
          <div class="mt-3 flex items-center justify-between text-xs">
            <span :class="svc.status === 'ok' ? 'text-[var(--success)]' : 'text-destructive'">
              {{ svc.status === 'ok' ? 'Operational' : 'Unavailable' }}
            </span>
            <span v-if="svc.latencyMs !== undefined" class="text-muted-foreground tabular-nums">{{ svc.latencyMs }} ms</span>
          </div>
          <div v-if="svc.message && svc.status !== 'ok'" class="mt-1 truncate text-xs text-destructive/80" :title="svc.message">
            {{ svc.message }}
          </div>
        </Card>
        <Card v-if="!services.length && servicesLoading" class="col-span-full flex items-center justify-center p-8 text-sm text-muted-foreground">
          <Spinner class="mr-2 size-4" /> Checking services…
        </Card>
      </div>
    </section>

    <!-- Data metrics -->
    <section>
      <h2 class="mb-3 flex items-center gap-2 text-sm font-medium text-muted-foreground">
        <Database class="size-4" /> Data
      </h2>
      <div v-if="metrics" class="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-4">
        <StatTile label="DB size" :value="metrics.database.sizePretty || '—'">
          <template #icon><HardDrive class="size-3.5" /></template>
        </StatTile>
        <StatTile label="Places" :value="formatNumber(metrics.geoPlaces.total)" :approx="metrics.geoPlaces.approx" sub="geo_places rows">
          <template #icon><MapPin class="size-3.5" /></template>
        </StatTile>
        <StatTile
          label="Named places"
          :value="formatNumber(metrics.geoPlaces.named)"
          :approx="metrics.geoPlaces.approx"
        />
        <StatTile
          label="Parent context"
          :value="metrics.geoPlaces.parentContextCoverage !== null ? metrics.geoPlaces.parentContextCoverage + '%' : '—'"
          sub="coverage of named"
        />
        <StatTile label="GTFS feeds" :value="formatNumber(metrics.gtfs.feeds)" :sub="`${formatNumber(metrics.gtfs.feedsWithRt)} with realtime`">
          <template #icon><TrainFront class="size-3.5" /></template>
        </StatTile>
        <StatTile label="GTFS stops" :value="formatNumber(metrics.gtfs.stops)" :sub="`${formatNumber(metrics.gtfs.routes)} routes`" />
        <StatTile label="GBFS systems" :value="formatNumber(metrics.gbfs.systems)" :sub="`${formatNumber(metrics.gbfs.stations)} stations`">
          <template #icon><Bike class="size-3.5" /></template>
        </StatTile>
        <StatTile label="Embeddings" :value="metrics.geoPlaces.embeddingCoverage !== null ? metrics.geoPlaces.embeddingCoverage + '%' : '—'" sub="semantic coverage" />
      </div>
      <Card v-else class="flex items-center justify-center p-8 text-sm text-muted-foreground">
        <Spinner class="mr-2 size-4" /> Loading metrics…
      </Card>
    </section>

    <!-- Recent jobs -->
    <section>
      <div class="mb-3 flex items-center justify-between">
        <h2 class="flex items-center gap-2 text-sm font-medium text-muted-foreground">
          <RefreshCw class="size-4" /> Recent jobs
        </h2>
        <RouterLink to="/jobs" class="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
          View all <ArrowRight class="size-3" />
        </RouterLink>
      </div>
      <Card>
        <div v-if="!recentJobs.length" class="p-8 text-center text-sm text-muted-foreground">
          No jobs run yet. Head to <RouterLink to="/scripts" class="text-foreground underline underline-offset-4">Scripts</RouterLink> to run one.
        </div>
        <RouterLink
          v-for="job in recentJobs"
          :key="job.id"
          :to="`/jobs/${job.id}`"
          class="flex items-center gap-3 border-b border-border px-4 py-3 last:border-0 hover:bg-accent/40"
        >
          <JobStatusBadge :status="job.status" />
          <span class="flex-1 truncate text-sm font-medium">{{ job.scriptName }}</span>
          <span class="hidden text-xs text-muted-foreground sm:inline">{{ formatDuration(job.durationMs) }}</span>
          <span class="text-xs text-muted-foreground">{{ timeAgo(job.startedAt) }}</span>
        </RouterLink>
      </Card>
    </section>
  </div>
</template>
