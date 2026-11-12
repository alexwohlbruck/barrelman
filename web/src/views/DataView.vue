<script setup lang="ts">
import { onMounted, h, ref, computed, type Component } from 'vue'
import { RouterLink } from 'vue-router'
import { RefreshCw, Database, MapPin, TrainFront, Bike, Layers, Map, Users, Scale } from 'lucide-vue-next'
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
import { getGtfsFeeds } from '@/lib/api'
import type { GtfsFeedSummary } from '@/lib/types'

const feeds = ref<GtfsFeedSummary[] | null>(null)
const feedsError = ref<string | null>(null)
const onlyRestricted = ref(false)

async function loadFeeds() {
  try {
    feeds.value = (await getGtfsFeeds()).feeds
    feedsError.value = null
  } catch (err) {
    feedsError.value = err instanceof Error ? err.message : String(err)
  }
}

function refresh() {
  refreshMetrics()
  loadFeeds()
}

onMounted(() => {
  if (!metrics.value) refreshMetrics()
  loadFeeds()
})

const restricts = (f: GtfsFeedSummary) =>
  f.license.commercialUseAllowed === 'no' || f.license.redistributionAllowed === 'no'

/** Restricted feeds first, then those with any terms on record, then the rest. */
const sortedFeeds = computed(() => {
  const rank = (f: GtfsFeedSummary) => (restricts(f) ? 0 : f.license.spdx || f.license.url ? 1 : 2)
  return [...(feeds.value ?? [])]
    .filter((f) => !onlyRestricted.value || restricts(f))
    .sort((a, b) => rank(a) - rank(b) || (a.name ?? a.feedId).localeCompare(b.name ?? b.feedId))
})

const answerVariant = (a: string | null) => (a === 'no' ? 'destructive' : a === 'yes' ? 'success' : 'muted')

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
      <Button variant="outline" size="sm" @click="refresh" :disabled="metricsLoading">
        <RefreshCw :class="['size-4', metricsLoading && 'animate-spin']" /> Refresh
      </Button>
    </template>
  </PageHeader>

  <div class="p-4 md:p-8">
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
          <Row label="Stop → OSM links" :value="formatNumber(metrics.transit.portolanStopLinks)" />
          <Row
            label="Feeds forbidding commercial use"
            :value="formatNumber(metrics.gtfs.feedsForbiddingCommercial)"
            :hint="metrics.gtfs.licensePolicy === 'commercial' ? 'withheld' : 'served (policy off)'"
          />
          <Row label="Feeds with licence on record" :value="formatNumber(metrics.gtfs.feedsWithLicense)" />
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
            <CardTitle class="flex items-center gap-2"><Map class="size-4" /> Boundary catalog</CardTitle>
          </CardHeader>
          <CardContent>
            <Row label="Regions available" :value="formatNumber(metrics.boundaries.count)" />
            <Row
              label="Last fetched"
              :value="metrics.boundaries.fetchedAt ? timeAgo(metrics.boundaries.fetchedAt) : 'never'"
              :hint="metrics.boundaries.count ? undefined : 'Run “Fetch Boundary Catalog”'"
            />
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

      <!-- GTFS feed licences -->
      <Card class="lg:col-span-2">
        <CardHeader class="flex-row items-center justify-between">
          <CardTitle class="flex items-center gap-2"><Scale class="size-4" /> Feed licences</CardTitle>
          <label class="flex items-center gap-2 text-xs text-muted-foreground">
            <input v-model="onlyRestricted" type="checkbox" class="accent-current" />
            Only feeds that restrict use
          </label>
        </CardHeader>
        <CardContent>
          <div v-if="feedsError" class="text-sm text-destructive">{{ feedsError }}</div>
          <div v-else-if="!feeds" class="flex items-center text-sm text-muted-foreground">
            <Spinner class="mr-2 size-4" /> Loading feeds…
          </div>
          <div v-else-if="!sortedFeeds.length" class="text-sm text-muted-foreground">
            {{ feeds.length ? 'No feed restricts commercial use or redistribution.' : 'No GTFS feeds imported.' }}
          </div>
          <div v-else class="max-h-[28rem] overflow-auto">
            <table class="w-full text-sm">
              <thead class="sticky top-0 bg-card text-xs text-muted-foreground">
                <tr class="border-b border-border">
                  <th class="pb-2 text-left font-medium">Feed</th>
                  <th class="pb-2 text-left font-medium">Licence</th>
                  <th class="pb-2 text-left font-medium">Commercial</th>
                  <th class="pb-2 text-left font-medium">Redistribution</th>
                  <th class="pb-2 text-right font-medium">Stops</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="f in sortedFeeds" :key="f.feedId" class="border-b border-border/50 last:border-0">
                  <td class="py-1.5 pr-4">
                    <div class="flex items-center gap-1.5">
                      <span>{{ f.name || f.feedId }}</span>
                      <Badge v-if="f.withheld" variant="warning">withheld</Badge>
                    </div>
                    <div class="text-xs text-muted-foreground">{{ f.onestopId || f.feedId }}</div>
                  </td>
                  <td class="py-1.5 pr-4">
                    <a
                      v-if="f.license.url"
                      :href="f.license.url"
                      target="_blank"
                      rel="noopener"
                      class="underline-offset-2 hover:underline"
                    >{{ f.license.spdx || 'Licence' }}</a>
                    <span v-else-if="f.license.spdx">{{ f.license.spdx }}</span>
                    <span v-else class="text-muted-foreground">None on record</span>
                  </td>
                  <td class="py-1.5 pr-4">
                    <Badge :variant="answerVariant(f.license.commercialUseAllowed)">{{ f.license.commercialUseAllowed || 'unknown' }}</Badge>
                  </td>
                  <td class="py-1.5 pr-4">
                    <Badge :variant="answerVariant(f.license.redistributionAllowed)">{{ f.license.redistributionAllowed || 'unknown' }}</Badge>
                  </td>
                  <td class="py-1.5 text-right tabular-nums">{{ formatNumber(f.stopCount) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <p class="mt-3 text-xs text-muted-foreground">
            Terms as Transitland records them. Run “Refresh Feed Licences” to re-read them.
          </p>
        </CardContent>
      </Card>

      <!-- Public API accounts -->
      <Card class="lg:col-span-2">
        <CardHeader>
          <CardTitle class="flex items-center gap-2"><Users class="size-4" /> Public API accounts</CardTitle>
        </CardHeader>
        <CardContent class="grid gap-x-10 gap-y-0 sm:grid-cols-2">
          <Row label="Accounts" :value="formatNumber(metrics.accounts.users)" />
          <Row label="Active API keys" :value="formatNumber(metrics.accounts.activeKeys)" />
          <Row label="Active this cycle" :value="formatNumber(metrics.accounts.activeThisCycle)" />
          <Row label="Paid accounts" :value="formatNumber(metrics.accounts.paidAccounts)" />
          <Row label="Credits billed this cycle" :value="formatNumber(metrics.accounts.creditsThisCycle)" />
          <Row label="Requests refused this cycle" :value="formatNumber(metrics.accounts.rejectedThisCycle)" />
        </CardContent>
      </Card>

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
