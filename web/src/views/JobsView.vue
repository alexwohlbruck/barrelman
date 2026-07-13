<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { RouterLink } from 'vue-router'
import { RefreshCw, ChevronRight, Inbox } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import JobStatusBadge from '@/components/JobStatusBadge.vue'
import Card from '@/components/ui/Card.vue'
import Button from '@/components/ui/Button.vue'
import { jobs, jobStats, refreshJobs } from '@/lib/store'
import { formatDuration, timeAgo, formatClock } from '@/lib/utils'
import type { JobStatus } from '@/lib/types'

const filter = ref<JobStatus | 'all'>('all')

const filters: { value: JobStatus | 'all'; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'running', label: 'Running' },
  { value: 'succeeded', label: 'Succeeded' },
  { value: 'failed', label: 'Failed' },
  { value: 'canceled', label: 'Canceled' },
]

const filtered = computed(() =>
  filter.value === 'all' ? jobs.value : jobs.value.filter((j) => j.status === filter.value),
)

onMounted(refreshJobs)
</script>

<template>
  <PageHeader title="Jobs" subtitle="Execution history and live runs">
    <template #actions>
      <Button variant="outline" size="sm" @click="refreshJobs"><RefreshCw class="size-4" /> Refresh</Button>
    </template>
  </PageHeader>

  <div class="p-8">
    <!-- Stat strip -->
    <div class="mb-5 grid grid-cols-2 gap-3 sm:grid-cols-4">
      <div class="rounded-lg border border-border bg-card px-4 py-3">
        <div class="text-xs text-muted-foreground">Total</div>
        <div class="text-xl font-semibold tabular-nums">{{ jobStats.total }}</div>
      </div>
      <div class="rounded-lg border border-border bg-card px-4 py-3">
        <div class="text-xs text-muted-foreground">Running</div>
        <div class="text-xl font-semibold tabular-nums text-info">{{ jobStats.running }}</div>
      </div>
      <div class="rounded-lg border border-border bg-card px-4 py-3">
        <div class="text-xs text-muted-foreground">Succeeded</div>
        <div class="text-xl font-semibold tabular-nums text-[var(--success)]">{{ jobStats.succeeded }}</div>
      </div>
      <div class="rounded-lg border border-border bg-card px-4 py-3">
        <div class="text-xs text-muted-foreground">Failed</div>
        <div class="text-xl font-semibold tabular-nums text-destructive">{{ jobStats.failed }}</div>
      </div>
    </div>

    <!-- Filter -->
    <div class="mb-4 flex flex-wrap gap-2">
      <button
        v-for="f in filters"
        :key="f.value"
        :class="[
          'rounded-full border px-3 py-1 text-xs font-medium transition-colors',
          filter === f.value ? 'border-primary bg-primary text-primary-foreground' : 'border-border text-muted-foreground hover:text-foreground',
        ]"
        @click="filter = f.value"
      >
        {{ f.label }}
      </button>
    </div>

    <!-- List -->
    <Card>
      <div v-if="!filtered.length" class="flex flex-col items-center gap-2 py-16 text-muted-foreground">
        <Inbox class="size-8 opacity-40" />
        <span class="text-sm">No {{ filter === 'all' ? '' : filter }} jobs</span>
      </div>
      <RouterLink
        v-for="job in filtered"
        :key="job.id"
        :to="`/jobs/${job.id}`"
        class="flex items-center gap-4 border-b border-border px-4 py-3 transition-colors last:border-0 hover:bg-accent/40"
      >
        <JobStatusBadge :status="job.status" />
        <div class="min-w-0 flex-1">
          <div class="truncate text-sm font-medium">{{ job.scriptName }}</div>
          <div class="truncate font-mono text-xs text-muted-foreground">{{ job.displayCommand }}</div>
        </div>
        <div class="hidden shrink-0 text-right sm:block">
          <div class="text-xs text-muted-foreground">{{ formatDuration(job.durationMs) }}</div>
          <div class="text-xs text-muted-foreground/70">{{ formatClock(job.startedAt) }}</div>
        </div>
        <div class="w-16 shrink-0 text-right text-xs text-muted-foreground">{{ timeAgo(job.startedAt) }}</div>
        <ChevronRight class="size-4 shrink-0 text-muted-foreground" />
      </RouterLink>
    </Card>
  </div>
</template>
