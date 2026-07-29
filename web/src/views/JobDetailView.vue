<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, computed } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import { ArrowLeft, Ban, RotateCcw, Copy, Clock } from 'lucide-vue-next'
import PageHeader from '@/components/PageHeader.vue'
import LogViewer from '@/components/jobs/LogViewer.vue'
import JobStatusBadge from '@/components/JobStatusBadge.vue'
import DangerBadge from '@/components/DangerBadge.vue'
import Button from '@/components/ui/Button.vue'
import Card from '@/components/ui/Card.vue'
import { getJob, streamJob, cancelJob } from '@/lib/api'
import { refreshJobs } from '@/lib/store'
import { toast } from '@/lib/toast'
import { formatDuration, formatClock, timeAgo } from '@/lib/utils'
import type { Job, LogLine } from '@/lib/types'

const route = useRoute()
const router = useRouter()
const jobId = computed(() => route.params.id as string)

const job = ref<Job | null>(null)
const logs = ref<LogLine[]>([])
const notFound = ref(false)
const canceling = ref(false)
let controller: AbortController | null = null

const isRunning = computed(() => job.value?.status === 'running')

async function start() {
  controller?.abort()
  logs.value = []
  try {
    const initial = await getJob(jobId.value)
    job.value = initial.job
    // Seed with backlog; the stream will also replay it, so track max seq.
  } catch {
    notFound.value = true
    return
  }

  controller = new AbortController()
  const seen = new Set<number>()
  try {
    await streamJob(
      jobId.value,
      (evt) => {
        if (evt.type === 'log') {
          if (!seen.has(evt.line.seq)) {
            seen.add(evt.line.seq)
            logs.value.push(evt.line)
          }
        } else {
          job.value = evt.job
        }
      },
      controller.signal,
    )
  } catch (err) {
    if (!(err instanceof DOMException && err.name === 'AbortError')) {
      // Stream ended or failed; final state already fetched via getJob polling below.
    }
  }
  // Ensure final state is accurate after the stream closes.
  try {
    const final = await getJob(jobId.value)
    job.value = final.job
    if (final.logs.length > logs.value.length) logs.value = final.logs
  } catch {
    /* ignore */
  }
  refreshJobs()
}

async function doCancel() {
  if (!job.value) return
  canceling.value = true
  try {
    const r = await cancelJob(job.value.id)
    toast({
      title: r.ok ? 'Job canceled' : 'Could not cancel',
      description: r.message,
      variant: r.ok ? 'success' : 'warning',
    })
  } finally {
    canceling.value = false
  }
}

function copyLogs() {
  const text = logs.value.map((l) => l.text).join('\n')
  navigator.clipboard.writeText(text)
  toast({ title: 'Logs copied', variant: 'success' })
}

onMounted(start)
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <PageHeader :title="job?.scriptName || 'Job'" :subtitle="job ? `Job ${job.id.slice(0, 8)}` : ''">
    <template #actions>
      <Button variant="outline" size="sm" as="a" @click="router.push('/jobs')">
        <ArrowLeft class="size-4" /> Jobs
      </Button>
      <Button v-if="isRunning" variant="destructive" size="sm" :disabled="canceling" @click="doCancel">
        <Ban class="size-4" /> Cancel
      </Button>
    </template>
  </PageHeader>

  <div class="p-8">
    <div v-if="notFound" class="py-20 text-center text-muted-foreground">
      Job not found. It may have been evicted from history.
      <div class="mt-3"><RouterLink to="/jobs" class="text-foreground underline underline-offset-4">Back to jobs</RouterLink></div>
    </div>

    <template v-else-if="job">
      <!-- Meta -->
      <Card class="mb-4 p-4">
        <div class="flex flex-wrap items-center gap-x-6 gap-y-3">
          <JobStatusBadge :status="job.status" />
          <DangerBadge :danger="job.danger" />
          <div class="flex items-center gap-1.5 text-sm text-muted-foreground">
            <Clock class="size-4" /> {{ formatDuration(job.durationMs) }}
            <span v-if="isRunning" class="text-info">· live</span>
          </div>
          <div class="text-sm text-muted-foreground">Started {{ timeAgo(job.startedAt) }} · {{ formatClock(job.startedAt) }}</div>
          <div v-if="job.exitCode !== undefined && job.exitCode !== null" class="text-sm text-muted-foreground">
            Exit code <code class="rounded bg-muted px-1.5 py-0.5 font-mono">{{ job.exitCode }}</code>
          </div>
          <div class="ml-auto flex items-center gap-2">
            <Button variant="ghost" size="sm" @click="copyLogs"><Copy class="size-3.5" /> Copy logs</Button>
            <Button v-if="!isRunning" variant="ghost" size="sm" @click="start"><RotateCcw class="size-3.5" /> Reload</Button>
          </div>
        </div>
        <div class="mt-3 rounded-lg border border-border bg-background/60 p-2.5">
          <code class="block whitespace-pre-wrap break-all font-mono text-xs text-muted-foreground">$ {{ job.displayCommand }}</code>
        </div>
        <p v-if="job.error" class="mt-2 text-sm text-destructive">{{ job.error }}</p>
      </Card>

      <!-- Logs -->
      <LogViewer :logs="logs" class="h-[calc(100vh-22rem)] min-h-80" />
    </template>
  </div>
</template>
