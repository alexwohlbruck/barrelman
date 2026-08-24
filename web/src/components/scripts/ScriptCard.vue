<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink } from 'vue-router'
import { Play, Clock, Lock, FileCode2, ChevronRight } from 'lucide-vue-next'
import Button from '@/components/ui/Button.vue'
import Badge from '@/components/ui/Badge.vue'
import Progress from '@/components/ui/Progress.vue'
import Spinner from '@/components/ui/Spinner.vue'
import DangerBadge from '@/components/DangerBadge.vue'
import QueuePosition from '@/components/jobs/QueuePosition.vue'
import { useJobProgress } from '@/lib/job-progress'
import type { ScriptDef, Job } from '@/lib/types'

/** `activeJob` is this script's own queued or running job, if it has one. */
const props = defineProps<{ script: ScriptDef; activeJob?: Job }>()
const emit = defineEmits<{ run: [] }>()

const running = computed(() => (props.activeJob?.status === 'running' ? props.activeJob : undefined))
const progress = useJobProgress(running)

// Only an exclusive script actually refuses a second run. For the rest a second
// run is legitimate — it just waits its turn on the worker — so the button stays
// live and says so.
const blocked = computed(() => props.script.exclusive && Boolean(props.activeJob))
const runLabel = computed(() => {
  if (blocked.value) return props.activeJob?.status === 'queued' ? 'Queued…' : 'Running…'
  return props.activeJob ? 'Run again' : 'Run'
})
</script>

<template>
  <div class="group flex flex-col rounded-xl border border-border bg-card p-4 transition-colors hover:border-muted-foreground/30">
    <div class="flex items-start justify-between gap-2">
      <h3 class="text-sm font-semibold leading-snug">{{ script.name }}</h3>
      <DangerBadge :danger="script.danger" />
    </div>
    <p class="mt-1.5 line-clamp-3 flex-1 text-xs text-muted-foreground">{{ script.description }}</p>

    <div class="mt-3 flex flex-wrap items-center gap-1.5">
      <Badge v-if="script.longRunning" variant="outline" class="text-[10px]">
        <Clock class="size-3" /> Long-running
      </Badge>
      <Badge
        v-if="script.exclusive"
        variant="outline"
        class="text-[10px]"
        title="Only one run of this script at a time. While one is queued or running, a second one is refused rather than started alongside it."
      >
        <Lock class="size-3" /> One at a time
      </Badge>
      <Badge v-if="script.source" variant="muted" class="text-[10px] font-mono">
        <FileCode2 class="size-3" /> {{ script.source }}
      </Badge>
    </div>

    <div class="mt-4 flex items-center gap-2">
      <Button
        size="sm"
        :variant="script.danger === 'destructive' ? 'destructive' : 'default'"
        class="flex-1"
        :disabled="blocked"
        @click="emit('run')"
      >
        <Play class="size-3.5" />
        {{ runLabel }}
      </Button>
    </div>

    <!-- The in-flight run, linked. Without this the card says "Running…" and
         gives no way to reach the logs it is talking about. -->
    <RouterLink
      v-if="activeJob"
      :to="`/jobs/${activeJob.id}`"
      class="mt-3 block rounded-lg border border-border bg-background/60 p-2.5 transition-colors hover:border-muted-foreground/40 hover:bg-accent/40"
    >
      <div class="flex items-center gap-1.5 text-[11px] font-medium">
        <Spinner v-if="running" class="size-3 text-info" />
        <span :class="running ? 'text-info' : 'text-[var(--warning)]'">
          {{ running ? 'Running now' : 'Queued' }}
        </span>
        <span class="ml-auto flex items-center text-muted-foreground">View job <ChevronRight class="size-3" /></span>
      </div>

      <template v-if="progress">
        <Progress
          class="mt-2"
          :value="progress.percent"
          :indeterminate="progress.indeterminate"
          :variant="script.danger === 'destructive' ? 'destructive' : 'default'"
        />
        <div class="mt-1.5 flex items-center justify-between text-[10px] text-muted-foreground">
          <span>
            <template v-if="progress.indeterminate">Starting…</template>
            <template v-else>{{ progress.percent }}%<template v-if="progress.label"> · {{ progress.label }}</template></template>
          </span>
          <span v-if="progress.etaLabel">{{ progress.etaLabel }}</span>
        </div>
      </template>
      <QueuePosition v-else :queue="activeJob.queue" compact class="mt-1.5" />
    </RouterLink>
  </div>
</template>
