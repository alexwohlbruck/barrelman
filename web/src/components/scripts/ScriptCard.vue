<script setup lang="ts">
import { toRef } from 'vue'
import { Play, Clock, Lock, FileCode2 } from 'lucide-vue-next'
import Button from '@/components/ui/Button.vue'
import Badge from '@/components/ui/Badge.vue'
import Progress from '@/components/ui/Progress.vue'
import DangerBadge from '@/components/DangerBadge.vue'
import { useJobProgress } from '@/lib/job-progress'
import type { ScriptDef, Job } from '@/lib/types'

const props = defineProps<{ script: ScriptDef; runningJob?: Job }>()
const emit = defineEmits<{ run: [] }>()
const progress = useJobProgress(toRef(props, 'runningJob'))
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
      <Badge v-if="script.exclusive" variant="outline" class="text-[10px]">
        <Lock class="size-3" /> Exclusive
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
        :disabled="Boolean(runningJob)"
        @click="emit('run')"
      >
        <Play class="size-3.5" />
        {{ runningJob ? 'Running…' : 'Run' }}
      </Button>
    </div>

    <div v-if="progress" class="mt-3">
      <Progress
        :value="progress.percent"
        :indeterminate="progress.indeterminate"
        :variant="script.danger === 'destructive' ? 'destructive' : 'default'"
      />
      <div class="mt-1.5 flex items-center justify-between text-[10px] text-muted-foreground">
        <span>
          <template v-if="progress.indeterminate">Running…</template>
          <template v-else>{{ progress.percent }}%<template v-if="progress.label"> · {{ progress.label }}</template></template>
        </span>
        <span v-if="progress.etaLabel">{{ progress.etaLabel }}</span>
      </div>
    </div>
  </div>
</template>
