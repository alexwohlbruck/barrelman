<script setup lang="ts">
import { computed } from 'vue'
import { Check, Loader2, X } from 'lucide-vue-next'
import type { Job } from '@/lib/types'

const props = defineProps<{ job: Job }>()

const stages = computed(() => props.job.stages)
const done = computed(() => props.job.status === 'succeeded')
const failed = computed(() => props.job.status === 'failed' || props.job.status === 'canceled')

type StageState = 'done' | 'current' | 'failed' | 'pending'

function stateOf(i: number): StageState {
  const s = stages.value
  if (!s) return 'pending'
  if (done.value) return 'done'
  if (i < s.index - 1) return 'done'
  if (i === s.index - 1) return failed.value ? 'failed' : 'current'
  return 'pending'
}

const anyLabels = computed(() => stages.value?.labels.some((l) => l) ?? false)
</script>

<template>
  <div v-if="stages">
    <!-- current stage headline -->
    <div class="mb-2 text-sm">
      <span class="font-medium text-foreground">Stage {{ stages.index }} of {{ stages.total }}</span>
      <span v-if="stages.labels[stages.index - 1]" class="text-muted-foreground">
        — {{ stages.labels[stages.index - 1] }}
      </span>
    </div>

    <!-- segmented bar: one slot per stage -->
    <div class="flex gap-1">
      <div
        v-for="i in stages.total"
        :key="i"
        class="h-1.5 flex-1 rounded-full transition-colors"
        :class="{
          'bg-primary': stateOf(i - 1) === 'done',
          'bg-primary/70 animate-pulse': stateOf(i - 1) === 'current',
          'bg-destructive': stateOf(i - 1) === 'failed',
          'bg-muted': stateOf(i - 1) === 'pending',
        }"
      />
    </div>

    <!-- labeled checklist, once any stage names are known -->
    <ol v-if="anyLabels" class="mt-3 space-y-1">
      <li
        v-for="i in stages.total"
        :key="i"
        class="flex items-center gap-2 text-xs"
      >
        <Check v-if="stateOf(i - 1) === 'done'" class="size-3.5 shrink-0 text-primary" />
        <Loader2 v-else-if="stateOf(i - 1) === 'current'" class="size-3.5 shrink-0 animate-spin text-primary" />
        <X v-else-if="stateOf(i - 1) === 'failed'" class="size-3.5 shrink-0 text-destructive" />
        <span v-else class="flex size-3.5 shrink-0 items-center justify-center">
          <span class="size-1.5 rounded-full bg-muted-foreground/40" />
        </span>
        <span :class="stateOf(i - 1) === 'pending' ? 'text-muted-foreground' : 'text-foreground'">
          {{ stages.labels[i - 1] || `Stage ${i}` }}
        </span>
      </li>
    </ol>
  </div>
</template>
