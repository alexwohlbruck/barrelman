<script setup lang="ts">
/**
 * Explains why a job hasn't started, and links to the job it is stuck behind.
 *
 * Without this a queued job looks identical to a job that silently failed to
 * start — which is the whole reason an operator hesitates to turn a nightly
 * schedule on.
 *
 * The link is a <button> rather than a <router-link> because this renders inside
 * the jobs list, whose rows are themselves links: a nested <a> gets unnested by
 * the HTML parser and breaks the row.
 */
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { Hourglass } from 'lucide-vue-next'
import type { QueuePlacement } from '@/lib/types'

const props = defineProps<{ queue?: QueuePlacement; compact?: boolean }>()
const router = useRouter()

const ordinal = computed(() => {
  const n = props.queue?.position ?? 0
  if (n === 1) return 'next in line'
  if (n === 2) return '2nd in line'
  if (n === 3) return '3rd in line'
  return `${n}th in line`
})
</script>

<template>
  <div v-if="queue" class="text-muted-foreground" :class="compact ? 'text-xs' : 'text-sm'">
    <Hourglass class="mr-1.5 inline size-3.5 -translate-y-px" />
    <template v-if="queue.waitingOn">
      Waiting on
      <button
        type="button"
        class="font-medium text-foreground underline underline-offset-4 hover:text-primary"
        @click.stop.prevent="router.push(`/jobs/${queue.waitingOn.id}`)"
      >
        {{ queue.waitingOn.scriptName }}
      </button>
      to finish · {{ ordinal }}
    </template>
    <template v-else>{{ ordinal }} — starting shortly</template>
  </div>
</template>
