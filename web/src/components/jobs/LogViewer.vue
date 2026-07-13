<script setup lang="ts">
import { ref, watch, nextTick, onMounted } from 'vue'
import { cn, formatClock } from '@/lib/utils'
import type { LogLine } from '@/lib/types'

const props = defineProps<{ logs: LogLine[]; class?: string }>()

const container = ref<HTMLElement | null>(null)
const stick = ref(true)

function onScroll() {
  const el = container.value
  if (!el) return
  stick.value = el.scrollHeight - el.scrollTop - el.clientHeight < 40
}

async function scrollToBottom() {
  await nextTick()
  const el = container.value
  if (el) el.scrollTop = el.scrollHeight
}

watch(
  () => props.logs.length,
  () => {
    if (stick.value) scrollToBottom()
  },
)

onMounted(scrollToBottom)

const streamClass: Record<string, string> = {
  stdout: 'text-foreground/90',
  stderr: 'text-destructive',
  system: 'text-info',
}
</script>

<template>
  <div
    ref="container"
    @scroll="onScroll"
    :class="cn('overflow-auto rounded-lg border border-border bg-[#0b0b0c] p-3 font-mono text-xs leading-relaxed', props.class)"
  >
    <div v-if="!logs.length" class="py-6 text-center text-muted-foreground">No output yet…</div>
    <div v-for="line in logs" :key="line.seq" class="flex gap-3 whitespace-pre-wrap break-all">
      <span class="shrink-0 select-none text-muted-foreground/50">{{ formatClock(line.t) }}</span>
      <span :class="cn('flex-1', streamClass[line.stream] || 'text-foreground/90')">{{ line.text }}</span>
    </div>
    <div v-if="!stick && logs.length" class="pointer-events-none sticky bottom-0 flex justify-center">
      <button
        class="pointer-events-auto rounded-full border border-border bg-card px-3 py-1 text-[11px] text-muted-foreground shadow hover:text-foreground"
        @click="() => { stick = true; scrollToBottom() }"
      >
        ↓ Jump to latest
      </button>
    </div>
  </div>
</template>
