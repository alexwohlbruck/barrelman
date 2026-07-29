<script setup lang="ts">
import { cn } from '@/lib/utils'

defineProps<{
  label: string
  value: string | number
  sub?: string
  approx?: boolean
  class?: string
}>()
</script>

<template>
  <div :class="cn('rounded-xl border border-border bg-card p-4', $props.class)">
    <div class="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-muted-foreground">
      <slot name="icon" />
      {{ label }}
    </div>
    <div class="mt-2 flex items-baseline gap-1.5">
      <span v-if="approx" class="text-lg text-muted-foreground">≈</span>
      <span class="text-2xl font-semibold tabular-nums">{{ value }}</span>
    </div>
    <div v-if="sub" class="mt-1 text-xs text-muted-foreground">{{ sub }}</div>
  </div>
</template>
