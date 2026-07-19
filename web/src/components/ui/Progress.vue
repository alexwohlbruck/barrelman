<script setup lang="ts">
const props = withDefaults(
  defineProps<{
    /** 0–100. Ignored when `indeterminate`. */
    value?: number
    indeterminate?: boolean
    variant?: 'default' | 'destructive'
    size?: 'sm' | 'md'
  }>(),
  { value: 0, indeterminate: false, variant: 'default', size: 'sm' },
)
</script>

<template>
  <div
    class="relative w-full overflow-hidden rounded-full bg-muted"
    :class="props.size === 'md' ? 'h-2' : 'h-1.5'"
  >
    <div
      v-if="indeterminate"
      class="progress-indeterminate absolute inset-y-0 w-1/3 rounded-full"
      :class="variant === 'destructive' ? 'bg-destructive' : 'bg-primary'"
    />
    <div
      v-else
      class="h-full rounded-full transition-[width] duration-700 ease-out"
      :class="variant === 'destructive' ? 'bg-destructive' : 'bg-primary'"
      :style="{ width: `${Math.max(0, Math.min(100, value))}%` }"
    />
  </div>
</template>

<style scoped>
@keyframes progress-slide {
  0% { left: -33%; }
  100% { left: 100%; }
}
.progress-indeterminate {
  animation: progress-slide 1.2s ease-in-out infinite;
}
</style>
