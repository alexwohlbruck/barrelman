<script setup lang="ts">
/**
 * Requests per bucket, stacked by outcome: served, rate-limited, otherwise
 * refused. One axis, counts only — latency has its own scale and lives in the
 * tables rather than on a second y-axis here.
 */
import { computed, ref } from 'vue'
import { formatNumber } from '@/lib/utils'
import { formatMs, otherRefused, percent } from '@/lib/traffic'
import type { TrafficPoint } from '@/lib/types'

const props = defineProps<{ series: TrafficPoint[]; bucket: 'hour' | 'day' }>()

const SEGMENTS = [
  { key: 'served', label: 'Served', class: 'bg-primary/75' },
  { key: 'limited', label: 'Rate-limited', class: 'bg-[var(--warning)]' },
  { key: 'other', label: 'Refused (quota, auth)', class: 'bg-muted-foreground/50' },
] as const

const bars = computed(() =>
  props.series.map((point) => ({
    point,
    served: point.served,
    limited: point.rateLimited,
    other: otherRefused(point),
  })),
)

const peak = computed(() => Math.max(1, ...props.series.map((p) => p.requests)))
const hovered = ref<number | null>(null)
const active = computed(() => (hovered.value === null ? null : bars.value[hovered.value] ?? null))

function label(t: string, long = false) {
  if (props.bucket === 'hour') {
    const date = new Date(t)
    return long
      ? date.toLocaleString(undefined, { weekday: 'short', hour: 'numeric', minute: '2-digit' })
      : date.toLocaleTimeString(undefined, { hour: 'numeric' })
  }
  const date = new Date(`${t}T00:00:00Z`)
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' })
}

/** A handful of evenly spaced axis labels, not one per bar. */
const tickEvery = computed(() => Math.max(1, Math.ceil(props.series.length / 8)))
</script>

<template>
  <div class="flex flex-col gap-3">
    <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
      <span v-for="segment in SEGMENTS" :key="segment.key" class="flex items-center gap-1.5">
        <span class="size-2.5 rounded-sm" :class="segment.class" />
        {{ segment.label }}
      </span>
    </div>

    <div class="relative">
      <div class="flex h-40 items-end gap-[2px]" @mouseleave="hovered = null">
        <div
          v-for="(bar, i) in bars"
          :key="bar.point.t"
          class="flex h-full flex-1 cursor-default flex-col justify-end"
          @mouseenter="hovered = i"
        >
          <!-- Stacked top to bottom: other, limited, served. 2px surface gaps between fills. -->
          <div
            v-if="bar.other"
            class="w-full rounded-t-[3px] bg-muted-foreground/50"
            :style="{ height: `${(bar.other / peak) * 100}%` }"
          />
          <div
            v-if="bar.limited"
            class="w-full bg-[var(--warning)]"
            :class="[bar.other ? 'mt-[2px]' : 'rounded-t-[3px]']"
            :style="{ height: `${(bar.limited / peak) * 100}%` }"
          />
          <div
            class="w-full transition-opacity"
            :class="[
              bar.other || bar.limited ? 'mt-[2px]' : 'rounded-t-[3px]',
              bar.served ? 'bg-primary/75' : 'bg-border',
              hovered === i ? 'opacity-100' : hovered === null ? 'opacity-100' : 'opacity-60',
            ]"
            :style="{ height: bar.served ? `${(bar.served / peak) * 100}%` : '2px' }"
          />
        </div>
      </div>

      <div
        v-if="active"
        class="pointer-events-none absolute top-0 z-10 w-52 rounded-lg border border-border bg-popover px-3 py-2 text-xs shadow-md"
        :style="{
          left: `${Math.min(Math.max(((hovered! + 0.5) / bars.length) * 100, 15), 85)}%`,
          transform: 'translate(-50%, 0)',
        }"
      >
        <div class="mb-1 font-medium">{{ label(active.point.t, true) }}</div>
        <div class="flex justify-between"><span class="text-muted-foreground">Requests</span><span class="tabular-nums">{{ formatNumber(active.point.requests) }}</span></div>
        <div class="flex justify-between">
          <span class="text-muted-foreground">Rate-limited</span>
          <span class="tabular-nums">{{ formatNumber(active.limited) }} · {{ percent(active.limited, active.point.requests) }}</span>
        </div>
        <div class="flex justify-between"><span class="text-muted-foreground">Other refusals</span><span class="tabular-nums">{{ formatNumber(active.other) }}</span></div>
        <div class="flex justify-between"><span class="text-muted-foreground">p95</span><span class="tabular-nums">{{ formatMs(active.point.p95Ms) }}</span></div>
      </div>
    </div>

    <div class="flex gap-[2px] text-[10px] text-muted-foreground">
      <span v-for="(bar, i) in bars" :key="bar.point.t" class="flex-1 overflow-visible whitespace-nowrap">
        {{ i % tickEvery === 0 ? label(bar.point.t) : '' }}
      </span>
    </div>
  </div>
</template>
