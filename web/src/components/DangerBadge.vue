<script setup lang="ts">
import Badge from '@/components/ui/Badge.vue'
import { ShieldCheck, AlertTriangle, Flame } from 'lucide-vue-next'
import type { DangerLevel } from '@/lib/types'

const props = defineProps<{ danger: DangerLevel }>()

const map: Record<DangerLevel, { variant: any; label: string }> = {
  safe: { variant: 'success', label: 'Safe' },
  caution: { variant: 'warning', label: 'Caution' },
  destructive: { variant: 'destructive', label: 'Destructive' },
}
</script>

<template>
  <Badge :variant="map[props.danger].variant">
    <ShieldCheck v-if="danger === 'safe'" class="size-3" />
    <AlertTriangle v-else-if="danger === 'caution'" class="size-3" />
    <Flame v-else class="size-3" />
    {{ map[props.danger].label }}
  </Badge>
</template>
