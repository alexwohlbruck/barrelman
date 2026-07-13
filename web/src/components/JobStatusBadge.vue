<script setup lang="ts">
import Badge from '@/components/ui/Badge.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { CheckCircle2, XCircle, Ban } from 'lucide-vue-next'
import type { JobStatus } from '@/lib/types'

const props = defineProps<{ status: JobStatus }>()

const map: Record<JobStatus, { variant: any; label: string }> = {
  running: { variant: 'info', label: 'Running' },
  succeeded: { variant: 'success', label: 'Succeeded' },
  failed: { variant: 'destructive', label: 'Failed' },
  canceled: { variant: 'muted', label: 'Canceled' },
}
</script>

<template>
  <Badge :variant="map[props.status].variant">
    <Spinner v-if="status === 'running'" class="size-3" />
    <CheckCircle2 v-else-if="status === 'succeeded'" class="size-3" />
    <XCircle v-else-if="status === 'failed'" class="size-3" />
    <Ban v-else class="size-3" />
    {{ map[props.status].label }}
  </Badge>
</template>
