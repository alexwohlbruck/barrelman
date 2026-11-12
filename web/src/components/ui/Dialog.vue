<script setup lang="ts">
import { DialogRoot, DialogPortal, DialogOverlay, DialogContent, DialogTitle, DialogDescription, DialogClose } from 'reka-ui'
import { X } from 'lucide-vue-next'
import { cn } from '@/lib/utils'

const props = defineProps<{ open: boolean; title?: string; description?: string; class?: string }>()
const emit = defineEmits<{ 'update:open': [value: boolean] }>()
</script>

<template>
  <DialogRoot :open="open" @update:open="emit('update:open', $event)">
    <DialogPortal>
      <DialogOverlay
        class="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0"
      />
      <DialogContent
        :class="cn(
          // Capped to the viewport so a long form (e.g. a script with many params) scrolls instead of running off-screen
          'fixed left-1/2 top-1/2 z-50 grid w-[calc(100%-2rem)] max-w-lg max-h-[calc(100dvh-2rem)] overflow-y-auto -translate-x-1/2 -translate-y-1/2 gap-4 border border-border bg-card p-6 shadow-lg rounded-xl',
          'data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95',
          props.class,
        )"
        @escape-key-down="emit('update:open', false)"
      >
        <div v-if="title || description" class="flex flex-col gap-1.5">
          <DialogTitle v-if="title" class="text-lg font-semibold leading-none tracking-tight">{{ title }}</DialogTitle>
          <DialogDescription v-if="description" class="text-sm text-muted-foreground">{{ description }}</DialogDescription>
        </div>
        <slot />
        <DialogClose
          class="absolute right-4 top-4 rounded-sm opacity-70 transition-opacity hover:opacity-100 focus:outline-none"
          @click="emit('update:open', false)"
        >
          <X class="size-4" />
          <span class="sr-only">Close</span>
        </DialogClose>
      </DialogContent>
    </DialogPortal>
  </DialogRoot>
</template>
