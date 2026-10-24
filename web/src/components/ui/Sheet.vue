<script setup lang="ts">
/**
 * A panel that slides in from the right edge. Same primitives as Dialog, so
 * focus trapping and Escape behave identically; the difference is that the
 * page behind stays visible down the left, which suits a detail view opened
 * from a list.
 */
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
        class="fixed inset-0 z-50 bg-black/40 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0"
      />
      <DialogContent
        :class="cn(
          'fixed inset-y-0 right-0 z-50 flex h-full w-full max-w-3xl flex-col border-l border-border bg-card shadow-xl',
          'data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:slide-out-to-right data-[state=open]:slide-in-from-right duration-200',
          props.class,
        )"
        @escape-key-down="emit('update:open', false)"
      >
        <div class="flex items-start justify-between gap-4 border-b border-border px-6 py-4">
          <div class="flex min-w-0 flex-col gap-1">
            <DialogTitle v-if="title" class="truncate text-lg font-semibold leading-tight tracking-tight">{{ title }}</DialogTitle>
            <DialogDescription v-if="description" class="text-sm text-muted-foreground">{{ description }}</DialogDescription>
          </div>
          <DialogClose
            class="mt-0.5 rounded-sm opacity-70 transition-opacity hover:opacity-100 focus:outline-none"
            @click="emit('update:open', false)"
          >
            <X class="size-4" />
            <span class="sr-only">Close</span>
          </DialogClose>
        </div>
        <div class="flex-1 overflow-y-auto px-6 py-5">
          <slot />
        </div>
      </DialogContent>
    </DialogPortal>
  </DialogRoot>
</template>
