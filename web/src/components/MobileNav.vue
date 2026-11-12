<script setup lang="ts">
/**
 * The sidebar doesn't fit beside the page on a phone, so below `md` it moves
 * into a drawer opened from a slim top bar. Same AppSidebar inside, so the two
 * layouts can never drift apart.
 */
import { ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { DialogRoot, DialogPortal, DialogOverlay, DialogContent, DialogTitle } from 'reka-ui'
import { Compass, Menu } from 'lucide-vue-next'
import AppSidebar from '@/components/AppSidebar.vue'

const open = ref(false)
const route = useRoute()

// Picking a page is the end of the drawer's job.
watch(() => route.fullPath, () => (open.value = false))
</script>

<template>
  <header class="flex h-14 shrink-0 items-center gap-3 border-b border-border bg-card/40 px-3">
    <button
      class="flex size-10 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground"
      aria-label="Open navigation"
      @click="open = true"
    >
      <Menu class="size-5" />
    </button>
    <div class="flex items-center gap-2">
      <div class="flex size-7 items-center justify-center rounded-md bg-primary text-primary-foreground">
        <Compass class="size-4" />
      </div>
      <span class="text-sm font-semibold">Barrelman</span>
    </div>

    <DialogRoot v-model:open="open">
      <DialogPortal>
        <DialogOverlay
          class="fixed inset-0 z-50 bg-black/40 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0"
        />
        <DialogContent
          class="fixed inset-y-0 left-0 z-50 w-72 max-w-[85vw] bg-background shadow-xl data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:slide-out-to-left data-[state=open]:slide-in-from-left duration-200"
          :aria-describedby="undefined"
        >
          <DialogTitle class="sr-only">Navigation</DialogTitle>
          <AppSidebar />
        </DialogContent>
      </DialogPortal>
    </DialogRoot>
  </header>
</template>
