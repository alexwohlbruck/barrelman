<script setup lang="ts">
import { computed, watch, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import AppSidebar from '@/components/AppSidebar.vue'
import Toaster from '@/components/ui/Toaster.vue'
import { isAuthenticated } from '@/lib/auth'
import { startJobPolling, stopJobPolling } from '@/lib/store'

const route = useRoute()
const showShell = computed(() => route.meta.public !== true && isAuthenticated())

function syncPolling() {
  if (showShell.value) startJobPolling()
  else stopJobPolling()
}

onMounted(syncPolling)
watch(showShell, syncPolling)
</script>

<template>
  <div v-if="showShell" class="flex h-screen overflow-hidden">
    <AppSidebar />
    <main class="flex-1 overflow-y-auto app-grid-bg">
      <RouterView />
    </main>
  </div>
  <RouterView v-else />
  <Toaster />
</template>
