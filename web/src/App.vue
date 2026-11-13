<script setup lang="ts">
import { computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import AppSidebar from '@/components/AppSidebar.vue'
import MobileNav from '@/components/MobileNav.vue'
import AdminAccessNotice from '@/components/AdminAccessNotice.vue'
import Toaster from '@/components/ui/Toaster.vue'
import Spinner from '@/components/ui/Spinner.vue'
import { isAdmin, isSignedIn, ready } from '@/lib/auth'
import { startJobPolling, stopJobPolling } from '@/lib/store'

const route = useRoute()
const showShell = computed(() => ready.value && route.meta.public !== true && isSignedIn.value)

/**
 * The job list is an operator concern and its endpoint is admin-gated, so
 * polling it as an ordinary developer would just produce a stream of 401s.
 */
function syncPolling() {
  if (showShell.value && isAdmin.value) startJobPolling()
  else stopJobPolling()
}

onMounted(syncPolling)
watch([showShell, isAdmin], syncPolling)
</script>

<template>
  <!-- Wait for the session probe rather than flashing the sign-in screen at a
       user who is already signed in. -->
  <div v-if="!ready" class="flex h-dvh items-center justify-center">
    <Spinner class="size-6" />
  </div>
  <div v-else-if="showShell" class="flex h-dvh overflow-hidden">
    <div class="hidden w-60 shrink-0 md:block"><AppSidebar /></div>
    <div class="flex min-w-0 flex-1 flex-col">
      <MobileNav class="md:hidden" />
      <main class="min-w-0 flex-1 overflow-y-auto app-grid-bg">
        <AdminAccessNotice />
        <RouterView />
      </main>
    </div>
  </div>
  <RouterView v-else />
  <Toaster />
</template>
