<script setup lang="ts">
/**
 * Shown to an administrator whose session the admin API refuses, so the
 * operator views don't just vanish without a reason. The text comes from the
 * server; the link takes them to the account page, where passkeys are added.
 */
import { computed } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { ShieldAlert } from 'lucide-vue-next'
import { adminAccess, adminBlocked } from '@/lib/auth'

const route = useRoute()
const onAccountPage = computed(() => route.path === '/account')
const needsPasskey = computed(() => adminAccess.value?.code === 'passkey_required')
</script>

<template>
  <div
    v-if="adminBlocked"
    class="flex items-start gap-3 border-b border-warning/30 bg-warning/10 px-8 py-3 text-sm"
    role="status"
  >
    <ShieldAlert class="mt-0.5 size-4 shrink-0 text-warning" />
    <div class="space-y-1">
      <p class="font-medium">Admin tools are unavailable in this session</p>
      <p class="text-muted-foreground">{{ adminAccess?.error }}</p>
      <RouterLink
        v-if="needsPasskey && !onAccountPage"
        to="/account"
        class="inline-block font-medium text-foreground underline underline-offset-4"
      >
        Go to your account page
      </RouterLink>
    </div>
  </div>
</template>
