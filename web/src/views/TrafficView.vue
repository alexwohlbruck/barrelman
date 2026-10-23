<script setup lang="ts">
/**
 * How the metered API is answering: who is being rate-limited and by which
 * layer, and how fast each endpoint group responds. Rows open the same
 * account pane as the Accounts page.
 */
import { ref } from 'vue'
import PageHeader from '@/components/PageHeader.vue'
import UserDetailPane from '@/components/UserDetailPane.vue'
import TrafficPanel from '@/components/traffic/TrafficPanel.vue'

const openAccount = ref<string | null>(null)
/** Closing the pane may follow a key change, so the tables re-read. */
const refresh = ref(0)

function close() {
  openAccount.value = null
  refresh.value += 1
}
</script>

<template>
  <PageHeader title="Traffic" subtitle="Rate limiting, refusals and response times across the metered API" />
  <div class="mx-auto max-w-5xl p-8">
    <TrafficPanel :refresh-key="refresh" @open-account="openAccount = $event" />
  </div>
  <UserDetailPane :user-id="openAccount" @close="close" />
</template>
