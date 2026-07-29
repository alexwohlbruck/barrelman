<script setup lang="ts">
import { RouterLink, useRoute } from 'vue-router'
import { LayoutDashboard, TerminalSquare, ListChecks, Database, FlaskConical, LogOut, Compass, MapPin } from 'lucide-vue-next'
import { jobStats } from '@/lib/store'
import { authRequired, clearKey } from '@/lib/auth'
import Badge from '@/components/ui/Badge.vue'

const route = useRoute()

const nav = [
  { to: '/dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/regions', label: 'Regions', icon: MapPin },
  { to: '/scripts', label: 'Scripts', icon: TerminalSquare },
  { to: '/jobs', label: 'Jobs', icon: ListChecks, badge: 'jobs' as const },
  { to: '/data', label: 'Data', icon: Database },
  { to: '/api', label: 'API Tester', icon: FlaskConical },
]

function isActive(to: string) {
  return route.path === to || (to !== '/dashboard' && route.path.startsWith(to))
}

function logout() {
  clearKey()
  window.location.href = '/console/login'
}
</script>

<template>
  <aside class="flex h-full w-60 shrink-0 flex-col border-r border-border bg-card/40">
    <div class="flex items-center gap-2.5 px-5 py-5">
      <div class="flex size-9 items-center justify-center rounded-lg bg-primary text-primary-foreground">
        <Compass class="size-5" />
      </div>
      <div class="leading-tight">
        <div class="text-sm font-semibold">Barrelman</div>
        <div class="text-xs text-muted-foreground">Admin Console</div>
      </div>
    </div>

    <nav class="flex flex-1 flex-col gap-1 px-3 py-2">
      <RouterLink
        v-for="item in nav"
        :key="item.to"
        :to="item.to"
        :class="[
          'group flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
          isActive(item.to)
            ? 'bg-accent text-accent-foreground'
            : 'text-muted-foreground hover:bg-accent/50 hover:text-foreground',
        ]"
      >
        <component :is="item.icon" class="size-4" />
        <span class="flex-1">{{ item.label }}</span>
        <Badge v-if="item.badge === 'jobs' && jobStats.running > 0" variant="info" class="px-1.5">
          {{ jobStats.running }}
        </Badge>
      </RouterLink>
    </nav>

    <div class="border-t border-border px-3 py-3">
      <button
        v-if="authRequired"
        class="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground"
        @click="logout"
      >
        <LogOut class="size-4" />
        Sign out
      </button>
      <p v-else class="px-3 py-1 text-xs text-muted-foreground">Open (dev) mode — no auth</p>
    </div>
  </aside>
</template>
