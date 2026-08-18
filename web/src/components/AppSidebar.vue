<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import {
  Activity,
  CalendarClock,
  Compass,
  CreditCard,
  Database,
  FlaskConical,
  KeyRound,
  LayoutDashboard,
  ListChecks,
  LogOut,
  MapPin,
  TerminalSquare,
  UserRound,
  Users,
} from 'lucide-vue-next'
import { jobStats } from '@/lib/store'
import { isAdmin, signOut, user } from '@/lib/auth'
import Badge from '@/components/ui/Badge.vue'

const route = useRoute()
const router = useRouter()

/** Everything a signed-in developer needs; this is the primary surface now. */
const accountNav = [
  { to: '/keys', label: 'API keys', icon: KeyRound },
  { to: '/usage', label: 'Usage', icon: Activity },
  { to: '/billing', label: 'Billing', icon: CreditCard },
  { to: '/account', label: 'Account', icon: UserRound },
]

/** Operator tools, shown only to admins. */
const adminNav = [
  { to: '/dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/accounts', label: 'Accounts', icon: Users },
  { to: '/regions', label: 'Regions', icon: MapPin },
  { to: '/scripts', label: 'Scripts', icon: TerminalSquare },
  { to: '/schedules', label: 'Schedules', icon: CalendarClock },
  { to: '/jobs', label: 'Jobs', icon: ListChecks, badge: 'jobs' as const },
  { to: '/data', label: 'Data', icon: Database },
  { to: '/api', label: 'API Tester', icon: FlaskConical },
]

const showAdmin = computed(() => isAdmin.value)
const identity = computed(() => user.value?.name || user.value?.email || '')

function isActive(to: string) {
  return route.path === to || route.path.startsWith(`${to}/`)
}

async function logout() {
  await signOut()
  router.replace({ name: 'login' })
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
        <div class="text-xs text-muted-foreground">Geospatial API</div>
      </div>
    </div>

    <nav class="flex flex-1 flex-col gap-1 overflow-y-auto px-3 py-2">
      <RouterLink
        v-for="item in accountNav"
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
      </RouterLink>

      <template v-if="showAdmin">
        <div class="mt-4 px-3 pb-1 text-xs font-medium text-muted-foreground/70">Operations</div>
        <RouterLink
          v-for="item in adminNav"
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
      </template>
    </nav>

    <div class="border-t border-border px-3 py-3">
      <div class="truncate px-3 pb-2 text-xs text-muted-foreground" :title="identity">{{ identity }}</div>
      <button
        class="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground"
        @click="logout"
      >
        <LogOut class="size-4" />
        Sign out
      </button>
    </div>
  </aside>
</template>
