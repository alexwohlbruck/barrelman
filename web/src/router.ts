import { createRouter, createWebHistory } from 'vue-router'
import { bootstrap, isAdmin, isAuthenticated, ready } from '@/lib/auth'

/**
 * Two tiers of route:
 *
 *   - Account routes (keys, usage, billing, account) — any signed-in user.
 *   - Operator routes (`meta.admin`) — imports, migrations, graph rebuilds.
 *     These are also enforced server-side; the guard here only keeps the nav
 *     honest, since a client-side check is not a security boundary.
 */
const router = createRouter({
  history: createWebHistory('/console/'),
  routes: [
    { path: '/', redirect: '/keys' },
    { path: '/login', name: 'login', component: () => import('@/views/LoginView.vue'), meta: { public: true } },

    // Account
    { path: '/keys', name: 'keys', component: () => import('@/views/ApiKeysView.vue') },
    { path: '/usage', name: 'usage', component: () => import('@/views/UsageView.vue') },
    { path: '/billing', name: 'billing', component: () => import('@/views/BillingView.vue') },
    { path: '/account', name: 'account', component: () => import('@/views/AccountView.vue') },

    // Operator
    { path: '/dashboard', name: 'dashboard', component: () => import('@/views/DashboardView.vue'), meta: { admin: true } },
    { path: '/accounts', name: 'accounts', component: () => import('@/views/UsersView.vue'), meta: { admin: true } },
    { path: '/regions', name: 'regions', component: () => import('@/views/RegionsView.vue'), meta: { admin: true } },
    { path: '/scripts', name: 'scripts', component: () => import('@/views/ScriptsView.vue'), meta: { admin: true } },
    { path: '/jobs', name: 'jobs', component: () => import('@/views/JobsView.vue'), meta: { admin: true } },
    { path: '/jobs/:id', name: 'job-detail', component: () => import('@/views/JobDetailView.vue'), meta: { admin: true } },
    { path: '/data', name: 'data', component: () => import('@/views/DataView.vue'), meta: { admin: true } },
    { path: '/api', name: 'api', component: () => import('@/views/ApiTesterView.vue'), meta: { admin: true } },

    { path: '/:pathMatch(.*)*', redirect: '/keys' },
  ],
})

router.beforeEach(async (to) => {
  // The session lives in an httpOnly cookie, so the only way to know whether we
  // are signed in is to ask. Do it once, before the first guarded navigation.
  if (!ready.value) await bootstrap()

  if (to.meta.public) return true

  if (!isAuthenticated.value) {
    return { name: 'login', query: to.fullPath !== '/keys' ? { redirect: to.fullPath } : {} }
  }

  // A non-admin who lands on an operator route (a bookmark, a stale link) goes
  // to their own keys rather than a 403 they can do nothing about.
  if (to.meta.admin && !isAdmin.value) return { name: 'keys' }

  return true
})

export default router
