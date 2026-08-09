import { createRouter, createWebHistory } from 'vue-router'
import { bootstrap, hasAccount, isAdmin, isAuthenticated, ready } from '@/lib/auth'

/**
 * Two tiers of route:
 *
 *   - Account routes (`meta.account`) — need a real account session. An
 *     operator holding only the shared admin key does not have one, and the
 *     API's /account/* routes would 401 them.
 *   - Operator routes (`meta.admin`) — imports, migrations, graph rebuilds.
 *     These are also enforced server-side; the guard here only keeps the nav
 *     honest, since a client-side check is not a security boundary.
 */
const router = createRouter({
  // Whatever Vite was built with, rather than a second copy of the same
  // literal. The two have to agree — assets resolve against Vite's base and
  // links against this one — and hardcoding it here meant a build for the
  // console subdomain would load its assets from / and then push URLs under
  // /console/. BASE_URL is Vite's own record of the base it used, so they
  // cannot drift apart.
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    // A plain redirect, deliberately: a redirect *function* is evaluated during
    // route resolution, before `bootstrap()` has answered, so it would send a
    // real account holder to the operator dashboard on a cold load. The
    // `meta.account` guard below then bounces an admin-key operator onward.
    { path: '/', redirect: '/keys' },
    { path: '/login', name: 'login', component: () => import('@/views/LoginView.vue'), meta: { public: true } },

    // Account
    { path: '/keys', name: 'keys', component: () => import('@/views/ApiKeysView.vue'), meta: { account: true } },
    { path: '/usage', name: 'usage', component: () => import('@/views/UsageView.vue'), meta: { account: true } },
    { path: '/billing', name: 'billing', component: () => import('@/views/BillingView.vue'), meta: { account: true } },
    { path: '/account', name: 'account', component: () => import('@/views/AccountView.vue'), meta: { account: true } },

    // Operator
    { path: '/dashboard', name: 'dashboard', component: () => import('@/views/DashboardView.vue'), meta: { admin: true } },
    { path: '/accounts', name: 'accounts', component: () => import('@/views/UsersView.vue'), meta: { admin: true } },
    { path: '/regions', name: 'regions', component: () => import('@/views/RegionsView.vue'), meta: { admin: true } },
    { path: '/scripts', name: 'scripts', component: () => import('@/views/ScriptsView.vue'), meta: { admin: true } },
    { path: '/schedules', name: 'schedules', component: () => import('@/views/SchedulesView.vue'), meta: { admin: true } },
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

  // Likewise an admin-key operator on an account route: those pages call
  // session-authenticated endpoints that would 401 and sign them out.
  if (to.meta.account && !hasAccount.value) return { name: 'dashboard' }

  return true
})

export default router
