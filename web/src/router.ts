import { createRouter, createWebHistory } from 'vue-router'
import { authRequired, adminKey } from '@/lib/auth'

const router = createRouter({
  history: createWebHistory('/console/'),
  routes: [
    { path: '/', redirect: '/dashboard' },
    { path: '/login', name: 'login', component: () => import('@/views/LoginView.vue'), meta: { public: true } },
    { path: '/dashboard', name: 'dashboard', component: () => import('@/views/DashboardView.vue') },
    { path: '/scripts', name: 'scripts', component: () => import('@/views/ScriptsView.vue') },
    { path: '/jobs', name: 'jobs', component: () => import('@/views/JobsView.vue') },
    { path: '/jobs/:id', name: 'job-detail', component: () => import('@/views/JobDetailView.vue') },
    { path: '/data', name: 'data', component: () => import('@/views/DataView.vue') },
    { path: '/api', name: 'api', component: () => import('@/views/ApiTesterView.vue') },
    { path: '/:pathMatch(.*)*', redirect: '/dashboard' },
  ],
})

router.beforeEach((to) => {
  if (to.meta.public) return true
  if (authRequired.value && !adminKey.value) {
    return { name: 'login', query: to.fullPath !== '/dashboard' ? { redirect: to.fullPath } : {} }
  }
  return true
})

export default router
