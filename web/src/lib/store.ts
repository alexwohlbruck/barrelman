import { ref } from 'vue'
import * as api from './api'
import type { Job, JobStats, DataMetrics, ServiceStatus } from './types'

// ── Jobs (globally polled) ────────────────────────────────────────────
export const jobs = ref<Job[]>([])
export const jobStats = ref<JobStats>({ total: 0, running: 0, succeeded: 0, failed: 0 })
export const jobsError = ref<string | null>(null)

export async function refreshJobs() {
  try {
    const r = await api.getJobs()
    jobs.value = r.jobs
    jobStats.value = r.stats
    jobsError.value = null
  } catch (err) {
    jobsError.value = err instanceof Error ? err.message : 'Failed to load jobs'
  }
}

// ── Services (on-demand) ──────────────────────────────────────────────
export const services = ref<ServiceStatus[]>([])
export const servicesLoading = ref(false)
export const servicesError = ref<string | null>(null)

export async function refreshServices() {
  servicesLoading.value = true
  try {
    const r = await api.getServices()
    services.value = r.services
    servicesError.value = null
  } catch (err) {
    servicesError.value = err instanceof Error ? err.message : 'Failed to load services'
  } finally {
    servicesLoading.value = false
  }
}

// ── Metrics (on-demand) ───────────────────────────────────────────────
export const metrics = ref<DataMetrics | null>(null)
export const metricsLoading = ref(false)
export const metricsError = ref<string | null>(null)

export async function refreshMetrics() {
  metricsLoading.value = true
  try {
    metrics.value = await api.getMetrics()
    metricsError.value = null
  } catch (err) {
    metricsError.value = err instanceof Error ? err.message : 'Failed to load metrics'
  } finally {
    metricsLoading.value = false
  }
}

// ── Global jobs poller ────────────────────────────────────────────────
let pollTimer: number | null = null

export function startJobPolling() {
  if (pollTimer !== null) return
  const tick = async () => {
    await refreshJobs()
    // Poll faster while something is running, slower when idle.
    const delay = jobStats.value.running > 0 ? 2000 : 6000
    pollTimer = window.setTimeout(tick, delay)
  }
  tick()
}

export function stopJobPolling() {
  if (pollTimer !== null) {
    clearTimeout(pollTimer)
    pollTimer = null
  }
}
