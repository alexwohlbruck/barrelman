import { ref } from 'vue'

export type ToastVariant = 'default' | 'success' | 'error' | 'warning'

export interface Toast {
  id: number
  title: string
  description?: string
  variant: ToastVariant
}

export const toasts = ref<Toast[]>([])
let counter = 0

export function toast(opts: { title: string; description?: string; variant?: ToastVariant; duration?: number }) {
  const id = ++counter
  toasts.value.push({ id, title: opts.title, description: opts.description, variant: opts.variant || 'default' })
  const duration = opts.duration ?? 5000
  if (duration > 0) setTimeout(() => dismissToast(id), duration)
  return id
}

export function dismissToast(id: number) {
  toasts.value = toasts.value.filter((t) => t.id !== id)
}
