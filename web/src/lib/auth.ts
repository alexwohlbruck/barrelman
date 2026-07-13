import { ref } from 'vue'

const STORAGE_KEY = 'barrelman_admin_key'

export const adminKey = ref<string>(localStorage.getItem(STORAGE_KEY) || '')
/** Whether the server requires an admin key at all (false in open dev mode). */
export const authRequired = ref<boolean>(true)

export function setKey(key: string) {
  adminKey.value = key
  localStorage.setItem(STORAGE_KEY, key)
}

export function clearKey() {
  adminKey.value = ''
  localStorage.removeItem(STORAGE_KEY)
}

export function isAuthenticated(): boolean {
  return !authRequired.value || Boolean(adminKey.value)
}
