import { createApp } from 'vue'
import './style.css'
import App from './App.vue'
import router from './router'
import { getConfig } from './lib/api'
import { authRequired } from './lib/auth'

// Discover whether the server requires an admin key before mounting so the
// router guard behaves correctly on first paint. Failure is non-fatal — we
// default to requiring auth.
async function bootstrap() {
  try {
    const config = await getConfig()
    authRequired.value = config.authRequired
  } catch {
    authRequired.value = true
  }
  createApp(App).use(router).mount('#app')
}

bootstrap()
