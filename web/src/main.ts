import { createApp } from 'vue'
import './style.css'
import App from './App.vue'
import router from './router'

// Session and sign-in configuration are loaded by the router's first guard
// (see lib/auth.ts `bootstrap`), so mounting does not wait on the network —
// App.vue renders a spinner until that probe answers.
createApp(App).use(router).mount('#app')
