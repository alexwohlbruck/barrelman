<script setup lang="ts">
import Input from '@/components/ui/Input.vue'
import Select from '@/components/ui/Select.vue'
import Switch from '@/components/ui/Switch.vue'
import Label from '@/components/ui/Label.vue'
import type { ScriptParam } from '@/lib/types'

const props = defineProps<{ param: ScriptParam; modelValue: unknown }>()
const emit = defineEmits<{ 'update:modelValue': [value: unknown] }>()
</script>

<template>
  <div class="flex flex-col gap-1.5">
    <div class="flex items-center justify-between gap-3">
      <Label class="flex items-center gap-1.5">
        {{ param.label }}
        <span v-if="param.required" class="text-destructive">*</span>
        <code v-if="param.apply === 'flag'" class="rounded bg-muted px-1 py-0.5 text-[10px] text-muted-foreground">
          {{ param.flag || '--' + param.name }}
        </code>
        <code v-else-if="param.apply === 'env'" class="rounded bg-muted px-1 py-0.5 text-[10px] text-muted-foreground">
          {{ param.envVar || param.name }}
        </code>
      </Label>
      <Switch
        v-if="param.type === 'boolean'"
        :model-value="Boolean(modelValue)"
        @update:model-value="emit('update:modelValue', $event)"
      />
    </div>

    <Select
      v-if="param.type === 'select'"
      :model-value="String(modelValue ?? param.default ?? '')"
      :options="param.options || []"
      @update:model-value="emit('update:modelValue', $event)"
    />
    <Input
      v-else-if="param.type !== 'boolean'"
      :model-value="modelValue as string"
      :type="param.secret ? 'password' : param.type === 'number' ? 'number' : 'text'"
      :placeholder="param.placeholder"
      @update:model-value="emit('update:modelValue', $event)"
    />

    <p v-if="param.description" class="text-xs text-muted-foreground">{{ param.description }}</p>
  </div>
</template>
