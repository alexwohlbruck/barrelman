import { join } from 'path'

export async function landmarkCacheAvailable(
  models: { path: string | null; sha256: string }[],
  cacheDir: string,
): Promise<boolean> {
  const available = await Promise.all(models.map(async model =>
    model.path === join(cacheDir, `${model.sha256}.glb`) && await Bun.file(model.path).exists(),
  ))
  return available.every(Boolean)
}
