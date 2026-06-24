import JSZip from 'jszip'
import { readFileSync, existsSync } from 'fs'
import { ensureGtfsSchema } from '../src/db'
import { parseShapes, importShapes, deriveRouteShapes, updateRouteShapes } from '../src/services/gtfs.service'

async function main() {
  await ensureGtfsSchema()

  const feedIds = process.argv.slice(2)
  const dir = './data/gtfs'

  let zips: string[]
  if (feedIds.length > 0) {
    zips = feedIds.map(id => `${id}.zip`)
  } else {
    const { readdirSync } = await import('fs')
    zips = readdirSync(dir).filter(f => f.endsWith('.zip'))
  }

  let total = 0
  let imported = 0
  let skipped = 0

  for (const zip of zips) {
    const feedId = zip.replace('.zip', '')
    const zipPath = `${dir}/${zip}`
    total++

    if (!existsSync(zipPath)) {
      console.log(`  skip ${feedId} — zip not found`)
      skipped++
      continue
    }

    try {
      const buffer = readFileSync(zipPath)
      const archive = await JSZip.loadAsync(buffer)

      const shapesFile = archive.file('shapes.txt')
      if (!shapesFile) {
        console.log(`  skip ${feedId} — no shapes.txt`)
        skipped++
        continue
      }

      const shapesContent = await shapesFile.async('string')
      const shapes = parseShapes(shapesContent)
      if (shapes.size === 0) {
        console.log(`  skip ${feedId} — shapes.txt is empty`)
        skipped++
        continue
      }

      const count = await importShapes(shapes, feedId)

      const tripsFile = archive.file('trips.txt')
      let routeCount = 0
      if (tripsFile) {
        const tripsContent = await tripsFile.async('string')
        const routeShapes = deriveRouteShapes(tripsContent)
        await updateRouteShapes(routeShapes, feedId)
        routeCount = routeShapes.size
      }

      console.log(`  ${feedId}: ${count} shapes, ${routeCount} routes linked`)
      imported++
    } catch (err) {
      console.error(`  ${feedId}: ERROR — ${err instanceof Error ? err.message : err}`)
    }
  }

  console.log(`\nDone: ${imported} imported, ${skipped} skipped, ${total} total`)
  process.exit(0)
}

main().catch(e => { console.error(e); process.exit(1) })
