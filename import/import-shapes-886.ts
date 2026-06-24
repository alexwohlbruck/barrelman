import JSZip from 'jszip'
import { readFileSync } from 'fs'
import { ensureGtfsSchema } from '../src/db'
import { parseShapes, importShapes, deriveRouteShapes, updateRouteShapes } from '../src/services/gtfs.service'

async function main() {
  await ensureGtfsSchema()

  const zipPath = './data/gtfs/886.zip'
  const buffer = readFileSync(zipPath)
  const zip = await JSZip.loadAsync(buffer)

  // Parse shapes.txt
  const shapesFile = zip.file('shapes.txt')
  if (!shapesFile) {
    console.error('No shapes.txt found in 886.zip')
    process.exit(1)
  }
  const shapesContent = await shapesFile.async('string')
  const shapes = parseShapes(shapesContent)
  const count = await importShapes(shapes, '886')
  console.log(`✓ Imported ${count} shapes for feed 886`)

  // Link routes to shapes via trips.txt
  const tripsFile = zip.file('trips.txt')
  if (tripsFile) {
    const tripsContent = await tripsFile.async('string')
    const routeShapes = deriveRouteShapes(tripsContent)
    await updateRouteShapes(routeShapes, '886')
    console.log(`✓ Linked ${routeShapes.size} routes to shapes`)
  }

  console.log('Done!')
  process.exit(0)
}

main().catch(e => { console.error(e); process.exit(1) })
