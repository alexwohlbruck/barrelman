import { describe, test, expect, beforeEach } from 'bun:test'
import { buildEdgeSegments, getEnrichedRoute, resetEncodedValues, type FetchFn } from './route.service'

/** A GraphHopper whose graph carries `encoded`, recording the details each route asks for. */
function graphHopper(encoded: string[]) {
  const asked: string[][] = []
  const fetchFn: FetchFn = async (url, init) => {
    if (url.endsWith('/info')) return Response.json({ encoded_values: Object.fromEntries(encoded.map(k => [k, []])) })
    asked.push(JSON.parse(String(init.body)).details)
    return Response.json({ paths: [] })
  }
  return { fetchFn, asked }
}

const request = { points: [[-73.99, 40.75], [-73.98, 40.75]], profile: 'car' }

beforeEach(resetEncodedValues)

describe('getEnrichedRoute', () => {
  test('asks for way ids where the graph has them', async () => {
    const gh = graphHopper(['osm_way_id', 'country'])
    await getEnrichedRoute(request, gh.fetchFn)
    expect(gh.asked[0]).toContain('osm_way_id')
    expect(gh.asked[0]).toContain('country')
  })

  test('leaves them out for a graph built without them, which would refuse the route', async () => {
    const gh = graphHopper(['road_class'])
    await getEnrichedRoute(request, gh.fetchFn)
    expect(gh.asked[0]).not.toContain('osm_way_id')
  })
})

describe('buildEdgeSegments', () => {
  test('keeps a segment per way, with its country', () => {
    const segments = buildEdgeSegments(
      { osm_way_id: [[0, 1, 11], [1, 2, 12]], road_class: [[0, 2, 'primary']], country: [[0, 2, 'USA']] },
      [0, 50, 120],
    )
    expect(segments.map(s => [s.osmWayId, s.startDistance, s.endDistance, s.country])).toEqual([
      [11, 0, 50, 'USA'],
      [12, 50, 120, 'USA'],
    ])
  })
})

describe('sidewalk street names', () => {
  test('edge segments along a named sidewalk carry its street', async () => {
    const coordinates = [[-73.99, 40.74, 10], [-73.9888, 40.74, 10]]
    const fetchFn: FetchFn = async url => url.endsWith('/info')
      ? Response.json({ encoded_values: { osm_way_id: [] } })
      : Response.json({ paths: [{ points: { coordinates }, details: { osm_way_id: [[0, 1, 42]], road_class: [[0, 1, 'footway']] } }] })
    const lookup = async (ids: number[]) => new Map(ids.filter(id => id === 42).map(id => [id, {
      coords: coordinates.map(([lon, lat]) => [lon, lat] as [number, number]),
      zones: [{ from: 0, to: 1, name: 'W 3rd St', source: 'geometry' as const }],
    }]))
    const { paths } = await getEnrichedRoute(request, fetchFn, lookup)
    expect(paths[0].edge_segments.map(s => [s.osmWayId, s.streetName, s.streetNameSource])).toEqual([[42, 'W 3rd St', 'geometry']])
  })
})
