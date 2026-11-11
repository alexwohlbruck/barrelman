import { describe, expect, test } from 'bun:test'
import { suitableCustomModel } from './bike-suitability-route.service'

// Two ways to the same place: way 1 is direct and red, way 2 a little longer and green.
const route = (wayId: number, time: number) => ({
  paths: [{ time, points: { coordinates: [[0, 0], [0.01, 0]] }, details: { osm_way_id: [[0, 1, wayId]] } }],
})

function fakeGraphHopper() {
  const models: any[] = []
  const fetchFn = async (_url: string, init?: RequestInit) => {
    const model = JSON.parse(String(init?.body)).custom_model
    models.push(model)
    const avoidsWay1 = JSON.stringify(model ?? {}).includes('osm_way_id == 1')
    return new Response(JSON.stringify(avoidsWay1 ? route(2, 700) : route(1, 600)))
  }
  return { fetchFn, models }
}

const lookup = async (ids: number[]) => new Map(ids.map(id => [id, id === 1 ? 15 : 95] as [number, number]))

describe('suitableCustomModel', () => {
  test('at safest it routes around the red way', async () => {
    const gh = fakeGraphHopper()
    const { model } = await suitableCustomModel({ points: [[0, 0], [0.01, 0]], profile: 'bike' }, 1, gh.fetchFn, lookup)
    expect(JSON.stringify(model)).toContain('osm_way_id == 1')
  })

  test('with little weight on safety it keeps the faster route', async () => {
    const gh = fakeGraphHopper()
    const { model } = await suitableCustomModel({ points: [[0, 0], [0.01, 0]], profile: 'bike', custom_model: { distance_influence: 70 } }, 0.05, gh.fetchFn, lookup)
    expect(model).toEqual({ distance_influence: 70 })
  })

  test('stops once a pass adds no new penalties', async () => {
    const gh = fakeGraphHopper()
    await suitableCustomModel({ points: [[0, 0], [0.01, 0]], profile: 'bike' }, 1, gh.fetchFn, lookup)
    expect(gh.models).toHaveLength(2)
  })
})
