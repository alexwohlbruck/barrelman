import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mock dependencies ───────────────────────────────────────────────

const mockDbExecute = mock(async () => [])
mock.module('../db', () => ({
  db: { execute: mockDbExecute },
}))

// ── Import under test ───────────────────────────────────────────────

const { getRouteShape, getShapeById } = await import('./shapes.service')

// ── Tests ───────────────────────────────────────────────────────────

describe('ShapesService — getRouteShape', () => {
  beforeEach(() => {
    mockDbExecute.mockReset()
  })

  test('returns null when route has no shape_id', async () => {
    mockDbExecute.mockImplementation(async () => [{ shape_id: null }])

    const result = await getRouteShape('feed-1', 'route-1')
    expect(result).toBeNull()
  })

  test('returns null when route does not exist', async () => {
    mockDbExecute.mockImplementation(async () => [])

    const result = await getRouteShape('feed-1', 'nonexistent')
    expect(result).toBeNull()
  })

  test('returns shape coordinates for a valid route', async () => {
    let callCount = 0
    const coords = [[1.5, 2.5], [3.5, 4.5], [5.5, 6.5]]

    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        // Route lookup
        return [{ shape_id: 'shape-42' }]
      }
      // Shape lookup — coordinates as JSONB (already parsed)
      return [{ coordinates: coords }]
    })

    const result = await getRouteShape('feed-10', 'route-10')
    expect(result).not.toBeNull()
    expect(result!.feedId).toBe('feed-10')
    expect(result!.routeId).toBe('route-10')
    expect(result!.shapeId).toBe('shape-42')
    expect(result!.coordinates).toEqual(coords)
  })

  test('handles coordinates stored as JSON string', async () => {
    let callCount = 0
    const coords = [[1, 2], [3, 4]]

    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) return [{ shape_id: 'shape-99' }]
      // Coordinates as string (some DB drivers return JSONB as string)
      return [{ coordinates: JSON.stringify(coords) }]
    })

    const result = await getRouteShape('feed-20', 'route-20')
    expect(result).not.toBeNull()
    expect(result!.coordinates).toEqual(coords)
  })

  test('returns null when shape record is missing', async () => {
    let callCount = 0
    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) return [{ shape_id: 'orphan-shape' }]
      return [] // Shape not found
    })

    const result = await getRouteShape('feed-30', 'route-30')
    expect(result).toBeNull()
  })
})

describe('ShapesService — getShapeById', () => {
  beforeEach(() => {
    mockDbExecute.mockReset()
  })

  test('returns coordinates for a valid shape_id', async () => {
    const coords = [[-80.0, 35.0], [-80.1, 35.1]]
    mockDbExecute.mockImplementation(async () => [{ coordinates: coords }])

    const result = await getShapeById('feed-a', 'shape-a')
    expect(result).toEqual(coords)
  })

  test('returns null when shape_id does not exist', async () => {
    mockDbExecute.mockImplementation(async () => [])

    const result = await getShapeById('feed-b', 'missing-shape')
    expect(result).toBeNull()
  })
})
