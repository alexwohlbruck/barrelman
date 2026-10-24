import { describe, test, expect } from 'bun:test'
import {
  planTrace,
  parseGpx,
  parseJsonPoints,
  matchTrace,
  MatchRequestError,
  type TracePoint,
} from './match.service'
import type { FetchFn } from './route.service'

/** ~11 m per 0.0001° of latitude — a dense run walks north in 10 s, ~22 m steps. */
function denseRun(n: number, lat0: number, t0 = 0): TracePoint[] {
  return Array.from({ length: n }, (_, i) => ({ lat: lat0 + i * 0.0002, lon: -80.84, time: t0 + i * 10_000 }))
}

describe('planTrace', () => {
  test('a dense trace is matched, in one piece when it fits a chunk', () => {
    expect(planTrace(denseRun(10, 35.2))).toEqual([{ kind: 'match', start: 0, end: 9 }])
  })

  test('a sparse trace — every gap too far — is routed through end to end', () => {
    const points = [
      { lat: 35.2271, lon: -80.8431 },
      { lat: 35.2085, lon: -80.8120 },
      { lat: 35.2150, lon: -80.7800 },
    ]
    expect(planTrace(points)).toEqual([{ kind: 'route', start: 0, end: 2 }])
  })

  test('dense runs are matched and the gap between them routed, sharing boundary points', () => {
    const points = [...denseRun(5, 35.2), ...denseRun(5, 35.3, 60_000)]
    expect(planTrace(points)).toEqual([
      { kind: 'match', start: 0, end: 4 },
      { kind: 'route', start: 4, end: 5 },
      { kind: 'match', start: 5, end: 9 },
    ])
  })

  test('isolated points between dense runs ride along in the routed connector', () => {
    const points = [...denseRun(4, 35.2), { lat: 35.25, lon: -80.84 }, ...denseRun(4, 35.3)]
    expect(planTrace(points)).toEqual([
      { kind: 'match', start: 0, end: 3 },
      { kind: 'route', start: 3, end: 5 },
      { kind: 'match', start: 5, end: 8 },
    ])
  })

  test('a long time gap splits a run even when the points are close', () => {
    const points = denseRun(6, 35.2)
    points[3].time! += 5 * 60_000
    points[4].time! += 5 * 60_000
    points[5].time! += 5 * 60_000
    expect(planTrace(points)).toEqual([
      { kind: 'match', start: 0, end: 2 },
      { kind: 'route', start: 2, end: 3 },
      { kind: 'match', start: 3, end: 5 },
    ])
  })

  test('without times, gaps are judged on distance alone', () => {
    const points = denseRun(6, 35.2).map(({ lat, lon }) => ({ lat, lon }))
    expect(planTrace(points)).toEqual([{ kind: 'match', start: 0, end: 5 }])
  })

  test('a dense run shorter than three points is routed, not matched', () => {
    const points = [...denseRun(2, 35.2), { lat: 35.3, lon: -80.84 }]
    expect(planTrace(points)).toEqual([{ kind: 'route', start: 0, end: 2 }])
  })

  test('long runs are chunked evenly, with no runt chunk at the end', () => {
    const pieces = planTrace(denseRun(45, 35.2), { chunkPoints: 20 })
    expect(pieces).toEqual([
      { kind: 'match', start: 0, end: 15 },
      { kind: 'match', start: 15, end: 29 },
      { kind: 'match', start: 29, end: 44 },
    ])
  })

  test('pieces always tile the trace', () => {
    const points = [
      { lat: 35.1, lon: -80.84 },
      ...denseRun(30, 35.2),
      { lat: 35.25, lon: -80.84 },
      { lat: 35.27, lon: -80.84 },
      ...denseRun(7, 35.3),
      { lat: 35.4, lon: -80.84 },
    ]
    const pieces = planTrace(points, { chunkPoints: 10 })
    expect(pieces[0].start).toBe(0)
    expect(pieces[pieces.length - 1].end).toBe(points.length - 1)
    for (let i = 1; i < pieces.length; i++) expect(pieces[i].start).toBe(pieces[i - 1].end)
  })
})

describe('input parsing', () => {
  test('GPX track points with times, self-closing points too', () => {
    const gpx = `<?xml version="1.0"?><gpx><trk><trkseg>
      <trkpt lat="35.1" lon="-80.1"><ele>200</ele><time>2026-05-01T10:00:00Z</time></trkpt>
      <trkpt lon='-80.2' lat='35.2'/>
    </trkseg></trk></gpx>`
    expect(parseGpx(gpx)).toEqual([
      { lat: 35.1, lon: -80.1, time: Date.parse('2026-05-01T10:00:00Z') },
      { lat: 35.2, lon: -80.2, time: undefined },
    ])
  })

  test('GPX route points when there is no track', () => {
    const gpx = `<gpx><rte><rtept lat="1" lon="2"></rtept><rtept lat="3" lon="4"></rtept></rte></gpx>`
    expect(parseGpx(gpx).map((p) => [p.lat, p.lon])).toEqual([[1, 2], [3, 4]])
  })

  test('JSON times as ISO strings, epoch seconds or epoch milliseconds', () => {
    const ms = Date.parse('2026-05-01T10:00:00Z')
    const points = parseJsonPoints([
      { lat: 1, lon: 2, time: '2026-05-01T10:00:00Z' },
      { lat: 1, lng: 2, time: ms / 1000 },
      { lat: 1, lon: 2, time: ms },
    ])
    expect(points.map((p) => p.time)).toEqual([ms, ms, ms])
    expect(points[1].lon).toBe(2)
  })

  test('rejects a point without coordinates', () => {
    expect(() => parseJsonPoints([{ lat: 1 }])).toThrow(MatchRequestError)
  })
})

// ── matchTrace against a fake GraphHopper ───────────────────────────

/**
 * Answers /match and /route with a straight line through the points it was
 * sent, with a road_class detail per edge — enough to check stitching.
 */
function fakeGraphHopper(opts: { matchStatus?: number } = {}) {
  const calls: Array<{ path: string; body: string }> = []
  const fetchFn: FetchFn = async (url, init) => {
    const path = new URL(url).pathname
    const body = String(init.body)
    calls.push({ path, body })

    let coords: number[][]
    if (path === '/match') {
      if (opts.matchStatus) {
        return new Response(JSON.stringify({ message: 'Sequence is broken for submitted track' }), { status: opts.matchStatus })
      }
      coords = [...body.matchAll(/lat="([^"]+)" lon="([^"]+)"/g)].map((m) => [Number(m[2]), Number(m[1]), 100])
    } else {
      coords = JSON.parse(body).points.map(([lon, lat]: number[]) => [lon, lat, 100])
    }
    const n = coords.length
    return Response.json({
      paths: [{
        distance: 10 * (n - 1),
        time: 1000 * (n - 1),
        ascend: 1,
        descend: 2,
        points: { type: 'LineString', coordinates: coords },
        details: {
          road_class: [[0, n - 1, path === '/match' ? 'residential' : 'primary']],
          street_name: [[0, n - 1, 'Main Street']],
        },
      }],
    })
  }
  return { fetchFn, calls }
}

describe('matchTrace', () => {
  test('a sparse trace makes one multi-point /route call and never calls /match', async () => {
    const { fetchFn, calls } = fakeGraphHopper()
    const points = [
      { lat: 35.2271, lon: -80.8431 },
      { lat: 35.2085, lon: -80.8120 },
      { lat: 35.2150, lon: -80.7800 },
    ]
    const res = await matchTrace({ profile: 'bike', points, snapPrevention: ['bridge', 'motorway'] }, fetchFn)

    expect(calls.map((c) => c.path)).toEqual(['/route'])
    expect(JSON.parse(calls[0].body).snap_preventions).toEqual(['bridge', 'motorway'])
    expect(res.paths[0].segments).toEqual([{ type: 'routed', from_point: 0, to_point: 2, distance: 20, time: 2000 }])
    expect(res.paths[0].snapped_points.map((p) => p.input_index)).toEqual([0, 1, 2])
    expect(res.map_matching).toMatchObject({ input_points: 3, matched_points: 0, routed_points: 3 })
  })

  test('stitches matched runs and routed gaps into one continuous path', async () => {
    const { fetchFn, calls } = fakeGraphHopper()
    const points = [...denseRun(4, 35.2), ...denseRun(4, 35.3, 60_000)]
    const res = await matchTrace({ profile: 'bike', points }, fetchFn)
    const path = res.paths[0]

    expect(calls.map((c) => c.path).sort()).toEqual(['/match', '/match', '/route'])
    // The connector starts and ends on the matched geometry either side.
    const route = JSON.parse(calls.find((c) => c.path === '/route')!.body)
    expect(route.points).toEqual([[points[3].lon, points[3].lat], [points[4].lon, points[4].lat]])

    // Shared join points are not duplicated: 4 + 2 + 4 coordinates, minus 2 joins.
    expect(path.points.coordinates).toHaveLength(8)
    expect(path.details.road_class).toEqual([
      [0, 3, 'residential'],
      [3, 4, 'primary'],
      [4, 7, 'residential'],
    ])
    // A detail that runs on across a join is merged, not split.
    expect(path.details.street_name).toEqual([[0, 7, 'Main Street']])
    expect(path.segments.map((s) => s.type)).toEqual(['matched', 'routed', 'matched'])
    expect(path.ascend).toBe(3)
    expect(path.snapped_points).toHaveLength(8)
    for (const p of path.snapped_points) expect(p.offset).toBeLessThan(0.5)
    const along = path.snapped_points.map((p) => p.distance_along)
    expect([...along].sort((a, b) => a - b)).toEqual(along)
  })

  test('a chunk the matcher rejects is routed instead, and says so', async () => {
    const { fetchFn, calls } = fakeGraphHopper({ matchStatus: 400 })
    const res = await matchTrace({ profile: 'bike', points: denseRun(6, 35.2) }, fetchFn)

    expect(calls.map((c) => c.path)).toEqual(['/match', '/route'])
    expect(res.paths[0].segments).toEqual([
      expect.objectContaining({ type: 'routed', fallback: 'match_failed', from_point: 0, to_point: 5 }),
    ])
  })

  test('a GraphHopper outage fails the request rather than being papered over', async () => {
    const { fetchFn } = fakeGraphHopper({ matchStatus: 503 })
    await expect(matchTrace({ profile: 'bike', points: denseRun(6, 35.2) }, fetchFn)).rejects.toThrow('GraphHopper returned 503')
  })

  test('validates its options', async () => {
    const { fetchFn, calls } = fakeGraphHopper()
    const points = denseRun(3, 35.2)
    await expect(matchTrace({ profile: '', points }, fetchFn)).rejects.toThrow('profile is required')
    await expect(matchTrace({ profile: 'bike', points: points.slice(0, 1) }, fetchFn)).rejects.toThrow('At least 2')
    await expect(matchTrace({ profile: 'bike', points, gpsAccuracy: 500 }, fetchFn)).rejects.toThrow('gps_accuracy')
    expect(calls).toHaveLength(0)
  })
})
