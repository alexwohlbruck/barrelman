-- =============================================================================
-- Plan one run of scripts/update-road-markings.sh over its rows of detail_dirty
-- =============================================================================
-- Takes :cell (cell size in degrees), :max_cells and :max_attempts. Cuts each
-- queued box into grid cells and keeps the cells that have road markings in
-- or beside them. Then:
--
--   * drops entries with no such cell (nobody built road markings there) and
--     entries that failed :max_attempts runs;
--   * picks whole entries, oldest first and failed ones last, while their
--     cells fit in :max_cells. An entry is cleared only once every one of its
--     cells is rebuilt, so all of them are rebuilt in the same run. The first
--     entry is always picked, however many cells it spans, so the queue moves.
--
-- Returns one row per line of output: ('dropped', unbuilt, given up),
-- ('entries', comma-separated ids picked) and ('cell', cx, cy) per cell to
-- rebuild, where cell (cx, cy) spans [cx, cx + 1) × [cy, cy + 1) times :cell.
-- One snapshot throughout, so an entry queued meanwhile is left alone.
-- =============================================================================
BEGIN ISOLATION LEVEL REPEATABLE READ;

DROP TABLE IF EXISTS _rmq_cells, _rmq_rank, _rmq_order, _rmq_pick, _rmq_dropped;
CREATE TEMP TABLE _rmq_cells AS
SELECT d.id, d.attempts, d.queued_at, cx, cy
FROM detail_dirty d,
     generate_series(floor(ST_XMin(d.box) / :cell)::int, floor(ST_XMax(d.box) / :cell)::int) cx,
     generate_series(floor(ST_YMin(d.box) / :cell)::int, floor(ST_YMax(d.box) / :cell)::int) cy
WHERE d.layer = 'road_markings' AND d.attempts < :max_attempts
  AND EXISTS (
    SELECT 1 FROM road_surfaces s
    WHERE s.geom && ST_MakeEnvelope((cx - 1) * :cell, (cy - 1) * :cell, (cx + 2) * :cell, (cy + 2) * :cell, 4326)
  );

CREATE TEMP TABLE _rmq_dropped (unbuilt bigint, gave_up bigint);
WITH gone AS (
  DELETE FROM detail_dirty d
  WHERE d.layer = 'road_markings' AND NOT EXISTS (SELECT 1 FROM _rmq_cells c WHERE c.id = d.id)
  RETURNING d.attempts >= :max_attempts AS gave_up
)
INSERT INTO _rmq_dropped
SELECT count(*) FILTER (WHERE NOT gave_up), count(*) FILTER (WHERE gave_up) FROM gone;

CREATE TEMP TABLE _rmq_rank AS
SELECT id, row_number() OVER (ORDER BY attempts, queued_at, id) AS rank
FROM (SELECT DISTINCT id, attempts, queued_at FROM _rmq_cells) e;

-- Each cell takes its place from the first entry that needs it.
CREATE TEMP TABLE _rmq_order AS
SELECT cx, cy, row_number() OVER (ORDER BY min(r.rank), cx, cy) AS n
FROM _rmq_cells c JOIN _rmq_rank r USING (id)
GROUP BY cx, cy;

CREATE TEMP TABLE _rmq_pick AS
SELECT r.id
FROM _rmq_rank r
JOIN _rmq_cells c USING (id)
JOIN _rmq_order o USING (cx, cy)
GROUP BY r.id, r.rank
HAVING max(o.n) <= :max_cells OR r.rank = 1;

COMMIT;

SELECT 'dropped' AS kind, unbuilt::text AS a, gave_up::text AS b FROM _rmq_dropped
UNION ALL
SELECT 'entries', string_agg(id::text, ','), NULL FROM _rmq_pick
UNION ALL
(SELECT 'cell', cx::text, cy::text
 FROM (SELECT DISTINCT c.cx, c.cy FROM _rmq_cells c JOIN _rmq_pick p USING (id)) x
 ORDER BY x.cx, x.cy);
