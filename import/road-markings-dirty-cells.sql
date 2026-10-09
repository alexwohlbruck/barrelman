-- The grid cells (:cell degrees) the road_markings_dirty queue touches, where
-- road markings have been built in or beside the cell, at most :max_cells.
-- One cell per row; scripts/update-road-markings.sh rebuilds each.
SELECT DISTINCT cx, cy
FROM road_markings_dirty d,
     generate_series(floor(ST_XMin(d.box) / :cell)::int, floor(ST_XMax(d.box) / :cell)::int) cx,
     generate_series(floor(ST_YMin(d.box) / :cell)::int, floor(ST_YMax(d.box) / :cell)::int) cy
WHERE EXISTS (
  SELECT 1 FROM road_surfaces s
  WHERE s.geom && ST_MakeEnvelope((cx - 1) * :cell, (cy - 1) * :cell, (cx + 2) * :cell, (cy + 2) * :cell, 4326)
)
ORDER BY cx, cy
LIMIT :max_cells;
