-- Bike suitability: every rideable street and path scored 0-100 for how
-- comfortable it is to ride, from its own OSM tags.
--
-- The model follows Level of Traffic Stress (Mekuria, Furth & Nixon 2012):
-- motor traffic sets the stress, a bike facility takes some share of it away,
-- and the riding surface, pedestrians, light and gradient cost points on top.
-- Every input is a tag on the way itself, so the layer regenerates with OSM and
-- needs no derived table: it is a plain view, read per tile through
-- `geo_places_highway_lines_geom_idx`.

-- A tagged speed limit in km/h, or what the class implies where none is tagged.
-- Defaults are typical US urban limits, where nearly all of our coverage is.
CREATE OR REPLACE FUNCTION bike_speed_kmh(tags jsonb) RETURNS real
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT COALESCE(
    CASE
      WHEN tags->>'maxspeed' ~* '^\s*[0-9.]+\s*mph' THEN substring(tags->>'maxspeed' from '^\s*([0-9.]+)')::real * 1.609
      WHEN tags->>'maxspeed' ~ '^\s*[0-9.]+\s*(km/h|kmh)?\s*$' THEN substring(tags->>'maxspeed' from '^\s*([0-9.]+)')::real
      WHEN tags->>'maxspeed' = 'walk' THEN 7
      WHEN tags->>'maxspeed' ~* 'living_street' THEN 15
    END,
    CASE tags->>'highway'
      WHEN 'motorway' THEN 105 WHEN 'motorway_link' THEN 72
      WHEN 'trunk' THEN 80 WHEN 'trunk_link' THEN 56
      WHEN 'primary' THEN 56 WHEN 'primary_link' THEN 48
      WHEN 'secondary' THEN 56 WHEN 'secondary_link' THEN 48
      WHEN 'tertiary' THEN 48 WHEN 'tertiary_link' THEN 40
      WHEN 'unclassified' THEN 48 WHEN 'road' THEN 48
      WHEN 'residential' THEN 40
      WHEN 'living_street' THEN 15
      WHEN 'service' THEN 24 WHEN 'track' THEN 24
      ELSE 0
    END)
$$;

-- Through lanes a rider shares with traffic in their direction.
CREATE OR REPLACE FUNCTION bike_lanes_per_direction(tags jsonb) RETURNS int
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT greatest(1, CASE
    WHEN tags->>'oneway' IN ('yes', '1', '-1', 'true') OR tags->>'junction' = 'roundabout'
      THEN COALESCE(substring(tags->>'lanes' from '^\s*([0-9]+)')::int, 1)
    ELSE COALESCE(
      substring(tags->>'lanes:forward' from '^\s*([0-9]+)')::int,
      ceil(substring(tags->>'lanes' from '^\s*([0-9]+)')::real / 2)::int,
      CASE WHEN tags->>'highway' IN ('trunk', 'primary') THEN 2 ELSE 1 END)
  END)
$$;

-- Whether a way belongs on the map at all: bikes may legally ride it, and it is
-- a way rather than a driveway or a parking aisle.
CREATE OR REPLACE FUNCTION bike_rideable(tags jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN tags->>'bicycle' IN ('no', 'private', 'use_sidepath') THEN false
    WHEN tags->>'area' = 'yes' OR tags->>'indoor' IN ('yes', 'corridor') THEN false
    WHEN tags->>'access' IN ('no', 'private') AND COALESCE(tags->>'bicycle', '') NOT IN ('yes', 'designated', 'permissive') THEN false
    WHEN tags->>'highway' IN ('footway', 'pedestrian', 'bridleway', 'busway', 'motorway', 'motorway_link')
      THEN COALESCE(tags->>'bicycle', '') IN ('yes', 'designated', 'permissive')
    WHEN tags->>'highway' = 'steps'
      THEN tags->>'ramp:bicycle' = 'yes' OR COALESCE(tags->>'bicycle', '') IN ('yes', 'designated')
    WHEN tags->>'highway' = 'service'
      THEN COALESCE(tags->>'service', '') NOT IN ('parking_aisle', 'driveway', 'drive-through', 'emergency_access')
    ELSE tags->>'highway' IN ('cycleway', 'path', 'track', 'living_street', 'residential', 'unclassified', 'road',
      'tertiary', 'tertiary_link', 'secondary', 'secondary_link', 'primary', 'primary_link', 'trunk', 'trunk_link')
  END
$$;

-- The best provision on either side, as one word. A side counts once it is
-- mapped any of the four ways OSM allows: plain, both, left or right.
CREATE OR REPLACE FUNCTION bike_facility(tags jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  hw text := tags->>'highway';
  side text;
  v text;
  best int := 0;
  rank int;
  label text[] := ARRAY['sharrow', 'shoulder', 'advisory', 'busway', 'lane', 'buffered_lane', 'track'];
BEGIN
  IF hw = 'cycleway' OR (hw IN ('path', 'footway', 'bridleway') AND tags->>'bicycle' = 'designated') THEN
    RETURN 'path';
  END IF;
  IF hw IN ('path', 'footway', 'pedestrian', 'bridleway', 'track', 'steps') THEN RETURN 'shared_path'; END IF;
  IF tags->>'bicycle_road' = 'yes' OR tags->>'cyclestreet' = 'yes' THEN RETURN 'cyclestreet'; END IF;

  FOREACH side IN ARRAY ARRAY['cycleway', 'cycleway:both', 'cycleway:left', 'cycleway:right'] LOOP
    v := tags->>side;
    rank := CASE
      WHEN v IN ('track', 'opposite_track', 'sidepath') THEN 7
      WHEN v IN ('lane', 'opposite_lane') AND tags->>(side || ':separation') IN
        ('kerb', 'bollard', 'flex_post', 'vertical_panel', 'planter', 'jersey_barrier', 'guard_rail', 'parking_lane') THEN 7
      WHEN v IN ('lane', 'opposite_lane', 'buffered_lane') AND (v = 'buffered_lane'
        OR COALESCE(tags->>(side || ':buffer'), 'no') NOT IN ('no', '0')
        OR tags->>(side || ':separation') IN ('solid_line', 'hatched', 'dashed_line;solid_line')) THEN 6
      WHEN v IN ('lane', 'opposite_lane') AND tags->>(side || ':lane') = 'advisory' THEN 3
      WHEN v IN ('lane', 'opposite_lane') THEN 5
      WHEN v IN ('share_busway', 'opposite_share_busway') THEN 4
      WHEN v = 'shoulder' OR tags->>'shoulder:bicycle' = 'yes' THEN 2
      WHEN v = 'shared_lane' THEN 1
      ELSE 0
    END;
    best := greatest(best, rank);
  END LOOP;
  RETURN COALESCE(label[best], 'none');
END
$$;

-- Points lost to the riding surface: material, wear, and a track's grade where
-- nothing better is tagged. An untagged path is taken as paved.
CREATE OR REPLACE FUNCTION bike_surface_penalty(tags jsonb) RETURNS real
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT COALESCE(CASE tags->>'surface'
      WHEN 'paving_stones' THEN 4 WHEN 'concrete:plates' THEN 3 WHEN 'concrete:lanes' THEN 8
      WHEN 'bricks' THEN 10 WHEN 'wood' THEN 6 WHEN 'metal' THEN 8
      WHEN 'sett' THEN 18 WHEN 'cobblestone' THEN 22 WHEN 'unhewn_cobblestone' THEN 28
      WHEN 'compacted' THEN 8 WHEN 'fine_gravel' THEN 10 WHEN 'unpaved' THEN 16
      WHEN 'gravel' THEN 20 WHEN 'pebblestone' THEN 24 WHEN 'rock' THEN 30
      WHEN 'dirt' THEN 22 WHEN 'ground' THEN 22 WHEN 'earth' THEN 22 WHEN 'woodchips' THEN 26
      WHEN 'grass' THEN 32 WHEN 'grass_paver' THEN 20 WHEN 'sand' THEN 35 WHEN 'mud' THEN 38
    END, CASE tags->>'tracktype'
      WHEN 'grade1' THEN 0 WHEN 'grade2' THEN 10 WHEN 'grade3' THEN 18 WHEN 'grade4' THEN 26 WHEN 'grade5' THEN 34
    END, CASE WHEN tags->>'highway' = 'track' THEN 16 END, 0)
  + COALESCE(CASE tags->>'smoothness'
      WHEN 'intermediate' THEN 4 WHEN 'bad' THEN 12 WHEN 'very_bad' THEN 20
      WHEN 'horrible' THEN 30 WHEN 'very_horrible' THEN 36 WHEN 'impassable' THEN 45
    END, 0)
$$;

-- The score itself, 0 (avoid) to 100 (anyone would ride it).
CREATE OR REPLACE FUNCTION bike_suitability_score(tags jsonb, facility text) RETURNS smallint
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  hw text := tags->>'highway';
  kmh real := bike_speed_kmh(tags);
  lanes int := bike_lanes_per_direction(tags);
  car_free boolean := COALESCE(facility IN ('path', 'shared_path') AND hw <> 'track'
    OR tags->>'motor_vehicle' IN ('no', 'agricultural', 'forestry') OR tags->>'motorcar' = 'no', false);
  traffic real := 0;
  relief real;
  score real;
  incline real;
  parked boolean;
BEGIN
  IF hw = 'steps' THEN RETURN 10; END IF;

  -- Motor traffic: speed carries most of it, then lanes to cross and share,
  -- then the class, which stands in for the volume OSM does not record.
  IF NOT car_free THEN
    traffic := CASE
      WHEN kmh <= 20 THEN 0
      WHEN kmh <= 30 THEN (kmh - 20) * 0.8
      WHEN kmh <= 50 THEN 8 + (kmh - 30) * 1.5
      WHEN kmh <= 70 THEN 38 + (kmh - 50) * 1.6
      ELSE least(95, 70 + (kmh - 70) * 1.0)
    END
    + CASE WHEN lanes >= 3 THEN 22 WHEN lanes = 2 THEN 12 ELSE 0 END
    + CASE
        WHEN hw IN ('trunk', 'trunk_link', 'motorway', 'motorway_link') THEN 25
        WHEN hw IN ('primary', 'primary_link') THEN 18
        WHEN hw IN ('secondary', 'secondary_link') THEN 12
        WHEN hw IN ('tertiary', 'tertiary_link') THEN 6
        WHEN hw IN ('unclassified', 'road') THEN 4
        ELSE 0
      END
    + CASE WHEN tags->>'hgv' = 'designated' THEN 8 ELSE 0 END
    - CASE WHEN tags ? 'traffic_calming' THEN 6 ELSE 0 END;
    traffic := least(100, greatest(0, traffic));
  END IF;

  -- The share of that stress a facility takes away. A painted line does less
  -- the faster the traffic beside it.
  relief := CASE facility
    WHEN 'track' THEN 0.85
    WHEN 'cyclestreet' THEN 0.7
    WHEN 'buffered_lane' THEN 0.6
    WHEN 'lane' THEN 0.45
    WHEN 'busway' THEN 0.35
    WHEN 'shoulder' THEN 0.3
    WHEN 'advisory' THEN 0.25
    WHEN 'sharrow' THEN 0.1
    ELSE 0
  END;
  IF facility IN ('lane', 'buffered_lane', 'advisory', 'shoulder') AND kmh > 60 THEN relief := relief * 0.7; END IF;

  score := 100 - traffic * (1 - relief) - bike_surface_penalty(tags);

  -- Sharing with people on foot: slower, and they step out.
  score := score - CASE
    WHEN tags->>'segregated' = 'yes' THEN 2
    WHEN hw = 'cycleway' AND COALESCE(tags->>'foot', '') NOT IN ('yes', 'designated') THEN 0
    WHEN hw = 'pedestrian' THEN 15
    WHEN hw = 'footway' THEN 12
    WHEN hw IN ('path', 'bridleway', 'cycleway') THEN 6
    ELSE 0
  END;

  -- Parked cars beside a painted lane: the dooring zone.
  parked := EXISTS (
    SELECT 1 FROM unnest(ARRAY['parking:both', 'parking:right', 'parking:left',
      'parking:lane:both', 'parking:lane:right', 'parking:lane:left']) k
    WHERE tags->>k IN ('lane', 'parallel', 'diagonal', 'perpendicular', 'street_side', 'on_street', 'half_on_kerb', 'yes'));
  IF parked AND facility IN ('lane', 'advisory', 'sharrow') THEN score := score - 6; END IF;

  -- Too narrow to pass or be passed.
  IF COALESCE(substring(COALESCE(tags->>'cycleway:width', tags->>'cycleway:right:width', tags->>'cycleway:both:width')
       from '^\s*([0-9.]+)\s*m?\s*$')::real, 9) < 1.5 THEN score := score - 5; END IF;
  IF facility IN ('path', 'shared_path')
     AND COALESCE(substring(tags->>'width' from '^\s*([0-9.]+)\s*m?\s*$')::real, 9) < 1.5 THEN score := score - 5; END IF;

  -- Grade, where it is tagged as a number.
  incline := abs(substring(tags->>'incline' from '^\s*(-?[0-9.]+)\s*%?\s*$')::real);
  IF incline > 4 THEN score := score - least(20, (incline - 4) * 2.5); END IF;

  IF tags->>'lit' = 'no' THEN score := score - 4;
  ELSIF tags->>'lit' = 'yes' AND car_free THEN score := score + 2; END IF;

  IF tags->>'bicycle' = 'dismount' THEN score := least(score, 20); END IF;

  RETURN round(least(100, greatest(0, score)))::smallint;
END
$$;

-- No window functions or joins, so Martin's tile envelope reaches the
-- partial index on highway lines; see the header of create-detail-views.sql.
DROP VIEW IF EXISTS bike_suitability CASCADE;
CREATE VIEW bike_suitability AS
SELECT osm_id * 4 + 1 as fid,
       geom,
       name,
       tags->>'highway' as highway,
       f.facility,
       bike_suitability_score(tags, f.facility) as score,
       round(bike_speed_kmh(tags))::smallint as speed_kmh,
       bike_lanes_per_direction(tags)::smallint as lanes,
       tags->>'surface' as surface,
       COALESCE(tags->>'bridge', 'no') <> 'no' as bridge,
       COALESCE(tags->>'tunnel', 'no') <> 'no' as tunnel
FROM geo_places, LATERAL (SELECT bike_facility(tags) as facility) f
WHERE geom_type = 'line'
  AND tags ? 'highway'
  AND bike_rideable(tags);

-- The score of the way that carries this one on past `pt`: one that ends there,
-- the same street by name where there is one.
CREATE OR REPLACE FUNCTION bike_neighbour_score(pt geometry, way_name text, self bigint) RETURNS smallint
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT b.score FROM bike_suitability b
  WHERE b.geom && pt AND b.fid <> self
    AND (ST_StartPoint(b.geom) = pt OR ST_EndPoint(b.geom) = pt)
  ORDER BY (b.name IS NOT DISTINCT FROM way_name) DESC, b.fid
  LIMIT 1
$$;

-- Every cycling way in one tile layer, `bicycle_ways`, for both of the map's
-- looks: the infrastructure table's ways from z9, and from z12 every rideable
-- street with its score. From z14 each scored way's ends are cut into short
-- steps that meet the next way's colour halfway, since MapLibre draws
-- `line-gradient` only on GeoJSON.
DROP FUNCTION IF EXISTS bike_suitability_tiles(integer, integer, integer);
CREATE OR REPLACE FUNCTION cycling_tiles(z integer, x integer, y integer) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  WITH env AS (
    SELECT ST_TileEnvelope(z, x, y) AS e3857,
           ST_Transform(ST_TileEnvelope(z, x, y, margin => 64.0 / 4096), 4326) AS e
  ),
  scored AS (
    SELECT b.* FROM bike_suitability b, env WHERE z >= 12 AND b.geom && env.e
  ),
  infra AS (
    SELECT c.* FROM bicycle_ways c, env WHERE c.geom && env.e
  ),
  joined AS (
    SELECT COALESCE(c.osm_id, (s.fid - 1) / 4) AS osm_id, COALESCE(s.fid, c.osm_id * 4 + 1) AS fid,
           COALESCE(c.geom, s.geom) AS geom, COALESCE(c.name, s.name) AS name,
           COALESCE(c.highway, s.highway) AS highway, COALESCE(c.surface, s.surface) AS surface,
           COALESCE(c.bridge, s.bridge) AS bridge, COALESCE(c.tunnel, s.tunnel) AS tunnel,
           c.infra_type, c.cycleway, c.cycleway_left, c.cycleway_right, c.bicycle, c.state, c.oneway, c.elevation,
           s.facility, s.score, s.speed_kmh, s.lanes
    FROM scored s FULL JOIN infra c ON c.osm_id = (s.fid - 1) / 4
  ),
  ways AS (
    SELECT j.*,
           CASE WHEN fade THEN ST_Length(j.geom::geography) END AS len,
           14 * 40075016.7 * cos(radians(ST_Y(ST_StartPoint(j.geom)))) / (512 * 2 ^ z) AS fade_m,
           CASE WHEN fade THEN bike_neighbour_score(ST_StartPoint(j.geom), j.name, j.fid) END AS n0,
           CASE WHEN fade THEN bike_neighbour_score(ST_EndPoint(j.geom), j.name, j.fid) END AS n1
    FROM joined j, LATERAL (SELECT z >= 14 AND j.score IS NOT NULL AS fade) f
  ),
  ends AS (
    SELECT w.*,
           CASE WHEN abs(n0 - score) >= 4 THEN least(1.0 / 2.5, fade_m / nullif(len, 0)) ELSE 0 END AS f0,
           CASE WHEN abs(n1 - score) >= 4 THEN least(1.0 / 2.5, fade_m / nullif(len, 0)) ELSE 0 END AS f1,
           (n0 + score) / 2.0 AS mid0, (n1 + score) / 2.0 AS mid1
    FROM ways w
  ),
  pieces AS (
    SELECT e.*, CASE WHEN COALESCE(f0, 0) + COALESCE(f1, 0) = 0 THEN geom
      ELSE ST_LineSubstring(geom, COALESCE(f0, 0), 1 - COALESCE(f1, 0)) END AS piece, score::real AS s FROM ends e
    UNION ALL
    SELECT e.*, ST_LineSubstring(geom, f0 * i / 6.0, f0 * (i + 1) / 6.0), mid0 + (score - mid0) * (i + 0.5) / 6
    FROM ends e, generate_series(0, 5) i WHERE f0 > 0
    UNION ALL
    SELECT e.*, ST_LineSubstring(geom, 1 - f1 * (i + 1) / 6.0, 1 - f1 * i / 6.0), mid1 + (score - mid1) * (i + 0.5) / 6
    FROM ends e, generate_series(0, 5) i WHERE f1 > 0
  )
  SELECT ST_AsMVT(t, 'bicycle_ways', 4096, 'g', 'osm_id')
  FROM (
    SELECT ST_AsMVTGeom(ST_Transform(p.piece, 3857), env.e3857, 4096, 64, true) AS g, p.osm_id,
           p.name, p.highway, p.surface, p.bridge, p.tunnel,
           p.infra_type, p.cycleway, p.cycleway_left, p.cycleway_right, p.bicycle, p.state, p.oneway, p.elevation,
           p.facility, round(p.s)::smallint AS score, p.score AS way_score, p.speed_kmh, p.lanes
    FROM pieces p, env
  ) t
  WHERE g IS NOT NULL
$$;
