-- Codes (IATA, ICAO, ref, short_name, alt_name) and multi-word-name
-- abbreviations, one pass.
--
-- The two touch heavily overlapping row sets, so as separate statements they
-- rewrote many named rows twice. The guards keep each column stable unless its
-- own derived value moved — an abbreviation-only change does not clobber codes
-- with NULL, and vice versa.
--
-- Run AFTER post-import.sql, BEFORE intersections and parent context (the
-- tsvector consumes name_abbrev).
UPDATE geo_places g
SET codes = CASE WHEN s.codes IS NOT NULL THEN s.codes ELSE g.codes END,
    name_abbrev = CASE WHEN s.abbrev IS NOT NULL THEN s.abbrev ELSE g.name_abbrev END
FROM (
  SELECT coalesce(c.id, a.id) AS id, c.codes, a.abbrev
  FROM (
    SELECT id,
      array_agg(DISTINCT lower(trim(code))) FILTER (WHERE trim(code) <> '') AS codes
    FROM geo_places,
    LATERAL unnest(
      string_to_array(coalesce(tags->>'iata', ''), ';') ||
      string_to_array(coalesce(tags->>'icao', ''), ';') ||
      string_to_array(coalesce(tags->>'ref', ''), ';') ||
      string_to_array(coalesce(tags->>'short_name', ''), ';') ||
      string_to_array(coalesce(tags->>'abbreviation', ''), ';') ||
      string_to_array(coalesce(tags->>'alt_name', ''), ';')
    ) AS code
    WHERE tags IS NOT NULL
      AND (
        tags->>'iata' IS NOT NULL OR
        tags->>'icao' IS NOT NULL OR
        tags->>'ref' IS NOT NULL OR
        tags->>'short_name' IS NOT NULL OR
        tags->>'abbreviation' IS NOT NULL OR
        tags->>'alt_name' IS NOT NULL
      )
    GROUP BY id
  ) c
  FULL OUTER JOIN (
    SELECT id,
      lower(string_agg(left(word, 1), '' ORDER BY ord)) AS abbrev
    FROM (
      SELECT id, word, ord
      FROM geo_places,
      LATERAL unnest(regexp_split_to_array(name, '\s+')) WITH ORDINALITY AS t(word, ord)
      WHERE name IS NOT NULL
        AND name ~ '^[\w\s\d\-''\.&]+$'
    ) words
    WHERE lower(word) NOT IN (
      'of','the','and','at','in','for','a','an',
      'de','la','le','les','du','des','et','au',
      'der','die','das','von','und','im','am',
      'del','los','las','el','dos','e',
      'di','della','dei','degli'
    )
    AND length(word) > 0
    GROUP BY id
    HAVING count(*) >= 2
  ) a ON a.id = c.id
) s
WHERE g.id = s.id
  AND ((s.codes IS NOT NULL AND g.codes IS DISTINCT FROM s.codes)
    OR (s.abbrev IS NOT NULL AND g.name_abbrev IS DISTINCT FROM s.abbrev));
