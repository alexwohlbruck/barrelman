"""segments.emit — write transit_line_segments (stage 6, phase A).

Idempotent DDL + delete-and-replace per build_key in ONE transaction,
same conventions as linegraph.emit. NULL semantics by kind:
  steady      -> offset_px set, off_from_px/off_to_px NULL
  transition  -> offset_px NULL, off_from_px/off_to_px set

offset_px / off_from_px / off_to_px are AUTHORITATIVE, expressed in each
feature's own travel frame (geometry direction). slot / line_count are
informational: slot is mirrored into the same frame, so on steady rows
offset_px == (slot - (line_count-1)/2) * gap_px holds, but consumers
(Phase-B tile functions included) must not re-derive offsets from slot —
merged transitions can carry an interior bundle's slot.

Zoom bands: the build emits the COMPLETE feature set once per transition-
length band (SegmentConfig.bands). band_minzoom/band_maxzoom partition
the zoom axis (maxzoom = next band's minzoom - 1, top band 99), so
transit_lines_rt2 selects exactly one band per request zoom with
`z BETWEEN band_minzoom AND band_maxzoom`. seg_id is unique per
(build_key, band_minzoom); the SERIAL id stays the MVT feature id.
The delete-and-replace per build_key covers ALL bands.
"""

from __future__ import annotations

DDL = """
CREATE TABLE IF NOT EXISTS transit_line_segments (
  id SERIAL PRIMARY KEY,
  build_key TEXT NOT NULL,
  band_minzoom INTEGER NOT NULL DEFAULT 15,
  band_maxzoom INTEGER NOT NULL DEFAULT 99,
  seg_id INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('steady', 'transition')),
  color_key TEXT NOT NULL,
  route_short_names TEXT,
  route_ids TEXT,
  feed_id TEXT,
  route_type INTEGER,
  route_color TEXT,
  route_text_color TEXT,
  slot INTEGER NOT NULL,
  line_count INTEGER NOT NULL,
  offset_px DOUBLE PRECISION,
  off_from_px DOUBLE PRECISION,
  off_to_px DOUBLE PRECISION,
  len_m DOUBLE PRECISION NOT NULL,
  geom geometry(LineString, 4326)
);
-- pre-band tables: add the band columns + retire the single-band unique
ALTER TABLE transit_line_segments
  ADD COLUMN IF NOT EXISTS band_minzoom INTEGER NOT NULL DEFAULT 15;
ALTER TABLE transit_line_segments
  ADD COLUMN IF NOT EXISTS band_maxzoom INTEGER NOT NULL DEFAULT 99;
ALTER TABLE transit_line_segments
  DROP CONSTRAINT IF EXISTS transit_line_segments_build_key_seg_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS transit_line_segments_bk_band_seg_uidx
  ON transit_line_segments (build_key, band_minzoom, seg_id);
CREATE INDEX IF NOT EXISTS transit_line_segments_geom_idx
  ON transit_line_segments USING GIST (geom);
CREATE INDEX IF NOT EXISTS transit_line_segments_build_key_idx
  ON transit_line_segments (build_key);
"""


def _ewkt_line(coords) -> str:
    # 15 dp (~0.1 um ground) round-trips the builder's float64 coords:
    # coarser quantization (7 dp ~ 1 cm) put micro-kinks on fillet
    # vertices spaced 8-22 cm apart, breaking the served rows' curvature
    # floor even though the in-memory geometry met it.
    return ("SRID=4326;LINESTRING("
            + ",".join(f"{lon:.15f} {lat:.15f}" for lon, lat in coords)
            + ")")


def emit_segments(band_segments, *, build_key: str, dsn: str) -> int:
    """Delete-and-replace ALL of the build_key's segment rows (every
    band) in one transaction. band_segments: [(band_minzoom,
    band_maxzoom, segments), ...]. Returns total row count."""
    import psycopg  # optional dep, --emit only

    n = 0
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("DELETE FROM transit_line_segments WHERE build_key = %s",
                    (build_key,))
        with cur.copy(
            "COPY transit_line_segments (build_key, band_minzoom,"
            " band_maxzoom, seg_id, kind, color_key, route_short_names,"
            " route_ids, feed_id, route_type, route_color,"
            " route_text_color, slot, line_count, offset_px, off_from_px,"
            " off_to_px, len_m, geom) FROM STDIN"
        ) as copy:
            for band_minzoom, band_maxzoom, segments in band_segments:
                for s in segments:
                    if len(s.coords) < 2:
                        continue
                    copy.write_row((
                        build_key, band_minzoom, band_maxzoom, s.seg_id,
                        s.kind, s.color_key, s.route_short_names,
                        s.route_ids, s.feed_id, s.route_type,
                        s.route_color, s.route_text_color, s.slot,
                        s.line_count, s.offset_px, s.off_from_px,
                        s.off_to_px, s.len_m, _ewkt_line(s.coords)))
                    n += 1
        conn.commit()
    return n
