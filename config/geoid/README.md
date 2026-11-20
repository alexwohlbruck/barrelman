# Geoid data

What `src/lib/bridge-decks/datum.ts` reads to convert an OSM `ele`, which is
metres above the EGM96 geoid, into the vertical datum of the terrain that
bridge decks stand on. `scripts/build-geoid-gap.ts` builds both files from the
sources below. Rebuild them only when one of those sources changes.

## `conus-gap.bin.gz`

GEOID18 N − EGM96 N, the height of NAVD88's zero above EGM96's, over the
conterminous US: 1181 × 521 nodes, 0.05° apart, from 125°W 24°N to 66°W
50°N. Each value is a little-endian int16 in centimetres. Rows run south to
north and each row runs west to east. Values are stored as differences from
the previous node in the row, and −32768 marks a node with no value (no
GEOID18 there). The file is gzipped. Read bilinearly, it is within 0.007 m
rms of the full models and 0.12 m at worst, in the steepest mountains. The
difference changes by millimetres per kilometre, so a bridge never sees the
grid's spacing.

- **GEOID18** (CONUS, `g2018u0`), U.S. National Geodetic Survey, NOAA. Public
  domain (U.S. Government work). Taken from PROJ-data's GeoTIFF conversion
  `us_noaa_g2018u0.tif` (<https://cdn.proj.org/us_noaa_g2018u0.tif>), a 1′
  grid of the NGS `.bin` (<https://geodesy.noaa.gov/GEOID/GEOID18/>).
- **EGM96**, NGA/NASA. Public domain. Taken from GeographicLib's 5′ grid
  `egm96-5.pgm` (<https://geographiclib.sourceforge.io/C++/doc/geoid.html>),
  read bilinearly. This is GeographicLib's GeoidEval without `-c`, which is
  what the values tagged in OSM were computed with.

## `conus-not-us.json`

The land of other countries inside that box (Canada, Mexico and the Bahamas),
by ISO code. A point there is left unconverted, because Mapterhorn's terrain
there is not on NAVD88. These are rings of [lng, lat] pairs, read even–odd.
They are clipped to the box and simplified to about 200 m, and islets under
about 1 km² are left out.

- **Natural Earth** 1:10m Admin 0 – Countries, v5.1.2
  (<https://www.naturalearthdata.com>). Public domain.

## Which terrain is on which datum

Mapterhorn does not convert vertical datums (mapterhorn/mapterhorn#255). Each
source keeps its own datum:

| Where | Mapterhorn source | Vertical datum | `ele` converted |
|---|---|---|---|
| Conterminous US | USGS 3DEP 1 m and 1/3″ DEMs (`us1*`, `usgs3dep13`) | NAVD88, per the USGS product metadata | Yes, by GEOID18 − EGM96 |
| Canada | NRCan HRDEM (`cahrdem2`) | CGVD2013 | No. CGG2013a − EGM96 is about 1.0–1.25 m in its cities |
| Other national DEMs | per country | the national datum | No |
| Elsewhere | Copernicus GLO-30 (`glo30`) | EGM2008 | No. EGM2008 − EGM96 is mostly under 0.5 m |

Alaska, Hawaii and the territories are outside the box. They are not
converted.
