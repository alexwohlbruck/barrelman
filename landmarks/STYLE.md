# Landmark art style

Landmarks look like the 3D landmarks in Apple Maps: clean and slightly
toy-like, but true to the real building in silhouette, proportion and
colour. A landmark has to read as itself at a glance, from any side, on a
phone at 80 to 600 px tall, next to the plain extruded buildings around it.

This guide comes from reviewing the first New York models against Apple
Maps and against photos. Every rule here fixes something that went wrong.

## Shape

- **Get the silhouette right first.** Overall proportions, setbacks, the
  crown and the spire are what people recognise. Match the footprint and
  heights of the OSM building and parts the model replaces, and use real
  dimensions for everything OSM doesn't record.
- **Use the real geometry of a feature.** A semicircular arch is a
  semicircle, not an oval or a pointed arch. A square plan is square. If the
  real building is symmetrical, the model is too: mirror its features rather
  than modelling one side.
- **Nothing hangs off the building.** Ornament stays inside the face it
  belongs to. An arch on a narrower face, a rim, a fin or a window must not
  poke past a corner or an edge. Only things that really project, such as
  the Chrysler's eagles, project.
- **Bevel the edges.** Piers, setback lips, parapets and cornices get a small
  chamfer or rounded bevel, about 0.3 to 0.6 m and smooth-shaded, so edges
  catch a highlight. Sharp boxes look like extrusions; soft edges are what
  make it feel finished.
- **Big readable forms, little detail.** No mullions, ledges, thin ribs,
  railings, fine sculpture or lettering. If it won't read at 200 px on a
  phone, leave it out.

## Ground

- **Build up from the lowest ground the building touches.** The model's
  y = 0 is the lowest terrain point under its footprint. The map finds that
  point itself, from its own terrain, so `elevation` stays 0: it is only
  extra lift for a model standing on something the terrain doesn't know
  about, like Liberty on Fort Wood. On a slope, the walls on the
  uphill side run down to y = 0 too, so they sink into the hill rather than
  leaving the downhill side floating over a gap. Never put y = 0 at the
  anchor's own ground height, or at the average.
- **Nothing important in the bottom of the uphill walls.** Doors, plinths
  and the first band of windows on the uphill side end up below ground, so
  start the facade detail at that side's real ground level.

- **Don't model the ground.** Grass, playing fields, infield dirt, sand,
  paving and water surfaces are already on the map, and a second copy of
  them sits a few centimetres above the terrain and flickers or floats on
  a slope. A stadium is its stands, roofs and towers; the pitch is the
  map's.

## Windows and facades

- **Windows are geometry, not texture.** A facade is a few broad window
  bands recessed 0.4 to 0.8 m between raised piers, following the real bay
  rhythm: typically four to eight bands per face per setback stage, running
  the stage's full height. Give them arched or flat tops where the real
  building has them.
- **Never a grid of dots or thin dark stripes.** Fine window grids shimmer
  at map distance, and thin dark bars read as a cage.
- **Special windows are flush, coloured shapes.** Triangles, portholes or
  rosettes are flat inset geometry in a darker material, set into the band
  they belong to. They are not spikes, cones or anything standing proud.

## Colour

- **Take colours from photos of the real building** in daylight: the sunlit
  side, unmodified. Don't lighten or desaturate them to suit the map; a
  landmark should look like the building, not like the plain buildings
  around it.
- **Three to six materials**, each covering a broad region: main walls,
  window bands, trim, metal, roof. One colour for the whole body is wrong.
- **Apple's conventions:**
  - window bands are a soft mid grey or grey-blue (darker where the real
    glass reads dark);
  - setback terraces on towers are a muted terracotta, about `#c8968a`,
    unless photos show a real finish (56 Leonard's are pale concrete).
    Only the terraces: on a low building the main roof is most of what a
    phone sees, and a terracotta one swamps it;
  - a main flat roof is a pale membrane grey, about `#bdb9b1`, unless photos
    show otherwise. A monument's roof is its own stone, a shade darker; real
    copper, slate or tile roofs keep their colour;
  - real roof gardens are muted green;
  - metal is a light silver grey with darker grey accents.

## Budget

- At most 5,000 triangles for most landmarks; 6,500 for the most complex.
- At most about 250 KB per GLB.
- No textures unless geometry genuinely can't do it.
- Use 8 to 12 segments per semicircle and 12 to 16 around a full circle.

## How the map lights it

Parchment shades a landmark like its buildings:

- a face turned from the sun keeps about 72% of its colour, so contrast is flat;
- faces pointing up are lifted slightly;
- colours go to the screen as sRGB with no tone mapping;
- there is no texture filtering for geometry, so fine high-contrast detail
  aliases.

Shadows fall on the ground under the model.

## Checking a model

Run `bun scripts/landmarks/preview.ts <model.glb> <out-dir> [photo.png ...]`.
It renders the model with the map's lighting from the south, west, a high
south-west three-quarter (the usual phone view), the north-east, above, and
at phone sizes of 200 and 80 px. Give it PNG photos to get side-by-side
comparisons. Look at every view and check:

- the silhouette and proportions match the photos;
- the colours match the photos, material by material;
- it is symmetrical wherever the building is;
- nothing pokes past an edge or a corner;
- it still reads as itself in the 80 px view.
