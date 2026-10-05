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
  - flat roofs and setback terraces are a muted terracotta, about `#c8968a`;
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
