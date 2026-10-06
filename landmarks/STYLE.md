# Landmark art style

Landmarks sit between Apple Maps' 3D landmarks and the
[Open Landmarks](https://github.com/benjamintd/open-landmarks) dataset: clean
and slightly toy-like, true to the real building in silhouette and
proportion, in one calm shared palette, so that a city's landmarks look like
one set and sit quietly among the plain extrusions around them. A landmark
has to read as itself at a glance, from any side, on a phone at 80 to 600 px
tall.

From Apple: soft bevelled edges, big readable forms, a toy-like finish.
From Open Landmarks: the shared material library and its names, windows as
slate panels on the walls, no textures, and the asset contract, so every
model here can be contributed there unchanged.

This guide comes from reviewing models against Apple Maps, Open Landmarks
and photos. Every rule here fixes something that went wrong.

## Shape

- **Realistic but abstract.** Keep every architectural feature that makes the
  real building recognisable (its frame lines, setbacks, sloped or notched
  tops, fins, corner slots, the pattern its facade makes from a distance)
  and draw each as the simplest clean geometry that still shows it. The
  palette and panel rules below simplify how a building is drawn; they never
  remove what it is. If applying them makes a model look less like the
  photo, the feature was lost: put it back in abstract form.

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

- **Windows are slate panels on the walls.** Use the `window` material
  (`#64798a`) as flat panels set on, or just into, the wall they belong to
  (within 0.06 m of it, as Open Landmarks checks). They follow the real bay
  rhythm and storey groups: on a tower, one panel per bay spanning two to
  four floors, with the wall showing between bays and between groups as
  piers and spandrels. On a low building, a panel per real window or per
  arched opening.
- **Not a texture, not a stripe, not a dot grid.** No painted window grids;
  no full-height dark stripes from base to crown; no single-floor dot grids
  that shimmer at map distance. If the real building is a glass curtain
  wall, it is a `window` wall with a few pale `trim` mullion and floor lines,
  not a dark slab. Where the real glass reads light and reflective, use a
  lighter `windowVariant` (sky grey-blue, around `#a9bfd1`) so the tower
  reads light as it does in daylight; it still glows at night.
- **Panels never float.** A window panel needs real wall behind it: not past
  a corner, not across a gap, not on a roof.
- **Special windows are flush shapes.** Triangles, portholes and rosettes are
  flat `window` (or `glass`) geometry set into the wall, never spikes or
  cones standing proud.

## Colour

- **Start from the shared palette** in `scripts/landmarks/palette.ts`, the
  Open Landmarks material library: `stone` `#efe4d3`, `trim` `#fff0dd`,
  `roof` `#9da6ad`, `metal`, `window` `#64798a`, `glass` `#a4c4d9`,
  `entrance`, `copper`, `patina`, `terracotta`. Most masonry is `stone`; most
  roofs and setback terraces are `roof`.
- **A landmark-specific finish only where colour is identity.** Rose granite,
  red brick, a gold dome, painted red steel, a green copper roof: use
  `finish()` with the photo's hue, but pulled to the palette's lightness, so
  it reads as "the pink one" without being darker or more saturated than the
  stone beside it. A finish replaces a library colour; it doesn't add
  contrast. Large dark surfaces (navy glass curtain walls, dark brown
  brick, black stone) are pulled light. A dark colour that is itself a
  defining feature (a stadium's black towers, a dark spire against pale
  roofs) stays dark enough to read, but no darker than charcoal `#4a4f57`.
- **At most six materials**, each covering a broad region. One colour for
  the whole body is also wrong: walls, windows, trim and roof at least.
- Roof gardens are muted green as a finish; real copper, slate or tile roofs
  use `copper`, `patina` or `roof`.

## Budget

- At most 5,000 triangles for most landmarks; 6,500 for the most complex,
  and never over Open Landmarks' low-LOD cap of 8,000 triangles and 250 KB.
- No textures. Open Landmarks allows none, and its validator rejects them.
- Use 8 to 12 segments per semicircle and 12 to 16 around a full circle.

## How the map lights it

Parchment shades a landmark like its buildings:

- a face turned from the sun keeps about 72% of its colour, so contrast is flat;
- faces pointing up are lifted slightly;
- colours go to the screen as sRGB with no tone mapping;
- there is no texture filtering for geometry, so fine high-contrast detail
  aliases.

Shadows fall on the ground under the model.

## Night

The map lights a landmark at night by glTF material name, the same
convention as Open Landmarks. Colours stay as they are by day; the name
only decides what glows.

- **`window`**: facade windows, which glow warm at night. Punched windows,
  window bands, arched and rose windows, clerestories, and the curtain wall
  of an office or residential tower, which is lit in reality. Names are
  unique in a GLB, so a model with several window colours names the rest
  `window-2`, `window-3` and so on: any name starting with `window` is a
  window.
- **`glass`**: structural glazing that stays dark: atria, skylights, glass
  roofs and crowns, the Rose Center's cube.
- **`entrance`**: a main entrance's doors or glazing, which glow at night.
  Only where the model already has a separate material for it.

Everything else is unlit. (Parchment also glows painted `window*` textures
by their alpha, for older models; new models have no textures.)

## Animation (experimental)

A few landmarks really move, and the Wonder Wheel turns. This is opt-in
and outside the Open Landmarks v1 contract. A model that moves must still
read as itself standing still, because that is all a client without
animation will draw.

- **Rigid nodes only.** A moving part is its own glTF node, moved by one
  standard glTF `animation` that sets the node's `rotation` or
  `translation`, with LINEAR keyframes. No skinning, no morph targets, no
  `scale` channels and no cubic splines.
- **The pivot is the node's origin.** Translate the node to the axle or
  hinge and author its vertices relative to that point. A child node moves
  with its parent. The Wonder Wheel's cars are children of the wheel, each
  turned back by as much as the wheel turns, so they hang plumb.
- **Everything else stays in the root node**, untransformed, as in a static
  model.
- **Frame 0 is the static model.** The first keyframe matches the nodes'
  own transforms. Previews, clients set to reduced motion and the server's
  bounds all use that pose.
- **One loop, a few minutes at most.** Keyframe times are in seconds, and
  the last keyframe matches the first so the loop is seamless. A real
  speed is often too slow to see on a map, so pick a livelier one and say
  so in the generator. The Wonder Wheel takes 90 s a turn, against the
  real wheel's 8 to 10 minutes.
- **Same budget.** The triangle count is what is drawn, counting a mesh
  shared by several nodes once per node.

The server sizes a moving node as a sphere about its pivot that holds it
in any pose, so its `height` can come out a little over the real one.

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
