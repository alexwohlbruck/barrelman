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

## Building a famous or intricate model

Simple buildings can be modelled straight from OSM and a photo or two.
Famous buildings, coasters, towers, rides and sculpture can't: people know
exactly what they look like, and a guess shows. The models that worked here
were all built this way; the ones that failed skipped a step.

### Gather evidence before modelling

Collect at least three kinds of evidence. Write down where each fact comes
from, in the generator's header.

- **OSM.** The footprint, the building parts and their heights, and for rides
  the `roller_coaster=track` ways and the station. Fetch them with the OSM
  API's map call; Overpass is unreliable. Treat OSM as exact where it is
  tagged and suspect everywhere else:
  - heights are often missing or wrong;
  - names are sometimes on the wrong building;
  - a coaster's trace can be simplified or turn the wrong way.
- **Published dimensions.** Height, length, drop, angle, capacity, element
  order. Sources:
  - Wikipedia;
  - RCDB and Coasterpedia for rides;
  - the National Register of Historic Places and local landmark commission
    reports for historic buildings.

  Prefer a published number to a measured one.
- **Photos from several sides.** Wikimedia Commons and Openverse
  (CC-licensed Flickr) for licensed photos, Mapillary for street level. Aim
  for at least one photo per side you model, plus one from above or far
  away.
  - Overhead shots (aerials, or the view from a nearby tower or wheel) are
    the most valuable. They show the plan, the roof and the relative heights
    in one picture.
- **Public-domain aerials.** USGS NAIP orthoimagery (US) and state
  orthophotos give the true plan and roof colours, and shadows give heights.
- **Look-only references.** News, venue and agency photos, and historic
  postcards, may be looked at and described but never copied. Note them
  separately in the header: a model resting on them isn't clean for Open
  Landmarks until licensed photos confirm it.
- **Commercial maps (Google, Apple, Mapbox).** Their imagery and 3D tiles
  are reference at most, and only when nothing else exists.
  - A model shaped against them can't be contributed to Open Landmarks. Say
    so in its header.
  - Never copy their geometry.

### Build in order: massing, then the identifying features, then the facade

1. **Massing.** Block out the plain volumes only: tiers, setbacks, the crown
   envelope, a ride's whole track. Render with `preview.ts` and put each
   view beside a photo taken from about the same direction. Fix proportions
   until the outline matches in every view you have evidence for. Most
   rejected models went wrong here and were never corrected, however much
   detail followed.
2. **The identifying features.** List the three to five things that make
   the landmark recognisable, from the photos, before modelling them. For
   example:
   - Bank of America's silver crown cage;
   - Duke Energy's stone frames, V crown and corner slots;
   - the Cyclone's sign;
   - the SkyTower's flag cabin.

   Model each as clean, simple geometry, then check each one against the
   photo it came from.
3. **The facade,** per the rules above: window bays, colour, trim.
4. **Phone size.** Check the 200 and 80 px views. If the identifying
   features vanish, make them bigger or bolder, not more detailed.

### Compare like with like

Put the photo and the render side by side at the same angle, rendering
the model from the photo's viewpoint if `preview.ts`'s fixed views don't
match. A render from the wrong side proves nothing. Name
what differs before calling a model done. Every review here found something
the builder hadn't seen.

### Roller coasters and other rides

Use `scripts/landmarks/coaster-kit.ts`. A coaster's generator should be
almost all data.

- **Plan.**
  - Start from the OSM track ways, chained in ride direction, and check the
    chain against an aerial.
  - If the trace is simplified, too short or turns the wrong way, redraw it
    over a public-domain orthophoto. Keep OSM's turnarounds where the photo
    confirms them. The real track length is the check.
- **Element list.**
  - Write the published elements in ride order: lift, drops, hills, turns,
    inversions, brakes.
  - Place each at a position along the track from its shape in plan.
    Turnarounds are the tight curves, and an airtime hill is a straight
    stretch between them.
  - Use published heights, measured above the local ground, which may slope.
- **Energy check.** The kit checks that no hill is higher than the train can
  reach, allowing for friction of 2 to 3% of the lift height per 100 m, and
  that it never stalls. Modelled top speed and track length should land
  within a few percent of the published figures.
- **Smoothness.**
  - Heights must be smooth over the track's length: crests are rounded,
    drops have a straight steep section, and pull-outs have a large radius.
  - Banking follows speed and curvature, and changes gradually.
  - The kit flags kinks and sudden twists. Fix every flag: a kinked first
    drop is the first thing anyone notices.
- **Structure.** The track is a broad ribbon in its real rail colours, never
  two thin rails. Supports are chunky columns or A-frames, never lattice. A
  wooden coaster's trestle is broad walls with large regular openings.
- **Photo matching.** Match renders to at least two photos from known
  positions, and check the hills line up.

### Towers, rides, sculpture and signs

- **Small, famous objects** (sculpture, statues, signs, vehicles) can spend
  more of their budget on detail. Keep every part a bold, simple mass that
  reads at 200 px. Real dimensions and the real pose matter more than
  surface detail.
- **Moving rides** (wheels, observation cabins, rotating sculpture): build
  the moving part as its own node from the start, even if it ships static,
  so it can be animated later (see Animation). Give it a detail that shows
  the motion, such as the flag stripes on the SkyTower's cabin.
- **Famous signs** (the Cyclone, a theatre marquee) may carry their
  lettering when the sign is the landmark. Build the letters as simple
  extruded block shapes on a flat panel, not a texture, and note the
  exception in the generator. Other marquees and signs are plain boxes.

### Report the doubts

Every generator header lists what is measured, what is published, what is
estimated and what is invented. Name the estimates: a height from a shadow,
a side no photo shows, an OSM tag that looked wrong. Whoever picks the model
up next, or reviews it for Open Landmarks, needs to know which parts to
trust.

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
