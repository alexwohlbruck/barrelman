# Landmarks

Hand-made 3D models that stand in for a building's extrusion on the map, the
way Apple Maps and Mapbox draw the Eiffel Tower as a tower rather than a box.

Barrelman syncs `catalog.json` into the database at startup and serves:

- `GET /tiles/landmarks/{z}/{x}/{y}`: a vector tile with one `landmarks`
  point layer, one point per placement.
- `GET /tiles/landmarks/models/{id}.{sha12}.glb`: the models. The file name
  includes a hash of the content, so these are served as immutable.

## Models and placements

The catalog keeps two lists, because a model and the places it stands are
different things.

A **model** is a GLB file plus its `license`, `author` and `source`. It has to
follow the frame contract below.

A **landmark** places a model. It gives:

- `lng`/`lat` of the anchor
- `bearing`: degrees clockwise from north that the model's north is turned to
- `scale`, plus an optional `elevation` and `minzoom`
- `replaces`: the OSM elements it stands in for

The Eiffel Tower model is placed twice: in Paris, and on the Las Vegas Strip
at `scale: 0.5`. The Statue of Liberty is placed on Liberty Island, with
`elevation: 10` so its pedestal rests on Fort Wood's map geometry. Its five
`replaces` entries hide only the pedestal parts, preserving the star-shaped fort.

## The frame contract

Every model uses the same frame, so a client can place it from a position, a
bearing and a scale alone:

- **+Y is up, -Z is north, +X is east.**
- **Units are metres.**
- **The origin is the anchor, on the ground.** The model stands on y = 0.
- **There is no transform on the root node.** The server sizes models from the
  POSITION accessors' min/max, which ignores node transforms.

## What `replaces` must list

The client hides buildings by OSM id. List the outline and every
`building:part` inside it, written as `way/123` or `relation/456`.

Detailed buildings are mapped twice in OSM: an outline over the whole
footprint, and `building:part` polygons inside it. A ref left off this list
keeps drawing, and shows through the model as a box.

To find the parts, select the outline in the iD editor and look at what sits
inside it, or ask Overpass:

```
way(5013364);map_to_area->.a;(way(area.a)["building:part"];);out ids tags;
```

Some parts are separate buildings that happen to sit inside the outline, such
as the ticket booths under the Eiffel Tower. Leave those off if the model
doesn't cover them.

## Making a model

Follow the art style in [`STYLE.md`](STYLE.md), and check every model with `scripts/landmarks/preview.ts` before adding it.

Keep models stylised, simple and accurate. A few thousand triangles is plenty.
For repeated detail like lattices, railings and window grids, use an
alpha-masked texture (`alphaMode: MASK`) on a few quads instead of real
geometry. It looks like the real thing at map distances, and it carries into
shadows.

`scripts/landmarks/mesh.ts` is a small, dependency-free kit for building
models in code: lofts, slabs, swept tubes, PNG masks and a GLB writer.
`scripts/landmarks/eiffel-tower.ts` is the worked example. It is the source of
truth for its asset, so to change the tower, edit the script and regenerate:

    bun scripts/landmarks/eiffel-tower.ts

The Statue of Liberty generator uses the same kit, with smooth, creased
copper forms and a flat-shaded granite pedestal:

    bun scripts/landmarks/statue-of-liberty.ts

Its local origin is the centre of the pedestal base on top of Fort Wood. The
heel is at model height 36.9 m and the flame tip at 83 m. Before placement she
faces south, with the raised torch on the west side; the catalog applies the
327° bearing. Like the Eiffel Tower generator, this is an offline asset-authoring
script, not an operational data-refresh task in the admin console.

Models from Blender or another tool work too, as long as they follow the
frame contract and carry a licence the catalog can state.
