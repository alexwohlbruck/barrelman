# Vendored map styles

`osm-bright.json` and `dark-matter.json` are copies of the OpenMapTiles styles
below, served by `/styles/basemap.json` and `/styles/basemap-dark.json`.

| File | Upstream | Commit |
|---|---|---|
| `osm-bright.json` | https://github.com/openmaptiles/osm-bright-gl-style | 563b249f7ae71528b1f1e327cb9c019d0dda4c50 |
| `dark-matter.json` | https://github.com/openmaptiles/dark-matter-gl-style | d17442cb66bad8c82bda59d199d5dafeead410cf |

Changes made here:

- `dark-matter.json`: the Metropolis fonts are replaced with Noto Sans
  (`Metropolis Light` and `Metropolis Regular` with `Noto Sans Regular`, the
  italics with `Noto Sans Italic`), because the glyphs we vendor
  (`assets/fonts`) are Noto Sans only.
- The sprites both styles use are vendored in `assets/sprites`; see
  `assets/LICENSE.md`.
- At request time (`src/routes/styles.ts`), both styles get their source, glyph
  and sprite URLs replaced, a new `name`, and licence metadata.

Both upstream licences follow, unchanged.

## OSM Bright

Copyright (c) 2024, MapTiler.com & OpenMapTiles contributors.  
Copyright (c) 2014, Mapbox.  
All rights reserved.

Modifications by MapTiler.com & OpenMapTiles contributors.  
Derived from "Mapbox Open Styles" https://github.com/mapbox/mapbox-gl-styles

# Code license: BSD 3-Clause License

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Design license: CC-BY 4.0

The visual design features of the style (also known as the "look and feel" of
the map) are licensed under the Creative Commons Attribution 4.0 license.

To view a copy of the license, visit https://creativecommons.org/licenses/by/4.0/.

Attribution for the design defined in this repository needs not to be provided
on map images, but should be reasonably accessible from maps based on this style
(for example, in a webpage linked from copyright notice on the map).

Products or services using maps derived from OpenMapTiles schema need to visibly
credit "OpenMapTiles.org" or reference "OpenMapTiles" with a link to
https://openmaptiles.org/.

For a browsable electronic map based on OpenMapTiles and OpenStreetMap data, the
credit should appear in the corner of the map. For example:

[© OpenMapTiles](https://openmaptiles.org/)
[© OpenStreetMap contributors](https://www.openstreetmap.org/copyright)

For printed and static maps a similar attribution should be made in a textual
description near the image, in the same fashion as if you cite a photograph.

Exceptions to OpenMapTiles attribution requirement can be in a written form granted
by MapTiler AG (info@maptiler.com).  
The project contributors grant MapTiler AG the license to give such
exceptions on a commercial basis.

## Dark Matter

Copyright (c) 2024, MapTiler.com & OpenMapTiles contributors.  
Copyright (c) 2015, CartoDB Inc.  
All rights reserved.

Derived from "CartoDB Basemaps" (https://github.com/CartoDB/CartoDB-basemaps)
designed by Stamen and Paul Norman for CartoDB Inc., licensed under CC-BY 3.0.

# Code license: BSD 3-Clause License

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Design license: CC-BY 4.0

The visual design features of the style (also known as the "look and feel" of
the map) are licensed under the Creative Commons Attribution 4.0 license.

To view a copy of the license, visit https://creativecommons.org/licenses/by/4.0/.

Attribution for the design defined in this repository needs not to be provided
on map images, but should be reasonably accessible from maps based on this style
(for example, in a webpage linked from copyright notice on the map).

Products or services using maps derived from OpenMapTiles schema need to visibly
credit "OpenMapTiles.org" or reference "OpenMapTiles" with a link to
https://openmaptiles.org/.

For a browsable electronic map based on OpenMapTiles and OpenStreetMap data, the
credit should appear in the corner of the map. For example:

[© OpenMapTiles](https://openmaptiles.org/)
[© OpenStreetMap contributors](https://www.openstreetmap.org/copyright)

For printed and static maps a similar attribution should be made in a textual
description near the image, in the same fashion as if you cite a photograph.

Exceptions to OpenMapTiles attribution requirement can be in a written form granted
by MapTiler AG (info@maptiler.com).  
The project contributors grant MapTiler AG the license to give such
exceptions on a commercial basis.
