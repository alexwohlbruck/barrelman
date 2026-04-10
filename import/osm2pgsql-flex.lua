-- osm2pgsql flex output configuration
-- Imports ALL OSM objects into a single geo_places table
-- Two geometry columns: centroid (Point) and geom (real shape)

local places = osm2pgsql.define_table({
    name = 'geo_places',
    ids = { type = 'any', id_column = 'osm_id', type_column = 'osm_type' },
    columns = {
        { column = 'id', type = 'text', not_null = true },
        { column = 'name', type = 'text' },
        { column = 'names', sql_type = 'text[]' },
        { column = 'tags', type = 'jsonb', not_null = true },
        { column = 'categories', sql_type = 'text[]' },
        { column = 'centroid', type = 'point', projection = 4326, not_null = true },
        { column = 'geom', type = 'geometry', projection = 4326, not_null = true },
        { column = 'geom_type', type = 'text', not_null = true },
        { column = 'admin_level', type = 'int' },
    },
})

-- ============================================================
-- Bicycle infrastructure tables
-- ============================================================

local bicycle_ways = osm2pgsql.define_table({
    name = 'bicycle_ways',
    ids = { type = 'way', id_column = 'osm_id' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'infra_type', type = 'text', not_null = true },
        { column = 'highway', type = 'text' },
        { column = 'cycleway', type = 'text' },
        { column = 'cycleway_left', type = 'text' },
        { column = 'cycleway_right', type = 'text' },
        { column = 'bicycle', type = 'text' },
        { column = 'surface', type = 'text' },
        { column = 'state', type = 'text' },  -- nil, 'proposed', or 'construction'
        { column = 'oneway', type = 'int' },
        { column = 'bridge', type = 'bool' },
        { column = 'tunnel', type = 'bool' },
        { column = 'geom', type = 'linestring', projection = 4326, not_null = true },
    },
})

local bicycle_routes = osm2pgsql.define_table({
    name = 'bicycle_routes',
    ids = { type = 'relation', id_column = 'osm_id' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'ref', type = 'text' },
        { column = 'network', type = 'text' },
        { column = 'route_type', type = 'text' },
        { column = 'geom', type = 'multilinestring', projection = 4326, not_null = true },
    },
})

-- Classify the bicycle infrastructure type from OSM tags.
-- Returns (infra_type, state) where state is nil, 'proposed', or 'construction'.
-- Returns (nil, nil) if the way has no bicycle-relevant infrastructure.
local function derive_bicycle_infra_type(tags)
    local highway = tags['highway']
    local cycleway = tags['cycleway']
    local cycleway_left = tags['cycleway:left'] or tags['cycleway:both']
    local cycleway_right = tags['cycleway:right'] or tags['cycleway:both']
    local bicycle = tags['bicycle']

    -- Detect proposed / under-construction state
    local state = nil

    -- highway=proposed + proposed=cycleway  OR  highway=construction + construction=cycleway
    if highway == 'proposed' then
        if tags['proposed'] == 'cycleway' or tags['proposed:highway'] == 'cycleway'
            or tags['proposed:bicycle'] then
            return 'cycleway', 'proposed'
        end
        return nil, nil
    end
    if highway == 'construction' then
        if tags['construction'] == 'cycleway' or tags['construction:highway'] == 'cycleway' then
            return 'cycleway', 'construction'
        end
        return nil, nil
    end

    -- cycleway:*=proposed or cycleway:*=construction on existing roads
    if cycleway == 'proposed' or cycleway_left == 'proposed' or cycleway_right == 'proposed'
        or tags['proposed:cycleway'] then
        state = 'proposed'
    elseif cycleway == 'construction' or cycleway_left == 'construction' or cycleway_right == 'construction'
        or tags['construction:cycleway'] then
        state = 'construction'
    end

    -- Dedicated cycleway (highway=cycleway)
    if highway == 'cycleway' then return 'cycleway', state end

    -- Bicycle road / cycle street
    if tags['bicycle_road'] == 'yes' then return 'bicycle_road', state end
    if tags['cyclestreet'] == 'yes' then return 'cycle_street', state end

    -- Cycle track alongside road (physically separated)
    if cycleway == 'track' or cycleway_left == 'track' or cycleway_right == 'track' then
        return 'cycle_track', state
    end

    -- Cycle lane (painted on road)
    if cycleway == 'lane' or cycleway_left == 'lane' or cycleway_right == 'lane' then
        return 'cycle_lane', state
    end

    -- Shared lane (sharrows)
    if cycleway == 'shared_lane' or cycleway_left == 'shared_lane' or cycleway_right == 'shared_lane' then
        return 'shared_lane', state
    end

    -- Opposite direction cycling allowed
    if cycleway == 'opposite' or cycleway == 'opposite_lane' or cycleway == 'opposite_track' then
        return 'opposite', state
    end

    -- Road shoulder open to bikes
    if cycleway == 'shoulder' or tags['shoulder:bicycle'] == 'yes' then
        return 'shoulder', state
    end

    -- If we detected a proposed/construction state but didn't match an infra type above,
    -- treat it as a generic proposed/construction cycleway
    if state then return 'cycle_lane', state end

    -- Path / footway / bridleway designated for bicycles
    if (highway == 'path' or highway == 'footway' or highway == 'bridleway')
        and bicycle == 'designated' then
        return 'path_bicycle', nil
    end

    -- Steps with bicycle ramp
    if highway == 'steps' and (tags['ramp:bicycle'] == 'yes' or bicycle == 'yes' or bicycle == 'designated') then
        return 'steps_bicycle', nil
    end

    -- Bicycle designated on any road
    if bicycle == 'designated' and highway then return 'bicycle_designated', nil end

    -- Bicycle permitted on road
    if bicycle == 'yes' and highway then return 'bicycle_yes', nil end

    return nil, nil
end

-- Insert a way into the bicycle_ways table if it has bicycle infrastructure
local function try_insert_bicycle_way(object, tags, linestring)
    local infra_type, state = derive_bicycle_infra_type(tags)
    if not infra_type then return end

    local oneway_val = 0
    if tags['oneway'] == 'yes' or tags['oneway'] == '1' then oneway_val = 1
    elseif tags['oneway'] == '-1' then oneway_val = -1 end

    bicycle_ways:insert({
        name = tags['name'],
        infra_type = infra_type,
        highway = tags['highway'],
        cycleway = tags['cycleway'],
        cycleway_left = tags['cycleway:left'] or tags['cycleway:both'],
        cycleway_right = tags['cycleway:right'] or tags['cycleway:both'],
        bicycle = tags['bicycle'],
        surface = tags['surface'],
        state = state,  -- nil, 'proposed', or 'construction'
        oneway = oneway_val,
        bridge = (tags['bridge'] and tags['bridge'] ~= 'no') or false,
        tunnel = (tags['tunnel'] and tags['tunnel'] ~= 'no') or false,
        geom = linestring,
    })
end

-- ============================================================

-- POI-relevant tag keys for category derivation
-- These follow @openstreetmap/id-tagging-schema preset format
local POI_KEYS = {
    'amenity', 'shop', 'tourism', 'leisure', 'office', 'craft',
    'healthcare', 'social_facility', 'historic', 'man_made',
    'aeroway', 'public_transport', 'emergency', 'place',
    'building', 'natural', 'landuse', 'waterway', 'power',
    'railway', 'highway', 'barrier', 'entrance', 'playground',
    'club', 'gambling', 'advertising',
}

-- Derive categories from tags following osm-tagging-schema preset IDs
-- e.g., amenity=restaurant -> "amenity/restaurant"
local function derive_categories(tags)
    local cats = {}
    for _, key in ipairs(POI_KEYS) do
        local val = tags[key]
        if val and val ~= 'yes' and val ~= 'no' then
            cats[#cats + 1] = key .. '/' .. val
        elseif val == 'yes' and (key == 'building' or key == 'natural' or key == 'landuse') then
            -- For generic "building=yes" etc., include as category only if named
            if tags['name'] then
                cats[#cats + 1] = key
            end
        end
    end

    -- Also derive cuisine as a category for filtering
    if tags['cuisine'] then
        for cuisine in string.gmatch(tags['cuisine'], '[^;]+') do
            cuisine = cuisine:match('^%s*(.-)%s*$') -- trim
            cats[#cats + 1] = 'cuisine/' .. cuisine
        end
    end

    return cats
end

-- Extract all name variants
local function extract_names(tags)
    local names = {}
    local seen = {}

    local function add(val)
        if val and not seen[val] then
            names[#names + 1] = val
            seen[val] = true
        end
    end

    -- Primary names
    add(tags['name'])
    add(tags['alt_name'])
    add(tags['short_name'])
    add(tags['official_name'])
    add(tags['brand'])
    add(tags['operator'])
    add(tags['old_name'])

    -- Localized names (name:en, name:es, etc.)
    for key, val in pairs(tags) do
        if key:sub(1, 5) == 'name:' then
            add(val)
        end
    end

    return names
end

-- Build the text ID: "node/123456", "way/789", "relation/42"
local function make_id(object)
    return object.type .. '/' .. object.id
end

-- Format a Lua table as a PostgreSQL text[] literal: {"val1","val2"}
local function to_pg_array(tbl)
    if not tbl or #tbl == 0 then return '{}' end
    local escaped = {}
    for _, v in ipairs(tbl) do
        -- Escape backslashes and double quotes, then wrap in quotes
        local s = tostring(v):gsub('\\', '\\\\'):gsub('"', '\\"')
        escaped[#escaped + 1] = '"' .. s .. '"'
    end
    return '{' .. table.concat(escaped, ',') .. '}'
end

-- Get admin_level if this is an admin boundary
local function get_admin_level(tags)
    if tags['boundary'] == 'administrative' and tags['admin_level'] then
        return tonumber(tags['admin_level'])
    end
    return nil
end

-- Convert tags to JSON string for the jsonb column
local function tags_to_json(tags)
    local parts = {}
    for k, v in pairs(tags) do
        if k ~= 'created_by' and k ~= 'source' then
            local ek = tostring(k):gsub('\\', '\\\\'):gsub('"', '\\"'):gsub('\n', '\\n'):gsub('\r', '\\r'):gsub('\t', '\\t')
            local ev = tostring(v):gsub('\\', '\\\\'):gsub('"', '\\"'):gsub('\n', '\\n'):gsub('\r', '\\r'):gsub('\t', '\\t')
            parts[#parts + 1] = '"' .. ek .. '":"' .. ev .. '"'
        end
    end
    return '{' .. table.concat(parts, ',') .. '}'
end

function osm2pgsql.process_node(object)
    if not next(object.tags) then return end

    local tags = object.tags
    local geom = object:as_point()

    places:insert({
        id = make_id(object),
        name = tags['name'],
        names = to_pg_array(extract_names(tags)),
        tags = tags,
        categories = to_pg_array(derive_categories(tags)),
        centroid = geom,
        geom = geom,
        geom_type = 'point',
        admin_level = get_admin_level(tags),
    })
end

function osm2pgsql.process_way(object)
    if not next(object.tags) then return end

    local tags = object.tags

    if object.is_closed and tags['area'] ~= 'no' then
        -- Closed way = area (building, park, etc.)
        local polygon = object:as_polygon()
        if polygon:is_null() then return end
        local centroid = polygon:centroid()

        places:insert({
            id = make_id(object),
            name = tags['name'],
            names = to_pg_array(extract_names(tags)),
            tags = tags,
            categories = to_pg_array(derive_categories(tags)),
            centroid = centroid,
            geom = polygon,
            geom_type = 'area',
            admin_level = get_admin_level(tags),
        })
    else
        -- Open way = line (road, river, path, etc.)
        local linestring = object:as_linestring()
        if linestring:is_null() then return end
        local centroid = linestring:centroid()

        places:insert({
            id = make_id(object),
            name = tags['name'],
            names = to_pg_array(extract_names(tags)),
            tags = tags,
            categories = to_pg_array(derive_categories(tags)),
            centroid = centroid,
            geom = linestring,
            geom_type = 'line',
            admin_level = get_admin_level(tags),
        })

        -- Also insert into bicycle_ways if it has bicycle infrastructure
        try_insert_bicycle_way(object, tags, linestring)
    end
end

function osm2pgsql.process_relation(object)
    if not next(object.tags) then return end

    local tags = object.tags
    local rtype = tags['type']

    if rtype == 'multipolygon' or rtype == 'boundary' then
        local polygon = object:as_multipolygon()
        if polygon:is_null() then return end
        local centroid = polygon:centroid()

        places:insert({
            id = make_id(object),
            name = tags['name'],
            names = to_pg_array(extract_names(tags)),
            tags = tags,
            categories = to_pg_array(derive_categories(tags)),
            centroid = centroid,
            geom = polygon,
            geom_type = 'area',
            admin_level = get_admin_level(tags),
        })
    elseif rtype == 'route' then
        local line = object:as_multilinestring()
        if line:is_null() then return end
        local centroid = line:centroid()

        places:insert({
            id = make_id(object),
            name = tags['name'],
            names = to_pg_array(extract_names(tags)),
            tags = tags,
            categories = to_pg_array(derive_categories(tags)),
            centroid = centroid,
            geom = line,
            geom_type = 'line',
            admin_level = get_admin_level(tags),
        })

        -- Insert bicycle/mtb route relations into bicycle_routes
        if tags['route'] == 'bicycle' or tags['route'] == 'mtb' then
            bicycle_routes:insert({
                name = tags['name'],
                ref = tags['ref'],
                network = tags['network'],
                route_type = tags['route'],
                geom = line,
            })
        end
    elseif rtype == 'site' or rtype == 'associatedStreet' or rtype == 'building' then
        -- Site relations (campuses, etc.) - use first member's location as centroid
        local polygon = object:as_multipolygon()
        if polygon:is_null() then return end
        local centroid = polygon:centroid()

        places:insert({
            id = make_id(object),
            name = tags['name'],
            names = to_pg_array(extract_names(tags)),
            tags = tags,
            categories = to_pg_array(derive_categories(tags)),
            centroid = centroid,
            geom = polygon,
            geom_type = 'area',
            admin_level = get_admin_level(tags),
        })
    end
end
