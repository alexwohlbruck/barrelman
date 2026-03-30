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
