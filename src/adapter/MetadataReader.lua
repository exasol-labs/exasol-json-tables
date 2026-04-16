local MetadataReader = {}
MetadataReader.__index = MetadataReader

local cjson = require("cjson")
local LocalMetadataReader = require("exasol.evscl.LocalMetadataReader")
local helpers = require("util.helpers")

local NULL_SUFFIX<const> = "|n"
local OBJECT_SUFFIX<const> = "|object"
local ARRAY_SUFFIX<const> = "|array"

local STRUCTURAL_COLUMNS<const> = {
    _id = true,
    _parent = true,
    _pos = true
}

function MetadataReader:new(exasol_context)
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function MetadataReader:_init(exasol_context)
    self._delegate = LocalMetadataReader:new(exasol_context)
end

local function is_structural_column(column_name)
    return STRUCTURAL_COLUMNS[column_name] == true
end

local function parse_column_name(column_name)
    if is_structural_column(column_name) then
        return nil
    end
    if string.sub(column_name, -string.len(NULL_SUFFIX)) == NULL_SUFFIX then
        return {
            baseName = string.sub(column_name, 1, -string.len(NULL_SUFFIX) - 1),
            kind = "nullMask"
        }
    end
    if string.sub(column_name, -string.len(OBJECT_SUFFIX)) == OBJECT_SUFFIX then
        return {
            baseName = string.sub(column_name, 1, -string.len(OBJECT_SUFFIX) - 1),
            kind = "object"
        }
    end
    if string.sub(column_name, -string.len(ARRAY_SUFFIX)) == ARRAY_SUFFIX then
        return {
            baseName = string.sub(column_name, 1, -string.len(ARRAY_SUFFIX) - 1),
            kind = "array"
        }
    end
    local base_name, suffix = string.match(column_name, "^(.*)|([^|]+)$")
    if base_name ~= nil then
        return {
            baseName = base_name,
            kind = "alternate",
            suffix = suffix
        }
    end
    return {
        baseName = column_name,
        kind = "primary"
    }
end

local function normalize_variant_label(raw_label)
    local normalized = string.upper(raw_label or "")
    if normalized == "BOOL" or normalized == "BOOLEAN" then
        return "BOOLEAN"
    elseif normalized == "INTEGER" or normalized == "NUMBER" or normalized == "DECIMAL" or normalized == "DOUBLE" then
        return "NUMBER"
    elseif normalized == "STRING" or normalized == "CHAR" or normalized == "VARCHAR" then
        return "STRING"
    elseif normalized == "OBJECT" then
        return "OBJECT"
    elseif normalized == "ARRAY" then
        return "ARRAY"
    end
    return normalized ~= "" and normalized or nil
end

local function infer_variant_label_from_data_type(column)
    if column == nil or column.dataType == nil then
        return nil
    end
    return normalize_variant_label(column.dataType.type)
end

local function infer_variant_label_from_member(member)
    if member == nil then
        return nil
    end
    if member.kind == "alternate" then
        return normalize_variant_label(member.suffix)
    elseif member.kind == "object" then
        return "OBJECT"
    elseif member.kind == "array" then
        return "ARRAY"
    end
    return infer_variant_label_from_data_type(member.column)
end

local function add_variant_entry(variant_columns, variant_order, variant_label, column_name)
    if variant_label == nil or column_name == nil or variant_columns[variant_label] ~= nil then
        return
    end
    variant_columns[variant_label] = column_name
    variant_order[#variant_order + 1] = variant_label
end

local function create_group(base_name)
    return {
        baseName = base_name,
        members = {},
        alternates = {},
        nullMask = nil,
        primary = nil,
        object = nil,
        array = nil
    }
end

function MetadataReader:_group_columns(columns)
    local groups = {}
    for index, column in ipairs(columns) do
        local parsed = parse_column_name(column.name)
        if parsed ~= nil then
            local group = groups[parsed.baseName]
            if group == nil then
                group = create_group(parsed.baseName)
                groups[parsed.baseName] = group
            end
            local member = {
                index = index,
                kind = parsed.kind,
                suffix = parsed.suffix,
                column = column
            }
            group.members[#group.members + 1] = member
            if parsed.kind == "nullMask" then
                group.nullMask = member
            elseif parsed.kind == "primary" then
                group.primary = member
            elseif parsed.kind == "object" then
                group.object = member
            elseif parsed.kind == "array" then
                group.array = member
            elseif parsed.kind == "alternate" then
                group.alternates[#group.alternates + 1] = member
            end
        end
    end
    return groups
end

local function count_non_null_variants(group)
    local count = 0
    if group.primary ~= nil then
        count = count + 1
    end
    if group.object ~= nil then
        count = count + 1
    end
    if group.array ~= nil then
        count = count + 1
    end
    count = count + #group.alternates
    return count
end

local function choose_visible_member(group)
    if group.primary ~= nil then
        return group.primary
    elseif group.object ~= nil then
        return group.object
    elseif group.array ~= nil then
        return group.array
    elseif group.alternates[1] ~= nil then
        return group.alternates[1]
    end
    return nil
end

local function choose_emission_member(group)
    local chosen = nil
    for _, member in ipairs(group.members) do
        if member.kind ~= "nullMask" then
            if chosen == nil or member.index < chosen.index then
                chosen = member
            end
        end
    end
    return chosen
end

local function encode_column_notes(group, visible_member, is_variant_group, visible_name)
    local notes = {}
    if group.nullMask ~= nil then
        notes.nullMaskColumn = group.nullMask.column.name
    end
    if is_variant_group then
        local variant_columns = {}
        local variant_order = {}
        add_variant_entry(variant_columns, variant_order, infer_variant_label_from_member(visible_member),
                visible_member.column.name)
        for _, alternate in ipairs(group.alternates) do
            add_variant_entry(variant_columns, variant_order, infer_variant_label_from_member(alternate),
                    alternate.column.name)
        end
        add_variant_entry(variant_columns, variant_order, infer_variant_label_from_member(group.object),
                group.object and group.object.column.name or nil)
        add_variant_entry(variant_columns, variant_order, infer_variant_label_from_member(group.array),
                group.array and group.array.column.name or nil)
        notes.physicalColumn = visible_member.column.name
        notes.variantColumns = variant_columns
        notes.variantOrder = variant_order
        notes.visibleName = visible_name
    end
    if next(notes) == nil then
        return nil
    end
    return cjson.encode(notes)
end

function MetadataReader:_rewrite_base_table(table_metadata)
    local groups = self:_group_columns(table_metadata.columns)
    local visible_columns = {}
    local emitted_groups = {}
    for _, column in ipairs(table_metadata.columns) do
        if is_structural_column(column.name) then
            visible_columns[#visible_columns + 1] = helpers.deep_copy(column)
        else
            local parsed = parse_column_name(column.name)
            local group = parsed and groups[parsed.baseName] or nil
            if group ~= nil then
                local visible_member = choose_visible_member(group)
                local emission_member = choose_emission_member(group)
                local is_variant_group = count_non_null_variants(group) > 1
                if visible_member ~= nil and emission_member ~= nil and not emitted_groups[group.baseName] and
                        column.name == emission_member.column.name then
                    local rewritten = helpers.deep_copy(visible_member.column)
                    local visible_name = is_variant_group and group.baseName or visible_member.column.name
                    rewritten.name = visible_name
                    rewritten.adapterNotes = encode_column_notes(group, visible_member, is_variant_group, visible_name)
                    visible_columns[#visible_columns + 1] = rewritten
                    emitted_groups[group.baseName] = true
                end
            else
                visible_columns[#visible_columns + 1] = helpers.deep_copy(column)
            end
        end
    end
    return {
        type = table_metadata.type or "table",
        name = table_metadata.name,
        columns = visible_columns
    }
end

function MetadataReader:read(schema_id, include_tables)
    local metadata = self._delegate:read(schema_id, include_tables)
    local rewritten_tables = {}
    for _, table_metadata in ipairs(metadata.tables or {}) do
        rewritten_tables[#rewritten_tables + 1] = self:_rewrite_base_table(table_metadata)
    end
    return {tables = rewritten_tables}
end

return MetadataReader
