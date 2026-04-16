--- This class reads schema, table and column metadata from the source.
-- @classmod LocalMetadataReader
local AbstractMetadataReader = {}
AbstractMetadataReader.__index = AbstractMetadataReader

local log = require("remotelog")
local ExaError = require("ExaError")
local text = require("exasol.vscl.text")

local DEFAULT_SRID <const> = 0

--- Create a new `AbstractMetadataReader`.
-- @param exasol_context handle to local database functions and status
-- @return metadata reader
function AbstractMetadataReader:new(exasol_context)
    assert(exasol_context ~= nil,
            "The metadata reader requires an Exasol context handle in order to read metadata from the database")
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function AbstractMetadataReader:_init(exasol_context)
    self._exasol_context = exasol_context
end

function AbstractMetadataReader:_get_exasol_context()
    return self._exasol_context
end

--- Get the metadata reader type
-- Override this function in the derived class.
-- @return always 'LOCAL'
function AbstractMetadataReader:_get_type()
    error("Called abstract function '_get_type'.")
end

function AbstractMetadataReader:_translate_parameterless_type(column_id, column_type)
    return {name = column_id, dataType = {type = column_type}}
end

function AbstractMetadataReader:_translate_decimal_type(column_id, column_type)
    local precision, scale = string.match(column_type, "DECIMAL%((%d+),(%d+)%)")
    return {name = column_id,
            dataType = {type = "DECIMAL", precision = tonumber(precision), scale = tonumber(scale)}}
end

function AbstractMetadataReader:_translate_char_type(column_id, column_type)
    local type, size, character_set = string.match(column_type, "(%a+)%((%d+)%) (%w+)")
    return {name = column_id, dataType = {type = type, size = tonumber(size), characterSet = character_set}}
end

-- Note that while users can optionally specify hash sizes in BITS, this is just a convenience method. Exasol
-- internally always stores hash size in bytes.
function AbstractMetadataReader:_translate_hash_type(column_id, column_type)
    local size = string.match(column_type, "HASHTYPE%((%d+) BYTE%)")
    return {name = column_id, dataType = {type = "HASHTYPE", bytesize = tonumber(size)}}
end

function AbstractMetadataReader:_translate_timestamp_type(column_id, local_time)
    if local_time then
        return {name = column_id, dataType = {type = "TIMESTAMP", withLocalTimeZone = true}}
    else
        return {name = column_id, dataType = {type = "TIMESTAMP"}}
    end
end

function AbstractMetadataReader:_translate_geometry_type(column_id, column_type)
    local srid = string.match(column_type, "GEOMETRY%((%d+)%)")
    if (srid == nil) then
        srid = DEFAULT_SRID
    else
        srid = tonumber(srid)
    end
    return {name = column_id, dataType = {type = "GEOMETRY", srid = srid}}
end

function AbstractMetadataReader:_translate_interval_year_to_month_type(column_id, column_type)
    local precision = string.match(column_type, "INTERVAL YEAR%((%d+)%) TO MONTH")
    return
    {
        name = column_id,
        dataType = {type = "INTERVAL", fromTo = "YEAR TO MONTH", precision = tonumber(precision)}
    }
end

function AbstractMetadataReader:_translate_interval_day_to_second(column_id, column_type)
    local precision, fraction = string.match(column_type, "INTERVAL DAY%((%d+)%) TO SECOND%((%d+)%)")
    return
    {
        name = column_id,
        dataType = {
            type = "INTERVAL",
            fromTo = "DAY TO SECONDS",
            precision = tonumber(precision),
            fraction = tonumber(fraction)
        }
    }
end

function AbstractMetadataReader:_translate_column_metadata(table_id, column)
    local column_id = column.COLUMN_NAME
    local column_type = column.COLUMN_TYPE
    if (column_type == "BOOLEAN") or (column_type == "DATE") or text.starts_with(column_type, "DOUBLE") then
        return self:_translate_parameterless_type(column_id, column_type)
    elseif text.starts_with(column_type, "DECIMAL") then
        return self:_translate_decimal_type(column_id, column_type)
    elseif text.starts_with(column_type, "CHAR") or text.starts_with(column_type, "VARCHAR") then
        return self:_translate_char_type(column_id, column_type)
    elseif text.starts_with(column_type, "HASHTYPE") then
        return self:_translate_hash_type(column_id, column_type)
    elseif string.find(column_type, "WITH LOCAL TIME ZONE", 1, true) then
        return self:_translate_timestamp_type(column_id, true)
    elseif text.starts_with(column_type, "TIMESTAMP") then
        return self:_translate_timestamp_type(column_id, false)
    elseif text.starts_with(column_type, "GEOMETRY") then
        return self:_translate_geometry_type(column_id, column_type)
    elseif text.starts_with(column_type, "INTERVAL YEAR") then
        return self:_translate_interval_year_to_month_type(column_id, column_type)
    elseif text.starts_with(column_type, "INTERVAL DAY") then
        return self:_translate_interval_day_to_second(column_id, column_type)
    else
        ExaError:new("E-EVSCL-MDR-4", "Column {{table}}.{{column}} has unsupported type {{type}}.",
                {table = table_id, column = column_id, type = column_type})
                :add_ticket_mitigation()
                :raise()
    end
end

function AbstractMetadataReader:_translate_columns_metadata(schema_id, table_id)
    local ok, result = self:_execute_column_metadata_query(schema_id, table_id)
    local translated_columns = {}
    if ok then
        for i = 1, #result do
            local column = result[i]
            table.insert(translated_columns, self:_translate_column_metadata(table_id, column))
        end
        return translated_columns
    else
        ExaError.error("E-EVSCL-MDR-3",
                "Unable to read column metadata from source table {{schema}}.{{table}}. Caused by: {{cause}}",
                {schema = schema_id, table = table_id, cause = result.error_message})
    end
end

--- Execute a query that produces the column metadata of a table in a schema.
-- @param _ schema name
-- @param _ table name
-- @return result set consisting of columns with name and type
-- @cover [impl -> dsn~reading-column-metadata-from-a-table~0]
function AbstractMetadataReader:_execute_column_metadata_query(_, _)
    error("Called abstract function '_execute_colum_metadata_query'.")
end

--- Check if a table should be included in the the virtual schema.
-- @param table_id name of the table to check
-- @param include_tables_lookup lookup table for names of database tables to include
-- @cover [impl -> dsn~include-tables~0]
function AbstractMetadataReader:_is_included_table(table_id, include_tables_lookup)
    return include_tables_lookup[table_id]
end

function AbstractMetadataReader:_create_lookup(include_tables)
    local lookup = {}
    if include_tables == nil then
        setmetatable(lookup, {__index = function(_, _)
            return true
        end})
    else
        log.debug("Setting filter for metadata scan to the following tables: "
                .. table.concat(include_tables, ", "))
        for _, table_id in ipairs(include_tables) do
            lookup[table_id] = true
        end
    end
    return lookup
end

function AbstractMetadataReader:_translate_table_scan_results(schema_id, result, include_tables)
    local tables = {}
    local include_tables_lookup = self:_create_lookup(include_tables)
    for i = 1, #result do
        local table_id = result[i].TABLE_NAME
        if self:_is_included_table(table_id, include_tables_lookup) then
            local columns = self:_translate_columns_metadata(schema_id, table_id)
            table.insert(tables, {name = table_id, columns = columns})
            log.debug("Found table '%s' (%d columns).")
        end
    end
    return tables
end

function AbstractMetadataReader:_translate_table_metadata(schema_id, include_tables)
    local ok, result = self:_execute_table_metadata_query(schema_id)
    if ok then
        return self:_translate_table_scan_results(schema_id, result, include_tables)
    else
        ExaError.error("E-EVSCL-MDR-2",
                "Unable to read table metadata from source schema {{schema}}. Caused by: {{cause}}",
                {schema = schema_id, cause = result.error_message})
    end
end

--- Execute a query that produces the list of table in the given schema.
-- @param _ schema name
-- @return result set with table names
-- @cover [impl -> dsn~reading-table-metadata-from-a-schema~0]
function AbstractMetadataReader:_execute_table_metadata_query(_)
    error("Called abstract function '_execute_table_metadata_query'.")
end

--- Read the database metadata of the given schema (i.e. the internal structure of that schema)
-- <p>
-- The scan can optionally be limited to a set of user-defined tables. If the list of tables to include in the scan
-- is omitted, then all tables in the source schema are scanned and reported.
-- </p>
-- @param schema_id schema to be scanned
-- @param include_tables list of tables to be included in the scan (optional, defaults to all tables in the schema)
-- @return schema metadata
function AbstractMetadataReader:read(schema_id, include_tables)
    log.debug("Reading metadata of source schema '" .. schema_id .. "'")
    local tables = self:_translate_table_metadata(schema_id, include_tables)
    return {tables = tables}
end

return AbstractMetadataReader