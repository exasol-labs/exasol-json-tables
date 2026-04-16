
--- This class reads schema, table and column metadata from a schema in the local Exasol database.
-- @classmod LocalMetadataReader
local LocalMetadataReader = {}
LocalMetadataReader.__index = LocalMetadataReader
local AbstractMetadataReader = require("exasol.evscl.AbstractMetadataReader")
setmetatable(LocalMetadataReader, {__index = AbstractMetadataReader})

--- Create a new `LocalMetadataReader`.
-- @param exasol_context handle to local database functions and status
-- @return metadata reader
function LocalMetadataReader:new(exasol_context)
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function LocalMetadataReader:_init(exasol_context)
    AbstractMetadataReader._init(self, exasol_context)
end

--- Get the metadata reader type
-- @return always 'LOCAL'
function AbstractMetadataReader:_get_type()
    return "LOCAL"
end

-- Override
function LocalMetadataReader:_execute_column_metadata_query(schema_id, table_id)
    local sql = '/*snapshot execution*/ SELECT "COLUMN_NAME", "COLUMN_TYPE" FROM "SYS"."EXA_ALL_COLUMNS"'
            .. ' WHERE "COLUMN_SCHEMA" = :s AND "COLUMN_TABLE" = :t ORDER BY "COLUMN_ORDINAL_POSITION"'
    return self._exasol_context.pquery_no_preprocessing(sql, {s = schema_id, t = table_id})
end

-- Override
function LocalMetadataReader:_execute_table_metadata_query(schema_id)
    local sql = '/*snapshot execution*/ SELECT "TABLE_NAME" FROM "SYS"."EXA_ALL_TABLES" WHERE "TABLE_SCHEMA" = :s'
    return self._exasol_context.pquery_no_preprocessing(sql, {s = schema_id})
end

return LocalMetadataReader