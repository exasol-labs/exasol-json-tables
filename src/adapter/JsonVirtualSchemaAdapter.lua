local JsonVirtualSchemaAdapter = {}
JsonVirtualSchemaAdapter.__index = JsonVirtualSchemaAdapter
local AbstractVirtualSchemaAdapter = require("exasol.vscl.AbstractVirtualSchemaAdapter")
setmetatable(JsonVirtualSchemaAdapter, {__index = AbstractVirtualSchemaAdapter})

local capabilities = require("adapter.capabilities")
local MetadataReader = require("adapter.MetadataReader")
local QueryRewriter = require("adapter.QueryRewriter")
local log = require("remotelog")

local VERSION<const> = "0.1.0"

function JsonVirtualSchemaAdapter:new(exasol_context)
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function JsonVirtualSchemaAdapter:_init(exasol_context)
    AbstractVirtualSchemaAdapter._init(self)
    self._exasol_context = exasol_context
end

function JsonVirtualSchemaAdapter:get_version()
    return VERSION
end

function JsonVirtualSchemaAdapter:get_name()
    return "JSON Virtual Schema Adapter"
end

function JsonVirtualSchemaAdapter:_read_schema_metadata(properties)
    local metadata_reader = MetadataReader:new(self._exasol_context)
    return metadata_reader:read(properties:get_schema_name(), properties:get_table_filter())
end

function JsonVirtualSchemaAdapter:create_virtual_schema(_, properties)
    properties:validate()
    return {type = "createVirtualSchema", schemaMetadata = self:_read_schema_metadata(properties)}
end

function JsonVirtualSchemaAdapter:refresh(_, properties)
    properties:validate()
    return {type = "refresh", schemaMetadata = self:_read_schema_metadata(properties)}
end

function JsonVirtualSchemaAdapter:set_properties(_, old_properties, new_properties)
    log.debug("Old properties " .. tostring(old_properties))
    log.debug("New properties " .. tostring(new_properties))
    local merged_properties = old_properties:merge(new_properties)
    log.debug("Merged properties " .. tostring(merged_properties))
    merged_properties:validate()
    return {type = "setProperties", schemaMetadata = self:_read_schema_metadata(merged_properties)}
end

function JsonVirtualSchemaAdapter:push_down(request, properties)
    properties:validate()
    local query_rewriter = QueryRewriter:new()
    local rewritten_query = query_rewriter:rewrite(request.pushdownRequest, properties:get_schema_name(),
            request.schemaMetadataInfo.adapterNotes, request.involvedTables)
    return {type = "pushdown", sql = rewritten_query}
end

function JsonVirtualSchemaAdapter:_define_capabilities()
    return capabilities
end

return JsonVirtualSchemaAdapter
