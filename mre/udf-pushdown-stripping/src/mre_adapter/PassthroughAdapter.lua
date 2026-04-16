local PassthroughAdapter = {}
PassthroughAdapter.__index = PassthroughAdapter
local AbstractVirtualSchemaAdapter = require("exasol.vscl.AbstractVirtualSchemaAdapter")
setmetatable(PassthroughAdapter, {__index = AbstractVirtualSchemaAdapter})

local LocalMetadataReader = require("exasol.evscl.LocalMetadataReader")
local QueryRewriter = require("mre_adapter.QueryRewriter")

local VERSION<const> = "0.1.0"
local CAPABILITIES<const> = {
    "SELECTLIST_PROJECTION",
    "SELECTLIST_EXPRESSIONS",
    "FN_ABS"
}

function PassthroughAdapter:new(exasol_context)
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function PassthroughAdapter:_init(exasol_context)
    AbstractVirtualSchemaAdapter._init(self)
    self._exasol_context = exasol_context
end

function PassthroughAdapter:get_version()
    return VERSION
end

function PassthroughAdapter:get_name()
    return "UDF Pushdown Stripping MRE Adapter"
end

function PassthroughAdapter:_read_schema_metadata(properties)
    local metadata_reader = LocalMetadataReader:new(self._exasol_context)
    return metadata_reader:read(properties:get_schema_name(), properties:get_table_filter())
end

function PassthroughAdapter:create_virtual_schema(_, properties)
    properties:validate()
    return {type = "createVirtualSchema", schemaMetadata = self:_read_schema_metadata(properties)}
end

function PassthroughAdapter:refresh(_, properties)
    properties:validate()
    return {type = "refresh", schemaMetadata = self:_read_schema_metadata(properties)}
end

function PassthroughAdapter:set_properties(_, old_properties, new_properties)
    local merged_properties = old_properties:merge(new_properties)
    merged_properties:validate()
    return {type = "setProperties", schemaMetadata = self:_read_schema_metadata(merged_properties)}
end

function PassthroughAdapter:push_down(request, properties)
    properties:validate()
    local query_rewriter = QueryRewriter:new()
    local rewritten_query = query_rewriter:rewrite(request.pushdownRequest, properties:get_schema_name())
    return {type = "pushdown", sql = rewritten_query}
end

function PassthroughAdapter:_define_capabilities()
    return CAPABILITIES
end

return PassthroughAdapter
