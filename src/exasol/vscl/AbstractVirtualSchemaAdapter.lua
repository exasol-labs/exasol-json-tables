--- This class implements an abstract base adapter with common behavior for some of the request callback functions.
--
-- When you derive a concrete adapter from this base class, we recommend keeping it stateless. This makes
-- parallelization easier, reduces complexity and saves you the trouble of cleaning up in the drop-virtual-schema
-- request.
--
-- [impl -> dsn~lua-virtual-schema-adapter-abstraction~0]
--
---@class AbstractVirtualSchemaAdapter
local AbstractVirtualSchemaAdapter = {}

local ExaError = require("ExaError")

function AbstractVirtualSchemaAdapter:_init()
    -- Intentionally empty
end

local function raise_abstract_method_call_error(method_name)
    ExaError:new("E-VSCL-8", "Attempted to call the abstract method AbstractVirtualSchemaAdapter:{{method|u}}.",
                 {method = {value = method_name, description = "abstract method that was called"}})
            :add_ticket_mitigation():raise()
end

--- Get the adapter name.
---@return string adapter_name name of the adapter
function AbstractVirtualSchemaAdapter:get_name()
    raise_abstract_method_call_error("get_name")
end

--- Get the adapter version.
---@return string version version of the adapter
function AbstractVirtualSchemaAdapter:get_version()
    raise_abstract_method_call_error("get_version")
end

--- Define the list of all capabilities this adapter supports.
-- Override this method in derived adapter class. Note that this differs from `get_capabilities` because
-- the later takes exclusions defined by the user into consideration.
---@return string[] capabilities list of all capabilities supported by this adapter
function AbstractVirtualSchemaAdapter:_define_capabilities()
    raise_abstract_method_call_error("_define_capabilities")
end

--- Create the Virtual Schema.
-- Create the virtual schema and provide the corresponding metadata.
---@param _request any virtual schema request
---@param _properties any user-defined properties
---@return any response metadata representing the structure and datatypes of the data source from Exasol's point of view
function AbstractVirtualSchemaAdapter:create_virtual_schema(_request, _properties)
    raise_abstract_method_call_error("create_virtual_schema")
end
--- Set new adapter properties.
-- This request provides two sets of user-defined properties. The old ones (i.e. the ones that where set before this
-- request) and the properties that the user changed.
-- A new property with a key that is not present in the old set of properties means the user added a new property.
-- New properties with existing keys override or unset existing properties. An unset property contains the special
-- value `AdapterProperties.null`.
---@param _request any virtual schema request
---@param _old_properties any old user-defined properties
---@param _new_properties any new user-defined properties
---@return any response same response as if you created a new Virtual Schema
function AbstractVirtualSchemaAdapter:set_properties(_request, _old_properties, _new_properties)
    raise_abstract_method_call_error("set_properties'")
end

--- Refresh the Virtual Schema.
-- This method reevaluates the metadata (structure and data types) that represents the data source.
---@param _request any virtual schema request
---@param _properties any user-defined properties
---@return any response same response as if you created a new Virtual Schema
function AbstractVirtualSchemaAdapter:refresh(_request, _properties)
    raise_abstract_method_call_error("refresh")
end

---@param original_capabilities string[]
---@param excluded_capabilities string[]
---@return string[]
-- [impl -> dsn~excluding-capabilities~0]
local function subtract_capabilities(original_capabilities, excluded_capabilities)
    local filtered_capabilities = {}
    for _, capability in ipairs(original_capabilities) do
        local is_excluded = false
        for _, excluded_capability in ipairs(excluded_capabilities) do
            if excluded_capability == capability then
                is_excluded = true
            end
        end
        if not is_excluded then
            table.insert(filtered_capabilities, capability)
        end
    end
    return filtered_capabilities
end

--- Get the adapter capabilities.
-- The basic `get_capabilities` handler in this class will out-of-the-box fit all derived adapters with the
-- rare exception of those that decide on capabilities at runtime depending on for example the version number of the
-- remote data source.
---@param _request any virtual schema request
---@param properties any user-defined properties
---@return table<string, any> capabilities list of non-excluded adapter capabilities
function AbstractVirtualSchemaAdapter:get_capabilities(_request, properties)
    if properties:has_excluded_capabilities() then
        return {
            type = "getCapabilities",
            capabilities = subtract_capabilities(self:_define_capabilities(), properties:get_excluded_capabilities())
        }
    else
        return {type = "getCapabilities", capabilities = self:_define_capabilities()}
    end
end

--- Push a query down to the data source
---@param _request any virtual schema request
---@param _properties any user-defined properties
---@return string rewritten_query rewritten query to be executed by the ExaLoader (`IMPORT`), value providing query
-- `SELECT ... FROM VALUES`, not recommended) or local Exasol query (`SELECT`).
function AbstractVirtualSchemaAdapter:push_down(_request, _properties)
    raise_abstract_method_call_error("push_down")
end

--- Drop the virtual schema.
-- Override this method to implement clean-up if the adapter is not stateless.
---@param _request any virtual schema request (not used)
---@param _properties any user-defined properties
---@return any response response confirming the request (otherwise empty)
function AbstractVirtualSchemaAdapter:drop_virtual_schema(_request, _properties)
    return {type = "dropVirtualSchema"}
end

return AbstractVirtualSchemaAdapter
