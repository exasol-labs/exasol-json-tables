--- This class abstracts access to the user-defined properties of the Virtual Schema.
-- @classmod ExasolBaseAdapterProperties
local ExasolBaseAdapterProperties = {}
ExasolBaseAdapterProperties.__index = ExasolBaseAdapterProperties
local AdapterProperties = require("exasol.vscl.AdapterProperties")
setmetatable(ExasolBaseAdapterProperties, AdapterProperties)

local ExaError = require("ExaError")
local text = require("exasol.vscl.text")

--- Create a new `ExasolAdapterProperties` instance
-- @param raw_properties unparsed user-defined properties
-- @return new instance
function ExasolBaseAdapterProperties:new(raw_properties)
    local instance = setmetatable({}, self)
    instance:_init(raw_properties)
    return instance
end

function ExasolBaseAdapterProperties:_init(raw_properties)
    AdapterProperties._init(self, raw_properties)
end

--- Get the class of the object
-- @return class
function ExasolBaseAdapterProperties:class()
    return ExasolBaseAdapterProperties
end

local SCHEMA_NAME_PROPERTY <const> = "SCHEMA_NAME"
local TABLE_FILTER_PROPERTY <const> = "TABLE_FILTER"

--- Validate the adapter properties.
-- @raise validation error
function ExasolBaseAdapterProperties:validate()
    AdapterProperties.validate(self) -- super call
    if not self:has_value(SCHEMA_NAME_PROPERTY) then
        ExaError:new("F-RLS-PROP-1", "Missing mandatory property '" .. SCHEMA_NAME_PROPERTY .. "'.")
                :add_mitigations("Please define the name of the source schema."):raise(0)
    end
    if self:is_property_set(TABLE_FILTER_PROPERTY) and self:is_empty(TABLE_FILTER_PROPERTY) then
        ExaError:new("F-RLS-PROP-2", "Table filter property '" .. TABLE_FILTER_PROPERTY .. "' must not be empty.")
                :add_mitigations("Please either remove the property or provide a comma separated list of tables"
                .. " to be included in the Virtual Schema."):raise(0)
    end
end

--- Get the name of the Virtual Schema's source schema.
-- @return name of the source schema
-- @cover [impl -> dsn~schema-name-property~0]
function ExasolBaseAdapterProperties:get_schema_name()
    return self:get(SCHEMA_NAME_PROPERTY)
end

--- Get the list of tables that the Virtual Schema should show after applying the table filter.
-- @return list of tables
-- @cover [impl -> dsn~table-filter-property~0]
function ExasolBaseAdapterProperties:get_table_filter()
    local filtered_tables = self:get(TABLE_FILTER_PROPERTY)
    return text.split(filtered_tables)
end

function ExasolBaseAdapterProperties:__tostring()
    return AdapterProperties.__tostring(self)
end

return ExasolBaseAdapterProperties
