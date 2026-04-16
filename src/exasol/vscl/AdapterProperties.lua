--- This class abstracts access to the user-defined properties of the Virtual Schema.
---@class AdapterProperties
---@field _raw_properties table<string, string>
local AdapterProperties = {null = {}}
AdapterProperties.__index = AdapterProperties

local text = require("exasol.vscl.text")
local ExaError = require("ExaError")

local EXCLUDED_CAPABILITIES_PROPERTY<const> = "EXCLUDED_CAPABILITIES"
local LOG_LEVEL_PROPERTY<const> = "LOG_LEVEL"
local DEBUG_ADDRESS_PROPERTY<const> = "DEBUG_ADDRESS"
local DEFAULT_LOG_PORT<const> = 3000

--- Create a new instance of adapter properties.
---@param raw_properties table<string, string> properties as key-value pairs
---@return AdapterProperties properties new instance
function AdapterProperties:new(raw_properties)
    local instance = setmetatable({}, self)
    instance:_init(raw_properties)
    return instance
end

function AdapterProperties:_init(raw_properties)
    self._raw_properties = raw_properties
end

--- Get the class of the object
---@return table class
function AdapterProperties:class()
    return AdapterProperties
end

--- Get the value of a property.
---@param property_name string name of the property to get
---@return string property_value
function AdapterProperties:get(property_name)
    return self._raw_properties[property_name]
end

--- Check if the property is set.
---@param property_name string name of the property to check
---@return boolean property_set `true` if the property is set (i.e. not `nil`)
function AdapterProperties:is_property_set(property_name)
    return self:get(property_name) ~= nil
end

--- Check if the property has a non-empty value.
---@param property_name string name of the property to check
---@return boolean has_value `true` if the property has a non-empty value (i.e. not `nil` or an empty string)
function AdapterProperties:has_value(property_name)
    local value = self:get(property_name)
    return value ~= nil and value ~= ""
end

--- Check if the property value is empty.
---@param property_name string name of the property to check
---@return boolean is_empty `true` if the property's value is empty (i.e. the property is set to an empty string)
function AdapterProperties:is_empty(property_name)
    return self:get(property_name) == ""
end

--- Check if the property contains the string `true` (case-sensitive).
---@param property_name string name of the property to check
---@return boolean is_true `true` if the property's value is the string `true`
function AdapterProperties:is_true(property_name)
    return self:get(property_name) == "true"
end

--- Check if the property evaluates to `false`.
---@param property_name string name of the property to check
---@return boolean is_false `true` if the property's value is anything else than the string `true`
function AdapterProperties:is_false(property_name)
    return not self:is_true(property_name)
end

function AdapterProperties:_validate_debug_address()
    if self:has_value(DEBUG_ADDRESS_PROPERTY) then
        local address = self:get(DEBUG_ADDRESS_PROPERTY)
        if not string.match(address, "^.-:[0-9]+$") then
            ExaError:new("F-VSCL-PROP-3", "Expected log address in " .. DEBUG_ADDRESS_PROPERTY
                                 .. " to look like '<ip>|<host>[:<port>]', but got {{address}} instead",
                         {address = address}):add_mitigations("Provide an valid IP address or host name")
                    :add_mitigations("Make sure host/ip and port number are separated by a colon"):add_mitigations(
                            "Optionally add a port number (default is 3000)"):add_mitigations(
                            "Don't add any whitespace characters"):raise(0)
        end
    end
end

function AdapterProperties:_validate_log_level()
    if self:has_value(LOG_LEVEL_PROPERTY) then
        local level = self:get_log_level()
        local allowed_levels = {"FATAL", "ERROR", "WARNING", "INFO", "CONFIG", "DEBUG", "TRACE"}
        local found = false
        for _, allowed in ipairs(allowed_levels) do
            if level == allowed then
                found = true
                break
            end
        end
        if not found then
            ExaError:new("F-VSCL-PROP-2", "Unknown log level {{level}} in " .. LOG_LEVEL_PROPERTY .. " property",
                         {level = level}):add_mitigations("Pick one of: " .. table.concat(allowed_levels, ", "))
                    :raise(0)
        end
    end
end

function AdapterProperties:_validate_excluded_capabilities()
    if self:has_value(EXCLUDED_CAPABILITIES_PROPERTY) then
        local value = self:get(EXCLUDED_CAPABILITIES_PROPERTY)
        if not string.match(value, "^[ A-Za-z0-9_,]*$") then
            ExaError:new("F-VSCL-PROP-1",
                         "Invalid character(s) in " .. EXCLUDED_CAPABILITIES_PROPERTY .. " property: {{value}}",
                         {value = value}):add_mitigations(
                    "Use only the following characters: ASCII letter, digit, underscore, comma, space"):raise(0)
        end
    end
end

--- Validate the adapter properties.
---@raise validation error
function AdapterProperties:validate()
    self:_validate_debug_address()
    self:_validate_log_level()
    self:_validate_excluded_capabilities()
end

--- Validate a boolean property.
---Allowed values are `true`, `false` or an unset variable.
---@raise validation error
function AdapterProperties:validate_boolean(property_name)
    local value = self:get(property_name)
    if not (value == nil or value == "true" or value == "false") then
        ExaError:new("F-VSCL-PROP-4", "Property '" .. property_name .. "' contains an illegal value: '" .. value .. "'")
                :add_mitigations("Either leave the property unset or choose one of 'true', 'false' (case-sensitive).")
                :raise(0)
    end
end

--- Get the log level
---@return string log_level
function AdapterProperties:get_log_level()
    return self:get(LOG_LEVEL_PROPERTY)
end

--- Check if the log level is set
---@return boolean has_log_level `true` if the log level is set
function AdapterProperties:has_log_level()
    return self:has_value(LOG_LEVEL_PROPERTY)
end

--- Get the list of names of the excluded capabilities.
---@return string[]? excluded_capabilities
function AdapterProperties:get_excluded_capabilities()
    return text.split(self:get(EXCLUDED_CAPABILITIES_PROPERTY))
end

--- Check if excluded capabilities are set
---@return boolean has_excluded_capabilities `true` if the excluded capabilities are set
function AdapterProperties:has_excluded_capabilities()
    return self:has_value(EXCLUDED_CAPABILITIES_PROPERTY)
end

--- Get the debug address (host and port)
---@return string? host, integer? port or `nil` if the property has no value
function AdapterProperties:get_debug_address()
    if self:has_value(DEBUG_ADDRESS_PROPERTY) then
        local debug_address = self:get(DEBUG_ADDRESS_PROPERTY)
        local colon_position = string.find(debug_address, ":", 1, true)
        if colon_position == nil then
            return debug_address, DEFAULT_LOG_PORT
        else
            local host = string.sub(debug_address, 1, colon_position - 1)
            local port = math.tointeger(tonumber(string.sub(debug_address, colon_position + 1)))
            return host, port
        end
    else
        return nil, nil
    end
end

--- Check if log address is set
---@return boolean has_debug_address `true` if the log address is set
function AdapterProperties:has_debug_address()
    return self:has_value(DEBUG_ADDRESS_PROPERTY)
end

--- Merge new properties into a set of existing ones
---@param new_properties AdapterProperties set of new properties to merge into the existing ones
---@return AdapterProperties merge_product
-- [impl -> dsn~merging-user-defined-properties~0]
function AdapterProperties:merge(new_properties)
    local merged_list = {}
    for key, value in pairs(new_properties._raw_properties) do
        if (value ~= nil) and (value ~= AdapterProperties.null) then
            merged_list[key] = value
        end
    end
    for key, value in pairs(self._raw_properties) do
        if new_properties._raw_properties[key] == nil then
            merged_list[key] = value
        end
    end
    local merged_properties = self:class():new(merged_list)
    return merged_properties
end

--- Create a string representation
---@return string string_representation
function AdapterProperties:__tostring()
    local keys = {}
    local i = 0
    for key, _ in pairs(self._raw_properties) do
        i = i + 1
        keys[i] = key
    end
    table.sort(keys)
    local str = {"("}
    for _, key in ipairs(keys) do
        if (#str > 1) then
            str[#str + 1] = ", "
        end
        str[#str + 1] = key
        str[#str + 1] = " = "
        str[#str + 1] = self._raw_properties[key]
    end
    str[#str + 1] = ")"
    return table.concat(str)
end

return AdapterProperties
