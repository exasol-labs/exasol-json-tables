local AdapterProperties = {}
AdapterProperties.__index = AdapterProperties
local ExasolBaseAdapterProperties = require("exasol.evscl.ExasolBaseAdapterProperties")
setmetatable(AdapterProperties, ExasolBaseAdapterProperties)

function AdapterProperties:new(raw_properties)
    local instance = setmetatable({}, self)
    instance:_init(raw_properties)
    return instance
end

function AdapterProperties:_init(raw_properties)
    ExasolBaseAdapterProperties._init(self, raw_properties)
end

function AdapterProperties:class()
    return AdapterProperties
end

function AdapterProperties:validate()
    ExasolBaseAdapterProperties.validate(self)
end

return AdapterProperties
