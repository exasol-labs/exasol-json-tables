--- This class provides a uniform way to define errors in a Lua application.
-- @classmod ExaError
local ExaError = {
    VERSION = "2.0.1",
}
ExaError.__index = ExaError

local MessageExpander = require("MessageExpander")

-- Lua 5.1 backward compatibility
-- luacheck: push ignore 122
if not table.unpack then table.unpack = _G.unpack end
-- luacheck: pop

local function expand(message, parameters)
    return MessageExpander:new(message, parameters):expand()
end

--- Convert error to a string representation.
-- Note that `__tostring` is the metamethod called by Lua's global `tostring` function.
-- This allows using the error message in places where Lua expects a string.
-- @return string representation of the error object
function ExaError:__tostring()
    local lines = {}
    if self._code then
        if self._message then
            table.insert(lines, self._code .. ": " .. self:get_message())
        else
            table.insert(lines, self._code)
        end
    else
        if self._message then
            table.insert(lines, self:get_message())
        else
            table.insert(lines, "<Missing error message. This should not happen. Please contact the software maker.>")
        end
    end
    if (self._mitigations ~= nil) and (#self._mitigations > 0) then
        table.insert(lines, "\nMitigations:\n")
        for _, mitigation in ipairs(self._mitigations) do
            table.insert(lines, "* " .. expand(mitigation, self._parameters))
        end
    end
    return table.concat(lines, "\n")
end

--- Concatenate an error object with another object.
-- @return String representing the concatenation.
function ExaError.__concat(left, right)
    return tostring(left) .. tostring(right)
end

--- Create a new instance of an error message.
-- @param code error code
-- @param message error message, optionally with placeholders
-- @param[opt={}] parameters parameter definitions used to replace the placeholders
-- @param[opt={}] mitigations mitigations users can try to solve the error
-- @return created object
function ExaError:new(code, message, parameters, mitigations)
    local instance = setmetatable({}, self)
    instance:_init(code, message, parameters, mitigations)
    return instance
end

function ExaError:_init(code, message, parameters, mitigations)
    self._code = code
    self._message = message
    self._parameters = parameters or {}
    self._mitigations = mitigations or {}
end

--- Add mitigations.
-- @param ... one or more mitigation descriptions
-- @return error message object
function ExaError:add_mitigations(...)
    for _, mitigation in ipairs({...}) do
        table.insert(self._mitigations, mitigation)
    end
    return self
end

--- Add issue ticket mitigation
-- This is a special kind of mitigation which you should use in case of internal software errors that should not happen.
-- For example when a path in the code is reached that should be unreachable if the code is correct.
-- @return error message object
function ExaError:add_ticket_mitigation()
    table.insert(self._mitigations,
        "This is an internal software error. Please report it via the project's ticket tracker.")
    return self
end

--- Get the error code.
-- @return error code
function ExaError:get_code()
    return self._code
end

--- Get the error message.
-- Placeholders in the raw message are replaced by the parameters given when building the error message object.
-- For fault tolerance, this method returns the raw message in case the parameters are missing.
-- @return error message
function ExaError:get_message()
    return expand(self._message, self._parameters)
end

function ExaError:get_raw_message()
    return self._message or ""
end

--- Get parameter definitions.
-- @return parameter defintions
function ExaError:get_parameters()
    return self._parameters
end

--- Get the description of a parameter.
-- @param parameter_name name of the parameter
-- @return parameter description or the string "`<missing parameter description>`"
function ExaError:get_parameter_description(parameter_name)
    return self._parameters[parameter_name].description or "<missing parameter description>"
end

--- Get the mitigations for the error.
-- @return list of mitigations
function ExaError:get_mitigations()
    return table.unpack(self._mitigations)
end

--- Raise the error.
-- Like in Lua's `error` function, you can optionally specify if and from which level down the stack trace
-- is included in the error message.
-- <ul>
-- <li>0: no stack trace</li>
-- <li>1: stack trace starts at the point inside `exaerror` where the error is raised
-- <li>2: stack trace starts at the calling function (default)</li>
-- <li>3+: stack trace starts below the calling function</li>
-- </ul>
-- @param level (optional) level from which down the stack trace will be displayed
-- @raise Lua error for the given error object
function ExaError:raise(level)
    level = (level == nil) and 2 or level
    error(tostring(self), level)
end

--- Raise an error that represents the error object's contents.
-- @param code error code
-- @param message error message, optionally with placeholders
-- @param[opt={}] parameters parameter definitions used to replace the placeholders
-- @param[opt={}] mitigations mitigations users can try to solve the error
-- @see M.create
-- @see M:new
-- @raise Lua error for the given error object
function ExaError.error(code, message, parameters, mitigations)
     ExaError:new(code, message, parameters, mitigations):raise()
end

return ExaError
