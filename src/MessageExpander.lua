--- This class provides a parser for messages with named parameters and can expand the message using the parameter
-- values.
-- @classmod MessageExpander
local MessageExpander = {}
MessageExpander.__index = MessageExpander

local FROM_STATE_INDEX = 1
local GUARD_INDEX = 2
local ACTION_INDEX = 3
local TO_STATE_INDEX = 4

--- Create a new instance of a message expander.
-- @param message to be expanded
-- @param parameters parameter definitions
-- @return message expander instance
function MessageExpander:new(message, parameters)
    local instance = setmetatable({}, self)
    instance:_init(message, parameters)
    return instance
end

function MessageExpander:_init(message, parameters)
    self._message = message
    self._parameters = parameters
    self._tokens = {}
    self._last_parameter = {characters = {}, quote = true}
end

local function tokenize(text)
    return string.gmatch(text, ".")
end

--- Expand the message.
-- Note that if no parameter values are supplied, the message will be returned as is, without any replacements.
-- @return expanded message
function MessageExpander:expand()
    if (self._parameters == nil) or (not next(self._parameters)) then
        return self._message
    else
        self:_run()
    end
    return table.concat(self._tokens)
end

function MessageExpander:_run()
    self.state = "TEXT"
    local token_iterator = tokenize(self._message)
    for token in token_iterator do
        self.state = self:_transit(token)
    end
end

function MessageExpander:_transit(token)
    for _, transition in ipairs(MessageExpander._transitions) do
        local from_state = transition[FROM_STATE_INDEX]
        local guard = transition[GUARD_INDEX]
        if(from_state == self.state and guard(token)) then
            local action = transition[ACTION_INDEX]
            action(self, token)
            local to_state = transition[TO_STATE_INDEX]
            return to_state
        end
    end
end

local function is_any()
    return true
end

local function is_opening_bracket(token)
    return token == "{"
end

local function is_closing_bracket(token)
    return token == "}"
end

-- We are intentionally not using the symbol itself here for compatibility reasons.
-- See https://github.com/exasol/error-reporting-lua/issues/15 for details.
local function is_pipe(token)
    return token == string.char(124)
end

local function is_u(token)
    return token == "u"
end

local function is_not_bracket(token)
    return not is_opening_bracket(token) and not is_closing_bracket(token)
end

local function add_token(self, token)
    table.insert(self._tokens, token)
end

local function add_open_plus_token(self, token)
    table.insert(self._tokens, "{")
    table.insert(self._tokens, token)
end

local function add_parameter_name(self, token)
    table.insert(self._last_parameter.characters, token)
end

local function set_unquoted(self)
    self._last_parameter.quote = false
end

local function unwrap_parameter_value(parameter)
    if parameter ~= nil and type(parameter) == "table" then
        return parameter.value
    else
        return parameter
    end
end

local function insert_parameter_value_into_token_list(self, value)
    if value == nil then
        table.insert(self._tokens, "<missing value>")
    else
        local type = type(value)
        if (type == "string") and (self._last_parameter.quote) then
            table.insert(self._tokens, "'")
            table.insert(self._tokens, value)
            table.insert(self._tokens, "'")
        elseif type == "boolean" then
            table.insert(self._tokens, tostring(value))
        elseif type == "table" or type == "thread" or type == "userdata" then
            table.insert(self._tokens, "<")
            table.insert(self._tokens, tostring(value))
            table.insert(self._tokens, ">")
        else
            table.insert(self._tokens, value)
        end
    end
end

local function replace_parameter(self)
    local parameter_name = table.concat(self._last_parameter.characters)
    local value = unwrap_parameter_value(self._parameters[parameter_name])
    insert_parameter_value_into_token_list(self, value)
    self._last_parameter.characters = {}
    self._last_parameter.quote = true
end

local function replace_and_add(self, token)
    replace_parameter(self)
    add_token(self, token)
end

local function do_nothing() end

MessageExpander._transitions = {
    {"TEXT"     , is_not_bracket    , add_token          , "TEXT"     },
    {"TEXT"     , is_opening_bracket, do_nothing         , "OPEN_1"   },
    {"OPEN_1"   , is_opening_bracket, do_nothing         , "PARAMETER"},
    {"OPEN_1"   , is_any            , add_open_plus_token, "TEXT"     },
    {"PARAMETER", is_closing_bracket, do_nothing         , "CLOSE_1"  },
    {"PARAMETER", is_pipe           , do_nothing         , "SWITCH"   },
    {"PARAMETER", is_any            , add_parameter_name , "PARAMETER"},
    {"SWITCH"   , is_closing_bracket, do_nothing         , "CLOSE_1"  },
    {"SWITCH"   , is_u              , set_unquoted       , "SWITCH"   },
    {"SWITCH"   , is_any            , do_nothing         , "SWITCH"   },
    {"CLOSE_1"  , is_closing_bracket, replace_parameter  , "TEXT"     },
    {"CLOSE_1"  , is_any            , replace_and_add    , "TEXT"     }
}

return MessageExpander
