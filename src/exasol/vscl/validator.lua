--- Validators for common input such as user names or port numbers.
-- @module exasol.validator
local ExaError = require("ExaError")

local validator = {}

local SQL_IDENTIFIER_DOC_URL<const> =
        "https://docs.exasol.com/db/latest/sql_references/basiclanguageelements.htm#SQLIdentifier"
local MAX_IDENTIFIER_LENGTH<const> = 128

---@param id string?
---@param id_type string
local function validate_identifier_not_nil(id, id_type)
    if id == nil then
        ExaError:new("E-EVSCL-VAL-5", "Identifier cannot be null (or Lua nil): {{id_type|u}} name",
                     {id_type = {value = id_type, description = "type of database object which should be identified"}})
                :raise()
    end
end

---@param id string
---@param id_type string
local function validate_identifier_length(id, id_type)
    local length = utf8.len(id)
    if length > MAX_IDENTIFIER_LENGTH then
        ExaError:new("E-EVSCL-VAL-4", "Identifier too long: {{id_type|u}} name with {{length}} characters.", {
            id_type = {value = id_type, description = "type of database object which should be identified"},
            length = {value = length, description = "actual length of the identifier"}
        }):raise()
    end
end

-- Currently we only support the characters between 0x41 (= A) up to 0x5A (= Z) (ASCII) in all classes.
local function is_unicode_uppercase_letter(char)
    return char >= 0x41 and char <= 0x5A;
end

-- Currently we only support the characters between 0x61 (= a) up to 0x7A (= z) (ASCII) in all classes.
local function is_unicode_lowercase_letter(char)
    return char >= 0x61 and char <= 0x7A
end

-- Currently we only support the digits between 0x30 (= 0) up to 0x39 (= 9) (ASCII) in all classes.
local function is_unicode_decimal_number(char)
    return char >= 0x30 and char <= 0x39
end

-- Currently we only support the punctuation character 0x5f (= _) (ASCII) in all classes.
local function is_unicode_connector_punctuation(char)
    return char == 0x5f -- underscore
end

local function is_middle_dot(char)
    return char == 0xB7;
end

--- Check if the character is a valid first character for an identifier.
-- <ul>
-- <li>Lu (upper-case letters): partial support</li>
-- <li>Ll (lower-case letters): partial support</li>
-- <li>Lt (title-case letters): not supported yet</li>
-- <li>Lm (modifier letters): not supported yet</li>
-- <li>Lo (other letters): not supported yet</li>
-- <li>Nl (letter numbers): not supported yet</li>
---@param char integer unicode character number
---@return boolean result `true` if the character is valid
local function is_valid_first_identifier_character(char)
    return is_unicode_uppercase_letter(char) or is_unicode_lowercase_letter(char)
end

--- Check if the character is a valid follow-up character for an identifier.
-- <ul>
-- <li>Mn (non-spacing marks): not supported yet</li>
-- <li>Mc (spacing combination marks): not supported yet</li>
-- <li>Nd (decimal numbers): partial support</li>
-- <li>Pc (connectors punctuations): partial support</li>
-- <li>Cf (formatting codes): not supported yet</li>
-- <li>unicode character U+00B7 (middle dot): supported</li>
---@param char integer unicode character number
---@return boolean result `true` if the character is valid
local function is_valid_followup_identifier_character(char)
    return is_valid_first_identifier_character(char) or is_unicode_decimal_number(char)
                   or is_unicode_connector_punctuation(char) or is_middle_dot(char)
end

---@param id string database object identifier
---@param id_type string type of the database object referenced by the identifier
local function validate_identifier_characters(id, id_type)
    for position, char in utf8.codes(id) do
        if (position == 1 and not is_valid_first_identifier_character(char))
                or (not is_valid_followup_identifier_character(char)) then
            ExaError:new("E-EVSCL-VAL-3", "Invalid character in {{id_type|u}} name at position {{position}}: {{id}}", {
                id_type = {value = id_type, description = "type of database object which should be identified"},
                position = {value = position, description = "position of the first illegal character in identifier"},
                id = {value = id, description = "value of the object identifier"}
            }):add_mitigations("Please note that " .. id_type .. " names are SQL identifiers. Refer to "
                                       .. SQL_IDENTIFIER_DOC_URL .. " for information about valid identifiers."):raise()
        end
    end
end

---@param id string? database object identifier (e.g. a table name)
---@param id_type string type of the database object
local function validate_sql_identifier(id, id_type)
    validate_identifier_not_nil(id, id_type)
    assert(id ~= nil)
    validate_identifier_length(id, id_type)
    validate_identifier_characters(id, id_type)
end

---@param id string? user name
function validator.validate_user(id)
    validate_sql_identifier(id, "user")
end

---@param port_string string? port as string (before it is proven to be a number)
function validator.validate_port(port_string)
    local port = tonumber(port_string)
    if port == nil then
        ExaError:new("E-EVSCL-VAL-1", "Illegal source database port (not a number): {{port}}",
                     {port = {value = port_string, "number of the port the source database listens on"}})
                :add_mitigations("Please enter a number between 1 and 65535"):raise()
    else
        if (port < 1) or (port > 65535) then
            ExaError:new("E-EVSCL-VAL-2", "Source database port is out of range: {{port}}",
                         {port = {value = port, "number of the port the source database listens on"}}):add_mitigations(
                    "Please pick a port between 1 and 65535", "The default Exasol port is 8563"):raise()
        end
    end
end

return validator
