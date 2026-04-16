--- This class rewrites the query.
-- @classmod AbstractQueryRewriter
local AbstractQueryRewriter = {_NAME = "AbstractQueryRewriter"}
AbstractQueryRewriter.__index = AbstractQueryRewriter

local log = require("remotelog")
local ExaError = require("ExaError")

--- Create a new instance of a `QueryRewriter`.
-- @return new instance
function AbstractQueryRewriter:new()
    local instance = setmetatable({}, self)
    instance:_init()
    return instance
end

function AbstractQueryRewriter:_init()
    -- intentionally empty
end

--- Get a the class of the object.
-- @return class
function AbstractQueryRewriter:class()
    return AbstractQueryRewriter
end

function AbstractQueryRewriter:_validate(query)
    if not query then
        ExaError.error("E-EVSCL-QRW-1", "Unable to rewrite query because it was <nil>.")
    end
    local push_down_type = query.type
    if(push_down_type ~= "select") then
        ExaError.error("E-EVSCL-QRW-2", "Unable to rewrite push-down query of type {{query_type}}"
            .. ". Only 'select' is supported.", {query_type =  push_down_type})
    end
end

local function is_select_star(select_list)
    return select_list == nil
end

local function is_empty_select_list(select_list)
    return next(select_list) == nil
end

local function replace_empty_select_list_with_constant_expression(query)
    log.debug('Empty select list pushed down. Replacing with constant expression to get correct number of rows.')
    query.selectList = {{type = "literal_bool", value = "true"}}
end

local function extend_query_element_with_source_schema(element, source_schema_id)
    local extended_element = {}
    if(type(element) == "table") then
        for key, value in pairs(element) do
            if(type(value) == "table") then
                extended_element[key] = extend_query_element_with_source_schema(value, source_schema_id)
            else
                extended_element[key] = value
            end
        end
        if(element.type ~= nil and element.type == "table" and element.schema == nil) then
            log.debug("Extended table '%s' with source schema '%s' ", element.name, source_schema_id)
            extended_element.schema = source_schema_id
        end
    end
    return extended_element
end

--- Add the source database schema to all query elements that represent a table.
-- Table elements in the query structure are lacking the information which schema they belong too. But without this
-- information, the database cannot locate the table, because the table name is only valid in the context of the
-- containing schema. So this method adds the missing information by adding the source schema of the Virtual Schema
-- into the table elements.
-- @return query with table elements that contain the source schema
function AbstractQueryRewriter:_extend_query_with_source_schema(query, source_schema_id)
    return extend_query_element_with_source_schema(query, source_schema_id)
end

--- Make sure the select list is never empty.
-- <ul>
-- <li>Expand the asterisk wildcard in the select list to a list of columns</li>
-- <li>Expand an empty select list to a constant expression</li>
-- </ul>
-- @param query query as provided by the VS interface
-- @return query with non-empty select list
function AbstractQueryRewriter:_expand_select_list(query)
    if is_select_star(query.selectList) then
        log.debug('Missing select list interpreted as: SELECT *')
    elseif is_empty_select_list(query.selectList) then
        replace_empty_select_list_with_constant_expression(query)
    end
end

--- Rewrite the original query.
-- @param _ structure containing the original push-down query
-- @param _ source schema the Exasol VS is put on top of
-- @param _ cache taken from the adapter notes
-- @param _ list of tables that appear in the query
-- @return string containing the rewritten query
function AbstractQueryRewriter:rewrite(_, _, _, _)
    error("Called abstract function 'rewrite'.")
end

return AbstractQueryRewriter