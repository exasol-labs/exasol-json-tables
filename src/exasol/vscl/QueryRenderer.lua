--- Renderer for SQL queries.
---@class QueryRenderer
---@field _original_query QueryStatement
---@field _appender_config AppenderConfig
local QueryRenderer = {}
QueryRenderer.__index = QueryRenderer

local Query = require("exasol.vscl.Query")
local SelectAppender = require("exasol.vscl.queryrenderer.SelectAppender")
local ImportAppender = require("exasol.vscl.queryrenderer.ImportAppender")

--- Create a new query renderer.
---@param original_query QueryStatement query structure as provided through the Virtual Schema API
---@param appender_config AppenderConfig configuration for the query renderer containing identifier quoting
---@return QueryRenderer query_renderer instance
function QueryRenderer:new(original_query, appender_config)
    local instance = setmetatable({}, self)
    instance:_init(original_query, appender_config)
    return instance
end

---@param original_query QueryStatement query structure as provided through the Virtual Schema API
---@param appender_config AppenderConfig configuration for the query renderer containing identifier quoting
function QueryRenderer:_init(original_query, appender_config)
    self._original_query = original_query
    self._appender_config = appender_config
end

---@param query QueryStatement
---@return ImportAppender|SelectAppender
local function get_appender_class(query)
    if query.type == "import" then
        return ImportAppender
    else
        return SelectAppender
    end
end

--- Render the query to a string.
---@return string rendered_query query as string
function QueryRenderer:render()
    local out_query = Query:new()
    local appender_class = get_appender_class(self._original_query)
    local appender = appender_class:new(out_query, self._appender_config)
    appender:append(self._original_query)
    return out_query:to_string()
end

return QueryRenderer
