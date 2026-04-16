local QueryRewriter = {_NAME = "QueryRewriter"}
QueryRewriter.__index = QueryRewriter
local AbstractQueryRewriter = require("exasol.evscl.AbstractQueryRewriter")
setmetatable(QueryRewriter, {__index = AbstractQueryRewriter})

local QueryRenderer = require("exasol.vscl.QueryRenderer")
local AbstractQueryAppender = require("exasol.vscl.queryrenderer.AbstractQueryAppender")
local helpers = require("util.helpers")

function QueryRewriter:new()
    local instance = setmetatable({}, self)
    instance:_init()
    return instance
end

function QueryRewriter:_init()
    AbstractQueryRewriter._init(self)
end

function QueryRewriter:rewrite(original_query, source_schema_id)
    self:_validate(original_query)
    local rewritten_query = helpers.deep_copy(original_query)
    self:_expand_select_list(rewritten_query)
    rewritten_query = self:_extend_query_with_source_schema(rewritten_query, source_schema_id)
    local renderer = QueryRenderer:new(rewritten_query, AbstractQueryAppender.DEFAULT_APPENDER_CONFIG)
    return renderer:render()
end

return QueryRewriter
