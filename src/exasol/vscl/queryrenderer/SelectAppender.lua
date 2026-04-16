--- Appender that can add top-level elements of a `SELECT` statement (or sub-select).
---@class SelectAppender: AbstractQueryAppender
local SelectAppender = {}
SelectAppender.__index = SelectAppender
local AbstractQueryAppender = require("exasol.vscl.queryrenderer.AbstractQueryAppender")
setmetatable(SelectAppender, {__index = AbstractQueryAppender})

local ExaError = require("ExaError")
local log = require("remotelog")
local ExpressionAppender = require("exasol.vscl.queryrenderer.ExpressionAppender")

local JOIN_TYPES<const> = {
    inner = "INNER",
    left_outer = "LEFT OUTER",
    right_outer = "RIGHT OUTER",
    full_outer = "FULL OUTER"
}

--- Get a map of supported JOIN type to the join keyword.
---@return table<string, string> join type (key) mapped to SQL join keyword
function SelectAppender.get_join_types()
    return JOIN_TYPES
end

--- Create a new query renderer.
---@param out_query Query query structure as provided through the Virtual Schema API
---@param appender_config AppenderConfig
---@return SelectAppender query renderer instance
function SelectAppender:new(out_query, appender_config)
    local instance = setmetatable({}, self)
    instance:_init(out_query, appender_config)
    return instance
end

---@param out_query Query
---@param appender_config AppenderConfig
function SelectAppender:_init(out_query, appender_config)
    AbstractQueryAppender._init(self, out_query, appender_config)
end

---@param select_list SelectList
function SelectAppender:_append_select_list_elements(select_list)
    for i = 1, #select_list do
        local element = select_list[i]
        self:_comma(i)
        self:_append_expression(element)
    end
end

---@param select_list SelectList?
function SelectAppender:_append_select_list(select_list)
    if not select_list then
        self:_append("*")
    else
        self:_append_select_list_elements(select_list)
    end
end

---@param table TableExpression
function SelectAppender:_append_table(table)
    if table.schema then
        if table.catalog then
            self:_append_identifier(table.catalog)
            self:_append('.')
        end
        self:_append_identifier(table.schema)
        self:_append('.')
    end
    self:_append_identifier(table.name)
    if table.alias then
        self:_append(' ')
        self:_append_identifier(table.alias)
    end
end

---@param from_clause FromClause
function SelectAppender:_append_from_expression(from_clause)
    if from_clause.type == "table" then
        self:_append_table(from_clause)
    elseif from_clause.type == "join" then
        self:_append("(")
        self:_append_join(from_clause)
        self:_append(")")
    else
        ExaError:new("E-VSCL-5", "Unable to render unknown SQL FROM clause type {{type}}.",
                     {type = {value = from_clause.type, description = "type of the FROM clause that was not recognized"}})
                :add_ticket_mitigation():raise()
    end
end

---@param join JoinExpression
function SelectAppender:_append_join(join)
    local join_type_keyword = JOIN_TYPES[join.join_type]
    if join_type_keyword then
        self:_append_from_expression(join.left)
        self:_append(' ')
        self:_append(join_type_keyword)
        self:_append(' JOIN ')
        self:_append_from_expression(join.right)
        self:_append(' ON ')
        self:_append_expression(join.condition)
    else
        ExaError:new("E-VSCL-6", "Unable to render unknown join type {{type}}.",
                     {type = {value = join.join_type, description = "type of join that was not recognized"}})
                :add_ticket_mitigation():raise()
    end
end

---@param from FromClause
function SelectAppender:_append_from(from)
    if from then
        self:_append(' FROM ')
        self:_append_from_expression(from)
    end
end

---@return ExpressionAppender
function SelectAppender:_expression_appender()
    return ExpressionAppender:new(self._out_query, self._appender_config)
end

---@param expression Expression
function SelectAppender:_append_expression(expression)
    self:_expression_appender():append_expression(expression)
end

---@param filter PredicateExpression
function SelectAppender:_append_filter(filter)
    if filter then
        self:_append(" WHERE ")
        self:_expression_appender():append_predicate(filter)
    end
end

--- Replace an unsupported expression in a `GROUP BY` clause with a supported one or return it unchanged.
--
-- This replaces numeric literals with the corresponding string value, as Exasol interprets
-- `GROUP BY <integer-constant>` as column number &mdash; which is not what the user intended. Also,
-- please note that `GROUP BY <constant>` always leads to grouping with a single group, regardless of the
-- actual value of the constant (except for `FALSE`, which is reserved).
--
---@param group_by_criteria Expression the original `GROUP BY` expression
---@return Expression alternative_expression a new, alternative expression or the original expression
---                                          if no replacement is necessary
local function workaround_group_by_integer(group_by_criteria)
    if group_by_criteria.type == "literal_exactnumeric" then
        local new_value = tostring(group_by_criteria.value)
        log.debug("Replacing numeric literal " .. new_value .. " with string literal in GROUP BY")
        return {type = "literal_string", value = new_value}
    else
        return group_by_criteria
    end
end

---@param group Expression[]?
function SelectAppender:_append_group_by(group)
    if group then
        self:_append(" GROUP BY ")
        for i, criteria in ipairs(group) do
            self:_comma(i)
            self:_expression_appender():append_expression(workaround_group_by_integer(criteria))
        end
    end
end

---@param order OrderByClause[]?
---@param in_parenthesis boolean?
function SelectAppender:_append_order_by(order, in_parenthesis)
    if order then
        if not in_parenthesis then
            self:_append(" ")
        end
        self:_append("ORDER BY ")
        for i, criteria in ipairs(order) do
            self:_comma(i)
            self:_expression_appender():append_expression(criteria.expression)
            if criteria.isAscending ~= nil then
                self:_append(criteria.isAscending and " ASC" or " DESC")
            end
            if criteria.nullsLast ~= nil then
                self:_append(criteria.nullsLast and " NULLS LAST" or " NULLS FIRST")
            end
        end
    end
end

---@param limit LimitClause
function SelectAppender:_append_limit(limit)
    if limit then
        self:_append(" LIMIT ")
        self:_append(limit.numElements)
        if limit.offset then
            self:_append(" OFFSET ")
            self:_append(limit.offset)
        end
    end
end

--- Append a sub-select statement.
-- This method is public to allow recursive queries (e.g. embedded into an `EXISTS` clause in an expression.
---@param sub_query SelectSqlStatement query appended
function SelectAppender:append_sub_select(sub_query)
    self:_append("(")
    self:append_select(sub_query)
    self:_append(")")
end

--- Append a `SELECT` statement.
---@param sub_query SelectSqlStatement query appended
function SelectAppender:append_select(sub_query)
    self:_append("SELECT ")
    self:_append_select_list(sub_query.selectList)
    self:_append_from(sub_query.from)
    self:_append_filter(sub_query.filter)
    self:_append_group_by(sub_query.groupBy)
    self:_append_order_by(sub_query.orderBy)
    self:_append_limit(sub_query.limit)
end

-- Alias for the main entry point allows uniform appender invocation
SelectAppender.append = SelectAppender.append_select

return SelectAppender
