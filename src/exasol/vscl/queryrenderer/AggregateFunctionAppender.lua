--- Appender for aggregate functions in an SQL statement.
---@class AggregateFunctionAppender: AbstractQueryAppender
local AggregateFunctionAppender = {}
AggregateFunctionAppender.__index = AggregateFunctionAppender
local AbstractQueryAppender = require("exasol.vscl.queryrenderer.AbstractQueryAppender")
setmetatable(AggregateFunctionAppender, {__index = AbstractQueryAppender})

local ExpressionAppender = require("exasol.vscl.queryrenderer.ExpressionAppender")
local SelectAppender = require("exasol.vscl.queryrenderer.SelectAppender")
local ExaError = require("ExaError")

--- Create a new instance of a `AggregateFunctionAppender`.
---@param out_query Query query to which the function will be appended
---@param appender_config AppenderConfig
---@return AggregateFunctionAppender renderer for aggregate functions
function AggregateFunctionAppender:new(out_query, appender_config)
    local instance = setmetatable({}, self)
    instance:_init(out_query, appender_config)
    return instance
end

---@param out_query Query
---@param appender_config AppenderConfig
function AggregateFunctionAppender:_init(out_query, appender_config)
    AbstractQueryAppender._init(self, out_query, appender_config)
end

--- Append an aggregate function to an SQL query.
---@param aggregate_function AggregateFunctionExpression function to append
function AggregateFunctionAppender:append_aggregate_function(aggregate_function)
    local function_name = string.lower(aggregate_function.name)
    local implementation = AggregateFunctionAppender["_" .. function_name]
    if implementation ~= nil then
        implementation(self, aggregate_function)
    else
        ExaError:new("E-VSCL-3", "Unable to render unsupported aggregate function type {{function_name}}.", {
            function_name = {value = function_name, description = "name of the SQL function that is not yet supported"}
        }):add_ticket_mitigation():raise()
    end
end

-- Alias for main appender function for uniform appender invocation
AggregateFunctionAppender.append = AggregateFunctionAppender.append_aggregate_function

---@param expression Expression
function AggregateFunctionAppender:_append_expression(expression)
    local expression_renderer = ExpressionAppender:new(self._out_query, self._appender_config)
    expression_renderer:append_expression(expression)
end

function AggregateFunctionAppender:_append_function_argument_list(distinct, arguments)
    self:_append("(")
    self:_append_distinct_modifier(distinct)
    self:_append_comma_separated_arguments(arguments)
    self:_append(")")
end

function AggregateFunctionAppender:_append_distinct_modifier(distinct)
    if distinct then
        self:_append("DISTINCT ")
    end
end

function AggregateFunctionAppender:_append_comma_separated_arguments(arguments)
    if (arguments) then
        for i = 1, #arguments do
            self:_comma(i)
            self:_append_expression(arguments[i])
        end
    end
end

function AggregateFunctionAppender:_append_distinct_function(f)
    self:_append(string.upper(f.name))
    local distinct = f.distinct or false
    self:_append_function_argument_list(distinct, f.arguments)
end

function AggregateFunctionAppender:_append_simple_function(f)
    assert(not f.distinct, "Aggregate function '" .. (f.name or "unknown") .. "' must not have a DISTINCT modifier.")
    self:_append(string.upper(f.name))
    self:_append_function_argument_list(false, f.arguments)
end

-- AggregateFunctionAppender._any is not implemented since ANY is an alias for SOME
AggregateFunctionAppender._approximate_count_distinct = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._avg = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._corr = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._covar_pop = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._covar_samp = AggregateFunctionAppender._append_simple_function

function AggregateFunctionAppender:_count(f)
    local distinct = f.distinct or false
    if (f.arguments == nil or next(f.arguments) == nil) then
        self:_append("COUNT(*)")
    elseif (#f.arguments == 1) then
        self:_append("COUNT")
        self:_append_function_argument_list(distinct, f.arguments)
    else
        self:_append("COUNT(")
        self:_append_distinct_modifier(distinct)
        -- Note the extra set of parenthesis that is required to count tuples!
        self:_append("(")
        self:_append_comma_separated_arguments(f.arguments)
        self:_append("))")
    end
end

AggregateFunctionAppender._every = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._first_value = AggregateFunctionAppender._append_simple_function

---@return SelectAppender
function AggregateFunctionAppender:_select_appender()
    return SelectAppender:new(self._out_query, self._appender_config)
end

function AggregateFunctionAppender:_group_concat(f)
    self:_append(string.upper(f.name))
    self:_append("(")
    if f.distinct then
        self:_append("DISTINCT ")
    end
    self:_append_comma_separated_arguments(f.arguments)
    if f.orderBy then
        self:_select_appender():_append_order_by(f.orderBy)
    end
    if f.separator then
        self:_append(" SEPARATOR ")
        self:_append_string_literal(f.separator)
    end
    self:_append(")")
end

AggregateFunctionAppender._grouping = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._grouping_id = AggregateFunctionAppender._grouping
AggregateFunctionAppender._last_value = AggregateFunctionAppender._append_simple_function

function AggregateFunctionAppender:_listagg(f)
    self:_append("LISTAGG(")
    if f.distinct then
        self:_append("DISTINCT ")
    end
    self:_append_expression(f.arguments[1])
    if f.separator then
        self:_append(", ")
        self:_append_expression(f.separator)
    end
    local overflow = f.overflowBehavior
    if overflow then
        if overflow.type == "ERROR" then
            self:_append(" ON OVERFLOW ERROR")
        elseif overflow.type == "TRUNCATE" then
            self:_append(" ON OVERFLOW TRUNCATE")
            if overflow.truncationFiller then
                self:_append(" ")
                self:_append_expression(overflow.truncationFiller)
            end
            self:_append((overflow.truncationType == "WITH COUNT") and " WITH COUNT" or " WITHOUT COUNT")
        end
    end
    self:_append(")")
    if f.orderBy then
        self:_append(" WITHIN GROUP (")
        self:_select_appender():_append_order_by(f.orderBy, true)
        self:_append(")")
    end
end

AggregateFunctionAppender._max = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._median = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._min = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._mul = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._regr_avgx = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_avgy = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_count = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_intercept = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_r2 = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_slope = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_sxx = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_sxy = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._regr_syy = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._st_intersection = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._st_union = AggregateFunctionAppender._append_simple_function
AggregateFunctionAppender._stddev = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._stddev_pop = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._stddev_samp = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._sum = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._some = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._var_pop = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._var_samp = AggregateFunctionAppender._append_distinct_function
AggregateFunctionAppender._variance = AggregateFunctionAppender._append_distinct_function

return AggregateFunctionAppender
