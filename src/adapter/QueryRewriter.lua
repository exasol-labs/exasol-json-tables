local QueryRewriter = {_NAME = "QueryRewriter"}
QueryRewriter.__index = QueryRewriter
local AbstractQueryRewriter = require("exasol.evscl.AbstractQueryRewriter")
setmetatable(QueryRewriter, {__index = AbstractQueryRewriter})

local cjson = require("cjson")
local QueryRenderer = require("exasol.vscl.QueryRenderer")
local AbstractQueryAppender = require("exasol.vscl.queryrenderer.AbstractQueryAppender")
local helpers = require("util.helpers")

local TRUE_LITERAL<const> = {type = "literal_bool", value = true}
local FALSE_LITERAL<const> = {type = "literal_bool", value = false}
local NULL_LITERAL<const> = {type = "literal_null"}
local TYPEOF_RESULT_DATA_TYPE<const> = {type = "VARCHAR", size = 40}

function QueryRewriter:new()
    local instance = setmetatable({}, self)
    instance:_init()
    return instance
end

function QueryRewriter:_init()
    AbstractQueryRewriter._init(self)
end

local function build_table_lookup(involved_tables)
    local lookup = {}
    for _, table_metadata in ipairs(involved_tables or {}) do
        lookup[table_metadata.name] = table_metadata
    end
    return lookup
end

local function build_column_notes_lookup(involved_tables)
    local lookup = {}
    for _, table_metadata in ipairs(involved_tables or {}) do
        lookup[table_metadata.name] = lookup[table_metadata.name] or {}
        for _, column_metadata in ipairs(table_metadata.columns or {}) do
            if column_metadata.adapterNotes then
                lookup[table_metadata.name][column_metadata.name] = cjson.decode(column_metadata.adapterNotes)
            end
        end
    end
    return lookup
end

local function create_column_reference(column_reference, _, column_name)
    local rewritten = helpers.deep_copy(column_reference)
    if column_name ~= nil then
        rewritten.name = column_name
    end
    return rewritten
end

local function create_mask_case_expression(mask_column_reference)
    return {
        type = "function_scalar_case",
        name = "CASE",
        basis = mask_column_reference,
        arguments = {TRUE_LITERAL},
        results = {TRUE_LITERAL, FALSE_LITERAL}
    }
end

local function create_mask_truth_predicate(mask_column_reference)
    return {
        type = "predicate_equal",
        left = create_mask_case_expression(mask_column_reference),
        right = TRUE_LITERAL
    }
end

local function create_is_not_null_predicate(expression)
    return {
        type = "predicate_is_not_null",
        expression = expression
    }
end

local function create_string_literal(value)
    return {
        type = "literal_string",
        value = value
    }
end

local function create_case_expression(arguments, results)
    return {
        type = "function_scalar_case",
        name = "CASE",
        basis = TRUE_LITERAL,
        arguments = arguments,
        results = results
    }
end

local function create_cast_expression(expression, data_type)
    return {
        type = "function_scalar_cast",
        name = "CAST",
        arguments = {expression},
        dataType = helpers.deep_copy(data_type)
    }
end

local function is_boolean_literal(expression, expected)
    return expression and expression.type == "literal_bool" and expression.value == expected
end

function QueryRewriter:_lookup_column_notes(column_reference, notes_lookup)
    local table_notes = notes_lookup[column_reference.tableName]
    if table_notes then
        return table_notes[column_reference.name]
    end
    return nil
end

function QueryRewriter:_resolve_column_reference(column_reference, notes_lookup)
    local column_notes = self:_lookup_column_notes(column_reference, notes_lookup)
    local rewritten = helpers.deep_copy(column_reference)
    if column_notes and column_notes.physicalColumn then
        rewritten.name = column_notes.physicalColumn
    end
    return rewritten, column_notes
end

function QueryRewriter:_build_variant_entries(column_reference, column_notes)
    local variant_entries = {}
    for _, variant_label in ipairs(column_notes.variantOrder or {}) do
        local physical_column = column_notes.variantColumns and column_notes.variantColumns[variant_label] or nil
        if physical_column ~= nil then
            variant_entries[#variant_entries + 1] = {
                label = variant_label,
                reference = create_column_reference(column_reference, nil, physical_column)
            }
        end
    end
    return variant_entries
end

function QueryRewriter:_build_mask_reference(column_reference, column_notes)
    if column_notes == nil or column_notes.nullMaskColumn == nil then
        return nil
    end
    return create_column_reference(column_reference, nil, column_notes.nullMaskColumn)
end

function QueryRewriter:_rewrite_explicit_null_marker(expression, notes_lookup)
    if string.upper(expression.name or "") ~= "CASE" or expression.basis ~= nil then
        return nil
    end
    local arguments = expression.arguments or {}
    local results = expression.results or {}
    if #arguments ~= 1 or #results ~= 2 then
        return nil
    end
    local predicate = arguments[1]
    if predicate == nil or predicate.type ~= "predicate_is_null" or predicate.expression == nil then
        return nil
    end
    local referenced_expression = predicate.expression
    if referenced_expression.type ~= "column" then
        return nil
    end
    if not (is_boolean_literal(results[1], true) and is_boolean_literal(results[2], false)) then
        return nil
    end
    local mask_reference = self:_build_mask_reference(
            referenced_expression,
            self:_lookup_column_notes(referenced_expression, notes_lookup)
    )
    if mask_reference == nil then
        return nil
    end
    return create_mask_truth_predicate(mask_reference)
end

function QueryRewriter:_rewrite_variant_typeof(expression, notes_lookup)
    if string.upper(expression.name or "") ~= "TYPEOF" then
        return nil
    end
    local arguments = expression.arguments or {}
    if #arguments ~= 1 or arguments[1].type ~= "column" then
        return nil
    end
    local column_reference = arguments[1]
    local column_notes = self:_lookup_column_notes(column_reference, notes_lookup)
    if column_notes == nil or column_notes.variantColumns == nil or column_notes.variantOrder == nil then
        return nil
    end
    local case_arguments = {}
    local case_results = {}
    for _, variant_entry in ipairs(self:_build_variant_entries(column_reference, column_notes)) do
        case_arguments[#case_arguments + 1] = create_is_not_null_predicate(variant_entry.reference)
        case_results[#case_results + 1] = create_string_literal(variant_entry.label)
    end
    local mask_reference = self:_build_mask_reference(column_reference, column_notes)
    if mask_reference ~= nil then
        case_arguments[#case_arguments + 1] = create_mask_truth_predicate(mask_reference)
        case_results[#case_results + 1] = create_string_literal("NULL")
    end
    case_results[#case_results + 1] = NULL_LITERAL
    return create_cast_expression(create_case_expression(case_arguments, case_results), TYPEOF_RESULT_DATA_TYPE)
end

function QueryRewriter:_rewrite_variant_cast(expression, notes_lookup)
    if expression.type ~= "function_scalar_cast" or string.upper(expression.name or "") ~= "CAST" then
        return nil
    end
    local arguments = expression.arguments or {}
    if #arguments ~= 1 or arguments[1].type ~= "column" then
        return nil
    end
    local column_reference = arguments[1]
    local column_notes = self:_lookup_column_notes(column_reference, notes_lookup)
    if column_notes == nil or column_notes.variantColumns == nil or column_notes.variantOrder == nil then
        return nil
    end
    local case_arguments = {}
    local case_results = {}
    for _, variant_entry in ipairs(self:_build_variant_entries(column_reference, column_notes)) do
        case_arguments[#case_arguments + 1] = create_is_not_null_predicate(variant_entry.reference)
        case_results[#case_results + 1] = create_cast_expression(variant_entry.reference, expression.dataType)
    end
    local mask_reference = self:_build_mask_reference(column_reference, column_notes)
    if mask_reference ~= nil then
        case_arguments[#case_arguments + 1] = create_mask_truth_predicate(mask_reference)
        case_results[#case_results + 1] = NULL_LITERAL
    end
    case_results[#case_results + 1] = NULL_LITERAL
    return create_case_expression(case_arguments, case_results)
end

function QueryRewriter:_rewrite_expression(expression, notes_lookup, table_lookup)
    if expression == nil then
        return nil
    end
    local expression_type = expression.type
    if expression_type == "column" then
        return self:_resolve_column_reference(expression, notes_lookup)
    elseif helpers.starts_with(expression_type, "literal_") then
        return helpers.deep_copy(expression)
    elseif expression_type == "predicate_is_null" then
        return {
            type = expression_type,
            expression = self:_rewrite_expression(expression.expression, notes_lookup, table_lookup)
        }
    elseif expression_type == "predicate_is_not_null" or expression_type == "predicate_not" then
        return {
            type = expression_type,
            expression = self:_rewrite_expression(expression.expression, notes_lookup, table_lookup)
        }
    elseif expression_type == "predicate_exists" then
        return {type = expression_type, query = self:_rewrite_select(expression.query, notes_lookup, table_lookup)}
    elseif expression_type == "predicate_and" or expression_type == "predicate_or" then
        local rewritten = {type = expression_type, expressions = {}}
        for i, nested in ipairs(expression.expressions or {}) do
            rewritten.expressions[i] = self:_rewrite_expression(nested, notes_lookup, table_lookup)
        end
        return rewritten
    elseif expression_type == "predicate_equal" or expression_type == "predicate_notequal" or
            expression_type == "predicate_greater" or expression_type == "predicate_less" or
            expression_type == "predicate_lessequal" or expression_type == "predicate_greaterequal" then
        return {
            type = expression_type,
            left = self:_rewrite_expression(expression.left, notes_lookup, table_lookup),
            right = self:_rewrite_expression(expression.right, notes_lookup, table_lookup)
        }
    elseif expression_type == "predicate_between" then
        return {
            type = expression_type,
            expression = self:_rewrite_expression(expression.expression, notes_lookup, table_lookup),
            left = self:_rewrite_expression(expression.left, notes_lookup, table_lookup),
            right = self:_rewrite_expression(expression.right, notes_lookup, table_lookup)
        }
    elseif expression_type == "predicate_in_constlist" then
        local rewritten = {
            type = expression_type,
            expression = self:_rewrite_expression(expression.expression, notes_lookup, table_lookup),
            arguments = {}
        }
        for i, argument in ipairs(expression.arguments or {}) do
            rewritten.arguments[i] = self:_rewrite_expression(argument, notes_lookup, table_lookup)
        end
        return rewritten
    elseif expression_type == "predicate_like" then
        return {
            type = expression_type,
            expression = self:_rewrite_expression(expression.expression, notes_lookup, table_lookup),
            pattern = self:_rewrite_expression(expression.pattern, notes_lookup, table_lookup),
            escapeChar = self:_rewrite_expression(expression.escapeChar, notes_lookup, table_lookup)
        }
    elseif expression_type == "predicate_like_regexp" then
        return {
            type = expression_type,
            expression = self:_rewrite_expression(expression.expression, notes_lookup, table_lookup),
            pattern = self:_rewrite_expression(expression.pattern, notes_lookup, table_lookup)
        }
    elseif expression_type == "predicate_is_json" or expression_type == "predicate_is_not_json" then
        local rewritten = helpers.deep_copy(expression)
        rewritten.expression = self:_rewrite_expression(expression.expression, notes_lookup, table_lookup)
        return rewritten
    elseif helpers.starts_with(expression_type, "function_scalar") then
        local explicit_null_marker = self:_rewrite_explicit_null_marker(expression, notes_lookup)
        if explicit_null_marker ~= nil then
            return explicit_null_marker
        end
        local rewritten_variant_typeof = self:_rewrite_variant_typeof(expression, notes_lookup)
        if rewritten_variant_typeof ~= nil then
            return rewritten_variant_typeof
        end
        local rewritten_variant_cast = self:_rewrite_variant_cast(expression, notes_lookup)
        if rewritten_variant_cast ~= nil then
            return rewritten_variant_cast
        end
        local rewritten = helpers.deep_copy(expression)
        if expression.basis then
            rewritten.basis = self:_rewrite_expression(expression.basis, notes_lookup, table_lookup)
        end
        if expression.arguments then
            rewritten.arguments = {}
            for i, argument in ipairs(expression.arguments) do
                rewritten.arguments[i] = self:_rewrite_expression(argument, notes_lookup, table_lookup)
            end
        end
        if expression.results then
            rewritten.results = {}
            for i, result in ipairs(expression.results) do
                rewritten.results[i] = self:_rewrite_expression(result, notes_lookup, table_lookup)
            end
        end
        if expression.emptyBehavior and expression.emptyBehavior.expression then
            rewritten.emptyBehavior.expression = self:_rewrite_expression(expression.emptyBehavior.expression,
                    notes_lookup, table_lookup)
        end
        if expression.errorBehavior and expression.errorBehavior.expression then
            rewritten.errorBehavior.expression = self:_rewrite_expression(expression.errorBehavior.expression,
                    notes_lookup, table_lookup)
        end
        if string.upper(expression.name or "") == "CASE" and expression.basis == nil and rewritten.arguments then
            rewritten.basis = TRUE_LITERAL
        end
        return rewritten
    elseif helpers.starts_with(expression_type, "function_aggregate") then
        local rewritten = helpers.deep_copy(expression)
        if expression.arguments then
            rewritten.arguments = {}
            for i, argument in ipairs(expression.arguments) do
                rewritten.arguments[i] = self:_rewrite_expression(argument, notes_lookup, table_lookup)
            end
        end
        if expression.orderBy then
            rewritten.orderBy = {}
            for i, order_by in ipairs(expression.orderBy) do
                rewritten.orderBy[i] = {
                    expression = self:_rewrite_expression(order_by.expression, notes_lookup, table_lookup),
                    isAscending = order_by.isAscending,
                    nullsLast = order_by.nullsLast
                }
            end
        end
        return rewritten
    elseif expression_type == "sub_select" then
        return self:_rewrite_select(expression, notes_lookup, table_lookup)
    else
        return helpers.deep_copy(expression)
    end
end

function QueryRewriter:_expand_select_star(query, table_lookup)
    if query.selectList ~= nil then
        if next(query.selectList) == nil then
            query.selectList = {{type = "literal_bool", value = true}}
        end
        return
    end
    local select_list = {}
    for _, table_expression in ipairs(helpers.collect_tables(query.from)) do
        local table_metadata = table_lookup[table_expression.name]
        if table_metadata then
            for _, column_metadata in ipairs(table_metadata.columns or {}) do
                select_list[#select_list + 1] = {
                    type = "column",
                    tableName = table_expression.name,
                    tableAlias = table_expression.alias,
                    name = column_metadata.name
                }
            end
        end
    end
    query.selectList = select_list
end

function QueryRewriter:_rewrite_from(from_clause, notes_lookup, table_lookup)
    if not from_clause then
        return nil
    end
    local rewritten = helpers.deep_copy(from_clause)
    if from_clause.type == "join" then
        rewritten.left = self:_rewrite_from(from_clause.left, notes_lookup, table_lookup)
        rewritten.right = self:_rewrite_from(from_clause.right, notes_lookup, table_lookup)
        rewritten.condition = self:_rewrite_expression(from_clause.condition, notes_lookup, table_lookup)
    end
    return rewritten
end

function QueryRewriter:_rewrite_select(query, notes_lookup, table_lookup)
    local rewritten = helpers.deep_copy(query)
    self:_expand_select_star(rewritten, table_lookup)
    if rewritten.selectList then
        local select_list = {}
        for i, expression in ipairs(rewritten.selectList) do
            select_list[i] = self:_rewrite_expression(expression, notes_lookup, table_lookup)
        end
        rewritten.selectList = select_list
    end
    rewritten.filter = self:_rewrite_expression(query.filter, notes_lookup, table_lookup)
    if query.groupBy then
        rewritten.groupBy = {}
        for i, group_by in ipairs(query.groupBy) do
            rewritten.groupBy[i] = self:_rewrite_expression(group_by, notes_lookup, table_lookup)
        end
    end
    if query.orderBy then
        rewritten.orderBy = {}
        for i, order_by in ipairs(query.orderBy) do
            rewritten.orderBy[i] = {
                expression = self:_rewrite_expression(order_by.expression, notes_lookup, table_lookup),
                isAscending = order_by.isAscending,
                nullsLast = order_by.nullsLast
            }
        end
    end
    rewritten.from = self:_rewrite_from(query.from, notes_lookup, table_lookup)
    return rewritten
end

function QueryRewriter:rewrite(original_query, source_schema_id, _, involved_tables)
    self:_validate(original_query)
    local table_lookup = build_table_lookup(involved_tables)
    local notes_lookup = build_column_notes_lookup(involved_tables)
    local rewritten_query = self:_rewrite_select(original_query, notes_lookup, table_lookup)
    rewritten_query = self:_extend_query_with_source_schema(rewritten_query, source_schema_id)
    local renderer = QueryRenderer:new(rewritten_query, AbstractQueryAppender.DEFAULT_APPENDER_CONFIG)
    return renderer:render()
end

return QueryRewriter
