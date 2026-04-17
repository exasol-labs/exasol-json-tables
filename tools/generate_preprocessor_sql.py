#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "examples" / "json_is_explicit_null_preprocessor.sql"
IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an installable Exasol SQL preprocessor script that rewrites configured "
            "function calls to an explicit-null marker expression for the JSON virtual schema adapter."
        )
    )
    parser.add_argument("--schema", default="JVS_PP", help="Schema that will own the preprocessor script.")
    parser.add_argument("--script", default="JSON_NULL_PREPROCESSOR", help="Preprocessor script name.")
    parser.add_argument(
        "--function-name",
        dest="function_names",
        action="append",
        default=None,
        help="Function name to rewrite. Repeat to install aliases. Default: JSON_IS_EXPLICIT_NULL.",
    )
    parser.add_argument(
        "--rewrite-path-identifiers",
        action="store_true",
        help='Rewrite quoted dotted identifiers like "child.value" and array access like "items[0].value".',
    )
    parser.add_argument(
        "--virtual-schema",
        dest="virtual_schemas",
        action="append",
        default=None,
        help=(
            "Virtual schema name that is allowed to use the JSON helper/path syntax. "
            "Repeat to allow multiple virtual schemas. Default: JSON_VS."
        ),
    )
    parser.add_argument(
        "--activate-session",
        action="store_true",
        help="Append an ALTER SESSION statement that activates the generated preprocessor for the current session.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output SQL file.")
    return parser.parse_args()


def validate_identifier(kind: str, value: str) -> str:
    if not IDENTIFIER_RE.match(value):
        raise SystemExit(f"{kind} must be an unquoted SQL identifier made of letters, digits, and underscores: {value}")
    return value.upper()


COMMON_LUA = """
    local CLAUSE_KEYWORDS = {
        WHERE = true, GROUP = true, ORDER = true, LIMIT = true, OFFSET = true,
        QUALIFY = true, CONNECT = true, START = true, UNION = true, MINUS = true,
        EXCEPT = true, HAVING = true
    }
    local QUERY_START_KEYWORDS = {
        SELECT = true, WITH = true, EXPLAIN = true
    }
    local EXPRESSION_START_KEYWORDS = {
        SELECT = true, WHERE = true, WHEN = true, THEN = true, ELSE = true,
        ON = true, AND = true, OR = true, NOT = true, BY = true, HAVING = true,
        QUALIFY = true, CONNECT = true, START = true, USING = true, IN = true,
        EXISTS = true, IS = true, CASE = true, BETWEEN = true, LIKE = true
    }
    local EXPRESSION_START_TOKENS = {
        [","] = true, ["("] = true, ["="] = true, [">"] = true, ["<"] = true,
        [">="] = true, ["<="] = true, ["<>"] = true, ["!="] = true, ["+"] = true,
        ["-"] = true, ["*"] = true, ["/"] = true, ["||"] = true
    }

    local function encode_quoted_identifier(identifier)
        return '"' .. string.gsub(identifier, '"', '""') .. '"'
    end

    local function encode_string_literal(value)
        return "'" .. string.gsub(value, "'", "''") .. "'"
    end

    local function decode_quoted_identifier(token)
        local out = {}
        local index = 2
        while index < #token do
            local ch = string.sub(token, index, index)
            if ch == '"' and string.sub(token, index + 1, index + 1) == '"' then
                out[#out + 1] = '"'
                index = index + 2
            else
                out[#out + 1] = ch
                index = index + 1
            end
        end
        return table.concat(out)
    end

    local function tokenize_path_sql(sqltext)
        local tokens = {}
        local index = 1
        while index <= #sqltext do
            local ch = string.sub(sqltext, index, index)
            local next_ch = string.sub(sqltext, index + 1, index + 1)
            if string.match(ch, "%s") then
                local start_index = index
                repeat
                    index = index + 1
                    ch = string.sub(sqltext, index, index)
                until index > #sqltext or not string.match(ch, "%s")
                tokens[#tokens + 1] = {type = "whitespace", text = string.sub(sqltext, start_index, index - 1)}
            elseif ch == "-" and next_ch == "-" then
                local start_index = index
                index = index + 2
                while index <= #sqltext and string.sub(sqltext, index, index) ~= "\\n" do
                    index = index + 1
                end
                if index <= #sqltext then
                    index = index + 1
                end
                tokens[#tokens + 1] = {type = "comment", text = string.sub(sqltext, start_index, index - 1)}
            elseif ch == "/" and next_ch == "*" then
                local start_index = index
                index = index + 2
                while index <= #sqltext - 1 and string.sub(sqltext, index, index + 1) ~= "*/" do
                    index = index + 1
                end
                if index <= #sqltext - 1 then
                    index = index + 2
                end
                tokens[#tokens + 1] = {type = "comment", text = string.sub(sqltext, start_index, index - 1)}
            elseif ch == "'" then
                local start_index = index
                index = index + 1
                while index <= #sqltext do
                    local current = string.sub(sqltext, index, index)
                    if current == "'" then
                        if string.sub(sqltext, index + 1, index + 1) == "'" then
                            index = index + 2
                        else
                            index = index + 1
                            break
                        end
                    else
                        index = index + 1
                    end
                end
                tokens[#tokens + 1] = {type = "string", text = string.sub(sqltext, start_index, index - 1)}
            elseif ch == '"' then
                local start_index = index
                index = index + 1
                while index <= #sqltext do
                    local current = string.sub(sqltext, index, index)
                    if current == '"' then
                        if string.sub(sqltext, index + 1, index + 1) == '"' then
                            index = index + 2
                        else
                            index = index + 1
                            break
                        end
                    else
                        index = index + 1
                    end
                end
                local token_text = string.sub(sqltext, start_index, index - 1)
                tokens[#tokens + 1] = {
                    type = "quoted_identifier",
                    text = token_text,
                    identifier = decode_quoted_identifier(token_text)
                }
            elseif string.match(ch, "[A-Za-z_]") then
                local start_index = index
                index = index + 1
                while index <= #sqltext and string.match(string.sub(sqltext, index, index), "[A-Za-z0-9_]") do
                    index = index + 1
                end
                tokens[#tokens + 1] = {type = "word", text = string.sub(sqltext, start_index, index - 1)}
            elseif string.match(ch, "%d") then
                local start_index = index
                index = index + 1
                while index <= #sqltext and string.match(string.sub(sqltext, index, index), "[0-9]") do
                    index = index + 1
                end
                tokens[#tokens + 1] = {type = "number", text = string.sub(sqltext, start_index, index - 1)}
            else
                local two_chars = string.sub(sqltext, index, index + 1)
                if two_chars == ">=" or two_chars == "<=" or two_chars == "<>" or two_chars == "!="
                        or two_chars == "||" then
                    tokens[#tokens + 1] = {type = "punct", text = two_chars}
                    index = index + 2
                else
                    tokens[#tokens + 1] = {type = "punct", text = ch}
                    index = index + 1
                end
            end
        end
        return tokens
    end

    local function next_significant_path_token(tokens, index)
        local current = index
        while current <= #tokens and (tokens[current].type == "whitespace" or tokens[current].type == "comment") do
            current = current + 1
        end
        if current <= #tokens then
            return tokens[current], current
        end
        return nil, nil
    end

    local function previous_significant_path_token(tokens, index)
        local current = index
        while current >= 1 and (tokens[current].type == "whitespace" or tokens[current].type == "comment") do
            current = current - 1
        end
        if current >= 1 then
            return tokens[current], current
        end
        return nil, nil
    end

    local function normalize_path_token(token)
        if token == nil or token.type ~= "word" then
            return nil
        end
        return string.upper(token.text)
    end

    local function split_identifier_token(token)
        local parts = {}
        local current = {}
        local in_quotes = false
        local index = 1
        while index <= #token do
            local ch = string.sub(token, index, index)
            if ch == '"' then
                if in_quotes and string.sub(token, index + 1, index + 1) == '"' then
                    current[#current + 1] = '"'
                    index = index + 2
                else
                    in_quotes = not in_quotes
                    index = index + 1
                end
            elseif ch == "." and not in_quotes then
                parts[#parts + 1] = table.concat(current)
                current = {}
                index = index + 1
            else
                current[#current + 1] = ch
                index = index + 1
            end
        end
        parts[#parts + 1] = table.concat(current)
        return parts
    end

    local function parse_identifier_token(token)
        if token == nil then
            return nil
        end
        if string.sub(token, 1, 1) ~= '"' and not sqlparsing.isidentifier(token) then
            return nil
        end
        return split_identifier_token(token)
    end

    local function next_significant_raw(tokens, index)
        local current = index
        while current <= #tokens and is_ignored(tokens[current]) do
            current = current + 1
        end
        return current
    end

    local function previous_significant_raw(tokens, index)
        local current = index
        while current >= 1 and is_ignored(tokens[current]) do
            current = current - 1
        end
        return current
    end

    local is_clause_keyword
    local is_join_keyword

    local function normalize_identifier_value(value)
        if value == nil then
            return nil
        end
        return string.upper(value)
    end

    local function read_alias_after_table(tokens, table_index)
        local alias_name = nil
        local alias_end_index = table_index
        local maybe_alias_index = next_significant_raw(tokens, table_index + 1)
        if maybe_alias_index <= #tokens and normalize(tokens[maybe_alias_index]) == "AS" then
            local alias_index = next_significant_raw(tokens, maybe_alias_index + 1)
            local alias_parts = parse_identifier_token(tokens[alias_index])
            if alias_parts ~= nil and #alias_parts == 1 then
                alias_name = alias_parts[1]
                alias_end_index = alias_index
            end
        else
            local alias_parts = parse_identifier_token(tokens[maybe_alias_index])
            if alias_parts ~= nil and #alias_parts == 1 and not is_clause_keyword(tokens[maybe_alias_index])
                    and not is_join_keyword(tokens[maybe_alias_index]) and tokens[maybe_alias_index] ~= ","
                    and tokens[maybe_alias_index] ~= ")" then
                alias_name = alias_parts[1]
                alias_end_index = maybe_alias_index
            end
        end
        return alias_name, alias_end_index
    end

    local function collect_top_level_table_references(tokens)
        local out = {}
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token == "(" then
                depth = depth + 1
            elseif token == ")" then
                depth = depth - 1
            elseif depth == 0 then
                local normalized = normalize(token)
                if normalized == "FROM" or normalized == "JOIN" then
                    local table_index = next_significant_raw(tokens, index + 1)
                    local table_parts = parse_identifier_token(tokens[table_index])
                    if table_parts ~= nil then
                        local alias_name, alias_end_index = read_alias_after_table(tokens, table_index)
                        out[#out + 1] = {
                            catalog_name = (#table_parts >= 3) and table_parts[#table_parts - 2] or nil,
                            schema_name = (#table_parts >= 2) and table_parts[#table_parts - 1] or nil,
                            table_name = table_parts[#table_parts],
                            alias_name = alias_name,
                            insert_after_index = alias_end_index
                        }
                        index = alias_end_index
                    end
                end
            end
            index = index + 1
        end
        return out
    end

    local function table_reference_is_allowed_virtual_schema(table_reference)
        if table_reference == nil or table_reference.schema_name == nil then
            return false
        end
        return ALLOWED_VIRTUAL_SCHEMAS[normalize_identifier_value(table_reference.schema_name)] == true
    end

    local function collect_allowed_virtual_table_references(table_references)
        local out = {}
        for _, table_reference in ipairs(table_references) do
            if table_reference_is_allowed_virtual_schema(table_reference) then
                out[#out + 1] = table_reference
            end
        end
        return out
    end

    local function read_base_table_reference(tokens)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token == "(" then
                depth = depth + 1
            elseif token == ")" then
                depth = depth - 1
            elseif depth == 0 and normalize(token) == "FROM" then
                local table_index = next_significant_raw(tokens, index + 1)
                local table_parts = parse_identifier_token(tokens[table_index])
                if table_parts == nil then
                    return nil
                end
                local alias_name, alias_end_index = read_alias_after_table(tokens, table_index)
                return {
                    catalog_name = (#table_parts >= 3) and table_parts[#table_parts - 2] or nil,
                    schema_name = (#table_parts >= 2) and table_parts[#table_parts - 1] or nil,
                    table_name = table_parts[#table_parts],
                    alias_name = alias_name,
                    insert_after_index = alias_end_index
                }
            end
            index = index + 1
        end
        return nil
    end

    local function query_has_top_level_join(tokens)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token == "(" then
                depth = depth + 1
            elseif token == ")" then
                depth = depth - 1
            elseif depth == 0 and normalize(token) == "JOIN" then
                return true
            end
            index = index + 1
        end
        return false
    end

    is_clause_keyword = function(token)
        local normalized = normalize(token)
        return normalized ~= nil and CLAUSE_KEYWORDS[normalized] == true
    end

    is_join_keyword = function(token)
        local normalized = normalize(token)
        return normalized == "JOIN" or normalized == "LEFT" or normalized == "RIGHT"
                or normalized == "FULL" or normalized == "INNER" or normalized == "CROSS"
    end

    local function is_path_query_statement(tokens)
        local first_token = next_significant_path_token(tokens, 1)
        if first_token == nil then
            return false
        end
        local normalized = normalize_path_token(first_token)
        return normalized ~= nil and QUERY_START_KEYWORDS[normalized] == true
    end

    local function can_start_expression_after_path_token(token)
        if token == nil then
            return false
        end
        if token.type == "punct" and EXPRESSION_START_TOKENS[token.text] == true then
            return true
        end
        local normalized = normalize_path_token(token)
        return normalized ~= nil and EXPRESSION_START_KEYWORDS[normalized] == true
    end

    local function is_clause_boundary_path_token(token)
        if token == nil then
            return true
        end
        if token.type == "punct" then
            return token.text == "," or token.text == ")"
        end
        local normalized = normalize_path_token(token)
        return normalized ~= nil and (CLAUSE_KEYWORDS[normalized] == true or normalized == "JOIN" or normalized == "LEFT"
                or normalized == "RIGHT" or normalized == "FULL" or normalized == "INNER"
                or normalized == "CROSS")
    end

    local function read_path_identifier(tokens, start_index)
        local token = tokens[start_index]
        if token == nil or token.type ~= "quoted_identifier" then
            return nil
        end

        local previous_token = previous_significant_path_token(tokens, start_index - 1)
        if previous_token ~= nil and (normalize_path_token(previous_token) == "AS"
                or (previous_token.type == "punct" and previous_token.text == ".")) then
            return nil
        end

        local next_token = next_significant_path_token(tokens, start_index + 1)
        if next_token ~= nil and next_token.type == "punct" and next_token.text == "." then
            return nil
        end

        if not can_start_expression_after_path_token(previous_token) and is_clause_boundary_path_token(next_token) then
            return nil
        end

        local identifier = token.identifier
        if string.find(identifier, "[", 1, true) or string.find(identifier, ".", 1, true) then
            return identifier
        end
        return nil
    end

    local function read_identifier_parts_from_path_tokens(tokens, index)
        local token, token_index = next_significant_path_token(tokens, index)
        if token == nil then
            return nil, nil
        end

        local parts = nil
        if token.type == "word" then
            parts = {token.text}
        elseif token.type == "quoted_identifier" then
            parts = {token.identifier}
        else
            return nil, nil
        end

        local current_index = token_index
        while true do
            local dot_token, dot_index = next_significant_path_token(tokens, current_index + 1)
            if dot_token == nil or dot_token.type ~= "punct" or dot_token.text ~= "." then
                break
            end

            local next_token, next_index = next_significant_path_token(tokens, dot_index + 1)
            if next_token == nil then
                break
            end
            if next_token.type == "word" then
                parts[#parts + 1] = next_token.text
            elseif next_token.type == "quoted_identifier" then
                parts[#parts + 1] = next_token.identifier
            else
                break
            end
            current_index = next_index
        end

        return parts, current_index
    end

    local function read_base_table_reference_from_path_tokens(tokens)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 and normalize_path_token(token) == "FROM" then
                local parts = nil
                parts, index = read_identifier_parts_from_path_tokens(tokens, index + 1)
                if parts == nil then
                    return nil
                end
                return {
                    schema_name = (#parts >= 2) and parts[#parts - 1] or nil,
                    table_name = parts[#parts]
                }
            end
            index = index + 1
        end
        return nil
    end

    local function path_tokens_have_top_level_join(tokens)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 and normalize_path_token(token) == "JOIN" then
                return true
            end
            index = index + 1
        end
        return false
    end
"""


JOIN_MODE_LUA = """
    local PATH_PLACEHOLDER_PREFIX = "__JVS_PATH_REF_"

    local function raise_path_error(path, message)
        error('JVS-PATH-ERROR: "' .. path .. '": ' .. message, 0)
    end

    local function encode_path_component(name)
        local out = {}
        for i = 1, string.len(name) do
            local byte = string.byte(name, i)
            local ch = string.char(byte)
            if string.match(ch, "[%w_]") or ch == "-" then
                out[#out + 1] = ch
            else
                out[#out + 1] = string.format("%%%02X", byte)
            end
        end
        return table.concat(out)
    end

    local function read_table_reference(tokens)
        return read_base_table_reference(tokens)
    end

    local function qualify_table_name(base_table, child_table_name)
        local out = {}
        if base_table.catalog_name ~= nil then
            out[#out + 1] = encode_quoted_identifier(base_table.catalog_name)
        end
        if base_table.schema_name ~= nil then
            out[#out + 1] = encode_quoted_identifier(base_table.schema_name)
        end
        out[#out + 1] = encode_quoted_identifier(child_table_name)
        return table.concat(out, ".")
    end

    local function derive_child_table_name(parent_table_name, segment)
        return parent_table_name .. "_" .. encode_path_component(segment)
    end

    local function derive_array_child_table_name(parent_table_name, segment)
        return parent_table_name .. "_" .. encode_path_component(segment) .. "_arr"
    end

    local function trim_selector_text(value)
        return string.gsub(string.gsub(value, "^%s+", ""), "%s+$", "")
    end

    local function parse_array_selector(selector_text)
        local trimmed = trim_selector_text(selector_text)
        if string.match(trimmed, "^%d+$") then
            return {
                kind = "index",
                index = tonumber(trimmed)
            }, nil
        end
        if trimmed == "" then
            return nil, "Empty array selector is not allowed."
        end
        if trimmed == "*" then
            return nil, 'Wildcard selectors are not supported yet. Use JOIN ... IN row."path" for full array traversal.'
        end
        if string.find(trimmed, ":", 1, true) ~= nil then
            return nil, 'Array slices are not supported yet. Use JOIN ... IN row."path" and filter on _index instead.'
        end
        if string.sub(trimmed, 1, 1) == "-" then
            return nil, 'Negative array indexes are not supported yet. Use LAST for the final element or JOIN ... IN row."path".'
        end
        local normalized = string.upper(trimmed)
        if normalized == "FIRST" then
            return {kind = "first"}, nil
        elseif normalized == "LAST" then
            return {kind = "last"}, nil
        elseif normalized == "SIZE" then
            return {kind = "size"}, nil
        end
        return nil, 'Unsupported array selector "' .. trimmed .. '". Supported selectors are numeric indexes, FIRST, LAST, and SIZE.'
    end

    local function serialize_array_selector(selector)
        if selector.kind == "index" then
            return tostring(selector.index)
        end
        return string.upper(selector.kind)
    end

    local function build_array_selector_sql(parent_ref, step)
        if step.selector.kind == "index" then
            return tostring(step.selector.index)
        elseif step.selector.kind == "first" then
            return "0"
        elseif step.selector.kind == "last" then
            return "(" .. parent_ref .. "." .. encode_quoted_identifier(step.name .. "|array") .. " - 1)"
        end
        return nil
    end

    local function parse_path_steps(path)
        local steps = {}
        local index = 1
        while index <= #path do
            local next_special = index
            while next_special <= #path do
                local ch = string.sub(path, next_special, next_special)
                if ch == "." or ch == "[" then
                    break
                end
                next_special = next_special + 1
            end
            if next_special == index then
                local current = string.sub(path, index, index)
                if current == "." then
                    return nil, "Empty path segment is not allowed."
                elseif current == "[" then
                    return nil, "An array selector must follow a property name."
                end
                return nil, "Invalid path syntax."
            end
            local segment = string.sub(path, index, next_special - 1)
            local special = string.sub(path, next_special, next_special)
            if special == "[" then
                local closing = string.find(path, "]", next_special + 1, true)
                if closing == nil then
                    return nil, "Missing closing ] in array selector."
                end
                local selector, selector_error = parse_array_selector(string.sub(path, next_special + 1, closing - 1))
                if selector == nil then
                    return nil, selector_error
                end
                steps[#steps + 1] = {
                    type = "array",
                    name = segment,
                    selector = selector
                }
                if closing < #path then
                    local next_char = string.sub(path, closing + 1, closing + 1)
                    if next_char == "[" then
                        return nil, 'Chained array indexing is not supported. Use JOIN ... IN row."path" to iterate nested arrays.'
                    end
                    if next_char ~= "." then
                        return nil, "Expected '.' after array selector."
                    end
                    index = closing + 2
                else
                    index = closing + 1
                end
            else
                steps[#steps + 1] = {
                    type = "property",
                    name = segment
                }
                if special == "." then
                    if next_special == #path then
                        return nil, "Path cannot end with '.'."
                    end
                    index = next_special + 1
                else
                    index = next_special
                end
            end
        end
        return steps, nil
    end

    local function serialize_step(step)
        if step.type == "array" then
            return step.name .. "[" .. serialize_array_selector(step.selector) .. "]"
        end
        return step.name
    end

    local function collect_path_references(tokens)
        local out = {}
        local path_references = {}
        local index = 1
        while index <= #tokens do
            local identifier = read_path_identifier(tokens, index)
            if identifier == nil then
                out[#out + 1] = tokens[index].text
                index = index + 1
            else
                local placeholder_name = PATH_PLACEHOLDER_PREFIX .. tostring(#path_references + 1) .. "__"
                local placeholder = encode_quoted_identifier(placeholder_name)
                path_references[#path_references + 1] = {
                    placeholder = placeholder,
                    path = identifier
                }
                out[#out + 1] = placeholder
                index = index + 1
            end
        end
        return table.concat(out), path_references
    end

    local function rewrite_path_identifiers_in_sql(sqltext)
        local original_tokens = tokenize_path_sql(sqltext)
        if not is_path_query_statement(original_tokens) then
            return sqltext
        end

        local placeholder_sql, path_references = collect_path_references(original_tokens)
        if #path_references == 0 then
            return placeholder_sql
        end

        local tokens = sqlparsing.tokenize(placeholder_sql)
        local table_references = collect_top_level_table_references(tokens)
        local allowed_references = collect_allowed_virtual_table_references(table_references)
        if #allowed_references == 0 then
            raise_scope_error(
                "JSON path syntax",
                'Qualify the JSON virtual-schema table in FROM/JOIN, for example FROM "JSON_VS"."SAMPLE".'
            )
        end
        local base_table = read_table_reference(tokens)
        if base_table == nil then
            error("JVS-PATH-ERROR: Path rewrite currently requires a query with a single base table in FROM.", 0)
        elseif not table_reference_is_allowed_virtual_schema(base_table) then
            raise_scope_error(
                "JSON path syntax",
                'Path rewriting currently requires the base table in FROM to be one of the configured JSON virtual schemas.'
            )
        end

        local root_ref = render_bound_identifier(base_table.alias_name or base_table.table_name)
        local join_aliases = {}
        local join_sql_parts = {}
        local replacements = {}
        local next_alias_id = 1

        for _, reference in ipairs(path_references) do
            local steps, parse_error = parse_path_steps(reference.path)
            if steps == nil or #steps == 0 then
                raise_path_error(reference.path, parse_error or "Invalid path syntax.")
            end
            local current_ref = root_ref
            local current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
            local current_table_name = base_table.table_name
            local prefix = {}
            local replacement = nil
            for step_index, step in ipairs(steps) do
                local is_last = step_index == #steps
                prefix[#prefix + 1] = serialize_step(step)
                local prefix_key = table.concat(prefix, ".")
                if step.type == "property" then
                    if is_last then
                        replacement = current_ref .. "." .. encode_quoted_identifier(step.name)
                    else
                        local existing = join_aliases[prefix_key]
                        local child_table_name = derive_child_table_name(current_table_name, step.name)
                        if existing == nil then
                            local alias_name = "__jvs_path_" .. tostring(next_alias_id)
                            next_alias_id = next_alias_id + 1
                            local alias_ref = encode_quoted_identifier(alias_name)
                            existing = {
                                alias_ref = alias_ref,
                                table_name = child_table_name
                            }
                            join_aliases[prefix_key] = existing
                            join_sql_parts[#join_sql_parts + 1] = " LEFT OUTER JOIN "
                                    .. qualify_table_name(base_table, child_table_name)
                                    .. " " .. alias_ref
                                    .. " ON (" .. current_ref .. "." .. encode_quoted_identifier(step.name .. "|object")
                                    .. " = " .. alias_ref .. "." .. encode_quoted_identifier("_id") .. ")"
                        end
                        current_ref = existing.alias_ref
                        current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
                        current_table_name = existing.table_name
                    end
                elseif step.type == "array" then
                    if step.selector.kind == "size" then
                        if not is_last then
                            raise_path_error(reference.path, "SIZE must be the last selector in a path.")
                        end
                        replacement = current_ref .. "." .. encode_quoted_identifier(step.name .. "|array")
                    else
                        local existing = join_aliases[prefix_key]
                        local child_table_name = derive_array_child_table_name(current_table_name, step.name)
                        local alias_name = "__jvs_path_" .. tostring(next_alias_id)
                        local selector_sql = build_array_selector_sql(current_ref, step)
                        if selector_sql == nil then
                            raise_path_error(reference.path, "Unsupported array selector.")
                        end
                        if existing == nil then
                            next_alias_id = next_alias_id + 1
                            local alias_ref = encode_quoted_identifier(alias_name)
                            existing = {
                                alias_ref = alias_ref,
                                table_name = child_table_name
                            }
                            join_aliases[prefix_key] = existing
                            join_sql_parts[#join_sql_parts + 1] = " LEFT OUTER JOIN "
                                    .. qualify_table_name(base_table, child_table_name)
                                    .. " " .. alias_ref
                                    .. " ON (" .. current_row_id .. " = " .. alias_ref .. "."
                                    .. encode_quoted_identifier("_parent")
                                    .. " AND " .. alias_ref .. "." .. encode_quoted_identifier("_pos")
                                    .. " = " .. selector_sql .. ")"
                        end
                        current_ref = existing.alias_ref
                        current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
                        current_table_name = existing.table_name
                        if is_last then
                            replacement = current_ref .. "." .. encode_quoted_identifier("_value")
                        end
                    end
                end
            end
            if replacement == nil then
                raise_path_error(reference.path, "Unable to rewrite the path expression.")
            end
            replacements[reference.placeholder] = replacement
        end

        local out = {}
        for index, token in ipairs(tokens) do
            out[#out + 1] = replacements[token] or token
            if index == base_table.insert_after_index and #join_sql_parts > 0 then
                out[#out + 1] = table.concat(join_sql_parts)
            end
        end
        return table.concat(out)
    end
"""


ARRAY_ITERATION_LUA = """
    local next_iterator_alias_id = 1

    local QUERY_BOUNDARY_KEYWORDS = {
        WHERE = true, GROUP = true, HAVING = true, QUALIFY = true, ORDER = true,
        LIMIT = true, OFFSET = true, UNION = true, EXCEPT = true, MINUS = true
    }

    local function raise_iterator_error(message)
        error("JVS-ITER-ERROR: " .. message, 0)
    end

    local function copy_scope(scope)
        local out = {}
        for key, value in pairs(scope or {}) do
            out[key] = value
        end
        return out
    end

    local function scope_key(name)
        return normalize_identifier_value(name)
    end

    local function render_bound_identifier(name)
        return encode_quoted_identifier(normalize_identifier_value(name))
    end

    local function bind_scope(scope, binding)
        scope[scope_key(binding.alias_name)] = binding
    end

    local function lookup_scope(scope, alias_name)
        return scope[scope_key(alias_name)]
    end

    local function slice_path_tokens_text(tokens, start_index, end_index)
        local out = {}
        for index = start_index, end_index do
            out[#out + 1] = tokens[index].text
        end
        return table.concat(out)
    end

    local function find_matching_path_paren(tokens, opening_index)
        local depth = 1
        local index = opening_index + 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
                if depth == 0 then
                    return index
                end
            end
            index = index + 1
        end
        return nil
    end

    local function path_token_is_query_start(token)
        if token == nil then
            return false
        end
        local normalized = normalize_path_token(token)
        return normalized ~= nil and QUERY_START_KEYWORDS[normalized] == true
    end

    local function path_token_is_query_boundary(token)
        if token == nil then
            return false
        end
        local normalized = normalize_path_token(token)
        return normalized ~= nil and QUERY_BOUNDARY_KEYWORDS[normalized] == true
    end

    local function path_token_is_source_boundary(token)
        if token == nil then
            return true
        end
        if token.type == "punct" then
            return token.text == "," or token.text == ")"
        end
        local normalized = normalize_path_token(token)
        return normalized ~= nil and (
                normalized == "ON" or normalized == "USING" or QUERY_BOUNDARY_KEYWORDS[normalized] == true
                or normalized == "JOIN" or normalized == "LEFT" or normalized == "RIGHT"
                or normalized == "FULL" or normalized == "INNER" or normalized == "CROSS"
        )
    end

    local function read_single_identifier_from_path_tokens(tokens, index)
        local token, token_index = next_significant_path_token(tokens, index)
        if token == nil then
            return nil, nil
        end
        if token.type == "word" then
            return token.text, token_index
        end
        if token.type == "quoted_identifier" then
            return token.identifier, token_index
        end
        return nil, nil
    end

    local function read_single_identifier_parts_from_path_tokens(tokens, index)
        local parts, end_index = read_identifier_parts_from_path_tokens(tokens, index)
        if parts == nil or #parts ~= 1 then
            return nil, nil
        end
        return parts[1], end_index
    end

    local function read_alias_after_source_path_tokens(tokens, source_end_index)
        local alias_name = nil
        local alias_end_index = source_end_index
        local maybe_alias, maybe_alias_index = next_significant_path_token(tokens, source_end_index + 1)
        if maybe_alias == nil then
            return nil, alias_end_index
        end
        if normalize_path_token(maybe_alias) == "AS" then
            local alias_token, alias_index = next_significant_path_token(tokens, maybe_alias_index + 1)
            if alias_token ~= nil then
                local parsed_alias_name = nil
                parsed_alias_name, _ = read_single_identifier_parts_from_path_tokens(tokens, alias_index)
                if parsed_alias_name ~= nil then
                    alias_name = parsed_alias_name
                    alias_end_index = alias_index
                end
            end
        elseif not path_token_is_source_boundary(maybe_alias) then
            local parsed_alias_name = nil
            parsed_alias_name, _ = read_single_identifier_parts_from_path_tokens(tokens, maybe_alias_index)
            if parsed_alias_name ~= nil then
                alias_name = parsed_alias_name
                alias_end_index = maybe_alias_index
            end
        end
        return alias_name, alias_end_index
    end

    local function read_standard_source_binding(tokens, source_start_index)
        local source_token, source_index = next_significant_path_token(tokens, source_start_index)
        if source_token == nil then
            return nil, nil
        end
        if source_token.type == "punct" and source_token.text == "(" then
            local closing_index = find_matching_path_paren(tokens, source_index)
            if closing_index == nil then
                return nil, nil
            end
            local alias_name, alias_end_index = read_alias_after_source_path_tokens(tokens, closing_index)
            if alias_name ~= nil then
                return {
                    alias_name = alias_name,
                    reference_sql = render_bound_identifier(alias_name),
                    kind = "derived_source",
                    table_name = nil,
                    schema_name = nil,
                    catalog_name = nil,
                    has_row_id = false
                }, alias_end_index
            end
            return nil, closing_index
        end
        local parts, table_end_index = read_identifier_parts_from_path_tokens(tokens, source_start_index)
        if parts == nil then
            return nil, nil
        end
        local alias_name, alias_end_index = read_alias_after_source_path_tokens(tokens, table_end_index)
        local resolved_alias_name = alias_name or parts[#parts]
        local binding = {
            alias_name = resolved_alias_name,
            reference_sql = render_bound_identifier(resolved_alias_name),
            kind = "other_source",
            table_name = parts[#parts],
            schema_name = (#parts >= 2) and parts[#parts - 1] or nil,
            catalog_name = (#parts >= 3) and parts[#parts - 2] or nil,
            has_row_id = true
        }
        if table_reference_is_allowed_virtual_schema({
            schema_name = binding.schema_name
        }) then
            binding.kind = "json_source"
        end
        return binding, alias_end_index
    end

    local function read_iterator_path_source(tokens, source_start_index)
        local iterator_token, iterator_index = next_significant_path_token(tokens, source_start_index)
        if iterator_token == nil then
            return nil
        end

        local is_value = false
        if normalize_path_token(iterator_token) == "VALUE" then
            is_value = true
            iterator_token, iterator_index = next_significant_path_token(tokens, iterator_index + 1)
            if iterator_token == nil then
                raise_iterator_error('Expected an iterator alias after VALUE.')
            end
        end

        local alias_name = nil
        alias_name, iterator_index = read_single_identifier_from_path_tokens(tokens, iterator_index)
        if alias_name == nil then
            return nil
        end

        local in_token, in_index = next_significant_path_token(tokens, iterator_index + 1)
        if in_token == nil or normalize_path_token(in_token) ~= "IN" then
            return nil
        end

        local root_alias_name = nil
        root_alias_name, in_index = read_single_identifier_from_path_tokens(tokens, in_index + 1)
        if root_alias_name == nil then
            raise_iterator_error('Expected an iterator path of the form row_alias."path.to.array".')
        end

        local dot_token, dot_index = next_significant_path_token(tokens, in_index + 1)
        if dot_token == nil or dot_token.type ~= "punct" or dot_token.text ~= "." then
            raise_iterator_error('Expected an iterator path of the form row_alias."path.to.array".')
        end

        local path_token, path_index = next_significant_path_token(tokens, dot_index + 1)
        if path_token == nil or path_token.type ~= "quoted_identifier" then
            raise_iterator_error('Expected an iterator path of the form row_alias."path.to.array".')
        end

        return {
            is_value = is_value,
            alias_name = alias_name,
            root_alias_name = root_alias_name,
            path = path_token.identifier,
            end_index = path_index
        }
    end

    local function encode_path_component_for_iterator(name)
        local out = {}
        for i = 1, string.len(name) do
            local byte = string.byte(name, i)
            local ch = string.char(byte)
            if string.match(ch, "[%w_]") or ch == "-" then
                out[#out + 1] = ch
            else
                out[#out + 1] = string.format("%%%02X", byte)
            end
        end
        return table.concat(out)
    end

    local function derive_child_table_name_for_iterator(parent_table_name, segment)
        return parent_table_name .. "_" .. encode_path_component_for_iterator(segment)
    end

    local function derive_array_child_table_name_for_iterator(parent_table_name, segment)
        return parent_table_name .. "_" .. encode_path_component_for_iterator(segment) .. "_arr"
    end

    local function qualify_table_name_for_iterator(binding, table_name)
        local out = {}
        if binding.catalog_name ~= nil then
            out[#out + 1] = encode_quoted_identifier(binding.catalog_name)
        end
        if binding.schema_name ~= nil then
            out[#out + 1] = encode_quoted_identifier(binding.schema_name)
        end
        out[#out + 1] = encode_quoted_identifier(table_name)
        return table.concat(out, ".")
    end

    local function trim_selector_text_for_iterator(value)
        return string.gsub(string.gsub(value, "^%s+", ""), "%s+$", "")
    end

    local function parse_iterator_array_path(path)
        local steps = {}
        local index = 1
        while index <= #path do
            local next_special = index
            while next_special <= #path do
                local ch = string.sub(path, next_special, next_special)
                if ch == "." or ch == "[" then
                    break
                end
                next_special = next_special + 1
            end
            if next_special == index then
                local current = string.sub(path, index, index)
                if current == "." then
                    return nil, nil, "Empty path segment is not allowed."
                elseif current == "[" then
                    return nil, nil, "An array iterator path must target an array property, not an indexed element."
                end
                return nil, nil, "Invalid iterator path syntax."
            end
            local segment = string.sub(path, index, next_special - 1)
            local special = string.sub(path, next_special, next_special)
            if special == "[" then
                local closing = string.find(path, "]", next_special + 1, true)
                if closing == nil then
                    return nil, nil, "Missing closing ] in iterator path."
                end
                local selector = trim_selector_text_for_iterator(string.sub(path, next_special + 1, closing - 1))
                return nil, nil,
                        'Iterator paths must name an array property directly. Use scalar bracket access for one element'
                        .. ' or JOIN ... IN row."path" for full traversal. Invalid selector [' .. selector .. '].'
            end
            steps[#steps + 1] = segment
            if special == "." then
                if next_special == #path then
                    return nil, nil, "Iterator path cannot end with '.'."
                end
                index = next_special + 1
            else
                index = next_special
            end
        end
        if #steps == 0 then
            return nil, nil, "Iterator path cannot be empty."
        end
        local object_steps = {}
        for step_index = 1, #steps - 1 do
            object_steps[#object_steps + 1] = steps[step_index]
        end
        return object_steps, steps[#steps], nil
    end

    local function build_iterator_relation_sql(qualified_table_name, alias_name, is_value)
        local relation_alias = render_bound_identifier(alias_name)
        local inner_alias_name = "__jvs_iter_src"
        local inner_alias = encode_quoted_identifier(inner_alias_name)
        if is_value then
            return "(SELECT " .. inner_alias .. ".*, "
                    .. inner_alias .. "." .. encode_quoted_identifier("_pos")
                    .. " AS " .. encode_quoted_identifier("_index") .. ", "
                    .. inner_alias .. "." .. encode_quoted_identifier("_value")
                    .. " AS " .. render_bound_identifier(alias_name)
                    .. " FROM " .. qualified_table_name .. " " .. inner_alias .. ") " .. relation_alias
        end
        return "(SELECT " .. inner_alias .. ".*, "
                .. inner_alias .. "." .. encode_quoted_identifier("_pos")
                .. " AS " .. encode_quoted_identifier("_index")
                .. " FROM " .. qualified_table_name .. " " .. inner_alias .. ") " .. relation_alias
    end

    local function new_generated_iterator_alias()
        local alias_name = "__jvs_iter_path_" .. tostring(next_iterator_alias_id)
        next_iterator_alias_id = next_iterator_alias_id + 1
        return alias_name, encode_quoted_identifier(alias_name)
    end

    local function build_iterator_binding(iterator_source, root_binding, array_child_table_name)
        return {
            alias_name = iterator_source.alias_name,
            reference_sql = render_bound_identifier(iterator_source.alias_name),
            kind = iterator_source.is_value and "iterator_value" or "iterator_row",
            table_name = array_child_table_name,
            schema_name = root_binding.schema_name,
            catalog_name = root_binding.catalog_name,
            has_row_id = not iterator_source.is_value
        }
    end

    local function build_iterator_join_clause(iterator_source, root_binding, join_kind)
        if root_binding == nil then
            raise_scope_error(
                "JSON array iteration syntax",
                'Qualify the JSON virtual-schema table in FROM/JOIN, for example FROM "JSON_VS"."SAMPLE" s.'
            )
        end
        if root_binding.kind ~= "json_source" and root_binding.kind ~= "iterator_row" then
            if root_binding.kind == "iterator_value" then
                raise_iterator_error('Scalar VALUE iterators cannot be used as the root of another iterator path.')
            end
            raise_scope_error(
                "JSON array iteration syntax",
                'Iterator roots must come from a configured JSON virtual schema or from an object-array iterator.'
            )
        end
        if root_binding.has_row_id ~= true then
            raise_iterator_error('This iterator root does not expose row identity, so nested array traversal is not supported.')
        end

        local object_steps, array_name, path_error = parse_iterator_array_path(iterator_source.path)
        if object_steps == nil then
            raise_iterator_error(path_error)
        end

        local current_ref = root_binding.reference_sql
        local current_table_name = root_binding.table_name
        local current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
        local out = {}
        for _, step_name in ipairs(object_steps) do
            local child_table_name = derive_child_table_name_for_iterator(current_table_name, step_name)
            local generated_alias_name, generated_alias = new_generated_iterator_alias()
            out[#out + 1] = " LEFT OUTER JOIN "
                    .. qualify_table_name_for_iterator(root_binding, child_table_name)
                    .. " " .. generated_alias
                    .. " ON (" .. current_ref .. "." .. encode_quoted_identifier(step_name .. "|object")
                    .. " = " .. generated_alias .. "." .. encode_quoted_identifier("_id") .. ")"
            current_ref = generated_alias
            current_table_name = child_table_name
            current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
        end

        local array_child_table_name = derive_array_child_table_name_for_iterator(current_table_name, array_name)
        local join_keyword = (join_kind == "left_join") and " LEFT OUTER JOIN " or " INNER JOIN "
        out[#out + 1] = join_keyword
                .. build_iterator_relation_sql(
                        qualify_table_name_for_iterator(root_binding, array_child_table_name),
                        iterator_source.alias_name,
                        iterator_source.is_value
                )
                .. " ON (" .. current_row_id .. " = " .. render_bound_identifier(iterator_source.alias_name)
                .. "." .. encode_quoted_identifier("_parent") .. ")"

        return table.concat(out), build_iterator_binding(iterator_source, root_binding, array_child_table_name), nil
    end

    local function build_iterator_from_clause(iterator_source, root_binding)
        if root_binding == nil then
            raise_scope_error(
                "JSON array iteration syntax",
                'Qualify the JSON virtual-schema table in FROM/JOIN, for example FROM "JSON_VS"."SAMPLE" s.'
            )
        end
        if root_binding.kind ~= "json_source" and root_binding.kind ~= "iterator_row" then
            if root_binding.kind == "iterator_value" then
                raise_iterator_error('Scalar VALUE iterators cannot be used as the root of another iterator path.')
            end
            raise_scope_error(
                "JSON array iteration syntax",
                'Iterator roots must come from a configured JSON virtual schema or from an object-array iterator.'
            )
        end
        if root_binding.has_row_id ~= true then
            raise_iterator_error('This iterator root does not expose row identity, so nested array traversal is not supported.')
        end

        local object_steps, array_name, path_error = parse_iterator_array_path(iterator_source.path)
        if object_steps == nil then
            raise_iterator_error(path_error)
        end

        local current_ref = root_binding.reference_sql
        local current_table_name = root_binding.table_name
        local current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
        local out = {}
        local correlation_filter_sql = nil
        if #object_steps == 0 then
            local array_child_table_name = derive_array_child_table_name_for_iterator(current_table_name, array_name)
            out[#out + 1] = "FROM "
                    .. build_iterator_relation_sql(
                            qualify_table_name_for_iterator(root_binding, array_child_table_name),
                            iterator_source.alias_name,
                            iterator_source.is_value
                    )
            correlation_filter_sql = "(" .. current_row_id .. " = " .. render_bound_identifier(iterator_source.alias_name)
                    .. "." .. encode_quoted_identifier("_parent") .. ")"
            return table.concat(out), build_iterator_binding(iterator_source, root_binding, array_child_table_name),
                    correlation_filter_sql
        end

        local first_step_name = object_steps[1]
        local first_child_table_name = derive_child_table_name_for_iterator(current_table_name, first_step_name)
        local first_alias_name, first_alias = new_generated_iterator_alias()
        out[#out + 1] = "FROM " .. qualify_table_name_for_iterator(root_binding, first_child_table_name)
                .. " " .. first_alias
        correlation_filter_sql = "(" .. current_ref .. "." .. encode_quoted_identifier(first_step_name .. "|object")
                .. " = " .. first_alias .. "." .. encode_quoted_identifier("_id") .. ")"
        current_ref = first_alias
        current_table_name = first_child_table_name
        current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")

        for step_index = 2, #object_steps do
            local step_name = object_steps[step_index]
            local child_table_name = derive_child_table_name_for_iterator(current_table_name, step_name)
            local generated_alias_name, generated_alias = new_generated_iterator_alias()
            out[#out + 1] = " INNER JOIN "
                    .. qualify_table_name_for_iterator(root_binding, child_table_name)
                    .. " " .. generated_alias
                    .. " ON (" .. current_ref .. "." .. encode_quoted_identifier(step_name .. "|object")
                    .. " = " .. generated_alias .. "." .. encode_quoted_identifier("_id") .. ")"
            current_ref = generated_alias
            current_table_name = child_table_name
            current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
        end

        local array_child_table_name = derive_array_child_table_name_for_iterator(current_table_name, array_name)
        out[#out + 1] = " INNER JOIN "
                .. build_iterator_relation_sql(
                        qualify_table_name_for_iterator(root_binding, array_child_table_name),
                        iterator_source.alias_name,
                        iterator_source.is_value
                )
                .. " ON (" .. current_row_id .. " = " .. render_bound_identifier(iterator_source.alias_name)
                .. "." .. encode_quoted_identifier("_parent") .. ")"
        return table.concat(out), build_iterator_binding(iterator_source, root_binding, array_child_table_name),
                correlation_filter_sql
    end

    local function read_join_prefix_at(tokens, index)
        local token = tokens[index]
        local normalized = normalize_path_token(token)
        if normalized == "FROM" then
            return {kind = "from", end_index = index}
        elseif normalized == "JOIN" then
            return {kind = "inner_join", end_index = index}
        elseif normalized == "INNER" then
            local join_token, join_index = next_significant_path_token(tokens, index + 1)
            if join_token ~= nil and normalize_path_token(join_token) == "JOIN" then
                return {kind = "inner_join", end_index = join_index}
            end
        elseif normalized == "LEFT" then
            local next_token, next_index = next_significant_path_token(tokens, index + 1)
            if next_token ~= nil and normalize_path_token(next_token) == "JOIN" then
                return {kind = "left_join", end_index = next_index}
            elseif next_token ~= nil and normalize_path_token(next_token) == "OUTER" then
                local join_token, join_index = next_significant_path_token(tokens, next_index + 1)
                if join_token ~= nil and normalize_path_token(join_token) == "JOIN" then
                    return {kind = "left_join", end_index = join_index}
                end
            end
        end
        return nil
    end

    local function render_pending_where(pending_filters)
        return table.concat(pending_filters, " AND ")
    end

    local function rewrite_iterator_index_references_in_sql(sqltext, scope)
        local tokens = tokenize_path_sql(sqltext)
        local out = {}
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            local identifier_name = nil
            if token.type == "quoted_identifier" then
                identifier_name = token.identifier
            elseif token.type == "word" then
                identifier_name = token.text
            end
            local binding = identifier_name and lookup_scope(scope, identifier_name) or nil
            local dot_token, dot_index = nil, nil
            local member_token, member_index = nil, nil
            if binding ~= nil then
                dot_token, dot_index = next_significant_path_token(tokens, index + 1)
                if dot_token ~= nil then
                    member_token, member_index = next_significant_path_token(tokens, dot_index + 1)
                end
            end
            local member_name = nil
            if member_token ~= nil and member_token.type == "quoted_identifier" then
                member_name = member_token.identifier
            elseif member_token ~= nil and member_token.type == "word" then
                member_name = member_token.text
            end
            if binding ~= nil and (binding.kind == "iterator_row" or binding.kind == "iterator_value")
                    and dot_token ~= nil and dot_token.type == "punct" and dot_token.text == "."
                    and member_token ~= nil and member_token.type == "word" then
                out[#out + 1] = token.text
                out[#out + 1] = "."
                out[#out + 1] = encode_quoted_identifier(member_name)
                index = member_index + 1
            else
                out[#out + 1] = token.text
                index = index + 1
            end
        end
        return table.concat(out)
    end

    local function rewrite_query_block_tokens(tokens, outer_scope)
        local scope = copy_scope(outer_scope or {})
        local out = {}
        local pending_filters = {}
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if depth == 0 and #pending_filters > 0 then
                if normalize_path_token(token) == "WHERE" then
                    out[#out + 1] = "WHERE (" .. render_pending_where(pending_filters) .. ") AND "
                    pending_filters = {}
                    index = index + 1
                    goto continue
                elseif path_token_is_query_boundary(token) then
                    out[#out + 1] = " WHERE " .. render_pending_where(pending_filters) .. " "
                    pending_filters = {}
                end
            end

            if token.type == "punct" and token.text == "(" then
                local closing_index = find_matching_path_paren(tokens, index)
                local first_inside, first_inside_index = next_significant_path_token(tokens, index + 1)
                if closing_index ~= nil and first_inside_index ~= nil and path_token_is_query_start(first_inside) then
                    out[#out + 1] = "("
                    out[#out + 1] = rewrite_query_block_tokens(
                            {table.unpack(tokens, index + 1, closing_index - 1)},
                            scope
                    )
                    out[#out + 1] = ")"
                    index = closing_index + 1
                else
                    depth = depth + 1
                    out[#out + 1] = token.text
                    index = index + 1
                end
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
                out[#out + 1] = token.text
                index = index + 1
            elseif depth == 0 then
                local prefix = read_join_prefix_at(tokens, index)
                if prefix ~= nil then
                    local source_token, source_start_index = next_significant_path_token(tokens, prefix.end_index + 1)
                    local iterator_source = source_start_index and read_iterator_path_source(tokens, source_start_index) or nil
                    if iterator_source ~= nil then
                        local root_binding = lookup_scope(scope, iterator_source.root_alias_name)
                        local rewritten_clause_sql = nil
                        local iterator_binding = nil
                        local correlation_filter_sql = nil
                        if prefix.kind == "from" then
                            rewritten_clause_sql, iterator_binding, correlation_filter_sql =
                                    build_iterator_from_clause(iterator_source, root_binding)
                            if correlation_filter_sql ~= nil then
                                pending_filters[#pending_filters + 1] = correlation_filter_sql
                            end
                        else
                            rewritten_clause_sql, iterator_binding = build_iterator_join_clause(
                                    iterator_source,
                                    root_binding,
                                    prefix.kind
                            )
                        end
                        out[#out + 1] = rewritten_clause_sql
                        bind_scope(scope, iterator_binding)
                        index = iterator_source.end_index + 1
                    else
                        local standard_binding, _ = read_standard_source_binding(tokens, source_start_index)
                        if standard_binding ~= nil and standard_binding.alias_name ~= nil then
                            bind_scope(scope, standard_binding)
                        end
                        out[#out + 1] = token.text
                        index = index + 1
                    end
                else
                    out[#out + 1] = token.text
                    index = index + 1
                end
            elseif token.type == "word" or token.type == "quoted_identifier" then
                local identifier_name = token.type == "quoted_identifier" and token.identifier or token.text
                local binding = lookup_scope(scope, identifier_name)
                local dot_token, dot_index = next_significant_path_token(tokens, index + 1)
                local member_token, member_index = nil, nil
                if dot_token ~= nil then
                    member_token, member_index = next_significant_path_token(tokens, dot_index + 1)
                end
                local member_name = nil
                if member_token ~= nil and member_token.type == "quoted_identifier" then
                    member_name = member_token.identifier
                elseif member_token ~= nil and member_token.type == "word" then
                    member_name = member_token.text
                end
                if binding ~= nil and (binding.kind == "iterator_row" or binding.kind == "iterator_value")
                        and dot_token ~= nil and dot_token.type == "punct" and dot_token.text == "."
                        and normalize_identifier_value(member_name) == "_INDEX" then
                    out[#out + 1] = token.text
                    out[#out + 1] = "."
                    out[#out + 1] = encode_quoted_identifier("_index")
                    index = member_index + 1
                else
                    out[#out + 1] = token.text
                    index = index + 1
                end
            else
                out[#out + 1] = token.text
                index = index + 1
            end
            ::continue::
        end

        if #pending_filters > 0 then
            out[#out + 1] = " WHERE " .. render_pending_where(pending_filters)
        end
        return rewrite_iterator_index_references_in_sql(table.concat(out), scope)
    end

    local function rewrite_array_iteration_in_sql(sqltext)
        local tokens = tokenize_path_sql(sqltext)
        local first_token = next_significant_path_token(tokens, 1)
        if first_token == nil or not path_token_is_query_start(first_token) then
            return sqltext
        end
        return rewrite_query_block_tokens(tokens, {})
    end
"""


DISABLED_MODE_LUA = """
    local function rewrite_path_identifiers_in_sql(sqltext)
        return sqltext
    end
"""


def render_sql(
    schema: str,
    script: str,
    function_names: list[str],
    virtual_schemas: list[str],
    rewrite_path_identifiers: bool,
    activate_session: bool,
) -> str:
    function_rows = "\n".join(f"        {name} = true," for name in function_names)
    function_set_lua = "{\n" + function_rows + "\n    }"
    function_list_sql = ", ".join(function_names)
    virtual_schema_rows = "\n".join(f"        {name} = true," for name in virtual_schemas)
    virtual_schema_set_lua = "{\n" + virtual_schema_rows + "\n    }"
    virtual_schema_list_sql = ", ".join(virtual_schemas)
    if not rewrite_path_identifiers:
        path_comment = "disabled"
        path_lua = DISABLED_MODE_LUA
    else:
        path_comment = "enabled (joins)"
        path_lua = JOIN_MODE_LUA

    activation_sql = ""
    if activate_session:
        activation_sql = f"\nALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema}.{script};"

    return f"""-- Generated by tools/generate_preprocessor_sql.py
-- Rewrites configured function calls to a CASE marker before virtual-schema optimization.
-- Configured function names: {function_list_sql}
-- JSON syntax allowed only for virtual schemas: {virtual_schema_list_sql}
-- Path identifier rewrite: {path_comment}

CREATE SCHEMA IF NOT EXISTS {schema};

CREATE OR REPLACE LUA PREPROCESSOR SCRIPT {schema}.{script} AS
    local TARGET_FUNCTIONS = {function_set_lua}
    local ALLOWED_VIRTUAL_SCHEMAS = {virtual_schema_set_lua}
    local ALLOWED_VIRTUAL_SCHEMA_LIST = {virtual_schema_list_sql!r}

    local function raise_function_error(function_name, message)
        error("JVS-FUNCTION-ERROR: " .. function_name .. ": " .. message, 0)
    end

    local function raise_scope_error(feature_name, message)
        error(
            "JVS-SCOPE-ERROR: " .. feature_name
                .. " is only available for configured JSON virtual schemas ("
                .. ALLOWED_VIRTUAL_SCHEMA_LIST .. "). "
                .. message,
            0
        )
    end

    local function normalize(token)
        return sqlparsing.normalize(token)
    end

    local function is_ignored(token)
        return sqlparsing.iswhitespaceorcomment(token)
    end
{COMMON_LUA}
{ARRAY_ITERATION_LUA}
{path_lua}

    local function next_significant(tokens, index)
        local current = index
        while current <= #tokens and is_ignored(tokens[current]) do
            current = current + 1
        end
        return current
    end

    local function read_call(tokens, start_index)
        local identifier_index = next_significant(tokens, start_index)
        if identifier_index > #tokens then
            return nil
        end

        local current = identifier_index
        local identifier_parts = parse_identifier_token(tokens[current])
        if identifier_parts == nil then
            return nil
        end

        local last_identifier = normalize(identifier_parts[#identifier_parts])
        if last_identifier == nil then
            return nil
        end

        while true do
            local dot_index = next_significant(tokens, current + 1)
            if dot_index > #tokens or tokens[dot_index] ~= "." then
                break
            end
            local next_identifier = next_significant(tokens, dot_index + 1)
            if next_identifier > #tokens then
                return nil
            end
            current = next_identifier
            local next_identifier_parts = parse_identifier_token(tokens[current])
            if next_identifier_parts == nil then
                return nil
            end
            last_identifier = normalize(next_identifier_parts[#next_identifier_parts])
            if last_identifier == nil then
                return nil
            end
        end

        local opening_paren = next_significant(tokens, current + 1)
        if opening_paren > #tokens or tokens[opening_paren] ~= "(" then
            return nil
        end

        return {{
            last_identifier = last_identifier,
            opening_paren = opening_paren
        }}
    end

    local function find_matching_paren(tokens, opening_paren)
        local depth = 1
        local top_level_commas = 0
        local index = opening_paren + 1
        while index <= #tokens do
            if not is_ignored(tokens[index]) then
                if tokens[index] == "(" then
                    depth = depth + 1
                elseif tokens[index] == ")" then
                    depth = depth - 1
                    if depth == 0 then
                        return index, top_level_commas
                    end
                elseif tokens[index] == "," and depth == 1 then
                    top_level_commas = top_level_commas + 1
                end
            end
            index = index + 1
        end
        return nil, nil
    end

    local function has_expression_argument(tokens, opening_paren, closing_paren)
        for index = opening_paren + 1, closing_paren - 1 do
            if not is_ignored(tokens[index]) then
                return true
            end
        end
        return false
    end

    local function helper_query_targets_allowed_virtual_schema(sqltext)
        local path_tokens = tokenize_path_sql(sqltext)
        local base_table = read_base_table_reference_from_path_tokens(path_tokens)
        if base_table == nil then
            return false, 'Qualify the JSON virtual-schema table in FROM/JOIN, for example FROM "JSON_VS"."SAMPLE".'
        end
        if not table_reference_is_allowed_virtual_schema(base_table) then
            return false, 'Qualify the JSON virtual-schema table in FROM/JOIN, for example FROM "JSON_VS"."SAMPLE".'
        end
        return true, nil
    end

    local function rewrite(sqltext)
        local rewritten_sql = rewrite_array_iteration_in_sql(sqltext)
        rewritten_sql = rewrite_path_identifiers_in_sql(rewritten_sql)
        local tokens = sqlparsing.tokenize(rewritten_sql)
        local out = {{}}
        local index = 1
        while index <= #tokens do
            local call = read_call(tokens, index)
            if call and TARGET_FUNCTIONS[call.last_identifier] then
                local closing_paren, top_level_commas = find_matching_paren(tokens, call.opening_paren)
                if closing_paren == nil then
                    raise_function_error(call.last_identifier, "Missing closing parenthesis.")
                elseif top_level_commas ~= 0 then
                    raise_function_error(call.last_identifier, "Expected exactly one argument.")
                elseif not has_expression_argument(tokens, call.opening_paren, closing_paren) then
                    raise_function_error(call.last_identifier, "Expected exactly one argument.")
                else
                    local helper_allowed, helper_scope_message = helper_query_targets_allowed_virtual_schema(sqltext)
                    if not helper_allowed then
                        raise_scope_error("JSON helper functions", helper_scope_message)
                    end
                    out[#out + 1] = "(CASE WHEN "
                    for argument_index = call.opening_paren + 1, closing_paren - 1 do
                        out[#out + 1] = tokens[argument_index]
                    end
                    out[#out + 1] = " IS NULL THEN TRUE ELSE FALSE END)"
                    index = closing_paren + 1
                end
            else
                out[#out + 1] = tokens[index]
                index = index + 1
            end
        end
        return table.concat(out)
    end

    sqlparsing.setsqltext(rewrite(sqlparsing.getsqltext()))
/
{activation_sql}

-- Enable explicitly with:
-- ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema}.{script};

-- Example:
-- SELECT
--   CAST("id" AS VARCHAR(10)),
--   CASE WHEN {function_names[0]}("note") THEN '1' ELSE '0' END
-- FROM JSON_VS.SAMPLE
-- ORDER BY "id";
"""


def main() -> None:
    args = parse_args()
    schema = validate_identifier("Schema", args.schema)
    script = validate_identifier("Script name", args.script)
    raw_function_names = args.function_names or ["JSON_IS_EXPLICIT_NULL"]
    function_names = [validate_identifier("Function name", value) for value in raw_function_names]
    raw_virtual_schemas = args.virtual_schemas or ["JSON_VS"]
    virtual_schemas = [validate_identifier("Virtual schema name", value) for value in raw_virtual_schemas]
    sql = render_sql(
        schema,
        script,
        function_names,
        virtual_schemas,
        args.rewrite_path_identifiers,
        args.activate_session,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(sql)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
