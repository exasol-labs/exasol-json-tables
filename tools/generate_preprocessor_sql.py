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

    local function is_clause_keyword(token)
        local normalized = normalize(token)
        return normalized ~= nil and CLAUSE_KEYWORDS[normalized] == true
    end

    local function is_join_keyword(token)
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
        local base_table = read_table_reference(tokens)
        if base_table == nil then
            error("JVS-PATH-ERROR: Path rewrite currently requires a query with a single base table in FROM.", 0)
        end

        local root_ref = encode_quoted_identifier(base_table.alias_name or base_table.table_name)
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


DISABLED_MODE_LUA = """
    local function rewrite_path_identifiers_in_sql(sqltext)
        return sqltext
    end
"""


def render_sql(
    schema: str,
    script: str,
    function_names: list[str],
    rewrite_path_identifiers: bool,
    activate_session: bool,
) -> str:
    function_rows = "\n".join(f"        {name} = true," for name in function_names)
    function_set_lua = "{\n" + function_rows + "\n    }"
    function_list_sql = ", ".join(function_names)
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
-- Path identifier rewrite: {path_comment}

CREATE SCHEMA IF NOT EXISTS {schema};

CREATE OR REPLACE LUA PREPROCESSOR SCRIPT {schema}.{script} AS
    local TARGET_FUNCTIONS = {function_set_lua}

    local function raise_function_error(function_name, message)
        error("JVS-FUNCTION-ERROR: " .. function_name .. ": " .. message, 0)
    end

    local function normalize(token)
        return sqlparsing.normalize(token)
    end

    local function is_ignored(token)
        return sqlparsing.iswhitespaceorcomment(token)
    end
{COMMON_LUA}
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

    local function rewrite(sqltext)
        local rewritten_sql = rewrite_path_identifiers_in_sql(sqltext)
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
    sql = render_sql(
        schema,
        script,
        function_names,
        args.rewrite_path_identifiers,
        args.activate_session,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(sql)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
