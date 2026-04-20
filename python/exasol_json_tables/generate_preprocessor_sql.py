#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "dist" / "json_preprocessor.sql"
DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT = "JVS_PREPROCESSOR_LIB"
IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an installable Exasol SQL preprocessor script that rewrites configured "
            "JSON helper calls and navigation syntax for the JSON query surface. This is the "
            "shared low-level generator used by the wrapper package tooling."
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
        "--disable-function-helpers",
        action="store_true",
        help="Do not install JSON helper function rewrites; generate only path / array syntax support.",
    )
    parser.add_argument(
        "--blocked-function-name",
        dest="blocked_function_names",
        action="append",
        default=None,
        help="Function name that should fail fast with a clear preprocessor error instead of reaching SQL resolution.",
    )
    parser.add_argument(
        "--blocked-function-message",
        default=None,
        help="Custom error message for blocked helper names. Default: This helper is not available in this build.",
    )
    parser.add_argument(
        "--rewrite-path-identifiers",
        action="store_true",
        help='Rewrite quoted dotted identifiers like "child.value" and array access like "items[0].value".',
    )
    parser.add_argument(
        "--allowed-schema",
        dest="allowed_schemas",
        action="append",
        default=None,
        help=(
            "Schema name that is allowed to use the JSON helper/path syntax. "
            "Repeat to allow multiple schemas. Default: JSON_VIEW."
        ),
    )
    parser.add_argument(
        "--helper-schema-map",
        dest="helper_schema_maps",
        action="append",
        default=None,
        help=(
            "Map an allowed public schema to an internal helper schema using PUBLIC=HELPER. "
            "Path and iterator rewrites will target the helper schema while queries still start from the public one."
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


def validate_helper_schema_map(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise SystemExit(f"Helper schema mapping must be PUBLIC=HELPER: {value}")
    public_schema, helper_schema = value.split("=", 1)
    return (
        validate_identifier("Helper mapping public schema", public_schema),
        validate_identifier("Helper mapping helper schema", helper_schema),
    )


def lua_quote_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def render_lua_value(value: object, indent: int = 0) -> str:
    indentation = " " * indent
    child_indentation = " " * (indent + 4)
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = ["{"]
        for key in sorted(value):
            rendered_key = f"[{lua_quote_string(key)}]"
            rendered_value = render_lua_value(value[key], indent + 4)
            lines.append(f"{child_indentation}{rendered_key} = {rendered_value},")
        lines.append(f"{indentation}}}")
        return "\n".join(lines)
    if isinstance(value, (list, tuple)):
        if not value:
            return "{}"
        lines = ["{"]
        for item in value:
            lines.append(f"{child_indentation}{render_lua_value(item, indent + 4)},")
        lines.append(f"{indentation}}}")
        return "\n".join(lines)
    if value is True:
        return "true"
    if value is False or value is None:
        return "false"
    if isinstance(value, (int, float)):
        return str(value)
    return lua_quote_string(str(value))


def render_lua_string_table(mapping: dict[str, object], indent: int = 0) -> str:
    return render_lua_value(mapping, indent)


def _build_preprocessor_config(
    *,
    function_names: list[str],
    blocked_function_names: list[str],
    blocked_function_message: str,
    allowed_schemas: list[str],
    helper_schema_map: dict[str, str],
    wrapper_group_config: dict[str, dict[str, dict[str, object]]] | None,
    wrapper_visible_column_config: dict[str, dict[str, dict[str, bool]]] | None,
    wrapper_to_json_config: dict[str, dict[str, dict[str, object]]] | None,
    regular_to_json_row_object_function: str | None,
    rewrite_path_identifiers: bool,
    helper_function_kinds: dict[str, str] | None = None,
) -> dict[str, object]:
    helper_function_kinds = helper_function_kinds or {name: "explicit_null" for name in function_names}
    example_allowed_schema = allowed_schemas[0] if allowed_schemas else "JSON_VIEW"
    helper_rewrite_mode = "wrapper" if wrapper_group_config else "marker"
    return {
        "helper_kind_by_name": helper_function_kinds,
        "blocked_functions": {name: True for name in blocked_function_names},
        "blocked_function_message": blocked_function_message,
        "allowed_json_schemas": {name: True for name in allowed_schemas},
        "allowed_json_schema_list": ", ".join(allowed_schemas),
        "example_allowed_schema": example_allowed_schema,
        "helper_schema_by_allowed_schema": dict(sorted(helper_schema_map.items())),
        "group_config_by_schema_and_table": wrapper_group_config or {},
        "visible_columns_by_schema_and_table": wrapper_visible_column_config or {},
        "to_json_config_by_schema_and_table": wrapper_to_json_config or {},
        "regular_to_json_row_object_function": regular_to_json_row_object_function or "",
        "helper_rewrite_mode": helper_rewrite_mode,
        "rewrite_path_identifiers": rewrite_path_identifiers,
    }


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
        EXISTS = true, IS = true, CASE = true, BETWEEN = true, LIKE = true,
        DISTINCT = true, ALL = true
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
        local canonical_sqltext = table.concat(sqlparsing.tokenize(sqltext))
        local tokens = {}
        local index = 1
        while index <= #canonical_sqltext do
            local ch = string.sub(canonical_sqltext, index, index)
            local next_ch = string.sub(canonical_sqltext, index + 1, index + 1)
            if string.match(ch, "%s") then
                local start_index = index
                repeat
                    index = index + 1
                    ch = string.sub(canonical_sqltext, index, index)
                until index > #canonical_sqltext or not string.match(ch, "%s")
                tokens[#tokens + 1] = {type = "whitespace", text = string.sub(canonical_sqltext, start_index, index - 1)}
            elseif ch == "-" and next_ch == "-" then
                local start_index = index
                index = index + 2
                while index <= #canonical_sqltext and string.sub(canonical_sqltext, index, index) ~= "\\n" do
                    index = index + 1
                end
                if index <= #canonical_sqltext then
                    index = index + 1
                end
                tokens[#tokens + 1] = {type = "comment", text = string.sub(canonical_sqltext, start_index, index - 1)}
            elseif ch == "/" and next_ch == "*" then
                local start_index = index
                index = index + 2
                while index <= #canonical_sqltext - 1 and string.sub(canonical_sqltext, index, index + 1) ~= "*/" do
                    index = index + 1
                end
                if index <= #canonical_sqltext - 1 then
                    index = index + 2
                end
                tokens[#tokens + 1] = {type = "comment", text = string.sub(canonical_sqltext, start_index, index - 1)}
            elseif ch == "'" then
                local start_index = index
                index = index + 1
                while index <= #canonical_sqltext do
                    local current = string.sub(canonical_sqltext, index, index)
                    if current == "'" then
                        if string.sub(canonical_sqltext, index + 1, index + 1) == "'" then
                            index = index + 2
                        else
                            index = index + 1
                            break
                        end
                    else
                        index = index + 1
                    end
                end
                tokens[#tokens + 1] = {type = "string", text = string.sub(canonical_sqltext, start_index, index - 1)}
            elseif ch == '"' then
                local start_index = index
                index = index + 1
                while index <= #canonical_sqltext do
                    local current = string.sub(canonical_sqltext, index, index)
                    if current == '"' then
                        if string.sub(canonical_sqltext, index + 1, index + 1) == '"' then
                            index = index + 2
                        else
                            index = index + 1
                            break
                        end
                    else
                        index = index + 1
                    end
                end
                local token_text = string.sub(canonical_sqltext, start_index, index - 1)
                tokens[#tokens + 1] = {
                    type = "quoted_identifier",
                    text = token_text,
                    identifier = decode_quoted_identifier(token_text)
                }
            elseif string.match(ch, "[A-Za-z_]") then
                local start_index = index
                index = index + 1
                while index <= #canonical_sqltext and string.match(string.sub(canonical_sqltext, index, index), "[A-Za-z0-9_]") do
                    index = index + 1
                end
                tokens[#tokens + 1] = {type = "word", text = string.sub(canonical_sqltext, start_index, index - 1)}
            elseif string.match(ch, "%d") then
                local start_index = index
                index = index + 1
                while index <= #canonical_sqltext and string.match(string.sub(canonical_sqltext, index, index), "[0-9]") do
                    index = index + 1
                end
                tokens[#tokens + 1] = {type = "number", text = string.sub(canonical_sqltext, start_index, index - 1)}
            else
                local two_chars = string.sub(canonical_sqltext, index, index + 1)
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

    local function find_matching_raw_paren(tokens, opening_index)
        local depth = 0
        local index = opening_index
        while index <= #tokens do
            local token = tokens[index]
            if token == "(" then
                depth = depth + 1
            elseif token == ")" then
                depth = depth - 1
                if depth == 0 then
                    return index
                end
            end
            index = index + 1
        end
        return nil
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
                    if table_index <= #tokens and tokens[table_index] == "(" then
                        local closing_index = find_matching_raw_paren(tokens, table_index)
                        if closing_index ~= nil then
                            local alias_name, alias_end_index = read_alias_after_table(tokens, closing_index)
                            if alias_name ~= nil then
                                out[#out + 1] = {
                                    table_name = alias_name,
                                    alias_name = alias_name,
                                    kind = "derived_source",
                                    insert_after_index = alias_end_index
                                }
                                index = alias_end_index
                            end
                        end
                    else
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
            end
            index = index + 1
        end
        return out
    end

    local function table_reference_is_in_allowed_schema(table_reference)
        if table_reference == nil or table_reference.schema_name == nil then
            return false
        end
        return ALLOWED_JSON_SCHEMAS[normalize_identifier_value(table_reference.schema_name)] == true
    end

    local function helper_schema_name_for_schema_name(schema_name)
        if schema_name == nil then
            return nil
        end
        local normalized = normalize_identifier_value(schema_name)
        return HELPER_SCHEMA_BY_ALLOWED_SCHEMA[normalized] or normalized
    end

    local function helper_schema_name_for_table_reference(table_reference)
        if table_reference == nil or table_reference.schema_name == nil then
            return nil
        end
        return helper_schema_name_for_schema_name(table_reference.schema_name)
    end

    local function collect_allowed_table_references(table_references)
        local out = {}
        for _, table_reference in ipairs(table_references) do
            if table_reference_is_in_allowed_schema(table_reference) then
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

    local function path_token_is_inside_to_json_call(tokens, token_index)
        local depth = 0
        local index = token_index - 1
        while index >= 1 do
            local token = tokens[index]
            if token.type == "punct" and token.text == ")" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == "(" then
                if depth == 0 then
                    local function_token = previous_significant_path_token(tokens, index - 1)
                    return function_token ~= nil
                            and HELPER_KIND_BY_NAME[normalize_path_token(function_token)] == "to_json"
                end
                depth = depth - 1
            end
            index = index - 1
        end
        return false
    end

    local function path_token_clause_and_depth(tokens, target_index)
        local depth = 0
        local clause = nil
        local index = 1
        while index <= target_index do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 then
                local normalized = normalize_path_token(token)
                if QUERY_START_KEYWORDS[normalized] == true then
                    clause = normalized
                elseif CLAUSE_KEYWORDS[normalized] == true or normalized == "FROM" or normalized == "JOIN"
                        or normalized == "LEFT" or normalized == "RIGHT" or normalized == "FULL"
                        or normalized == "INNER" or normalized == "CROSS" then
                    clause = normalized
                end
            end
            index = index + 1
        end
        return clause, depth
    end

    local function path_reference_auto_alias_sql(tokens, start_index, end_index, display_name)
        local clause, depth = path_token_clause_and_depth(tokens, start_index)
        if clause ~= "SELECT" or depth ~= 0 then
            return ""
        end
        local next_token = next_significant_path_token(tokens, end_index + 1)
        if next_token ~= nil and not is_clause_boundary_path_token(next_token) then
            return ""
        end
        return " AS " .. encode_quoted_identifier(display_name)
    end

    local function read_identifier_parts_from_path_tokens(tokens, index)
        local token, token_index = next_significant_path_token(tokens, index)
        if token == nil then
            return nil, nil, nil
        end

        local parts = nil
        local last_part_quoted = false
        if token.type == "word" then
            parts = {token.text}
        elseif token.type == "quoted_identifier" then
            parts = {token.identifier}
            last_part_quoted = true
        else
            return nil, nil, nil
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
                last_part_quoted = false
            elseif next_token.type == "quoted_identifier" then
                parts[#parts + 1] = next_token.identifier
                last_part_quoted = true
            else
                break
            end
            current_index = next_index
        end

        return parts, current_index, last_part_quoted
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

    local function qualify_table_name(base_table, child_table_name)
        local out = {}
        if base_table.catalog_name ~= nil then
            out[#out + 1] = encode_quoted_identifier(base_table.catalog_name)
        end
        local schema_name = helper_schema_name_for_table_reference(base_table) or base_table.schema_name
        if schema_name ~= nil then
            out[#out + 1] = encode_quoted_identifier(schema_name)
        end
        out[#out + 1] = encode_quoted_identifier(child_table_name)
        return table.concat(out, ".")
    end

    local function derive_child_table_name(parent_table_name, segment)
        if segment == "_value" then
            segment = "value"
        end
        return parent_table_name .. "_" .. encode_path_component(segment)
    end

    local function derive_array_child_table_name(parent_table_name, segment)
        if segment == "_value" then
            segment = "value"
        end
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
        if trimmed == "?" then
            return {
                kind = "parameter"
            }, nil
        end
        if string.upper(trimmed) == "PARAM" then
            return {
                kind = "parameter"
            }, nil
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
        if string.match(trimmed, "^[%a_][%w_]*$") then
            return {
                kind = "field",
                field_name = trimmed
            }, nil
        end
        return nil,
                'Unsupported array selector "' .. trimmed .. '". Supported selectors are numeric indexes, FIRST, LAST, SIZE, ?, PARAM, and direct field names.'
    end

    local function serialize_array_selector(selector)
        if selector.kind == "index" then
            return tostring(selector.index)
        elseif selector.kind == "parameter" then
            return "?"
        elseif selector.kind == "field" then
            return selector.field_name
        end
        return string.upper(selector.kind)
    end

    local function build_array_selector_sql(binding, current_table_name, selector_ref, array_ref, step, array_column_name, path)
        array_column_name = array_column_name or (step.name .. "|array")
        if step.selector.kind == "index" then
            return tostring(step.selector.index)
        elseif step.selector.kind == "first" then
            return "0"
        elseif step.selector.kind == "last" then
            return "(" .. array_ref .. "." .. encode_quoted_identifier(array_column_name) .. " - 1)"
        elseif step.selector.kind == "parameter" then
            return "?"
        elseif step.selector.kind == "field" then
            local schema_name = binding.helper_schema_name
                    or helper_schema_name_for_table_reference(binding)
                    or binding.schema_name
            local schema_tables = VISIBLE_COLUMNS_BY_SCHEMA_AND_TABLE[normalize_identifier_value(schema_name)]
            local table_columns = schema_tables and schema_tables[normalize_identifier_value(current_table_name)] or nil
            local normalized_field_name = normalize_identifier_value(step.selector.field_name)
            local field_is_visible = table_columns ~= nil and table_columns[normalized_field_name] == true
            local field_is_object_reference = table_columns ~= nil
                    and table_columns[normalized_field_name .. "|OBJECT"] == true
            local field_is_array_reference = table_columns ~= nil
                    and table_columns[normalized_field_name .. "|ARRAY"] == true
            local field_is_iterator_index = normalized_field_name == "_INDEX"
                    and (binding.kind == "iterator_row" or binding.kind == "iterator_value")
                    and normalize_identifier_value(current_table_name) == normalize_identifier_value(binding.table_name)
            if not field_is_visible and not field_is_iterator_index then
                if field_is_object_reference or field_is_array_reference then
                    raise_path_error(
                        path,
                        'Array selector "' .. step.selector.field_name
                                .. '" resolves to a nested object/array reference, not a scalar selector column. '
                                .. 'Use a scalar column such as [id] or traverse the nested branch explicitly.'
                    )
                end
                raise_path_error(
                    path,
                    'Array selector "' .. step.selector.field_name .. '" must be ?, PARAM, or a visible field on the current row.'
                )
            end
            return selector_ref .. "." .. encode_quoted_identifier(step.selector.field_name)
        end
        return nil
    end

    local function lookup_path_group_config_for_binding(binding, table_name, visible_name)
        if binding == nil or table_name == nil or visible_name == nil then
            return nil
        end
        local schema_name = binding.helper_schema_name
                or helper_schema_name_for_table_reference(binding)
                or binding.schema_name
        local schema_tables = GROUP_CONFIG_BY_SCHEMA_AND_TABLE[normalize_identifier_value(schema_name)]
        if schema_tables == nil then
            return nil
        end
        local table_groups = schema_tables[normalize_identifier_value(table_name)]
        if table_groups == nil then
            return nil
        end
        local normalized_visible_name = normalize_identifier_value(visible_name)
        local group_config = table_groups[normalized_visible_name]
        if group_config ~= nil then
            return group_config
        end
        return table_groups[normalize_identifier_value(visible_name .. "|array")]
    end

    local function table_has_visible_column(binding, table_name, column_name)
        if binding == nil or table_name == nil or column_name == nil then
            return false
        end
        local schema_name = binding.helper_schema_name
                or helper_schema_name_for_table_reference(binding)
                or binding.schema_name
        local schema_tables = VISIBLE_COLUMNS_BY_SCHEMA_AND_TABLE[normalize_identifier_value(schema_name)]
        local table_columns = schema_tables and schema_tables[normalize_identifier_value(table_name)] or nil
        return table_columns ~= nil and table_columns[normalize_identifier_value(column_name)] == true
    end

    local function ensure_property_step_can_navigate(path, binding, current_table_name, step_name)
        local group_config = lookup_path_group_config_for_binding(binding, current_table_name, step_name)
        local variant_columns = group_config and group_config.variantColumns or nil
        if variant_columns ~= nil and variant_columns["ARRAY"] ~= nil and variant_columns["OBJECT"] == nil then
            raise_path_error(
                path,
                'Path step "' .. step_name .. '" resolves to an array. '
                        .. 'Use JOIN ... IN row."' .. step_name .. '" for traversal or "'
                        .. step_name .. '[index]" for a single element.'
            )
        end
    end

    local function add_path_projection_column(column_names, seen_columns, column_name)
        if column_name == nil then
            return
        end
        local key = normalize_identifier_value(column_name)
        if key == nil or seen_columns[key] then
            return
        end
        seen_columns[key] = true
        column_names[#column_names + 1] = column_name
    end

    local function build_path_root_projection_join(table_reference, column_names, join_state)
        if column_names == nil or #column_names == 0 then
            return table_reference.reference_sql or render_bound_identifier(table_reference.alias_name or table_reference.table_name)
        end

        local key_parts = {normalize_identifier_value(table_reference.alias_name or table_reference.table_name)}
        for _, column_name in ipairs(column_names) do
            key_parts[#key_parts + 1] = normalize_identifier_value(column_name)
        end
        local join_key = table.concat(key_parts, "|")
        local existing = join_state.alias_by_key[join_key]
        if existing ~= nil then
            return existing
        end

        local alias_name = "__jvs_phidden_" .. tostring(join_state.manager.next_alias_id)
        join_state.manager.next_alias_id = join_state.manager.next_alias_id + 1
        local alias_ref = encode_quoted_identifier(alias_name)
        join_state.alias_by_key[join_key] = alias_ref

        local helper_table_sql = {}
        if table_reference.catalog_name ~= nil then
            helper_table_sql[#helper_table_sql + 1] = encode_quoted_identifier(table_reference.catalog_name)
        end
        helper_table_sql[#helper_table_sql + 1] = encode_quoted_identifier(
                helper_schema_name_for_table_reference(table_reference) or table_reference.schema_name
        )
        helper_table_sql[#helper_table_sql + 1] = encode_quoted_identifier(table_reference.table_name)

        local projected_columns = {encode_quoted_identifier("_id")}
        local seen_columns = {["_ID"] = true}
        for _, column_name in ipairs(column_names) do
            add_path_projection_column(projected_columns, seen_columns, encode_quoted_identifier(column_name))
        end

        local base_ref = table_reference.reference_sql or render_bound_identifier(table_reference.alias_name or table_reference.table_name)
        join_state.join_sql_parts[#join_state.join_sql_parts + 1] =
                " LEFT OUTER JOIN (SELECT "
                .. table.concat(projected_columns, ", ")
                .. " FROM " .. table.concat(helper_table_sql, ".") .. ") "
                .. alias_ref
                .. " ON (" .. base_ref .. "." .. encode_quoted_identifier("_id")
                .. " = " .. alias_ref .. "." .. encode_quoted_identifier("_id") .. ")"
        return alias_ref
    end

    local function count_variant_columns(variant_columns)
        if variant_columns == nil then
            return 0
        end
        local count = 0
        for _ in pairs(variant_columns) do
            count = count + 1
        end
        return count
    end

    local function resolve_root_hidden_path_source(binding, current_table_name, step_name, variant_label, join_state)
        if binding == nil or binding.kind ~= "json_source" then
            return nil, nil
        end
        if normalize_identifier_value(current_table_name) ~= normalize_identifier_value(binding.table_name) then
            return nil, nil
        end
        local group_config = lookup_path_group_config_for_binding(binding, current_table_name, step_name)
        local variant_columns = group_config and group_config.variantColumns or nil
        if variant_columns == nil or count_variant_columns(variant_columns) <= 1 then
            return nil, nil
        end
        local hidden_column_name = variant_columns[variant_label]
        if hidden_column_name == nil then
            return nil, nil
        end
        return build_path_root_projection_join(binding, {hidden_column_name}, join_state), hidden_column_name
    end

    local function resolve_object_step_source(binding, current_ref, current_table_name, step_name, join_state)
        local hidden_ref, hidden_column_name = resolve_root_hidden_path_source(
                binding,
                current_table_name,
                step_name,
                "OBJECT",
                join_state
        )
        if hidden_ref ~= nil then
            return hidden_ref, hidden_column_name
        end
        return current_ref, (step_name .. "|object")
    end

    local function resolve_array_step_source(binding, current_ref, current_table_name, step_name, join_state)
        local hidden_ref, hidden_column_name = resolve_root_hidden_path_source(
                binding,
                current_table_name,
                step_name,
                "ARRAY",
                join_state
        )
        if hidden_ref ~= nil then
            return hidden_ref, hidden_column_name
        end
        return current_ref, (step_name .. "|array")
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

    local function read_base_table_reference_for_path_tokens(tokens)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 and normalize_path_token(token) == "FROM" then
                local binding, alias_end_index = read_standard_source_binding(tokens, index + 1)
                if binding == nil then
                    return nil
                end
                binding.insert_after_index = alias_end_index
                return binding
            end
            index = index + 1
        end
        return nil
    end

    local function collect_path_references(tokens)
        local path_references = {}
        local index = 1
        while index <= #tokens do
            local identifier = nil
            if not path_token_is_inside_to_json_call(tokens, index) then
                identifier = read_path_identifier(tokens, index)
            end
            if identifier == nil then
                index = index + 1
            else
                path_references[#path_references + 1] = {
                    token_index = index,
                    path = identifier,
                    display_name = identifier,
                }
                index = index + 1
            end
        end
        return path_references
    end

    local function collect_qualified_path_references(tokens)
        local references = {}
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token ~= nil and (token.type == "word" or token.type == "quoted_identifier") then
                local dot_token, dot_index = next_significant_path_token(tokens, index + 1)
                local member_token, member_index = nil, nil
                if dot_token ~= nil and dot_token.type == "punct" and dot_token.text == "." then
                    member_token, member_index = next_significant_path_token(tokens, dot_index + 1)
                end
                if member_token ~= nil and member_token.type == "quoted_identifier"
                        and not path_token_is_inside_to_json_call(tokens, member_index)
                        and (string.find(member_token.identifier, "[", 1, true) ~= nil
                                or string.find(member_token.identifier, ".", 1, true) ~= nil) then
                    local root_name = token.type == "quoted_identifier" and token.identifier or token.text
                    references[#references + 1] = {
                        token_index = index,
                        end_index = member_index,
                        root_name = root_name,
                        path = member_token.identifier,
                        display_name = root_name .. "." .. member_token.identifier,
                    }
                    index = member_index + 1
                else
                    index = index + 1
                end
            else
                index = index + 1
            end
        end
        return references
    end

    local function collect_top_level_source_bindings_for_paths(tokens)
        local lookup = {}
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 then
                local prefix = read_join_prefix_at(tokens, index)
                if prefix ~= nil then
                    local binding, insert_after_index = read_standard_source_binding(tokens, prefix.end_index + 1)
                    if binding ~= nil then
                        binding.insert_after_index = insert_after_index
                        local alias_key = normalize_identifier_value(binding.alias_name or binding.table_name)
                        if alias_key ~= nil then
                            lookup[alias_key] = binding
                        end
                        local table_key = normalize_identifier_value(binding.table_name)
                        if table_key ~= nil and lookup[table_key] == nil then
                            lookup[table_key] = binding
                        end
                        if insert_after_index ~= nil then
                            index = insert_after_index
                        end
                    end
                end
            end
            index = index + 1
        end
        return lookup
    end

    local function add_join_insertion(join_insertions, insert_after_index, join_sql)
        if insert_after_index == nil or join_sql == nil or join_sql == "" then
            return
        end
        if join_insertions[insert_after_index] == nil then
            join_insertions[insert_after_index] = {}
        end
        join_insertions[insert_after_index][#join_insertions[insert_after_index] + 1] = join_sql
    end

    local function build_qualified_path_rewrite_plan(tokens)
        local references = collect_qualified_path_references(tokens)
        if #references == 0 then
            return {}, {}
        end

        local binding_lookup = collect_top_level_source_bindings_for_paths(tokens)
        local join_aliases = {}
        local join_insertions = {}
        local replacements = {}
        local alias_manager = {next_alias_id = 1}
        local root_hidden_join_states = {}

        local function path_join_state_for_binding(binding)
            local binding_key = normalize_identifier_value(binding.alias_name or binding.table_name)
            local state = root_hidden_join_states[binding_key]
            if state ~= nil then
                return state
            end
            if join_insertions[binding.insert_after_index] == nil then
                join_insertions[binding.insert_after_index] = {}
            end
            state = {
                manager = alias_manager,
                alias_by_key = {},
                join_sql_parts = join_insertions[binding.insert_after_index]
            }
            root_hidden_join_states[binding_key] = state
            return state
        end

        for _, reference in ipairs(references) do
            local binding = binding_lookup[normalize_identifier_value(reference.root_name)]
            if binding == nil then
                raise_path_error(
                    reference.path,
                    'Could not resolve the qualified path root "' .. reference.root_name .. '" in the current query block.'
                )
            elseif binding.kind == "derived_source" then
                raise_path_error(
                    reference.path,
                    'Path syntax does not resolve through derived-table aliases yet. '
                            .. 'Move the JSON path into the inner SELECT or query the wrapper view directly.'
                )
            elseif binding.kind == "iterator_value" then
                raise_path_error(
                    reference.path,
                    'Path and bracket syntax is not supported on VALUE iterators yet. '
                            .. 'Use plain SQL on the scalar iterator value or iterate an object array instead.'
                )
            elseif binding.kind ~= "iterator_row" and binding.kind ~= "json_source" then
                raise_path_error(
                    reference.path,
                    'Qualified path roots must be wrapper-table aliases or object-array iterator aliases.'
                )
            elseif binding.has_row_id ~= true then
                raise_path_error(
                    reference.path,
                    'This path root does not expose row identity, so nested path traversal is not supported.'
                )
            end

            local steps, parse_error = parse_path_steps(reference.path)
            if steps == nil or #steps == 0 then
                raise_path_error(reference.path, parse_error or "Invalid path syntax.")
            end

            local current_ref = binding.reference_sql or render_bound_identifier(binding.alias_name or binding.table_name)
            local current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
            local current_table_name = binding.table_name
            local prefix = {}
            local replacement = nil

            for step_index, step in ipairs(steps) do
                local is_last = step_index == #steps
                prefix[#prefix + 1] = serialize_step(step)
                local prefix_key = normalize_identifier_value(binding.alias_name or binding.table_name)
                        .. "|" .. table.concat(prefix, ".")

                if step.type == "property" then
                    if is_last then
                        replacement = current_ref .. "." .. encode_quoted_identifier(step.name)
                    else
                        ensure_property_step_can_navigate(reference.path, binding, current_table_name, step.name)
                        local existing = join_aliases[prefix_key]
                        local child_table_name = derive_child_table_name_for_iterator(current_table_name, step.name)
                        local step_source_ref, step_object_column_name = resolve_object_step_source(
                                binding,
                                current_ref,
                                current_table_name,
                                step.name,
                                path_join_state_for_binding(binding)
                        )
                        if existing == nil then
                            local alias_name = "__jvs_qpath_" .. tostring(alias_manager.next_alias_id)
                            alias_manager.next_alias_id = alias_manager.next_alias_id + 1
                            local alias_ref = encode_quoted_identifier(alias_name)
                            existing = {
                                alias_ref = alias_ref,
                                table_name = child_table_name
                            }
                            join_aliases[prefix_key] = existing
                            add_join_insertion(
                                join_insertions,
                                binding.insert_after_index,
                                " LEFT OUTER JOIN "
                                        .. qualify_table_name_for_iterator(binding, child_table_name)
                                        .. " " .. alias_ref
                                        .. " ON (" .. step_source_ref .. "." .. encode_quoted_identifier(step_object_column_name)
                                        .. " = " .. alias_ref .. "." .. encode_quoted_identifier("_id") .. ")"
                            )
                        end
                        current_ref = existing.alias_ref
                        current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
                        current_table_name = existing.table_name
                    end
                elseif step.type == "array" then
                    local array_source_ref, array_column_name = resolve_array_step_source(
                            binding,
                            current_ref,
                            current_table_name,
                            step.name,
                            path_join_state_for_binding(binding)
                    )
                    if step.selector.kind == "size" then
                        if not is_last then
                            raise_path_error(reference.path, "SIZE must be the last selector in a path.")
                        end
                        replacement = array_source_ref .. "." .. encode_quoted_identifier(array_column_name)
                    else
                        local existing = join_aliases[prefix_key]
                        local child_table_name = derive_array_child_table_name_for_iterator(current_table_name, step.name)
                        local selector_sql = build_array_selector_sql(
                                binding,
                                current_table_name,
                                current_ref,
                                array_source_ref,
                                step,
                                array_column_name,
                                reference.path
                        )
                        if selector_sql == nil then
                            raise_path_error(reference.path, "Unsupported array selector.")
                        end
                        if existing == nil then
                            local alias_name = "__jvs_qpath_" .. tostring(alias_manager.next_alias_id)
                            alias_manager.next_alias_id = alias_manager.next_alias_id + 1
                            local alias_ref = encode_quoted_identifier(alias_name)
                            existing = {
                                alias_ref = alias_ref,
                                table_name = child_table_name
                            }
                            join_aliases[prefix_key] = existing
                            add_join_insertion(
                                join_insertions,
                                binding.insert_after_index,
                                " LEFT OUTER JOIN "
                                        .. qualify_table_name_for_iterator(binding, child_table_name)
                                        .. " " .. alias_ref
                                        .. " ON (" .. current_row_id .. " = " .. alias_ref .. "."
                                        .. encode_quoted_identifier("_parent")
                                        .. " AND " .. alias_ref .. "." .. encode_quoted_identifier("_pos")
                                        .. " = " .. selector_sql .. ")"
                            )
                        end
                        current_ref = existing.alias_ref
                        current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
                        current_table_name = existing.table_name
                        if is_last then
                            if not table_has_visible_column(binding, current_table_name, "_value") then
                                raise_path_error(
                                    reference.path,
                                    'Bracket access on object-array elements requires a trailing property, '
                                            .. 'for example "items[LAST].value", or a nested JOIN ... IN.'
                                )
                            end
                            replacement = current_ref .. "." .. encode_quoted_identifier("_value")
                        end
                    end
                end
            end

            if replacement == nil then
                raise_path_error(reference.path, "Unable to rewrite the qualified path expression.")
            end

            replacements[reference.token_index] = {
                end_index = reference.end_index,
                replacement_sql = replacement
                        .. path_reference_auto_alias_sql(
                                tokens,
                                reference.token_index,
                                reference.end_index,
                                reference.display_name
                        )
            }
        end

        return replacements, join_insertions
    end

    local function rewrite_path_query_block_sql(sqltext)
        local original_tokens = tokenize_path_sql(sqltext)
        if not is_path_query_statement(original_tokens) then
            return sqltext
        end

        local replacements, join_insertions = build_qualified_path_rewrite_plan(original_tokens)
        local path_references = collect_path_references(original_tokens)
        if #path_references == 0 and next(replacements) == nil then
            return sqltext
        end

        local base_table = nil
        if #path_references > 0 then
            local base_binding = read_base_source_binding_from_path_tokens(original_tokens)
            base_table = read_base_table_reference_for_path_tokens(original_tokens)
            if base_table == nil then
                if base_binding ~= nil and base_binding.kind == "derived_source" then
                    raise_scope_error(
                        "JSON path syntax",
                        'JSON path syntax does not resolve through derived tables yet. '
                                .. 'Move the JSON path into the inner SELECT or query the wrapper view directly.'
                    )
                end
                error("JVS-PATH-ERROR: Path rewrite currently requires a query with a single base table in FROM.", 0)
            elseif base_table.kind == "derived_source" then
                raise_scope_error(
                    "JSON path syntax",
                    'JSON path syntax does not resolve through derived tables yet. '
                            .. 'Move the JSON path into the inner SELECT or query the wrapper view directly.'
                )
            elseif base_table.kind ~= "json_source" then
                raise_scope_error(
                    "JSON path syntax",
                    'Path rewriting currently requires the base table in FROM to be one of the configured JSON schemas.'
                )
            end
        end

        if #path_references > 0 then
            local root_ref = base_table.reference_sql or render_bound_identifier(base_table.alias_name or base_table.table_name)
            local join_aliases = {}
            local join_sql_parts = {}
            local alias_manager = {next_alias_id = 1}
            local root_hidden_join_state = {
                manager = alias_manager,
                alias_by_key = {},
                join_sql_parts = join_sql_parts
            }

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
                            ensure_property_step_can_navigate(reference.path, base_table, current_table_name, step.name)
                            local existing = join_aliases[prefix_key]
                            local child_table_name = derive_child_table_name(current_table_name, step.name)
                            local step_source_ref, step_object_column_name = resolve_object_step_source(
                                    base_table,
                                    current_ref,
                                    current_table_name,
                                    step.name,
                                    root_hidden_join_state
                            )
                            if existing == nil then
                                local alias_name = "__jvs_path_" .. tostring(alias_manager.next_alias_id)
                                alias_manager.next_alias_id = alias_manager.next_alias_id + 1
                                local alias_ref = encode_quoted_identifier(alias_name)
                                existing = {
                                    alias_ref = alias_ref,
                                    table_name = child_table_name
                                }
                                join_aliases[prefix_key] = existing
                                join_sql_parts[#join_sql_parts + 1] = " LEFT OUTER JOIN "
                                        .. qualify_table_name(base_table, child_table_name)
                                        .. " " .. alias_ref
                                        .. " ON (" .. step_source_ref .. "." .. encode_quoted_identifier(step_object_column_name)
                                        .. " = " .. alias_ref .. "." .. encode_quoted_identifier("_id") .. ")"
                            end
                            current_ref = existing.alias_ref
                            current_row_id = current_ref .. "." .. encode_quoted_identifier("_id")
                            current_table_name = existing.table_name
                        end
                    elseif step.type == "array" then
                        local array_source_ref, array_column_name = resolve_array_step_source(
                                base_table,
                                current_ref,
                                current_table_name,
                                step.name,
                                root_hidden_join_state
                        )
                        if step.selector.kind == "size" then
                            if not is_last then
                                raise_path_error(reference.path, "SIZE must be the last selector in a path.")
                            end
                            replacement = array_source_ref .. "." .. encode_quoted_identifier(array_column_name)
                        else
                            local existing = join_aliases[prefix_key]
                            local child_table_name = derive_array_child_table_name(current_table_name, step.name)
                            local alias_name = "__jvs_path_" .. tostring(alias_manager.next_alias_id)
                            local selector_sql = build_array_selector_sql(
                                    base_table,
                                    current_table_name,
                                    current_ref,
                                    array_source_ref,
                                    step,
                                    array_column_name,
                                    reference.path
                            )
                            if selector_sql == nil then
                                raise_path_error(reference.path, "Unsupported array selector.")
                            end
                            if existing == nil then
                                alias_manager.next_alias_id = alias_manager.next_alias_id + 1
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
                                if not table_has_visible_column(base_table, current_table_name, "_value") then
                                    raise_path_error(
                                        reference.path,
                                        'Bracket access on object-array elements requires a trailing property, '
                                                .. 'for example "items[LAST].value", or a nested JOIN ... IN.'
                                    )
                                end
                                replacement = current_ref .. "." .. encode_quoted_identifier("_value")
                            end
                        end
                    end
                end
                if replacement == nil then
                    raise_path_error(reference.path, "Unable to rewrite the path expression.")
                end
                replacements[reference.token_index] = {
                    end_index = reference.token_index,
                    replacement_sql = replacement
                            .. path_reference_auto_alias_sql(
                                    original_tokens,
                                    reference.token_index,
                                    reference.token_index,
                                    reference.display_name
                            )
                }
            end

            if #join_sql_parts > 0 then
                add_join_insertion(join_insertions, base_table.insert_after_index, table.concat(join_sql_parts))
            end
        end

        local out = {}
        local index = 1
        while index <= #original_tokens do
            local replacement = replacements[index]
            if replacement ~= nil then
                out[#out + 1] = replacement.replacement_sql
                local pending_joins = join_insertions[replacement.end_index]
                if pending_joins ~= nil then
                    out[#out + 1] = table.concat(pending_joins)
                end
                index = replacement.end_index + 1
            else
                out[#out + 1] = original_tokens[index].text
                local pending_joins = join_insertions[index]
                if pending_joins ~= nil then
                    out[#out + 1] = table.concat(pending_joins)
                end
                index = index + 1
            end
        end
        return table.concat(out)
    end

    local function rewrite_path_identifiers_in_sql(sqltext)
        return rewrite_sql_with_query_blocks(sqltext, rewrite_path_query_block_sql)
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

    local function render_bound_identifier(name, is_quoted)
        if is_quoted then
            return encode_quoted_identifier(name)
        end
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
            return nil, nil, nil
        end
        if token.type == "word" then
            return token.text, token_index, false
        end
        if token.type == "quoted_identifier" then
            return token.identifier, token_index, true
        end
        return nil, nil, nil
    end

    local function read_single_identifier_parts_from_path_tokens(tokens, index)
        local parts, end_index, is_quoted = read_identifier_parts_from_path_tokens(tokens, index)
        if parts == nil or #parts ~= 1 then
            return nil, nil, nil
        end
        return parts[1], end_index, is_quoted
    end

    local function read_alias_after_source_path_tokens(tokens, source_end_index)
        local alias_name = nil
        local alias_end_index = source_end_index
        local alias_quoted = false
        local maybe_alias, maybe_alias_index = next_significant_path_token(tokens, source_end_index + 1)
        if maybe_alias == nil then
            return nil, alias_end_index, alias_quoted
        end
        if normalize_path_token(maybe_alias) == "AS" then
            local alias_token, alias_index = next_significant_path_token(tokens, maybe_alias_index + 1)
            if alias_token ~= nil then
                local parsed_alias_name, _, parsed_alias_quoted = read_single_identifier_parts_from_path_tokens(
                    tokens,
                    alias_index
                )
                if parsed_alias_name ~= nil then
                    alias_name = parsed_alias_name
                    alias_end_index = alias_index
                    alias_quoted = parsed_alias_quoted
                end
            end
        elseif not path_token_is_source_boundary(maybe_alias) then
            local parsed_alias_name, _, parsed_alias_quoted = read_single_identifier_parts_from_path_tokens(
                tokens,
                maybe_alias_index
            )
            if parsed_alias_name ~= nil then
                alias_name = parsed_alias_name
                alias_end_index = maybe_alias_index
                alias_quoted = parsed_alias_quoted
            end
        end
        return alias_name, alias_end_index, alias_quoted
    end

    local function detect_generated_iterator_subquery_binding(tokens, source_index, closing_index, alias_name, alias_quoted)
        if alias_name == nil then
            return nil
        end

        local inner_sql = slice_path_tokens_text(tokens, source_index + 1, closing_index - 1)
        if string.find(inner_sql, encode_quoted_identifier("__jvs_iter_src"), 1, true) == nil then
            return nil
        end

        local inner_tokens = tokenize_path_sql(inner_sql)
        local first_token = next_significant_path_token(inner_tokens, 1)
        if first_token == nil or normalize_path_token(first_token) ~= "SELECT" then
            return nil
        end

        local base_table = read_base_table_reference_from_path_tokens(inner_tokens)
        if base_table == nil then
            return nil
        end

        local alias_ref = render_bound_identifier(alias_name, alias_quoted)
        local is_value = string.find(
                inner_sql,
                encode_quoted_identifier("_value") .. " AS " .. alias_ref,
                1,
                true
        ) ~= nil

        return {
            alias_name = alias_name,
            reference_sql = alias_ref,
            kind = is_value and "iterator_value" or "iterator_row",
            table_name = base_table.table_name,
            schema_name = base_table.schema_name,
            catalog_name = base_table.catalog_name,
            has_row_id = not is_value,
            helper_schema_name = helper_schema_name_for_schema_name(base_table.schema_name)
        }
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
            local alias_name, alias_end_index, alias_quoted = read_alias_after_source_path_tokens(tokens, closing_index)
            local generated_iterator_binding = detect_generated_iterator_subquery_binding(
                tokens,
                source_index,
                closing_index,
                alias_name,
                alias_quoted
            )
            if generated_iterator_binding ~= nil then
                return generated_iterator_binding, alias_end_index
            end
            if alias_name ~= nil then
                return {
                    alias_name = alias_name,
                    reference_sql = render_bound_identifier(alias_name, alias_quoted),
                    kind = "derived_source",
                    table_name = nil,
                    schema_name = nil,
                    catalog_name = nil,
                    has_row_id = false
                }, alias_end_index
            end
            return nil, closing_index
        end
        local parts, table_end_index, table_name_quoted = read_identifier_parts_from_path_tokens(tokens, source_start_index)
        if parts == nil then
            return nil, nil
        end
        local alias_name, alias_end_index, alias_quoted = read_alias_after_source_path_tokens(tokens, table_end_index)
        local resolved_alias_name = alias_name or parts[#parts]
        local resolved_alias_quoted = table_name_quoted
        if alias_name ~= nil then
            resolved_alias_quoted = alias_quoted
        end
        local binding = {
            alias_name = resolved_alias_name,
            reference_sql = render_bound_identifier(resolved_alias_name, resolved_alias_quoted),
            kind = "other_source",
            table_name = parts[#parts],
            schema_name = (#parts >= 2) and parts[#parts - 1] or nil,
            catalog_name = (#parts >= 3) and parts[#parts - 2] or nil,
            has_row_id = true,
            helper_schema_name = nil
        }
        if table_reference_is_in_allowed_schema({
            schema_name = binding.schema_name
        }) then
            binding.kind = "json_source"
            binding.helper_schema_name = helper_schema_name_for_schema_name(binding.schema_name)
        end
        return binding, alias_end_index
    end

    local function read_base_source_binding_from_path_tokens(tokens)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 and normalize_path_token(token) == "FROM" then
                local binding, _ = read_standard_source_binding(tokens, index + 1)
                return binding
            end
            index = index + 1
        end
        return nil
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

        local alias_name, iterator_index, alias_quoted = read_single_identifier_from_path_tokens(tokens, iterator_index)
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
            alias_quoted = alias_quoted,
            reference_sql = render_bound_identifier(alias_name, alias_quoted),
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
        local schema_name = binding.helper_schema_name or binding.schema_name
        if schema_name ~= nil then
            out[#out + 1] = encode_quoted_identifier(schema_name)
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

    local function build_iterator_relation_sql(qualified_table_name, iterator_source)
        local relation_alias = iterator_source.reference_sql
        local inner_alias_name = "__jvs_iter_src"
        local inner_alias = encode_quoted_identifier(inner_alias_name)
        if iterator_source.is_value then
            return "(SELECT " .. inner_alias .. ".*, "
                    .. inner_alias .. "." .. encode_quoted_identifier("_pos")
                    .. " AS " .. encode_quoted_identifier("_index") .. ", "
                    .. inner_alias .. "." .. encode_quoted_identifier("_value")
                    .. " AS " .. iterator_source.reference_sql
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
        local iterator_schema_name = root_binding.helper_schema_name or root_binding.schema_name
        return {
            alias_name = iterator_source.alias_name,
            reference_sql = iterator_source.reference_sql,
            kind = iterator_source.is_value and "iterator_value" or "iterator_row",
            table_name = array_child_table_name,
            schema_name = iterator_schema_name,
            catalog_name = root_binding.catalog_name,
            has_row_id = not iterator_source.is_value,
            helper_schema_name = iterator_schema_name
        }
    end

    local function build_iterator_join_clause(iterator_source, root_binding, join_kind)
        if root_binding == nil then
            raise_scope_error(
                "JSON array iteration syntax",
                json_schema_scope_example()
            )
        end
        if root_binding.kind ~= "json_source" and root_binding.kind ~= "iterator_row" then
            if root_binding.kind == "iterator_value" then
                raise_iterator_error('Scalar VALUE iterators cannot be used as the root of another iterator path.')
            end
            raise_scope_error(
                "JSON array iteration syntax",
                'Iterator roots must come from a configured JSON schema or from an object-array iterator.'
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
                        iterator_source
                )
                .. " ON (" .. current_row_id .. " = " .. iterator_source.reference_sql
                .. "." .. encode_quoted_identifier("_parent") .. ")"

        return table.concat(out), build_iterator_binding(iterator_source, root_binding, array_child_table_name), nil
    end

    local function build_iterator_from_clause(iterator_source, root_binding)
        if root_binding == nil then
            raise_scope_error(
                "JSON array iteration syntax",
                json_schema_scope_example()
            )
        end
        if root_binding.kind ~= "json_source" and root_binding.kind ~= "iterator_row" then
            if root_binding.kind == "iterator_value" then
                raise_iterator_error('Scalar VALUE iterators cannot be used as the root of another iterator path.')
            end
            raise_scope_error(
                "JSON array iteration syntax",
                'Iterator roots must come from a configured JSON schema or from an object-array iterator.'
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
                            iterator_source
                    )
            correlation_filter_sql = "(" .. current_row_id .. " = " .. iterator_source.reference_sql
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
                        iterator_source
                )
                .. " ON (" .. current_row_id .. " = " .. iterator_source.reference_sql
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

    local function rewrite_sql_with_query_blocks(sqltext, query_block_rewriter, options)
        options = options or {}
        local recurse_parenthesized_query_blocks = options.recurse_parenthesized_query_blocks ~= false
        local recurse_as_query_blocks = options.recurse_as_query_blocks ~= false
        local tokens = tokenize_path_sql(sqltext)
        local out = {}
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                local closing_index = find_matching_path_paren(tokens, index)
                local first_inside, first_inside_index = next_significant_path_token(tokens, index + 1)
                if recurse_parenthesized_query_blocks and closing_index ~= nil and first_inside_index ~= nil
                        and path_token_is_query_start(first_inside) then
                    out[#out + 1] = "("
                    out[#out + 1] = rewrite_sql_with_query_blocks(
                            slice_path_tokens_text(tokens, index + 1, closing_index - 1),
                            query_block_rewriter,
                            options
                    )
                    out[#out + 1] = ")"
                    index = closing_index + 1
                else
                    out[#out + 1] = token.text
                    index = index + 1
                end
            elseif normalize_path_token(token) == "AS" then
                local next_token, next_index = next_significant_path_token(tokens, index + 1)
                if recurse_as_query_blocks and next_token ~= nil and path_token_is_query_start(next_token) then
                    out[#out + 1] = token.text
                    if next_index > index + 1 then
                        out[#out + 1] = slice_path_tokens_text(tokens, index + 1, next_index - 1)
                    end
                    out[#out + 1] = rewrite_sql_with_query_blocks(
                            slice_path_tokens_text(tokens, next_index, #tokens),
                            query_block_rewriter,
                            options
                    )
                    return table.concat(out)
                else
                    out[#out + 1] = token.text
                    index = index + 1
                end
            else
                out[#out + 1] = token.text
                index = index + 1
            end
        end

        local nested_sql = table.concat(out)
        local nested_tokens = tokenize_path_sql(nested_sql)
        local first_token = next_significant_path_token(nested_tokens, 1)
        if first_token ~= nil and path_token_is_query_start(first_token) then
            local function top_level_set_operator_end(operator_index)
                local operator_token = nested_tokens[operator_index]
                local normalized = normalize_path_token(operator_token)
                if normalized == "UNION" or normalized == "EXCEPT" or normalized == "MINUS" then
                    local next_token, next_index = next_significant_path_token(nested_tokens, operator_index + 1)
                    if next_token ~= nil then
                        local next_normalized = normalize_path_token(next_token)
                        if next_normalized == "ALL" or next_normalized == "DISTINCT" then
                            return next_index
                        end
                    end
                end
                return operator_index
            end

            local function rewrite_top_level_set_query()
                local depth = 0
                local branch_start = 1
                local found_set_operator = false
                local rewritten_parts = {}
                local token_index = 1
                while token_index <= #nested_tokens do
                    local token = nested_tokens[token_index]
                    if token.type == "punct" and token.text == "(" then
                        depth = depth + 1
                    elseif token.type == "punct" and token.text == ")" then
                        depth = depth - 1
                    elseif depth == 0 then
                        local normalized = normalize_path_token(token)
                        if normalized == "UNION" or normalized == "EXCEPT" or normalized == "MINUS" then
                            found_set_operator = true
                            rewritten_parts[#rewritten_parts + 1] = query_block_rewriter(
                                    slice_path_tokens_text(nested_tokens, branch_start, token_index - 1)
                            )
                            local operator_end = top_level_set_operator_end(token_index)
                            rewritten_parts[#rewritten_parts + 1] = slice_path_tokens_text(
                                    nested_tokens,
                                    token_index,
                                    operator_end
                            )
                            branch_start = operator_end + 1
                            token_index = operator_end
                        end
                    end
                    token_index = token_index + 1
                end
                if not found_set_operator then
                    return nil
                end
                rewritten_parts[#rewritten_parts + 1] = query_block_rewriter(
                        slice_path_tokens_text(nested_tokens, branch_start, #nested_tokens)
                )
                return table.concat(rewritten_parts)
            end

            local rewritten_set_query = rewrite_top_level_set_query()
            if rewritten_set_query ~= nil then
                return rewritten_set_query
            end
            return query_block_rewriter(nested_sql)
        end
        return nested_sql
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

    local function rewrite_query_block_tokens(tokens, outer_scope, options)
        options = options or {}
        local recurse_parenthesized_query_blocks = options.recurse_parenthesized_query_blocks ~= false
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
                if recurse_parenthesized_query_blocks and closing_index ~= nil and first_inside_index ~= nil
                        and path_token_is_query_start(first_inside) then
                    out[#out + 1] = "("
                    out[#out + 1] = rewrite_query_block_tokens(
                            {table.unpack(tokens, index + 1, closing_index - 1)},
                            scope,
                            options
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
                if token.type == "word" or token.type == "quoted_identifier" then
                    local identifier_name = token.type == "quoted_identifier" and token.identifier or token.text
                    local binding = lookup_scope(scope, identifier_name)
                    local dot_token, dot_index = next_significant_path_token(tokens, index + 1)
                    local member_token, member_index = nil, nil
                    if binding ~= nil and dot_token ~= nil then
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
                            and member_token ~= nil and member_token.type == "quoted_identifier"
                            and (string.find(member_name, "[", 1, true) ~= nil
                                    or string.find(member_name, ".", 1, true) ~= nil) then
                        raise_path_error(
                            member_name,
                            'Path and bracket syntax on iterator aliases is not supported yet. '
                                    .. 'Use direct iterator columns, nested JOIN ... IN alias."path", or root-level JSON paths.'
                        )
                    elseif binding ~= nil and (binding.kind == "iterator_row" or binding.kind == "iterator_value")
                            and dot_token ~= nil and dot_token.type == "punct" and dot_token.text == "."
                            and normalize_identifier_value(member_name) == "_INDEX" then
                        out[#out + 1] = token.text
                        out[#out + 1] = "."
                        out[#out + 1] = encode_quoted_identifier("_index")
                        index = member_index + 1
                        goto continue
                    end
                end
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
                        and member_token ~= nil and member_token.type == "quoted_identifier"
                        and (string.find(member_name, "[", 1, true) ~= nil
                                or string.find(member_name, ".", 1, true) ~= nil) then
                    raise_path_error(
                        member_name,
                        'Path and bracket syntax on iterator aliases is not supported yet. '
                                .. 'Use direct iterator columns, nested JOIN ... IN alias."path", or root-level JSON paths.'
                    )
                elseif binding ~= nil and (binding.kind == "iterator_row" or binding.kind == "iterator_value")
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

    local function rewrite_array_iteration_query_sql(sqltext, options)
        local tokens = tokenize_path_sql(sqltext)
        local first_token = next_significant_path_token(tokens, 1)
        if first_token == nil or not path_token_is_query_start(first_token) then
            return sqltext
        end
        return rewrite_query_block_tokens(tokens, {}, options)
    end

    local function rewrite_array_iteration_in_sql(sqltext)
        return rewrite_sql_with_query_blocks(
                sqltext,
                rewrite_array_iteration_query_sql,
                {recurse_parenthesized_query_blocks = false}
        )
    end
"""


MARKER_HELPER_REWRITE_LUA = """
    local function collect_helper_call_replacements(original_sqltext, tokens)
        local replacements = {}
        local index = 1
        while index <= #tokens do
            local call = read_call(tokens, index)
            local helper_kind = call and HELPER_KIND_BY_NAME[call.last_identifier] or nil
            if call and helper_kind == "explicit_null" then
                local closing_paren, top_level_commas = find_matching_paren(tokens, call.opening_paren)
                if closing_paren == nil then
                    raise_function_error(call.last_identifier, "Missing closing parenthesis.")
                elseif top_level_commas ~= 0 then
                    raise_function_error(call.last_identifier, "Expected exactly one argument.")
                elseif not has_expression_argument(tokens, call.opening_paren, closing_paren) then
                    raise_function_error(call.last_identifier, "Expected exactly one argument.")
                else
                    local helper_allowed, helper_scope_message = helper_query_targets_allowed_schema(original_sqltext)
                    if not helper_allowed then
                        raise_scope_error("JSON helper functions", helper_scope_message)
                    end
                    local out = {"(CASE WHEN "}
                    for argument_index = call.opening_paren + 1, closing_paren - 1 do
                        out[#out + 1] = tokens[argument_index]
                    end
                    out[#out + 1] = " IS NULL THEN TRUE ELSE FALSE END)"
                    replacements[index] = {
                        closing_paren = closing_paren,
                        replacement_sql = table.concat(out)
                    }
                    index = closing_paren + 1
                end
            else
                index = index + 1
            end
        end
        return replacements, {}
    end
"""


WRAPPER_EXPLICIT_NULL_HELPER_LUA = """
    local VARIANT_LABEL_ORDER = {"NUMBER", "STRING", "BOOLEAN", "OBJECT", "ARRAY"}

    local function lookup_json_table_config(schema_name, table_name)
        if schema_name == nil or table_name == nil then
            return nil
        end
        local schema_tables = GROUP_CONFIG_BY_SCHEMA_AND_TABLE[normalize_identifier_value(schema_name)]
        if schema_tables == nil then
            return nil
        end
        return schema_tables[normalize_identifier_value(table_name)]
    end

    local function lookup_group_config(schema_name, table_name, visible_name)
        local table_config = lookup_json_table_config(schema_name, table_name)
        if table_config == nil or visible_name == nil then
            return nil
        end
        return table_config[normalize_identifier_value(visible_name)]
    end

    local function lookup_to_json_root_config(schema_name, table_name)
        if schema_name == nil or table_name == nil then
            return nil
        end
        local schema_tables = TO_JSON_CONFIG_BY_SCHEMA_AND_TABLE[normalize_identifier_value(schema_name)]
        if schema_tables == nil then
            return nil
        end
        return schema_tables[normalize_identifier_value(table_name)]
    end

    local function read_simple_identifier_argument(tokens, opening_paren, closing_paren)
        local argument_token = nil
        for index = opening_paren + 1, closing_paren - 1 do
            if not is_ignored(tokens[index]) then
                if argument_token ~= nil then
                    return nil
                end
                argument_token = tokens[index]
            end
        end
        if argument_token == nil then
            return nil
        end
        local parts = parse_identifier_token(argument_token)
        if parts == nil or #parts == 0 then
            return nil
        end
        local normalized_parts = {}
        for index, part in ipairs(parts) do
            normalized_parts[index] = normalize_identifier_value(part)
        end
        return normalized_parts
    end

    local function read_simple_identifier_argument_in_range(tokens, start_index, end_index)
        local identifier_index = next_significant(tokens, start_index)
        if identifier_index > end_index then
            return nil
        end

        local current = identifier_index
        local identifier_parts = parse_identifier_token(tokens[current])
        if identifier_parts == nil then
            return nil
        end

        local normalized_parts = {}
        for _, part in ipairs(identifier_parts) do
            normalized_parts[#normalized_parts + 1] = normalize_identifier_value(part)
        end

        while true do
            local dot_index = next_significant(tokens, current + 1)
            if dot_index > end_index or tokens[dot_index] ~= "." then
                break
            end
            local next_identifier_index = next_significant(tokens, dot_index + 1)
            if next_identifier_index > end_index then
                return nil
            end
            current = next_identifier_index
            local next_identifier_parts = parse_identifier_token(tokens[current])
            if next_identifier_parts == nil then
                return nil
            end
            for _, part in ipairs(next_identifier_parts) do
                normalized_parts[#normalized_parts + 1] = normalize_identifier_value(part)
            end
        end

        if next_significant(tokens, current + 1) <= end_index then
            return nil
        end
        return normalized_parts
    end

    local function argument_range_is_star(tokens, start_index, end_index)
        local token_index = next_significant(tokens, start_index)
        if token_index > end_index or tokens[token_index] ~= "*" then
            return false
        end
        return next_significant(tokens, token_index + 1) > end_index
    end

    local function split_call_argument_ranges(tokens, opening_paren, closing_paren)
        local ranges = {}
        local depth = 0
        local argument_start = nil
        local saw_any_token = false
        local index = opening_paren + 1
        while index < closing_paren do
            local token = tokens[index]
            if not is_ignored(token) then
                saw_any_token = true
                if argument_start == nil then
                    argument_start = index
                end
                if token == "(" then
                    depth = depth + 1
                elseif token == ")" then
                    if depth > 0 then
                        depth = depth - 1
                    end
                elseif token == "," and depth == 0 then
                    local argument_end = previous_significant_raw(tokens, index - 1)
                    if argument_end == nil or argument_start == nil or argument_end < argument_start then
                        return nil, "TO_JSON does not allow empty arguments."
                    end
                    ranges[#ranges + 1] = {
                        start_index = argument_start,
                        end_index = argument_end
                    }
                    argument_start = nil
                end
            end
            index = index + 1
        end

        if argument_start == nil then
            if saw_any_token then
                return nil, "TO_JSON does not allow a trailing comma."
            end
            return {}, nil
        end

        local final_end = previous_significant_raw(tokens, closing_paren - 1)
        if final_end == nil or final_end < argument_start then
            return nil, "TO_JSON does not allow empty arguments."
        end
        ranges[#ranges + 1] = {
            start_index = argument_start,
            end_index = final_end
        }
        return ranges, nil
    end

    local function trim_sql_argument_text(value)
        return string.gsub(string.gsub(value, "^%s+", ""), "%s+$", "")
    end

    local function raw_argument_text(tokens, start_index, end_index)
        local parts = {}
        for index = start_index, end_index do
            parts[#parts + 1] = tokens[index]
        end
        return trim_sql_argument_text(table.concat(parts))
    end

    local function to_json_invalid_argument_message(tokens, start_index, end_index)
        local raw = raw_argument_text(tokens, start_index, end_index)
        if raw == "" then
            return nil
        end
        if string.sub(raw, 1, 1) == '"' and string.sub(raw, -1, -1) == '"' then
            local decoded = decode_quoted_identifier(raw)
            if string.find(decoded, ".", 1, true) ~= nil or string.find(decoded, "[", 1, true) ~= nil then
                return 'TO_JSON subset arguments must be visible top-level properties. Nested paths such as "meta.info.note" and bracket expressions such as "tags[SIZE]" are not supported.'
            end
        end
        return nil
    end

    local function collect_helper_table_reference_lookup(tokens)
        local lookup = {}
        for _, table_reference in ipairs(collect_top_level_table_references(tokens)) do
            local alias_key = normalize_identifier_value(table_reference.alias_name or table_reference.table_name)
            if alias_key ~= nil then
                lookup[alias_key] = table_reference
            end
            local table_key = normalize_identifier_value(table_reference.table_name)
            if table_key ~= nil and lookup[table_key] == nil then
                lookup[table_key] = table_reference
            end
        end
        return lookup
    end

    local function collect_helper_table_reference_lookup_from_sql(sqltext)
        local lookup = {}
        local tokens = tokenize_path_sql(sqltext)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 then
                local prefix = read_join_prefix_at(tokens, index)
                if prefix ~= nil then
                    local binding, insert_after_index = read_standard_source_binding(tokens, prefix.end_index + 1)
                    if binding ~= nil then
                        binding.insert_after_index = insert_after_index
                        local alias_key = normalize_identifier_value(binding.alias_name or binding.table_name)
                        if alias_key ~= nil then
                            lookup[alias_key] = binding
                        end
                        local table_key = normalize_identifier_value(binding.table_name)
                        if table_key ~= nil and lookup[table_key] == nil then
                            lookup[table_key] = binding
                        end
                    end
                end
            end
            index = index + 1
        end
        return lookup
    end

    local function query_has_top_level_user_join(sqltext)
        local tokens = tokenize_path_sql(sqltext)
        local depth = 0
        local index = 1
        while index <= #tokens do
            local token = tokens[index]
            if token.type == "punct" and token.text == "(" then
                depth = depth + 1
            elseif token.type == "punct" and token.text == ")" then
                depth = depth - 1
            elseif depth == 0 then
                local prefix = read_join_prefix_at(tokens, index)
                if prefix ~= nil and prefix.kind ~= "from" then
                    local binding, _ = read_standard_source_binding(tokens, prefix.end_index + 1)
                    if binding == nil then
                        return true
                    end
                    local alias_name = normalize_identifier_value(binding.alias_name or "")
                    if string.sub(alias_name, 1, 6) ~= "__JVS_" then
                        return true
                    end
                end
            end
            index = index + 1
        end
        return false
    end

    local function collect_to_json_display_names(root_config)
        local names = {}
        local seen = {}
        local display_lookup = root_config and root_config.displayNameByArgumentName or nil
        if display_lookup == nil then
            return names
        end
        for _, display_name in pairs(display_lookup) do
            local normalized_name = normalize_identifier_value(display_name)
            if normalized_name ~= nil and not seen[normalized_name] then
                seen[normalized_name] = true
                names[#names + 1] = display_name
            end
        end
        table.sort(names, function(left, right)
            return normalize_identifier_value(left) < normalize_identifier_value(right)
        end)
        return names
    end

    local REGULAR_TO_JSON_COLUMNS_BY_SCHEMA_AND_TABLE = {}
    local REGULAR_TO_JSON_COLUMN_LOOKUP_BY_SCHEMA_AND_TABLE = {}
    local TO_JSON_CURRENT_SCHEMA_NAME = nil

    local function current_schema_name_for_to_json()
        if TO_JSON_CURRENT_SCHEMA_NAME ~= nil then
            return TO_JSON_CURRENT_SCHEMA_NAME
        end
        local ok, rows = pquery([[SELECT CURRENT_SCHEMA FROM DUAL]])
        if not ok or rows == nil or rows[1] == nil or rows[1][1] == nil then
            raise_function_error(
                "TO_JSON",
                "Could not resolve the current schema for an unqualified table reference."
            )
        end
        TO_JSON_CURRENT_SCHEMA_NAME = normalize_identifier_value(rows[1][1])
        return TO_JSON_CURRENT_SCHEMA_NAME
    end

    local function schema_name_for_to_json_table_reference(table_reference)
        if table_reference == nil then
            return nil
        end
        if table_reference.schema_name ~= nil then
            return normalize_identifier_value(table_reference.schema_name)
        end
        return current_schema_name_for_to_json()
    end

    local function load_regular_to_json_columns(table_reference)
        if table_reference == nil or table_reference.table_name == nil then
            raise_function_error("TO_JSON", "Could not resolve the selected row source.")
        end
        local schema_name = schema_name_for_to_json_table_reference(table_reference)
        local table_name = normalize_identifier_value(table_reference.table_name)
        if schema_name == nil or table_name == nil then
            raise_function_error("TO_JSON", "Could not resolve the selected row source.")
        end

        local schema_columns = REGULAR_TO_JSON_COLUMNS_BY_SCHEMA_AND_TABLE[schema_name]
        if schema_columns == nil then
            schema_columns = {}
            REGULAR_TO_JSON_COLUMNS_BY_SCHEMA_AND_TABLE[schema_name] = schema_columns
        end
        local schema_lookup = REGULAR_TO_JSON_COLUMN_LOOKUP_BY_SCHEMA_AND_TABLE[schema_name]
        if schema_lookup == nil then
            schema_lookup = {}
            REGULAR_TO_JSON_COLUMN_LOOKUP_BY_SCHEMA_AND_TABLE[schema_name] = schema_lookup
        end
        if schema_columns[table_name] ~= nil and schema_lookup[table_name] ~= nil then
            return schema_columns[table_name], schema_lookup[table_name]
        end

        local ok, rows = pquery(
            [[
SELECT COLUMN_NAME
FROM SYS.EXA_ALL_COLUMNS
WHERE UPPER(COLUMN_SCHEMA) = :schema_name
  AND UPPER(COLUMN_TABLE) = :table_name
ORDER BY COLUMN_ORDINAL_POSITION
]],
            {
                schema_name = schema_name,
                table_name = table_name
            }
        )
        if not ok then
            raise_function_error(
                "TO_JSON",
                'Could not load column metadata for "' .. schema_name .. '"."' .. table_name .. '".'
            )
        end

        local column_names = {}
        local lookup = {}
        for _, row in ipairs(rows or {}) do
            local column_name = row[1]
            if column_name ~= nil then
                column_names[#column_names + 1] = column_name
                lookup[normalize_identifier_value(column_name)] = column_name
            end
        end
        schema_columns[table_name] = column_names
        schema_lookup[table_name] = lookup
        return column_names, lookup
    end

    local function collect_regular_to_json_display_names(table_reference)
        local column_names = load_regular_to_json_columns(table_reference)
        local names = {}
        for _, column_name in ipairs(column_names) do
            names[#names + 1] = column_name
        end
        return names
    end

    local function regular_to_json_star_exposes_contract_columns(column_names)
        local saw_structural = false
        local saw_contract_marker = false
        for _, column_name in ipairs(column_names or {}) do
            local normalized = normalize_identifier_value(column_name)
            if normalized == "_ID" or normalized == "_PARENT" or normalized == "_POS" then
                saw_structural = true
            end
            if string.find(column_name, "|", 1, true) ~= nil then
                saw_contract_marker = true
            end
        end
        return saw_structural and saw_contract_marker
    end

    local function resolve_regular_to_json_column_name(table_reference, member_name)
        local accepted_names, lookup = load_regular_to_json_columns(table_reference)
        local actual_name = lookup[normalize_identifier_value(member_name)]
        if actual_name ~= nil then
            return actual_name
        end
        local accepted_sql = #accepted_names > 0 and table.concat(accepted_names, ", ") or "(none)"
        raise_function_error(
            "TO_JSON",
            'TO_JSON subset arguments must be visible top-level columns on the current row source. Accepted names: '
                    .. accepted_sql .. "."
        )
    end

    local function same_table_reference(left, right)
        if left == nil or right == nil then
            return false
        end
        return normalize_identifier_value(left.schema_name) == normalize_identifier_value(right.schema_name)
                and normalize_identifier_value(left.table_name) == normalize_identifier_value(right.table_name)
                and normalize_identifier_value(left.alias_name or left.table_name)
                    == normalize_identifier_value(right.alias_name or right.table_name)
    end

    local function table_reference_sql(table_reference)
        if table_reference.reference_sql ~= nil then
            return table_reference.reference_sql
        end
        if table_reference.alias_name ~= nil and string.sub(table_reference.alias_name, 1, 6) == "__jvs_" then
            return encode_quoted_identifier(table_reference.alias_name)
        end
        return render_bound_identifier(table_reference.alias_name or table_reference.table_name)
    end

    local function add_projection_column(column_names, seen_columns, column_name)
        if column_name == nil then
            return
        end
        local key = normalize_identifier_value(column_name)
        if key == nil or seen_columns[key] then
            return
        end
        seen_columns[key] = true
        column_names[#column_names + 1] = column_name
    end

    local function collect_group_projection_columns(group_config, include_variant_columns, include_null_mask, scalar_only)
        local column_names = {}
        local seen_columns = {}
        local variant_columns = group_config and group_config.variantColumns or nil
        if include_variant_columns and variant_columns ~= nil then
            for _, label in ipairs(VARIANT_LABEL_ORDER) do
                if not scalar_only or label == "NUMBER" or label == "STRING" or label == "BOOLEAN" then
                    add_projection_column(column_names, seen_columns, variant_columns[label])
                end
            end
        end
        if include_null_mask then
            add_projection_column(column_names, seen_columns, group_config and group_config.nullMaskName or nil)
        end
        return column_names
    end

    local function build_helper_reference(reference_sql, projected_alias_by_name)
        return {
            reference_sql = reference_sql,
            projected_alias_by_name = projected_alias_by_name or {}
        }
    end

    local function helper_reference_table_sql(reference)
        if type(reference) == "table" then
            return reference.reference_sql
        end
        return reference
    end

    local function projected_column_name(reference, column_name)
        if type(reference) == "table" and reference.projected_alias_by_name ~= nil then
            local projected = reference.projected_alias_by_name[normalize_identifier_value(column_name)]
            if projected ~= nil then
                return projected
            end
        end
        return column_name
    end

    local function build_boolean_from_mask(reference, null_mask_name)
        if null_mask_name == nil then
            return "FALSE"
        end
        return "(CASE WHEN " .. helper_reference_table_sql(reference) .. "."
                .. encode_quoted_identifier(projected_column_name(reference, null_mask_name))
                .. " = TRUE THEN TRUE ELSE FALSE END)"
    end

    local function render_column_reference(reference, column_name)
        return helper_reference_table_sql(reference) .. "." .. encode_quoted_identifier(
                projected_column_name(reference, column_name)
        )
    end

    local function build_helper_root_projection_join(table_reference, column_names, join_state)
        if column_names == nil or #column_names == 0 then
            return build_helper_reference(table_reference_sql(table_reference))
        end

        local key_parts = {normalize_identifier_value(table_reference.alias_name or table_reference.table_name)}
        for _, column_name in ipairs(column_names) do
            key_parts[#key_parts + 1] = normalize_identifier_value(column_name)
        end
        local join_key = table.concat(key_parts, "|")
        local existing = join_state.alias_by_key[join_key]
        if existing ~= nil then
            return existing
        end

        local alias_name = "__jvs_null_" .. tostring(join_state.next_alias_id)
        join_state.next_alias_id = join_state.next_alias_id + 1
        local alias_ref = encode_quoted_identifier(alias_name)
        local projected_alias_by_name = {}
        local row_id_alias = "__jvs_row_id"
        local helper_reference = build_helper_reference(alias_ref, projected_alias_by_name)
        join_state.alias_by_key[join_key] = helper_reference

        local helper_table_sql = {}
        if table_reference.catalog_name ~= nil then
            helper_table_sql[#helper_table_sql + 1] = encode_quoted_identifier(table_reference.catalog_name)
        end
        helper_table_sql[#helper_table_sql + 1] = encode_quoted_identifier(
            helper_schema_name_for_table_reference(table_reference) or table_reference.schema_name
        )
        helper_table_sql[#helper_table_sql + 1] = encode_quoted_identifier(table_reference.table_name)

        local projected_columns = {
            encode_quoted_identifier("_id") .. " AS " .. encode_quoted_identifier(row_id_alias)
        }
        for column_index, column_name in ipairs(column_names) do
            local projected_alias = "__jvs_col_" .. tostring(column_index)
            projected_alias_by_name[normalize_identifier_value(column_name)] = projected_alias
            projected_columns[#projected_columns + 1] = encode_quoted_identifier(column_name)
                    .. " AS " .. encode_quoted_identifier(projected_alias)
        end

        join_state.join_sql_parts[#join_state.join_sql_parts + 1] =
                " LEFT OUTER JOIN (SELECT "
                .. table.concat(projected_columns, ", ")
                .. " FROM " .. table.concat(helper_table_sql, ".") .. ") "
                .. alias_ref
                .. " ON (" .. table_reference_sql(table_reference) .. "." .. encode_quoted_identifier("_id")
                .. " = " .. alias_ref .. "." .. encode_quoted_identifier(row_id_alias) .. ")"
        return helper_reference
    end

    local function resolve_wrapper_helper_argument(
            function_name,
            original_sqltext,
            tokens,
            opening_paren,
            closing_paren,
            base_table,
            table_reference_lookup,
            join_state
    )
        local identifier_parts = read_simple_identifier_argument(tokens, opening_paren, closing_paren)
        if identifier_parts == nil then
            raise_function_error(
                function_name,
                'Expected a JSON property reference such as "note", root_alias."note", or "path.to.note".'
            )
        end

        local visible_name = identifier_parts[#identifier_parts]
        local table_reference = nil
        if #identifier_parts == 1 then
            local binding = table_reference_lookup[visible_name]
            if binding ~= nil then
                if binding.kind == "iterator_value" then
                    raise_function_error(
                        function_name,
                        'JSON helper functions are not supported on VALUE iterators yet. '
                                .. 'Use plain SQL on the scalar iterator value or iterate an object array instead.'
                    )
                elseif binding.kind == "iterator_row" then
                    raise_function_error(
                        function_name,
                        'Expected a JSON property reference such as item."value", not the iterator alias by itself.'
                    )
                end
            end
            if query_has_top_level_user_join(original_sqltext) then
                raise_function_error(
                    function_name,
                    'Unqualified helper arguments are not supported in joined queries. Qualify the JSON property reference, for example JSON_IS_EXPLICIT_NULL(root_alias."note").'
                )
            end
            table_reference = base_table
            if table_reference == nil then
                raise_function_error(
                    function_name,
                    "JSON helper functions currently require a query with a single base JSON table in FROM."
                )
            end
        else
            local qualifier_name = identifier_parts[#identifier_parts - 1]
            table_reference = table_reference_lookup[qualifier_name]
            if table_reference == nil then
                raise_function_error(
                    function_name,
                    "Could not resolve helper argument to a JSON property reference in the current query block."
                )
            end
            if table_reference.kind == "derived_source" then
                raise_function_error(
                    function_name,
                    'JSON helper functions do not resolve through derived-table aliases yet. '
                            .. 'Move the helper call into the inner SELECT or query the wrapper view directly.'
                )
            end
            if table_reference.kind == "iterator_value" then
                raise_function_error(
                    function_name,
                    'JSON helper functions are not supported on VALUE iterators yet. '
                            .. 'Use plain SQL on the scalar iterator value or iterate an object array instead.'
                )
            end
        end

        local group_config = lookup_group_config(table_reference.schema_name, table_reference.table_name, visible_name)
        if group_config == nil then
            raise_function_error(
                function_name,
                "Helper arguments must resolve to a JSON property on the wrapper surface."
            )
        end

        return table_reference, group_config, visible_name
    end

    local function helper_reference_sql(table_reference, group_config, join_state, include_variant_columns, include_null_mask, scalar_only)
        local table_schema_name = normalize_identifier_value(table_reference.schema_name)
        local helper_schema_name = normalize_identifier_value(helper_schema_name_for_table_reference(table_reference))
        if table_schema_name ~= helper_schema_name then
            return build_helper_root_projection_join(
                table_reference,
                collect_group_projection_columns(group_config, include_variant_columns, include_null_mask, scalar_only),
                join_state
            )
        end
        return build_helper_reference(table_reference_sql(table_reference))
    end

    local function build_wrapper_explicit_null_replacement(table_reference, group_config, join_state)
        local null_mask_name = group_config.nullMaskName
        if null_mask_name == nil then
            return "FALSE"
        end

        local reference_sql = helper_reference_sql(table_reference, group_config, join_state, false, true, false)
        return build_boolean_from_mask(reference_sql, null_mask_name)
    end

    local function build_wrapper_variant_typeof_replacement(table_reference, group_config, join_state)
        local variant_columns = group_config.variantColumns or {}
        local reference_sql = helper_reference_sql(table_reference, group_config, join_state, true, true, false)
        local out = {"(CASE"}
        for _, label in ipairs(VARIANT_LABEL_ORDER) do
            local column_name = variant_columns[label]
            if column_name ~= nil then
                out[#out + 1] = " WHEN " .. render_column_reference(reference_sql, column_name)
                        .. " IS NOT NULL THEN " .. encode_string_literal(label)
            end
        end
        if group_config.nullMaskName ~= nil then
            out[#out + 1] = " WHEN " .. build_boolean_from_mask(reference_sql, group_config.nullMaskName)
                    .. " THEN " .. encode_string_literal("NULL")
        end
        out[#out + 1] = " ELSE NULL END)"
        return table.concat(out)
    end

    local function build_wrapper_variant_cast_replacement(table_reference, group_config, join_state, cast_target_sql)
        local variant_columns = group_config.variantColumns or {}
        local reference_sql = helper_reference_sql(table_reference, group_config, join_state, true, false, true)
        local out = {"(CASE"}
        for _, label in ipairs(VARIANT_LABEL_ORDER) do
            if label == "NUMBER" or label == "STRING" or label == "BOOLEAN" then
                local column_name = variant_columns[label]
                if column_name ~= nil then
                    local column_ref = render_column_reference(reference_sql, column_name)
                    out[#out + 1] = " WHEN " .. column_ref
                            .. " IS NOT NULL THEN CAST(" .. column_ref .. " AS " .. cast_target_sql .. ")"
                end
            end
        end
        out[#out + 1] = " ELSE NULL END)"
        return table.concat(out)
    end

    local function build_to_json_projection_join(table_reference, root_config, column_names, to_json_state)
        if column_names == nil or #column_names == 0 then
            raise_function_error("TO_JSON", "Internal error: missing export projection columns.")
        end
        local row_key_column = root_config.rowKeyColumn
        local row_key_source_columns = root_config.rowKeySourceColumns or {}
        if row_key_column == nil or row_key_column == "" or #row_key_source_columns == 0 then
            raise_function_error("TO_JSON", "Internal error: missing export row-key metadata.")
        end

        local join_key = normalize_identifier_value(table_reference.alias_name or table_reference.table_name)
                .. "|" .. normalize_identifier_value(root_config.exportViewQualified)
        for _, column_name in ipairs(column_names) do
            join_key = join_key .. "|" .. normalize_identifier_value(column_name)
        end
        local existing = to_json_state.alias_by_key[join_key]
        if existing ~= nil then
            return existing
        end

        local alias_name = "__jvs_to_json_" .. tostring(to_json_state.next_alias_id)
        to_json_state.next_alias_id = to_json_state.next_alias_id + 1
        local alias_ref = encode_quoted_identifier(alias_name)
        local projected_alias_by_name = {}
        local row_key_alias = "__jvs_row_key"
        local helper_reference = build_helper_reference(alias_ref, projected_alias_by_name)
        to_json_state.alias_by_key[join_key] = helper_reference
        local projected_columns = {
            encode_quoted_identifier(row_key_column) .. " AS " .. encode_quoted_identifier(row_key_alias)
        }
        for column_index, column_name in ipairs(column_names) do
            local projected_alias = "__jvs_col_" .. tostring(column_index)
            projected_alias_by_name[normalize_identifier_value(column_name)] = projected_alias
            projected_columns[#projected_columns + 1] = encode_quoted_identifier(column_name)
                    .. " AS " .. encode_quoted_identifier(projected_alias)
        end
        local row_key_expr_parts = {}
        for _, source_column_name in ipairs(row_key_source_columns) do
            row_key_expr_parts[#row_key_expr_parts + 1] =
                    encode_string_literal(source_column_name .. "=")
                    .. " || CAST("
                    .. table_reference_sql(table_reference) .. "." .. encode_quoted_identifier(source_column_name)
                    .. " AS VARCHAR(2000000))"
        end
        local row_key_expr = table.concat(row_key_expr_parts, " || '|' || ")
        add_join_insertion(
            to_json_state.join_insertions,
            table_reference.insert_after_index,
            " LEFT OUTER JOIN (SELECT " .. table.concat(projected_columns, ", ")
                    .. " FROM " .. root_config.exportViewQualified .. ") " .. alias_ref
                    .. " ON (" .. row_key_expr .. " = "
                    .. alias_ref .. "." .. encode_quoted_identifier(row_key_alias) .. ")"
        )
        return helper_reference
    end

    local function build_regular_to_json_object_call(table_reference, column_names)
        if REGULAR_TO_JSON_ROW_OBJECT_FUNCTION == nil or REGULAR_TO_JSON_ROW_OBJECT_FUNCTION == "" then
            raise_function_error("TO_JSON", "Internal error: missing regular-row JSON serializer.")
        end
        local arguments = {}
        for _, column_name in ipairs(column_names) do
            arguments[#arguments + 1] = encode_string_literal(column_name)
            arguments[#arguments + 1] = table_reference_sql(table_reference)
                    .. "." .. encode_quoted_identifier(column_name)
        end
        return REGULAR_TO_JSON_ROW_OBJECT_FUNCTION .. "(" .. table.concat(arguments, ", ") .. ")"
    end

    local function read_to_json_star_target(tokens, start_index, end_index)
        if argument_range_is_star(tokens, start_index, end_index) then
            return {
                kind = "plain_star"
            }
        end
        if tokens[end_index] ~= "*" then
            return nil
        end
        local dot_index = previous_significant_raw(tokens, end_index - 1)
        if dot_index == nil or tokens[dot_index] ~= "." then
            return nil
        end
        local qualifier_parts = read_simple_identifier_argument_in_range(tokens, start_index, dot_index - 1)
        if qualifier_parts == nil or #qualifier_parts ~= 1 then
            return nil
        end
        return {
            kind = "qualified_star",
            qualifier_name = qualifier_parts[1]
        }
    end

    local function resolve_to_json_star_table_reference(
            function_name,
            original_sqltext,
            tokens,
            argument_range,
            base_table,
            table_reference_lookup
    )
        local star_target = read_to_json_star_target(tokens, argument_range.start_index, argument_range.end_index)
        if star_target == nil then
            return nil
        end
        if star_target.kind == "plain_star" then
            if query_has_top_level_user_join(original_sqltext) then
                raise_function_error(
                    function_name,
                    'TO_JSON(*) is not supported in joined queries. Use TO_JSON(root_alias.*) for ordinary tables or TO_JSON(root_alias."id", root_alias."meta", ...) instead.'
                )
            end
            if base_table == nil then
                local path_base_binding = read_base_source_binding_from_path_tokens(tokenize_path_sql(original_sqltext))
                if path_base_binding ~= nil and path_base_binding.kind == "derived_source" then
                    raise_function_error(
                        function_name,
                        'TO_JSON does not resolve through derived-table aliases yet. Move the call into the inner SELECT or query the base table directly.'
                    )
                end
                raise_function_error(
                    function_name,
                    "TO_JSON(*) currently requires a query with a single base source in FROM."
                )
            end
            if base_table.kind == "derived_source" then
                raise_function_error(
                    function_name,
                    'TO_JSON does not resolve through derived-table aliases yet. Move the call into the inner SELECT or query the base table directly.'
                )
            end
            if base_table.kind == "iterator_value" then
                raise_function_error(
                    function_name,
                    'TO_JSON is not supported on VALUE iterators yet. Use plain SQL on the scalar iterator value instead.'
                )
            end
            return base_table
        end

        local table_reference = table_reference_lookup[star_target.qualifier_name]
        if table_reference == nil then
            raise_function_error(
                function_name,
                'Could not resolve the TO_JSON star qualifier "' .. star_target.qualifier_name .. '" in the current query block.'
            )
        end
        local normalized_alias_name = normalize_identifier_value(table_reference.alias_name or "")
        if string.sub(normalized_alias_name, 1, 6) == "__JVS_" then
            raise_function_error(
                function_name,
                "TO_JSON(alias.*) must qualify a visible base table or wrapper root alias."
            )
        end
        if table_reference.kind == "derived_source" then
            raise_function_error(
                function_name,
                'TO_JSON does not resolve through derived-table aliases yet. Move the call into the inner SELECT or query the base table directly.'
            )
        end
        if table_reference.kind == "iterator_value" then
            raise_function_error(
                function_name,
                'TO_JSON is not supported on VALUE iterators yet. Use plain SQL on the scalar iterator value instead.'
            )
        end
        return table_reference
    end

    local function resolve_to_json_table_reference(
            function_name,
            original_sqltext,
            base_table,
            table_reference_lookup,
            identifier_parts
    )
        if identifier_parts == nil or #identifier_parts == 0 then
            raise_function_error(
                function_name,
                'Expected TO_JSON(*) or one or more top-level property references such as "id", "meta", or root_alias."meta".'
            )
        end

        local member_name = identifier_parts[#identifier_parts]
        if string.find(member_name, ".", 1, true) ~= nil or string.find(member_name, "[", 1, true) ~= nil then
            raise_function_error(
                function_name,
                'TO_JSON subset arguments must be visible top-level properties. Nested paths such as "meta.info.note" and bracket expressions such as "tags[SIZE]" are not supported.'
            )
        end

        local table_reference = nil
        if #identifier_parts == 1 then
            if query_has_top_level_user_join(original_sqltext) then
                raise_function_error(
                    function_name,
                    'Unqualified TO_JSON arguments are not supported in joined queries. Qualify each property, for example TO_JSON(root_alias."id", root_alias."meta") or TO_JSON(root_alias.*).'
                )
            end
            if base_table == nil then
                local path_base_binding = read_base_source_binding_from_path_tokens(tokenize_path_sql(original_sqltext))
                if path_base_binding ~= nil and path_base_binding.kind == "derived_source" then
                    raise_function_error(
                        function_name,
                        'TO_JSON does not resolve through derived-table aliases yet. Move the call into the inner SELECT or query the base table directly.'
                    )
                end
                raise_function_error(
                    function_name,
                    "TO_JSON currently requires a query with a single base source in FROM."
                )
            end
            if base_table.kind == "derived_source" then
                raise_function_error(
                    function_name,
                    'TO_JSON does not resolve through derived-table aliases yet. Move the call into the inner SELECT or query the base table directly.'
                )
            end
            if base_table.kind == "iterator_value" then
                raise_function_error(
                    function_name,
                    'TO_JSON is not supported on VALUE iterators yet. Use plain SQL on the scalar iterator value instead.'
                )
            end
            table_reference = base_table
        elseif #identifier_parts == 2 then
            local qualifier_name = identifier_parts[1]
            table_reference = table_reference_lookup[qualifier_name]
            if table_reference == nil then
                raise_function_error(
                    function_name,
                    'Could not resolve the TO_JSON argument qualifier "' .. qualifier_name .. '" in the current query block.'
                )
            end
            local normalized_alias_name = normalize_identifier_value(table_reference.alias_name or "")
            if string.sub(normalized_alias_name, 1, 6) == "__JVS_" then
                raise_function_error(
                    function_name,
                    'TO_JSON subset arguments must be visible top-level properties. Nested paths such as "meta.info.note" and bracket expressions such as "tags[SIZE]" are not supported.'
                )
            end
            if table_reference.kind == "derived_source" then
                raise_function_error(
                    function_name,
                    'TO_JSON does not resolve through derived-table aliases yet. Move the call into the inner SELECT or query the wrapper view directly.'
                )
            end
            if table_reference.kind == "iterator_value" then
                raise_function_error(
                    function_name,
                    'TO_JSON is not supported on VALUE iterators yet. Use plain SQL on the scalar iterator value instead.'
                )
            end
        else
            raise_function_error(
                function_name,
                'TO_JSON subset arguments must be top-level property references, optionally qualified as root_alias."property".'
            )
        end

        local root_config = lookup_to_json_root_config(table_reference.schema_name, table_reference.table_name)
        return table_reference, root_config, member_name
    end

    local function build_to_json_star_replacement(
            function_name,
            original_sqltext,
            tokens,
            argument_range,
            base_table,
            table_reference_lookup,
            to_json_state
    )
        local target_table_reference = resolve_to_json_star_table_reference(
            function_name,
            original_sqltext,
            tokens,
            argument_range,
            base_table,
            table_reference_lookup
        )
        local root_config = lookup_to_json_root_config(
            target_table_reference.schema_name,
            target_table_reference.table_name
        )
        if root_config == nil then
            local column_names = collect_regular_to_json_display_names(target_table_reference)
            if regular_to_json_star_exposes_contract_columns(column_names) then
                raise_function_error(
                    function_name,
                    "TO_JSON(*) on source-family tables would expose internal contract columns. Query the wrapper view instead."
                )
            end
            return build_regular_to_json_object_call(target_table_reference, column_names)
        end
        local export_ref = build_to_json_projection_join(
            target_table_reference,
            root_config,
            {root_config.fullJsonColumn},
            to_json_state
        )
        return render_column_reference(export_ref, root_config.fullJsonColumn)
    end

    local function build_to_json_subset_replacement(
            function_name,
            original_sqltext,
            tokens,
            opening_paren,
            closing_paren,
            base_table,
            table_reference_lookup,
            to_json_state
    )
        local argument_ranges, split_error = split_call_argument_ranges(tokens, opening_paren, closing_paren)
        if argument_ranges == nil then
            raise_function_error(function_name, split_error)
        end
        if #argument_ranges == 0 then
            raise_function_error(
                function_name,
                'Expected TO_JSON(*) or one or more top-level property references such as "id", "meta", or root_alias."meta".'
            )
        end

        local target_table_reference = nil
        local target_root_config = nil
        local target_regular_source = false
        local selected_column_names = {}
        local seen_selected_columns = {}

        for _, argument_range in ipairs(argument_ranges) do
            if read_to_json_star_target(tokens, argument_range.start_index, argument_range.end_index) ~= nil then
                raise_function_error(
                    function_name,
                    'TO_JSON(*) cannot be mixed with explicit properties. Use either TO_JSON(*) or TO_JSON(col1, col2, ...).'
                )
            end

            local identifier_parts = read_simple_identifier_argument_in_range(
                tokens,
                argument_range.start_index,
                argument_range.end_index
            )
            if identifier_parts == nil then
                local invalid_argument_message = to_json_invalid_argument_message(
                    tokens,
                    argument_range.start_index,
                    argument_range.end_index
                )
                if invalid_argument_message ~= nil then
                    raise_function_error(function_name, invalid_argument_message)
                end
            end
            local table_reference, root_config, member_name = resolve_to_json_table_reference(
                function_name,
                original_sqltext,
                base_table,
                table_reference_lookup,
                identifier_parts
            )

            if target_table_reference == nil then
                target_table_reference = table_reference
                target_root_config = root_config
            elseif not same_table_reference(target_table_reference, table_reference) then
                raise_function_error(
                    function_name,
                    "All TO_JSON subset arguments must resolve to the same row source."
                )
            end

            if root_config ~= nil then
                local fragment_lookup = root_config.fragmentColumnByArgumentName or {}
                local fragment_column_name = fragment_lookup[normalize_identifier_value(member_name)]
                if fragment_column_name == nil then
                    local accepted_names = collect_to_json_display_names(root_config)
                    local accepted_sql = #accepted_names > 0 and table.concat(accepted_names, ", ") or "(none)"
                    raise_function_error(
                        function_name,
                        'TO_JSON subset arguments must be visible top-level properties on the current root. Accepted names: '
                                .. accepted_sql .. "."
                    )
                end
                local normalized_fragment = normalize_identifier_value(fragment_column_name)
                if not seen_selected_columns[normalized_fragment] then
                    seen_selected_columns[normalized_fragment] = true
                    selected_column_names[#selected_column_names + 1] = fragment_column_name
                end
            else
                target_regular_source = true
                local actual_column_name = resolve_regular_to_json_column_name(table_reference, member_name)
                local normalized_column = normalize_identifier_value(actual_column_name)
                if not seen_selected_columns[normalized_column] then
                    seen_selected_columns[normalized_column] = true
                    selected_column_names[#selected_column_names + 1] = actual_column_name
                end
            end
        end

        if target_regular_source then
            return build_regular_to_json_object_call(target_table_reference, selected_column_names)
        end

        local export_ref = build_to_json_projection_join(
            target_table_reference,
            target_root_config,
            selected_column_names,
            to_json_state
        )
        local qualified_fragments = {}
        for _, fragment_column_name in ipairs(selected_column_names) do
            qualified_fragments[#qualified_fragments + 1] = render_column_reference(export_ref, fragment_column_name)
        end
        return target_root_config.optionalFragmentsFunction .. "(" .. table.concat(qualified_fragments, ", ") .. ")"
    end

    local function build_to_json_replacement(
            function_name,
            original_sqltext,
            tokens,
            opening_paren,
            closing_paren,
            base_table,
            table_reference_lookup,
            to_json_state
    )
        if not has_expression_argument(tokens, opening_paren, closing_paren) then
            raise_function_error(
                function_name,
                'Expected TO_JSON(*) or one or more top-level property references such as "id", "meta", or root_alias."meta".'
            )
        end

        local argument_ranges, split_error = split_call_argument_ranges(tokens, opening_paren, closing_paren)
        if argument_ranges == nil then
            raise_function_error(function_name, split_error)
        end
        if #argument_ranges == 1
                and read_to_json_star_target(tokens, argument_ranges[1].start_index, argument_ranges[1].end_index) ~= nil then
            return build_to_json_star_replacement(
                function_name,
                original_sqltext,
                tokens,
                argument_ranges[1],
                base_table,
                table_reference_lookup,
                to_json_state
            )
        end

        return build_to_json_subset_replacement(
            function_name,
            original_sqltext,
            tokens,
            opening_paren,
            closing_paren,
            base_table,
            table_reference_lookup,
            to_json_state
        )
    end

    local function collect_helper_call_replacements(original_sqltext, tokens, base_table)
        local replacements = {}
        local table_reference_lookup = collect_helper_table_reference_lookup_from_sql(original_sqltext)
        local raw_table_reference_lookup = collect_helper_table_reference_lookup(tokens)
        for key, raw_reference in pairs(raw_table_reference_lookup) do
            local existing_reference = table_reference_lookup[key]
            if existing_reference ~= nil then
                existing_reference.insert_after_index = raw_reference.insert_after_index
            else
                table_reference_lookup[key] = raw_reference
            end
        end
        local join_state = {
            next_alias_id = 1,
            alias_by_key = {},
            join_sql_parts = {}
        }
        local to_json_state = {
            next_alias_id = 1,
            alias_by_key = {},
            join_insertions = {}
        }
        local index = 1
        while index <= #tokens do
            local call = read_call(tokens, index)
            local helper_kind = call and HELPER_KIND_BY_NAME[call.last_identifier] or nil
            if call and helper_kind ~= nil then
                local closing_paren, top_level_commas = find_matching_paren(tokens, call.opening_paren)
                if closing_paren == nil then
                    raise_function_error(call.last_identifier, "Missing closing parenthesis.")
                else
                    local replacement_sql = nil
                    if helper_kind == "to_json" then
                        replacement_sql = build_to_json_replacement(
                            call.last_identifier,
                            original_sqltext,
                            tokens,
                            call.opening_paren,
                            closing_paren,
                            base_table,
                            table_reference_lookup,
                            to_json_state
                        )
                    else
                        local helper_allowed, helper_scope_message = helper_query_targets_allowed_schema(original_sqltext)
                        if not helper_allowed then
                            raise_scope_error("JSON helper functions", helper_scope_message)
                        end
                        if top_level_commas ~= 0 then
                            raise_function_error(call.last_identifier, "Expected exactly one argument.")
                        elseif not has_expression_argument(tokens, call.opening_paren, closing_paren) then
                            raise_function_error(call.last_identifier, "Expected exactly one argument.")
                        end
                        local table_reference, group_config = resolve_wrapper_helper_argument(
                            call.last_identifier,
                            original_sqltext,
                            tokens,
                            call.opening_paren,
                            closing_paren,
                            base_table,
                            table_reference_lookup
                        )
                        if helper_kind == "explicit_null" then
                            replacement_sql = build_wrapper_explicit_null_replacement(table_reference, group_config, join_state)
                        elseif helper_kind == "variant_typeof" then
                            replacement_sql = build_wrapper_variant_typeof_replacement(table_reference, group_config, join_state)
                        elseif helper_kind == "variant_as_varchar" then
                            replacement_sql = build_wrapper_variant_cast_replacement(
                                table_reference,
                                group_config,
                                join_state,
                                "VARCHAR(2000000)"
                            )
                        elseif helper_kind == "variant_as_decimal" then
                            replacement_sql = build_wrapper_variant_cast_replacement(
                                table_reference,
                                group_config,
                                join_state,
                                "DECIMAL(36,18)"
                            )
                        elseif helper_kind == "variant_as_boolean" then
                            replacement_sql = build_wrapper_variant_cast_replacement(
                                table_reference,
                                group_config,
                                join_state,
                                "BOOLEAN"
                            )
                        else
                            raise_function_error(call.last_identifier, "Unsupported helper rewrite kind: " .. tostring(helper_kind))
                        end
                    end
                    replacements[index] = {
                        closing_paren = closing_paren,
                        replacement_sql = replacement_sql
                    }
                    index = closing_paren + 1
                end
            else
                index = index + 1
            end
        end
        return replacements, join_state.join_sql_parts, to_json_state.join_insertions
    end

    local function rewrite_helper_query_block_sql(sqltext)
        local tokens = sqlparsing.tokenize(sqltext)
        local base_table = read_base_table_reference(tokens)
        local path_base_table = read_base_table_reference_for_path_tokens(tokenize_path_sql(sqltext))
        if base_table ~= nil and path_base_table ~= nil and path_base_table.reference_sql ~= nil then
            base_table.reference_sql = path_base_table.reference_sql
        end
        local helper_call_replacements, helper_join_sql_parts, to_json_join_insertions =
                collect_helper_call_replacements(sqltext, tokens, base_table)
        local out = {}
        local index = 1
        while index <= #tokens do
            local call = read_call(tokens, index)
            if call and BLOCKED_FUNCTIONS[call.last_identifier] then
                raise_function_error(call.last_identifier, BLOCKED_FUNCTION_MESSAGE)
            end

            local replacement = helper_call_replacements[index]
            if replacement ~= nil then
                out[#out + 1] = replacement.replacement_sql
                index = replacement.closing_paren + 1
            else
                out[#out + 1] = tokens[index]
                if base_table ~= nil and index == base_table.insert_after_index and #helper_join_sql_parts > 0 then
                    out[#out + 1] = table.concat(helper_join_sql_parts)
                end
                local pending_to_json_joins = to_json_join_insertions[index]
                if pending_to_json_joins ~= nil then
                    out[#out + 1] = table.concat(pending_to_json_joins)
                end
                index = index + 1
            end
        end
        return table.concat(out)
    end

    local function rewrite_helper_calls_in_sql(sqltext)
        return rewrite_sql_with_query_blocks(sqltext, rewrite_helper_query_block_sql)
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
    blocked_function_names: list[str],
    blocked_function_message: str,
    allowed_schemas: list[str],
    helper_schema_map: dict[str, str],
    wrapper_group_config: dict[str, dict[str, dict[str, object]]] | None,
    wrapper_visible_column_config: dict[str, dict[str, dict[str, bool]]] | None,
    wrapper_to_json_config: dict[str, dict[str, dict[str, object]]] | None,
    regular_to_json_row_object_function: str | None,
    rewrite_path_identifiers: bool,
    activate_session: bool,
    helper_function_kinds: dict[str, str] | None = None,
    library_script: str = DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT,
) -> str:
    validated_library_script = validate_identifier("Library script", library_script)
    helper_function_kinds = helper_function_kinds or {name: "explicit_null" for name in function_names}
    configured_function_names = list(helper_function_kinds.keys())
    function_list_sql = ", ".join(configured_function_names) if configured_function_names else "(disabled)"
    blocked_function_list_sql = ", ".join(blocked_function_names) if blocked_function_names else "(none)"
    allowed_schema_list_sql = ", ".join(allowed_schemas)
    example_allowed_schema = allowed_schemas[0] if allowed_schemas else "JSON_VIEW"
    helper_schema_comment = (
        ", ".join(f"{public_schema}->{helper_schema}" for public_schema, helper_schema in sorted(helper_schema_map.items()))
        if helper_schema_map
        else "(none)"
    )
    config_lua = render_lua_string_table(
        _build_preprocessor_config(
            function_names=function_names,
            blocked_function_names=blocked_function_names,
            blocked_function_message=blocked_function_message,
            allowed_schemas=allowed_schemas,
            helper_schema_map=helper_schema_map,
            wrapper_group_config=wrapper_group_config,
            wrapper_visible_column_config=wrapper_visible_column_config,
            wrapper_to_json_config=wrapper_to_json_config,
            regular_to_json_row_object_function=regular_to_json_row_object_function,
            rewrite_path_identifiers=rewrite_path_identifiers,
            helper_function_kinds=helper_function_kinds,
        ),
        4,
    )
    has_variant_helpers = any(kind != "explicit_null" for kind in helper_function_kinds.values())
    if wrapper_group_config:
        helper_mode = "wrapper semantic helpers" if has_variant_helpers else "wrapper explicit-null joins"
    else:
        helper_mode = "CASE-marker compatibility helpers"
    path_comment = "enabled (joins)" if rewrite_path_identifiers else "disabled"

    activation_sql = ""
    if activate_session:
        activation_sql = f"\nALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema}.{script};"

    example_sql = f"""-- Example:
-- SELECT
--   CAST("id" AS VARCHAR(10)),
--   CASE WHEN {configured_function_names[0]}("note") THEN '1' ELSE '0' END
-- FROM {example_allowed_schema}.SAMPLE
-- ORDER BY "id";""" if configured_function_names else "-- Example helper functions: disabled in this build."

    return f"""-- Generated by tools/generate_preprocessor_sql.py
-- Rewrites configured helper calls and JSON navigation syntax before SQL compilation.
-- Configured function names: {function_list_sql}
-- Blocked function names: {blocked_function_list_sql}
-- JSON syntax allowed only for configured JSON schemas: {allowed_schema_list_sql}
-- Helper schema mappings: {helper_schema_comment}
-- Helper rewrite mode: {helper_mode}
-- Path identifier rewrite: {path_comment}
-- Imported library script: {schema}.{validated_library_script}

CREATE SCHEMA IF NOT EXISTS {schema};

CREATE OR REPLACE LUA PREPROCESSOR SCRIPT {schema}.{script} AS
    exa.import("{schema}.{validated_library_script}", "JVS_PREPROCESSOR_LIB")
    local CONFIG = {config_lua}
    sqlparsing.setsqltext(JVS_PREPROCESSOR_LIB.rewrite(sqlparsing.getsqltext(), CONFIG))
/
{activation_sql}

-- Enable explicitly with:
-- ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema}.{script};

{example_sql}
"""


def main() -> None:
    args = parse_args()
    schema = validate_identifier("Schema", args.schema)
    script = validate_identifier("Script name", args.script)
    raw_function_names = [] if args.disable_function_helpers else (args.function_names or ["JSON_IS_EXPLICIT_NULL"])
    function_names = [validate_identifier("Function name", value) for value in raw_function_names]
    blocked_function_names = [
        validate_identifier("Blocked function name", value) for value in (args.blocked_function_names or [])
    ]
    overlapping_functions = sorted(set(function_names) & set(blocked_function_names))
    if overlapping_functions:
        raise SystemExit("Function names cannot be both rewritten and blocked: " + ", ".join(overlapping_functions))
    raw_allowed_schemas = args.allowed_schemas or ["JSON_VIEW"]
    allowed_schemas = [validate_identifier("Allowed schema name", value) for value in raw_allowed_schemas]
    helper_schema_map = dict(validate_helper_schema_map(value) for value in (args.helper_schema_maps or []))
    unknown_helper_mappings = sorted(set(helper_schema_map) - set(allowed_schemas))
    if unknown_helper_mappings:
        raise SystemExit(
            "Helper schema mappings may only target configured allowed schemas: "
            + ", ".join(unknown_helper_mappings)
        )
    sql = render_sql(
        schema,
        script,
        function_names,
        blocked_function_names,
        args.blocked_function_message or "This helper is not available in this build.",
        allowed_schemas,
        helper_schema_map,
        None,
        None,
        None,
        None,
        args.rewrite_path_identifiers,
        args.activate_session,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(sql)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
