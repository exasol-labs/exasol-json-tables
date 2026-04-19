#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from .generate_preprocessor_sql import validate_identifier


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "dist" / "exasol-json-tables" / "json_export_helpers.sql"
DEFAULT_SCHEMA = "JVS_JSON_EXPORT"
JSON_QUOTE_STRING_SCRIPT = "JSON_QUOTE_STRING"
JSON_OBJECT_FROM_FRAGMENTS_SCRIPT = "JSON_OBJECT_FROM_FRAGMENTS"
JSON_ARRAY_FROM_JSON_SORTED_SCRIPT = "JSON_ARRAY_FROM_JSON_SORTED"
JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS_SCRIPT = "JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS"
JSON_OBJECT_FROM_NAME_VALUE_PAIRS_SCRIPT = "JSON_OBJECT_FROM_NAME_VALUE_PAIRS"


@dataclass(frozen=True)
class JsonExportHelperNames:
    schema: str
    json_quote_string: str
    json_object_from_fragments: str
    json_array_from_json_sorted: str
    json_object_from_optional_fragments: str
    json_object_from_name_value_pairs: str


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_qualified(schema: str, name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(name)}"


def helper_names(schema: str = DEFAULT_SCHEMA) -> JsonExportHelperNames:
    validated_schema = validate_identifier("Schema", schema)
    return JsonExportHelperNames(
        schema=validated_schema,
        json_quote_string=quote_qualified(validated_schema, JSON_QUOTE_STRING_SCRIPT),
        json_object_from_fragments=quote_qualified(validated_schema, JSON_OBJECT_FROM_FRAGMENTS_SCRIPT),
        json_array_from_json_sorted=quote_qualified(validated_schema, JSON_ARRAY_FROM_JSON_SORTED_SCRIPT),
        json_object_from_optional_fragments=quote_qualified(
            validated_schema,
            JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS_SCRIPT,
        ),
        json_object_from_name_value_pairs=quote_qualified(
            validated_schema,
            JSON_OBJECT_FROM_NAME_VALUE_PAIRS_SCRIPT,
        ),
    )


def generate_json_export_helper_statements(schema: str = DEFAULT_SCHEMA) -> list[str]:
    names = helper_names(schema)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {names.schema}",
        f"""
CREATE OR REPLACE LUA SCALAR SCRIPT {names.schema}.{JSON_QUOTE_STRING_SCRIPT}(v VARCHAR(2000000))
RETURNS VARCHAR(2000000) AS
local cjson = require("cjson")
function run(ctx)
    if ctx[1] == nil or ctx[1] == null then
        return "null"
    end
    return cjson.encode(ctx[1])
end
/
""".strip(),
        f"""
CREATE OR REPLACE LUA SET SCRIPT {names.schema}.{JSON_OBJECT_FROM_FRAGMENTS_SCRIPT}(
    ord DECIMAL(18,0),
    frag VARCHAR(2000000)
) RETURNS VARCHAR(2000000) AS
local function isnull(v)
    return v == nil or v == null
end
function run(ctx)
    local parts = {{}}
    repeat
        if not isnull(ctx[2]) then
            local pos = 0
            if not isnull(ctx[1]) then
                pos = tonumber(tostring(ctx[1]))
            end
            parts[#parts + 1] = {{ pos = pos, frag = ctx[2] }}
        end
    until not ctx.next()
    if #parts == 0 then
        return "{{}}"
    end
    table.sort(parts, function(a, b)
        if a.pos == b.pos then
            return a.frag < b.frag
        end
        return a.pos < b.pos
    end)
    local out = {{}}
    for i, item in ipairs(parts) do
        out[i] = item.frag
    end
    return "{{" .. table.concat(out, ",") .. "}}"
end
/
""".strip(),
        f"""
CREATE OR REPLACE LUA SET SCRIPT {names.schema}.{JSON_ARRAY_FROM_JSON_SORTED_SCRIPT}(
    pos DECIMAL(18,0),
    child_json VARCHAR(2000000)
) RETURNS VARCHAR(2000000) AS
local function isnull(v)
    return v == nil or v == null
end
function run(ctx)
    local parts = {{}}
    repeat
        if not isnull(ctx[2]) then
            local pos = 0
            if not isnull(ctx[1]) then
                pos = tonumber(tostring(ctx[1]))
            end
            parts[#parts + 1] = {{ pos = pos, frag = ctx[2] }}
        end
    until not ctx.next()
    if #parts == 0 then
        return "[]"
    end
    table.sort(parts, function(a, b)
        if a.pos == b.pos then
            return a.frag < b.frag
        end
        return a.pos < b.pos
    end)
    local out = {{}}
    for i, item in ipairs(parts) do
        out[i] = item.frag
    end
    return "[" .. table.concat(out, ",") .. "]"
end
/
""".strip(),
        f"""
CREATE OR REPLACE LUA SCALAR SCRIPT {names.schema}.{JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS_SCRIPT}(...)
RETURNS VARCHAR(2000000) AS
local function isnull(v)
    return v == nil or v == null
end
function run(ctx)
    local out = {{}}
    for i = 1, exa.meta.input_column_count do
        if not isnull(ctx[i]) then
            out[#out + 1] = ctx[i]
        end
    end
    if #out == 0 then
        return "{{}}"
    end
    return "{{" .. table.concat(out, ",") .. "}}"
end
/
""".strip(),
        f"""
CREATE OR REPLACE LUA SCALAR SCRIPT {names.schema}.{JSON_OBJECT_FROM_NAME_VALUE_PAIRS_SCRIPT}(...)
RETURNS VARCHAR(2000000) AS
local cjson = require("cjson")
local function isnull(v)
    return v == nil or v == null
end
local function sql_type_family(sql_type)
    local normalized = string.upper(tostring(sql_type or ""))
    if string.find(normalized, "BOOLEAN", 1, true) ~= nil then
        return "BOOLEAN"
    end
    if string.find(normalized, "DECIMAL", 1, true) ~= nil
            or string.find(normalized, "DOUBLE", 1, true) ~= nil
            or string.find(normalized, "FLOAT", 1, true) ~= nil
            or string.find(normalized, "NUMBER", 1, true) ~= nil then
        return "NUMBER"
    end
    return "STRING"
end
local function render_json_value(value, sql_type)
    if isnull(value) then
        return "null"
    end
    local family = sql_type_family(sql_type)
    if family == "BOOLEAN" then
        if value then
            return "true"
        end
        return "false"
    end
    if family == "NUMBER" then
        return tostring(value)
    end
    return cjson.encode(tostring(value))
end
function run(ctx)
    local cols = exa.meta.input_columns
    local out = {{}}
    local index = 1
    while index <= #cols do
        if index == #cols then
            error("JSON_OBJECT_FROM_NAME_VALUE_PAIRS expects alternating key/value arguments")
        end
        local key = ctx[index]
        if isnull(key) then
            error("JSON_OBJECT_FROM_NAME_VALUE_PAIRS does not allow NULL property names")
        end
        out[#out + 1] = cjson.encode(tostring(key))
                .. ":"
                .. render_json_value(ctx[index + 1], cols[index + 1].sql_type)
        index = index + 2
    end
    if #out == 0 then
        return "{{}}"
    end
    return "{{" .. table.concat(out, ",") .. "}}"
end
/
""".strip(),
    ]


def generate_json_export_helper_sql_text(schema: str = DEFAULT_SCHEMA) -> str:
    statements = generate_json_export_helper_statements(schema)
    return (
        "-- Generated by tools/generate_json_export_helper_sql.py\n\n"
        + statements[0]
        + ";\n\n"
        + "\n\n".join(statements[1:])
        + "\n"
    )


def install_json_export_helpers(con, schema: str = DEFAULT_SCHEMA) -> JsonExportHelperNames:
    statements = generate_json_export_helper_statements(schema)
    con.execute(statements[0])
    for statement in statements[1:]:
        con.execute(statement)
    return helper_names(schema)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate installable Exasol SQL for the generic Lua JSON export helper layer. "
            "This produces the helper scripts validated in the prototype studies."
        )
    )
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema that will own the helper scripts.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output SQL file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sql_text = generate_json_export_helper_sql_text(args.schema)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(sql_text)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
