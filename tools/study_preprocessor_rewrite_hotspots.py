#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import statistics
import time

import _package_bootstrap  # noqa: F401

from exasol_json_tables.generate_json_export_helper_sql import install_json_export_helpers
from exasol_json_tables.generate_json_export_views_sql import install_json_export_views
from exasol_json_tables.generate_preprocessor_library_sql import DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT
from exasol_json_tables.generate_wrapper_preprocessor_sql import generate_wrapper_preprocessor_sql_text
from exasol_json_tables.nano_support import connect, install_source_fixture, install_wrapper_views
from exasol_json_tables.preprocessor_library_builder import (
    LIBRARY_TEMPLATE_PATH,
    RUNTIME_PIPELINE_LUA,
    compact_lua_body,
    iter_preprocessor_library_modules,
)
from exasol_json_tables.wrapper_package_tool import execute_generated_preprocessor_sql


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "dist" / "preprocessor_rewrite_hotspots.json"
SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW_RW_PERF"
HELPER_SCHEMA = "JSON_VIEW_RW_PERF_INTERNAL"
PREPROCESSOR_SCRIPT = "JSON_RW_PERF_PREPROCESSOR"
WARM_REPETITIONS = 20


POST_ARRAY_WALKER_GATE_LUA = """
    local function raw_text_reference_known_helper(raw_sqltext_upper)
        if string.find(raw_sqltext_upper, "TO_JSON", 1, true) ~= nil then
            return true
        end
        for function_name in pairs(HELPER_KIND_BY_NAME) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        for function_name in pairs(BLOCKED_FUNCTIONS) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        return false
    end

    local function quoted_identifier_contains_path_syntax(raw_sqltext)
        local index = 1
        while true do
            local start_index = string.find(raw_sqltext, '"', index, true)
            if start_index == nil then
                return false
            end
            local current = start_index + 1
            while current <= #raw_sqltext do
                local ch = string.sub(raw_sqltext, current, current)
                if ch == '"' then
                    if string.sub(raw_sqltext, current + 1, current + 1) == '"' then
                        current = current + 2
                    else
                        local identifier_text = string.sub(raw_sqltext, start_index + 1, current - 1)
                        if string.find(identifier_text, ".", 1, true) ~= nil
                                or string.find(identifier_text, "[", 1, true) ~= nil then
                            return true
                        end
                        index = current + 1
                        break
                    end
                else
                    current = current + 1
                end
            end
            if current > #raw_sqltext then
                return false
            end
        end
    end

    local function raw_text_might_need_iterator_rewrite(raw_sqltext)
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, ' IN "', 1, true) ~= nil then
            return true
        end
        return string.find(raw_sqltext, '%f[%w]IN%f[%W]%s*[A-Za-z_][A-Za-z0-9_]*%s*%.%s*"', 1) ~= nil
    end

    local function query_might_need_helper_rewrite(raw_sqltext)
        return raw_text_reference_known_helper(string.upper(raw_sqltext))
    end

    local function query_might_need_path_rewrite(raw_sqltext)
        if not REWRITE_PATH_IDENTIFIERS then
            return false
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        return false
    end

    local function query_might_need_shared_walker_rewrite(raw_sqltext)
        return query_might_need_path_rewrite(raw_sqltext) or query_might_need_helper_rewrite(raw_sqltext)
    end

    local function query_might_need_runtime_rewrite(raw_sqltext)
        if query_might_need_helper_rewrite(raw_sqltext) then
            return true
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        if raw_text_might_need_iterator_rewrite(raw_sqltext) then
            return true
        end
        return false
    end

    local function rewrite_path_identifiers_in_sql_dispatch(raw_sqltext)
        if REWRITE_PATH_IDENTIFIERS then
            return rewrite_path_identifiers_in_sql_join_mode(raw_sqltext)
        end
        return rewrite_path_identifiers_in_sql_disabled(raw_sqltext)
    end

    local function rewrite_helper_calls_in_sql_dispatch(raw_sqltext)
        if HELPER_REWRITE_MODE == "wrapper" then
            return rewrite_helper_calls_in_sql_wrapper_mode(raw_sqltext)
        end
        return rewrite_helper_calls_in_sql_marker_mode(raw_sqltext)
    end

    local function rewrite_query_block_pipeline_sql(query_sql)
        local rewritten_sql = query_sql
        if REWRITE_PATH_IDENTIFIERS then
            rewritten_sql = rewrite_path_query_block_sql(rewritten_sql)
        end
        if HELPER_REWRITE_MODE == "wrapper" then
            return rewrite_helper_query_block_sql(rewritten_sql)
        end
        return rewrite_helper_query_block_sql_marker_mode(rewritten_sql)
    end

    local function rewrite_with_shared_query_block_walker(raw_sqltext)
        return rewrite_sql_with_query_blocks(raw_sqltext, rewrite_query_block_pipeline_sql)
    end

    if not query_might_need_runtime_rewrite(sqltext) then
        return sqltext
    end

    local rewritten_sql = rewrite_array_iteration_in_sql(sqltext)
    if not query_might_need_shared_walker_rewrite(rewritten_sql) then
        return rewritten_sql
    end
    return rewrite_with_shared_query_block_walker(rewritten_sql)
"""


QUERY_BLOCK_STAGE_GATES_LUA = """
    local function raw_text_reference_known_helper(raw_sqltext_upper)
        if string.find(raw_sqltext_upper, "TO_JSON", 1, true) ~= nil then
            return true
        end
        for function_name in pairs(HELPER_KIND_BY_NAME) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        for function_name in pairs(BLOCKED_FUNCTIONS) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        return false
    end

    local function quoted_identifier_contains_path_syntax(raw_sqltext)
        local index = 1
        while true do
            local start_index = string.find(raw_sqltext, '"', index, true)
            if start_index == nil then
                return false
            end
            local current = start_index + 1
            while current <= #raw_sqltext do
                local ch = string.sub(raw_sqltext, current, current)
                if ch == '"' then
                    if string.sub(raw_sqltext, current + 1, current + 1) == '"' then
                        current = current + 2
                    else
                        local identifier_text = string.sub(raw_sqltext, start_index + 1, current - 1)
                        if string.find(identifier_text, ".", 1, true) ~= nil
                                or string.find(identifier_text, "[", 1, true) ~= nil then
                            return true
                        end
                        index = current + 1
                        break
                    end
                else
                    current = current + 1
                end
            end
            if current > #raw_sqltext then
                return false
            end
        end
    end

    local function raw_text_might_need_iterator_rewrite(raw_sqltext)
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, ' IN "', 1, true) ~= nil then
            return true
        end
        return string.find(raw_sqltext, '%f[%w]IN%f[%W]%s*[A-Za-z_][A-Za-z0-9_]*%s*%.%s*"', 1) ~= nil
    end

    local function query_might_need_helper_rewrite(raw_sqltext)
        return raw_text_reference_known_helper(string.upper(raw_sqltext))
    end

    local function query_might_need_path_rewrite(raw_sqltext)
        if not REWRITE_PATH_IDENTIFIERS then
            return false
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        return false
    end

    local function query_might_need_runtime_rewrite(raw_sqltext)
        if query_might_need_helper_rewrite(raw_sqltext) then
            return true
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        if raw_text_might_need_iterator_rewrite(raw_sqltext) then
            return true
        end
        return false
    end

    local function rewrite_path_identifiers_in_sql_dispatch(raw_sqltext)
        if REWRITE_PATH_IDENTIFIERS then
            return rewrite_path_identifiers_in_sql_join_mode(raw_sqltext)
        end
        return rewrite_path_identifiers_in_sql_disabled(raw_sqltext)
    end

    local function rewrite_helper_calls_in_sql_dispatch(raw_sqltext)
        if HELPER_REWRITE_MODE == "wrapper" then
            return rewrite_helper_calls_in_sql_wrapper_mode(raw_sqltext)
        end
        return rewrite_helper_calls_in_sql_marker_mode(raw_sqltext)
    end

    local function rewrite_query_block_pipeline_sql(query_sql)
        local rewritten_sql = query_sql
        if query_might_need_path_rewrite(query_sql) then
            rewritten_sql = rewrite_path_query_block_sql(rewritten_sql)
        end
        if HELPER_REWRITE_MODE == "wrapper" then
            if query_might_need_helper_rewrite(rewritten_sql) then
                return rewrite_helper_query_block_sql(rewritten_sql)
            end
            return rewritten_sql
        end
        if query_might_need_helper_rewrite(rewritten_sql) then
            return rewrite_helper_query_block_sql_marker_mode(rewritten_sql)
        end
        return rewritten_sql
    end

    local function rewrite_with_shared_query_block_walker(raw_sqltext)
        return rewrite_sql_with_query_blocks(raw_sqltext, rewrite_query_block_pipeline_sql)
    end

    if not query_might_need_runtime_rewrite(sqltext) then
        return sqltext
    end

    local rewritten_sql = rewrite_array_iteration_in_sql(sqltext)
    return rewrite_with_shared_query_block_walker(rewritten_sql)
"""


COMBINED_RW_GATES_LUA = """
    local function raw_text_reference_known_helper(raw_sqltext_upper)
        if string.find(raw_sqltext_upper, "TO_JSON", 1, true) ~= nil then
            return true
        end
        for function_name in pairs(HELPER_KIND_BY_NAME) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        for function_name in pairs(BLOCKED_FUNCTIONS) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        return false
    end

    local function quoted_identifier_contains_path_syntax(raw_sqltext)
        local index = 1
        while true do
            local start_index = string.find(raw_sqltext, '"', index, true)
            if start_index == nil then
                return false
            end
            local current = start_index + 1
            while current <= #raw_sqltext do
                local ch = string.sub(raw_sqltext, current, current)
                if ch == '"' then
                    if string.sub(raw_sqltext, current + 1, current + 1) == '"' then
                        current = current + 2
                    else
                        local identifier_text = string.sub(raw_sqltext, start_index + 1, current - 1)
                        if string.find(identifier_text, ".", 1, true) ~= nil
                                or string.find(identifier_text, "[", 1, true) ~= nil then
                            return true
                        end
                        index = current + 1
                        break
                    end
                else
                    current = current + 1
                end
            end
            if current > #raw_sqltext then
                return false
            end
        end
    end

    local function raw_text_might_need_iterator_rewrite(raw_sqltext)
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, ' IN "', 1, true) ~= nil then
            return true
        end
        return string.find(raw_sqltext, '%f[%w]IN%f[%W]%s*[A-Za-z_][A-Za-z0-9_]*%s*%.%s*"', 1) ~= nil
    end

    local function query_might_need_helper_rewrite(raw_sqltext)
        return raw_text_reference_known_helper(string.upper(raw_sqltext))
    end

    local function query_might_need_path_rewrite(raw_sqltext)
        if not REWRITE_PATH_IDENTIFIERS then
            return false
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        return false
    end

    local function query_might_need_shared_walker_rewrite(raw_sqltext)
        return query_might_need_path_rewrite(raw_sqltext) or query_might_need_helper_rewrite(raw_sqltext)
    end

    local function query_might_need_runtime_rewrite(raw_sqltext)
        if query_might_need_helper_rewrite(raw_sqltext) then
            return true
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        if raw_text_might_need_iterator_rewrite(raw_sqltext) then
            return true
        end
        return false
    end

    local function rewrite_path_identifiers_in_sql_dispatch(raw_sqltext)
        if REWRITE_PATH_IDENTIFIERS then
            return rewrite_path_identifiers_in_sql_join_mode(raw_sqltext)
        end
        return rewrite_path_identifiers_in_sql_disabled(raw_sqltext)
    end

    local function rewrite_helper_calls_in_sql_dispatch(raw_sqltext)
        if HELPER_REWRITE_MODE == "wrapper" then
            return rewrite_helper_calls_in_sql_wrapper_mode(raw_sqltext)
        end
        return rewrite_helper_calls_in_sql_marker_mode(raw_sqltext)
    end

    local function rewrite_query_block_pipeline_sql(query_sql)
        local rewritten_sql = query_sql
        if query_might_need_path_rewrite(query_sql) then
            rewritten_sql = rewrite_path_query_block_sql(rewritten_sql)
        end
        if HELPER_REWRITE_MODE == "wrapper" then
            if query_might_need_helper_rewrite(rewritten_sql) then
                return rewrite_helper_query_block_sql(rewritten_sql)
            end
            return rewritten_sql
        end
        if query_might_need_helper_rewrite(rewritten_sql) then
            return rewrite_helper_query_block_sql_marker_mode(rewritten_sql)
        end
        return rewritten_sql
    end

    local function rewrite_with_shared_query_block_walker(raw_sqltext)
        return rewrite_sql_with_query_blocks(raw_sqltext, rewrite_query_block_pipeline_sql)
    end

    if not query_might_need_runtime_rewrite(sqltext) then
        return sqltext
    end

    local rewritten_sql = rewrite_array_iteration_in_sql(sqltext)
    if not query_might_need_shared_walker_rewrite(rewritten_sql) then
        return rewritten_sql
    end
    return rewrite_with_shared_query_block_walker(rewritten_sql)
"""


@dataclass(frozen=True)
class Variant:
    name: str
    description: str
    runtime_pipeline_lua: str


VARIANTS = (
    Variant(
        name="current_compact",
        description="Current compact runtime pipeline.",
        runtime_pipeline_lua=RUNTIME_PIPELINE_LUA,
    ),
    Variant(
        name="post_array_walker_gate",
        description="Skip the shared walker after array iteration when only iterator rewrite was needed.",
        runtime_pipeline_lua=POST_ARRAY_WALKER_GATE_LUA,
    ),
    Variant(
        name="query_block_stage_gates",
        description="Run path/helper sub-stages only for query blocks that reference them.",
        runtime_pipeline_lua=QUERY_BLOCK_STAGE_GATES_LUA,
    ),
    Variant(
        name="combined_rw_gates",
        description="Combine post-array walker gate and per-query-block path/helper stage gates.",
        runtime_pipeline_lua=COMBINED_RW_GATES_LUA,
    ),
)


QUERIES = {
    "path_only": f'SELECT COALESCE("meta.info.note", \'NULL\') FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "id"',
    "helper_only": f'SELECT JSON_TYPEOF("value") FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "id"',
    "path_and_helper": (
        f'SELECT COALESCE(JSON_AS_VARCHAR("meta.info.note"), \'NULL\') '
        f'FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "id"'
    ),
    "rowset_only": (
        f'SELECT CAST(s."id" AS VARCHAR(10)), item._index '
        f'FROM "{WRAPPER_SCHEMA}"."SAMPLE" s '
        'JOIN item IN s."items" '
        'ORDER BY s."_id", item._index'
    ),
    "rowset_exists": (
        f'SELECT CAST(s."id" AS VARCHAR(10)) FROM "{WRAPPER_SCHEMA}"."SAMPLE" s '
        'WHERE EXISTS (SELECT 1 FROM item IN s."items" WHERE item.label = \'B\' AND item.value = \'second\') '
        'ORDER BY s."id"'
    ),
    "iterator_path": (
        f'SELECT CAST(s."id" AS VARCHAR(10)), COALESCE(item."nested.note", \'NULL\') '
        f'FROM "{WRAPPER_SCHEMA}"."SAMPLE" s '
        'JOIN item IN s."items" '
        'ORDER BY s."_id", item._index'
    ),
    "iterator_helper": (
        f'SELECT CAST(s."id" AS VARCHAR(10)), JSON_TYPEOF(item."value") '
        f'FROM "{WRAPPER_SCHEMA}"."SAMPLE" s '
        'JOIN item IN s."items" '
        'ORDER BY s."_id", item._index'
    ),
    "iterator_path_and_helper": (
        f'SELECT CAST(s."id" AS VARCHAR(10)), COALESCE(JSON_AS_VARCHAR(item."nested.note"), \'NULL\') '
        f'FROM "{WRAPPER_SCHEMA}"."SAMPLE" s '
        'JOIN item IN s."items" '
        'ORDER BY s."_id", item._index'
    ),
    "to_json_root_subset": f'SELECT TO_JSON("meta", "items") FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "_id"',
    "to_json_iterator_star": (
        f'SELECT TO_JSON(item.*) '
        f'FROM "{WRAPPER_SCHEMA}"."SAMPLE" s '
        'JOIN item IN s."items" '
        'ORDER BY s."_id", item._index'
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark rewrite-heavy preprocessor variants against the same Nano-backed wrapper workload."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Result JSON output path.")
    parser.add_argument("--repetitions", type=int, default=WARM_REPETITIONS, help="Warm repetitions per query.")
    return parser.parse_args()


def cleanup_schema(con, schema_name: str) -> None:
    con.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def prepare_base_fixture() -> dict[str, object]:
    con = connect()
    try:
        cleanup_schema(con, WRAPPER_SCHEMA)
        cleanup_schema(con, HELPER_SCHEMA)
        install_source_fixture(con, include_deep_fixture=False)
        manifest = install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=WRAPPER_SCHEMA,
            helper_schema=HELPER_SCHEMA,
        )
        install_json_export_helpers(con, HELPER_SCHEMA)
        install_json_export_views(
            con,
            source_schema=SOURCE_SCHEMA,
            schema=HELPER_SCHEMA,
            udf_schema=HELPER_SCHEMA,
        )
        return manifest
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


def _module_block(name: str, body: str) -> str:
    return f"-- [module: {name}]\n{body.strip(chr(10))}"


def render_variant_library_body(variant: Variant) -> str:
    template = LIBRARY_TEMPLATE_PATH.read_text()
    for module in iter_preprocessor_library_modules():
        body = variant.runtime_pipeline_lua if module.name == "runtime_pipeline" else module.body
        template = template.replace(module.placeholder, _module_block(module.name, body))
    rendered = template.strip() + "\n"
    return compact_lua_body(rendered)


def install_variant_preprocessor(con, variant: Variant, manifest: dict[str, object]) -> dict[str, object]:
    schema_name = f"JVS_PP_RW_{variant.name.upper()}"
    cleanup_schema(con, schema_name)
    con.execute(f"CREATE SCHEMA {schema_name}")

    library_body = render_variant_library_body(variant)
    library_sql = f"CREATE OR REPLACE SCRIPT {schema_name}.{DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT} AS\n{library_body}/"
    start = time.perf_counter()
    con.execute(library_sql)
    create_library_ms = (time.perf_counter() - start) * 1000.0

    preprocessor_sql = generate_wrapper_preprocessor_sql_text(
        schema=schema_name,
        script=PREPROCESSOR_SCRIPT,
        wrapper_schemas=[WRAPPER_SCHEMA],
        helper_schemas=[HELPER_SCHEMA],
        manifests=[manifest],
    )
    start = time.perf_counter()
    execute_generated_preprocessor_sql(con, preprocessor_sql)
    create_preprocessor_ms = (time.perf_counter() - start) * 1000.0

    return {
        "schema": schema_name,
        "libraryBodyBytes": len(library_body.encode("utf-8")),
        "preprocessorBytes": len(preprocessor_sql.encode("utf-8")),
        "createLibraryMs": create_library_ms,
        "createPreprocessorMs": create_preprocessor_ms,
    }


def timed_fetchall(con, sql: str) -> float:
    start = time.perf_counter()
    con.execute(sql).fetchall()
    return (time.perf_counter() - start) * 1000.0


def benchmark_variant(variant: Variant, manifest: dict[str, object], repetitions: int) -> dict[str, object]:
    install_con = connect()
    try:
        install_info = install_variant_preprocessor(install_con, variant, manifest)
    finally:
        try:
            install_con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        install_con.close()

    con = connect()
    try:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {install_info['schema']}.{PREPROCESSOR_SCRIPT}")
        benchmark_rows: dict[str, object] = {}
        for query_name, sql in QUERIES.items():
            timings_ms: list[float] = []
            for _ in range(repetitions):
                timings_ms.append(timed_fetchall(con, sql))
            benchmark_rows[query_name] = {
                "medianMs": statistics.median(timings_ms),
                "minMs": min(timings_ms),
                "maxMs": max(timings_ms),
            }
        return {
            "name": variant.name,
            "description": variant.description,
            "install": install_info,
            "queries": benchmark_rows,
        }
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


def main() -> None:
    args = parse_args()
    manifest = prepare_base_fixture()
    results = {
        "repetitions": args.repetitions,
        "variants": [benchmark_variant(variant, manifest, args.repetitions) for variant in VARIANTS],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {args.output}")
    for variant in results["variants"]:
        print(
            f'{variant["name"]}: '
            f'rowset_only={variant["queries"]["rowset_only"]["medianMs"]:.3f} ms, '
            f'path_only={variant["queries"]["path_only"]["medianMs"]:.3f} ms, '
            f'helper_only={variant["queries"]["helper_only"]["medianMs"]:.3f} ms, '
            f'iterator_path_and_helper={variant["queries"]["iterator_path_and_helper"]["medianMs"]:.3f} ms'
        )


if __name__ == "__main__":
    main()
