#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]


def _load_builder():
    import sys

    python_root = str(ROOT / "python")
    if python_root not in sys.path:
        sys.path.insert(0, python_root)

    from exasol_json_tables.preprocessor_library_builder import (  # type: ignore
        compact_lua_body,
        generate_preprocessor_library_body,
        iter_preprocessor_library_modules,
    )
    from exasol_json_tables.generate_preprocessor_library_sql import (  # type: ignore
        generate_preprocessor_library_sql_text,
    )

    return compact_lua_body, generate_preprocessor_library_body, iter_preprocessor_library_modules, generate_preprocessor_library_sql_text


def test_library_builder_uses_named_modules() -> None:
    _, generate_preprocessor_library_body, iter_preprocessor_library_modules, _ = _load_builder()
    modules = iter_preprocessor_library_modules()
    names = [module.name for module in modules]
    assert names == [
        "parser_core",
        "array_iteration",
        "path_rewrite",
        "path_rewrite_disabled",
        "helper_core",
        "helper_rewrite_marker",
        "helper_rewrite_wrapper",
        "runtime_pipeline",
    ]

    body = generate_preprocessor_library_body()
    for module in modules:
        marker = f"-- [module: {module.name}]"
        assert marker in body
        assert body.count(marker) == 1


def test_library_builder_resolves_all_placeholders() -> None:
    _, generate_preprocessor_library_body, _, _ = _load_builder()
    body = generate_preprocessor_library_body()
    assert re.search(r"__[A-Z0-9_]+__", body) is None
    assert "function rewrite(sqltext, config)" in body
    assert "rewrite_with_shared_query_block_walker" in body
    assert "query_might_need_runtime_rewrite" in body
    assert "if not query_might_need_runtime_rewrite(sqltext) then" in body
    assert "raw_text_reference_known_helper" in body
    assert "quoted_identifier_contains_path_syntax" in body
    assert "raw_text_might_need_iterator_rewrite" in body


def test_compact_library_output_is_smaller_and_placeholder_free() -> None:
    compact_lua_body, generate_preprocessor_library_body, _, _ = _load_builder()
    pretty_body = generate_preprocessor_library_body()
    compact_body = compact_lua_body(pretty_body)
    assert re.search(r"__[A-Z0-9_]+__", compact_body) is None
    assert "-- [module:" not in compact_body
    assert "function rewrite(sqltext, config)" in compact_body
    assert len(compact_body) < len(pretty_body)


def test_generated_library_sql_uses_compact_runtime_body() -> None:
    _, generate_preprocessor_library_body, _, generate_preprocessor_library_sql_text = _load_builder()
    compact_body = generate_preprocessor_library_body(compact=True)
    sql_text = generate_preprocessor_library_sql_text("JVS_WRAP_PP", "JVS_PREPROCESSOR_LIB")

    assert compact_body in sql_text
    assert "-- [module:" not in sql_text
    assert "-- Shared JSON Tables preprocessor runtime library." in sql_text
    assert "CREATE OR REPLACE SCRIPT JVS_WRAP_PP.JVS_PREPROCESSOR_LIB AS" in sql_text


if __name__ == "__main__":
    test_library_builder_uses_named_modules()
    test_library_builder_resolves_all_placeholders()
    test_compact_library_output_is_smaller_and_placeholder_free()
    test_generated_library_sql_uses_compact_runtime_body()
    print("-- preprocessor library builder regression --")
    print("verified named runtime modules, module markers, and placeholder-free shared library output")
