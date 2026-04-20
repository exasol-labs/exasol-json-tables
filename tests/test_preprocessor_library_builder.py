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
        generate_preprocessor_library_body,
        iter_preprocessor_library_modules,
    )

    return generate_preprocessor_library_body, iter_preprocessor_library_modules


def test_library_builder_uses_named_modules() -> None:
    generate_preprocessor_library_body, iter_preprocessor_library_modules = _load_builder()
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
    generate_preprocessor_library_body, _ = _load_builder()
    body = generate_preprocessor_library_body()
    assert re.search(r"__[A-Z0-9_]+__", body) is None
    assert "function rewrite(sqltext, config)" in body
    assert "rewrite_with_shared_query_block_walker" in body
    assert "query_might_need_runtime_rewrite" in body
    assert "if not query_might_need_runtime_rewrite(sqltext) then" in body


if __name__ == "__main__":
    test_library_builder_uses_named_modules()
    test_library_builder_resolves_all_placeholders()
    print("-- preprocessor library builder regression --")
    print("verified named runtime modules, module markers, and placeholder-free shared library output")
