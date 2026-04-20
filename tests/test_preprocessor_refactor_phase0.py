#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import _bootstrap  # noqa: F401

from _fixture_expected_json import sample_fixture_documents
from nano_support import ROOT, connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_PHASE0_PP"
PREPROCESSOR_SCRIPT = "JSON_PHASE0_PREPROCESSOR"
GENERIC_OUTPUT = ROOT / "dist" / "phase0_generic_preprocessor.sql"
LIBRARY_OUTPUT = ROOT / "dist" / "phase0_preprocessor_library.sql"
WRAPPER_OUTPUT = ROOT / "dist" / "phase0_wrapper_preprocessor.sql"
MANIFEST_PATH = ROOT / "dist" / "json_wrapper_manifest_test.json"
# Phase-1 measured baseline on 2026-04-20:
# - generic preprocessor: 1_856 bytes
# - preprocessor library: 179_582 bytes
# - wrapper preprocessor: 53_518 bytes
# Keep a modest guard band so future refactors can shrink freely, while accidental growth still trips.
GENERIC_SIZE_CEILING_BYTES = 5_000
LIBRARY_SIZE_CEILING_BYTES = 190_000
WRAPPER_SIZE_CEILING_BYTES = 65_000


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def fetch_json_rows(con, sql: str) -> list[object]:
    return [json.loads(row[0]) for row in con.execute(sql).fetchall()]


def generate_phase0_artifacts() -> tuple[int, int, int]:
    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "generate_preprocessor_library_sql.py"),
            "--schema",
            PREPROCESSOR_SCHEMA,
            "--output",
            str(LIBRARY_OUTPUT),
        ],
        check=True,
    )
    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "generate_preprocessor_sql.py"),
            "--schema",
            PREPROCESSOR_SCHEMA,
            "--script",
            PREPROCESSOR_SCRIPT,
            "--function-name",
            "JSON_IS_EXPLICIT_NULL",
            "--rewrite-path-identifiers",
            "--allowed-schema",
            WRAPPER_SCHEMA,
            "--output",
            str(GENERIC_OUTPUT),
        ],
        check=True,
    )
    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "generate_wrapper_preprocessor_sql.py"),
            "--schema",
            PREPROCESSOR_SCHEMA,
            "--script",
            PREPROCESSOR_SCRIPT,
            "--wrapper-schema",
            WRAPPER_SCHEMA,
            "--helper-schema",
            HELPER_SCHEMA,
            "--manifest",
            str(MANIFEST_PATH),
            "--output",
            str(WRAPPER_OUTPUT),
        ],
        check=True,
    )
    return GENERIC_OUTPUT.stat().st_size, LIBRARY_OUTPUT.stat().st_size, WRAPPER_OUTPUT.stat().st_size


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
        install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=WRAPPER_SCHEMA,
            helper_schema=HELPER_SCHEMA,
        )
        install_wrapper_preprocessor(
            con,
            [WRAPPER_SCHEMA],
            [HELPER_SCHEMA],
            schema_name=PREPROCESSOR_SCHEMA,
            script_name=PREPROCESSOR_SCRIPT,
        )

        comment_and_string_rows = con.execute(
            f"""
            SELECT
              CAST("id" AS VARCHAR(10)) AS doc_id,
              '-- literal "tags[LAST]" TO_JSON(*) JOIN VALUE tag IN s."tags"' AS literal_text,
              COALESCE("child.value", 'NULL') AS child_value
            FROM {WRAPPER_SCHEMA}.SAMPLE
            /* block comment with "meta.info.note" and JOIN item IN s."items" */
            -- trailing comment with TO_JSON(*)
            ORDER BY "id"
            """
        ).fetchall()
        assert_equal(
            comment_and_string_rows,
            [
                ("1", '-- literal "tags[LAST]" TO_JSON(*) JOIN VALUE tag IN s."tags"', "child-1"),
                ("2", '-- literal "tags[LAST]" TO_JSON(*) JOIN VALUE tag IN s."tags"', "NULL"),
                ("3", '-- literal "tags[LAST]" TO_JSON(*) JOIN VALUE tag IN s."tags"', "NULL"),
            ],
            "comments and string literals should not interfere with rewrite",
        )

        cte_rows = con.execute(
            f"""
            WITH docs AS (
              SELECT
                CAST("id" AS VARCHAR(10)) AS doc_id,
                COALESCE("tags[LAST]", 'NULL') AS last_tag,
                COALESCE("meta.info.note", 'NULL') AS deep_note
              FROM {WRAPPER_SCHEMA}.SAMPLE
            )
            SELECT doc_id, last_tag, deep_note
            FROM docs
            ORDER BY doc_id
            """
        ).fetchall()
        assert_equal(
            cte_rows,
            [("1", "blue", "deep"), ("2", "green", "NULL"), ("3", "NULL", "NULL")],
            "CTE wrapper query block rewrite",
        )

        union_rows = con.execute(
            f"""
            SELECT doc_id, branch_value
            FROM (
              SELECT
                CAST("id" AS VARCHAR(10)) AS doc_id,
                COALESCE("tags[LAST]", 'NULL') AS branch_value
              FROM {WRAPPER_SCHEMA}.SAMPLE
              WHERE "id" = 1
              UNION ALL
              SELECT
                CAST("id" AS VARCHAR(10)) AS doc_id,
                COALESCE("meta.info.note", 'NULL') AS branch_value
              FROM {WRAPPER_SCHEMA}.SAMPLE
              WHERE "id" = 2
            ) q
            ORDER BY doc_id
            """
        ).fetchall()
        assert_equal(
            union_rows,
            [("1", "blue"), ("2", "NULL")],
            "top-level set query rewrite",
        )

        nested_to_json_rows = fetch_json_rows(
            con,
            f"""
            SELECT doc_json
            FROM (
              SELECT TO_JSON(*) AS doc_json
              FROM {WRAPPER_SCHEMA}.SAMPLE
              WHERE "_id" <= 2
            ) q
            ORDER BY doc_json
            """,
        )
        expected_subset = sorted(sample_fixture_documents()[:2], key=lambda row: json.dumps(row, sort_keys=False))
        actual_subset = sorted(nested_to_json_rows, key=lambda row: json.dumps(row, sort_keys=False))
        assert_equal(actual_subset, expected_subset, "nested subquery TO_JSON(*) rewrite")
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        finally:
            con.close()

    generic_size_bytes, library_size_bytes, wrapper_size_bytes = generate_phase0_artifacts()
    if generic_size_bytes > GENERIC_SIZE_CEILING_BYTES:
        raise AssertionError(
            f"generic preprocessor size regression: expected <= {GENERIC_SIZE_CEILING_BYTES} bytes, "
            f"got {generic_size_bytes}"
        )
    if library_size_bytes > LIBRARY_SIZE_CEILING_BYTES:
        raise AssertionError(
            f"preprocessor library size regression: expected <= {LIBRARY_SIZE_CEILING_BYTES} bytes, "
            f"got {library_size_bytes}"
        )
    if wrapper_size_bytes > WRAPPER_SIZE_CEILING_BYTES:
        raise AssertionError(
            f"wrapper preprocessor size regression: expected <= {WRAPPER_SIZE_CEILING_BYTES} bytes, "
            f"got {wrapper_size_bytes}"
        )

    print("-- preprocessor refactor phase 0 baseline --")
    print(f"generic preprocessor size: {generic_size_bytes} bytes")
    print(f"preprocessor library size: {library_size_bytes} bytes")
    print(f"wrapper preprocessor size: {wrapper_size_bytes} bytes")


if __name__ == "__main__":
    main()
