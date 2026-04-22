#!/usr/bin/env python3
"""
BUG-011 regression: empty object-array child table caused invalid SQL generation.

When a JSON array field contained only empty arrays across all documents, the
ingest schema produced a child table with only structural columns (_parent, _pos)
and no data columns. The export view generator entered the fragment-building branch
but produced no fragment SELECTs, so the fragment CTE was never emitted into the
WITH clause. However, the CTE name was still registered in fragment_ctes, causing
topfrags_sql to emit a LEFT JOIN against a non-existent CTE, which Exasol rejected
with "object FRAGMENTS_... not found".

Fix: only register fragment_ctes[table_name] when fragment_selects is non-empty
(i.e., only when the fragment CTE is actually emitted).
"""

from __future__ import annotations

import json

import _bootstrap  # noqa: F401

from generate_json_export_views_sql import (
    generate_json_export_artifacts_from_source_columns,
    install_json_export_views,
)
from generate_json_export_helper_sql import install_json_export_helpers
from nano_support import connect
from wrapper_schema_support import ColumnMeta


SOURCE_SCHEMA = "JVS_SRC_EMPTY_ARR"
EXPORT_SCHEMA = "JVS_EMPTY_ARR_EXPORT"
UDF_SCHEMA = "JVS_EMPTY_ARR_UDF"


def _col(table: str, name: str, type_name: str, ordinal: int, **kwargs) -> ColumnMeta:
    return ColumnMeta(schema=SOURCE_SCHEMA, table=table, name=name, type_name=type_name, ordinal=ordinal, **kwargs)


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_not_contains(text: str, needle: str, label: str) -> None:
    if needle in text:
        raise AssertionError(f"{label}: unexpected substring {needle!r} found in:\n{text}")


def main() -> None:
    # Source schema: DOCS root with an "items" array field whose child table has
    # only structural columns (_parent, _pos) — no data columns. This happens when
    # all arrays in the ingested data were empty ([]).
    source_columns = {
        "DOCS": [
            _col("DOCS", "_id", "DECIMAL", 1, size=None, precision=18, scale=0),
            _col("DOCS", "name", "VARCHAR", 2, size=100, precision=None, scale=None),
            _col("DOCS", "items|array", "DECIMAL", 3, size=None, precision=18, scale=0),
        ],
        "DOCS_items_arr": [
            _col("DOCS_items_arr", "_parent", "DECIMAL", 1, size=None, precision=18, scale=0),
            _col("DOCS_items_arr", "_pos", "DECIMAL", 2, size=None, precision=18, scale=0),
        ],
    }

    # BUG-011: this must not raise "object FRAGMENTS_... not found" or any similar
    # error caused by referencing an unregistered fragment CTE.
    artifacts = generate_json_export_artifacts_from_source_columns(
        source_columns,
        source_schema=SOURCE_SCHEMA,
        schema=EXPORT_SCHEMA,
        udf_schema=UDF_SCHEMA,
    )

    # The unregistered CTE name must not appear in the generated SQL.
    assert_not_contains(
        artifacts.sql,
        "FRAGMENTS_DOCS_items_arr",
        "BUG-011 empty-array: unregistered fragment CTE must not appear in generated SQL",
    )

    # Install into a real DB to confirm the SQL itself is valid Exasol syntax.
    con = connect()
    try:
        con.execute(f"DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE")
        con.execute(f"CREATE SCHEMA {SOURCE_SCHEMA}")
        con.execute(f"OPEN SCHEMA {SOURCE_SCHEMA}")
        con.execute(
            'CREATE OR REPLACE TABLE DOCS'
            ' ("_id" DECIMAL(18,0) NOT NULL, "name" VARCHAR(100), "items|array" DECIMAL(18,0))'
        )
        con.execute(
            'CREATE OR REPLACE TABLE "DOCS_items_arr"'
            ' ("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL)'
        )
        con.execute("INSERT INTO DOCS VALUES (1, 'alpha', NULL)")
        con.execute("INSERT INTO DOCS VALUES (2, 'beta', NULL)")

        install_json_export_helpers(con, UDF_SCHEMA)
        con.execute(f"DROP SCHEMA IF EXISTS {EXPORT_SCHEMA} CASCADE")
        install_json_export_views(
            con,
            source_schema=SOURCE_SCHEMA,
            schema=EXPORT_SCHEMA,
            udf_schema=UDF_SCHEMA,
        )

        root_names = artifacts.root_names["DOCS"]
        rows = con.execute(
            f'SELECT "{root_names.full_json_column}" FROM {root_names.qualified_view}'
            f' ORDER BY "_id"'
        ).fetchall()
        docs = [json.loads(row[0]) for row in rows]

        assert_equal(len(docs), 2, "BUG-011 exported row count")
        assert_equal(docs[0].get("name"), "alpha", "BUG-011 doc 1 name field")
        assert_equal(docs[1].get("name"), "beta", "BUG-011 doc 2 name field")
    finally:
        con.close()

    print("-- BUG-011 empty-array fragment CTE regression --")
    print("SQL generation and view installation succeeded for empty object-array schema")


if __name__ == "__main__":
    main()
