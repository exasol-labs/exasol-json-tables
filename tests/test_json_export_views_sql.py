#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import _bootstrap  # noqa: F401

from _fixture_expected_json import deepdoc_fixture_documents, sample_fixture_documents
from generate_json_export_helper_sql import install_json_export_helpers
from generate_json_export_views_sql import (
    generate_json_export_artifacts,
    generate_json_export_artifacts_from_source_columns,
    install_json_export_views,
    json_export_root_names_from_wrapper_manifest,
    json_export_view_name,
)
from nano_support import ROOT, connect, install_source_fixture, install_wrapper_views
from wrapper_schema_support import ColumnMeta, quote_identifier


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
EXPORT_SCHEMA = "JVS_JSON_EXPORT_PHASE2"
UDF_SCHEMA = "JVS_JSON_EXPORT_HELPERS"
OUTPUT_PATH = ROOT / "dist" / "json_export_views_test.sql"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_contains(text: str, expected: str, label: str) -> None:
    if expected not in text:
        raise AssertionError(f"{label} mismatch.\nExpected substring: {expected!r}\nActual: {text}")


def assert_not_contains(text: str, expected: str, label: str) -> None:
    if expected in text:
        raise AssertionError(f"{label} mismatch.\nUnexpected substring: {expected!r}\nActual: {text}")


def parse_fragment(fragment: str | None) -> object | None:
    if fragment is None:
        return None
    return json.loads("{" + fragment + "}")


def fetch_full_json_list(con, qualified_view: str, full_json_column: str, id_column: str) -> list[object]:
    rows = con.execute(
        f"""
        SELECT {quote_identifier(full_json_column)}
        FROM {qualified_view}
        ORDER BY {quote_identifier(id_column)}
        """
    ).fetchall()
    return [json.loads(row[0]) for row in rows]


def make_column(
    table: str,
    name: str,
    type_name: str,
    ordinal: int,
    *,
    size: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
    schema: str = SOURCE_SCHEMA,
) -> ColumnMeta:
    return ColumnMeta(
        schema=schema,
        table=table,
        name=name,
        type_name=type_name,
        ordinal=ordinal,
        size=size,
        precision=precision,
        scale=scale,
    )


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
        manifest = install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=WRAPPER_SCHEMA,
            helper_schema=HELPER_SCHEMA,
        )
        install_json_export_helpers(con, UDF_SCHEMA)

        artifacts = generate_json_export_artifacts(
            con,
            source_schema=SOURCE_SCHEMA,
            schema=EXPORT_SCHEMA,
            udf_schema=UDF_SCHEMA,
        )
        manifest_names = json_export_root_names_from_wrapper_manifest(manifest, schema=EXPORT_SCHEMA)

        assert_equal(artifacts.root_tables, ("DEEPDOC", "SAMPLE"), "generated export roots")
        assert_equal(artifacts.root_names["SAMPLE"], manifest_names["SAMPLE"], "SAMPLE export naming plan")
        assert_equal(artifacts.root_names["DEEPDOC"], manifest_names["DEEPDOC"], "DEEPDOC export naming plan")
        assert_equal(
            artifacts.root_names["SAMPLE"].fragment_column_for_base_name("child"),
            artifacts.root_names["SAMPLE"].fragment_column_for_visible_name("child|object"),
            "base-name vs visible-name fragment lookup",
        )
        assert_contains(
            artifacts.sql,
            f'"{UDF_SCHEMA}"."JSON_OBJECT_FROM_FRAGMENTS"',
            "export SQL should reference helper UDFs",
        )
        assert_not_contains(artifacts.sql, "CREATE OR REPLACE LUA", "export SQL should not redefine helper UDFs")

        subprocess.run(
            [
                "python3",
                str(ROOT / "tools" / "generate_json_export_views_sql.py"),
                "--source-schema",
                SOURCE_SCHEMA,
                "--schema",
                EXPORT_SCHEMA,
                "--udf-schema",
                UDF_SCHEMA,
                "--output",
                str(OUTPUT_PATH),
            ],
            check=True,
        )
        assert_equal(OUTPUT_PATH.read_text(), artifacts.sql, "tool-generated export SQL")

        con.execute(f"DROP SCHEMA IF EXISTS {EXPORT_SCHEMA} CASCADE")
        install_json_export_views(
            con,
            source_schema=SOURCE_SCHEMA,
            schema=EXPORT_SCHEMA,
            udf_schema=UDF_SCHEMA,
        )
        install_json_export_views(
            con,
            source_schema=SOURCE_SCHEMA,
            schema=EXPORT_SCHEMA,
            udf_schema=UDF_SCHEMA,
        )

        installed_objects = con.execute(
            f"""
            SELECT OBJECT_NAME, OBJECT_TYPE
            FROM SYS.EXA_ALL_OBJECTS
            WHERE ROOT_NAME = '{EXPORT_SCHEMA}'
            ORDER BY OBJECT_TYPE, OBJECT_NAME
            """
        ).fetchall()
        expected_view_names = sorted(
            (json_export_view_name(str(table["tableName"])), "VIEW")
            for table in manifest["tables"]
        )
        assert_equal(
            installed_objects,
            expected_view_names,
            "installed export views",
        )

        sample_names = artifacts.root_names["SAMPLE"]
        deepdoc_names = artifacts.root_names["DEEPDOC"]
        sample_json = fetch_full_json_list(
            con,
            sample_names.qualified_view,
            sample_names.full_json_column,
            sample_names.id_column,
        )
        deepdoc_json = fetch_full_json_list(
            con,
            deepdoc_names.qualified_view,
            deepdoc_names.full_json_column,
            deepdoc_names.id_column,
        )
        assert_equal(
            sample_json,
            sample_fixture_documents(),
            "SAMPLE export view full JSON",
        )
        assert_equal(
            deepdoc_json,
            deepdoc_fixture_documents(),
            "DEEPDOC export view full JSON",
        )

        note_col = quote_identifier(sample_names.fragment_column_for_base_name("note"))
        child_col = quote_identifier(sample_names.fragment_column_for_base_name("child"))
        meta_col = quote_identifier(sample_names.fragment_column_for_base_name("meta"))
        tags_col = quote_identifier(sample_names.fragment_column_for_base_name("tags"))
        items_col = quote_identifier(sample_names.fragment_column_for_base_name("items"))
        fragment_rows = con.execute(
            f"""
            SELECT
              {quote_identifier(sample_names.id_column)},
              {note_col},
              {child_col},
              {meta_col},
              {tags_col},
              {items_col}
            FROM {sample_names.qualified_view}
            ORDER BY {quote_identifier(sample_names.id_column)}
            """
        ).fetchall()

        assert_equal(parse_fragment(fragment_rows[0][1]), {"note": "x"}, "row 1 note fragment")
        assert_equal(parse_fragment(fragment_rows[1][1]), {"note": None}, "row 2 note fragment")
        assert_equal(parse_fragment(fragment_rows[2][1]), None, "row 3 note fragment")

        assert_equal(parse_fragment(fragment_rows[0][2]), {"child": {"value": "child-1"}}, "row 1 child fragment")
        assert_equal(parse_fragment(fragment_rows[1][2]), None, "row 2 child fragment")
        assert_equal(parse_fragment(fragment_rows[2][2]), {"child": None}, "row 3 child fragment")

        assert_equal(
            parse_fragment(fragment_rows[0][3]),
            {"meta": {"flag": True, "info": {"note": "deep"}, "items": [{"value": "m1"}, {"value": "m2"}]}},
            "row 1 meta fragment",
        )
        assert_equal(
            parse_fragment(fragment_rows[1][3]),
            {"meta": {"flag": False, "items": [{"value": "m3"}]}},
            "row 2 meta fragment",
        )
        assert_equal(parse_fragment(fragment_rows[2][3]), None, "row 3 meta fragment")

        assert_equal(parse_fragment(fragment_rows[0][4]), {"tags": ["red", "blue"]}, "row 1 tags fragment")
        assert_equal(parse_fragment(fragment_rows[1][4]), {"tags": ["green"]}, "row 2 tags fragment")
        assert_equal(parse_fragment(fragment_rows[2][4]), None, "row 3 tags fragment")

        assert_equal(
            parse_fragment(fragment_rows[0][5]),
            {
                "items": [
                    {
                        "amount": 7,
                        "enabled": True,
                        "label": "A",
                        "nested": {
                            "active": True,
                            "items": [{"value": "na-1"}, {"value": "na-2"}],
                            "note": "nested-a",
                            "pick": 1,
                            "score": 11,
                        },
                        "optional": "x",
                        "value": "first",
                    },
                    {
                        "enabled": False,
                        "label": "B",
                        "nested": {
                            "active": False,
                            "items": [{"value": "nb-1"}],
                            "note": "nested-b",
                            "pick": 0,
                            "score": 12,
                        },
                        "optional": None,
                        "value": "second",
                    },
                ]
            },
            "row 1 items fragment",
        )
        assert_equal(
            parse_fragment(fragment_rows[1][5]),
            {
                "items": [
                    {
                        "amount": 5,
                        "label": "C",
                        "value": "only",
                    }
                ]
            },
            "row 2 items fragment",
        )
        assert_equal(parse_fragment(fragment_rows[2][5]), None, "row 3 items fragment")

        empty_array_source_schema = "JVS_SRC_EMPTY_ARR"
        empty_array_export_schema = "JVS_EMPTY_ARR_EXPORT"
        empty_array_udf_schema = "JVS_EMPTY_ARR_UDF"
        empty_array_source_columns = {
            "DOCS": [
                make_column("DOCS", "_id", "DECIMAL", 1, precision=18, scale=0, schema=empty_array_source_schema),
                make_column("DOCS", "name", "VARCHAR(100)", 2, size=100, schema=empty_array_source_schema),
                make_column(
                    "DOCS",
                    "items|array",
                    "DECIMAL",
                    3,
                    precision=18,
                    scale=0,
                    schema=empty_array_source_schema,
                ),
            ],
            "DOCS_items_arr": [
                make_column(
                    "DOCS_items_arr",
                    "_parent",
                    "DECIMAL",
                    1,
                    precision=18,
                    scale=0,
                    schema=empty_array_source_schema,
                ),
                make_column(
                    "DOCS_items_arr",
                    "_pos",
                    "DECIMAL",
                    2,
                    precision=18,
                    scale=0,
                    schema=empty_array_source_schema,
                ),
            ],
        }
        empty_array_artifacts = generate_json_export_artifacts_from_source_columns(
            empty_array_source_columns,
            source_schema=empty_array_source_schema,
            schema=empty_array_export_schema,
            udf_schema=empty_array_udf_schema,
        )
        assert_not_contains(
            empty_array_artifacts.sql.lower(),
            "left join fragments_docs_items_arr ",
            "empty-array child should not reference an unregistered fragment CTE",
        )

        con.execute(f"DROP SCHEMA IF EXISTS {empty_array_source_schema} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {empty_array_export_schema} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {empty_array_udf_schema} CASCADE")
        con.execute(f"CREATE SCHEMA {empty_array_source_schema}")
        con.execute(f"OPEN SCHEMA {empty_array_source_schema}")
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

        install_json_export_helpers(con, empty_array_udf_schema)
        install_json_export_views(
            con,
            source_schema=empty_array_source_schema,
            schema=empty_array_export_schema,
            udf_schema=empty_array_udf_schema,
        )

        empty_array_root = empty_array_artifacts.root_names["DOCS"]
        empty_array_rows = con.execute(
            f"""
            SELECT {quote_identifier(empty_array_root.full_json_column)}
            FROM {empty_array_root.qualified_view}
            ORDER BY {quote_identifier(empty_array_root.id_column)}
            """
        ).fetchall()
        empty_array_docs = [json.loads(row[0]) for row in empty_array_rows]
        assert_equal(
            empty_array_docs,
            [{"name": "alpha"}, {"name": "beta"}],
            "empty-array child export documents",
        )

        value_object_source_schema = "JVS_SRC_VALUE_OBJECTS"
        value_object_export_schema = "JVS_VALUE_OBJECTS_EXPORT"
        value_object_udf_schema = "JVS_VALUE_OBJECTS_UDF"
        value_object_source_columns = {
            "EXPERIMENTS": [
                make_column("EXPERIMENTS", "_id", "DECIMAL", 1, precision=18, scale=0, schema=value_object_source_schema),
                make_column("EXPERIMENTS", "experiment_id", "VARCHAR(100)", 2, size=100, schema=value_object_source_schema),
                make_column(
                    "EXPERIMENTS",
                    "measurements|array",
                    "DECIMAL",
                    3,
                    precision=18,
                    scale=0,
                    schema=value_object_source_schema,
                ),
            ],
            "EXPERIMENTS_measurements_arr": [
                make_column(
                    "EXPERIMENTS_measurements_arr",
                    "_id",
                    "DECIMAL",
                    1,
                    precision=18,
                    scale=0,
                    schema=value_object_source_schema,
                ),
                make_column(
                    "EXPERIMENTS_measurements_arr",
                    "_parent",
                    "DECIMAL",
                    2,
                    precision=18,
                    scale=0,
                    schema=value_object_source_schema,
                ),
                make_column(
                    "EXPERIMENTS_measurements_arr",
                    "_pos",
                    "DECIMAL",
                    3,
                    precision=18,
                    scale=0,
                    schema=value_object_source_schema,
                ),
                make_column(
                    "EXPERIMENTS_measurements_arr",
                    "_value",
                    "DECIMAL",
                    4,
                    precision=18,
                    scale=0,
                    schema=value_object_source_schema,
                ),
                make_column(
                    "EXPERIMENTS_measurements_arr",
                    "unit",
                    "VARCHAR(20)",
                    5,
                    size=20,
                    schema=value_object_source_schema,
                ),
            ],
        }
        value_object_artifacts = generate_json_export_artifacts_from_source_columns(
            value_object_source_columns,
            source_schema=value_object_source_schema,
            schema=value_object_export_schema,
            udf_schema=value_object_udf_schema,
        )
        con.execute(f"DROP SCHEMA IF EXISTS {value_object_source_schema} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {value_object_export_schema} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {value_object_udf_schema} CASCADE")
        con.execute(f"CREATE SCHEMA {value_object_source_schema}")
        con.execute(f"OPEN SCHEMA {value_object_source_schema}")
        con.execute(
            'CREATE OR REPLACE TABLE EXPERIMENTS'
            ' ("_id" DECIMAL(18,0) NOT NULL, "experiment_id" VARCHAR(100), "measurements|array" DECIMAL(18,0))'
        )
        con.execute(
            'CREATE OR REPLACE TABLE "EXPERIMENTS_measurements_arr"'
            ' ("_id" DECIMAL(18,0) NOT NULL, "_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL,'
            ' "_value" DECIMAL(18,0), "unit" VARCHAR(20))'
        )
        con.execute("INSERT INTO EXPERIMENTS VALUES (1, 'exp-1', 2)")
        con.execute("INSERT INTO EXPERIMENTS VALUES (2, 'exp-2', NULL)")
        con.execute('INSERT INTO "EXPERIMENTS_measurements_arr" VALUES (101, 1, 0, 10, \'mg/dL\')')
        con.execute('INSERT INTO "EXPERIMENTS_measurements_arr" VALUES (102, 1, 1, 20, \'mmol/L\')')

        install_json_export_helpers(con, value_object_udf_schema)
        install_json_export_views(
            con,
            source_schema=value_object_source_schema,
            schema=value_object_export_schema,
            udf_schema=value_object_udf_schema,
        )

        value_object_root = value_object_artifacts.root_names["EXPERIMENTS"]
        value_object_docs = fetch_full_json_list(
            con,
            value_object_root.qualified_view,
            value_object_root.full_json_column,
            value_object_root.id_column,
        )
        assert_equal(
            value_object_docs,
            [
                {
                    "experiment_id": "exp-1",
                    "measurements": [
                        {"value": 10, "unit": "mg/dL"},
                        {"value": 20, "unit": "mmol/L"},
                    ],
                },
                {"experiment_id": "exp-2"},
            ],
            "object-array value field survives full JSON export",
        )
    finally:
        con.close()

    print("-- json export view SQL regression --")
    print("generated, installed, and validated hidden export views:", Path(OUTPUT_PATH))


if __name__ == "__main__":
    main()
