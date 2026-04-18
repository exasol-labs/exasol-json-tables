#!/usr/bin/env python3

from __future__ import annotations

import _bootstrap  # noqa: F401

from nano_support import connect, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_VARIANT_SRC"
WRAPPER_SCHEMA = "JSON_VIEW_VARIANT"
HELPER_SCHEMA = "JSON_VIEW_VARIANT_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_WRAP_VARIANT_PP"
PREPROCESSOR_SCRIPT = "JSON_WRAPPER_VARIANT_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def install_variant_fixture(con) -> None:
    statements = [
        f"DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE",
        f"CREATE SCHEMA {SOURCE_SCHEMA}",
        f"OPEN SCHEMA {SOURCE_SCHEMA}",
        """
        CREATE OR REPLACE TABLE DOCS (
          "_id" DECIMAL(18,0) NOT NULL,
          "doc_id" DECIMAL(18,0),
          "title" VARCHAR(100),
          "flex|object" DECIMAL(18,0),
          "flex|array" DECIMAL(18,0),
          "flex|string" VARCHAR(100),
          "flex|n" BOOLEAN
        )
        """,
        """
        CREATE OR REPLACE TABLE "DOCS_flex" (
          "_id" DECIMAL(18,0) NOT NULL,
          "note" VARCHAR(100),
          "flag" BOOLEAN
        )
        """,
        """
        CREATE OR REPLACE TABLE "DOCS_flex_arr" (
          "_id" DECIMAL(18,0) NOT NULL,
          "_parent" DECIMAL(18,0) NOT NULL,
          "_pos" DECIMAL(18,0) NOT NULL,
          "value" VARCHAR(100),
          "kind" VARCHAR(100)
        )
        """,
        """
        INSERT INTO DOCS VALUES
          (1, 101, 'object-doc', 5001, NULL, NULL, FALSE),
          (2, 102, 'array-doc', NULL, 2, NULL, FALSE),
          (3, 103, 'string-doc', NULL, NULL, 'scalar-fallback', FALSE),
          (4, 104, 'null-doc', NULL, NULL, NULL, TRUE)
        """,
        """
        INSERT INTO "DOCS_flex" VALUES
          (5001, 'object-note', TRUE)
        """,
        """
        INSERT INTO "DOCS_flex_arr" VALUES
          (6001, 2, 0, 'arr-1', 'head'),
          (6002, 2, 1, 'arr-2', 'tail')
        """,
    ]
    for statement in statements:
        con.execute(statement)


def fetch_all(sql: str):
    con = connect()
    try:
        install_wrapper_preprocessor(
            con,
            [WRAPPER_SCHEMA],
            [HELPER_SCHEMA],
            schema_name=PREPROCESSOR_SCHEMA,
            script_name=PREPROCESSOR_SCRIPT,
        )
        return con.execute(sql).fetchall()
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


def main() -> None:
    con = connect()
    try:
        install_variant_fixture(con)
        install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=WRAPPER_SCHEMA,
            helper_schema=HELPER_SCHEMA,
            generate_preprocessor=True,
            preprocessor_schema=PREPROCESSOR_SCHEMA,
            preprocessor_script=PREPROCESSOR_SCRIPT,
        )
        install_wrapper_preprocessor(
            con,
            [WRAPPER_SCHEMA],
            [HELPER_SCHEMA],
            schema_name=PREPROCESSOR_SCHEMA,
            script_name=PREPROCESSOR_SCRIPT,
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    root_variant_rows = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)),
          COALESCE(JSON_TYPEOF("flex"), 'MISSING'),
          COALESCE(JSON_AS_VARCHAR("flex"), 'NULL'),
          CASE WHEN JSON_IS_EXPLICIT_NULL("flex") THEN '1' ELSE '0' END
        FROM {WRAPPER_SCHEMA}.DOCS
        ORDER BY "doc_id"
    """)
    assert_equal(
        root_variant_rows,
        [
            ("101", "OBJECT", "NULL", "0"),
            ("102", "ARRAY", "NULL", "0"),
            ("103", "STRING", "scalar-fallback", "0"),
            ("104", "NULL", "NULL", "1"),
        ],
        "root mixed variant helpers",
    )

    object_variant_rows = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)),
          COALESCE("flex.note", 'NULL'),
          COALESCE(CAST("flex.flag" AS VARCHAR(10)), 'NULL'),
          COALESCE(JSON_TYPEOF("flex.note"), 'MISSING'),
          COALESCE(JSON_AS_VARCHAR("flex.note"), 'NULL')
        FROM {WRAPPER_SCHEMA}.DOCS
        ORDER BY "doc_id"
    """)
    assert_equal(
        object_variant_rows,
        [
            ("101", "object-note", "TRUE", "STRING", "object-note"),
            ("102", "NULL", "NULL", "MISSING", "NULL"),
            ("103", "NULL", "NULL", "MISSING", "NULL"),
            ("104", "NULL", "NULL", "MISSING", "NULL"),
        ],
        "object-branch variant navigation",
    )

    array_variant_rows = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)),
          COALESCE(CAST("flex[SIZE]" AS VARCHAR(10)), 'NULL'),
          COALESCE("flex[LAST].value", 'NULL'),
          COALESCE("flex[FIRST].kind", 'NULL'),
          COALESCE(JSON_TYPEOF("flex[LAST].value"), 'MISSING'),
          COALESCE(JSON_AS_VARCHAR("flex[LAST].value"), 'NULL')
        FROM {WRAPPER_SCHEMA}.DOCS
        ORDER BY "doc_id"
    """)
    assert_equal(
        array_variant_rows,
        [
            ("101", "NULL", "NULL", "NULL", "MISSING", "NULL"),
            ("102", "2", "arr-2", "head", "STRING", "arr-2"),
            ("103", "NULL", "NULL", "NULL", "MISSING", "NULL"),
            ("104", "NULL", "NULL", "NULL", "MISSING", "NULL"),
        ],
        "array-branch variant navigation",
    )

    filter_rows = fetch_all(f"""
        SELECT CAST("doc_id" AS VARCHAR(10))
        FROM {WRAPPER_SCHEMA}.DOCS
        WHERE (JSON_TYPEOF("flex") = 'OBJECT' AND "flex.note" = 'object-note')
           OR (JSON_TYPEOF("flex") = 'ARRAY' AND "flex[LAST].value" = 'arr-2')
           OR (JSON_TYPEOF("flex") = 'STRING' AND JSON_AS_VARCHAR("flex") = 'scalar-fallback')
           OR JSON_IS_EXPLICIT_NULL("flex")
        ORDER BY "doc_id"
    """)
    assert_equal(filter_rows, [("101",), ("102",), ("103",), ("104",)], "mixed variant filtering")

    print("-- wrapper variant semantics regression --")
    print("root variant rows:", root_variant_rows)
    print("object branch rows:", object_variant_rows)
    print("array branch rows:", array_variant_rows)
    print("filter rows:", filter_rows)


if __name__ == "__main__":
    main()
