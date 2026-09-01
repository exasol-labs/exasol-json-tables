#!/usr/bin/env python3

"""COMPILE_SQL: the same rewrite, reached without a session preprocessor.

The acceptance test is deliberately harsh: every statement here is compiled and then run
in a session whose preprocessor is explicitly NULL. If the compiled SQL needed the
session, these queries fail.
"""

import json

import _bootstrap  # noqa: F401

from personal_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views
from exasol_json_tables.compile_sql_tool import (
    COMPILE_RESULT_COLUMNS,
    compile_or_raise,
    compile_sql,
    install_compile_sql,
)


COMPILE_SCHEMA = "JVS_COMPILE_TEST"

SOURCE_A = "JVS_SRC"
WRAPPER_A = "JSON_VIEW_CMP"
HELPER_A = "JSON_VIEW_CMP_INTERNAL"

SOURCE_B = "JVS_CMP_SRC_B"
WRAPPER_B = "JSON_VIEW_CMP_B"
HELPER_B = "JSON_VIEW_CMP_B_INTERNAL"

PP_SCHEMA = "JVS_CMP_PP"
PP_SCRIPT = "JSON_CMP_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def cleanup(con) -> None:
    for schema in [COMPILE_SCHEMA, PP_SCHEMA, HELPER_A, WRAPPER_A, HELPER_B, WRAPPER_B, SOURCE_B]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def install_second_family(con) -> None:
    """A package that shares nothing with the first, so a join across them is a real test."""
    for statement in [
        f'DROP SCHEMA IF EXISTS "{SOURCE_B}" CASCADE',
        f'CREATE SCHEMA "{SOURCE_B}"',
        f'OPEN SCHEMA "{SOURCE_B}"',
        'CREATE OR REPLACE TABLE ORDERS ("_id" DECIMAL(18,0) NOT NULL, "sample_id" DECIMAL(18,0),'
        ' "total" DECIMAL(18,2), "ship|object" DECIMAL(18,0), "lines|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "ORDERS_ship" ("_id" DECIMAL(18,0) NOT NULL, "city" VARCHAR(100))',
        'CREATE OR REPLACE TABLE "ORDERS_lines_arr" ("_parent" DECIMAL(18,0) NOT NULL,'
        ' "_pos" DECIMAL(18,0) NOT NULL, "_value" VARCHAR(100))',
        "INSERT INTO \"ORDERS_ship\" VALUES (10, 'Berlin')",
        "INSERT INTO \"ORDERS_ship\" VALUES (11, 'Lisbon')",
        # The `|array` marker carries the element count, so it must agree with the child rows.
        "INSERT INTO ORDERS VALUES (1, 1, 99.50, 10, 2)",
        "INSERT INTO ORDERS VALUES (2, 2, 12.25, 11, 1)",
        "INSERT INTO \"ORDERS_lines_arr\" VALUES (1, 0, 'first-line')",
        "INSERT INTO \"ORDERS_lines_arr\" VALUES (1, 1, 'second-line')",
        "INSERT INTO \"ORDERS_lines_arr\" VALUES (2, 0, 'only-line')",
        "OPEN SCHEMA SYS",
    ]:
        con.execute(statement)
    install_wrapper_views(con, source_schema=SOURCE_B, wrapper_schema=WRAPPER_B, helper_schema=HELPER_B)


def compile_and_run(con, sql: str):
    return con.execute(compile_or_raise(con, sql, schema=COMPILE_SCHEMA)).fetchall()


def main() -> None:
    con = connect()
    try:
        cleanup(con)
        install_source_fixture(con, include_deep_fixture=True)
        install_wrapper_views(con, source_schema=SOURCE_A, wrapper_schema=WRAPPER_A, helper_schema=HELPER_A)
        install_second_family(con)

        packages = install_compile_sql(
            con,
            schema=COMPILE_SCHEMA,
            wrapper_schemas=[WRAPPER_A, WRAPPER_B],
        )
        assert_equal(
            sorted(str(manifest["publicSchema"]) for manifest in packages),
            sorted([WRAPPER_A, WRAPPER_B]),
            "packages served by the compile script",
        )

        # Everything below runs with no preprocessor. That is the whole point.
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")

        assert_equal(
            compile_and_run(
                con,
                f'SELECT CAST("id" AS VARCHAR(10)), "meta.info.note" FROM {WRAPPER_A}.SAMPLE ORDER BY "id"',
            ),
            [("1", "deep"), ("2", None), ("3", None)],
            "nested object path",
        )
        assert_equal(
            compile_and_run(
                con,
                f'SELECT CAST("id" AS VARCHAR(10)), "tags[FIRST]", CAST("tags[SIZE]" AS VARCHAR(10))'
                f' FROM {WRAPPER_A}.SAMPLE ORDER BY "id"',
            ),
            [("1", "red", "2"), ("2", "green", "1"), ("3", None, None)],
            "bracket access and array size",
        )
        assert_equal(
            compile_and_run(
                con,
                f'SELECT CAST(s."id" AS VARCHAR(10)), item."label" FROM {WRAPPER_A}.SAMPLE s'
                f' JOIN item IN s."items" ORDER BY 1, 2',
            ),
            [("1", "A"), ("1", "B"), ("2", "C")],
            "object-array iterator",
        )
        assert_equal(
            compile_and_run(
                con,
                f'SELECT CAST(s."id" AS VARCHAR(10)), tag FROM {WRAPPER_A}.SAMPLE s'
                f' JOIN VALUE tag IN s."tags" ORDER BY 1, 2',
            ),
            [("1", "blue"), ("1", "red"), ("2", "green")],
            "scalar-array VALUE iterator",
        )
        assert_equal(
            compile_and_run(
                con,
                f'SELECT CAST("id" AS VARCHAR(10)),'
                f' CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN 1 ELSE 0 END'
                f' FROM {WRAPPER_A}.SAMPLE ORDER BY "id"',
            ),
            [("1", 0), ("2", 1), ("3", 0)],
            "semantic helper",
        )

        # The ask A1 exists for: one statement across two packages, which one
        # preprocessor per session cannot express.
        cross_package_sql = (
            f'SELECT CAST(s."id" AS VARCHAR(10)), s."meta.info.note",'
            f' CAST(o."total" AS VARCHAR(10)), o."ship.city", CAST(o."lines[SIZE]" AS VARCHAR(10))'
            f' FROM {WRAPPER_A}.SAMPLE s'
            f' JOIN {WRAPPER_B}.ORDERS o ON o."sample_id" = s."id" ORDER BY 1'
        )
        assert_equal(
            compile_and_run(con, cross_package_sql),
            [("1", "deep", "99.5", "Berlin", "2"), ("2", None, "12.25", "Lisbon", "1")],
            "cross-package join",
        )
        cross_package_result = compile_sql(con, cross_package_sql, schema=COMPILE_SCHEMA)
        cross_plan = json.loads(cross_package_result["PLAN_JSON"])
        assert_equal(cross_plan["rewritten"], True, "cross-package plan rewritten flag")
        assert_equal(cross_plan["packageCount"], 2, "cross-package plan package count")
        assert_equal(
            sorted(package["publicSchema"] for package in cross_plan["referencedPackages"]),
            sorted([WRAPPER_A, WRAPPER_B]),
            "cross-package plan referenced packages",
        )

        # A statement with nothing to rewrite comes back untouched, not mangled.
        passthrough = compile_sql(con, "SELECT 1 FROM DUAL", schema=COMPILE_SCHEMA)
        assert_equal(passthrough["STATUS"], "OK", "passthrough status")
        assert_equal(passthrough["GENERATED_SQL"], "SELECT 1 FROM DUAL", "passthrough SQL")
        assert_equal(json.loads(passthrough["PLAN_JSON"])["rewritten"], False, "passthrough rewritten flag")
        assert_equal(passthrough["ERROR_CODE"], None, "passthrough error code")
        assert_equal(passthrough["CLARIFICATION_JSON"], None, "passthrough clarification")

        # A refusal arrives as data before execution, not as an exception mid-query.
        for label, sql, expected_code, expected_path in [
            ("wildcard", f'SELECT "tags[*]" FROM {WRAPPER_A}.SAMPLE', "JVS-PATH-ERROR", "tags[*]"),
            (
                "value over object array",
                f'SELECT s."id" FROM {WRAPPER_A}.SAMPLE s JOIN VALUE item IN s."items"',
                "JVS-ITER-ERROR",
                "items",
            ),
        ]:
            result = compile_sql(con, sql, schema=COMPILE_SCHEMA)
            assert_equal(result["STATUS"], "ERROR", f"{label} status")
            assert_equal(result["ERROR_CODE"], expected_code, f"{label} error code")
            assert_equal(result["GENERATED_SQL"], None, f"{label} generated sql")
            clarification = json.loads(result["CLARIFICATION_JSON"])
            assert_equal(clarification["code"], expected_code, f"{label} clarification code")
            assert_equal(clarification["path"], expected_path, f"{label} clarification path")
            assert_equal(
                sorted(clarification["allowedSchemas"]),
                sorted([WRAPPER_A, WRAPPER_B]),
                f"{label} clarification allowed schemas",
            )

        # A statement outside the configured schemas is a scope refusal, with no path.
        scope_result = compile_sql(con, f'SELECT "meta.info.note" FROM {SOURCE_A}.SAMPLE', schema=COMPILE_SCHEMA)
        assert_equal(scope_result["ERROR_CODE"], "JVS-SCOPE-ERROR", "scope error code")
        assert_equal("path" in json.loads(scope_result["CLARIFICATION_JSON"]), False, "scope error has no path")

        assert_equal(list(compile_sql(con, "SELECT 1", schema=COMPILE_SCHEMA)), list(COMPILE_RESULT_COLUMNS), "result columns")

        # Compiling and preprocessing are the same rewrite, so they must agree.
        preprocessed_sql = (
            f'SELECT CAST(s."id" AS VARCHAR(10)), s."meta.info.note", item."label"'
            f' FROM {WRAPPER_A}.SAMPLE s JOIN item IN s."items" ORDER BY 1, 3'
        )
        compiled_rows = compile_and_run(con, preprocessed_sql)
        # The fixture helpers share one generated-manifest path in dist/, and package B
        # wrote it last, so re-generate package A's before building its preprocessor.
        install_wrapper_views(con, source_schema=SOURCE_A, wrapper_schema=WRAPPER_A, helper_schema=HELPER_A)
        install_wrapper_preprocessor(
            con,
            [WRAPPER_A],
            [HELPER_A],
            schema_name=PP_SCHEMA,
            script_name=PP_SCRIPT,
        )
        con.execute(f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{PP_SCHEMA}"."{PP_SCRIPT}"')
        try:
            preprocessed_rows = con.execute(preprocessed_sql).fetchall()
        finally:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        assert_equal(compiled_rows, preprocessed_rows, "compiled rows vs preprocessed rows")
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        try:
            cleanup(con)
        finally:
            con.close()

    print("-- compile-sql regression --")
    print("verified COMPILE_SQL compiles and runs with no session preprocessor,")
    print("including a cross-package join, structured refusals, and preprocessor parity")


if __name__ == "__main__":
    main()
