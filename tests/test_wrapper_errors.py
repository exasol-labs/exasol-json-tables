#!/usr/bin/env python3

from pathlib import Path
import subprocess

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


def assert_contains_all(text: str, fragments: list[str], label: str) -> None:
    missing = [fragment for fragment in fragments if fragment not in text]
    if missing:
        raise AssertionError(f"{label} missing fragments {missing!r}.\nActual text: {text}")


def assert_query_error(con, sql: str, expected_fragments: list[str], label: str) -> None:
    try:
        con.execute(sql).fetchall()
    except Exception as exc:
        message = str(exc)
        assert_contains_all(message, expected_fragments, label)
        return
    raise AssertionError(f"{label} should have failed, but the query succeeded: {sql}")


def assert_query_rows(con, sql: str, expected_rows, label: str) -> None:
    actual_rows = con.execute(sql).fetchall()
    if actual_rows != expected_rows:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected_rows}\nActual:   {actual_rows}")


def assert_subprocess_error(cmd: list[str], expected_fragments: list[str], label: str) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        raise AssertionError(f"{label} should have failed, but exited successfully: {' '.join(cmd)}")
    combined_output = (result.stdout or "") + (result.stderr or "")
    assert_contains_all(combined_output, expected_fragments, label)


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
        install_wrapper_views(
            con,
            source_schema="JVS_SRC",
            wrapper_schema="JSON_VIEW",
            helper_schema="JSON_VIEW_INTERNAL",
            generate_preprocessor=True,
        )
        install_wrapper_preprocessor(con, ["JSON_VIEW"], ["JSON_VIEW_INTERNAL"])

        assert_query_rows(
            con,
            'SELECT CAST("id" AS VARCHAR(10)) FROM JVS_SRC.SAMPLE ORDER BY "id"',
            [("1",), ("2",), ("3",)],
            "regular table baseline query",
        )

        assert_query_error(
            con,
            'SELECT "tags[NOPE]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", 'Array selector "NOPE" must be ?, PARAM, or a visible field on the current row'],
            "unsupported selector error",
        )
        assert_query_error(
            con,
            'SELECT "tags[-1]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Negative array indexes are not supported yet", "Use LAST"],
            "negative index error",
        )
        assert_query_error(
            con,
            'SELECT "tags[*]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Wildcard selectors are not supported yet", "JOIN ... IN"],
            "wildcard selector error",
        )
        assert_query_error(
            con,
            'SELECT "tags[1:3]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Array slices are not supported yet", "_index"],
            "array slice error",
        )
        assert_query_error(
            con,
            'SELECT "tags[id + 1]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", 'Unsupported array selector "id + 1"', "direct field names"],
            "complex expression selector error",
        )
        assert_query_error(
            con,
            'SELECT "items[0][1]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Chained array indexing is not supported"],
            "nested array index error",
        )
        assert_query_error(
            con,
            'SELECT "meta.items[SIZE].value" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "SIZE must be the last selector in a path"],
            "size non-terminal error",
        )
        assert_query_error(
            con,
            'SELECT CAST("id" AS VARCHAR(10)), "items.value" FROM JSON_VIEW.SAMPLE ORDER BY "id"',
            ["JVS-PATH-ERROR", '"items.value"', 'JOIN ... IN row."items"', '"items[index]"'],
            "array property dot-traversal guidance error",
        )
        assert_query_error(
            con,
            'SELECT "meta..note" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Empty path segment is not allowed"],
            "empty path segment error",
        )
        assert_query_error(
            con,
            'SELECT s."meta.info.missing" FROM JSON_VIEW.SAMPLE s ORDER BY s."_id"',
            ["JVS-PATH-ERROR", 'Field "missing" is not visible on object path "meta.info"', "describe wrapper --json"],
            "missing nested field error",
        )
        assert_query_error(
            con,
            'SELECT "name.value" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", 'Path step "name" resolves to a scalar value'],
            "scalar path navigation error",
        )
        assert_query_error(
            con,
            'SELECT "meta[0]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", 'Path step "meta" resolves to an object', "dotted navigation"],
            "object bracket guidance error",
        )
        assert_query_error(
            con,
            'SELECT "meta." FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Path cannot end with '.'"],
            "trailing dot path error",
        )
        assert_query_error(
            con,
            'SELECT "tags[]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Empty array selector is not allowed"],
            "empty array selector error",
        )
        assert_query_error(
            con,
            'SELECT "[0]" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "array selector must follow a property name"],
            "selector without property error",
        )
        assert_query_error(
            con,
            'SELECT "tags[" FROM JSON_VIEW.SAMPLE',
            ["JVS-PATH-ERROR", "Missing closing ] in array selector"],
            "missing closing bracket error",
        )
        assert_query_error(
            con,
            'SELECT "child.value" FROM (SELECT * FROM JSON_VIEW.SAMPLE) s',
            ["JVS-SCOPE-ERROR", "JSON path syntax", "derived tables"],
            "unsupported query shape error",
        )
        assert_query_error(
            con,
            'SELECT s."child.value" FROM (SELECT * FROM JSON_VIEW.SAMPLE) s',
            ["JVS-PATH-ERROR", "child.value", "derived-table aliases"],
            "qualified derived-table path error",
        )
        assert_query_error(
            con,
            'SELECT "child.value" FROM JVS_SRC.SAMPLE',
            ["JVS-SCOPE-ERROR", "JSON path syntax", "JSON_VIEW"],
            "regular table path scope error",
        )
        assert_query_error(
            con,
            'SELECT "tags[0]" FROM JVS_SRC.SAMPLE',
            ["JVS-SCOPE-ERROR", "JSON path syntax", "JSON_VIEW"],
            "regular table array scope error",
        )
        assert_query_error(
            con,
            '''
            SELECT s."id"
            FROM JVS_SRC.SAMPLE s
            JOIN item IN s."items"
            ''',
            ["JVS-SCOPE-ERROR", "JSON array iteration syntax", "JSON_VIEW"],
            "regular table iterator scope error",
        )
        assert_query_error(
            con,
            '''
            SELECT s."id"
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."items[0]"
            ''',
            ["JVS-ITER-ERROR", "Iterator paths must name an array property directly", "scalar bracket access"],
            "indexed iterator path error",
        )
        assert_query_error(
            con,
            '''
            SELECT s."id"
            FROM JSON_VIEW.SAMPLE s
            JOIN VALUE tag IN s."tags"
            LEFT JOIN VALUE nested IN tag."extras"
            ''',
            ["JVS-ITER-ERROR", "Scalar VALUE iterators cannot be used as the root"],
            "value iterator root error",
        )
        assert_query_error(
            con,
            '''
            SELECT JSON_TYPEOF(tag)
            FROM JSON_VIEW.SAMPLE s
            JOIN VALUE tag IN s."tags"
            ''',
            ["JVS-FUNCTION-ERROR", "JSON_TYPEOF", "VALUE iterators"],
            "value iterator helper error",
        )
        assert_query_error(
            con,
            '''
            SELECT s."id"
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."meta.items[LAST]"
            ''',
            ["JVS-ITER-ERROR", "Iterator paths must name an array property directly"],
            "terminal indexed iterator path error",
        )
        assert_query_error(
            con,
            '''
            SELECT
              tag."extras[LAST]"
            FROM JSON_VIEW.SAMPLE s
            JOIN VALUE tag IN s."tags"
            ''',
            ["JVS-PATH-ERROR", "extras[LAST]", "VALUE iterators"],
            "value iterator qualified path error",
        )
        assert_query_error(
            con,
            "SELECT JSON_IS_EXPLICIT_NULL() FROM JSON_VIEW.SAMPLE",
            ["JVS-FUNCTION-ERROR", "JSON_IS_EXPLICIT_NULL", "Expected exactly one argument"],
            "zero-argument helper error",
        )
        assert_query_error(
            con,
            'SELECT JSON_IS_EXPLICIT_NULL("note", "name") FROM JSON_VIEW.SAMPLE',
            ["JVS-FUNCTION-ERROR", "JSON_IS_EXPLICIT_NULL", "Expected exactly one argument"],
            "multi-argument helper error",
        )
        assert_query_error(
            con,
            'SELECT JSON_IS_EXPLICIT_NULL("note" FROM JSON_VIEW.SAMPLE',
            ["JVS-FUNCTION-ERROR", "JSON_IS_EXPLICIT_NULL", "Missing closing parenthesis"],
            "missing closing parenthesis helper error",
        )
        assert_query_error(
            con,
            'SELECT JSON_TYPEOF() FROM JSON_VIEW.SAMPLE',
            ["JVS-FUNCTION-ERROR", "JSON_TYPEOF", "Expected exactly one argument"],
            "wrapper variant typeof zero-argument error",
        )
        assert_query_error(
            con,
            'SELECT JSON_AS_DECIMAL("value", "name") FROM JSON_VIEW.SAMPLE',
            ["JVS-FUNCTION-ERROR", "JSON_AS_DECIMAL", "Expected exactly one argument"],
            "wrapper variant decimal multi-argument error",
        )
        assert_query_error(
            con,
            'SELECT JSON_IS_EXPLICIT_NULL("note") FROM JVS_SRC.SAMPLE',
            ["JVS-SCOPE-ERROR", "JSON helper functions", "JSON_VIEW"],
            "regular table helper scope error",
        )
        assert_query_error(
            con,
            'SELECT JSON_IS_EXPLICIT_NULL("note") FROM JSON_VIEW_INTERNAL.SAMPLE',
            ["JVS-SCOPE-ERROR", "JSON helper functions", "JSON_VIEW"],
            "helper schema helper scope error",
        )
        assert_query_error(
            con,
            'SELECT JSON_TYPEOF("value") FROM JVS_SRC.SAMPLE',
            ["JVS-SCOPE-ERROR", "JSON helper functions", "JSON_VIEW"],
            "regular table variant helper scope error",
        )
        assert_query_error(
            con,
            '''
            SELECT s."id"
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."items"
            WHERE JSON_IS_EXPLICIT_NULL("note")
            ''',
            ["JVS-FUNCTION-ERROR", "JSON_IS_EXPLICIT_NULL", "Unqualified helper arguments are not supported in joined queries"],
            "wrapper unqualified helper in joined query error",
        )
        assert_query_error(
            con,
            '''
            SELECT s."id"
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."items"
            WHERE JSON_TYPEOF("value") = 'NUMBER'
            ''',
            ["JVS-FUNCTION-ERROR", "JSON_TYPEOF", "Unqualified helper arguments are not supported in joined queries"],
            "wrapper variant helper in joined query error",
        )
        assert_query_error(
            con,
            'SELECT JSON_IS_EXPLICIT_NULL("child.value") FROM (SELECT * FROM JSON_VIEW.SAMPLE) s',
            ["JVS-SCOPE-ERROR", "JSON path syntax", "derived tables"],
            "wrapper derived-table helper scope error",
        )
        assert_query_error(
            con,
            'SELECT JSON_TYPEOF(s."value") FROM (SELECT * FROM JSON_VIEW.SAMPLE) s',
            ["JVS-SCOPE-ERROR", "JSON helper functions", "derived tables"],
            "qualified derived-table helper scope error",
        )

        assert_query_rows(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
              CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END
            FROM JSON_VIEW.SAMPLE
            ORDER BY "id"
            """,
            [("1", "0", "0"), ("2", "1", "0"), ("3", "0", "1")],
            "wrapper explicit-null helper query",
        )

        assert_query_rows(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              COALESCE(JSON_TYPEOF("value"), 'MISSING'),
              COALESCE(JSON_AS_VARCHAR("value"), 'NULL'),
              COALESCE(CAST(JSON_AS_DECIMAL("value") AS VARCHAR(60)), 'NULL'),
              COALESCE(CAST(JSON_AS_BOOLEAN("meta.flag") AS VARCHAR(10)), 'NULL')
            FROM JSON_VIEW.SAMPLE
            ORDER BY "id"
            """,
            [("1", "NUMBER", "42", "42", "TRUE"), ("2", "STRING", "43", "43", "FALSE"), ("3", "NULL", "NULL", "NULL", "NULL")],
            "wrapper variant helper query",
        )

        packaged_wrapper_sql = (ROOT / "dist" / "json_wrapper_preprocessor_packaged_test.sql").read_text()
        assert_contains_all(
            packaged_wrapper_sql,
            [
                "Configured function names: JSON_IS_EXPLICIT_NULL, JNULL, JSON_TYPEOF, JSON_AS_VARCHAR, JSON_AS_DECIMAL, JSON_AS_BOOLEAN, TO_JSON",
                "JSON syntax allowed only for configured JSON schemas: JSON_VIEW",
                "Helper rewrite mode: wrapper semantic helpers",
            ],
            "packaged wrapper preprocessor output",
        )

        invalid_output_path = Path(ROOT / "dist" / "should_not_exist.sql")
        assert_subprocess_error(
            [
                "python3",
                str(ROOT / "tools" / "generate_preprocessor_sql.py"),
                "--function-name",
                "bad-name",
                "--output",
                str(invalid_output_path),
            ],
            ["Function name must be an unquoted SQL identifier"],
            "shared generator identifier validation",
        )
        assert_subprocess_error(
            [
                "python3",
                str(ROOT / "tools" / "generate_wrapper_views_sql.py"),
                "--source-schema",
                "JSON_VIEW",
                "--wrapper-schema",
                "JSON_VIEW",
                "--output",
                str(ROOT / "dist" / "should_not_write_wrapper_views.sql"),
                "--manifest-output",
                str(ROOT / "dist" / "should_not_write_wrapper_manifest.json"),
            ],
            ["must all be distinct", "source=JSON_VIEW", "wrapper=JSON_VIEW"],
            "wrapper generator distinct schema validation",
        )
        assert_subprocess_error(
            [
                "python3",
                str(ROOT / "tools" / "generate_wrapper_preprocessor_sql.py"),
                "--wrapper-schema",
                "JSON_VIEW",
                "--helper-schema",
                "JSON_VIEW",
                "--output",
                str(ROOT / "dist" / "should_not_write_wrapper_preprocessor.sql"),
            ],
            ["must differ from its helper schema"],
            "wrapper preprocessor schema-pair validation",
        )

        print("-- wrapper error regression --")
        print("validated wrapper helper errors, scope guards, semantic helpers, and generator validation")
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()
