#!/usr/bin/env python3

from pathlib import Path
import subprocess

from nano_support import ROOT, bundle_adapter, connect, install_preprocessor, install_virtual_schema_fixture


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


def assert_subprocess_error(cmd: list[str], expected_fragments: list[str], label: str) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        raise AssertionError(f"{label} should have failed, but exited successfully: {' '.join(cmd)}")
    combined_output = (result.stdout or "") + (result.stderr or "")
    assert_contains_all(combined_output, expected_fragments, label)


def main() -> None:
    adapter_code = bundle_adapter()
    con = connect()
    try:
        install_virtual_schema_fixture(con, adapter_code, include_deep_fixture=True)
        install_preprocessor(con, ["JSON_IS_EXPLICIT_NULL", "JNULL"], rewrite_path_identifiers=True)

        assert_query_error(
            con,
            'SELECT "tags[NOPE]" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", 'Unsupported array selector "NOPE"'],
            "unsupported selector error",
        )

        assert_query_error(
            con,
            'SELECT "tags[-1]" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Negative array indexes are not supported yet", "Use LAST"],
            "negative index error",
        )

        assert_query_error(
            con,
            'SELECT "tags[*]" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Wildcard selectors are not supported yet", "JOIN ... IN"],
            "wildcard selector error",
        )

        assert_query_error(
            con,
            'SELECT "tags[1:3]" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Array slices are not supported yet", "_index"],
            "array slice error",
        )

        assert_query_error(
            con,
            'SELECT "items[0][1]" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Chained array indexing is not supported"],
            "nested array index error",
        )

        assert_query_error(
            con,
            'SELECT "meta.items[SIZE].value" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "SIZE must be the last selector in a path"],
            "size non-terminal error",
        )

        assert_query_error(
            con,
            'SELECT "meta..note" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Empty path segment is not allowed"],
            "empty path segment error",
        )

        assert_query_error(
            con,
            'SELECT "meta." FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Path cannot end with '.'"],
            "trailing dot path error",
        )

        assert_query_error(
            con,
            'SELECT "tags[]" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Empty array selector is not allowed"],
            "empty array selector error",
        )

        assert_query_error(
            con,
            'SELECT "[0]" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "array selector must follow a property name"],
            "selector without property error",
        )

        assert_query_error(
            con,
            'SELECT "tags[" FROM JSON_VS.SAMPLE',
            ["JVS-PATH-ERROR", "Missing closing ] in array selector"],
            "missing closing bracket error",
        )

        assert_query_error(
            con,
            'SELECT "child.value" FROM (SELECT * FROM JSON_VS.SAMPLE) s',
            ["JVS-PATH-ERROR", "single base table in FROM"],
            "unsupported query shape error",
        )

        assert_query_error(
            con,
            "SELECT JSON_IS_EXPLICIT_NULL() FROM JSON_VS.SAMPLE",
            ["JVS-FUNCTION-ERROR", "JSON_IS_EXPLICIT_NULL", "Expected exactly one argument"],
            "zero-argument helper error",
        )

        assert_query_error(
            con,
            'SELECT JSON_IS_EXPLICIT_NULL("note", "name") FROM JSON_VS.SAMPLE',
            ["JVS-FUNCTION-ERROR", "JSON_IS_EXPLICIT_NULL", "Expected exactly one argument"],
            "multi-argument helper error",
        )

        assert_query_error(
            con,
            'SELECT JSON_IS_EXPLICIT_NULL("note" FROM JSON_VS.SAMPLE',
            ["JVS-FUNCTION-ERROR", "JSON_IS_EXPLICIT_NULL", "Missing closing parenthesis"],
            "missing closing parenthesis helper error",
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
            "generator identifier validation",
        )

        print("-- preprocessor error regression --")
        print("validated unsupported selectors, malformed paths, unsupported query shapes, and helper arity errors")
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()
