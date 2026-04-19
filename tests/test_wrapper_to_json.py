#!/usr/bin/env python3

from __future__ import annotations

import json

import _bootstrap  # noqa: F401

from _fixture_expected_json import bigdoc_fixture_documents, deepdoc_fixture_documents, sample_fixture_documents
from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_TO_JSON_PP"
PREPROCESSOR_SCRIPT = "JSON_TO_JSON_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_contains(text: str, expected: str, label: str) -> None:
    if expected not in text:
        raise AssertionError(f"{label} mismatch.\nExpected substring: {expected!r}\nActual: {text}")


def fetch_json_rows(con, sql: str) -> list[object]:
    return [json.loads(row[0]) for row in con.execute(sql).fetchall()]


def fetch_error_text(con, sql: str) -> str:
    try:
        con.execute(sql).fetchall()
    except Exception as exc:
        return str(exc)
    raise AssertionError(f"Expected query to fail: {sql}")


def project_top_level(rows: list[dict[str, object]], keys: list[str]) -> list[dict[str, object]]:
    projected: list[dict[str, object]] = []
    for row in rows:
        projected_row: dict[str, object] = {}
        for key in keys:
            if key in row:
                projected_row[key] = row[key]
        projected.append(projected_row)
    return projected


def install_big_number_fixture(con) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE BIGDOC (
          "_id" DECIMAL(18,0) NOT NULL,
          "label" VARCHAR(100),
          "big" DECIMAL(36,0),
          "big|n" BOOLEAN
        )
        """
    )
    con.execute(
        """
        INSERT INTO BIGDOC VALUES
          (1, 'huge', CAST(123456789012345678901234567890123456 AS DECIMAL(36,0)), FALSE),
          (2, 'null', NULL, TRUE),
          (3, 'small', 7, FALSE)
        """
    )


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
        install_big_number_fixture(con)
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

        sample_expected = sample_fixture_documents()
        deepdoc_expected = deepdoc_fixture_documents()
        bigdoc_expected = bigdoc_fixture_documents()

        sample_full_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(*) FROM {WRAPPER_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        deepdoc_full_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(*) FROM {WRAPPER_SCHEMA}.DEEPDOC ORDER BY "_id"',
        )
        bigdoc_full_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(*) FROM {WRAPPER_SCHEMA}.BIGDOC ORDER BY "_id"',
        )
        assert_equal(sample_full_rows, sample_expected, "TO_JSON(*) SAMPLE")
        assert_equal(deepdoc_full_rows, deepdoc_expected, "TO_JSON(*) DEEPDOC")
        assert_equal(bigdoc_full_rows, bigdoc_expected, "TO_JSON(*) BIGDOC")

        scalar_subset_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("id", "name", "value", "note") FROM {WRAPPER_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        recursive_subset_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("meta", "items") FROM {WRAPPER_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        assert_equal(
            scalar_subset_rows,
            project_top_level(sample_expected, ["id", "name", "value", "note"]),
            "TO_JSON scalar subset",
        )
        assert_equal(
            recursive_subset_rows,
            project_top_level(sample_expected, ["meta", "items"]),
            "TO_JSON recursive subset",
        )
        bigdoc_subset_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("label", "big") FROM {WRAPPER_SCHEMA}.BIGDOC ORDER BY "_id"',
        )
        assert_equal(
            bigdoc_subset_rows,
            project_top_level(bigdoc_expected, ["label", "big"]),
            "TO_JSON large-number subset",
        )

        base_name_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("child", "tags") FROM {WRAPPER_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        visible_name_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("child|object", "tags|array") FROM {WRAPPER_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        assert_equal(base_name_rows, visible_name_rows, "base-name vs visible-name TO_JSON subset")
        assert_equal(
            base_name_rows,
            project_top_level(sample_expected, ["child", "tags"]),
            "normalized child/tags subset",
        )

        joined_subset_rows = fetch_json_rows(
            con,
            f"""
            SELECT TO_JSON(s."id", s."meta")
            FROM {WRAPPER_SCHEMA}.SAMPLE s
            JOIN {WRAPPER_SCHEMA}.SAMPLE peer
              ON s."_id" = peer."_id"
            ORDER BY s."_id"
            """,
        )
        assert_equal(
            joined_subset_rows,
            project_top_level(sample_expected, ["id", "meta"]),
            "qualified TO_JSON subset in joined query",
        )
        repeated_call_rows = con.execute(
            f"""
            SELECT
              TO_JSON(*) AS full_json,
              TO_JSON("meta") AS meta_json,
              TO_JSON("meta") AS meta_json_again,
              TO_JSON("id", "meta") AS id_meta_json
            FROM {WRAPPER_SCHEMA}.SAMPLE
            ORDER BY "_id"
            """
        ).fetchall()
        assert_equal(
            [json.loads(row[0]) for row in repeated_call_rows],
            sample_expected,
            "repeated TO_JSON full rows",
        )
        assert_equal(
            [json.loads(row[1]) for row in repeated_call_rows],
            project_top_level(sample_expected, ["meta"]),
            "repeated TO_JSON meta rows",
        )
        assert_equal(
            [json.loads(row[2]) for row in repeated_call_rows],
            project_top_level(sample_expected, ["meta"]),
            "repeated TO_JSON meta rows again",
        )
        assert_equal(
            [json.loads(row[3]) for row in repeated_call_rows],
            project_top_level(sample_expected, ["id", "meta"]),
            "repeated TO_JSON id/meta rows",
        )

        joined_unqualified_error = fetch_error_text(
            con,
            f"""
            SELECT TO_JSON("id", "meta")
            FROM {WRAPPER_SCHEMA}.SAMPLE s
            JOIN {WRAPPER_SCHEMA}.SAMPLE peer
              ON s."_id" = peer."_id"
            ORDER BY s."_id"
            """,
        )
        joined_star_error = fetch_error_text(
            con,
            f"""
            SELECT TO_JSON(*)
            FROM {WRAPPER_SCHEMA}.SAMPLE s
            JOIN {WRAPPER_SCHEMA}.SAMPLE peer
              ON s."_id" = peer."_id"
            ORDER BY s."_id"
            """,
        )
        path_argument_error = fetch_error_text(
            con,
            f'SELECT TO_JSON("meta.info.note") FROM {WRAPPER_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        mixed_root_error = fetch_error_text(
            con,
            f"""
            SELECT TO_JSON(s."id", peer."name")
            FROM {WRAPPER_SCHEMA}.SAMPLE s
            JOIN {WRAPPER_SCHEMA}.SAMPLE peer
              ON s."_id" = peer."_id"
            ORDER BY s."_id"
            """,
        )

        assert_contains(
            joined_unqualified_error,
            "Unqualified TO_JSON arguments are not supported in joined queries.",
            "joined unqualified TO_JSON error",
        )
        assert_contains(
            joined_star_error,
            "TO_JSON(*) is not supported in joined queries.",
            "joined TO_JSON(*) error",
        )
        assert_contains(
            path_argument_error,
            'Nested paths such as "meta.info.note" and bracket expressions such as "tags[SIZE]" are not supported.',
            "nested-path TO_JSON error",
        )
        assert_contains(
            mixed_root_error,
            "All TO_JSON subset arguments must resolve to the same row source.",
            "mixed-root TO_JSON error",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    print("-- wrapper TO_JSON regression --")
    print("validated full-row and subset TO_JSON rewrite behavior on SAMPLE and DEEPDOC")


if __name__ == "__main__":
    main()
