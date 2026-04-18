#!/usr/bin/env python3

from __future__ import annotations

import json

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views
from in_session_wrapper_installer import install_wrapper_surface_in_session
from result_family_json_export import export_root_family_to_json
from result_family_materializer import (
    ResultTableSpec,
    SynthesizedFamilySpec,
    materialize_family_preserving_subset,
    materialize_synthesized_family,
)


SOURCE_SCHEMA = "JVS_SRC"
SOURCE_WRAPPER_SCHEMA = "JSON_VIEW"
SOURCE_HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
SOURCE_PP_SCHEMA = "JVS_WRAP_STUDY_PP"
SOURCE_PP_SCRIPT = "JSON_WRAPPER_STUDY_PREPROCESSOR"

SUBSET_SOURCE_SCHEMA = "JVS_RESULT_SRC"
SUBSET_WRAPPER_SCHEMA = "JSON_VIEW_RESULT"
SUBSET_HELPER_SCHEMA = "JSON_VIEW_RESULT_INTERNAL"
SUBSET_PP_SCHEMA = "JVS_RESULT_PP"
SUBSET_PP_SCRIPT = "JSON_WRAPPER_RESULT_PREPROCESSOR"

SUMMARY_SOURCE_SCHEMA = "JVS_SUMMARY_SRC"
SUMMARY_WRAPPER_SCHEMA = "JSON_VIEW_SUMMARY"
SUMMARY_HELPER_SCHEMA = "JSON_VIEW_SUMMARY_INTERNAL"
SUMMARY_PP_SCHEMA = "JVS_SUMMARY_PP"
SUMMARY_PP_SCRIPT = "JSON_WRAPPER_SUMMARY_PREPROCESSOR"

NESTED_SOURCE_SCHEMA = "JVS_NESTED_RESULT_SRC"
NESTED_WRAPPER_SCHEMA = "JSON_VIEW_NESTED_RESULT"
NESTED_HELPER_SCHEMA = "JSON_VIEW_NESTED_RESULT_INTERNAL"
NESTED_PP_SCHEMA = "JVS_NESTED_RESULT_PP"
NESTED_PP_SCRIPT = "JSON_WRAPPER_NESTED_RESULT_PREPROCESSOR"

TEMP_SOURCE_SCHEMA = "JVS_TEMP_RESULT_SRC"
TEMP_WRAPPER_SCHEMA = "JSON_VIEW_TEMP_RESULT"
TEMP_HELPER_SCHEMA = "JSON_VIEW_TEMP_RESULT_INTERNAL"
TEMP_PP_SCHEMA = "JVS_TEMP_RESULT_PP"
TEMP_PP_SCRIPT = "JSON_WRAPPER_TEMP_RESULT_PREPROCESSOR"
OUTPUT_PATH = ROOT / "dist" / "structured_result_materialization_results.json"

def qident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qqualified(schema: str, name: str) -> str:
    return f"{qident(schema)}.{qident(name)}"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def query_rows(con, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def activate_preprocessor(con, schema: str, script: str) -> None:
    con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema}.{script}")


def deactivate_preprocessor(con) -> None:
    con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")


def test_temporary_table_support(con) -> dict[str, Any]:
    attempts = {
        "local_temporary": "CREATE LOCAL TEMPORARY TABLE TMP_LOCAL_RESULT (id DECIMAL(18,0))",
        "temporary": "CREATE TEMPORARY TABLE TMP_RESULT (id DECIMAL(18,0))",
    }
    results: dict[str, Any] = {}
    for label, sql in attempts.items():
        try:
            con.execute(sql)
        except Exception as exc:
            results[label] = {
                "supported": False,
                "error": str(exc),
            }
        else:
            results[label] = {
                "supported": True,
            }
            con.execute(f"DROP TABLE IF EXISTS {sql.split()[4]}")
    return results


def test_local_temporary_wrapper_family(con) -> dict[str, Any]:
    deactivate_preprocessor(con)
    con.execute(f"DROP SCHEMA IF EXISTS {TEMP_SOURCE_SCHEMA} CASCADE")
    con.execute(f"DROP SCHEMA IF EXISTS {TEMP_WRAPPER_SCHEMA} CASCADE")
    con.execute(f"DROP SCHEMA IF EXISTS {TEMP_HELPER_SCHEMA} CASCADE")
    con.execute(f"CREATE SCHEMA {TEMP_SOURCE_SCHEMA}")
    con.execute(f"OPEN SCHEMA {TEMP_SOURCE_SCHEMA}")
    con.execute(
        """
        CREATE LOCAL TEMPORARY TABLE TMP_DOC (
          "_id" DECIMAL(18,0) NOT NULL,
          "doc_id" DECIMAL(18,0),
          "title" VARCHAR(100),
          "tags|array" DECIMAL(18,0)
        )
        """
    )
    con.execute(
        """
        CREATE LOCAL TEMPORARY TABLE "TMP_DOC_tags_arr" (
          "_parent" DECIMAL(18,0) NOT NULL,
          "_pos" DECIMAL(18,0) NOT NULL,
          "_value" VARCHAR(100)
        )
        """
    )
    con.execute(
        """
        INSERT INTO TMP_DOC VALUES
          (1, 1, 'alpha', 2),
          (2, 2, 'beta', 1),
          (3, 3, 'gamma', 0)
        """
    )
    con.execute(
        """
        INSERT INTO "TMP_DOC_tags_arr" VALUES
          (1, 0, 'red'),
          (1, 1, 'blue'),
          (2, 0, 'green')
        """
    )

    cli_result: dict[str, Any]
    try:
        install_wrapper_views(
            con,
            source_schema=TEMP_SOURCE_SCHEMA,
            wrapper_schema=f"{TEMP_WRAPPER_SCHEMA}_CLI",
            helper_schema=f"{TEMP_HELPER_SCHEMA}_CLI",
            generate_preprocessor=False,
        )
    except Exception as exc:
        cli_result = {
            "supported": False,
            "error": str(exc),
        }
    else:
        cli_tables = query_rows(
            con,
            f"""
            SELECT TABLE_NAME
            FROM SYS.EXA_ALL_TABLES
            WHERE TABLE_SCHEMA = '{TEMP_WRAPPER_SCHEMA}_CLI'
            ORDER BY TABLE_NAME
            """,
        )
        cli_result = {
            "supported": len(cli_tables) > 0,
            "tables": [table_name for (table_name,) in cli_tables],
        }

    install_result = install_wrapper_surface_in_session(
        con,
        source_schema=TEMP_SOURCE_SCHEMA,
        wrapper_schema=TEMP_WRAPPER_SCHEMA,
        helper_schema=TEMP_HELPER_SCHEMA,
        preprocessor_schema=TEMP_PP_SCHEMA,
        preprocessor_script=TEMP_PP_SCRIPT,
        activate_preprocessor_session=True,
    )

    wrapper_rows = query_rows(
        con,
        f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)),
          COALESCE("tags[LAST]", 'NULL')
        FROM {TEMP_WRAPPER_SCHEMA}.TMP_DOC
        ORDER BY "doc_id"
        """
    )
    expected_wrapper_rows = [
        ("1", "blue"),
        ("2", "green"),
        ("3", "NULL"),
    ]
    assert_equal(wrapper_rows, expected_wrapper_rows, "local temporary wrapper rows")

    rowset_rows = query_rows(
        con,
        f"""
        SELECT
          CAST(d."doc_id" AS VARCHAR(10)),
          COALESCE(CAST(tag._index AS VARCHAR(10)), 'NULL'),
          COALESCE(tag, 'NULL')
        FROM {TEMP_WRAPPER_SCHEMA}.TMP_DOC d
        LEFT JOIN VALUE tag IN d."tags"
        ORDER BY d."doc_id", tag._index
        """
    )
    expected_rowset_rows = [
        ("1", "0", "red"),
        ("1", "1", "blue"),
        ("2", "0", "green"),
        ("3", "NULL", "NULL"),
    ]
    assert_equal(rowset_rows, expected_rowset_rows, "local temporary rowset rows")

    other_session_result: dict[str, Any]
    other = connect()
    try:
        try:
            other.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {TEMP_PP_SCHEMA}.{TEMP_PP_SCRIPT}")
            other_rows = other.execute(
                f"""
                SELECT
                  CAST("doc_id" AS VARCHAR(10)),
                  COALESCE("tags[LAST]", 'NULL')
                FROM {TEMP_WRAPPER_SCHEMA}.TMP_DOC
                ORDER BY "doc_id"
                """
            ).fetchall()
        except Exception as exc:
            other_session_result = {
                "supported": False,
                "error": str(exc),
            }
        else:
            other_session_result = {
                "supported": True,
                "wrapperRows": other_rows,
            }
    finally:
        other.close()

    return {
        "cliGenerator": cli_result,
        "inSessionGenerator": {
            "supported": True,
            "manifestRoots": [root["tableName"] for root in install_result.manifest["roots"]],
            "wrapperRows": wrapper_rows,
            "rowsetRows": rowset_rows,
        },
        "crossSessionQuery": other_session_result,
    }


def expected_subset_json() -> list[dict[str, Any]]:
    return [
        {
            "id": 1,
            "name": "alpha",
            "note": "x",
            "child": {"value": "child-1"},
            "meta": {
                "flag": True,
                "info": {"note": "deep"},
                "items": [{"value": "m1"}, {"value": "m2"}],
            },
            "value": 42,
            "tags": ["red", "blue"],
            "items": [
                {
                    "value": "first",
                    "label": "A",
                    "optional": "x",
                    "amount": 7,
                    "enabled": True,
                    "nested": {
                        "note": "nested-a",
                        "score": 11,
                        "active": True,
                        "pick": 1,
                        "items": [{"value": "na-1"}, {"value": "na-2"}],
                    },
                },
                {
                    "value": "second",
                    "label": "B",
                    "optional": None,
                    "enabled": False,
                    "nested": {
                        "note": "nested-b",
                        "score": 12,
                        "active": False,
                        "pick": 0,
                        "items": [{"value": "nb-1"}],
                    },
                },
            ],
        },
        {
            "id": 2,
            "name": "beta",
            "note": None,
            "meta": {
                "flag": False,
                "items": [{"value": "m3"}],
            },
            "value": "43",
            "tags": ["green"],
            "items": [
                {
                    "value": "only",
                    "label": "C",
                    "amount": 5,
                }
            ],
        },
    ]


def clone_root_family_subset(con, root_table: str, root_filter_sql: str) -> dict[str, Any]:
    activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)
    original_rows = query_rows(
        con,
        f"""
        SELECT
          CAST("id" AS VARCHAR(10)),
          COALESCE("child.value", 'NULL'),
          COALESCE("meta.info.note", 'NULL'),
          COALESCE("tags[LAST]", 'NULL'),
          COALESCE("items[LAST].value", 'NULL')
        FROM {SOURCE_WRAPPER_SCHEMA}.SAMPLE
        WHERE {root_filter_sql}
        ORDER BY "id"
        """,
    )

    materialized = materialize_family_preserving_subset(
        con,
        source_helper_schema=SOURCE_HELPER_SCHEMA,
        target_schema=SUBSET_SOURCE_SCHEMA,
        root_table=root_table,
        root_filter_sql=root_filter_sql,
    )

    subset_manifest = install_wrapper_views(
        con,
        source_schema=SUBSET_SOURCE_SCHEMA,
        wrapper_schema=SUBSET_WRAPPER_SCHEMA,
        helper_schema=SUBSET_HELPER_SCHEMA,
        generate_preprocessor=True,
        preprocessor_schema=SUBSET_PP_SCHEMA,
        preprocessor_script=SUBSET_PP_SCRIPT,
    )
    install_wrapper_preprocessor(
        con,
        [SUBSET_WRAPPER_SCHEMA],
        [SUBSET_HELPER_SCHEMA],
        schema_name=SUBSET_PP_SCHEMA,
        script_name=SUBSET_PP_SCRIPT,
    )
    activate_preprocessor(con, SUBSET_PP_SCHEMA, SUBSET_PP_SCRIPT)

    subset_rows = query_rows(
        con,
        f"""
        SELECT
          CAST("id" AS VARCHAR(10)),
          COALESCE("child.value", 'NULL'),
          COALESCE("meta.info.note", 'NULL'),
          COALESCE("tags[LAST]", 'NULL'),
          COALESCE("items[LAST].value", 'NULL')
        FROM {SUBSET_WRAPPER_SCHEMA}.SAMPLE
        ORDER BY "id"
        """
    )
    assert_equal(subset_rows, original_rows, "family-preserving subset wrapper rows")

    subset_rowset_rows = query_rows(
        con,
        f"""
        SELECT
          CAST(s."id" AS VARCHAR(10)),
          CAST(item._index AS VARCHAR(10)),
          item.value,
          item.label
        FROM {SUBSET_WRAPPER_SCHEMA}.SAMPLE s
        JOIN item IN s."items"
        ORDER BY s."id", item._index
        """
    )
    expected_rowset_rows = [
        ("1", "0", "first", "A"),
        ("1", "1", "second", "B"),
        ("2", "0", "only", "C"),
    ]
    assert_equal(subset_rowset_rows, expected_rowset_rows, "family-preserving subset rowset rows")

    reconstructed_json = export_root_family_to_json(con, materialized_family=materialized)
    assert_equal(reconstructed_json, expected_subset_json(), "family-preserving subset reconstructed JSON")

    activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)
    return {
        "createdTables": materialized.created_tables,
        "relationshipsUsed": [
            (
                relationship.parent_table,
                relationship.child_table,
                relationship.segment_name,
                relationship.relation_kind,
            )
            for relationship in materialized.relationships_used
        ],
        "wrapperRows": subset_rows,
        "rowsetRows": subset_rowset_rows,
        "reconstructedJson": reconstructed_json,
        "manifestRoots": [root["tableName"] for root in subset_manifest["roots"]],
    }


def build_summary_family(con) -> dict[str, Any]:
    activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)
    materialize_synthesized_family(
        con,
        target_schema=SUMMARY_SOURCE_SCHEMA,
        family_spec=SynthesizedFamilySpec(
            root_table="DOC_SUMMARY",
            table_specs=[
                ResultTableSpec(
                    table_name="DOC_SUMMARY",
                    select_sql=f"""
                    SELECT
                      CAST("id" AS DECIMAL(18,0)) AS "_id",
                      "id" AS "doc_id",
                      CASE WHEN "items[SIZE]" IS NULL THEN 0 ELSE "items[SIZE]" END AS "item_count",
                      CASE
                        WHEN "items[SIZE]" IS NULL THEN 0
                        WHEN "items[SIZE]" > 2 THEN 2
                        ELSE "items[SIZE]"
                      END AS "top_items|array"
                    FROM {SOURCE_WRAPPER_SCHEMA}.SAMPLE
                    """,
                ),
                ResultTableSpec(
                    table_name="DOC_SUMMARY_top_items_arr",
                    select_sql=f"""
                    SELECT
                      CAST(ROW_NUMBER() OVER (ORDER BY s."id", item._index) AS DECIMAL(18,0)) AS "_id",
                      s."id" AS "_parent",
                      item._index AS "_pos",
                      item.value AS "value",
                      item.label AS "label",
                      item.amount AS "amount"
                    FROM {SOURCE_WRAPPER_SCHEMA}.SAMPLE s
                    JOIN item IN s."items"
                    WHERE item._index < 2
                    """,
                ),
            ],
        ),
    )

    summary_manifest = install_wrapper_views(
        con,
        source_schema=SUMMARY_SOURCE_SCHEMA,
        wrapper_schema=SUMMARY_WRAPPER_SCHEMA,
        helper_schema=SUMMARY_HELPER_SCHEMA,
        generate_preprocessor=True,
        preprocessor_schema=SUMMARY_PP_SCHEMA,
        preprocessor_script=SUMMARY_PP_SCRIPT,
    )
    install_wrapper_preprocessor(
        con,
        [SUMMARY_WRAPPER_SCHEMA],
        [SUMMARY_HELPER_SCHEMA],
        schema_name=SUMMARY_PP_SCHEMA,
        script_name=SUMMARY_PP_SCRIPT,
    )
    activate_preprocessor(con, SUMMARY_PP_SCHEMA, SUMMARY_PP_SCRIPT)

    root_rows = query_rows(
        con,
        f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)),
          CAST("item_count" AS VARCHAR(10)),
          COALESCE("top_items[FIRST].label", 'NULL'),
          COALESCE("top_items[LAST].value", 'NULL')
        FROM {SUMMARY_WRAPPER_SCHEMA}.DOC_SUMMARY
        ORDER BY "doc_id"
        """
    )
    rowset_rows = query_rows(
        con,
        f"""
        SELECT
          CAST(s."doc_id" AS VARCHAR(10)),
          CAST(item._index AS VARCHAR(10)),
          item.value,
          item.label,
          COALESCE(CAST(item.amount AS VARCHAR(20)), 'NULL')
        FROM {SUMMARY_WRAPPER_SCHEMA}.DOC_SUMMARY s
        JOIN item IN s."top_items"
        ORDER BY s."doc_id", item._index
        """
    )
    expected_root_rows = [
        ("1", "2", "A", "second"),
        ("2", "1", "C", "only"),
        ("3", "0", "NULL", "NULL"),
    ]
    expected_rowset_rows = [
        ("1", "0", "first", "A", "7"),
        ("1", "1", "second", "B", "NULL"),
        ("2", "0", "only", "C", "5"),
    ]
    assert_equal(root_rows, expected_root_rows, "summary wrapper rows")
    assert_equal(rowset_rows, expected_rowset_rows, "summary rowset rows")

    reconstructed_json = export_root_family_to_json(con, source_schema=SUMMARY_SOURCE_SCHEMA, root_table="DOC_SUMMARY")
    expected_json = [
        {
            "doc_id": 1,
            "item_count": 2,
            "top_items": [
                {"value": "first", "label": "A", "amount": 7},
                {"value": "second", "label": "B"},
            ],
        },
        {
            "doc_id": 2,
            "item_count": 1,
            "top_items": [
                {"value": "only", "label": "C", "amount": 5},
            ],
        },
        {
            "doc_id": 3,
            "item_count": 0,
            "top_items": [],
        },
    ]
    assert_equal(reconstructed_json, expected_json, "summary reconstructed JSON")

    activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)
    return {
        "wrapperRows": root_rows,
        "rowsetRows": rowset_rows,
        "reconstructedJson": reconstructed_json,
        "manifestRoots": [root["tableName"] for root in summary_manifest["roots"]],
    }


def build_nested_family(con) -> dict[str, Any]:
    activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)
    materialize_synthesized_family(
        con,
        target_schema=NESTED_SOURCE_SCHEMA,
        family_spec=SynthesizedFamilySpec(
            root_table="DOC_REPORT",
            table_specs=[
                ResultTableSpec(
                    table_name="DOC_REPORT",
                    select_sql=f"""
                    SELECT
                      CAST("id" AS DECIMAL(18,0)) AS "_id",
                      "id" AS "doc_id",
                      CAST(1000 + "id" AS DECIMAL(18,0)) AS "summary|object",
                      CASE WHEN "items[SIZE]" IS NULL THEN 0 ELSE "items[SIZE]" END AS "items|array"
                    FROM {SOURCE_WRAPPER_SCHEMA}.SAMPLE
                    """,
                ),
                ResultTableSpec(
                    table_name="DOC_REPORT_summary",
                    select_sql=f"""
                    SELECT
                      CAST(1000 + "id" AS DECIMAL(18,0)) AS "_id",
                      "name" AS "name_copy",
                      CASE WHEN "items[SIZE]" IS NULL THEN 0 ELSE "items[SIZE]" END AS "item_count",
                      CASE
                        WHEN JSON_IS_EXPLICIT_NULL("note") THEN 'explicit-null'
                        WHEN "note" IS NULL THEN 'missing'
                        ELSE 'present'
                      END AS "note_state"
                    FROM {SOURCE_WRAPPER_SCHEMA}.SAMPLE
                    """,
                ),
                ResultTableSpec(
                    table_name="DOC_REPORT_items_arr",
                    select_sql=f"""
                    SELECT
                      CAST((s."id" * 100) + item._index + 1 AS DECIMAL(18,0)) AS "_id",
                      s."id" AS "_parent",
                      item._index AS "_pos",
                      item.label AS "label",
                      item.value AS "value",
                      item.amount AS "amount",
                      item.enabled AS "enabled",
                      item."nested.note" AS "nested_note",
                      item."nested.items[SIZE]" AS "extras|array"
                    FROM {SOURCE_WRAPPER_SCHEMA}.SAMPLE s
                    JOIN item IN s."items"
                    """,
                ),
                ResultTableSpec(
                    table_name="DOC_REPORT_items_arr_extras_arr",
                    select_sql=f"""
                    SELECT
                      CAST((s."id" * 100) + item._index + 1 AS DECIMAL(18,0)) AS "_parent",
                      extra._index AS "_pos",
                      extra.value AS "_value"
                    FROM {SOURCE_WRAPPER_SCHEMA}.SAMPLE s
                    JOIN item IN s."items"
                    JOIN extra IN item."nested.items"
                    """,
                ),
            ],
        ),
    )

    nested_manifest = install_wrapper_views(
        con,
        source_schema=NESTED_SOURCE_SCHEMA,
        wrapper_schema=NESTED_WRAPPER_SCHEMA,
        helper_schema=NESTED_HELPER_SCHEMA,
        generate_preprocessor=True,
        preprocessor_schema=NESTED_PP_SCHEMA,
        preprocessor_script=NESTED_PP_SCRIPT,
    )
    install_wrapper_preprocessor(
        con,
        [NESTED_WRAPPER_SCHEMA],
        [NESTED_HELPER_SCHEMA],
        schema_name=NESTED_PP_SCHEMA,
        script_name=NESTED_PP_SCRIPT,
    )
    activate_preprocessor(con, NESTED_PP_SCHEMA, NESTED_PP_SCRIPT)

    root_rows = query_rows(
        con,
        f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)),
          "summary.note_state",
          COALESCE("items[FIRST].label", 'NULL'),
          COALESCE("items[FIRST].extras[LAST]", 'NULL')
        FROM {NESTED_WRAPPER_SCHEMA}.DOC_REPORT
        ORDER BY "doc_id"
        """
    )
    expected_root_rows = [
        ("1", "present", "A", "na-2"),
        ("2", "explicit-null", "C", "NULL"),
        ("3", "missing", "NULL", "NULL"),
    ]
    assert_equal(root_rows, expected_root_rows, "nested report root rows")

    rowset_rows = query_rows(
        con,
        f"""
        SELECT
          CAST(r."doc_id" AS VARCHAR(10)),
          CAST(item._index AS VARCHAR(10)),
          item.label,
          COALESCE(item.nested_note, 'NULL'),
          COALESCE(CAST(extra._index AS VARCHAR(10)), 'NULL'),
          COALESCE(extra, 'NULL')
        FROM {NESTED_WRAPPER_SCHEMA}.DOC_REPORT r
        JOIN item IN r."items"
        LEFT JOIN VALUE extra IN item."extras"
        ORDER BY r."doc_id", item._index, extra._index
        """
    )
    expected_rowset_rows = [
        ("1", "0", "A", "nested-a", "0", "na-1"),
        ("1", "0", "A", "nested-a", "1", "na-2"),
        ("1", "1", "B", "nested-b", "0", "nb-1"),
        ("2", "0", "C", "NULL", "NULL", "NULL"),
    ]
    assert_equal(rowset_rows, expected_rowset_rows, "nested report rowset rows")

    reconstructed_json = export_root_family_to_json(con, source_schema=NESTED_SOURCE_SCHEMA, root_table="DOC_REPORT")
    expected_json = [
        {
            "doc_id": 1,
            "summary": {
                "name_copy": "alpha",
                "item_count": 2,
                "note_state": "present",
            },
            "items": [
                {
                    "label": "A",
                    "value": "first",
                    "amount": 7,
                    "enabled": True,
                    "nested_note": "nested-a",
                    "extras": ["na-1", "na-2"],
                },
                {
                    "label": "B",
                    "value": "second",
                    "enabled": False,
                    "nested_note": "nested-b",
                    "extras": ["nb-1"],
                },
            ],
        },
        {
            "doc_id": 2,
            "summary": {
                "name_copy": "beta",
                "item_count": 1,
                "note_state": "explicit-null",
            },
            "items": [
                {
                    "label": "C",
                    "value": "only",
                    "amount": 5,
                }
            ],
        },
        {
            "doc_id": 3,
            "summary": {
                "name_copy": "gamma",
                "item_count": 0,
                "note_state": "missing",
            },
            "items": [],
        },
    ]
    assert_equal(reconstructed_json, expected_json, "nested report reconstructed JSON")

    activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)
    return {
        "wrapperRows": root_rows,
        "rowsetRows": rowset_rows,
        "reconstructedJson": reconstructed_json,
        "manifestRoots": [root["tableName"] for root in nested_manifest["roots"]],
    }


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
        install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=SOURCE_WRAPPER_SCHEMA,
            helper_schema=SOURCE_HELPER_SCHEMA,
            generate_preprocessor=True,
            preprocessor_schema=SOURCE_PP_SCHEMA,
            preprocessor_script=SOURCE_PP_SCRIPT,
        )
        install_wrapper_preprocessor(
            con,
            [SOURCE_WRAPPER_SCHEMA],
            [SOURCE_HELPER_SCHEMA],
            schema_name=SOURCE_PP_SCHEMA,
            script_name=SOURCE_PP_SCRIPT,
        )
        activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)

        temp_support = test_temporary_table_support(con)
        temp_family_support = test_local_temporary_wrapper_family(con)
        activate_preprocessor(con, SOURCE_PP_SCHEMA, SOURCE_PP_SCRIPT)
        subset_result = clone_root_family_subset(con, "SAMPLE", '"id" IN (1, 2)')
        summary_result = build_summary_family(con)
        nested_result = build_nested_family(con)
    finally:
        try:
            deactivate_preprocessor(con)
        except Exception:
            pass
        con.close()

    results = {
        "temporaryTableSupport": temp_support,
        "localTemporaryFamilySupport": temp_family_support,
        "familyPreservingSubset": subset_result,
        "synthesizedSummaryFamily": summary_result,
        "synthesizedNestedFamily": nested_result,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {OUTPUT_PATH}")
    print("-- structured result materialization study --")
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
