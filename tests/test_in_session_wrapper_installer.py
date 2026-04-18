#!/usr/bin/env python3

import _bootstrap  # noqa: F401

from in_session_wrapper_installer import install_wrapper_surface_in_session
from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views
from result_family_materializer import (
    ResultTableSpec,
    SynthesizedFamilySpec,
    materialize_synthesized_family,
)


BASE_SOURCE_SCHEMA = "JVS_SRC"
BASE_WRAPPER_SCHEMA = "JSON_VIEW"
BASE_HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
BASE_PP_SCHEMA = "JVS_TEMP_PHASE2_BASE_PP"
BASE_PP_SCRIPT = "JSON_TEMP_PHASE2_BASE_PREPROCESSOR"

TEMP_SOURCE_SCHEMA = "JVS_TEMP_PHASE2_SRC"
TEMP_WRAPPER_SCHEMA = "JSON_VIEW_TEMP_PHASE2"
TEMP_HELPER_SCHEMA = "JSON_VIEW_TEMP_PHASE2_INTERNAL"
TEMP_PP_SCHEMA = "JVS_TEMP_PHASE2_PP"
TEMP_PP_SCRIPT = "JSON_TEMP_PHASE2_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=False)
        install_wrapper_views(
            con,
            source_schema=BASE_SOURCE_SCHEMA,
            wrapper_schema=BASE_WRAPPER_SCHEMA,
            helper_schema=BASE_HELPER_SCHEMA,
            generate_preprocessor=True,
            preprocessor_schema=BASE_PP_SCHEMA,
            preprocessor_script=BASE_PP_SCRIPT,
        )
        install_wrapper_preprocessor(
            con,
            [BASE_WRAPPER_SCHEMA],
            [BASE_HELPER_SCHEMA],
            schema_name=BASE_PP_SCHEMA,
            script_name=BASE_PP_SCRIPT,
        )

        materialized = materialize_synthesized_family(
            con,
            target_schema=TEMP_SOURCE_SCHEMA,
            table_kind="local_temporary",
            family_spec=SynthesizedFamilySpec(
                root_table="TMP_REPORT",
                table_specs=[
                    ResultTableSpec(
                        table_name="TMP_REPORT",
                        select_sql=f"""
                        SELECT
                          CAST("id" AS DECIMAL(18,0)) AS "_id",
                          "id" AS "doc_id",
                          CASE WHEN "items[SIZE]" IS NULL THEN 0 ELSE "items[SIZE]" END AS "items|array"
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE
                        """,
                    ),
                    ResultTableSpec(
                        table_name="TMP_REPORT_items_arr",
                        select_sql=f"""
                        SELECT
                          CAST((s."id" * 100) + item._index + 1 AS DECIMAL(18,0)) AS "_id",
                          s."id" AS "_parent",
                          item._index AS "_pos",
                          item.label AS "label",
                          item.value AS "value"
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE s
                        JOIN item IN s."items"
                        """,
                    ),
                ],
            ),
        )
        assert_equal(materialized.table_kind, "local_temporary", "materialized table kind")
        assert_equal(materialized.family_description.root_tables, ["TMP_REPORT"], "local temporary roots")

        install_result = install_wrapper_surface_in_session(
            con,
            materialized_family=materialized,
            wrapper_schema=TEMP_WRAPPER_SCHEMA,
            helper_schema=TEMP_HELPER_SCHEMA,
            preprocessor_schema=TEMP_PP_SCHEMA,
            preprocessor_script=TEMP_PP_SCRIPT,
            activate_preprocessor_session=True,
        )
        assert_equal(
            [root["tableName"] for root in install_result.manifest["roots"]],
            ["TMP_REPORT"],
            "installed manifest roots",
        )

        wrapper_rows = con.execute(
            f"""
            SELECT
              CAST("doc_id" AS VARCHAR(10)),
              COALESCE("items[FIRST].label", 'NULL'),
              COALESCE("items[LAST].value", 'NULL')
            FROM {TEMP_WRAPPER_SCHEMA}.TMP_REPORT
            ORDER BY "doc_id"
            """
        ).fetchall()
        assert_equal(
            wrapper_rows,
            [("1", "A", "second"), ("2", "C", "only"), ("3", "NULL", "NULL")],
            "local temp wrapper rows",
        )

        rowset_rows = con.execute(
            f"""
            SELECT
              CAST(r."doc_id" AS VARCHAR(10)),
              CAST(item._index AS VARCHAR(10)),
              item.label,
              item.value
            FROM {TEMP_WRAPPER_SCHEMA}.TMP_REPORT r
            JOIN item IN r."items"
            ORDER BY r."doc_id", item._index
            """
        ).fetchall()
        assert_equal(
            rowset_rows,
            [("1", "0", "A", "first"), ("1", "1", "B", "second"), ("2", "0", "C", "only")],
            "local temp rowset rows",
        )

        cross_session = connect()
        try:
            cross_session.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {TEMP_PP_SCHEMA}.{TEMP_PP_SCRIPT}")
            cross_rows = cross_session.execute(
                f"""
                SELECT
                  CAST("doc_id" AS VARCHAR(10)),
                  COALESCE("items[LAST].value", 'NULL')
                FROM {TEMP_WRAPPER_SCHEMA}.TMP_REPORT
                ORDER BY "doc_id"
                """
            ).fetchall()
        finally:
            try:
                cross_session.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
            except Exception:
                pass
            cross_session.close()
        assert_equal(
            cross_rows,
            [("1", "second"), ("2", "only"), ("3", "NULL")],
            "cross-session local temp wrapper rows",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()
