#!/usr/bin/env python3

import _bootstrap  # noqa: F401

from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views
from result_family_materializer import (
    StructuredArrayNodeSpec,
    StructuredFieldSpec,
    StructuredObjectNodeSpec,
    StructuredShapeSpec,
    ResultTableSpec,
    SynthesizedFamilySpec,
    materialize_family_preserving_subset,
    materialize_synthesized_family,
    validate_result_family_spec,
)


BASE_SOURCE_SCHEMA = "JVS_SRC"
BASE_WRAPPER_SCHEMA = "JSON_VIEW"
BASE_HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
BASE_PP_SCHEMA = "JVS_RESULT_MAT_PP"
BASE_PP_SCRIPT = "JSON_RESULT_MAT_PREPROCESSOR"

SUBSET_SOURCE_SCHEMA = "JVS_MAT_SUBSET_SRC"
SUBSET_WRAPPER_SCHEMA = "JSON_VIEW_MAT_SUBSET"
SUBSET_HELPER_SCHEMA = "JSON_VIEW_MAT_SUBSET_INTERNAL"
SUBSET_PP_SCHEMA = "JVS_MAT_SUBSET_PP"
SUBSET_PP_SCRIPT = "JSON_MAT_SUBSET_PREPROCESSOR"

REPORT_SOURCE_SCHEMA = "JVS_MAT_REPORT_SRC"
REPORT_WRAPPER_SCHEMA = "JSON_VIEW_MAT_REPORT"
REPORT_HELPER_SCHEMA = "JSON_VIEW_MAT_REPORT_INTERNAL"
REPORT_PP_SCHEMA = "JVS_MAT_REPORT_PP"
REPORT_PP_SCRIPT = "JSON_MAT_REPORT_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def activate(con, schema: str, script: str) -> None:
    con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema}.{script}")


def current_schema(con) -> str:
    return str(con.execute("SELECT CURRENT_SCHEMA FROM DUAL").fetchall()[0][0])


def main() -> None:
    con = connect()
    try:
        validate_result_family_spec(
            StructuredShapeSpec(
                root_table="VALID_DOC",
                root=StructuredObjectNodeSpec(
                    from_sql="FROM DUAL",
                    id_sql="1",
                    fields=[StructuredFieldSpec(name="name", sql="'ok'")],
                    objects=[],
                    arrays=[],
                ),
            )
        )
        try:
            validate_result_family_spec(
                StructuredShapeSpec(
                    root_table="BROKEN_DOC",
                    root=StructuredObjectNodeSpec(
                        from_sql="FROM DUAL",
                        id_sql="1",
                        fields=[],
                        objects=[],
                        arrays=[
                            StructuredArrayNodeSpec(
                                name="items",
                                from_sql="FROM DUAL",
                                parent_id_sql="1",
                                position_sql="0",
                                value_sql="'x'",
                            )
                        ],
                    ),
                )
            )
            raise AssertionError("expected validate_result_family_spec to reject arrays without array_ref fields")
        except ValueError as error:
            if "requires a matching array_ref field" not in str(error):
                raise AssertionError(f"unexpected validation error: {error}") from error

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
        activate(con, BASE_PP_SCHEMA, BASE_PP_SCRIPT)
        con.execute("OPEN SCHEMA SYS")

        subset_result = materialize_family_preserving_subset(
            con,
            source_helper_schema=BASE_HELPER_SCHEMA,
            target_schema=SUBSET_SOURCE_SCHEMA,
            root_table="SAMPLE",
            root_filter_sql='"id" IN (1, 2)',
        )
        assert_equal(
            subset_result.created_tables,
            [
                "SAMPLE",
                "SAMPLE_child",
                "SAMPLE_items_arr",
                "SAMPLE_items_arr_nested",
                "SAMPLE_items_arr_nested_items_arr",
                "SAMPLE_meta",
                "SAMPLE_meta_info",
                "SAMPLE_meta_items_arr",
                "SAMPLE_tags_arr",
            ],
            "subset created tables",
        )
        assert_equal(subset_result.family_description.root_tables, ["SAMPLE"], "subset root tables")
        assert_equal(current_schema(con), "SYS", "subset materialization should preserve current schema")

        install_wrapper_views(
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
        activate(con, SUBSET_PP_SCHEMA, SUBSET_PP_SCRIPT)

        subset_rows = con.execute(
            f"""
            SELECT
              CAST("id" AS VARCHAR(10)),
              COALESCE("child.value", 'NULL'),
              COALESCE("tags[LAST]", 'NULL'),
              COALESCE("items[LAST].value", 'NULL')
            FROM {SUBSET_WRAPPER_SCHEMA}.SAMPLE
            ORDER BY "id"
            """
        ).fetchall()
        assert_equal(
            subset_rows,
            [("1", "child-1", "blue", "second"), ("2", "NULL", "green", "only")],
            "subset wrapper rows",
        )

        activate(con, BASE_PP_SCHEMA, BASE_PP_SCRIPT)
        con.execute("OPEN SCHEMA SYS")
        report_result = materialize_synthesized_family(
            con,
            target_schema=REPORT_SOURCE_SCHEMA,
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
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE
                        """,
                    ),
                    ResultTableSpec(
                        table_name="DOC_REPORT_summary",
                        select_sql=f"""
                        SELECT
                          CAST(1000 + "id" AS DECIMAL(18,0)) AS "_id",
                          "name" AS "name_copy",
                          CASE
                            WHEN JSON_IS_EXPLICIT_NULL("note") THEN 'explicit-null'
                            WHEN "note" IS NULL THEN 'missing'
                            ELSE 'present'
                          END AS "note_state"
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE
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
                          item."nested.note" AS "nested_note",
                          item."nested.items[SIZE]" AS "extras|array"
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE s
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
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE s
                        JOIN item IN s."items"
                        JOIN extra IN item."nested.items"
                        """,
                    ),
                ],
            ),
        )
        assert_equal(report_result.family_description.root_tables, ["DOC_REPORT"], "report root tables")
        assert_equal(current_schema(con), "SYS", "synthesized materialization should preserve current schema")
        assert_equal(
            report_result.family_description.family_tables_by_root["DOC_REPORT"],
            [
                "DOC_REPORT",
                "DOC_REPORT_items_arr",
                "DOC_REPORT_items_arr_extras_arr",
                "DOC_REPORT_summary",
            ],
            "report family tables",
        )

        activate(con, BASE_PP_SCHEMA, BASE_PP_SCRIPT)
        con.execute("OPEN SCHEMA SYS")
        temp_result = materialize_synthesized_family(
            con,
            target_schema="JVS_MAT_TEMP_SRC",
            table_kind="local_temporary",
            family_spec=SynthesizedFamilySpec(
                root_table="TMP_DOC",
                table_specs=[
                    ResultTableSpec(
                        table_name="TMP_DOC",
                        select_sql=f"""
                        SELECT
                          CAST("id" AS DECIMAL(18,0)) AS "_id",
                          "id" AS "doc_id"
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE
                        """,
                    )
                ],
            ),
        )
        assert_equal(temp_result.family_description.root_tables, ["TMP_DOC"], "local temp roots")
        assert_equal(current_schema(con), "SYS", "local temporary materialization should preserve current schema")

        install_wrapper_views(
            con,
            source_schema=REPORT_SOURCE_SCHEMA,
            wrapper_schema=REPORT_WRAPPER_SCHEMA,
            helper_schema=REPORT_HELPER_SCHEMA,
            generate_preprocessor=True,
            preprocessor_schema=REPORT_PP_SCHEMA,
            preprocessor_script=REPORT_PP_SCRIPT,
        )
        install_wrapper_preprocessor(
            con,
            [REPORT_WRAPPER_SCHEMA],
            [REPORT_HELPER_SCHEMA],
            schema_name=REPORT_PP_SCHEMA,
            script_name=REPORT_PP_SCRIPT,
        )
        activate(con, REPORT_PP_SCHEMA, REPORT_PP_SCRIPT)

        report_rows = con.execute(
            f"""
            SELECT
              CAST("doc_id" AS VARCHAR(10)),
              "summary.note_state",
              COALESCE("items[FIRST].label", 'NULL'),
              COALESCE("items[FIRST].extras[LAST]", 'NULL')
            FROM {REPORT_WRAPPER_SCHEMA}.DOC_REPORT
            ORDER BY "doc_id"
            """
        ).fetchall()
        assert_equal(
            report_rows,
            [("1", "present", "A", "na-2"), ("2", "explicit-null", "C", "NULL"), ("3", "missing", "NULL", "NULL")],
            "report wrapper rows",
        )

        rowset_rows = con.execute(
            f"""
            SELECT
              CAST(r."doc_id" AS VARCHAR(10)),
              CAST(item._index AS VARCHAR(10)),
              item.label,
              COALESCE(CAST(extra._index AS VARCHAR(10)), 'NULL'),
              COALESCE(extra, 'NULL')
            FROM {REPORT_WRAPPER_SCHEMA}.DOC_REPORT r
            JOIN item IN r."items"
            LEFT JOIN VALUE extra IN item."extras"
            ORDER BY r."doc_id", item._index, extra._index
            """
        ).fetchall()
        assert_equal(
            rowset_rows,
            [
                ("1", "0", "A", "0", "na-1"),
                ("1", "0", "A", "1", "na-2"),
                ("1", "1", "B", "0", "nb-1"),
                ("2", "0", "C", "NULL", "NULL"),
            ],
            "report rowset rows",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()
