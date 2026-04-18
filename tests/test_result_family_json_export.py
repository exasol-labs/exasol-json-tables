#!/usr/bin/env python3

import _bootstrap  # noqa: F401

from in_session_wrapper_installer import install_wrapper_surface_in_session
from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views
from result_family_json_export import export_all_root_families_to_json, export_root_family_to_json
from result_family_materializer import (
    ResultTableSpec,
    SynthesizedFamilySpec,
    materialize_family_preserving_subset,
    materialize_synthesized_family,
)


BASE_SOURCE_SCHEMA = "JVS_SRC"
BASE_WRAPPER_SCHEMA = "JSON_VIEW"
BASE_HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
BASE_PP_SCHEMA = "JVS_EXPORT_BASE_PP"
BASE_PP_SCRIPT = "JSON_EXPORT_BASE_PREPROCESSOR"

SUBSET_SOURCE_SCHEMA = "JVS_EXPORT_SUBSET_SRC"
TEMP_SOURCE_SCHEMA = "JVS_EXPORT_TEMP_SRC"
TEMP_WRAPPER_SCHEMA = "JSON_VIEW_EXPORT_TEMP"
TEMP_HELPER_SCHEMA = "JSON_VIEW_EXPORT_TEMP_INTERNAL"
TEMP_PP_SCHEMA = "JVS_EXPORT_TEMP_PP"
TEMP_PP_SCRIPT = "JSON_EXPORT_TEMP_PREPROCESSOR"

AGG_SOURCE_SCHEMA = "JVS_EXPORT_AGG_SRC"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def activate(con, schema: str, script: str) -> None:
    con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema}.{script}")


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
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

        subset_result = materialize_family_preserving_subset(
            con,
            source_helper_schema=BASE_HELPER_SCHEMA,
            target_schema=SUBSET_SOURCE_SCHEMA,
            root_table="SAMPLE",
            root_filter_sql='"id" IN (1, 2)',
        )
        subset_json = export_root_family_to_json(con, materialized_family=subset_result)
        assert_equal(
            subset_json,
            [
                {
                    "child": {"value": "child-1"},
                    "id": 1,
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
                    ],
                    "meta": {
                        "flag": True,
                        "info": {"note": "deep"},
                        "items": [{"value": "m1"}, {"value": "m2"}],
                    },
                    "name": "alpha",
                    "note": "x",
                    "tags": ["red", "blue"],
                    "value": 42,
                },
                {
                    "id": 2,
                    "items": [{"amount": 5, "label": "C", "value": "only"}],
                    "meta": {"flag": False, "items": [{"value": "m3"}]},
                    "name": "beta",
                    "note": None,
                    "tags": ["green"],
                    "value": "43",
                },
            ],
            "materialized subset export",
        )

        temp_materialized = materialize_synthesized_family(
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
        install_result = install_wrapper_surface_in_session(
            con,
            materialized_family=temp_materialized,
            wrapper_schema=TEMP_WRAPPER_SCHEMA,
            helper_schema=TEMP_HELPER_SCHEMA,
            preprocessor_schema=TEMP_PP_SCHEMA,
            preprocessor_script=TEMP_PP_SCRIPT,
            activate_preprocessor_session=True,
        )
        temp_json = export_root_family_to_json(
            con,
            installed_wrapper=install_result,
            root_table="TMP_REPORT",
        )
        assert_equal(
            temp_json,
            [
                {
                    "doc_id": 1,
                    "items": [{"label": "A", "value": "first"}, {"label": "B", "value": "second"}],
                },
                {
                    "doc_id": 2,
                    "items": [{"label": "C", "value": "only"}],
                },
                {
                    "doc_id": 3,
                    "items": [],
                },
            ],
            "installed wrapper export",
        )

        activate(con, BASE_PP_SCHEMA, BASE_PP_SCRIPT)
        agg_materialized = materialize_synthesized_family(
            con,
            target_schema=AGG_SOURCE_SCHEMA,
            family_spec=SynthesizedFamilySpec(
                root_table="AGG_REPORT",
                table_specs=[
                    ResultTableSpec(
                        table_name="AGG_REPORT",
                        select_sql=f"""
                        SELECT
                          CAST(1 AS DECIMAL(18,0)) AS "_id",
                          SUM(CAST("id" AS DECIMAL(18,0))) AS "total_id",
                          COUNT(*) AS "doc_count",
                          COUNT(*) AS "segments|array"
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE
                        """,
                    ),
                    ResultTableSpec(
                        table_name="AGG_REPORT_segments_arr",
                        select_sql=f"""
                        SELECT
                          CAST(100 + item._index AS DECIMAL(18,0)) AS "_id",
                          CAST(1 AS DECIMAL(18,0)) AS "_parent",
                          item._index AS "_pos",
                          COUNT(*) AS "line_count",
                          SUM(COALESCE(CAST(item.amount AS DECIMAL(18,0)), 0)) AS "amount_total"
                        FROM {BASE_WRAPPER_SCHEMA}.SAMPLE s
                        JOIN item IN s."items"
                        GROUP BY item._index
                        """,
                    ),
                ],
            ),
        )
        agg_json = export_root_family_to_json(con, materialized_family=agg_materialized)
        assert_equal(
            agg_json,
            [
                {
                    "doc_count": 3,
                    "segments": [
                        {"amount_total": 12, "line_count": 2},
                        {"amount_total": 0, "line_count": 1},
                    ],
                    "total_id": 6,
                }
            ],
            "aggregate numeric export",
        )

        all_roots = export_all_root_families_to_json(con, source_schema=BASE_SOURCE_SCHEMA)
        assert_equal(sorted(all_roots), ["DEEPDOC", "SAMPLE"], "multi-root export keys")
        assert_equal(
            all_roots["DEEPDOC"],
            [
                {
                    "chain": {
                        "next": {
                            "next": {
                                "next": {
                                    "next": {
                                        "next": {
                                            "next": {
                                                "next": {
                                                    "entries": [
                                                        {"extras": ["x0", "x1"], "kind": "root", "value": "e0"},
                                                        {"kind": "mid", "value": "e1"},
                                                        {"extras": ["tail-extra"], "kind": "tail", "value": "e2"},
                                                    ],
                                                    "leaf_note": "bottom",
                                                    "reading": 100,
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "doc_id": 101,
                    "metrics": [10, 20, 30],
                    "profile": {"nickname": None, "prefs": {"theme": "dark"}},
                    "tags": ["alpha", "beta", "gamma"],
                    "title": "deep-alpha",
                },
                {
                    "chain": {
                        "next": {
                            "next": {
                                "next": {
                                    "next": {
                                        "next": {
                                            "next": {
                                                "next": {
                                                    "entries": [{"extras": ["solo-extra"], "kind": "solo", "value": "other"}],
                                                    "leaf_note": None,
                                                    "reading": "101",
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "doc_id": 102,
                    "metrics": [7],
                    "profile": {"prefs": {"theme": None}},
                    "tags": ["delta"],
                    "title": "deep-beta",
                },
                {
                    "doc_id": 103,
                    "title": "deep-gamma",
                },
            ],
            "deep root export",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()
