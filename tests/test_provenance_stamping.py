#!/usr/bin/env python3

"""Every path that creates a family stamps it, and the two contract versions agree."""

import json
import re
from pathlib import Path

import _bootstrap  # noqa: F401

from personal_support import connect, install_source_fixture, install_wrapper_views
from exasol_json_tables.provenance import (
    CONTRACT_VERSION,
    RESULT_FAMILY_CONNECTION,
    build_provenance_comment,
    parse_provenance_comment,
    table_paths_from_relationships,
)
from result_family_materializer import (
    ResultTableSpec,
    SynthesizedFamilySpec,
    materialize_family_preserving_subset,
    materialize_synthesized_family,
)
from wrapper_schema_support import Relationship


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_RS = ROOT / "crates" / "json_tables_core" / "src" / "contract.rs"

BASE_SOURCE_SCHEMA = "JVS_SRC"
BASE_WRAPPER_SCHEMA = "JSON_VIEW_PROV"
BASE_HELPER_SCHEMA = "JSON_VIEW_PROV_INTERNAL"

SUBSET_SCHEMA = "JVS_PROV_SUBSET_SRC"
SYNTH_SCHEMA = "JVS_PROV_SYNTH_SRC"
SUBSET_WRAPPER_SCHEMA = "JSON_VIEW_PROV_SUBSET"
SUBSET_HELPER_SCHEMA = "JSON_VIEW_PROV_SUBSET_INTERNAL"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def test_contract_version_matches_the_rust_constant() -> None:
    match = re.search(r"pub const CONTRACT_VERSION: u32 = (\d+);", CONTRACT_RS.read_text())
    if match is None:
        raise AssertionError(f"CONTRACT_VERSION not found in {CONTRACT_RS}")
    assert_equal(CONTRACT_VERSION, int(match.group(1)), "python vs rust CONTRACT_VERSION")


def test_comment_field_order_matches_the_rust_writer() -> None:
    comment = build_provenance_comment(
        source="table://X.Y",
        source_connection="table",
        imported_at="2026-09-01T10:00:00Z",
        table_path="root",
        source_modified_at="2026-09-01T09:00:00Z",
    )
    assert_equal(
        comment,
        'COPY provenance {"source":"table://X.Y","sourceConnection":"table",'
        '"importedAt":"2026-09-01T10:00:00Z","tablePath":"root",'
        '"tool":"exasol-json-tables","contractVersion":1,'
        '"sourceModifiedAt":"2026-09-01T09:00:00Z"}',
        "comment text",
    )
    assert_equal(parse_provenance_comment(comment)["contractVersion"], 1, "parsed contractVersion")
    assert_equal(parse_provenance_comment("no provenance here"), None, "non-provenance comment")


def test_table_paths_follow_the_rust_table_path_shape() -> None:
    relationships = [
        Relationship("DOC", "DOC_meta", "meta", "object"),
        Relationship("DOC_meta", "DOC_meta_info", "info", "object"),
        Relationship("DOC", "DOC_items_arr", "items", "array"),
        Relationship("DOC_items_arr", "DOC_items_arr_tags_arr", "tags", "array"),
    ]
    tables = [
        "DOC",
        "DOC_meta",
        "DOC_meta_info",
        "DOC_items_arr",
        "DOC_items_arr_tags_arr",
    ]
    assert_equal(
        table_paths_from_relationships("DOC", relationships, tables),
        {
            "DOC": "root",
            "DOC_meta": "meta",
            "DOC_meta_info": "meta.info",
            "DOC_items_arr": "items[]",
            "DOC_items_arr_tags_arr": "items[].tags[]",
        },
        "table paths",
    )


def fetch_comments(con, schema: str) -> dict[str, str | None]:
    rows = con.execute(
        f"""
        SELECT TABLE_NAME, TABLE_COMMENT
        FROM SYS.EXA_ALL_TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        """
    ).fetchall()
    return {name: comment for name, comment in rows}


def assert_family_is_stamped(con, schema: str, expected_paths: dict[str, str], label: str) -> None:
    comments = fetch_comments(con, schema)
    assert_equal(sorted(comments), sorted(expected_paths), f"{label} tables")
    for table_name, expected_path in expected_paths.items():
        provenance = parse_provenance_comment(comments[table_name])
        if provenance is None:
            raise AssertionError(f"{label}: {schema}.{table_name} carries no provenance comment")
        assert_equal(provenance["tool"], "exasol-json-tables", f"{label} {table_name} tool")
        assert_equal(
            provenance["sourceConnection"],
            RESULT_FAMILY_CONNECTION,
            f"{label} {table_name} sourceConnection",
        )
        assert_equal(
            provenance["contractVersion"], CONTRACT_VERSION, f"{label} {table_name} contractVersion"
        )
        assert_equal(provenance["tablePath"], expected_path, f"{label} {table_name} tablePath")
        if not str(provenance["importedAt"]).startswith("20"):
            raise AssertionError(f"{label} {table_name} importedAt: {provenance['importedAt']!r}")


def cleanup(con) -> None:
    for schema in [
        SUBSET_WRAPPER_SCHEMA,
        SUBSET_HELPER_SCHEMA,
        BASE_WRAPPER_SCHEMA,
        BASE_HELPER_SCHEMA,
        SUBSET_SCHEMA,
        SYNTH_SCHEMA,
    ]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def main() -> None:
    test_contract_version_matches_the_rust_constant()
    test_comment_field_order_matches_the_rust_writer()
    test_table_paths_follow_the_rust_table_path_shape()

    con = connect()
    try:
        cleanup(con)
        install_source_fixture(con)
        install_wrapper_views(
            con,
            source_schema=BASE_SOURCE_SCHEMA,
            wrapper_schema=BASE_WRAPPER_SCHEMA,
            helper_schema=BASE_HELPER_SCHEMA,
        )

        materialize_family_preserving_subset(
            con,
            source_helper_schema=BASE_HELPER_SCHEMA,
            target_schema=SUBSET_SCHEMA,
            root_table="SAMPLE",
            root_filter_sql='"id" IN (1, 2)',
        )
        subset_comments = fetch_comments(con, SUBSET_SCHEMA)
        unstamped = sorted(
            name
            for name, comment in subset_comments.items()
            if parse_provenance_comment(comment) is None
        )
        assert_equal(unstamped, [], "unstamped tables in the materialized subset")
        root_provenance = parse_provenance_comment(subset_comments["SAMPLE"])
        assert_equal(root_provenance["tablePath"], "root", "subset root tablePath")
        assert_equal(
            root_provenance["source"],
            f"table://{BASE_HELPER_SCHEMA}.SAMPLE",
            "subset source locator",
        )
        assert_equal(
            parse_provenance_comment(subset_comments["SAMPLE_child"])["tablePath"],
            "child",
            "subset child tablePath",
        )
        assert_equal(
            parse_provenance_comment(subset_comments["SAMPLE_tags_arr"])["tablePath"],
            "tags[]",
            "subset array child tablePath",
        )

        materialize_synthesized_family(
            con,
            target_schema=SYNTH_SCHEMA,
            family_spec=SynthesizedFamilySpec(
                root_table="REPORT",
                table_specs=[
                    ResultTableSpec(
                        table_name="REPORT",
                        select_sql=f"""
                        SELECT
                          "_id" AS "_id",
                          "name" AS "label",
                          "_id" AS "lines|array"
                        FROM {BASE_HELPER_SCHEMA}."SAMPLE"
                        """,
                    ),
                    ResultTableSpec(
                        table_name="REPORT_lines_arr",
                        select_sql=f"""
                        SELECT
                          "_id" AS "_parent",
                          0 AS "_pos",
                          "name" AS "_value"
                        FROM {BASE_HELPER_SCHEMA}."SAMPLE"
                        """,
                    ),
                ],
            ),
        )
        assert_family_is_stamped(
            con,
            SYNTH_SCHEMA,
            {"REPORT": "root", "REPORT_lines_arr": "lines[]"},
            "synthesized family",
        )
        synth_source = parse_provenance_comment(fetch_comments(con, SYNTH_SCHEMA)["REPORT"])["source"]
        if BASE_HELPER_SCHEMA not in synth_source:
            raise AssertionError(f"synthesized source locator: {synth_source!r}")

        # The wrapper generator copies the root comment onto the public view, so a
        # stamped result family also produces a stamped wrapper.
        install_wrapper_views(
            con,
            source_schema=SUBSET_SCHEMA,
            wrapper_schema=SUBSET_WRAPPER_SCHEMA,
            helper_schema=SUBSET_HELPER_SCHEMA,
        )
        view_comment = con.execute(
            f"""
            SELECT VIEW_COMMENT
            FROM SYS.EXA_ALL_VIEWS
            WHERE VIEW_SCHEMA = '{SUBSET_WRAPPER_SCHEMA}' AND VIEW_NAME = 'SAMPLE'
            """
        ).fetchval()
        assert_equal(view_comment, subset_comments["SAMPLE"], "wrapper view provenance")
    finally:
        try:
            cleanup(con)
        finally:
            con.close()

    print("-- provenance stamping --")
    print(json.dumps({"contractVersion": CONTRACT_VERSION}, separators=(",", ":")))
    print("verified materialized families and their wrappers carry provenance")


if __name__ == "__main__":
    main()
