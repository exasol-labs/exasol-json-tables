#!/usr/bin/env python3

"""Regression tests for the preprocessor-free flattened view surface.

The local checks run without a database. The Nano-backed checks prove the
point of the whole surface: a consumer that never issues `ALTER SESSION` can
still query the ingested documents with plain, unquoted, UPPERCASE SQL.
"""

from __future__ import annotations

import json
import shutil
import ssl
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pyexasol

import _bootstrap  # noqa: F401

from exasol_json_tables.generate_flat_views_sql import (
    IdentifierAllocator,
    MAX_IDENTIFIER_LENGTH,
    RESERVED_WORDS,
    build_join_key_lines,
    default_flat_schema,
    flatten_identifier,
    generate_flat_surface,
)
from exasol_json_tables.wrapper_schema_support import (
    build_relationships,
    build_root_families,
    build_table_models,
    find_root_tables,
    source_columns_from_manifest,
)
from nano_support import ROOT, connect


CLI = ROOT / "tools" / "exasol_json_tables.py"

WORKFLOW_NAME = "flat_demo"
SOURCE_SCHEMA = "EJT_FLAT_DEMO_SRC"
WRAPPER_SCHEMA = "EJT_FLAT_DEMO_VIEW"
HELPER_SCHEMA = "EJT_FLAT_DEMO_VIEW_INTERNAL"
FLAT_SCHEMA = "EJT_FLAT_DEMO_FLAT"
PREPROCESSOR_SCHEMA = "EJT_FLAT_DEMO_PP"


def _column(name: str, ordinal: int, type_name: str = "VARCHAR(2000000)") -> dict[str, Any]:
    return {"name": name, "typeName": type_name, "ordinal": ordinal}


def _synthetic_source_manifest() -> dict[str, Any]:
    """A source family built to exercise every flattening rule at once."""
    long_name = "x" * 200
    return {
        "format": "exasol-json-tables-source-manifest",
        "version": 1,
        "stem": "EVENTS",
        "tables": [
            {
                "tableName": "EVENTS",
                "columns": [
                    _column("_id", 1, "DECIMAL(18,0)"),
                    _column("order", 2),
                    _column("sub-category", 3),
                    _column("sub category", 4),
                    _column("123abc", 5),
                    _column("!!", 6),
                    _column("_id_", 7),
                    _column(long_name + "a", 8),
                    _column(long_name + "b", 9),
                    _column("value", 10),
                    _column("value|bool", 11, "BOOLEAN"),
                    _column("dimensions|object", 12, "DECIMAL(18,0)"),
                    _column("tags|array", 13, "DECIMAL(18,0)"),
                ],
            },
            {
                "tableName": "EVENTS_dimensions",
                "columns": [
                    _column("_id", 1, "DECIMAL(18,0)"),
                    _column("region", 2),
                    _column("labels|array", 3, "DECIMAL(18,0)"),
                ],
            },
            {
                "tableName": "EVENTS_dimensions_labels_arr",
                "columns": [
                    _column("_parent", 1, "DECIMAL(18,0)"),
                    _column("_pos", 2, "DECIMAL(18,0)"),
                    _column("_value", 3),
                ],
            },
            {
                "tableName": "EVENTS_tags_arr",
                "columns": [
                    _column("_parent", 1, "DECIMAL(18,0)"),
                    _column("_pos", 2, "DECIMAL(18,0)"),
                    _column("_value", 3),
                ],
            },
        ],
    }


def _synthetic_flat_surface():
    source_columns = source_columns_from_manifest(_synthetic_source_manifest(), SOURCE_SCHEMA)
    table_models = build_table_models(source_columns)
    relationships = build_relationships(table_models)
    root_tables = find_root_tables(table_models, relationships)
    root_by_table = build_root_families(root_tables, relationships)
    return generate_flat_surface(
        source_schema=SOURCE_SCHEMA,
        flat_schema=FLAT_SCHEMA,
        table_models=table_models,
        relationships=relationships,
        root_tables=root_tables,
        root_by_table=root_by_table,
    )


def test_flatten_identifier_rules() -> None:
    assert flatten_identifier(["order_id"]) == "ORDER_ID"
    assert flatten_identifier(["customer", "address", "city"]) == "CUSTOMER_ADDRESS_CITY"
    # non-identifier characters become underscores, runs collapse, edges trim
    assert flatten_identifier(["sub-category"]) == "SUB_CATEGORY"
    assert flatten_identifier(["sub category"]) == "SUB_CATEGORY"
    assert flatten_identifier(["a..b"]) == "A_B"
    assert flatten_identifier(["_id"]) == "ID"
    assert flatten_identifier(["dimensions", "sub-category"]) == "DIMENSIONS_SUB_CATEGORY"
    # nothing legal left over
    assert flatten_identifier(["!!"]) == "FIELD"
    assert flatten_identifier([""]) == "FIELD"
    # identifiers may not start with a digit
    assert flatten_identifier(["123abc"]) == "C_123ABC"
    # reserved words are dodged rather than left to fail at query time
    assert flatten_identifier(["order"]) == "ORDER_COL"
    assert flatten_identifier(["state"]) == "STATE_COL"
    assert flatten_identifier(["value"]) == "VALUE_COL"
    # a reserved word only matters on its own, not as a path part
    assert flatten_identifier(["payment", "method"]) == "PAYMENT_METHOD"
    # 128 character limit
    long_name = flatten_identifier(["y" * 400])
    assert len(long_name) == MAX_IDENTIFIER_LENGTH
    assert "ORDER" in RESERVED_WORDS
    print("-- flatten_identifier rules --")


def test_identifier_allocator_resolves_collisions() -> None:
    allocator = IdentifierAllocator()
    assert allocator.allocate(["sub-category"]) == "SUB_CATEGORY"
    assert allocator.allocate(["sub category"]) == "SUB_CATEGORY_2"
    assert allocator.allocate(["sub_category"]) == "SUB_CATEGORY_3"
    # collisions caused by truncation stay unique and stay within the limit
    truncated = IdentifierAllocator()
    first = truncated.allocate(["z" * 300])
    second = truncated.allocate(["z" * 400])
    assert first != second
    assert len(first) == MAX_IDENTIFIER_LENGTH
    assert len(second) <= MAX_IDENTIFIER_LENGTH
    print("-- identifier allocator collisions --")


def test_default_flat_schema() -> None:
    assert default_flat_schema("EJT_ORDERS_VIEW") == "EJT_ORDERS_FLAT"
    assert default_flat_schema("JSON_VIEW") == "JSON_FLAT"
    assert default_flat_schema("ANALYTICS") == "ANALYTICS_FLAT"
    print("-- default flat schema --")


def test_flat_surface_shape_from_source_manifest() -> None:
    surface = _synthetic_flat_surface()
    entities = {entity.view_name: entity for entity in surface.entities}
    assert set(entities) == {"EVENTS", "EVENTS_TAGS", "EVENTS_DIMENSIONS_LABELS"}

    root = entities["EVENTS"]
    root_columns = [column.name for column in root.columns]

    # every emitted identifier is plain, uppercase, and unquoted-safe
    for entity in surface.entities:
        assert entity.view_name.isupper()
        assert entity.view_name not in RESERVED_WORDS
        for column in entity.columns:
            assert column.name.isupper(), column.name
            assert "|" not in column.name
            assert " " not in column.name
            assert len(column.name) <= MAX_IDENTIFIER_LENGTH
            assert column.name not in RESERVED_WORDS, column.name
            assert column.name[0].isalpha() or column.name[0] == "C"

    # structural columns get typeable names, and win any name race
    assert root_columns[0] == "ROW_ID"
    # reserved words, illegal starts and unusable spellings are all repaired
    assert "ORDER_COL" in root_columns
    assert "C_123ABC" in root_columns
    assert "FIELD" in root_columns
    # `_id` as a JSON property collides with the structural ROW_ID name
    assert "ID" in root_columns
    # two different spellings flatten onto one name, so the later one is suffixed
    assert "SUB_CATEGORY" in root_columns
    assert "SUB_CATEGORY_2" in root_columns
    # truncated long names stay unique
    long_names = [name for name in root_columns if name.startswith("X" * 100)]
    assert len(long_names) == 2
    assert len(set(long_names)) == 2
    assert all(len(name) <= MAX_IDENTIFIER_LENGTH for name in long_names)

    # nested objects are folded into the owning entity
    assert "DIMENSIONS_REGION" in root_columns
    # the object link is exposed only because a nested array needs it as a key
    assert "DIMENSIONS_ID" in root_columns
    # arrays stay separate, with a length marker on the parent
    assert "TAGS_LENGTH" in root_columns
    assert "DIMENSIONS_LABELS_LENGTH" in root_columns

    # variant members collapse back into one column
    value_column = next(column for column in root.columns if column.json_path == "value")
    assert "COALESCE(" in value_column.expression

    tags = entities["EVENTS_TAGS"]
    assert [column.name for column in tags.columns] == ["PARENT_ID", "ARRAY_INDEX", "ELEMENT_VALUE"]
    assert tags.parent is not None
    assert tags.parent.view == "EVENTS"
    assert tags.parent.parent_column == "ROW_ID"
    assert tags.parent.child_column == "PARENT_ID"
    assert tags.parent.order_column == "ARRAY_INDEX"

    # an array hanging off a folded object joins through the exposed object id
    labels = entities["EVENTS_DIMENSIONS_LABELS"]
    assert labels.parent is not None
    assert labels.parent.view == "EVENTS"
    assert labels.parent.parent_column == "DIMENSIONS_ID"

    # the generated SQL never needs the preprocessor
    assert "SQL_PREPROCESSOR_SCRIPT" not in surface.sql.replace(
        "No SQL_PREPROCESSOR_SCRIPT needed.", ""
    )
    assert surface.sql.startswith(f'DROP SCHEMA IF EXISTS "{FLAT_SCHEMA}" CASCADE;')

    join_key_lines = build_join_key_lines(surface.manifest)
    assert any("EVENTS.ROW_ID = " in line and "EVENTS_TAGS.PARENT_ID" in line for line in join_key_lines)
    assert any("EVENTS.DIMENSIONS_ID = " in line for line in join_key_lines)
    print("-- flat surface shape --")


def test_flat_surface_is_deterministic() -> None:
    first = _synthetic_flat_surface()
    second = _synthetic_flat_surface()
    assert first.sql == second.sql
    assert first.manifest == second.manifest
    print("-- flat surface determinism --")


def cleanup_workflow_schemas(con) -> None:
    for schema in [PREPROCESSOR_SCHEMA, HELPER_SCHEMA, WRAPPER_SCHEMA, FLAT_SCHEMA, SOURCE_SCHEMA]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def run_ingest_and_wrap(tmp: Path, *, json_mode: bool, extra_args: list[str] | None = None):
    fixture = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "orders.json"
    input_path = tmp / "ORDERS.json"
    shutil.copyfile(fixture, input_path)
    command = [
        "python3",
        str(CLI),
        "ingest-and-wrap",
        "--input",
        str(input_path),
        "--name",
        WORKFLOW_NAME,
        "--artifact-dir",
        str(tmp / "artifacts"),
        "--dsn",
        "127.0.0.1:8563",
        "--user",
        "sys",
        "--password",
        "exasol",
        "--exasol-temp-dir",
        str(tmp / "staging"),
        "--exasol-cleanup",
        "--if-exists",
        "replace",
    ]
    if json_mode:
        command.append("--json")
    command.extend(extra_args or [])
    return subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)


def connect_without_preprocessor():
    """A plain session: nothing is activated on it, ever."""
    return pyexasol.connect(
        dsn="127.0.0.1:8563",
        user="sys",
        password="exasol",
        schema="SYS",
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
    )


def test_flat_views_are_queryable_without_activation() -> None:
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_flat_") as tmpdir:
        tmp = Path(tmpdir)
        con = connect()
        try:
            cleanup_workflow_schemas(con)
        finally:
            con.close()

        try:
            run_ingest_and_wrap(tmp, json_mode=False)

            con = connect_without_preprocessor()
            try:
                # The wrapper surface is unusable on a session that cannot set
                # session state -- this is the gap the flat surface closes.
                wrapper_failed = False
                try:
                    con.execute(
                        f'SELECT "customer.address.city" FROM "{WRAPPER_SCHEMA}"."ORDERS" LIMIT 1'
                    ).fetchall()
                except Exception:
                    wrapper_failed = True
                assert wrapper_failed, "wrapper path syntax resolved without an active preprocessor"

                # Plain, unquoted, UPPERCASE SQL against the flat surface.
                rows = con.execute(
                    f"""
                    SELECT ORDER_ID, CUSTOMER_TIER, CUSTOMER_ADDRESS_CITY, PAYMENT_METHOD, ITEMS_LENGTH
                    FROM {FLAT_SCHEMA}.ORDERS
                    ORDER BY ORDER_ID
                    """
                ).fetchall()
                assert rows, "flat root view returned no rows"
                assert rows[0][0] == "ORD-001"
                assert rows[0][2] == "Berlin"
                assert rows[0][3] == "card"

                # The documented join keys really join.
                joined = con.execute(
                    f"""
                    SELECT o.ORDER_ID, i.ARRAY_INDEX, i.SKU, i.QTY
                    FROM {FLAT_SCHEMA}.ORDERS o
                    JOIN {FLAT_SCHEMA}.ORDERS_ITEMS i ON i.PARENT_ID = o.ROW_ID
                    WHERE o.ORDER_ID = 'ORD-001'
                    ORDER BY i.ARRAY_INDEX
                    """
                ).fetchall()
                assert [row[2] for row in joined] == ["P001", "P003"]
                assert [row[1] for row in joined] == [0, 1]

                tags = con.execute(
                    f"""
                    SELECT t.ELEMENT_VALUE
                    FROM {FLAT_SCHEMA}.ORDERS o
                    JOIN {FLAT_SCHEMA}.ORDERS_TAGS t ON t.PARENT_ID = o.ROW_ID
                    WHERE o.ORDER_ID = 'ORD-001'
                    ORDER BY t.ARRAY_INDEX
                    """
                ).fetchall()
                assert [row[0] for row in tags] == ["repeat", "bulk"]

                # An aggregate of the kind a dashboard would actually run.
                aggregated = con.execute(
                    f"""
                    SELECT CUSTOMER_ADDRESS_COUNTRY, COUNT(*)
                    FROM {FLAT_SCHEMA}.ORDERS
                    GROUP BY CUSTOMER_ADDRESS_COUNTRY
                    ORDER BY 1
                    """
                ).fetchall()
                assert aggregated

                # No column in the flat schema needs quoting.
                catalog_rows = con.execute(
                    f"""
                    SELECT COLUMN_TABLE, COLUMN_NAME, COLUMN_COMMENT
                    FROM SYS.EXA_ALL_COLUMNS
                    WHERE COLUMN_SCHEMA = '{FLAT_SCHEMA}'
                    """
                ).fetchall()
                assert catalog_rows
                for _table_name, column_name, _column_comment in catalog_rows:
                    assert column_name == column_name.upper()
                    assert "|" not in column_name
                    assert column_name not in RESERVED_WORDS
                comments = {
                    (str(row[0]), str(row[1])): str(row[2]) if row[2] is not None else ""
                    for row in catalog_rows
                }
                # Column comments carry the original JSON path back.
                assert comments[("ORDERS", "CUSTOMER_ADDRESS_CITY")] == "customer.address.city"
            finally:
                con.close()
        finally:
            con = connect()
            try:
                cleanup_workflow_schemas(con)
            finally:
                con.close()
    print("-- flat views queryable without activation --")


def test_cli_reports_flat_views_and_join_keys() -> None:
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_flat_cli_") as tmpdir:
        tmp = Path(tmpdir)
        con = connect()
        try:
            cleanup_workflow_schemas(con)
        finally:
            con.close()

        try:
            result = run_ingest_and_wrap(tmp, json_mode=False)
            stdout = result.stdout

            # Item 1: the flat surface is announced, with the schema it lives in.
            assert "Next steps:" in stdout
            assert f"Flattened views (plain SQL, no ALTER SESSION, UPPERCASE columns) in {FLAT_SCHEMA}:" in stdout
            assert f"{FLAT_SCHEMA}.ORDERS " in stdout
            assert f"{FLAT_SCHEMA}.ORDERS_ITEMS " in stdout

            # Item 2: the manifest's relationships become concrete join keys.
            assert "Join keys" in stdout
            assert "ORDERS.source_manifest.json" in stdout
            assert (
                f"{FLAT_SCHEMA}.ORDERS.ROW_ID = {FLAT_SCHEMA}.ORDERS_ITEMS.PARENT_ID" in stdout
            )
            assert "ordered by ARRAY_INDEX" in stdout

            # The join keys the CLI prints are the ones the ingest layer recorded.
            source_manifest_path = next((tmp / "artifacts").rglob("*.source_manifest.json"))
            source_manifest = json.loads(source_manifest_path.read_text())
            ingested_arrays = {
                str(relationship["segmentName"])
                for relationship in source_manifest["relationships"]
                if relationship["relationKind"] == "array"
            }
            assert ingested_arrays == {"items", "tags"}

            json_result = run_ingest_and_wrap(tmp, json_mode=True)
            payload = json.loads(json_result.stdout)
            next_actions = payload["nextActions"]
            assert next_actions["flatSchema"] == FLAT_SCHEMA
            assert next_actions["flatViews"] == ["ORDERS", "ORDERS_ITEMS", "ORDERS_TAGS"]
            assert any(
                f"{FLAT_SCHEMA}.ORDERS.ROW_ID = {FLAT_SCHEMA}.ORDERS_ITEMS.PARENT_ID" in line
                for line in next_actions["joinKeys"]
            )
            assert next_actions["flatSmokeTestSql"].startswith("SELECT ROW_ID")
            assert payload["objects"]["flatSchema"] == FLAT_SCHEMA
            assert payload["wrapper"]["flat"]["activationRequired"] is False
            assert payload["validation"]["installed"]["flatSurface"]["probe"]["ok"] is True
        finally:
            con = connect()
            try:
                cleanup_workflow_schemas(con)
            finally:
                con.close()
    print("-- cli reports flat views and join keys --")


def test_flat_views_can_be_disabled() -> None:
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_flat_off_") as tmpdir:
        tmp = Path(tmpdir)
        con = connect()
        try:
            cleanup_workflow_schemas(con)
        finally:
            con.close()

        try:
            result = run_ingest_and_wrap(tmp, json_mode=True, extra_args=["--no-flat-views"])
            payload = json.loads(result.stdout)
            assert payload["artifacts"]["flatViewsSql"] is None
            assert "flatSchema" not in payload["nextActions"]
            assert "flat" not in payload["wrapper"]

            package_config_path = Path(payload["artifacts"]["packageConfig"])
            package_config = json.loads(package_config_path.read_text())
            assert package_config["flatSchema"] is None
            assert package_config["generatedFiles"]["flatViewsSql"] is None

            con = connect()
            try:
                schemas = con.execute(
                    f"SELECT SCHEMA_NAME FROM SYS.EXA_ALL_SCHEMAS WHERE SCHEMA_NAME = '{FLAT_SCHEMA}'"
                ).fetchall()
                assert schemas == []
            finally:
                con.close()
        finally:
            con = connect()
            try:
                cleanup_workflow_schemas(con)
            finally:
                con.close()
    print("-- flat views can be disabled --")


if __name__ == "__main__":
    test_flatten_identifier_rules()
    test_identifier_allocator_resolves_collisions()
    test_default_flat_schema()
    test_flat_surface_shape_from_source_manifest()
    test_flat_surface_is_deterministic()
    test_flat_views_are_queryable_without_activation()
    test_cli_reports_flat_views_and_join_keys()
    test_flat_views_can_be_disabled()
    print("-- flat views regression --")
    print("verified identifier flattening, collision handling, preprocessor-free querying, and CLI join-key output")
