#!/usr/bin/env python3

import json
from pathlib import Path
import subprocess
from typing import Optional

import pyexasol

from .generate_json_export_helper_sql import install_json_export_helpers
from .generate_json_export_views_sql import install_json_export_views
from .generate_preprocessor_library_sql import install_preprocessor_library


ROOT = Path(__file__).resolve().parents[2]


def connect():
    return pyexasol.connect(dsn="127.0.0.1:8563", user="sys", password="exasol", schema="SYS")


def install_wrapper_views(
    con,
    source_schema: str = "JVS_SRC",
    wrapper_schema: str = "JSON_VIEW",
    helper_schema: Optional[str] = None,
    generate_preprocessor: bool = False,
    preprocessor_schema: str = "JVS_WRAP_PP",
    preprocessor_script: str = "JSON_WRAPPER_PREPROCESSOR",
    activate_preprocessor_session: bool = False,
):
    output_path = ROOT / "dist" / "json_wrapper_views_test.sql"
    manifest_path = ROOT / "dist" / "json_wrapper_manifest_test.json"
    preprocessor_output_path = ROOT / "dist" / "json_wrapper_preprocessor_packaged_test.sql"
    helper_schema = helper_schema or f"{wrapper_schema}_INTERNAL"
    cmd = [
        "python3",
        str(ROOT / "tools" / "generate_wrapper_views_sql.py"),
        "--dsn",
        "127.0.0.1:8563",
        "--user",
        "sys",
        "--password",
        "exasol",
        "--source-schema",
        source_schema,
        "--wrapper-schema",
        wrapper_schema,
        "--helper-schema",
        helper_schema,
        "--output",
        str(output_path),
        "--manifest-output",
        str(manifest_path),
    ]
    if generate_preprocessor:
        cmd.extend(
            [
                "--preprocessor-output",
                str(preprocessor_output_path),
                "--preprocessor-schema",
                preprocessor_schema,
                "--preprocessor-script",
                preprocessor_script,
            ]
        )
        if activate_preprocessor_session:
            cmd.append("--activate-preprocessor-session")
    subprocess.run(cmd, check=True)
    content = output_path.read_text()
    statements = [statement.strip() for statement in content.split(";\n") if statement.strip()]
    for statement in statements:
        con.execute(statement)
    return json.loads(manifest_path.read_text())


def install_wrapper_preprocessor(
    con,
    wrapper_schemas: list[str],
    helper_schemas: Optional[list[str]] = None,
    manifest_paths: Optional[list[Path]] = None,
    schema_name: str = "JVS_WRAP_PP",
    script_name: str = "JSON_WRAPPER_PREPROCESSOR",
    to_json_function_names: Optional[list[str]] = None,
) -> None:
    output_path = ROOT / "dist" / "json_wrapper_preprocessor_test.sql"
    helper_schemas = helper_schemas or [f"{wrapper_schema}_INTERNAL" for wrapper_schema in wrapper_schemas]
    manifest_paths = manifest_paths or [ROOT / "dist" / "json_wrapper_manifest_test.json" for _ in wrapper_schemas]
    if len(helper_schemas) != len(wrapper_schemas):
        raise ValueError("wrapper_schemas and helper_schemas must have the same length")
    if len(manifest_paths) != len(wrapper_schemas):
        raise ValueError("wrapper_schemas and manifest_paths must have the same length")
    for wrapper_schema, helper_schema in zip(wrapper_schemas, helper_schemas):
        if wrapper_schema.upper() == helper_schema.upper():
            raise ValueError("wrapper_schemas and helper_schemas must differ for every schema pair")
    cmd = [
        "python3",
        str(ROOT / "tools" / "generate_wrapper_preprocessor_sql.py"),
        "--schema",
        schema_name,
        "--script",
        script_name,
        "--output",
        str(output_path),
    ]
    for wrapper_schema in wrapper_schemas:
        cmd.extend(["--wrapper-schema", wrapper_schema])
    for helper_schema in helper_schemas:
        cmd.extend(["--helper-schema", helper_schema])
    for manifest_path in manifest_paths:
        cmd.extend(["--manifest", str(manifest_path)])
    for function_name in to_json_function_names or []:
        cmd.extend(["--to-json-function-name", function_name])
    subprocess.run(cmd, check=True)

    manifests = [json.loads(path.read_text()) for path in manifest_paths]
    for helper_schema, manifest in zip(helper_schemas, manifests):
        install_json_export_helpers(con, helper_schema)
        install_json_export_views(
            con,
            source_schema=manifest["sourceSchema"],
            schema=helper_schema,
            udf_schema=helper_schema,
        )

    content = output_path.read_text()
    from .wrapper_package_tool import execute_generated_preprocessor_sql

    con.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    install_preprocessor_library(con, schema_name)
    execute_generated_preprocessor_sql(con, content)
    con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema_name}.{script_name}")


def _base_fixture_statements() -> list[str]:
    return [
        "DROP SCHEMA IF EXISTS JVS_SRC CASCADE",
        "CREATE SCHEMA JVS_SRC",
        "OPEN SCHEMA JVS_SRC",
        'CREATE OR REPLACE TABLE SAMPLE ("_id" DECIMAL(18,0) NOT NULL, "id" DECIMAL(18,0), "name" VARCHAR(100), "note" VARCHAR(100), "note|n" BOOLEAN, "child|object" DECIMAL(18,0), "child|n" BOOLEAN, "meta|object" DECIMAL(18,0), "value" DECIMAL(18,0), "value|string" VARCHAR(100), "value|n" BOOLEAN, "shape|object" DECIMAL(18,0), "shape|array" DECIMAL(18,0), "tags|array" DECIMAL(18,0), "items|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "SAMPLE_child" ("_id" DECIMAL(18,0) NOT NULL, "value" VARCHAR(100))',
        'CREATE OR REPLACE TABLE "SAMPLE_meta" ("_id" DECIMAL(18,0) NOT NULL, "info|object" DECIMAL(18,0), "flag" BOOLEAN, "items|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "SAMPLE_meta_info" ("_id" DECIMAL(18,0) NOT NULL, "note" VARCHAR(100), "note|n" BOOLEAN)',
        'CREATE OR REPLACE TABLE "SAMPLE_tags_arr" ("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "_value" VARCHAR(100))',
        'CREATE OR REPLACE TABLE "SAMPLE_items_arr" ("_id" DECIMAL(18,0) NOT NULL, "_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "value" VARCHAR(100), "label" VARCHAR(100), "optional" VARCHAR(100), "optional|n" BOOLEAN, "amount" DECIMAL(18,0), "enabled" BOOLEAN, "nested|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "SAMPLE_items_arr_nested" ("_id" DECIMAL(18,0) NOT NULL, "note" VARCHAR(100), "score" DECIMAL(18,0), "active" BOOLEAN, "pick" DECIMAL(18,0), "items|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "SAMPLE_items_arr_nested_items_arr" ("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "value" VARCHAR(100))',
        'CREATE OR REPLACE TABLE "SAMPLE_meta_items_arr" ("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "value" VARCHAR(100))',
        "INSERT INTO SAMPLE VALUES (1, 1, 'alpha', 'x', FALSE, 1, FALSE, 10, 42, NULL, FALSE, 10, NULL, 2, 2)",
        "INSERT INTO SAMPLE VALUES (2, 2, 'beta', NULL, TRUE, NULL, FALSE, 20, NULL, '43', FALSE, NULL, 3, 1, 1)",
        "INSERT INTO SAMPLE VALUES (3, 3, 'gamma', NULL, FALSE, NULL, TRUE, NULL, NULL, NULL, TRUE, NULL, NULL, NULL, NULL)",
        'INSERT INTO "SAMPLE_child" VALUES (1, \'child-1\')',
        'INSERT INTO "SAMPLE_meta" VALUES (10, 100, TRUE, 2)',
        'INSERT INTO "SAMPLE_meta" VALUES (20, NULL, FALSE, 1)',
        'INSERT INTO "SAMPLE_meta_info" VALUES (100, \'deep\', FALSE)',
        'INSERT INTO "SAMPLE_tags_arr" VALUES (1, 0, \'red\')',
        'INSERT INTO "SAMPLE_tags_arr" VALUES (1, 1, \'blue\')',
        'INSERT INTO "SAMPLE_tags_arr" VALUES (2, 0, \'green\')',
        'INSERT INTO "SAMPLE_items_arr" VALUES (1001, 1, 0, \'first\', \'A\', \'x\', FALSE, 7, TRUE, 7001)',
        'INSERT INTO "SAMPLE_items_arr" VALUES (1002, 1, 1, \'second\', \'B\', NULL, TRUE, NULL, FALSE, 7002)',
        'INSERT INTO "SAMPLE_items_arr" VALUES (1003, 2, 0, \'only\', \'C\', NULL, FALSE, 5, NULL, NULL)',
        'INSERT INTO "SAMPLE_items_arr_nested" VALUES (7001, \'nested-a\', 11, TRUE, 1, 2)',
        'INSERT INTO "SAMPLE_items_arr_nested" VALUES (7002, \'nested-b\', 12, FALSE, 0, 1)',
        'INSERT INTO "SAMPLE_items_arr_nested_items_arr" VALUES (7001, 0, \'na-1\')',
        'INSERT INTO "SAMPLE_items_arr_nested_items_arr" VALUES (7001, 1, \'na-2\')',
        'INSERT INTO "SAMPLE_items_arr_nested_items_arr" VALUES (7002, 0, \'nb-1\')',
        'INSERT INTO "SAMPLE_meta_items_arr" VALUES (10, 0, \'m1\')',
        'INSERT INTO "SAMPLE_meta_items_arr" VALUES (10, 1, \'m2\')',
        'INSERT INTO "SAMPLE_meta_items_arr" VALUES (20, 0, \'m3\')',
    ]


def _deep_fixture_statements() -> list[str]:
    return [
        'CREATE OR REPLACE TABLE DEEPDOC ("_id" DECIMAL(18,0) NOT NULL, "doc_id" DECIMAL(18,0), '
        '"title" VARCHAR(100), "profile|object" DECIMAL(18,0), "chain|object" DECIMAL(18,0), '
        '"tags|array" DECIMAL(18,0), "metrics|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_profile" ("_id" DECIMAL(18,0) NOT NULL, "nickname" VARCHAR(100), '
        '"nickname|n" BOOLEAN, "prefs|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_profile_prefs" ("_id" DECIMAL(18,0) NOT NULL, "theme" VARCHAR(100), '
        '"theme|n" BOOLEAN)',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain" ("_id" DECIMAL(18,0) NOT NULL, "next|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next" ("_id" DECIMAL(18,0) NOT NULL, "next|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next" ("_id" DECIMAL(18,0) NOT NULL, "next|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next_next" ("_id" DECIMAL(18,0) NOT NULL, "next|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next_next_next" ("_id" DECIMAL(18,0) NOT NULL, "next|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next_next_next_next" ("_id" DECIMAL(18,0) NOT NULL, "next|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next_next_next_next_next" ("_id" DECIMAL(18,0) NOT NULL, "next|object" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next_next_next_next_next_next" ("_id" DECIMAL(18,0) NOT NULL, '
        '"leaf_note" VARCHAR(100), "leaf_note|n" BOOLEAN, "reading" DECIMAL(18,0), '
        '"reading|string" VARCHAR(100), "reading|n" BOOLEAN, "entries|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_tags_arr" ("_parent" DECIMAL(18,0) NOT NULL, '
        '"_pos" DECIMAL(18,0) NOT NULL, "_value" VARCHAR(100))',
        'CREATE OR REPLACE TABLE "DEEPDOC_metrics_arr" ("_parent" DECIMAL(18,0) NOT NULL, '
        '"_pos" DECIMAL(18,0) NOT NULL, "_value" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" '
        '("_id" DECIMAL(18,0) NOT NULL, "_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, '
        '"value" VARCHAR(100), "kind" VARCHAR(100), "extras|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr_extras_arr" '
        '("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "_value" VARCHAR(100))',
        "INSERT INTO DEEPDOC VALUES (1, 101, 'deep-alpha', 900, 1000, 3, 3)",
        "INSERT INTO DEEPDOC VALUES (2, 102, 'deep-beta', 901, 2000, 1, 1)",
        "INSERT INTO DEEPDOC VALUES (3, 103, 'deep-gamma', NULL, NULL, NULL, NULL)",
        'INSERT INTO "DEEPDOC_profile" VALUES (900, NULL, TRUE, 910)',
        'INSERT INTO "DEEPDOC_profile" VALUES (901, NULL, FALSE, 911)',
        'INSERT INTO "DEEPDOC_profile_prefs" VALUES (910, \'dark\', FALSE)',
        'INSERT INTO "DEEPDOC_profile_prefs" VALUES (911, NULL, TRUE)',
        'INSERT INTO "DEEPDOC_chain" VALUES (1000, 1001)',
        'INSERT INTO "DEEPDOC_chain_next" VALUES (1001, 1002)',
        'INSERT INTO "DEEPDOC_chain_next_next" VALUES (1002, 1003)',
        'INSERT INTO "DEEPDOC_chain_next_next_next" VALUES (1003, 1004)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next" VALUES (1004, 1005)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next" VALUES (1005, 1006)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next" VALUES (1006, 1007)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next" VALUES (1007, \'bottom\', FALSE, 100, NULL, FALSE, 3)',
        'INSERT INTO "DEEPDOC_chain" VALUES (2000, 2001)',
        'INSERT INTO "DEEPDOC_chain_next" VALUES (2001, 2002)',
        'INSERT INTO "DEEPDOC_chain_next_next" VALUES (2002, 2003)',
        'INSERT INTO "DEEPDOC_chain_next_next_next" VALUES (2003, 2004)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next" VALUES (2004, 2005)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next" VALUES (2005, 2006)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next" VALUES (2006, 2007)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next" VALUES (2007, NULL, TRUE, NULL, \'101\', FALSE, 1)',
        'INSERT INTO "DEEPDOC_tags_arr" VALUES (1, 0, \'alpha\')',
        'INSERT INTO "DEEPDOC_tags_arr" VALUES (1, 1, \'beta\')',
        'INSERT INTO "DEEPDOC_tags_arr" VALUES (1, 2, \'gamma\')',
        'INSERT INTO "DEEPDOC_tags_arr" VALUES (2, 0, \'delta\')',
        'INSERT INTO "DEEPDOC_metrics_arr" VALUES (1, 0, 10)',
        'INSERT INTO "DEEPDOC_metrics_arr" VALUES (1, 1, 20)',
        'INSERT INTO "DEEPDOC_metrics_arr" VALUES (1, 2, 30)',
        'INSERT INTO "DEEPDOC_metrics_arr" VALUES (2, 0, 7)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (5000, 1007, 0, \'e0\', \'root\', 2)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (5001, 1007, 1, \'e1\', \'mid\', NULL)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (5002, 1007, 2, \'e2\', \'tail\', 1)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (6000, 2007, 0, \'other\', \'solo\', 1)',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr_extras_arr" VALUES (5000, 0, \'x0\')',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr_extras_arr" VALUES (5000, 1, \'x1\')',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr_extras_arr" VALUES (5002, 0, \'tail-extra\')',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr_extras_arr" VALUES (6000, 0, \'solo-extra\')',
    ]


def install_source_fixture(con, include_deep_fixture: bool = False) -> None:
    statements = _base_fixture_statements()
    if include_deep_fixture:
        statements.extend(_deep_fixture_statements())
    for stmt in statements:
        con.execute(stmt)


def print_query_rows(con, title: str, sql: str) -> None:
    print(f"-- {title} --")
    for row in con.execute(sql).fetchall():
        print(row)
