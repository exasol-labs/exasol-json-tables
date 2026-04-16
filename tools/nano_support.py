#!/usr/bin/env python3

from pathlib import Path
import subprocess
from typing import Optional

import pyexasol


ROOT = Path(__file__).resolve().parents[1]


def bundle_adapter() -> str:
    subprocess.run(["python3", str(ROOT / "tools" / "bundle.py")], check=True)
    return (ROOT / "dist" / "adapter.lua").read_text()


def connect():
    return pyexasol.connect(dsn="127.0.0.1:8563", user="sys", password="exasol", schema="SYS")


def install_preprocessor(
    con,
    function_names: list[str],
    rewrite_path_identifiers: bool = False,
    virtual_schemas: Optional[list[str]] = None,
) -> None:
    output_path = ROOT / "dist" / "json_null_preprocessor_test.sql"
    cmd = ["python3", str(ROOT / "tools" / "generate_preprocessor_sql.py"), "--output", str(output_path)]
    for function_name in function_names:
        cmd.extend(["--function-name", function_name])
    for virtual_schema in (virtual_schemas or ["JSON_VS"]):
        cmd.extend(["--virtual-schema", virtual_schema])
    if rewrite_path_identifiers:
        cmd.append("--rewrite-path-identifiers")
    subprocess.run(cmd, check=True)

    content = output_path.read_text()
    schema_name = "JVS_PP"
    script_name = "JSON_NULL_PREPROCESSOR"
    script_marker = f"CREATE OR REPLACE LUA PREPROCESSOR SCRIPT {schema_name}.{script_name} AS\n"
    script_body = content.split(script_marker, 1)[1].split("\n/\n", 1)[0]

    con.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    con.execute(f"CREATE OR REPLACE LUA PREPROCESSOR SCRIPT {schema_name}.{script_name} AS\n" + script_body + "\n/")
    con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {schema_name}.{script_name}")


def _base_fixture_statements() -> list[str]:
    return [
        "DROP FORCE VIRTUAL SCHEMA IF EXISTS JSON_VS CASCADE",
        "DROP SCHEMA IF EXISTS JVS_VS CASCADE",
        "DROP SCHEMA IF EXISTS JVS_SRC CASCADE",
        "CREATE SCHEMA JVS_SRC",
        "OPEN SCHEMA JVS_SRC",
        'CREATE OR REPLACE TABLE SAMPLE ("_id" DECIMAL(18,0) NOT NULL, "id" DECIMAL(18,0), "name" VARCHAR(100), "note" VARCHAR(100), "note|n" BOOLEAN, "child|object" DECIMAL(18,0), "child|n" BOOLEAN, "meta|object" DECIMAL(18,0), "value" DECIMAL(18,0), "value|string" VARCHAR(100), "value|n" BOOLEAN, "shape|object" DECIMAL(18,0), "shape|array" DECIMAL(18,0), "tags|array" DECIMAL(18,0), "items|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "SAMPLE_child" ("_id" DECIMAL(18,0) NOT NULL, "value" VARCHAR(100))',
        'CREATE OR REPLACE TABLE "SAMPLE_meta" ("_id" DECIMAL(18,0) NOT NULL, "info|object" DECIMAL(18,0), "flag" BOOLEAN, "items|array" DECIMAL(18,0))',
        'CREATE OR REPLACE TABLE "SAMPLE_meta_info" ("_id" DECIMAL(18,0) NOT NULL, "note" VARCHAR(100), "note|n" BOOLEAN)',
        'CREATE OR REPLACE TABLE "SAMPLE_tags_arr" ("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "_value" VARCHAR(100))',
        'CREATE OR REPLACE TABLE "SAMPLE_items_arr" ("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "value" VARCHAR(100), "label" VARCHAR(100))',
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
        'INSERT INTO "SAMPLE_items_arr" VALUES (1, 0, \'first\', \'A\')',
        'INSERT INTO "SAMPLE_items_arr" VALUES (1, 1, \'second\', \'B\')',
        'INSERT INTO "SAMPLE_items_arr" VALUES (2, 0, \'only\', \'C\')',
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
        '("_parent" DECIMAL(18,0) NOT NULL, "_pos" DECIMAL(18,0) NOT NULL, "value" VARCHAR(100), "kind" VARCHAR(100))',
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
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (1007, 0, \'e0\', \'root\')',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (1007, 1, \'e1\', \'mid\')',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (1007, 2, \'e2\', \'tail\')',
        'INSERT INTO "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr" VALUES (2007, 0, \'other\', \'solo\')',
    ]


def install_virtual_schema_fixture(con, adapter_code: str, include_deep_fixture: bool = False,
                                   extra_adapter_properties: Optional[dict[str, str]] = None) -> None:
    statements = _base_fixture_statements()
    if include_deep_fixture:
        statements.extend(_deep_fixture_statements())
    property_clauses = ["SCHEMA_NAME='JVS_SRC'"]
    for key, value in (extra_adapter_properties or {}).items():
        escaped_value = value.replace("'", "''")
        property_clauses.append(f"{key}='{escaped_value}'")
    statements.extend([
        "CREATE SCHEMA JVS_VS",
        "OPEN SCHEMA JVS_VS",
        "CREATE OR REPLACE LUA ADAPTER SCRIPT JSON_VS_ADAPTER AS\n" + adapter_code + "\n/",
        'CREATE VIRTUAL SCHEMA JSON_VS USING "JVS_VS"."JSON_VS_ADAPTER" WITH ' + " ".join(property_clauses),
    ])
    for stmt in statements:
        con.execute(stmt)


def print_query_rows(con, title: str, sql: str) -> None:
    print(f"-- {title} --")
    for row in con.execute(sql).fetchall():
        print(row)
