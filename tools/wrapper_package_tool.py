#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from generate_preprocessor_sql import validate_identifier
from generate_wrapper_preprocessor_sql import (
    DEFAULT_EXPLICIT_NULL_FUNCTION_NAMES,
    DEFAULT_VARIANT_BOOLEAN_FUNCTION_NAMES,
    DEFAULT_VARIANT_DECIMAL_FUNCTION_NAMES,
    DEFAULT_VARIANT_TYPEOF_FUNCTION_NAMES,
    DEFAULT_VARIANT_VARCHAR_FUNCTION_NAMES,
    generate_wrapper_preprocessor_sql_text,
)
from wrapper_schema_support import ROOT, connect_for_generation, generate_wrapper_artifacts


DEFAULT_PACKAGE_DIR = ROOT / "examples"
DEFAULT_PACKAGE_NAME = "json_wrapper"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Manage the wrapper-view package lifecycle: generate wrapper artifacts, regenerate the "
            "preprocessor from a validated manifest/config pair, install the package, and validate "
            "the generated or installed package."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate the full wrapper package: views SQL, manifest, preprocessor SQL, and package config.",
    )
    add_connection_arguments(generate_parser)
    add_generation_arguments(generate_parser)

    regenerate_parser = subparsers.add_parser(
        "regenerate-preprocessor",
        help="Regenerate only the preprocessor SQL from an existing package config and manifest.",
    )
    add_package_config_argument(regenerate_parser)
    regenerate_parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Manifest JSON file. Default: the manifest path stored in the package config.",
    )
    regenerate_parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Preprocessor SQL output file. Default: the preprocessor path stored in the package config.",
    )
    regenerate_parser.add_argument(
        "--activate-session",
        action="store_true",
        help="Append an ALTER SESSION statement to the regenerated preprocessor SQL.",
    )

    install_parser = subparsers.add_parser(
        "install",
        help="Install the generated wrapper package into Exasol from an existing package config.",
    )
    add_connection_arguments(install_parser)
    add_package_config_argument(install_parser)
    install_parser.add_argument(
        "--views-sql",
        type=Path,
        default=None,
        help="Wrapper views/helper SQL file. Default: the views SQL path stored in the package config.",
    )
    install_parser.add_argument(
        "--preprocessor-sql",
        type=Path,
        default=None,
        help="Preprocessor SQL file. Default: the preprocessor path stored in the package config.",
    )
    install_parser.add_argument(
        "--skip-views",
        action="store_true",
        help="Do not install the wrapper views/helper schema SQL.",
    )
    install_parser.add_argument(
        "--skip-preprocessor",
        action="store_true",
        help="Do not install the wrapper preprocessor SQL.",
    )
    install_parser.add_argument(
        "--activate-session",
        action="store_true",
        help=(
            "After installation, activate the wrapper preprocessor in the installer session and run a smoke-test query. "
            "This is useful for local/dev verification only; the activation ends when the command exits."
        ),
    )

    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate package files, and optionally validate that the package is installed in Exasol.",
    )
    add_package_config_argument(validate_parser)
    validate_parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Manifest JSON file. Default: the manifest path stored in the package config.",
    )
    validate_parser.add_argument(
        "--views-sql",
        type=Path,
        default=None,
        help="Wrapper views/helper SQL file. Default: the views SQL path stored in the package config.",
    )
    validate_parser.add_argument(
        "--preprocessor-sql",
        type=Path,
        default=None,
        help="Preprocessor SQL file. Default: the preprocessor path stored in the package config.",
    )
    validate_parser.add_argument(
        "--check-installed",
        action="store_true",
        help="Also validate the installed database objects using the supplied Exasol connection parameters.",
    )
    add_connection_arguments(validate_parser, required=False)
    return parser.parse_args()


def add_connection_arguments(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument("--dsn", default="127.0.0.1:8563" if not required else None, help="Exasol DSN.")
    parser.add_argument("--user", default="sys" if not required else None, help="Exasol user.")
    parser.add_argument("--password", default="exasol" if not required else None, help="Exasol password.")


def add_package_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--package-config",
        type=Path,
        required=True,
        help="Wrapper package config JSON generated by the `generate` command.",
    )


def add_generation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-schema", default="JVS_SRC", help="Physical source schema.")
    parser.add_argument("--wrapper-schema", default="JSON_VIEW", help="Generated public wrapper schema.")
    parser.add_argument(
        "--helper-schema",
        default=None,
        help="Generated internal helper schema. Default: <wrapper-schema>_INTERNAL.",
    )
    parser.add_argument(
        "--preprocessor-schema",
        default="JVS_WRAP_PP",
        help="Schema that will own the generated wrapper preprocessor script.",
    )
    parser.add_argument(
        "--preprocessor-script",
        default="JSON_WRAPPER_PREPROCESSOR",
        help="Generated wrapper preprocessor script name.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_PACKAGE_DIR,
        help="Directory where the package files will be written.",
    )
    parser.add_argument(
        "--package-name",
        default=DEFAULT_PACKAGE_NAME,
        help="Package file prefix. Generated files will be named <package-name>_views.sql etc.",
    )
    parser.add_argument(
        "--function-name",
        dest="function_names",
        action="append",
        default=None,
        help="Explicit-null helper name to enable. Repeat to install aliases.",
    )
    parser.add_argument(
        "--variant-typeof-function-name",
        dest="variant_typeof_function_names",
        action="append",
        default=None,
        help="Variant typeof helper name to enable. Repeat to install aliases.",
    )
    parser.add_argument(
        "--variant-varchar-function-name",
        dest="variant_varchar_function_names",
        action="append",
        default=None,
        help="Variant VARCHAR extraction helper name to enable. Repeat to install aliases.",
    )
    parser.add_argument(
        "--variant-decimal-function-name",
        dest="variant_decimal_function_names",
        action="append",
        default=None,
        help="Variant DECIMAL extraction helper name to enable. Repeat to install aliases.",
    )
    parser.add_argument(
        "--variant-boolean-function-name",
        dest="variant_boolean_function_names",
        action="append",
        default=None,
        help="Variant BOOLEAN extraction helper name to enable. Repeat to install aliases.",
    )
    parser.add_argument(
        "--blocked-helper-name",
        dest="blocked_helper_names",
        action="append",
        default=None,
        help="Additional helper name that should fail fast on the wrapper surface.",
    )
    parser.add_argument(
        "--blocked-helper-message",
        default="This helper is not available on the wrapper surface yet.",
        help="Error message used when a blocked helper is called on the wrapper surface.",
    )
    parser.add_argument(
        "--activate-session",
        action="store_true",
        help="Append an ALTER SESSION statement to the generated preprocessor SQL.",
    )


def validate_distinct_schemas(source_schema: str, wrapper_schema: str, helper_schema: str) -> None:
    if len({source_schema, wrapper_schema, helper_schema}) != 3:
        raise SystemExit(
            "Source schema, wrapper schema, and helper schema must all be distinct "
            f"(got source={source_schema}, wrapper={wrapper_schema}, helper={helper_schema})."
        )


def build_package_paths(output_dir: Path, package_name: str) -> dict[str, Path]:
    output_dir = output_dir.resolve()
    package_name = package_name.strip()
    if package_name == "":
        raise SystemExit("Package name must not be empty.")
    return {
        "viewsSql": output_dir / f"{package_name}_views.sql",
        "manifest": output_dir / f"{package_name}_manifest.json",
        "preprocessorSql": output_dir / f"{package_name}_preprocessor.sql",
        "packageConfig": output_dir / f"{package_name}_package.json",
    }


def load_package_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def relative_path_text(path: Path, *, base_dir: Path) -> str:
    return str(path.resolve().relative_to(base_dir.resolve()))


def resolve_configured_path(config_path: Path, relative_or_absolute: str) -> Path:
    configured = Path(relative_or_absolute)
    if configured.is_absolute():
        return configured
    return config_path.resolve().parent / configured


def package_config_from_args(args: argparse.Namespace, paths: dict[str, Path]) -> dict[str, Any]:
    source_schema = validate_identifier("Source schema", args.source_schema)
    wrapper_schema = validate_identifier("Wrapper schema", args.wrapper_schema)
    helper_schema = validate_identifier("Helper schema", args.helper_schema or f"{args.wrapper_schema}_INTERNAL")
    validate_distinct_schemas(source_schema, wrapper_schema, helper_schema)
    preprocessor_schema = validate_identifier("Preprocessor schema", args.preprocessor_schema)
    preprocessor_script = validate_identifier("Preprocessor script", args.preprocessor_script)

    function_names = [
        validate_identifier("Function name", value)
        for value in (args.function_names or DEFAULT_EXPLICIT_NULL_FUNCTION_NAMES)
    ]
    variant_typeof_function_names = [
        validate_identifier("Variant typeof function name", value)
        for value in (args.variant_typeof_function_names or DEFAULT_VARIANT_TYPEOF_FUNCTION_NAMES)
    ]
    variant_varchar_function_names = [
        validate_identifier("Variant VARCHAR function name", value)
        for value in (args.variant_varchar_function_names or DEFAULT_VARIANT_VARCHAR_FUNCTION_NAMES)
    ]
    variant_decimal_function_names = [
        validate_identifier("Variant DECIMAL function name", value)
        for value in (args.variant_decimal_function_names or DEFAULT_VARIANT_DECIMAL_FUNCTION_NAMES)
    ]
    variant_boolean_function_names = [
        validate_identifier("Variant BOOLEAN function name", value)
        for value in (args.variant_boolean_function_names or DEFAULT_VARIANT_BOOLEAN_FUNCTION_NAMES)
    ]
    blocked_helper_names = [
        validate_identifier("Blocked helper name", value)
        for value in (args.blocked_helper_names or [])
    ]

    output_dir = Path(args.output_dir).resolve()
    return {
        "version": 1,
        "packageName": args.package_name,
        "sourceSchema": source_schema,
        "wrapperSchema": wrapper_schema,
        "helperSchema": helper_schema,
        "preprocessor": {
            "schema": preprocessor_schema,
            "script": preprocessor_script,
            "activateSession": bool(args.activate_session),
        },
        "helperProfile": {
            "explicitNullFunctionNames": function_names,
            "variantTypeofFunctionNames": variant_typeof_function_names,
            "variantVarcharFunctionNames": variant_varchar_function_names,
            "variantDecimalFunctionNames": variant_decimal_function_names,
            "variantBooleanFunctionNames": variant_boolean_function_names,
            "blockedHelperNames": blocked_helper_names,
            "blockedHelperMessage": args.blocked_helper_message,
        },
        "generatedFiles": {
            "viewsSql": relative_path_text(paths["viewsSql"], base_dir=output_dir),
            "manifest": relative_path_text(paths["manifest"], base_dir=output_dir),
            "preprocessorSql": relative_path_text(paths["preprocessorSql"], base_dir=output_dir),
        },
    }


def load_manifest_and_validate(config: dict[str, Any], manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    manifest_public_schema = validate_identifier("Manifest public schema", manifest["publicSchema"])
    manifest_helper_schema = validate_identifier("Manifest helper schema", manifest["helperSchema"])
    if manifest_public_schema != config["wrapperSchema"] or manifest_helper_schema != config["helperSchema"]:
        raise SystemExit(
            f"Manifest {manifest_path} describes {manifest_public_schema}/{manifest_helper_schema}, "
            f"but the package config expects {config['wrapperSchema']}/{config['helperSchema']}."
        )
    return manifest


def generate_preprocessor_from_package_config(config: dict[str, Any], manifest: dict[str, Any]) -> str:
    helper_profile = config["helperProfile"]
    preprocessor_config = config["preprocessor"]
    return generate_wrapper_preprocessor_sql_text(
        schema=preprocessor_config["schema"],
        script=preprocessor_config["script"],
        wrapper_schemas=[config["wrapperSchema"]],
        helper_schemas=[config["helperSchema"]],
        manifests=[manifest],
        function_names=helper_profile["explicitNullFunctionNames"],
        variant_typeof_function_names=helper_profile["variantTypeofFunctionNames"],
        variant_varchar_function_names=helper_profile["variantVarcharFunctionNames"],
        variant_decimal_function_names=helper_profile["variantDecimalFunctionNames"],
        variant_boolean_function_names=helper_profile["variantBooleanFunctionNames"],
        blocked_helper_names=helper_profile["blockedHelperNames"],
        blocked_helper_message=helper_profile["blockedHelperMessage"],
        activate_session=bool(preprocessor_config["activateSession"]),
    )


def split_plain_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    chars: list[str] = []
    in_string = False
    index = 0
    while index < len(sql_text):
        ch = sql_text[index]
        chars.append(ch)
        if ch == "'":
            if in_string and index + 1 < len(sql_text) and sql_text[index + 1] == "'":
                chars.append(sql_text[index + 1])
                index += 2
                continue
            in_string = not in_string
        elif ch == ";" and not in_string:
            statement = "".join(chars[:-1]).strip()
            if statement:
                statements.append(statement)
            chars = []
        index += 1
    trailing = "".join(chars).strip()
    if trailing:
        statements.append(trailing)
    return statements


def execute_plain_sql_file(con, sql_text: str) -> None:
    for statement in split_plain_sql_statements(sql_text):
        con.execute(statement)


def strip_leading_comments(sql_text: str) -> str:
    lines = sql_text.splitlines()
    index = 0
    while index < len(lines) and lines[index].lstrip().startswith("--"):
        index += 1
    return "\n".join(lines[index:]).strip() + "\n"


def execute_generated_preprocessor_sql(con, sql_text: str) -> None:
    cleaned = strip_leading_comments(sql_text)
    marker = "CREATE OR REPLACE LUA PREPROCESSOR SCRIPT "
    marker_index = cleaned.find(marker)
    if marker_index == -1:
        raise ValueError("Generated preprocessor SQL is missing CREATE OR REPLACE LUA PREPROCESSOR SCRIPT.")

    prefix_sql = cleaned[:marker_index].strip()
    if prefix_sql:
        execute_plain_sql_file(con, prefix_sql)

    script_tail = cleaned[marker_index:]
    script_end_marker = "\n/\n"
    script_end_index = script_tail.find(script_end_marker)
    if script_end_index == -1:
        raise ValueError("Generated preprocessor SQL is missing the terminating '/' line.")
    script_sql = script_tail[: script_end_index + len("\n/")].strip()
    con.execute(script_sql)

    suffix_sql = script_tail[script_end_index + len(script_end_marker):].strip()
    if suffix_sql:
        execute_plain_sql_file(con, suffix_sql)


def encode_quoted_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def build_activation_sql(config: dict[str, Any], *, include_semicolon: bool = True) -> str:
    sql = (
        "ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "
        f'{encode_quoted_identifier(config["preprocessor"]["schema"])}.{encode_quoted_identifier(config["preprocessor"]["script"])}'
    )
    if include_semicolon:
        return sql + ";"
    return sql


def iter_scalar_group_names(table: dict[str, Any]) -> list[str]:
    scalar_names: list[str] = []
    for group in table.get("groups", []):
        visible_name = group["visibleName"]
        if visible_name == "_id":
            continue
        if visible_name.endswith("|object") or visible_name.endswith("|array"):
            continue
        scalar_names.append(visible_name)
    return scalar_names


def score_smoke_scalar_name(name: str) -> tuple[int, int, str]:
    normalized = name.lower()
    exact_scores = {
        "title": 0,
        "name": 1,
        "label": 2,
        "status": 3,
        "type": 4,
        "value": 5,
        "theme": 6,
        "doc_id": 10,
        "id": 11,
    }
    if normalized in exact_scores:
        return (exact_scores[normalized], len(normalized), normalized)
    if normalized.endswith("_id"):
        return (12, len(normalized), normalized)
    if any(token in normalized for token in ["title", "name", "label", "status", "type", "value", "theme"]):
        return (14, len(normalized), normalized)
    if any(token in normalized for token in ["note", "nickname", "optional", "comment", "description", "message"]):
        return (40, len(normalized), normalized)
    return (20, len(normalized), normalized)


def choose_preferred_scalar_name(scalar_names: list[str], *, exclude: set[str] | None = None) -> str | None:
    exclude = exclude or set()
    candidates = [name for name in scalar_names if name not in exclude]
    if not candidates:
        return None
    return min(candidates, key=score_smoke_scalar_name)


def choose_display_id_name(scalar_names: list[str]) -> str:
    for name in scalar_names:
        normalized = name.lower()
        if normalized == "id" or normalized == "doc_id" or normalized.endswith("_id"):
            return name
    return "_id"


def build_smoke_test_query(config: dict[str, Any], manifest: dict[str, Any]) -> str:
    table_lookup = {table["tableName"]: table for table in manifest["tables"]}
    wrapper_schema = config["wrapperSchema"]
    helper_profile = config.get("helperProfile", {})
    variant_varchar_helpers = helper_profile.get("variantVarcharFunctionNames") or []

    def cast_id_expression(column_name: str) -> str:
        return f'CAST("{column_name}" AS VARCHAR(200))'

    def build_helper_smoke_query(root: dict[str, Any], root_table: dict[str, Any]) -> str | None:
        if not variant_varchar_helpers:
            return None
        scalar_names = iter_scalar_group_names(root_table)
        if not scalar_names:
            return None
        display_id_name = choose_display_id_name(scalar_names)
        sample_name = choose_preferred_scalar_name(scalar_names, exclude={display_id_name})
        if sample_name is None:
            sample_name = choose_preferred_scalar_name(scalar_names)
        if sample_name is None:
            return None
        helper_name = variant_varchar_helpers[0]
        helper_expr = f'{helper_name}("{sample_name}")'
        return (
            f'SELECT {cast_id_expression(display_id_name)} AS "sample_id", '
            f"COALESCE({helper_expr}, 'NULL') AS \"sample_value\" "
            f'FROM {encode_quoted_identifier(wrapper_schema)}.{encode_quoted_identifier(root["publicView"])} '
            f"ORDER BY CASE WHEN {helper_expr} IS NULL THEN 1 ELSE 0 END, 1 "
            "LIMIT 5;"
        )

    for root in sorted(manifest["roots"], key=lambda root: root["publicView"]):
        root_table = table_lookup[root["tableName"]]
        helper_query = build_helper_smoke_query(root, root_table)
        if helper_query is not None:
            return helper_query

        relationship_lookup: dict[str, list[dict[str, Any]]] = {}
        for relationship in root.get("relationships", []):
            relationship_lookup.setdefault(relationship["parentTable"], []).append(relationship)

        def find_object_scalar_paths(table_name: str, prefix: list[str]) -> list[list[str]]:
            out: list[list[str]] = []
            object_relationships = [
                relationship for relationship in relationship_lookup.get(table_name, [])
                if relationship["relationKind"] == "object"
            ]
            for relationship in object_relationships:
                segment = relationship["segmentName"]
                child_table = table_lookup.get(relationship["childTable"])
                if child_table is None:
                    continue
                for scalar_name in iter_scalar_group_names(child_table):
                    out.append(prefix + [segment, scalar_name])
            for relationship in object_relationships:
                segment = relationship["segmentName"]
                child_table = table_lookup.get(relationship["childTable"])
                if child_table is None:
                    continue
                out.extend(find_object_scalar_paths(relationship["childTable"], prefix + [segment]))
            return out

        object_path_candidates = find_object_scalar_paths(root["tableName"], [])
        if object_path_candidates:
            display_id_name = choose_display_id_name(iter_scalar_group_names(root_table))
            object_path = min(
                object_path_candidates,
                key=lambda path: (len(path), score_smoke_scalar_name(path[-1]), ".".join(path)),
            )
            object_expr = f'"{ ".".join(object_path) }"'
            return (
                f'SELECT {cast_id_expression(display_id_name)} AS "sample_id", '
                f"COALESCE({object_expr}, 'NULL') AS \"sample_value\" "
                f'FROM {encode_quoted_identifier(wrapper_schema)}.{encode_quoted_identifier(root["publicView"])} '
                f"ORDER BY CASE WHEN {object_expr} IS NULL THEN 1 ELSE 0 END, 1 "
                "LIMIT 5;"
            )

        root_array_relationships = [
            relationship for relationship in root.get("relationships", [])
            if relationship["parentTable"] == root["tableName"] and relationship["relationKind"] == "array"
        ]
        if root_array_relationships:
            segment = root_array_relationships[0]["segmentName"]
            display_id_name = choose_display_id_name(iter_scalar_group_names(root_table))
            array_expr = f'CAST("{segment}[SIZE]" AS VARCHAR(20))'
            return (
                f'SELECT {cast_id_expression(display_id_name)} AS "sample_id", '
                f"COALESCE({array_expr}, 'NULL') AS \"sample_value\" "
                f'FROM {encode_quoted_identifier(wrapper_schema)}.{encode_quoted_identifier(root["publicView"])} '
                f"ORDER BY CASE WHEN {array_expr} IS NULL THEN 1 ELSE 0 END, 1 "
                "LIMIT 5;"
            )

        return (
            f"SELECT * FROM {encode_quoted_identifier(wrapper_schema)}."
            f'{encode_quoted_identifier(root["publicView"])} LIMIT 5;'
        )

    raise SystemExit("Manifest does not contain any root views for the wrapper package.")


def print_install_next_steps(config: dict[str, Any], smoke_test_sql: str) -> None:
    print("Next steps:")
    print(build_activation_sql(config, include_semicolon=True))
    print(smoke_test_sql)


def print_activation_reminder(config: dict[str, Any], smoke_test_sql: str) -> None:
    print("Activation reminder:")
    print(build_activation_sql(config, include_semicolon=True))
    print(smoke_test_sql)


def validate_package_files(config_path: Path, config: dict[str, Any], manifest_path: Path, views_sql_path: Path, preprocessor_sql_path: Path) -> None:
    if not views_sql_path.exists():
        raise SystemExit(f"Wrapper views SQL file does not exist: {views_sql_path}")
    if not manifest_path.exists():
        raise SystemExit(f"Manifest file does not exist: {manifest_path}")
    if not preprocessor_sql_path.exists():
        raise SystemExit(f"Preprocessor SQL file does not exist: {preprocessor_sql_path}")
    if config["wrapperSchema"] == config["helperSchema"]:
        raise SystemExit("Wrapper schema and helper schema must differ in the package config.")
    if config["sourceSchema"] in {config["wrapperSchema"], config["helperSchema"]}:
        raise SystemExit("Source schema must differ from the wrapper and helper schemas in the package config.")
    load_manifest_and_validate(config, manifest_path)


def validate_installed_package(con, config: dict[str, Any], manifest: dict[str, Any]) -> None:
    wrapper_schema = config["wrapperSchema"]
    helper_schema = config["helperSchema"]
    roots = sorted(root["publicView"] for root in manifest["roots"])
    public_tables = [
        row[0] for row in con.execute(
            f"""
            SELECT DISTINCT COLUMN_TABLE
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = '{wrapper_schema}'
            ORDER BY COLUMN_TABLE
            """
        ).fetchall()
    ]
    if public_tables != roots:
        raise SystemExit(f"Installed public wrapper schema does not match manifest roots. Expected {roots}, got {public_tables}.")

    helper_table_names = {
        row[0]
        for row in con.execute(
            f"""
            SELECT DISTINCT COLUMN_TABLE
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = '{helper_schema}'
            """
        ).fetchall()
    }
    expected_helper_tables = {table["tableName"] for table in manifest["tables"]}
    expected_helper_tables.update({"__JVS_ROOTS", "__JVS_RELATIONSHIPS", "__JVS_COLUMN_MEMBERS"})
    missing_helper_tables = sorted(expected_helper_tables - helper_table_names)
    if missing_helper_tables:
        raise SystemExit(f"Installed helper schema is missing expected tables/views: {missing_helper_tables}")

    root_count = con.execute(
        f'SELECT COUNT(*) FROM "{helper_schema}"."__JVS_ROOTS"'
    ).fetchone()[0]
    if int(root_count) != len(manifest["roots"]):
        raise SystemExit(
            f'Installed "__JVS_ROOTS" count mismatch. Expected {len(manifest["roots"])}, got {root_count}.'
        )

    script_schema = config["preprocessor"]["schema"]
    script_name = config["preprocessor"]["script"]
    script_rows = con.execute(
        f"""
        SELECT COUNT(*)
        FROM SYS.EXA_ALL_SCRIPTS
        WHERE SCRIPT_SCHEMA = '{script_schema}'
          AND SCRIPT_NAME = '{script_name}'
        """
    ).fetchone()
    if script_rows is None or int(script_rows[0]) != 1:
        raise SystemExit(f"Installed preprocessor script {script_schema}.{script_name} was not found.")


def command_generate(args: argparse.Namespace) -> None:
    paths = build_package_paths(args.output_dir, args.package_name)
    config = package_config_from_args(args, paths)
    con = connect_for_generation(args.dsn, args.user, args.password)
    try:
        artifacts = generate_wrapper_artifacts(
            con,
            config["sourceSchema"],
            config["wrapperSchema"],
            config["helperSchema"],
        )
    finally:
        con.close()

    paths["viewsSql"].parent.mkdir(parents=True, exist_ok=True)
    paths["viewsSql"].write_text(artifacts.sql)
    write_json(paths["manifest"], artifacts.manifest)
    paths["preprocessorSql"].write_text(generate_preprocessor_from_package_config(config, artifacts.manifest))
    write_json(paths["packageConfig"], config)

    print(f"Wrote {paths['viewsSql']}")
    print(f"Wrote {paths['manifest']}")
    print(f"Wrote {paths['preprocessorSql']}")
    print(f"Wrote {paths['packageConfig']}")


def command_regenerate_preprocessor(args: argparse.Namespace) -> None:
    config_path = args.package_config.resolve()
    config = load_package_config(config_path)
    manifest_path = args.manifest or resolve_configured_path(config_path, config["generatedFiles"]["manifest"])
    manifest = load_manifest_and_validate(config, manifest_path)
    output_path = (args.output or resolve_configured_path(config_path, config["generatedFiles"]["preprocessorSql"])).resolve()
    regenerated_config = json.loads(json.dumps(config))
    if args.activate_session:
        regenerated_config["preprocessor"]["activateSession"] = True
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(generate_preprocessor_from_package_config(regenerated_config, manifest))
    print(f"Wrote {output_path}")


def command_install(args: argparse.Namespace) -> None:
    config_path = args.package_config.resolve()
    config = load_package_config(config_path)
    views_sql_path = (args.views_sql or resolve_configured_path(config_path, config["generatedFiles"]["viewsSql"])).resolve()
    preprocessor_sql_path = (
        args.preprocessor_sql or resolve_configured_path(config_path, config["generatedFiles"]["preprocessorSql"])
    ).resolve()
    manifest_path = resolve_configured_path(config_path, config["generatedFiles"]["manifest"]).resolve()
    validate_package_files(config_path, config, manifest_path, views_sql_path, preprocessor_sql_path)
    manifest = load_manifest_and_validate(config, manifest_path)
    smoke_test_sql = build_smoke_test_query(config, manifest)
    smoke_test_rows = None

    con = connect_for_generation(args.dsn, args.user, args.password)
    try:
        if not args.skip_views:
            execute_plain_sql_file(con, views_sql_path.read_text())
        if not args.skip_preprocessor:
            execute_generated_preprocessor_sql(con, preprocessor_sql_path.read_text())
        if args.activate_session:
            con.execute(build_activation_sql(config, include_semicolon=False))
            smoke_test_rows = con.execute(smoke_test_sql).fetchall()
    finally:
        con.close()

    if not args.skip_views:
        print(f"Installed {views_sql_path}")
    if not args.skip_preprocessor:
        print(f"Installed {preprocessor_sql_path}")
    print_install_next_steps(config, smoke_test_sql)
    if args.activate_session:
        print("Activated preprocessor in the installer session and ran the smoke test.")
        print("Activation note: this activation is session-local and ends when the install command exits.")
        print("Smoke test rows:")
        print(smoke_test_rows)


def command_validate(args: argparse.Namespace) -> None:
    config_path = args.package_config.resolve()
    config = load_package_config(config_path)
    manifest_path = (args.manifest or resolve_configured_path(config_path, config["generatedFiles"]["manifest"])).resolve()
    views_sql_path = (args.views_sql or resolve_configured_path(config_path, config["generatedFiles"]["viewsSql"])).resolve()
    preprocessor_sql_path = (
        args.preprocessor_sql or resolve_configured_path(config_path, config["generatedFiles"]["preprocessorSql"])
    ).resolve()
    validate_package_files(config_path, config, manifest_path, views_sql_path, preprocessor_sql_path)
    manifest = load_manifest_and_validate(config, manifest_path)

    if args.check_installed:
        con = connect_for_generation(args.dsn, args.user, args.password)
        try:
            validate_installed_package(con, config, manifest)
        finally:
            con.close()

    print(f"Validated {config_path}")
    if args.check_installed:
        print(
            "Validated installed package for "
            f'{config["wrapperSchema"]}/{config["helperSchema"]}/{config["preprocessor"]["schema"]}.{config["preprocessor"]["script"]}'
        )
        print_activation_reminder(config, build_smoke_test_query(config, manifest))


def main() -> None:
    args = parse_args()
    if args.command == "generate":
        command_generate(args)
    elif args.command == "regenerate-preprocessor":
        command_regenerate_preprocessor(args)
    elif args.command == "install":
        command_install(args)
    elif args.command == "validate":
        command_validate(args)
    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
