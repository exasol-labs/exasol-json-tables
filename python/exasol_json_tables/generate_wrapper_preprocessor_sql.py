#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .generate_json_export_helper_sql import helper_names
from .generate_json_export_views_sql import json_export_root_names_from_wrapper_manifest
from .generate_preprocessor_sql import render_sql, validate_identifier


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "dist" / "exasol-json-tables" / "json_wrapper_preprocessor.sql"
DEFAULT_EXPLICIT_NULL_FUNCTION_NAMES = ["JSON_IS_EXPLICIT_NULL", "JNULL"]
DEFAULT_VARIANT_TYPEOF_FUNCTION_NAMES = ["JSON_TYPEOF"]
DEFAULT_VARIANT_VARCHAR_FUNCTION_NAMES = ["JSON_AS_VARCHAR"]
DEFAULT_VARIANT_DECIMAL_FUNCTION_NAMES = ["JSON_AS_DECIMAL"]
DEFAULT_VARIANT_BOOLEAN_FUNCTION_NAMES = ["JSON_AS_BOOLEAN"]
DEFAULT_TO_JSON_FUNCTION_NAMES = ["TO_JSON"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the Exasol SQL preprocessor for the wrapper-view architecture. "
            "It enables path, bracket, rowset, explicit-null, and variant helper syntax over "
            "the public wrapper schema using the generated manifest/helper contract."
        )
    )
    parser.add_argument("--schema", default="JVS_WRAP_PP", help="Schema that will own the preprocessor script.")
    parser.add_argument("--script", default="JSON_WRAPPER_PREPROCESSOR", help="Preprocessor script name.")
    parser.add_argument(
        "--wrapper-schema",
        dest="wrapper_schemas",
        action="append",
        default=None,
        help="Public wrapper schema allowed to use JSON path and array syntax. Repeat to allow multiple schemas.",
    )
    parser.add_argument(
        "--helper-schema",
        dest="helper_schemas",
        action="append",
        default=None,
        help="Internal helper schema paired with each wrapper schema. Default: <wrapper-schema>_INTERNAL.",
    )
    parser.add_argument(
        "--manifest",
        dest="manifest_paths",
        action="append",
        default=None,
        help=(
            "Wrapper manifest JSON file generated for the corresponding wrapper/helper schema pair. "
            "Repeat to match each --wrapper-schema."
        ),
    )
    parser.add_argument(
        "--function-name",
        dest="function_names",
        action="append",
        default=None,
        help="Explicit-null helper name to enable on the wrapper surface. Repeat to install aliases. Default: JSON_IS_EXPLICIT_NULL, JNULL.",
    )
    parser.add_argument(
        "--variant-typeof-function-name",
        dest="variant_typeof_function_names",
        action="append",
        default=None,
        help="Variant typeof helper name to enable. Repeat to install aliases. Default: JSON_TYPEOF.",
    )
    parser.add_argument(
        "--variant-varchar-function-name",
        dest="variant_varchar_function_names",
        action="append",
        default=None,
        help="Variant VARCHAR extraction helper name to enable. Repeat to install aliases. Default: JSON_AS_VARCHAR.",
    )
    parser.add_argument(
        "--variant-decimal-function-name",
        dest="variant_decimal_function_names",
        action="append",
        default=None,
        help="Variant DECIMAL extraction helper name to enable. Repeat to install aliases. Default: JSON_AS_DECIMAL.",
    )
    parser.add_argument(
        "--variant-boolean-function-name",
        dest="variant_boolean_function_names",
        action="append",
        default=None,
        help="Variant BOOLEAN extraction helper name to enable. Repeat to install aliases. Default: JSON_AS_BOOLEAN.",
    )
    parser.add_argument(
        "--blocked-helper-name",
        dest="blocked_helper_names",
        action="append",
        default=None,
        help="Additional helper name that should still fail fast on the wrapper surface.",
    )
    parser.add_argument(
        "--to-json-function-name",
        dest="to_json_function_names",
        action="append",
        default=None,
        help="TO_JSON helper name to enable. Repeat to install aliases. Default: TO_JSON.",
    )
    parser.add_argument(
        "--blocked-helper-message",
        default="This helper is not available on the wrapper surface yet.",
        help="Error message used when a blocked helper is called on the wrapper surface.",
    )
    parser.add_argument(
        "--activate-session",
        action="store_true",
        help="Append an ALTER SESSION statement that activates the generated preprocessor for the current session.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output SQL file.")
    return parser.parse_args()


def _normalize_wrapper_config(
    wrapper_schemas: list[str] | None,
    helper_schemas: list[str] | None,
) -> tuple[list[str], list[str]]:
    raw_wrapper_schemas = wrapper_schemas or ["JSON_VIEW"]
    if helper_schemas is not None and len(helper_schemas) != len(raw_wrapper_schemas):
        raise SystemExit("--helper-schema must be provided the same number of times as --wrapper-schema.")
    raw_helper_schemas = helper_schemas or [f"{wrapper_schema}_INTERNAL" for wrapper_schema in raw_wrapper_schemas]

    normalized_wrapper_schemas = [validate_identifier("Wrapper schema", value) for value in raw_wrapper_schemas]
    normalized_helper_schemas = [validate_identifier("Helper schema", value) for value in raw_helper_schemas]

    mappings: dict[str, str] = {}
    for wrapper_schema, helper_schema in zip(normalized_wrapper_schemas, normalized_helper_schemas):
        if wrapper_schema == helper_schema:
            raise SystemExit(
                f'Wrapper schema "{wrapper_schema}" must differ from its helper schema "{helper_schema}".'
            )
        previous_helper_schema = mappings.get(wrapper_schema)
        if previous_helper_schema is not None and previous_helper_schema != helper_schema:
            raise SystemExit(
                f'Wrapper schema "{wrapper_schema}" was mapped to both "{previous_helper_schema}" and "{helper_schema}".'
            )
        mappings[wrapper_schema] = helper_schema
    return normalized_wrapper_schemas, normalized_helper_schemas


def _load_manifests(
    manifest_paths: list[Path] | None,
    wrapper_schemas: list[str],
    helper_schemas: list[str],
) -> list[dict]:
    if manifest_paths is None or len(manifest_paths) == 0:
        raise SystemExit("Wrapper semantic helper generation requires --manifest for each --wrapper-schema.")
    if len(manifest_paths) != len(wrapper_schemas):
        raise SystemExit("--manifest must be provided the same number of times as --wrapper-schema.")

    manifests: list[dict] = []
    for manifest_path, wrapper_schema, helper_schema in zip(manifest_paths, wrapper_schemas, helper_schemas):
        manifest = json.loads(manifest_path.read_text())
        manifest_public_schema = validate_identifier("Manifest public schema", manifest["publicSchema"])
        manifest_helper_schema = validate_identifier("Manifest helper schema", manifest["helperSchema"])
        if manifest_public_schema != wrapper_schema or manifest_helper_schema != helper_schema:
            raise SystemExit(
                f"Manifest {manifest_path} describes {manifest_public_schema}/{manifest_helper_schema}, "
                f"but the requested wrapper/helper pair is {wrapper_schema}/{helper_schema}."
            )
        manifests.append(manifest)
    return manifests


def _normalize_variant_label(raw_label: str | None) -> str | None:
    normalized = (raw_label or "").upper()
    if normalized in {"BOOL", "BOOLEAN"}:
        return "BOOLEAN"
    if normalized in {"INTEGER", "NUMBER", "DECIMAL", "DOUBLE"}:
        return "NUMBER"
    if normalized in {"STRING", "CHAR", "VARCHAR"}:
        return "STRING"
    if normalized in {"OBJECT", "ARRAY"}:
        return normalized
    return normalized or None


def _infer_variant_label(member: dict) -> str | None:
    member_name = str(member["name"])
    if member_name.endswith("|object"):
        return "OBJECT"
    if member_name.endswith("|array"):
        return "ARRAY"
    if "|" in member_name:
        return _normalize_variant_label(member_name.rsplit("|", 1)[1])
    member_type = str(member["type"]).split("(", 1)[0].strip()
    return _normalize_variant_label(member_type)


def _build_group_config(manifests: list[dict]) -> dict[str, dict[str, dict[str, object]]]:
    config: dict[str, dict[str, dict[str, str]]] = {}
    for manifest in manifests:
        public_schema = validate_identifier("Manifest public schema", manifest["publicSchema"])
        helper_schema = validate_identifier("Manifest helper schema", manifest["helperSchema"])

        roots_by_table = {str(root["tableName"]).upper(): root for root in manifest["roots"]}
        public_schema_tables = config.setdefault(public_schema, {})
        helper_schema_tables = config.setdefault(helper_schema, {})

        for table in manifest["tables"]:
            table_name = validate_identifier("Manifest table name", table["tableName"])
            table_groups: dict[str, dict[str, object]] = {}
            for group in table["groups"]:
                visible_name = group["visibleName"]
                if visible_name is None:
                    continue
                group_config: dict[str, object] = {}
                if group["nullMaskName"] is not None:
                    group_config["nullMaskName"] = str(group["nullMaskName"])
                variant_columns: dict[str, str] = {}
                for member in group["members"]:
                    variant_label = _infer_variant_label(member)
                    if variant_label is None or variant_label in variant_columns:
                        continue
                    variant_columns[variant_label] = str(member["name"])
                if variant_columns:
                    group_config["variantColumns"] = variant_columns
                if group_config:
                    table_groups[str(visible_name).upper()] = group_config
            helper_schema_tables.setdefault(table_name, {}).update(table_groups)
            if table.get("isPublicRoot"):
                public_view_name = validate_identifier("Manifest public view", roots_by_table[table_name]["publicView"])
                public_schema_tables.setdefault(public_view_name, {}).update(table_groups)
    return config


def _build_visible_column_config(manifests: list[dict]) -> dict[str, dict[str, dict[str, bool]]]:
    config: dict[str, dict[str, dict[str, bool]]] = {}
    for manifest in manifests:
        public_schema = validate_identifier("Manifest public schema", manifest["publicSchema"])
        helper_schema = validate_identifier("Manifest helper schema", manifest["helperSchema"])

        roots_by_table = {str(root["tableName"]).upper(): root for root in manifest["roots"]}
        public_schema_tables = config.setdefault(public_schema, {})
        helper_schema_tables = config.setdefault(helper_schema, {})

        for table in manifest["tables"]:
            table_name = validate_identifier("Manifest table name", table["tableName"])
            visible_columns: dict[str, bool] = {"_ID": True}
            for group in table["groups"]:
                visible_name = group["visibleName"]
                if visible_name is None:
                    continue
                visible_columns[str(visible_name).upper()] = True
            helper_schema_tables.setdefault(table_name, {}).update(visible_columns)
            if table.get("isPublicRoot"):
                public_view_name = validate_identifier("Manifest public view", roots_by_table[table_name]["publicView"])
                public_schema_tables.setdefault(public_view_name, {}).update(visible_columns)
    return config


def _build_to_json_config(manifests: list[dict]) -> dict[str, dict[str, dict[str, object]]]:
    config: dict[str, dict[str, dict[str, object]]] = {}
    for manifest in manifests:
        public_schema = validate_identifier("Manifest public schema", manifest["publicSchema"])
        helper_schema = validate_identifier("Manifest helper schema", manifest["helperSchema"])
        public_schema_tables = config.setdefault(public_schema, {})
        helper_udf_names = helper_names(helper_schema)
        export_root_names = json_export_root_names_from_wrapper_manifest(manifest, schema=helper_schema)

        roots_by_table = {str(root["tableName"]).upper(): root for root in manifest["roots"]}
        for root_table, root_names in export_root_names.items():
            root = roots_by_table[root_table]
            argument_to_fragment: dict[str, str] = {}
            display_name_by_argument: dict[str, str] = {}
            for fragment in root_names.fragments:
                normalized_base_name = str(fragment.base_name).upper()
                argument_to_fragment[normalized_base_name] = fragment.column_name
                display_name_by_argument[normalized_base_name] = fragment.base_name

                normalized_visible_name = str(fragment.visible_name).upper()
                argument_to_fragment[normalized_visible_name] = fragment.column_name
                display_name_by_argument[normalized_visible_name] = fragment.visible_name

            public_schema_tables[validate_identifier("Manifest public view", root["publicView"])] = {
                "rootTable": root_table,
                "exportViewQualified": root_names.qualified_view,
                "idColumn": root_names.id_column,
                "fullJsonColumn": root_names.full_json_column,
                "optionalFragmentsFunction": helper_udf_names.json_object_from_optional_fragments,
                "fragmentColumnByArgumentName": argument_to_fragment,
                "displayNameByArgumentName": display_name_by_argument,
            }
    return config


def _add_helper_kind(mapping: dict[str, str], function_name: str, helper_kind: str) -> None:
    previous_kind = mapping.get(function_name)
    if previous_kind is not None and previous_kind != helper_kind:
        raise SystemExit(
            f'Helper function "{function_name}" was assigned multiple helper kinds: {previous_kind}, {helper_kind}.'
        )
    mapping[function_name] = helper_kind


def generate_wrapper_preprocessor_sql_text(
    *,
    schema: str = "JVS_WRAP_PP",
    script: str = "JSON_WRAPPER_PREPROCESSOR",
    wrapper_schemas: list[str] | None = None,
    helper_schemas: list[str] | None = None,
    manifests: list[dict] | None = None,
    function_names: list[str] | None = None,
    variant_typeof_function_names: list[str] | None = None,
    variant_varchar_function_names: list[str] | None = None,
    variant_decimal_function_names: list[str] | None = None,
    variant_boolean_function_names: list[str] | None = None,
    to_json_function_names: list[str] | None = None,
    blocked_helper_names: list[str] | None = None,
    blocked_helper_message: str = "This helper is not available on the wrapper surface yet.",
    activate_session: bool = False,
) -> str:
    normalized_schema = validate_identifier("Schema", schema)
    normalized_script = validate_identifier("Script name", script)
    normalized_wrapper_schemas, normalized_helper_schemas = _normalize_wrapper_config(wrapper_schemas, helper_schemas)
    if manifests is None:
        raise SystemExit("Wrapper semantic helper generation requires manifest data.")
    group_config = _build_group_config(manifests)
    visible_column_config = _build_visible_column_config(manifests)
    to_json_config = _build_to_json_config(manifests)
    normalized_function_names = [
        validate_identifier("Function name", value)
        for value in (function_names or DEFAULT_EXPLICIT_NULL_FUNCTION_NAMES)
    ]
    normalized_variant_typeof_function_names = [
        validate_identifier("Variant typeof function name", value)
        for value in (variant_typeof_function_names or DEFAULT_VARIANT_TYPEOF_FUNCTION_NAMES)
    ]
    normalized_variant_varchar_function_names = [
        validate_identifier("Variant VARCHAR function name", value)
        for value in (variant_varchar_function_names or DEFAULT_VARIANT_VARCHAR_FUNCTION_NAMES)
    ]
    normalized_variant_decimal_function_names = [
        validate_identifier("Variant DECIMAL function name", value)
        for value in (variant_decimal_function_names or DEFAULT_VARIANT_DECIMAL_FUNCTION_NAMES)
    ]
    normalized_variant_boolean_function_names = [
        validate_identifier("Variant BOOLEAN function name", value)
        for value in (variant_boolean_function_names or DEFAULT_VARIANT_BOOLEAN_FUNCTION_NAMES)
    ]
    normalized_to_json_function_names = [
        validate_identifier("TO_JSON function name", value)
        for value in (to_json_function_names or DEFAULT_TO_JSON_FUNCTION_NAMES)
    ]
    helper_function_kinds: dict[str, str] = {}
    for function_name in normalized_function_names:
        _add_helper_kind(helper_function_kinds, function_name, "explicit_null")
    for function_name in normalized_variant_typeof_function_names:
        _add_helper_kind(helper_function_kinds, function_name, "variant_typeof")
    for function_name in normalized_variant_varchar_function_names:
        _add_helper_kind(helper_function_kinds, function_name, "variant_as_varchar")
    for function_name in normalized_variant_decimal_function_names:
        _add_helper_kind(helper_function_kinds, function_name, "variant_as_decimal")
    for function_name in normalized_variant_boolean_function_names:
        _add_helper_kind(helper_function_kinds, function_name, "variant_as_boolean")
    for function_name in normalized_to_json_function_names:
        _add_helper_kind(helper_function_kinds, function_name, "to_json")
    normalized_blocked_helpers = [
        validate_identifier("Blocked helper name", value)
        for value in (blocked_helper_names or [])
    ]
    helper_schema_map = {
        wrapper_schema: helper_schema
        for wrapper_schema, helper_schema in zip(normalized_wrapper_schemas, normalized_helper_schemas)
    }
    regular_to_json_row_object_function = helper_names(normalized_helper_schemas[0]).json_object_from_name_value_pairs
    return render_sql(
        normalized_schema,
        normalized_script,
        list(helper_function_kinds.keys()),
        normalized_blocked_helpers,
        blocked_helper_message,
        normalized_wrapper_schemas,
        helper_schema_map,
        group_config,
        visible_column_config,
        to_json_config,
        regular_to_json_row_object_function,
        True,
        activate_session,
        helper_function_kinds,
    )


def main() -> None:
    args = parse_args()
    sql = generate_wrapper_preprocessor_sql_text(
        schema=args.schema,
        script=args.script,
        wrapper_schemas=args.wrapper_schemas,
        helper_schemas=args.helper_schemas,
        manifests=_load_manifests(
            [Path(path) for path in (args.manifest_paths or [])],
            *_normalize_wrapper_config(args.wrapper_schemas, args.helper_schemas),
        ),
        function_names=args.function_names,
        variant_typeof_function_names=args.variant_typeof_function_names,
        variant_varchar_function_names=args.variant_varchar_function_names,
        variant_decimal_function_names=args.variant_decimal_function_names,
        variant_boolean_function_names=args.variant_boolean_function_names,
        to_json_function_names=args.to_json_function_names,
        blocked_helper_names=args.blocked_helper_names,
        blocked_helper_message=args.blocked_helper_message,
        activate_session=args.activate_session,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(sql)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
