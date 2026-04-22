#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .generate_json_export_helper_sql import DEFAULT_SCHEMA as DEFAULT_UDF_SCHEMA
from .generate_json_export_helper_sql import helper_names
from .generate_preprocessor_sql import validate_identifier
from .wrapper_schema_support import (
    ColumnMeta,
    Group,
    ROOT,
    TableModel,
    build_relationships,
    build_table_models,
    choose_visible_member,
    fetch_source_columns,
    find_root_tables,
    quote_identifier,
    quote_qualified,
    source_columns_from_manifest,
    sql_literal,
    visible_name_for_group,
)


DEFAULT_OUTPUT = ROOT / "dist" / "exasol-json-tables" / "json_export_views.sql"
DEFAULT_SCHEMA = "JVS_JSON_EXPORT_VIEWS"
FULL_JSON_COLUMN = "__JVS_JSON_FULL"
ROW_KEY_COLUMN = "__JVS_ROW_KEY"
ID_COLUMN = "_id"


@dataclass(frozen=True)
class JsonExportFragmentColumn:
    base_name: str
    visible_name: str
    column_name: str


@dataclass(frozen=True)
class JsonExportRootNames:
    root_table: str
    schema: str
    view_name: str
    qualified_view: str
    fragments: tuple[JsonExportFragmentColumn, ...]
    id_column: str = ID_COLUMN
    full_json_column: str = FULL_JSON_COLUMN

    def fragment_column_for_base_name(self, base_name: str) -> str | None:
        for fragment in self.fragments:
            if fragment.base_name == base_name:
                return fragment.column_name
        return None

    def fragment_column_for_visible_name(self, visible_name: str) -> str | None:
        for fragment in self.fragments:
            if fragment.visible_name == visible_name:
                return fragment.column_name
        return None


@dataclass(frozen=True)
class JsonExportArtifacts:
    sql: str
    schema: str
    udf_schema: str
    root_tables: tuple[str, ...]
    root_names: dict[str, JsonExportRootNames]
    select_sql_by_root: dict[str, str]


def _encode_internal_name_component(value: str) -> str:
    out: list[str] = []
    for ch in value:
        if ch.isalnum() or ch == "_":
            out.append(ch)
        else:
            out.append(f"_X{ord(ch):02X}_")
    return "".join(out)


def json_export_view_name(root_table: str) -> str:
    return f"__JVS_JSON_EXPORT_{validate_identifier('Root table', root_table)}"


def json_export_fragment_column_name(base_name: str) -> str:
    return f"__JVS_FRAG_{_encode_internal_name_component(base_name)}"


def _build_root_names(root_table: str, schema: str, groups: list[tuple[str, str]]) -> JsonExportRootNames:
    validated_schema = validate_identifier("Export schema", schema)
    validated_root_table = validate_identifier("Root table", root_table)
    fragments = tuple(
        JsonExportFragmentColumn(
            base_name=base_name,
            visible_name=visible_name,
            column_name=json_export_fragment_column_name(base_name),
        )
        for base_name, visible_name in sorted(groups)
    )
    view_name = json_export_view_name(validated_root_table)
    return JsonExportRootNames(
        root_table=validated_root_table,
        schema=validated_schema,
        view_name=view_name,
        qualified_view=quote_qualified(validated_schema, view_name),
        fragments=fragments,
    )


def json_export_root_names_from_wrapper_manifest(
    manifest: dict[str, Any],
    schema: str | None = None,
) -> dict[str, JsonExportRootNames]:
    export_schema = validate_identifier("Export schema", schema or manifest["helperSchema"])
    root_tables = {validate_identifier("Manifest root table", root["tableName"]) for root in manifest["roots"]}
    root_specs = {
        validate_identifier("Manifest table name", table["tableName"]): table
        for table in manifest["tables"]
        if validate_identifier("Manifest table name", table["tableName"]) in root_tables
    }
    names_by_root: dict[str, JsonExportRootNames] = {}
    for root_table in sorted(root_tables):
        root_spec = root_specs[root_table]
        groups: list[tuple[str, str]] = []
        for group in root_spec["groups"]:
            visible_name = group["visibleName"]
            if visible_name is None:
                continue
            groups.append((str(group["baseName"]), str(visible_name)))
        names_by_root[root_table] = _build_root_names(root_table, export_schema, groups)
    return names_by_root


def _cte_name(prefix: str, table_name: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in table_name)
    return f"{prefix}_{safe}"


def _is_string_type(type_name: str) -> bool:
    upper = type_name.upper()
    return upper.startswith("VARCHAR") or upper.startswith("CHAR")


def _is_boolean_type(type_name: str) -> bool:
    return type_name.upper().startswith("BOOLEAN")


def _is_numeric_type(type_name: str) -> bool:
    upper = type_name.upper()
    return upper.startswith("DECIMAL") or upper.startswith("DOUBLE")


def _scalar_value_json_expr(quote_string_udf: str, column: ColumnMeta, expr: str) -> str:
    if _is_numeric_type(column.type_name):
        return f"CAST({expr} AS VARCHAR(2000000))"
    if _is_boolean_type(column.type_name):
        return f"CASE WHEN {expr} THEN 'true' ELSE 'false' END"
    if _is_string_type(column.type_name):
        return f"{quote_string_udf}({expr})"
    return f"{quote_string_udf}(CAST({expr} AS VARCHAR(2000000)))"


def _scalar_group_fragment_expr(
    quote_string_udf: str,
    group: Group,
    base_alias: str,
    *,
    include_null_mask: bool = True,
) -> tuple[str, str] | None:
    value_branches: list[tuple[str, str]] = []
    if group.primary is not None:
        expr = f'{base_alias}.{quote_identifier(group.primary.name)}'
        value_branches.append((f"{expr} IS NOT NULL", _scalar_value_json_expr(quote_string_udf, group.primary, expr)))
    for alternate in sorted(group.alternates, key=lambda column: column.ordinal):
        expr = f'{base_alias}.{quote_identifier(alternate.name)}'
        value_branches.append((f"{expr} IS NOT NULL", _scalar_value_json_expr(quote_string_udf, alternate, expr)))

    if not value_branches and (group.null_mask is None or not include_null_mask):
        return None

    fragments: list[str] = []
    presence_terms: list[str] = []
    property_prefix = sql_literal(f'"{group.base_name}":')
    property_null = sql_literal(f'"{group.base_name}":null')
    for condition, rendered in value_branches:
        fragments.append(f"WHEN {condition} THEN {property_prefix} || {rendered}")
        presence_terms.append(condition)
    if include_null_mask and group.null_mask is not None:
        null_mask_expr = f'{base_alias}.{quote_identifier(group.null_mask.name)}'
        fragments.append(f"WHEN {null_mask_expr} IS TRUE THEN {property_null}")
        presence_terms.append(f"{null_mask_expr} IS TRUE")

    if not fragments or not presence_terms:
        return None

    return ("CASE " + " ".join(fragments) + " END", " OR ".join(presence_terms))


def _value_group_json_expr(
    quote_string_udf: str,
    group: Group,
    base_alias: str,
    *,
    object_alias: str | None = None,
    array_alias: str | None = None,
) -> str | None:
    branches: list[tuple[str, str]] = []
    if group.primary is not None:
        expr = f'{base_alias}.{quote_identifier(group.primary.name)}'
        branches.append((f"{expr} IS NOT NULL", _scalar_value_json_expr(quote_string_udf, group.primary, expr)))
    for alternate in sorted(group.alternates, key=lambda column: column.ordinal):
        expr = f'{base_alias}.{quote_identifier(alternate.name)}'
        branches.append((f"{expr} IS NOT NULL", _scalar_value_json_expr(quote_string_udf, alternate, expr)))
    if group.object_member is not None:
        marker_expr = f'{base_alias}.{quote_identifier(group.object_member.name)}'
        if object_alias is None:
            raise AssertionError(f"Value group {group.base_name} is missing an object export alias")
        branches.append((f"{marker_expr} IS NOT NULL", f"{object_alias}.j"))
    if group.array_member is not None:
        marker_expr = f'{base_alias}.{quote_identifier(group.array_member.name)}'
        if array_alias is None:
            raise AssertionError(f"Value group {group.base_name} is missing an array export alias")
        branches.append((f"{marker_expr} IS NOT NULL", f"COALESCE({array_alias}.j, '[]')"))
    if group.null_mask is not None:
        null_mask_expr = f'{base_alias}.{quote_identifier(group.null_mask.name)}'
        branches.append((f"{null_mask_expr} IS TRUE", "'null'"))
    if not branches:
        return None
    return "CASE " + " ".join(f"WHEN {condition} THEN {rendered}" for condition, rendered in branches) + " END"


def _structural_key_columns(model: TableModel) -> list[str]:
    names = {column.name for column in model.columns}
    return [name for name in ("_id", "_parent", "_pos") if name in names]


def _row_key_expr(model: TableModel, base_alias: str) -> str:
    key_columns = _structural_key_columns(model)
    if not key_columns:
        raise AssertionError(f"Table {model.name} has no structural key columns")
    parts = [
        sql_literal(f"{column_name}=")
        + f" || CAST({base_alias}.{quote_identifier(column_name)} AS VARCHAR(2000000))"
        for column_name in key_columns
    ]
    return " || '|' || ".join(parts)


def _root_groups(model: TableModel) -> list[tuple[str, str]]:
    groups: list[tuple[str, str]] = []
    for group in sorted(model.groups.values(), key=lambda item: item.base_name):
        visible_member = choose_visible_member(group)
        if visible_member is None and group.null_mask is None:
            continue
        groups.append((group.base_name, visible_name_for_group(group, visible_member)))
    return groups


def _build_table_export_select_sql(
    *,
    source_schema: str,
    table_name: str,
    table_models: dict[str, TableModel],
    relationships: list[Any],
    udf_schema: str,
    export_names: JsonExportRootNames,
) -> str:
    udf_names = helper_names(udf_schema)
    relationships_by_parent: dict[str, list[Any]] = {}
    relationship_by_parent_segment_kind: dict[tuple[str, str, str], Any] = {}
    for relationship in relationships:
        relationships_by_parent.setdefault(relationship.parent_table, []).append(relationship)
        relationship_by_parent_segment_kind[
            (relationship.parent_table, relationship.segment_name, relationship.relation_kind)
        ] = relationship

    visited: set[str] = set()
    postorder: list[str] = []

    def visit(table_name: str) -> None:
        if table_name in visited:
            return
        visited.add(table_name)
        for relationship in relationships_by_parent.get(table_name, []):
            visit(relationship.child_table)
        postorder.append(table_name)

    visit(table_name)

    row_ctes: dict[str, str] = {}
    array_ctes: dict[str, str] = {}
    fragment_ctes: dict[str, str] = {}
    ctes: list[str] = []

    for table_name in postorder:
        model = table_models[table_name]
        child_relationships = relationships_by_parent.get(table_name, [])
        row_cte = _cte_name("rowjson", table_name)
        row_ctes[table_name] = row_cte
        source_qtable = quote_qualified(source_schema, table_name)
        key_columns = _structural_key_columns(model)
        has_parent = any(column.name == "_parent" for column in model.columns)
        if not key_columns:
            raise AssertionError(f"Table {table_name} has no structural key columns")

        if has_parent and "_value" in model.groups:
            group = model.groups["_value"]
            non_value_groups = [item for item in sorted(model.groups.values(), key=lambda entry: entry.base_name) if item.base_name != "_value"]
            key_select_sql = ",\n      ".join(
                f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
                for column_name in key_columns
            )
            object_relation = relationship_by_parent_segment_kind.get((table_name, "_value", "object"))
            array_relation = relationship_by_parent_segment_kind.get((table_name, "_value", "array"))
            join_lines: list[str] = []
            object_alias: str | None = None
            array_alias: str | None = None
            if object_relation is not None:
                object_alias = "child_value_obj"
                marker_expr = f'base.{quote_identifier(group.object_member.name)}'
                join_lines.append(
                    f"LEFT JOIN {row_ctes[object_relation.child_table]} {object_alias}\n"
                    f'      ON {object_alias}."_id" = {marker_expr}'
                )
            if array_relation is not None:
                if "_id" not in key_columns:
                    raise AssertionError(f"Value array table {table_name} is missing _id for nested array export")
                array_alias = "child_value_arr"
                join_lines.append(
                    f"LEFT JOIN {array_ctes[array_relation.child_table]} {array_alias}\n"
                    f'      ON {array_alias}.parent_id = base."_id"'
                )
            json_expr = _value_group_json_expr(
                udf_names.json_quote_string,
                group,
                "base",
                object_alias=object_alias,
                array_alias=array_alias,
            )
            if not non_value_groups:
                if json_expr is None:
                    raise AssertionError(f"Value array table {table_name} has no serializable value branches")
                joins_sql = ""
                if join_lines:
                    joins_sql = "\n    " + "\n    ".join(join_lines)
                ctes.append(
                    f"""
{row_cte} AS (
    SELECT
      {key_select_sql},
      {json_expr} AS j
    FROM {source_qtable} base{joins_sql}
)""".strip()
                )
            else:
                object_fragment_cte = _cte_name("fragments", table_name)
                object_row_cte = _cte_name("objectjson", table_name)
                fragment_selects: list[str] = []
                fragment_key_sql = ",\n      ".join(
                    f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
                    for column_name in key_columns
                )
                group_ord = 0
                for value_group in non_value_groups:
                    object_child_relation = relationship_by_parent_segment_kind.get(
                        (table_name, value_group.base_name, "object")
                    )
                    array_child_relation = relationship_by_parent_segment_kind.get(
                        (table_name, value_group.base_name, "array")
                    )
                    group_ord += 10
                    if value_group.null_mask is not None:
                        null_mask_expr = f'base.{quote_identifier(value_group.null_mask.name)}'
                        fragment_selects.append(
                            f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(value_group.base_name)} AS frag_key,
      {sql_literal(f'"{value_group.base_name}":null')} AS frag
    FROM {source_qtable} base
    WHERE {null_mask_expr} IS TRUE""".strip()
                        )
                    if object_child_relation is not None:
                        child_cte = row_ctes[object_child_relation.child_table]
                        marker_expr = f'base.{quote_identifier(value_group.base_name + "|object")}'
                        fragment_selects.append(
                            f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(value_group.base_name)} AS frag_key,
      {sql_literal(f'"{value_group.base_name}":')} || child.j AS frag
    FROM {source_qtable} base
    JOIN {child_cte} child
      ON child."_id" = {marker_expr}
    WHERE {marker_expr} IS NOT NULL""".strip()
                        )
                    if array_child_relation is not None:
                        if "_id" not in key_columns:
                            raise AssertionError(
                                f"Array relation parent {table_name} is missing _id for {value_group.base_name}"
                            )
                        child_array_cte = array_ctes[array_child_relation.child_table]
                        marker_expr = f'base.{quote_identifier(value_group.base_name + "|array")}'
                        fragment_selects.append(
                            f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(value_group.base_name)} AS frag_key,
      {sql_literal(f'"{value_group.base_name}":')} || COALESCE(child_arr.j, '[]') AS frag
    FROM {source_qtable} base
    LEFT JOIN {child_array_cte} child_arr
      ON child_arr.parent_id = base."_id"
    WHERE {marker_expr} IS NOT NULL""".strip()
                        )
                    fragment_expr = _scalar_group_fragment_expr(
                        udf_names.json_quote_string,
                        value_group,
                        "base",
                        include_null_mask=False,
                    )
                    if fragment_expr is not None:
                        rendered, predicate = fragment_expr
                        fragment_selects.append(
                            f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(value_group.base_name)} AS frag_key,
      {rendered} AS frag
    FROM {source_qtable} base
    WHERE {predicate}""".strip()
                        )

                if fragment_selects:
                    key_select_columns = [
                        f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
                        for column_name in key_columns
                    ]
                    group_by_columns = [f'base.{quote_identifier(column_name)}' for column_name in key_columns]
                    join_predicates = [
                        f'frag.{quote_identifier(column_name)} = base.{quote_identifier(column_name)}'
                        for column_name in key_columns
                    ]
                    object_select_columns = list(key_select_columns)
                    object_select_columns.append(
                        f"{udf_names.json_object_from_fragments}(frag.ord, frag.frag) AS j"
                    )
                    fragment_union_sql = "\n".join(
                        ["    " + fragment_selects[0]]
                        + ["    UNION ALL\n    " + item for item in fragment_selects[1:]]
                    )
                    ctes.append(
                        f"""
{object_fragment_cte} AS (
{fragment_union_sql}
),
{object_row_cte} AS (
    SELECT
      {", ".join(object_select_columns)}
    FROM {source_qtable} base
    LEFT JOIN {object_fragment_cte} frag
      ON {" AND ".join(join_predicates)}
    GROUP BY {", ".join(group_by_columns)}
)""".strip()
                    )
                else:
                    ctes.append(
                        f"""
{object_row_cte} AS (
    SELECT
      {key_select_sql},
      '{{}}' AS j
    FROM {source_qtable} base
)""".strip()
                    )

                join_predicates = [
                    f'obj_json.{quote_identifier(column_name)} = base.{quote_identifier(column_name)}'
                    for column_name in key_columns
                ]
                final_json_expr = "obj_json.j"
                if json_expr is not None:
                    final_json_expr = (
                        f"CASE WHEN obj_json.j <> '{{}}' THEN obj_json.j "
                        f"ELSE COALESCE({json_expr}, obj_json.j) END"
                    )
                ctes.append(
                    f"""
{row_cte} AS (
    SELECT
      {key_select_sql},
      {final_json_expr} AS j
    FROM {source_qtable} base
    LEFT JOIN {object_row_cte} obj_json
      ON {" AND ".join(join_predicates)}{''.join(f'''
    {line}''' for line in join_lines)}
)""".strip()
                )
        else:
            fragment_cte = _cte_name("fragments", table_name)
            fragment_selects: list[str] = []
            fragment_key_sql = ",\n      ".join(
                f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
                for column_name in key_columns
            )
            group_ord = 0
            for group in sorted(model.groups.values(), key=lambda item: item.base_name):
                object_relation = relationship_by_parent_segment_kind.get((table_name, group.base_name, "object"))
                array_relation = relationship_by_parent_segment_kind.get((table_name, group.base_name, "array"))
                group_ord += 10
                if group.null_mask is not None:
                    null_mask_expr = f'base.{quote_identifier(group.null_mask.name)}'
                    fragment_selects.append(
                        f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(group.base_name)} AS frag_key,
      {sql_literal(f'"{group.base_name}":null')} AS frag
    FROM {source_qtable} base
    WHERE {null_mask_expr} IS TRUE""".strip()
                    )
                if object_relation is not None:
                    child_cte = row_ctes[object_relation.child_table]
                    marker_expr = f'base.{quote_identifier(group.base_name + "|object")}'
                    fragment_selects.append(
                        f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(group.base_name)} AS frag_key,
      {sql_literal(f'"{group.base_name}":')} || child.j AS frag
    FROM {source_qtable} base
    JOIN {child_cte} child
      ON child."_id" = {marker_expr}
    WHERE {marker_expr} IS NOT NULL""".strip()
                    )

                if array_relation is not None:
                    if "_id" not in key_columns:
                        raise AssertionError(
                            f"Array relation parent {table_name} is missing _id for {group.base_name}"
                        )
                    child_array_cte = array_ctes[array_relation.child_table]
                    marker_expr = f'base.{quote_identifier(group.base_name + "|array")}'
                    fragment_selects.append(
                        f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(group.base_name)} AS frag_key,
      {sql_literal(f'"{group.base_name}":')} || COALESCE(child_arr.j, '[]') AS frag
    FROM {source_qtable} base
    LEFT JOIN {child_array_cte} child_arr
      ON child_arr.parent_id = base."_id"
    WHERE {marker_expr} IS NOT NULL""".strip()
                    )

                fragment_expr = _scalar_group_fragment_expr(
                    udf_names.json_quote_string,
                    group,
                    "base",
                    include_null_mask=False,
                )
                if fragment_expr is None:
                    continue
                rendered, predicate = fragment_expr
                fragment_selects.append(
                    f"""
    SELECT
      {fragment_key_sql},
      {group_ord} AS ord,
      {sql_literal(group.base_name)} AS frag_key,
      {rendered} AS frag
    FROM {source_qtable} base
    WHERE {predicate}""".strip()
                )

            if not fragment_selects:
                key_select_sql = ",\n      ".join(
                    f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
                    for column_name in key_columns
                )
                ctes.append(
                    f"""
{row_cte} AS (
    SELECT
      {key_select_sql},
      '{{}}' AS j
    FROM {source_qtable} base
)""".strip()
                )
            else:
                fragment_ctes[table_name] = fragment_cte
                key_select_columns = [
                    f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
                    for column_name in key_columns
                ]
                group_by_columns = [f'base.{quote_identifier(column_name)}' for column_name in key_columns]
                join_predicates = [
                    f'frag.{quote_identifier(column_name)} = base.{quote_identifier(column_name)}'
                    for column_name in key_columns
                ]
                select_columns = list(key_select_columns)
                select_columns.append(
                    f"{udf_names.json_object_from_fragments}(frag.ord, frag.frag) AS j"
                )
                fragment_union_sql = "\n".join(
                    ["    " + fragment_selects[0]]
                    + ["    UNION ALL\n    " + item for item in fragment_selects[1:]]
                )
                ctes.append(
                    f"""
{fragment_cte} AS (
{fragment_union_sql}
),
{row_cte} AS (
    SELECT
      {", ".join(select_columns)}
    FROM {source_qtable} base
    LEFT JOIN {fragment_cte} frag
      ON {" AND ".join(join_predicates)}
    GROUP BY {", ".join(group_by_columns)}
)""".strip()
                )

        if has_parent:
            array_cte = _cte_name("arrayjson", table_name)
            array_ctes[table_name] = array_cte
            ctes.append(
                f"""
{array_cte} AS (
    SELECT
      child."_parent" AS parent_id,
      {udf_names.json_array_from_json_sorted}(child."_pos", child.j) AS j
    FROM {row_cte} child
    GROUP BY child."_parent"
)""".strip()
            )

    target_model = table_models[table_name]
    target_key_columns = _structural_key_columns(target_model)
    target_source_qtable = quote_qualified(source_schema, table_name)
    target_row_cte = row_ctes[table_name]
    target_fragment_cte = fragment_ctes.get(table_name)
    top_fragments_cte = _cte_name("topfrags", table_name)
    export_cte = _cte_name("export", table_name)

    fragment_columns = [
        f'MAX(CASE WHEN frag.frag_key = {sql_literal(fragment.base_name)} THEN frag.frag END) '
        f'AS {quote_identifier(fragment.column_name)}'
        for fragment in export_names.fragments
    ]
    target_fragment_select_lines = [
        f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
        for column_name in target_key_columns
    ]
    target_fragment_select_lines.append(f'{_row_key_expr(target_model, "base")} AS {quote_identifier(ROW_KEY_COLUMN)}')
    target_fragment_select_lines.extend(fragment_columns)
    target_fragment_select_sql = ",\n      ".join(target_fragment_select_lines)

    top_fragments_sql = (
        f"""
{top_fragments_cte} AS (
    SELECT
      {target_fragment_select_sql}
    FROM {target_source_qtable} base
    LEFT JOIN {target_fragment_cte} frag
      ON {" AND ".join(
          f'frag.{quote_identifier(column_name)} = base.{quote_identifier(column_name)}'
          for column_name in target_key_columns
      )}
    GROUP BY {", ".join(f'base.{quote_identifier(column_name)}' for column_name in target_key_columns)}
)""".strip()
        if target_fragment_cte is not None
        else f"""
{top_fragments_cte} AS (
    SELECT
      {", ".join(
          [f'base.{quote_identifier(column_name)} AS {quote_identifier(column_name)}' for column_name in target_key_columns]
          + [f'{_row_key_expr(target_model, "base")} AS {quote_identifier(ROW_KEY_COLUMN)}']
          + [f'CAST(NULL AS VARCHAR(2000000)) AS {quote_identifier(fragment.column_name)}' for fragment in export_names.fragments]
      )}
    FROM {target_source_qtable} base
)""".strip()
    )
    ctes.append(top_fragments_sql)

    export_select_columns = [
        f'target_row.{quote_identifier(column_name)} AS {quote_identifier(column_name)}'
        for column_name in target_key_columns
    ]
    export_select_columns.extend(
        [
            f'frag.{quote_identifier(ROW_KEY_COLUMN)} AS {quote_identifier(ROW_KEY_COLUMN)}',
            f'target_row.j AS {quote_identifier(FULL_JSON_COLUMN)}',
        ]
    )
    export_select_columns.extend(
        f'frag.{quote_identifier(fragment.column_name)} AS {quote_identifier(fragment.column_name)}'
        for fragment in export_names.fragments
    )
    ctes.append(
        f"""
{export_cte} AS (
    SELECT
      {", ".join(export_select_columns)}
    FROM {target_row_cte} target_row
    LEFT JOIN {top_fragments_cte} frag
      ON {" AND ".join(
          f'frag.{quote_identifier(column_name)} = target_row.{quote_identifier(column_name)}'
          for column_name in target_key_columns
      )}
)""".strip()
    )

    final_columns = [quote_identifier(column_name) for column_name in target_key_columns]
    final_columns.extend(
        [
            quote_identifier(ROW_KEY_COLUMN),
            quote_identifier(FULL_JSON_COLUMN),
        ]
    )
    final_columns.extend(quote_identifier(fragment.column_name) for fragment in export_names.fragments)
    final_column_sql = ",\n  ".join(final_columns)
    return "WITH\n" + ",\n".join(ctes) + f"""
SELECT
  {final_column_sql}
FROM {export_cte}
ORDER BY {", ".join(quote_identifier(column_name) for column_name in target_key_columns)}
"""


def generate_json_export_artifacts_from_source_columns(
    source_columns: dict[str, list[ColumnMeta]],
    *,
    source_schema: str,
    schema: str = DEFAULT_SCHEMA,
    udf_schema: str = DEFAULT_UDF_SCHEMA,
) -> JsonExportArtifacts:
    validated_source_schema = validate_identifier("Source schema", source_schema)
    validated_schema = validate_identifier("Export schema", schema)
    validated_udf_schema = validate_identifier("JSON helper schema", udf_schema)

    table_models = build_table_models(source_columns)
    relationships = build_relationships(table_models)
    root_tables = tuple(find_root_tables(table_models, relationships))
    root_names: dict[str, JsonExportRootNames] = {}
    select_sql_by_root: dict[str, str] = {}
    statements: list[str] = [f"CREATE SCHEMA IF NOT EXISTS {validated_schema}"]
    table_export_names = {
        table_name: _build_root_names(table_name, validated_schema, _root_groups(model))
        for table_name, model in sorted(table_models.items())
    }

    for table_name in sorted(table_models):
        statements.append(
            f"CREATE OR REPLACE VIEW {table_export_names[table_name].qualified_view} AS\n"
            + _build_table_export_select_sql(
                source_schema=validated_source_schema,
                table_name=table_name,
                table_models=table_models,
                relationships=relationships,
                udf_schema=validated_udf_schema,
                export_names=table_export_names[table_name],
            )
        )

    for root_table in root_tables:
        root_names[root_table] = table_export_names[root_table]
        select_sql_by_root[root_table] = _build_table_export_select_sql(
            source_schema=validated_source_schema,
            table_name=root_table,
            table_models=table_models,
            relationships=relationships,
            udf_schema=validated_udf_schema,
            export_names=root_names[root_table],
        )

    return JsonExportArtifacts(
        sql=";\n\n".join(statements) + ";\n",
        schema=validated_schema,
        udf_schema=validated_udf_schema,
        root_tables=root_tables,
        root_names=root_names,
        select_sql_by_root=select_sql_by_root,
    )


def generate_json_export_artifacts(
    con,
    *,
    source_schema: str,
    schema: str = DEFAULT_SCHEMA,
    udf_schema: str = DEFAULT_UDF_SCHEMA,
) -> JsonExportArtifacts:
    source_columns = fetch_source_columns(con, validate_identifier("Source schema", source_schema))
    return generate_json_export_artifacts_from_source_columns(
        source_columns,
        source_schema=source_schema,
        schema=schema,
        udf_schema=udf_schema,
    )


def generate_json_export_artifacts_from_source_manifest(
    source_manifest: dict[str, Any],
    *,
    source_schema: str,
    schema: str = DEFAULT_SCHEMA,
    udf_schema: str = DEFAULT_UDF_SCHEMA,
) -> JsonExportArtifacts:
    return generate_json_export_artifacts_from_source_columns(
        source_columns_from_manifest(source_manifest, validate_identifier("Source schema", source_schema)),
        source_schema=source_schema,
        schema=schema,
        udf_schema=udf_schema,
    )


def install_json_export_views(
    con,
    *,
    source_schema: str,
    schema: str = DEFAULT_SCHEMA,
    udf_schema: str = DEFAULT_UDF_SCHEMA,
) -> JsonExportArtifacts:
    artifacts = generate_json_export_artifacts(
        con,
        source_schema=source_schema,
        schema=schema,
        udf_schema=udf_schema,
    )
    statements = [statement.strip() for statement in artifacts.sql.split(";\n") if statement.strip()]
    for statement in statements:
        con.execute(statement)
    return artifacts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate installable Exasol SQL for hidden per-root JSON export views. "
            "These views expose full-document JSON and top-level fragment columns over a "
            "conforming JSON Table source schema."
        )
    )
    parser.add_argument("--dsn", default="127.0.0.1:8563", help="Exasol DSN.")
    parser.add_argument("--user", default="sys", help="Exasol user.")
    parser.add_argument("--password", default="exasol", help="Exasol password.")
    parser.add_argument("--source-schema", default="JVS_SRC", help="Physical source schema.")
    parser.add_argument(
        "--source-manifest",
        type=Path,
        default=None,
        help="Optional source-manifest JSON emitted by the ingest layer. When provided, generation uses it instead of live source-schema introspection.",
    )
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema that will own the hidden export views.")
    parser.add_argument(
        "--udf-schema",
        default=DEFAULT_UDF_SCHEMA,
        help="Schema that owns the generic JSON export helper UDFs.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output SQL file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.source_manifest is not None:
        artifacts = generate_json_export_artifacts_from_source_manifest(
            json.loads(args.source_manifest.read_text()),
            source_schema=args.source_schema,
            schema=args.schema,
            udf_schema=args.udf_schema,
        )
    else:
        from .wrapper_schema_support import connect_for_generation

        con = connect_for_generation(args.dsn, args.user, args.password)
        try:
            artifacts = generate_json_export_artifacts(
                con,
                source_schema=args.source_schema,
                schema=args.schema,
                udf_schema=args.udf_schema,
            )
        finally:
            con.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(artifacts.sql)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
