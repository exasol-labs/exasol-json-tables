#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Iterable

import pyexasol


ROOT = Path(__file__).resolve().parents[2]

STRUCTURAL_COLUMNS = {"_id", "_parent", "_pos"}
NULL_SUFFIX = "|n"
OBJECT_SUFFIX = "|object"
ARRAY_SUFFIX = "|array"
STRING_CAST_SIZE = 2000000
VARCHAR_RE = re.compile(r"^VARCHAR\((\d+)\)$", re.IGNORECASE)
DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$", re.IGNORECASE)


@dataclass(frozen=True)
class ColumnMeta:
    schema: str
    table: str
    name: str
    type_name: str
    ordinal: int
    size: int | None
    precision: int | None
    scale: int | None


@dataclass
class Group:
    base_name: str
    members: list[ColumnMeta] = field(default_factory=list)
    primary: ColumnMeta | None = None
    object_member: ColumnMeta | None = None
    array_member: ColumnMeta | None = None
    alternates: list[ColumnMeta] = field(default_factory=list)
    null_mask: ColumnMeta | None = None


@dataclass(frozen=True)
class Relationship:
    parent_table: str
    child_table: str
    segment_name: str
    relation_kind: str


@dataclass
class TableModel:
    name: str
    columns: list[ColumnMeta]
    groups: dict[str, Group]


@dataclass
class WrapperArtifacts:
    sql: str
    manifest: dict[str, Any]
    public_schema: str
    helper_schema: str
    root_tables: list[str]


def connect_for_generation(
    dsn: str,
    user: str,
    password: str,
    schema: str = "SYS",
):
    return pyexasol.connect(dsn=dsn, user=user, password=password, schema=schema)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_qualified(schema: str, name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(name)}"


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_column_name(column_name: str) -> tuple[str, str, str | None] | None:
    if column_name in STRUCTURAL_COLUMNS:
        return None
    if column_name.endswith(NULL_SUFFIX):
        return (column_name[: -len(NULL_SUFFIX)], "nullMask", None)
    if column_name.endswith(OBJECT_SUFFIX):
        return (column_name[: -len(OBJECT_SUFFIX)], "object", None)
    if column_name.endswith(ARRAY_SUFFIX):
        return (column_name[: -len(ARRAY_SUFFIX)], "array", None)
    if "|" in column_name:
        base_name, suffix = column_name.rsplit("|", 1)
        return (base_name, "alternate", suffix)
    return (column_name, "primary", None)


def encode_path_component(name: str) -> str:
    out: list[str] = []
    for ch in name:
        if ch.isalnum() or ch in {"_", "-"}:
            out.append(ch)
        else:
            out.append(f"%{ord(ch):02X}")
    return "".join(out)


def derive_child_table_name(parent_table_name: str, segment_name: str) -> str:
    return f"{parent_table_name}_{encode_path_component(segment_name)}"


def derive_array_child_table_name(parent_table_name: str, segment_name: str) -> str:
    return f"{parent_table_name}_{encode_path_component(segment_name)}_arr"


def fetch_source_columns(con, source_schema: str) -> dict[str, list[ColumnMeta]]:
    rows = con.execute(
        f"""
        SELECT
          COLUMN_SCHEMA,
          COLUMN_TABLE,
          COLUMN_NAME,
          COLUMN_TYPE,
          COLUMN_ORDINAL_POSITION,
          COLUMN_MAXSIZE,
          COLUMN_NUM_PREC,
          COLUMN_NUM_SCALE
        FROM SYS.EXA_ALL_COLUMNS
        WHERE COLUMN_SCHEMA = '{source_schema.upper()}'
        ORDER BY COLUMN_TABLE, COLUMN_ORDINAL_POSITION
        """
    ).fetchall()
    tables: dict[str, list[ColumnMeta]] = {}
    for row in rows:
        column = ColumnMeta(
            schema=row[0],
            table=row[1],
            name=row[2],
            type_name=row[3],
            ordinal=int(row[4]),
            size=row[5],
            precision=row[6],
            scale=row[7],
        )
        tables.setdefault(column.table, []).append(column)
    return tables


def parse_type_metadata(type_name: str) -> tuple[int | None, int | None, int | None]:
    upper = type_name.upper()
    varchar_match = VARCHAR_RE.match(upper)
    if varchar_match is not None:
        return (int(varchar_match.group(1)), None, None)
    decimal_match = DECIMAL_RE.match(upper)
    if decimal_match is not None:
        return (None, int(decimal_match.group(1)), int(decimal_match.group(2)))
    return (None, None, None)


def source_columns_from_manifest(source_manifest: dict[str, Any], source_schema: str) -> dict[str, list[ColumnMeta]]:
    if source_manifest.get("format") != "exasol-json-tables-source-manifest":
        raise SystemExit(
            "Source manifest must have format='exasol-json-tables-source-manifest', "
            f"got {source_manifest.get('format')!r}."
        )
    tables: dict[str, list[ColumnMeta]] = {}
    for table_spec in source_manifest.get("tables", []):
        table_name = str(table_spec["tableName"])
        columns: list[ColumnMeta] = []
        for column_spec in table_spec.get("columns", []):
            type_name = str(column_spec["typeName"])
            size, precision, scale = parse_type_metadata(type_name)
            columns.append(
                ColumnMeta(
                    schema=source_schema,
                    table=table_name,
                    name=str(column_spec["name"]),
                    type_name=type_name,
                    ordinal=int(column_spec["ordinal"]),
                    size=size,
                    precision=precision,
                    scale=scale,
                )
            )
        columns.sort(key=lambda column: column.ordinal)
        tables[table_name] = columns
    if not tables:
        raise SystemExit("Source manifest does not describe any tables.")
    return tables


def group_columns(columns: Iterable[ColumnMeta]) -> dict[str, Group]:
    groups: dict[str, Group] = {}
    for column in columns:
        parsed = parse_column_name(column.name)
        if parsed is None:
            continue
        base_name, kind, _ = parsed
        group = groups.setdefault(base_name, Group(base_name=base_name))
        group.members.append(column)
        if kind == "primary":
            group.primary = column
        elif kind == "object":
            group.object_member = column
        elif kind == "array":
            group.array_member = column
        elif kind == "alternate":
            group.alternates.append(column)
        elif kind == "nullMask":
            group.null_mask = column
    return groups


def choose_visible_member(group: Group) -> ColumnMeta | None:
    if group.primary is not None:
        return group.primary
    if group.object_member is not None:
        return group.object_member
    if group.array_member is not None:
        return group.array_member
    if group.alternates:
        return group.alternates[0]
    return None


def count_non_null_members(group: Group) -> int:
    count = 0
    if group.primary is not None:
        count += 1
    if group.object_member is not None:
        count += 1
    if group.array_member is not None:
        count += 1
    count += len(group.alternates)
    return count


def visible_name_for_group(group: Group, visible_member: ColumnMeta) -> str:
    if count_non_null_members(group) > 1:
        return group.base_name
    return visible_member.name


def ordered_non_null_members(group: Group) -> list[ColumnMeta]:
    members: list[ColumnMeta] = []
    if group.primary is not None:
        members.append(group.primary)
    members.extend(sorted(group.alternates, key=lambda column: column.ordinal))
    if group.object_member is not None:
        members.append(group.object_member)
    if group.array_member is not None:
        members.append(group.array_member)
    return members


def projection_type_signature(column: ColumnMeta) -> tuple[str, int | None, int | None, int | None]:
    return (column.type_name.upper(), column.size, column.precision, column.scale)


def can_use_native_coalesce(columns: list[ColumnMeta]) -> bool:
    signatures = {projection_type_signature(column) for column in columns}
    return len(signatures) == 1


def render_projection_expression(group: Group) -> str:
    visible_member = choose_visible_member(group)
    if visible_member is None:
        raise ValueError(f"No visible member for group {group.base_name}")
    members = ordered_non_null_members(group)
    if len(members) == 1:
        return quote_identifier(members[0].name)
    if can_use_native_coalesce(members):
        return "COALESCE(" + ", ".join(quote_identifier(member.name) for member in members) + ")"
    return "COALESCE(" + ", ".join(
        f"CAST({quote_identifier(member.name)} AS VARCHAR({STRING_CAST_SIZE}))" for member in members
    ) + ")"


def build_table_models(source_columns: dict[str, list[ColumnMeta]]) -> dict[str, TableModel]:
    return {
        table_name: TableModel(
            name=table_name,
            columns=columns,
            groups=group_columns(columns),
        )
        for table_name, columns in source_columns.items()
    }


def build_relationships(table_models: dict[str, TableModel]) -> list[Relationship]:
    table_names = set(table_models)
    relationships: list[Relationship] = []
    seen: set[tuple[str, str, str, str]] = set()
    for parent_table, model in table_models.items():
        for column in model.columns:
            parsed = parse_column_name(column.name)
            if parsed is None:
                continue
            base_name, kind, _ = parsed
            if kind == "object":
                child_table = derive_child_table_name(parent_table, base_name)
                if child_table in table_names:
                    key = (parent_table, child_table, base_name, "object")
                    if key not in seen:
                        relationships.append(
                            Relationship(
                                parent_table=parent_table,
                                child_table=child_table,
                                segment_name=base_name,
                                relation_kind="object",
                            )
                        )
                        seen.add(key)
            elif kind == "array":
                child_table = derive_array_child_table_name(parent_table, base_name)
                if child_table in table_names:
                    key = (parent_table, child_table, base_name, "array")
                    if key not in seen:
                        relationships.append(
                            Relationship(
                                parent_table=parent_table,
                                child_table=child_table,
                                segment_name=base_name,
                                relation_kind="array",
                            )
                        )
                        seen.add(key)
    relationships.sort(key=lambda item: (item.parent_table, item.child_table, item.segment_name, item.relation_kind))
    return relationships


def find_root_tables(table_models: dict[str, TableModel], relationships: list[Relationship]) -> list[str]:
    incoming: dict[str, int] = {table_name: 0 for table_name in table_models}
    for relationship in relationships:
        incoming[relationship.child_table] = incoming.get(relationship.child_table, 0) + 1
    return sorted(table_name for table_name, count in incoming.items() if count == 0)


def build_root_families(root_tables: list[str], relationships: list[Relationship]) -> dict[str, str]:
    children_by_parent: dict[str, list[str]] = {}
    for relationship in relationships:
        children_by_parent.setdefault(relationship.parent_table, []).append(relationship.child_table)
    root_by_table: dict[str, str] = {}
    for root_table in root_tables:
        stack = [root_table]
        while stack:
            current = stack.pop()
            existing_root = root_by_table.get(current)
            if existing_root is not None and existing_root != root_table:
                raise ValueError(f"Table {current} belongs to multiple root families: {existing_root}, {root_table}")
            if existing_root == root_table:
                continue
            root_by_table[current] = root_table
            stack.extend(children_by_parent.get(current, []))
    return root_by_table


def generate_public_view_sql(public_schema: str, table_model: TableModel, source_schema: str) -> str:
    select_lines: list[str] = []
    emitted_groups: set[str] = set()
    for column in table_model.columns:
        if column.name in STRUCTURAL_COLUMNS:
            select_lines.append(f"  {quote_identifier(column.name)}")
            continue
        parsed = parse_column_name(column.name)
        if parsed is None:
            continue
        base_name, kind, _ = parsed
        if kind == "nullMask" or base_name in emitted_groups:
            continue
        group = table_model.groups[base_name]
        visible_member = choose_visible_member(group)
        if visible_member is None or column.name != visible_member.name:
            continue
        visible_name = visible_name_for_group(group, visible_member)
        expression = render_projection_expression(group)
        if expression == quote_identifier(visible_name):
            select_lines.append(f"  {expression}")
        else:
            select_lines.append(f"  {expression} AS {quote_identifier(visible_name)}")
        emitted_groups.add(base_name)

    select_sql = ",\n".join(select_lines)
    return (
        f"CREATE OR REPLACE VIEW {quote_qualified(public_schema, table_model.name)} AS\n"
        f"SELECT\n{select_sql}\n"
        f"FROM {quote_qualified(source_schema, table_model.name)}"
    )


def generate_helper_view_sql(helper_schema: str, source_schema: str, table_name: str) -> str:
    return (
        f"CREATE OR REPLACE VIEW {quote_qualified(helper_schema, table_name)} AS\n"
        f"SELECT *\n"
        f"FROM {quote_qualified(source_schema, table_name)}"
    )


def build_manifest(
    source_schema: str,
    public_schema: str,
    helper_schema: str,
    table_models: dict[str, TableModel],
    root_tables: list[str],
    relationships: list[Relationship],
    root_by_table: dict[str, str],
) -> dict[str, Any]:
    relationships_by_root: dict[str, list[dict[str, str]]] = {root_table: [] for root_table in root_tables}
    for relationship in relationships:
        root_table = root_by_table[relationship.parent_table]
        relationships_by_root[root_table].append(
            {
                "parentTable": relationship.parent_table,
                "childTable": relationship.child_table,
                "segmentName": relationship.segment_name,
                "relationKind": relationship.relation_kind,
            }
        )

    tables_manifest: list[dict[str, Any]] = []
    for table_name in sorted(table_models):
        model = table_models[table_name]
        groups_manifest: list[dict[str, Any]] = []
        for group in sorted(model.groups.values(), key=lambda item: item.base_name):
            visible_member = choose_visible_member(group)
            visible_name = visible_name_for_group(group, visible_member) if visible_member is not None else None
            members = ordered_non_null_members(group)
            groups_manifest.append(
                {
                    "baseName": group.base_name,
                    "visibleName": visible_name,
                    "nullMaskName": group.null_mask.name if group.null_mask is not None else None,
                    "members": [
                        {
                            "name": member.name,
                            "type": member.type_name,
                            "ordinal": member.ordinal,
                            "isPrimary": visible_member is not None and member.name == visible_member.name,
                        }
                        for member in members
                    ],
                }
            )
        tables_manifest.append(
            {
                "tableName": table_name,
                "rootTable": root_by_table[table_name],
                "isPublicRoot": table_name in root_tables,
                "groups": groups_manifest,
            }
        )

    roots_manifest = [
        {
            "tableName": root_table,
            "publicView": root_table,
            "familyTables": sorted(table_name for table_name, family_root in root_by_table.items() if family_root == root_table),
            "relationships": relationships_by_root[root_table],
        }
        for root_table in root_tables
    ]

    return {
        "sourceSchema": source_schema,
        "publicSchema": public_schema,
        "helperSchema": helper_schema,
        "roots": roots_manifest,
        "tables": tables_manifest,
    }


def render_insert_statements(schema: str, table_name: str, columns: list[str], rows: list[list[Any]]) -> list[str]:
    statements: list[str] = []
    qualified_table = quote_qualified(schema, table_name)
    column_sql = ", ".join(quote_identifier(column) for column in columns)
    for row in rows:
        values_sql_parts: list[str] = []
        for value in row:
            if value is None:
                values_sql_parts.append("NULL")
            elif isinstance(value, bool):
                values_sql_parts.append("TRUE" if value else "FALSE")
            elif isinstance(value, (int, float)):
                values_sql_parts.append(str(value))
            else:
                values_sql_parts.append(sql_literal(str(value)))
        statements.append(f"INSERT INTO {qualified_table} ({column_sql}) VALUES ({', '.join(values_sql_parts)})")
    return statements


def generate_metadata_sql(
    helper_schema: str,
    source_schema: str,
    public_schema: str,
    table_models: dict[str, TableModel],
    root_tables: list[str],
    relationships: list[Relationship],
    root_by_table: dict[str, str],
) -> list[str]:
    statements = [
        f"""CREATE OR REPLACE TABLE {quote_qualified(helper_schema, "__JVS_ROOTS")} (
  "ROOT_TABLE" VARCHAR(200) NOT NULL,
  "SOURCE_SCHEMA" VARCHAR(200) NOT NULL,
  "PUBLIC_SCHEMA" VARCHAR(200) NOT NULL,
  "PUBLIC_VIEW" VARCHAR(200) NOT NULL,
  "HELPER_SCHEMA" VARCHAR(200) NOT NULL
)""",
        f"""CREATE OR REPLACE TABLE {quote_qualified(helper_schema, "__JVS_RELATIONSHIPS")} (
  "ROOT_TABLE" VARCHAR(200) NOT NULL,
  "PARENT_TABLE" VARCHAR(200) NOT NULL,
  "CHILD_TABLE" VARCHAR(200) NOT NULL,
  "SEGMENT_NAME" VARCHAR(200) NOT NULL,
  "RELATION_KIND" VARCHAR(20) NOT NULL
)""",
        f"""CREATE OR REPLACE TABLE {quote_qualified(helper_schema, "__JVS_COLUMN_MEMBERS")} (
  "ROOT_TABLE" VARCHAR(200) NOT NULL,
  "SOURCE_TABLE" VARCHAR(200) NOT NULL,
  "BASE_NAME" VARCHAR(500) NOT NULL,
  "VISIBLE_NAME" VARCHAR(500),
  "MEMBER_NAME" VARCHAR(500) NOT NULL,
  "MEMBER_KIND" VARCHAR(20) NOT NULL,
  "MEMBER_TYPE" VARCHAR(200) NOT NULL,
  "IS_VISIBLE" BOOLEAN NOT NULL,
  "IS_PRIMARY" BOOLEAN NOT NULL,
  "NULL_MASK_NAME" VARCHAR(500)
)""",
    ]

    root_rows = [
        [root_table, source_schema, public_schema, root_table, helper_schema]
        for root_table in root_tables
    ]
    statements.extend(
        render_insert_statements(
            helper_schema,
            "__JVS_ROOTS",
            ["ROOT_TABLE", "SOURCE_SCHEMA", "PUBLIC_SCHEMA", "PUBLIC_VIEW", "HELPER_SCHEMA"],
            root_rows,
        )
    )

    relationship_rows = [
        [
            root_by_table[relationship.parent_table],
            relationship.parent_table,
            relationship.child_table,
            relationship.segment_name,
            relationship.relation_kind,
        ]
        for relationship in relationships
    ]
    statements.extend(
        render_insert_statements(
            helper_schema,
            "__JVS_RELATIONSHIPS",
            ["ROOT_TABLE", "PARENT_TABLE", "CHILD_TABLE", "SEGMENT_NAME", "RELATION_KIND"],
            relationship_rows,
        )
    )

    column_rows: list[list[Any]] = []
    for table_name in sorted(table_models):
        model = table_models[table_name]
        root_table = root_by_table[table_name]
        for group in sorted(model.groups.values(), key=lambda item: item.base_name):
            visible_member = choose_visible_member(group)
            visible_name = visible_name_for_group(group, visible_member) if visible_member is not None else None
            null_mask_name = group.null_mask.name if group.null_mask is not None else None
            if group.primary is not None:
                column_rows.append([
                    root_table,
                    table_name,
                    group.base_name,
                    visible_name,
                    group.primary.name,
                    "primary",
                    group.primary.type_name,
                    visible_member is not None and group.primary.name == visible_member.name,
                    True,
                    null_mask_name,
                ])
            for alternate in sorted(group.alternates, key=lambda column: column.ordinal):
                column_rows.append([
                    root_table,
                    table_name,
                    group.base_name,
                    visible_name,
                    alternate.name,
                    "alternate",
                    alternate.type_name,
                    visible_member is not None and alternate.name == visible_member.name,
                    False,
                    null_mask_name,
                ])
            if group.object_member is not None:
                column_rows.append([
                    root_table,
                    table_name,
                    group.base_name,
                    visible_name,
                    group.object_member.name,
                    "object",
                    group.object_member.type_name,
                    visible_member is not None and group.object_member.name == visible_member.name,
                    False,
                    null_mask_name,
                ])
            if group.array_member is not None:
                column_rows.append([
                    root_table,
                    table_name,
                    group.base_name,
                    visible_name,
                    group.array_member.name,
                    "array",
                    group.array_member.type_name,
                    visible_member is not None and group.array_member.name == visible_member.name,
                    False,
                    null_mask_name,
                ])
        statements.extend(
            render_insert_statements(
                helper_schema,
                "__JVS_COLUMN_MEMBERS",
                [
                    "ROOT_TABLE",
                    "SOURCE_TABLE",
                    "BASE_NAME",
                    "VISIBLE_NAME",
                    "MEMBER_NAME",
                    "MEMBER_KIND",
                    "MEMBER_TYPE",
                    "IS_VISIBLE",
                    "IS_PRIMARY",
                    "NULL_MASK_NAME",
                ],
                column_rows,
            )
        )
        column_rows = []
    return statements


def generate_wrapper_artifacts(
    con,
    source_schema: str,
    public_schema: str,
    helper_schema: str,
) -> WrapperArtifacts:
    source_schema = source_schema.upper()
    public_schema = public_schema.upper()
    helper_schema = helper_schema.upper()
    source_columns = fetch_source_columns(con, source_schema)
    return generate_wrapper_artifacts_from_source_columns(
        source_columns,
        source_schema=source_schema,
        public_schema=public_schema,
        helper_schema=helper_schema,
    )


def generate_wrapper_artifacts_from_source_columns(
    source_columns: dict[str, list[ColumnMeta]],
    *,
    source_schema: str,
    public_schema: str,
    helper_schema: str,
) -> WrapperArtifacts:
    table_models = build_table_models(source_columns)
    relationships = build_relationships(table_models)
    root_tables = find_root_tables(table_models, relationships)
    root_by_table = build_root_families(root_tables, relationships)

    statements = [
        f"DROP SCHEMA IF EXISTS {quote_identifier(public_schema)} CASCADE",
        f"DROP SCHEMA IF EXISTS {quote_identifier(helper_schema)} CASCADE",
        f"CREATE SCHEMA {quote_identifier(public_schema)}",
        f"CREATE SCHEMA {quote_identifier(helper_schema)}",
    ]
    for root_table in root_tables:
        statements.append(generate_public_view_sql(public_schema, table_models[root_table], source_schema))
    for table_name in sorted(table_models):
        statements.append(generate_helper_view_sql(helper_schema, source_schema, table_name))
    statements.extend(
        generate_metadata_sql(
            helper_schema,
            source_schema,
            public_schema,
            table_models,
            root_tables,
            relationships,
            root_by_table,
        )
    )

    manifest = build_manifest(
        source_schema,
        public_schema,
        helper_schema,
        table_models,
        root_tables,
        relationships,
        root_by_table,
    )
    return WrapperArtifacts(
        sql=";\n\n".join(statements) + ";\n",
        manifest=manifest,
        public_schema=public_schema,
        helper_schema=helper_schema,
        root_tables=root_tables,
    )


def generate_wrapper_artifacts_from_source_manifest(
    source_manifest: dict[str, Any],
    *,
    source_schema: str,
    public_schema: str,
    helper_schema: str,
) -> WrapperArtifacts:
    return generate_wrapper_artifacts_from_source_columns(
        source_columns_from_manifest(source_manifest, source_schema.upper()),
        source_schema=source_schema.upper(),
        public_schema=public_schema.upper(),
        helper_schema=helper_schema.upper(),
    )
