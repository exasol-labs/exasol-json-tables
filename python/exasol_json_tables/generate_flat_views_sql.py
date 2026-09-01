#!/usr/bin/env python3

"""Preprocessor-free flattened views over an ingested table family.

The wrapper surface in :mod:`wrapper_schema_support` is deliberately
JSON-shaped: it keeps the original property spelling, exposes structural
columns such as ``dimensions|object``, and needs an active
``SQL_PREPROCESSOR_SCRIPT`` before path, bracket, iterator, or helper syntax
resolves.

Consumers that cannot set session state -- BI tools, dashboard servers,
pooled connections, generated SQL -- cannot use that surface at all, and fall
back to hand-quoting the raw source columns.

This module generates a second, additive surface for exactly those consumers:
one ordinary view per entity, with UPPERCASE column names that are safe to type
unquoted, nested objects folded into the owning entity, and arrays kept as
separate views joined on plain key columns.
"""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from .wrapper_schema_support import (
    STRING_CAST_SIZE,
    ColumnMeta,
    Group,
    Relationship,
    TableModel,
    can_use_native_coalesce,
    quote_identifier,
    quote_qualified,
    sql_literal,
)


# Exasol identifiers are limited to 128 characters.
MAX_IDENTIFIER_LENGTH = 128

ROW_ID_COLUMN = "ROW_ID"
PARENT_ID_COLUMN = "PARENT_ID"
ARRAY_INDEX_COLUMN = "ARRAY_INDEX"
ELEMENT_VALUE_SEGMENT = "ELEMENT_VALUE"
ARRAY_LENGTH_SUFFIX = "LENGTH"
OBJECT_ID_SUFFIX = "ID"
RESERVED_WORD_SUFFIX = "_COL"
FALLBACK_IDENTIFIER = "FIELD"
NUMERIC_PREFIX = "C_"

ARRAY_ELEMENT_COLUMN = "_value"

_NON_IDENTIFIER_RE = re.compile(r"[^A-Z0-9_]")
_UNDERSCORE_RUN_RE = re.compile(r"_+")

# Reserved words rejected by Exasol in unquoted position, as reported by
# `SELECT KEYWORD FROM SYS.EXA_SQL_KEYWORDS WHERE RESERVED = TRUE`. A word that
# is on this list but no longer reserved only costs a `_COL` suffix, so erring
# on the side of a longer list is safe.
RESERVED_WORDS = frozenset(
    {
        "ABSOLUTE", "ACTION", "ADD", "AFTER", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "APPEND",
        "ARE", "ARRAY", "AS", "ASC", "ASENSITIVE", "ASSERTION", "AT", "ATTRIBUTE", "AUTHID",
        "AUTHORIZATION", "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BIT", "BLOB", "BLOCKED",
        "BOOL", "BOOLEAN", "BOTH", "BY", "BYTE", "CALL", "CALLED", "CARDINALITY", "CASCADE",
        "CASCADED", "CASE", "CASESPECIFIC", "CAST", "CATALOG", "CHAIN", "CHAR", "CHARACTER",
        "CHARACTERISTICS", "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA",
        "CHECK", "CHECKED", "CLOB", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLLATION_CATALOG",
        "COLLATION_NAME", "COLLATION_SCHEMA", "COLUMN", "COMMIT", "CONDITION", "CONNECTION",
        "CONNECT_BY_ISCYCLE", "CONNECT_BY_ISLEAF", "CONNECT_BY_ROOT", "CONSTANT", "CONSTRAINT",
        "CONSTRAINTS", "CONSTRAINT_STATE_DEFAULT", "CONSTRUCTOR", "CONTAINS", "CONTINUE", "CONTROL",
        "CONVERT", "CORRESPONDING", "CREATE", "CS", "CSV", "CUBE", "CURRENT", "CURRENT_CLUSTER",
        "CURRENT_CLUSTER_UID", "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA",
        "CURRENT_SESSION", "CURRENT_STATEMENT", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
        "CURSOR", "CYCLE", "DATA", "DATALINK", "DATE", "DATETIME_INTERVAL_CODE",
        "DATETIME_INTERVAL_PRECISION", "DAY", "DBTIMEZONE", "DEALLOCATE", "DEC", "DECIMAL",
        "DECLARE", "DEFAULT", "DEFAULT_LIKE_ESCAPE_CHARACTER", "DEFERRABLE", "DEFERRED", "DEFINED",
        "DEFINER", "DELETE", "DEREF", "DERIVED", "DESC", "DESCRIBE", "DESCRIPTOR", "DETERMINISTIC",
        "DISABLE", "DISABLED", "DISCONNECT", "DISPATCH", "DISTINCT", "DLURLCOMPLETE", "DLURLPATH",
        "DLURLPATHONLY", "DLURLSCHEME", "DLURLSERVER", "DLVALUE", "DO", "DOMAIN", "DOUBLE", "DROP",
        "DYNAMIC", "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH", "ELSE", "ELSEIF", "ELSIF",
        "EMITS", "ENABLE", "ENABLED", "END", "END-EXEC", "ENDIF", "ENFORCE", "EQUALS", "ERRORS",
        "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXPORT", "EXTERNAL",
        "EXTRACT", "FALSE", "FBV", "FETCH", "FILE", "FINAL", "FIRST", "FLOAT", "FOLLOWING", "FOR",
        "FORALL", "FORCE", "FORMAT", "FOUND", "FREE", "FROM", "FS", "FULL", "FUNCTION", "GENERAL",
        "GENERATED", "GEOMETRY", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GRANTED", "GROUP",
        "GROUPING", "GROUPS", "GROUP_CONCAT", "HASHTYPE", "HASHTYPE_FORMAT", "HAVING", "HIGH",
        "HOLD", "HOUR", "IDENTITY", "IF", "IFNULL", "IMMEDIATE", "IMPERSONATE", "IMPLEMENTATION",
        "IMPORT", "IN", "INDEX", "INDICATOR", "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT",
        "INSTANCE", "INSTANTIABLE", "INT", "INTEGER", "INTEGRITY", "INTERSECT", "INTERVAL", "INTO",
        "INVERSE", "INVOKER", "IS", "ITERATE", "JOIN", "KEY_MEMBER", "KEY_TYPE", "LARGE", "LAST",
        "LATERAL", "LDAP", "LEADING", "LEAVE", "LEFT", "LEVEL", "LIKE", "LIMIT", "LISTAGG", "LOCAL",
        "LOCALTIME", "LOCALTIMESTAMP", "LOCATOR", "LOG", "LONGVARCHAR", "LOOP", "LOW", "MAP",
        "MATCH", "MATCHED", "MERGE", "METHOD", "MINUS", "MINUTE", "MOD", "MODIFIES", "MODIFY",
        "MODULE", "MONTH", "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NEXT",
        "NLS_DATE_FORMAT", "NLS_DATE_LANGUAGE", "NLS_FIRST_DAY_OF_WEEK", "NLS_NUMERIC_CHARACTERS",
        "NLS_TIMESTAMP_FORMAT", "NO", "NOCYCLE", "NOLOGGING", "NONE", "NOT", "NULL", "NULLIF",
        "NUMBER", "NUMERIC", "NVARCHAR", "NVARCHAR2", "OBJECT", "OF", "OFF", "OLD", "ON", "ONLY",
        "OPEN", "OPTION", "OPTIONS", "OR", "ORDER", "ORDERING", "ORDINALITY", "OTHERS", "OUT",
        "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING", "PAD", "PARALLEL_ENABLE",
        "PARAMETER", "PARAMETER_SPECIFIC_CATALOG", "PARAMETER_SPECIFIC_NAME",
        "PARAMETER_SPECIFIC_SCHEMA", "PARQUET", "PARTIAL", "PATH", "PERMISSION", "PLACING", "PLUS",
        "POSITION", "PRECEDING", "PREFERRING", "PREPARE", "PRESERVE", "PRIOR", "PRIVILEGES",
        "PROCEDURE", "PROFILE", "QUALIFY", "RANDOM", "RANGE", "READ", "READS", "REAL", "RECOVERY",
        "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REFRESH", "REGEXP_LIKE", "RELATIVE",
        "RELEASE", "RENAME", "REPEAT", "REPLACE", "RESTORE", "RESTRICT", "RESULT", "RETURN",
        "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK",
        "ROLLUP", "ROUTINE", "ROW", "ROWS", "ROWTYPE", "SAVEPOINT", "SCHEMA", "SCOPE", "SCOPE_USER",
        "SCRIPT", "SCROLL", "SEARCH", "SECOND", "SECTION", "SECURITY", "SELECT", "SELECTIVE", "SELF",
        "SENSITIVE", "SEPARATOR", "SEQUENCE", "SESSION", "SESSIONTIMEZONE", "SESSION_USER", "SET",
        "SETS", "SHORTINT", "SIMILAR", "SMALLINT", "SOME", "SOURCE", "SPACE", "SPECIFIC",
        "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIGINT", "SQL_BIT",
        "SQL_CHAR", "SQL_DATE", "SQL_DECIMAL", "SQL_DOUBLE", "SQL_FLOAT", "SQL_INTEGER",
        "SQL_LONGVARCHAR", "SQL_NUMERIC", "SQL_PREPROCESSOR_SCRIPT", "SQL_REAL", "SQL_SMALLINT",
        "SQL_TIMESTAMP", "SQL_TINYINT", "SQL_TYPE_DATE", "SQL_TYPE_TIMESTAMP", "SQL_VARCHAR",
        "START", "STATE", "STATEMENT", "STATIC", "STRUCTURE", "STYLE", "SUBSTRING", "SUBTYPE",
        "SYSDATE", "SYSTEM", "SYSTEM_USER", "SYSTIMESTAMP", "TABLE", "TEMPORARY", "TEXT", "THEN",
        "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYINT", "TO", "TRAILING",
        "TRANSACTION", "TRANSFORM", "TRANSFORMS", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE",
        "TRUNCATE", "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNLINK", "UNNEST", "UNTIL", "UPDATE",
        "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARRAY", "VERIFY",
        "VIEW", "WHEN", "WHENEVER", "WHERE", "WHILE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK",
        "YEAR", "YES", "ZONE",
    }
)


def flatten_identifier(parts: Sequence[str]) -> str:
    """Turn nested JSON path parts into one legal, unquoted-safe identifier.

    The rules, in order:

    1. join the parts with ``_``
    2. uppercase, and replace every character that is not ``A-Z``, ``0-9`` or
       ``_`` with ``_``
    3. collapse runs of ``_`` and trim leading/trailing ``_`` (so a Mongo-style
       ``_id`` property becomes ``ID``)
    4. fall back to ``FIELD`` if nothing is left
    5. prefix ``C_`` if the result starts with a digit
    6. append ``_COL`` if the result is an Exasol reserved word
    7. truncate to 128 characters
    """
    joined = "_".join(part for part in parts if part)
    folded = _NON_IDENTIFIER_RE.sub("_", joined.upper())
    collapsed = _UNDERSCORE_RUN_RE.sub("_", folded).strip("_")
    if not collapsed:
        collapsed = FALLBACK_IDENTIFIER
    if collapsed[0].isdigit():
        collapsed = NUMERIC_PREFIX + collapsed
    if collapsed in RESERVED_WORDS:
        collapsed = collapsed + RESERVED_WORD_SUFFIX
    return collapsed[:MAX_IDENTIFIER_LENGTH]


class IdentifierAllocator:
    """Hands out identifiers that are unique within one namespace.

    Flattening is lossy: ``sub-category`` and ``sub_category`` both normalize to
    ``SUB_CATEGORY``, and truncation at 128 characters can collide too. The
    first claimant keeps the plain name and later ones get ``_2``, ``_3``, ...
    appended, so the mapping stays stable for a given input order.
    """

    def __init__(self) -> None:
        self._used: set[str] = set()

    def allocate(self, parts: Sequence[str]) -> str:
        base = flatten_identifier(parts)
        if base not in self._used:
            self._used.add(base)
            return base
        index = 2
        while True:
            suffix = f"_{index}"
            candidate = base[: MAX_IDENTIFIER_LENGTH - len(suffix)] + suffix
            if candidate not in self._used:
                self._used.add(candidate)
                return candidate
            index += 1


@dataclass(frozen=True)
class FlatColumn:
    name: str
    kind: str
    expression: str
    json_path: str | None
    source_table: str
    source_columns: list[str]
    comment: str


@dataclass(frozen=True)
class FlatParentLink:
    view: str
    parent_column: str
    child_column: str
    order_column: str | None
    json_path: str


@dataclass
class FlatEntity:
    view_name: str
    source_table: str
    root_table: str
    kind: str
    json_path: str
    alias: str
    columns: list[FlatColumn] = field(default_factory=list)
    joins: list[str] = field(default_factory=list)
    parent: FlatParentLink | None = None
    comment: str = ""


@dataclass
class FlatSurface:
    schema: str
    source_schema: str
    entities: list[FlatEntity]
    sql: str
    manifest: dict[str, Any]


def default_flat_schema(wrapper_schema: str) -> str:
    """Flat surface schema name derived from the wrapper schema."""
    stem = wrapper_schema.upper()
    if stem.endswith("_VIEW"):
        stem = stem[: -len("_VIEW")]
    return f"{stem}_FLAT"


def _segment_label(segment_name: str) -> str:
    if segment_name == ARRAY_ELEMENT_COLUMN:
        return ELEMENT_VALUE_SEGMENT
    return segment_name


def _json_path(prefix: str, segment_name: str) -> str:
    if segment_name == ARRAY_ELEMENT_COLUMN:
        # The array element itself: its path is the array path already carried
        # by the enclosing entity.
        return prefix or "*"
    return f"{prefix}.{segment_name}" if prefix else segment_name


def _scalar_members(group: Group) -> list[ColumnMeta]:
    members: list[ColumnMeta] = []
    if group.primary is not None:
        members.append(group.primary)
    members.extend(sorted(group.alternates, key=lambda column: column.ordinal))
    return members


def _render_scalar_expression(alias: str, group: Group) -> tuple[str, list[str]] | None:
    """SQL for the scalar face of a group, or ``None`` when it has none.

    Variant properties keep one column per JSON scalar type in the source
    family. They are coalesced back into a single column here, exactly as the
    wrapper root view does, so the flat surface stays one column per property.
    """
    members = _scalar_members(group)
    if not members:
        has_non_scalar = group.object_member is not None or group.array_member is not None
        if not has_non_scalar and (group.null_mask is not None or group.empty_mask is not None):
            return (f"CAST(NULL AS VARCHAR({STRING_CAST_SIZE}))", [])
        return None
    names = [member.name for member in members]
    if len(members) == 1:
        return (f"{alias}.{quote_identifier(members[0].name)}", names)
    if can_use_native_coalesce(members):
        rendered = ", ".join(f"{alias}.{quote_identifier(member.name)}" for member in members)
        return (f"COALESCE({rendered})", names)
    rendered = ", ".join(
        f"CAST({alias}.{quote_identifier(member.name)} AS VARCHAR({STRING_CAST_SIZE}))"
        for member in members
    )
    return (f"COALESCE({rendered})", names)


@dataclass(frozen=True)
class _PendingEntity:
    source_table: str
    name_path: tuple[str, ...]
    parent_view: str
    parent_column: str | None
    json_path: str


class _FlatSurfaceBuilder:
    def __init__(
        self,
        *,
        source_schema: str,
        flat_schema: str,
        table_models: dict[str, TableModel],
        relationships: Iterable[Relationship],
        root_tables: Sequence[str],
        root_by_table: dict[str, str],
    ) -> None:
        self.source_schema = source_schema
        self.flat_schema = flat_schema
        self.table_models = table_models
        self.root_tables = list(root_tables)
        self.root_by_table = root_by_table
        self.relationships_by_parent: dict[str, list[Relationship]] = {}
        for relationship in relationships:
            self.relationships_by_parent.setdefault(relationship.parent_table, []).append(relationship)
        self.view_allocator = IdentifierAllocator()

    def build(self) -> list[FlatEntity]:
        entities: list[FlatEntity] = []
        queue: deque[_PendingEntity] = deque(
            _PendingEntity(
                source_table=root_table,
                name_path=(),
                parent_view="",
                parent_column=None,
                json_path="",
            )
            for root_table in self.root_tables
        )
        while queue:
            pending = queue.popleft()
            entity, children = self._build_entity(pending)
            entities.append(entity)
            queue.extend(children)
        return entities

    def _build_entity(self, pending: _PendingEntity) -> tuple[FlatEntity, list[_PendingEntity]]:
        table = pending.source_table
        root_table = self.root_by_table.get(table, table)
        view_name = self.view_allocator.allocate([root_table] + list(pending.name_path))
        entity = FlatEntity(
            view_name=view_name,
            source_table=table,
            root_table=root_table,
            kind="array" if pending.name_path else "root",
            json_path=pending.json_path,
            alias="t0",
        )

        allocator = IdentifierAllocator()
        model = self.table_models[table]
        physical_names = {column.name for column in model.columns}

        row_id_column: str | None = None
        if "_id" in physical_names:
            row_id_column = allocator.allocate([ROW_ID_COLUMN])
            entity.columns.append(
                FlatColumn(
                    name=row_id_column,
                    kind="rowId",
                    expression=f'{entity.alias}."_id"',
                    json_path=None,
                    source_table=table,
                    source_columns=["_id"],
                    comment="row id of this entity",
                )
            )
        parent_column_name: str | None = None
        if "_parent" in physical_names:
            parent_column_name = allocator.allocate([PARENT_ID_COLUMN])
            parent_reference = (
                f"{self.flat_schema}.{pending.parent_view}.{pending.parent_column}"
                if pending.parent_column is not None
                else "the parent entity"
            )
            entity.columns.append(
                FlatColumn(
                    name=parent_column_name,
                    kind="parentId",
                    expression=f'{entity.alias}."_parent"',
                    json_path=None,
                    source_table=table,
                    source_columns=["_parent"],
                    comment=f"join key to {parent_reference}",
                )
            )
        order_column_name: str | None = None
        if "_pos" in physical_names:
            order_column_name = allocator.allocate([ARRAY_INDEX_COLUMN])
            entity.columns.append(
                FlatColumn(
                    name=order_column_name,
                    kind="arrayIndex",
                    expression=f'{entity.alias}."_pos"',
                    json_path=None,
                    source_table=table,
                    source_columns=["_pos"],
                    comment="0-based position in the source array",
                )
            )

        children: list[_PendingEntity] = []
        object_key_columns: dict[str, str | None] = {table: row_id_column}
        self._emit_table(
            table=table,
            alias=entity.alias,
            name_prefix=[],
            path_prefix=pending.json_path,
            entity=entity,
            allocator=allocator,
            alias_counter=[0],
            object_key_columns=object_key_columns,
            children=children,
            entity_name_path=pending.name_path,
        )

        if pending.parent_view and parent_column_name is not None:
            entity.parent = FlatParentLink(
                view=pending.parent_view,
                parent_column=pending.parent_column or "",
                child_column=parent_column_name,
                order_column=order_column_name,
                json_path=pending.json_path,
            )
        entity.comment = self._entity_comment(entity)
        return entity, children

    def _entity_comment(self, entity: FlatEntity) -> str:
        if entity.kind == "root":
            summary = f"JSON Tables flat view of {self.source_schema}.{entity.source_table} (root documents)."
        else:
            summary = (
                f"JSON Tables flat view of {self.source_schema}.{entity.source_table} "
                f"(array {entity.json_path})."
            )
        if entity.parent is not None and entity.parent.parent_column:
            summary += (
                f" Join {entity.parent.child_column} = "
                f"{self.flat_schema}.{entity.parent.view}.{entity.parent.parent_column}."
            )
            if entity.parent.order_column is not None:
                summary += f" Array order: {entity.parent.order_column}."
        summary += " No SQL_PREPROCESSOR_SCRIPT needed."
        return summary

    def _emit_table(
        self,
        *,
        table: str,
        alias: str,
        name_prefix: list[str],
        path_prefix: str,
        entity: FlatEntity,
        allocator: IdentifierAllocator,
        alias_counter: list[int],
        object_key_columns: dict[str, str | None],
        children: list[_PendingEntity],
        entity_name_path: tuple[str, ...],
    ) -> None:
        model = self.table_models[table]
        relationships = self.relationships_by_parent.get(table, [])
        object_children = {
            relationship.segment_name: relationship
            for relationship in relationships
            if relationship.relation_kind == "object"
        }
        array_children = {
            relationship.segment_name: relationship
            for relationship in relationships
            if relationship.relation_kind == "array"
        }
        for group in model.groups.values():
            base_name = group.base_name
            label = _segment_label(base_name)
            json_path = _json_path(path_prefix, base_name)

            scalar = _render_scalar_expression(alias, group)
            if scalar is not None:
                expression, source_columns = scalar
                name = allocator.allocate(name_prefix + [label])
                entity.columns.append(
                    FlatColumn(
                        name=name,
                        kind="scalar",
                        expression=expression,
                        json_path=json_path,
                        source_table=table,
                        source_columns=source_columns,
                        comment=json_path,
                    )
                )

            if group.array_member is not None:
                name = allocator.allocate(name_prefix + [label, ARRAY_LENGTH_SUFFIX])
                entity.columns.append(
                    FlatColumn(
                        name=name,
                        kind="arrayLength",
                        expression=f"{alias}.{quote_identifier(group.array_member.name)}",
                        json_path=json_path,
                        source_table=table,
                        source_columns=[group.array_member.name],
                        comment=f"number of elements in {json_path}",
                    )
                )
                relationship = array_children.get(base_name)
                if relationship is not None:
                    children.append(
                        _PendingEntity(
                            source_table=relationship.child_table,
                            name_path=entity_name_path + tuple(name_prefix) + (label,),
                            parent_view=entity.view_name,
                            parent_column=object_key_columns.get(table),
                            json_path=f"{json_path}[]",
                        )
                    )

            if group.object_member is None:
                continue
            relationship = object_children.get(base_name)
            if relationship is None:
                continue
            child_table = relationship.child_table
            child_needs_key = any(
                child.relation_kind == "array"
                for child in self.relationships_by_parent.get(child_table, [])
            )
            if child_needs_key:
                key_name = allocator.allocate(name_prefix + [label, OBJECT_ID_SUFFIX])
                entity.columns.append(
                    FlatColumn(
                        name=key_name,
                        kind="objectId",
                        expression=f"{alias}.{quote_identifier(group.object_member.name)}",
                        json_path=json_path,
                        source_table=table,
                        source_columns=[group.object_member.name],
                        comment=f"row id of {json_path}, join key for its nested arrays",
                    )
                )
                object_key_columns[child_table] = key_name
            alias_counter[0] += 1
            child_alias = f"t{alias_counter[0]}"
            entity.joins.append(
                f"LEFT JOIN {quote_qualified(self.source_schema, child_table)} {child_alias}"
                f' ON {child_alias}."_id" = {alias}.{quote_identifier(group.object_member.name)}'
            )
            self._emit_table(
                table=child_table,
                alias=child_alias,
                name_prefix=name_prefix + [label],
                path_prefix=json_path,
                entity=entity,
                allocator=allocator,
                alias_counter=alias_counter,
                object_key_columns=object_key_columns,
                children=children,
                entity_name_path=entity_name_path,
            )


def render_flat_view_sql(entity: FlatEntity, *, source_schema: str, flat_schema: str) -> str:
    column_definitions = ",\n".join(
        f"  {quote_identifier(column.name)} COMMENT IS {sql_literal(column.comment)}"
        for column in entity.columns
    )
    select_lines = ",\n".join(
        f"  {column.expression} AS {quote_identifier(column.name)}" for column in entity.columns
    )
    statement = (
        f"CREATE OR REPLACE VIEW {quote_qualified(flat_schema, entity.view_name)} (\n"
        f"{column_definitions}\n"
        f") AS\n"
        f"SELECT\n{select_lines}\n"
        f"FROM {quote_qualified(source_schema, entity.source_table)} {entity.alias}"
    )
    for join in entity.joins:
        statement += f"\n{join}"
    statement += f"\nCOMMENT IS {sql_literal(entity.comment)}"
    return statement


def build_flat_manifest(
    *,
    source_schema: str,
    flat_schema: str,
    entities: Sequence[FlatEntity],
) -> dict[str, Any]:
    return {
        "schema": flat_schema,
        "sourceSchema": source_schema,
        "activationRequired": False,
        "entities": [
            {
                "view": entity.view_name,
                "sourceTable": entity.source_table,
                "rootTable": entity.root_table,
                "kind": entity.kind,
                "jsonPath": entity.json_path,
                "parent": (
                    None
                    if entity.parent is None
                    else {
                        "view": entity.parent.view,
                        "parentColumn": entity.parent.parent_column,
                        "childColumn": entity.parent.child_column,
                        "orderColumn": entity.parent.order_column,
                        "jsonPath": entity.parent.json_path,
                    }
                ),
                "columns": [
                    {
                        "name": column.name,
                        "kind": column.kind,
                        "jsonPath": column.json_path,
                        "sourceTable": column.source_table,
                        "sourceColumns": list(column.source_columns),
                    }
                    for column in entity.columns
                ],
            }
            for entity in entities
        ],
    }


def generate_flat_surface(
    *,
    source_schema: str,
    flat_schema: str,
    table_models: dict[str, TableModel],
    relationships: Iterable[Relationship],
    root_tables: Sequence[str],
    root_by_table: dict[str, str],
) -> FlatSurface:
    source_schema = source_schema.upper()
    flat_schema = flat_schema.upper()
    entities = _FlatSurfaceBuilder(
        source_schema=source_schema,
        flat_schema=flat_schema,
        table_models=table_models,
        relationships=relationships,
        root_tables=root_tables,
        root_by_table=root_by_table,
    ).build()
    statements = [
        f"DROP SCHEMA IF EXISTS {quote_identifier(flat_schema)} CASCADE",
        f"CREATE SCHEMA {quote_identifier(flat_schema)}",
    ]
    statements.extend(
        render_flat_view_sql(entity, source_schema=source_schema, flat_schema=flat_schema)
        for entity in entities
    )
    return FlatSurface(
        schema=flat_schema,
        source_schema=source_schema,
        entities=entities,
        sql=";\n\n".join(statements) + ";\n",
        manifest=build_flat_manifest(
            source_schema=source_schema,
            flat_schema=flat_schema,
            entities=entities,
        ),
    )


def build_join_key_lines(flat_manifest: dict[str, Any]) -> list[str]:
    """Human-readable join keys between the generated flat entity views."""
    schema = str(flat_manifest["schema"])
    lines: list[str] = []
    for entity in flat_manifest["entities"]:
        parent = entity.get("parent")
        if parent is None:
            continue
        line = (
            f'{schema}.{parent["view"]}.{parent["parentColumn"]}'
            f' = {schema}.{entity["view"]}.{parent["childColumn"]}'
            f'   ({entity["jsonPath"]}'
        )
        if parent.get("orderColumn"):
            line += f', ordered by {parent["orderColumn"]}'
        line += ")"
        lines.append(line)
    return lines
