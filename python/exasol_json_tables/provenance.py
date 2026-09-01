#!/usr/bin/env python3

"""Provenance comments for families this side of the project creates.

The Rust ingest crates stamp every table they load (see
``crates/json_tables_core/src/manifest.rs``). Families that Python creates -- a
materialized result family, say -- are the same contract and the same consumers
read them, so they carry the same comment in the same shape. A family without one
is a family a consumer can only guess about.

``CONTRACT_VERSION`` mirrors ``json_tables_core::contract::CONTRACT_VERSION``;
``test_provenance_stamping.py`` fails if the two drift.
"""

from __future__ import annotations

import json
from typing import Iterable

from .wrapper_schema_support import Relationship, quote_qualified, sql_literal


CONTRACT_VERSION = 1
TOOL_NAME = "exasol-json-tables"
COMMENT_PREFIX = "COPY provenance "

#: Source kind for a family created by materializing a query result.
RESULT_FAMILY_CONNECTION = "result-family"


def build_provenance_comment(
    *,
    source: str,
    source_connection: str,
    imported_at: str,
    table_path: str,
    source_modified_at: str | None = None,
) -> str:
    """One ``COPY provenance {...}`` comment, field-for-field as the Rust side writes it."""
    fields: dict[str, object] = {
        "source": source,
        "sourceConnection": source_connection,
        "importedAt": imported_at,
        "tablePath": table_path,
        "tool": TOOL_NAME,
        "contractVersion": CONTRACT_VERSION,
    }
    if source_modified_at is not None:
        fields["sourceModifiedAt"] = source_modified_at
    return COMMENT_PREFIX + json.dumps(fields, separators=(",", ":"))


def provenance_comment_statement(schema: str, table_name: str, comment: str) -> str:
    return f"COMMENT ON TABLE {quote_qualified(schema, table_name)} IS {sql_literal(comment)}"


def parse_provenance_comment(comment: str | None) -> dict[str, object] | None:
    """The comment's fields, or ``None`` if this is not a provenance comment."""
    if not comment or not comment.startswith(COMMENT_PREFIX):
        return None
    try:
        parsed = json.loads(comment[len(COMMENT_PREFIX) :])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def database_timestamp(con) -> str:
    """The database's own clock, so timestamps agree with everything else recorded there."""
    value = con.execute("SELECT CURRENT_TIMESTAMP").fetchval()
    text = str(value)
    if "T" in text:
        return text
    date_part, _, time_part = text.partition(" ")
    time_part = time_part.split(".")[0] or "00:00:00"
    return f"{date_part}T{time_part}Z"


def table_paths_from_relationships(
    root_table: str,
    relationships: Iterable[Relationship],
    tables: Iterable[str],
) -> dict[str, str]:
    """Map each table of a family to its ``tablePath``, in the Rust ``TablePath`` shape.

    The root is ``root``; a child is its parent's path plus the segment name, with
    ``[]`` appended for an array segment -- ``meta.info``, ``items[]``.
    """
    children_by_parent: dict[str, list[Relationship]] = {}
    for relationship in relationships:
        children_by_parent.setdefault(relationship.parent_table, []).append(relationship)

    paths = {root_table: "root"}
    pending = [root_table]
    while pending:
        parent = pending.pop()
        for relationship in children_by_parent.get(parent, []):
            if relationship.child_table in paths:
                continue
            segment = relationship.segment_name
            if relationship.relation_kind == "array":
                segment = f"{segment}[]"
            prefix = paths[parent]
            paths[relationship.child_table] = segment if prefix == "root" else f"{prefix}.{segment}"
            pending.append(relationship.child_table)
    return {table: paths[table] for table in tables if table in paths}


def stamp_family_provenance(
    con,
    *,
    schema: str,
    root_table: str,
    tables: Iterable[str],
    relationships: Iterable[Relationship],
    source: str,
    source_connection: str,
    imported_at: str | None = None,
) -> dict[str, str]:
    """Stamp every table of a family, returning the comment written per table."""
    tables = list(tables)
    imported_at = imported_at or database_timestamp(con)
    paths = table_paths_from_relationships(root_table, relationships, tables)
    written: dict[str, str] = {}
    for table_name in tables:
        comment = build_provenance_comment(
            source=source,
            source_connection=source_connection,
            imported_at=imported_at,
            table_path=paths.get(table_name, table_name),
        )
        con.execute(provenance_comment_statement(schema, table_name, comment))
        written[table_name] = comment
    return written
