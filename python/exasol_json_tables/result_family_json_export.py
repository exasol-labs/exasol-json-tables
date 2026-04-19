from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from .in_session_wrapper_installer import InSessionWrapperInstallResult
from .result_family_materializer import FamilyDescription, MaterializedFamilyResult, describe_source_families
from .wrapper_schema_support import ColumnMeta, build_root_families, build_table_models, fetch_source_columns


MISSING = object()


def qident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qqualified(schema: str, name: str) -> str:
    return f"{qident(schema)}.{qident(name)}"


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _normalize_typed_scalar(value: Any, column: ColumnMeta) -> Any:
    normalized = normalize_scalar(value)
    if normalized is not value:
        return normalized
    if isinstance(value, str) and column.type_name.upper().startswith("DECIMAL"):
        try:
            return normalize_scalar(Decimal(value))
        except InvalidOperation:
            return value
    return value


def _query_columns(con, schema: str, table: str) -> list[ColumnMeta]:
    sql = f"""
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
        WHERE COLUMN_SCHEMA = '{schema}'
          AND COLUMN_TABLE = '{table}'
        ORDER BY COLUMN_ORDINAL_POSITION
    """
    return [
        ColumnMeta(
            schema=row[0],
            table=row[1],
            name=row[2],
            type_name=row[3],
            ordinal=int(row[4]),
            size=row[5],
            precision=row[6],
            scale=row[7],
        )
        for row in con.execute(sql).fetchall()
    ]


def _query_dict_rows(con, schema: str, table: str) -> list[dict[str, Any]]:
    columns = _query_columns(con, schema, table)
    rows = con.execute(f"SELECT * FROM {qqualified(schema, table)}").fetchall()
    return [
        {
            column.name: _normalize_typed_scalar(value, column)
            for column, value in zip(columns, row)
        }
        for row in rows
    ]


def _resolve_source_schema(
    *,
    source_schema: str | None,
    materialized_family: MaterializedFamilyResult | None,
    installed_wrapper: InSessionWrapperInstallResult | None,
) -> str:
    resolved_source_schema = source_schema
    if materialized_family is not None:
        if resolved_source_schema is None:
            resolved_source_schema = materialized_family.source_schema
        elif resolved_source_schema != materialized_family.source_schema:
            raise ValueError(
                f"source_schema={resolved_source_schema!r} does not match "
                f"materialized_family.source_schema={materialized_family.source_schema!r}"
            )
    if installed_wrapper is not None:
        if resolved_source_schema is None:
            resolved_source_schema = installed_wrapper.source_schema
        elif resolved_source_schema != installed_wrapper.source_schema:
            raise ValueError(
                f"source_schema={resolved_source_schema!r} does not match "
                f"installed_wrapper.source_schema={installed_wrapper.source_schema!r}"
            )
    if resolved_source_schema is None:
        raise ValueError("source_schema is required when no materialized_family or installed_wrapper is provided.")
    return resolved_source_schema


def _resolve_family_description(
    con,
    *,
    source_schema: str,
    family_description: FamilyDescription | None,
    materialized_family: MaterializedFamilyResult | None,
) -> FamilyDescription:
    if family_description is not None:
        if family_description.source_schema != source_schema:
            raise ValueError(
                f"family_description.source_schema={family_description.source_schema!r} "
                f"does not match source_schema={source_schema!r}"
            )
        return family_description
    if materialized_family is not None:
        if materialized_family.family_description.source_schema != source_schema:
            raise ValueError(
                f"materialized_family.family_description.source_schema="
                f"{materialized_family.family_description.source_schema!r} "
                f"does not match source_schema={source_schema!r}"
            )
        return materialized_family.family_description
    return describe_source_families(con, source_schema)


def _resolve_root_table(
    *,
    root_table: str | None,
    family_description: FamilyDescription,
    materialized_family: MaterializedFamilyResult | None,
    installed_wrapper: InSessionWrapperInstallResult | None,
) -> str:
    resolved_root_table = root_table
    if resolved_root_table is None and materialized_family is not None:
        resolved_root_table = materialized_family.root_table
    if resolved_root_table is None and installed_wrapper is not None:
        manifest_roots = [root["tableName"] for root in installed_wrapper.manifest["roots"]]
        if len(manifest_roots) == 1:
            resolved_root_table = manifest_roots[0]
        else:
            raise ValueError(
                "root_table is required when installed_wrapper.manifest contains multiple roots: "
                f"{manifest_roots!r}"
            )
    if resolved_root_table is None:
        if len(family_description.root_tables) == 1:
            resolved_root_table = family_description.root_tables[0]
        else:
            raise ValueError(
                "root_table is required when the source schema contains multiple roots: "
                f"{family_description.root_tables!r}"
            )
    if resolved_root_table not in family_description.root_tables:
        raise ValueError(
            f"root_table={resolved_root_table!r} is not present in source schema "
            f"{family_description.source_schema!r}; roots={family_description.root_tables!r}"
        )
    return resolved_root_table


def export_root_family_to_json(
    con,
    *,
    source_schema: str | None = None,
    root_table: str | None = None,
    family_description: FamilyDescription | None = None,
    materialized_family: MaterializedFamilyResult | None = None,
    installed_wrapper: InSessionWrapperInstallResult | None = None,
) -> list[dict[str, Any]]:
    resolved_source_schema = _resolve_source_schema(
        source_schema=source_schema,
        materialized_family=materialized_family,
        installed_wrapper=installed_wrapper,
    )
    resolved_family_description = _resolve_family_description(
        con,
        source_schema=resolved_source_schema,
        family_description=family_description,
        materialized_family=materialized_family,
    )
    resolved_root_table = _resolve_root_table(
        root_table=root_table,
        family_description=resolved_family_description,
        materialized_family=materialized_family,
        installed_wrapper=installed_wrapper,
    )

    source_columns = _query_source_columns(con, resolved_family_description)
    table_models = source_columns["tableModels"]
    root_by_table = source_columns["rootByTable"]

    family_tables = sorted(
        table_name
        for table_name, family_root in root_by_table.items()
        if family_root == resolved_root_table
    )
    rows_by_table = {
        table_name: _query_dict_rows(con, resolved_source_schema, table_name)
        for table_name in family_tables
    }
    rows_by_id = {
        table_name: {
            normalize_scalar(row["_id"]): row
            for row in table_rows
            if "_id" in row and row["_id"] is not None
        }
        for table_name, table_rows in rows_by_table.items()
    }
    rows_by_parent: dict[str, dict[Any, list[dict[str, Any]]]] = {}
    for table_name, table_rows in rows_by_table.items():
        parent_rows: dict[Any, list[dict[str, Any]]] = {}
        for row in table_rows:
            if "_parent" in row:
                parent_rows.setdefault(normalize_scalar(row["_parent"]), []).append(row)
        for row_list in parent_rows.values():
            row_list.sort(key=lambda item: item["_pos"])
        rows_by_parent[table_name] = parent_rows

    relationships_by_parent: dict[str, list[Any]] = {}
    relationship_by_parent_segment: dict[tuple[str, str], Any] = {}
    for relationship in resolved_family_description.relationships:
        if root_by_table[relationship.parent_table] != resolved_root_table:
            continue
        relationships_by_parent.setdefault(relationship.parent_table, []).append(relationship)
        relationship_by_parent_segment[(relationship.parent_table, relationship.segment_name)] = relationship

    def scalar_group_value(group, row: dict[str, Any]) -> Any:
        if group.primary is not None:
            value = row.get(group.primary.name)
            if value is not None:
                return normalize_scalar(value)
        for alternate in sorted(group.alternates, key=lambda column: column.ordinal):
            value = row.get(alternate.name)
            if value is not None:
                return normalize_scalar(value)
        if group.null_mask is not None and row.get(group.null_mask.name) is True:
            return None
        return MISSING

    def build_array_elements(table_name: str, parent_id: Any) -> list[Any]:
        table_rows = rows_by_parent.get(table_name, {}).get(parent_id, [])
        model = table_models[table_name]
        table_relationships = relationships_by_parent.get(table_name, [])
        if set(model.groups) == {"_value"} and not table_relationships:
            value_group = model.groups["_value"]
            elements: list[Any] = []
            for row in table_rows:
                value = scalar_group_value(value_group, row)
                elements.append(None if value is MISSING else value)
            return elements
        return [build_object(table_name, row) for row in table_rows]

    def build_object(table_name: str, row: dict[str, Any]) -> dict[str, Any]:
        model = table_models[table_name]
        document: dict[str, Any] = {}
        for group in sorted(model.groups.values(), key=lambda item: item.base_name):
            relation = relationship_by_parent_segment.get((table_name, group.base_name))
            if relation is not None and relation.relation_kind == "object":
                child_id = row.get(f"{group.base_name}|object")
                if child_id is not None:
                    child_row = rows_by_id[relation.child_table].get(normalize_scalar(child_id))
                    if child_row is None:
                        raise AssertionError(
                            f"Missing child row {relation.child_table}.{child_id} "
                            f"for {table_name}.{group.base_name}"
                        )
                    document[group.base_name] = build_object(relation.child_table, child_row)
                    continue
                if group.null_mask is not None and row.get(group.null_mask.name) is True:
                    document[group.base_name] = None
                    continue
            elif relation is not None and relation.relation_kind == "array":
                array_marker = row.get(f"{group.base_name}|array")
                if array_marker is not None:
                    document[group.base_name] = build_array_elements(
                        relation.child_table,
                        normalize_scalar(row["_id"]),
                    )
                    continue
                if group.null_mask is not None and row.get(group.null_mask.name) is True:
                    document[group.base_name] = None
                    continue

            scalar_value = scalar_group_value(group, row)
            if scalar_value is not MISSING:
                document[group.base_name] = scalar_value

        return document

    root_rows = sorted(rows_by_table[resolved_root_table], key=lambda row: normalize_scalar(row["_id"]))
    return [build_object(resolved_root_table, row) for row in root_rows]


def export_all_root_families_to_json(
    con,
    *,
    source_schema: str | None = None,
    family_description: FamilyDescription | None = None,
    materialized_family: MaterializedFamilyResult | None = None,
    installed_wrapper: InSessionWrapperInstallResult | None = None,
) -> dict[str, list[dict[str, Any]]]:
    resolved_source_schema = _resolve_source_schema(
        source_schema=source_schema,
        materialized_family=materialized_family,
        installed_wrapper=installed_wrapper,
    )
    resolved_family_description = _resolve_family_description(
        con,
        source_schema=resolved_source_schema,
        family_description=family_description,
        materialized_family=materialized_family,
    )
    return {
        root_table: export_root_family_to_json(
            con,
            source_schema=resolved_source_schema,
            root_table=root_table,
            family_description=resolved_family_description,
        )
        for root_table in resolved_family_description.root_tables
    }


def _query_source_columns(con, family_description: FamilyDescription) -> dict[str, Any]:
    source_columns = {}
    for root_tables in family_description.family_tables_by_root.values():
        for table_name in root_tables:
            if table_name in source_columns:
                continue
            source_columns[table_name] = _query_columns(con, family_description.source_schema, table_name)
    # Rebuild table models using the source schema metadata helper for correctness.
    rebuilt_description = describe_source_families(con, family_description.source_schema)

    fetched_columns = fetch_source_columns(con, family_description.source_schema)
    table_models = build_table_models(fetched_columns)
    root_by_table = build_root_families(rebuilt_description.root_tables, rebuilt_description.relationships)
    return {
        "sourceColumns": source_columns,
        "tableModels": table_models,
        "rootByTable": root_by_table,
    }
