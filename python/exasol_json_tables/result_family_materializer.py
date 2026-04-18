from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Union

from .wrapper_schema_support import (
    Relationship,
    build_relationships,
    build_root_families,
    build_table_models,
    fetch_source_columns,
    find_root_tables,
    load_installed_wrapper_manifests,
)


TableKind = Literal["table", "local_temporary"]
MaterializationKind = Literal["family_preserving_subset", "synthesized_family", "structured_shape"]
StructuredFieldKind = Literal["scalar", "object_ref", "array_ref"]


def qident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qqualified(schema: str, name: str) -> str:
    return f"{qident(schema)}.{qident(name)}"


@dataclass(frozen=True)
class ResultTableSpec:
    table_name: str
    select_sql: str


@dataclass(frozen=True)
class FamilyPreservingSubsetSpec:
    source_helper_schema: str
    root_table: str
    root_filter_sql: str


@dataclass(frozen=True)
class SynthesizedFamilySpec:
    root_table: str
    table_specs: list[ResultTableSpec]


@dataclass(frozen=True)
class StructuredFieldSpec:
    name: str
    sql: str
    kind: StructuredFieldKind = "scalar"


@dataclass(frozen=True)
class StructuredObjectNodeSpec:
    from_sql: str
    id_sql: str
    fields: list[StructuredFieldSpec]
    objects: list["StructuredObjectNodeSpec"]
    arrays: list["StructuredArrayNodeSpec"]
    name: str | None = None


@dataclass(frozen=True)
class StructuredArrayNodeSpec:
    name: str
    from_sql: str
    parent_id_sql: str
    position_sql: str
    row_id_sql: str | None = None
    value_sql: str | None = None
    fields: list[StructuredFieldSpec] | None = None
    objects: list[StructuredObjectNodeSpec] | None = None
    arrays: list["StructuredArrayNodeSpec"] | None = None


@dataclass(frozen=True)
class StructuredShapeSpec:
    root_table: str
    root: StructuredObjectNodeSpec


@dataclass(frozen=True)
class FamilyDescription:
    source_schema: str
    root_tables: list[str]
    relationships: list[Relationship]
    family_tables_by_root: dict[str, list[str]]


@dataclass(frozen=True)
class MaterializedFamilyResult:
    source_schema: str
    root_table: str
    created_tables: list[str]
    family_description: FamilyDescription
    relationships_used: list[Relationship]
    table_kind: TableKind


ResultFamilyMaterializationSpec = Union[FamilyPreservingSubsetSpec, SynthesizedFamilySpec, StructuredShapeSpec]
FROM_OR_JOIN_SCHEMA_RE = re.compile(
    r'(?is)\b(?:FROM|JOIN)\s+(?:"((?:[^"]|"")+)"|([A-Za-z][A-Za-z0-9_]*))\s*\.\s*(?:"(?:[^"]|"")+"|[A-Za-z][A-Za-z0-9_]*)'
)


def _prepare_target_schema(con, target_schema: str, table_kind: TableKind, reset_schema: bool) -> None:
    if table_kind == "table":
        if reset_schema:
            con.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    elif table_kind == "local_temporary":
        if reset_schema:
            con.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        con.execute(f"OPEN SCHEMA {target_schema}")
    else:  # pragma: no cover - defensive
        raise ValueError(f"Unsupported table_kind: {table_kind}")


def _create_table_as_select(
    con,
    target_schema: str,
    table_name: str,
    select_sql: str,
    table_kind: TableKind,
) -> None:
    if table_kind == "table":
        con.execute(
            f"""
            CREATE TABLE {qqualified(target_schema, table_name)} AS
            {select_sql}
            """
        )
    else:
        con.execute(
            f"""
            CREATE LOCAL TEMPORARY TABLE {qident(table_name)} AS
            {select_sql}
            """
        )


def _extract_schema_names_from_sql(sql: str) -> set[str]:
    matches: set[str] = set()
    for match in FROM_OR_JOIN_SCHEMA_RE.finditer(sql):
        schema_name = match.group(1) or match.group(2)
        if schema_name:
            matches.add(schema_name.replace('""', '"'))
    return matches


def _wrapper_manifests_for_family_spec(con, family_spec: SynthesizedFamilySpec) -> list[dict[str, Any]]:
    referenced_schemas: set[str] = set()
    for table_spec in family_spec.table_specs:
        referenced_schemas.update(_extract_schema_names_from_sql(table_spec.select_sql))
    if not referenced_schemas:
        return []
    return load_installed_wrapper_manifests(con, referenced_schemas)


def _ephemeral_preprocessor_names(target_schema: str) -> tuple[str, str]:
    return (f"{target_schema}_PP_TMP", "JSON_TABLES_RESULT_PREPROCESSOR")


def _install_ephemeral_wrapper_preprocessor(
    con,
    *,
    target_schema: str,
    manifests: list[dict[str, Any]],
) -> tuple[str, str]:
    from .generate_wrapper_preprocessor_sql import generate_wrapper_preprocessor_sql_text
    from .wrapper_package_tool import execute_generated_preprocessor_sql

    script_schema, script_name = _ephemeral_preprocessor_names(target_schema)
    sql_text = generate_wrapper_preprocessor_sql_text(
        schema=script_schema,
        script=script_name,
        wrapper_schemas=[str(manifest["publicSchema"]) for manifest in manifests],
        helper_schemas=[str(manifest["helperSchema"]) for manifest in manifests],
        manifests=manifests,
        activate_session=False,
    )
    execute_generated_preprocessor_sql(con, sql_text)
    con.execute(f"OPEN SCHEMA {qident(target_schema)}")
    con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {qqualified(script_schema, script_name)}")
    return script_schema, script_name


def describe_source_families(con, source_schema: str) -> FamilyDescription:
    source_columns = fetch_source_columns(con, source_schema)
    table_models = build_table_models(source_columns)
    relationships = build_relationships(table_models)
    root_tables = find_root_tables(table_models, relationships)
    root_by_table = build_root_families(root_tables, relationships)
    family_tables_by_root = {
        root_table: sorted(
            table_name
            for table_name, family_root in root_by_table.items()
            if family_root == root_table
        )
        for root_table in root_tables
    }
    return FamilyDescription(
        source_schema=source_schema,
        root_tables=root_tables,
        relationships=relationships,
        family_tables_by_root=family_tables_by_root,
    )


def relationship_to_dict(relationship: Relationship) -> dict[str, str]:
    return {
        "parentTable": relationship.parent_table,
        "childTable": relationship.child_table,
        "segmentName": relationship.segment_name,
        "relationKind": relationship.relation_kind,
    }


def relationship_from_dict(value: dict[str, Any]) -> Relationship:
    return Relationship(
        parent_table=value["parentTable"],
        child_table=value["childTable"],
        segment_name=value["segmentName"],
        relation_kind=value["relationKind"],
    )


def result_table_spec_to_dict(table_spec: ResultTableSpec) -> dict[str, str]:
    return {
        "tableName": table_spec.table_name,
        "selectSql": table_spec.select_sql,
    }


def result_table_spec_from_dict(value: dict[str, Any]) -> ResultTableSpec:
    return ResultTableSpec(
        table_name=value["tableName"],
        select_sql=value["selectSql"],
    )


def structured_field_spec_to_dict(field_spec: StructuredFieldSpec) -> dict[str, str]:
    value = {
        "name": field_spec.name,
        "sql": field_spec.sql,
    }
    if field_spec.kind != "scalar":
        value["kind"] = field_spec.kind
    return value


def structured_field_spec_from_dict(value: dict[str, Any]) -> StructuredFieldSpec:
    return StructuredFieldSpec(
        name=value["name"],
        sql=value["sql"],
        kind=value.get("kind", "scalar"),
    )


def structured_object_node_spec_to_dict(node_spec: StructuredObjectNodeSpec) -> dict[str, Any]:
    value: dict[str, Any] = {
        "fromSql": node_spec.from_sql,
        "idSql": node_spec.id_sql,
        "fields": [structured_field_spec_to_dict(field_spec) for field_spec in node_spec.fields],
    }
    if node_spec.name is not None:
        value["name"] = node_spec.name
    if node_spec.objects:
        value["objects"] = [structured_object_node_spec_to_dict(node) for node in node_spec.objects]
    if node_spec.arrays:
        value["arrays"] = [structured_array_node_spec_to_dict(node) for node in node_spec.arrays]
    return value


def structured_object_node_spec_from_dict(value: dict[str, Any]) -> StructuredObjectNodeSpec:
    return StructuredObjectNodeSpec(
        name=value.get("name"),
        from_sql=value["fromSql"],
        id_sql=value["idSql"],
        fields=[structured_field_spec_from_dict(field) for field in value.get("fields", [])],
        objects=[structured_object_node_spec_from_dict(node) for node in value.get("objects", [])],
        arrays=[structured_array_node_spec_from_dict(node) for node in value.get("arrays", [])],
    )


def structured_array_node_spec_to_dict(node_spec: StructuredArrayNodeSpec) -> dict[str, Any]:
    value: dict[str, Any] = {
        "name": node_spec.name,
        "fromSql": node_spec.from_sql,
        "parentIdSql": node_spec.parent_id_sql,
        "positionSql": node_spec.position_sql,
    }
    if node_spec.row_id_sql is not None:
        value["rowIdSql"] = node_spec.row_id_sql
    if node_spec.value_sql is not None:
        value["valueSql"] = node_spec.value_sql
    if node_spec.fields:
        value["fields"] = [structured_field_spec_to_dict(field_spec) for field_spec in node_spec.fields]
    if node_spec.objects:
        value["objects"] = [structured_object_node_spec_to_dict(node) for node in node_spec.objects]
    if node_spec.arrays:
        value["arrays"] = [structured_array_node_spec_to_dict(node) for node in node_spec.arrays]
    return value


def structured_array_node_spec_from_dict(value: dict[str, Any]) -> StructuredArrayNodeSpec:
    return StructuredArrayNodeSpec(
        name=value["name"],
        from_sql=value["fromSql"],
        parent_id_sql=value["parentIdSql"],
        position_sql=value["positionSql"],
        row_id_sql=value.get("rowIdSql"),
        value_sql=value.get("valueSql"),
        fields=[structured_field_spec_from_dict(field) for field in value.get("fields", [])],
        objects=[structured_object_node_spec_from_dict(node) for node in value.get("objects", [])],
        arrays=[structured_array_node_spec_from_dict(node) for node in value.get("arrays", [])],
    )


def structured_shape_spec_to_dict(shape_spec: StructuredShapeSpec) -> dict[str, Any]:
    return {
        "kind": "structured_shape",
        "rootTable": shape_spec.root_table,
        "root": structured_object_node_spec_to_dict(shape_spec.root),
    }


def structured_shape_spec_from_dict(value: dict[str, Any]) -> StructuredShapeSpec:
    return StructuredShapeSpec(
        root_table=value["rootTable"],
        root=structured_object_node_spec_from_dict(value["root"]),
    )


def family_description_to_dict(description: FamilyDescription) -> dict[str, Any]:
    return {
        "sourceSchema": description.source_schema,
        "rootTables": list(description.root_tables),
        "relationships": [relationship_to_dict(relationship) for relationship in description.relationships],
        "familyTablesByRoot": {
            root_table: list(table_names)
            for root_table, table_names in sorted(description.family_tables_by_root.items())
        },
    }


def family_description_from_dict(value: dict[str, Any]) -> FamilyDescription:
    return FamilyDescription(
        source_schema=value["sourceSchema"],
        root_tables=list(value["rootTables"]),
        relationships=[relationship_from_dict(item) for item in value["relationships"]],
        family_tables_by_root={
            root_table: list(table_names)
            for root_table, table_names in value["familyTablesByRoot"].items()
        },
    )


def materialized_family_result_to_dict(result: MaterializedFamilyResult) -> dict[str, Any]:
    return {
        "sourceSchema": result.source_schema,
        "rootTable": result.root_table,
        "createdTables": list(result.created_tables),
        "familyDescription": family_description_to_dict(result.family_description),
        "relationshipsUsed": [relationship_to_dict(relationship) for relationship in result.relationships_used],
        "tableKind": result.table_kind,
    }


def materialized_family_result_from_dict(value: dict[str, Any]) -> MaterializedFamilyResult:
    return MaterializedFamilyResult(
        source_schema=value["sourceSchema"],
        root_table=value["rootTable"],
        created_tables=list(value["createdTables"]),
        family_description=family_description_from_dict(value["familyDescription"]),
        relationships_used=[relationship_from_dict(item) for item in value["relationshipsUsed"]],
        table_kind=value["tableKind"],
    )


def result_family_spec_to_dict(spec: ResultFamilyMaterializationSpec) -> dict[str, Any]:
    if isinstance(spec, FamilyPreservingSubsetSpec):
        return {
            "kind": "family_preserving_subset",
            "sourceHelperSchema": spec.source_helper_schema,
            "rootTable": spec.root_table,
            "rootFilterSql": spec.root_filter_sql,
        }
    if isinstance(spec, StructuredShapeSpec):
        return structured_shape_spec_to_dict(spec)
    return {
        "kind": "synthesized_family",
        "rootTable": spec.root_table,
        "tableSpecs": [result_table_spec_to_dict(table_spec) for table_spec in spec.table_specs],
    }


def result_family_spec_from_dict(value: dict[str, Any]) -> ResultFamilyMaterializationSpec:
    kind = value["kind"]
    if kind == "family_preserving_subset":
        return FamilyPreservingSubsetSpec(
            source_helper_schema=value["sourceHelperSchema"],
            root_table=value["rootTable"],
            root_filter_sql=value["rootFilterSql"],
        )
    if kind == "synthesized_family":
        return SynthesizedFamilySpec(
            root_table=value["rootTable"],
            table_specs=[result_table_spec_from_dict(item) for item in value["tableSpecs"]],
        )
    if kind == "structured_shape":
        return structured_shape_spec_from_dict(value)
    raise ValueError(f"Unsupported result family materialization kind: {kind!r}")


def materialize_result_family(
    con,
    *,
    target_schema: str,
    spec: ResultFamilyMaterializationSpec,
    table_kind: TableKind = "table",
    reset_schema: bool = True,
) -> MaterializedFamilyResult:
    if isinstance(spec, FamilyPreservingSubsetSpec):
        return materialize_family_preserving_subset(
            con,
            source_helper_schema=spec.source_helper_schema,
            target_schema=target_schema,
            root_table=spec.root_table,
            root_filter_sql=spec.root_filter_sql,
            table_kind=table_kind,
            reset_schema=reset_schema,
        )
    if isinstance(spec, StructuredShapeSpec):
        return materialize_synthesized_family(
            con,
            target_schema=target_schema,
            family_spec=compile_structured_shape_spec(spec),
            table_kind=table_kind,
            reset_schema=reset_schema,
        )
    return materialize_synthesized_family(
        con,
        target_schema=target_schema,
        family_spec=spec,
        table_kind=table_kind,
        reset_schema=reset_schema,
    )


def materialize_family_preserving_subset(
    con,
    *,
    source_helper_schema: str,
    target_schema: str,
    root_table: str,
    root_filter_sql: str,
    table_kind: TableKind = "table",
    reset_schema: bool = True,
) -> MaterializedFamilyResult:
    _prepare_target_schema(con, target_schema, table_kind, reset_schema)
    _create_table_as_select(
        con,
        target_schema,
        root_table,
        f"""
        SELECT *
        FROM {qqualified(source_helper_schema, root_table)}
        WHERE {root_filter_sql}
        """,
        table_kind,
    )

    relationship_rows = con.execute(
        f"""
        SELECT PARENT_TABLE, CHILD_TABLE, SEGMENT_NAME, RELATION_KIND
        FROM {qqualified(source_helper_schema, "__JVS_RELATIONSHIPS")}
        WHERE ROOT_TABLE = '{root_table}'
        ORDER BY PARENT_TABLE, CHILD_TABLE, SEGMENT_NAME
        """
    ).fetchall()
    pending = [
        Relationship(
            parent_table=parent,
            child_table=child,
            segment_name=segment,
            relation_kind=relation_kind,
        )
        for parent, child, segment, relation_kind in relationship_rows
    ]
    created_tables = {root_table}

    while pending:
        progress = False
        remaining: list[Relationship] = []
        for relationship in pending:
            if relationship.parent_table not in created_tables:
                remaining.append(relationship)
                continue
            if relationship.relation_kind == "object":
                join_predicate = (
                    f'p.{qident(relationship.segment_name + "|object")} = '
                    f'c.{qident("_id")}'
                )
            elif relationship.relation_kind == "array":
                join_predicate = (
                    f'c.{qident("_parent")} = '
                    f'p.{qident("_id")}'
                )
            else:
                raise ValueError(f"Unsupported relation kind: {relationship.relation_kind}")
            _create_table_as_select(
                con,
                target_schema,
                relationship.child_table,
                f"""
                SELECT c.*
                FROM {qqualified(source_helper_schema, relationship.child_table)} c
                JOIN {qqualified(target_schema, relationship.parent_table)} p
                  ON {join_predicate}
                """,
                table_kind,
            )
            created_tables.add(relationship.child_table)
            progress = True
        if not progress:
            unresolved = [
                {
                    "parent": relationship.parent_table,
                    "child": relationship.child_table,
                    "segment": relationship.segment_name,
                    "kind": relationship.relation_kind,
                }
                for relationship in remaining
            ]
            raise AssertionError(f"Could not resolve family materialization order for {unresolved!r}")
        pending = remaining

    family_description = describe_source_families(con, target_schema)
    if root_table not in family_description.root_tables:
        raise AssertionError(f"Expected {root_table} to be a root table in {target_schema}")
    return MaterializedFamilyResult(
        source_schema=target_schema,
        root_table=root_table,
        created_tables=sorted(created_tables),
        family_description=family_description,
        relationships_used=[
            relationship
            for relationship in family_description.relationships
            if relationship.parent_table in created_tables and relationship.child_table in created_tables
        ],
        table_kind=table_kind,
    )


def _format_select_sql(select_items: list[tuple[str, str]], from_sql: str) -> str:
    if not select_items:
        raise ValueError("select_items must not be empty")
    rendered_items = ",\n  ".join(
        f"{expression} AS {qident(alias)}"
        for expression, alias in select_items
    )
    return "SELECT\n  " + rendered_items + "\n" + from_sql.strip()


def _validate_unique_names(names: list[str], *, label: str) -> None:
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"{label} contain duplicate names: {duplicates!r}")


def _validate_structured_object_node(node: StructuredObjectNodeSpec, *, label: str) -> None:
    field_names = [field.name for field in node.fields]
    object_names = [child.name for child in node.objects if child.name is not None]
    array_names = [child.name for child in node.arrays]
    _validate_unique_names(field_names, label=f"{label} fields")
    _validate_unique_names(object_names, label=f"{label} objects")
    _validate_unique_names(array_names, label=f"{label} arrays")

    field_kind_by_name = {field.name: field.kind for field in node.fields}
    for object_name in object_names:
        if field_kind_by_name.get(object_name) != "object_ref":
            raise ValueError(
                f"{label} object {object_name!r} requires a matching object_ref field in the parent node."
            )
    for array_name in array_names:
        if field_kind_by_name.get(array_name) != "array_ref":
            raise ValueError(
                f"{label} array {array_name!r} requires a matching array_ref field in the parent node."
            )

    for child in node.objects:
        child_label = f"{label}.{child.name}" if child.name is not None else label
        _validate_structured_object_node(child, label=child_label)
    for child in node.arrays:
        _validate_structured_array_node(child, label=f"{label}.{child.name}")


def _validate_structured_array_node(node: StructuredArrayNodeSpec, *, label: str) -> None:
    fields = node.fields or []
    objects = node.objects or []
    arrays = node.arrays or []
    field_names = [field.name for field in fields]
    object_names = [child.name for child in objects if child.name is not None]
    array_names = [child.name for child in arrays]
    _validate_unique_names(field_names, label=f"{label} fields")
    _validate_unique_names(object_names, label=f"{label} objects")
    _validate_unique_names(array_names, label=f"{label} arrays")

    if node.value_sql is not None:
        if node.row_id_sql is not None:
            raise ValueError(f"{label} must not specify row_id_sql for a scalar array node.")
        if fields or objects or arrays:
            raise ValueError(f"{label} scalar array nodes cannot also define nested fields, objects, or arrays.")
        return
    if node.row_id_sql is None:
        raise ValueError(f"{label} object array nodes require row_id_sql.")

    field_kind_by_name = {field.name: field.kind for field in fields}
    for object_name in object_names:
        if field_kind_by_name.get(object_name) != "object_ref":
            raise ValueError(
                f"{label} object {object_name!r} requires a matching object_ref field in the parent array node."
            )
    for array_name in array_names:
        if field_kind_by_name.get(array_name) != "array_ref":
            raise ValueError(
                f"{label} array {array_name!r} requires a matching array_ref field in the parent array node."
            )

    for child in objects:
        child_label = f"{label}.{child.name}" if child.name is not None else label
        _validate_structured_object_node(child, label=child_label)
    for child in arrays:
        _validate_structured_array_node(child, label=f"{label}.{child.name}")


def _compile_structured_object_node(
    *,
    table_name: str,
    node: StructuredObjectNodeSpec,
) -> list[ResultTableSpec]:
    select_items: list[tuple[str, str]] = [(node.id_sql, "_id")]
    for field in node.fields:
        if field.kind == "scalar":
            alias = field.name
        elif field.kind == "object_ref":
            alias = f"{field.name}|object"
        elif field.kind == "array_ref":
            alias = f"{field.name}|array"
        else:  # pragma: no cover - defensive
            raise ValueError(f"Unsupported structured field kind: {field.kind!r}")
        select_items.append((field.sql, alias))

    table_specs = [
        ResultTableSpec(
            table_name=table_name,
            select_sql=_format_select_sql(select_items, node.from_sql),
        )
    ]
    for child in node.objects:
        if child.name is None:
            raise ValueError(f"Structured object node under {table_name!r} is missing a name.")
        table_specs.extend(
            _compile_structured_object_node(
                table_name=f"{table_name}_{child.name}",
                node=child,
            )
        )
    for child in node.arrays:
        table_specs.extend(
            _compile_structured_array_node(
                table_name=f"{table_name}_{child.name}_arr",
                node=child,
            )
        )
    return table_specs


def _compile_structured_array_node(
    *,
    table_name: str,
    node: StructuredArrayNodeSpec,
) -> list[ResultTableSpec]:
    if node.value_sql is not None:
        table_specs = [
            ResultTableSpec(
                table_name=table_name,
                select_sql=_format_select_sql(
                    [
                        (node.parent_id_sql, "_parent"),
                        (node.position_sql, "_pos"),
                        (node.value_sql, "_value"),
                    ],
                    node.from_sql,
                ),
            )
        ]
    else:
        select_items: list[tuple[str, str]] = [
            (node.row_id_sql, "_id"),
            (node.parent_id_sql, "_parent"),
            (node.position_sql, "_pos"),
        ]
        for field in node.fields or []:
            if field.kind == "scalar":
                alias = field.name
            elif field.kind == "object_ref":
                alias = f"{field.name}|object"
            elif field.kind == "array_ref":
                alias = f"{field.name}|array"
            else:  # pragma: no cover - defensive
                raise ValueError(f"Unsupported structured field kind: {field.kind!r}")
            select_items.append((field.sql, alias))

        table_specs = [
            ResultTableSpec(
                table_name=table_name,
                select_sql=_format_select_sql(select_items, node.from_sql),
            )
        ]
        for child in node.objects or []:
            if child.name is None:
                raise ValueError(f"Structured object node under {table_name!r} is missing a name.")
            table_specs.extend(
                _compile_structured_object_node(
                    table_name=f"{table_name}_{child.name}",
                    node=child,
                )
            )
        for child in node.arrays or []:
            table_specs.extend(
                _compile_structured_array_node(
                    table_name=f"{table_name}_{child.name}_arr",
                    node=child,
                )
            )
    return table_specs


def compile_structured_shape_spec(shape_spec: StructuredShapeSpec) -> SynthesizedFamilySpec:
    _validate_structured_object_node(shape_spec.root, label=shape_spec.root_table)
    return SynthesizedFamilySpec(
        root_table=shape_spec.root_table,
        table_specs=_compile_structured_object_node(
            table_name=shape_spec.root_table,
            node=shape_spec.root,
        ),
    )


def materialize_synthesized_family(
    con,
    *,
    target_schema: str,
    family_spec: SynthesizedFamilySpec,
    table_kind: TableKind = "table",
    reset_schema: bool = True,
) -> MaterializedFamilyResult:
    if not family_spec.table_specs:
        raise ValueError("family_spec.table_specs must not be empty")
    table_names = [table_spec.table_name for table_spec in family_spec.table_specs]
    if family_spec.root_table not in table_names:
        raise ValueError(
            f"Root table {family_spec.root_table!r} must be present in table_specs (got {table_names!r})"
        )
    if len(set(table_names)) != len(table_names):
        raise ValueError(f"table_specs contain duplicate table names: {table_names!r}")

    _prepare_target_schema(con, target_schema, table_kind, reset_schema)
    wrapper_manifests = _wrapper_manifests_for_family_spec(con, family_spec)
    temp_preprocessor_schema: str | None = None
    try:
        if wrapper_manifests:
            temp_preprocessor_schema, _ = _install_ephemeral_wrapper_preprocessor(
                con,
                target_schema=target_schema,
                manifests=wrapper_manifests,
            )
        for table_spec in family_spec.table_specs:
            _create_table_as_select(
                con,
                target_schema,
                table_spec.table_name,
                table_spec.select_sql,
                table_kind,
            )
    finally:
        if temp_preprocessor_schema is not None:
            try:
                con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
            finally:
                con.execute(f"DROP SCHEMA IF EXISTS {qident(temp_preprocessor_schema)} CASCADE")

    family_description = describe_source_families(con, target_schema)
    if family_spec.root_table not in family_description.root_tables:
        raise AssertionError(
            f"Expected synthesized root {family_spec.root_table} to be a root table in {target_schema}; "
            f"got roots {family_description.root_tables!r}"
        )
    created_tables = [table_spec.table_name for table_spec in family_spec.table_specs]
    return MaterializedFamilyResult(
        source_schema=target_schema,
        root_table=family_spec.root_table,
        created_tables=created_tables,
        family_description=family_description,
        relationships_used=[
            relationship
            for relationship in family_description.relationships
            if relationship.parent_table in created_tables and relationship.child_table in created_tables
        ],
        table_kind=table_kind,
    )
