# Structured Results

Structured results let you take SQL output and put it back into the same nested contract that Exasol JSON Tables uses for ingested JSON.

That means a result can become:

- a nested result that can be queried again through the wrapper surface
- a durable intermediate result for downstream SQL
- a wrapped result that can be emitted as final JSON through `TO_JSON(...)`

This is useful both when the input already came from JSON and when the input is ordinary relational data that you want to turn into document-shaped output.

## The Mental Model

The easiest way to think about it is:

- the wrapper surface makes JSON-shaped source data pleasant to query
- structured results take SQL output and put it back into that same JSON-shaped contract
- `TO_JSON(*)` or `TO_JSON("field1", "field2")` is usually the final outlet once that wrapped contract exists

If the result is nested, it will usually become:

- one root table
- plus child tables for nested objects and arrays

## When To Use Structured Results

Use structured results when:

- you want nested output from ordinary relational tables
- you want to persist a nested intermediate result inside Exasol
- you want to keep shape-building inside SQL instead of rebuilding everything in application code
- you want the final nested output to come from the SQL surface through `TO_JSON(...)`

If you only need a flat analytical result, plain SQL tables or views are usually enough.

## Two Authoring Levels

There are two ways to describe a structured result:

- `structured_shape`
  This is the higher-level, recommended starting point for common nested outputs.
- `synthesized_family`
  This is the lower-level format when you want exact control over the generated table family.

## Two Main Workflows

### 1. One-Shot Preview

Use this when you want to check the nested result shape immediately:

```bash
exasol-json-tables structured-results preview-json \
  --result-family-config ./dist/result_family_input.json \
  --target-schema JVS_RESULT_PREVIEW \
  --table-kind local_temporary
```

That materializes the family in the current command session and prints the JSON rows directly. Treat it as a fast validation tool for the shape, not the primary durable output surface.

### 2. Durable Package

Use this when you want a reusable result family that can be installed, queried again, and handed off operationally:

```bash
exasol-json-tables structured-results package \
  --source-schema JVS_RESULT_SRC \
  --wrapper-schema JSON_VIEW_RESULT \
  --helper-schema JSON_VIEW_RESULT_INTERNAL \
  --preprocessor-schema JVS_RESULT_PP \
  --preprocessor-script JSON_RESULT_PREPROCESSOR \
  --output-dir ./dist \
  --package-name json_result \
  --result-family-config ./dist/result_family_input.json
```

This writes:

- the normal wrapper package files
- the persisted materialization config
- the materialized family manifest

Install it like any other wrapper package:

```bash
exasol-json-tables wrap install \
  --package-config ./dist/json_result_package.json
```

For result-family packages, `install` also recreates the durable source-like family before it installs the wrapper views and preprocessor.

## Primary Final Outlet: `TO_JSON`

Once a structured result is wrapped, the normal way to emit final JSON is SQL:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RESULT_PP.JSON_RESULT_PREPROCESSOR;

SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "_id";
```

If you only want selected top-level properties, keep the same wrapped root and choose them explicitly:

```sql
SELECT TO_JSON("doc_id", "items") AS doc_json
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "_id";
```

Important semantics:

- on wrapped result families, `TO_JSON(*)` recursively serializes the whole wrapped row
- `TO_JSON("field1", "field2")` keeps only the selected top-level properties and recursively serializes those branches
- on ordinary tables or views, `TO_JSON` is a flat row serializer; nested output still comes from materializing a family first
- `structured-results preview-json` and the Python exporter remain useful for preview, automation, and oracle-style validation, but they are no longer the main user-facing final outlet

## Quickstart Example

For a durable structured result, the easiest starting point is usually `structured_shape`.

Example:

```json
{
  "kind": "structured_shape",
  "rootTable": "DOC_REPORT",
  "root": {
    "fromSql": "FROM JSON_VIEW.SAMPLE",
    "idSql": "\"id\"",
    "fields": [
      {"name": "doc_id", "sql": "\"id\""},
      {"name": "items", "kind": "array_ref", "sql": "CASE WHEN \"items[SIZE]\" IS NULL THEN 0 ELSE \"items[SIZE]\" END"}
    ],
    "arrays": [
      {
        "name": "items",
        "fromSql": "FROM JSON_VIEW.SAMPLE s JOIN item IN s.\"items\"",
        "rowIdSql": "CAST((s.\"id\" * 100) + item._index + 1 AS DECIMAL(18,0))",
        "parentIdSql": "s.\"id\"",
        "positionSql": "item._index",
        "fields": [
          {"name": "label", "sql": "item.label"},
          {"name": "value", "sql": "item.value"}
        ]
      }
    ]
  }
}
```

After installation and activation, the structured result behaves like any other wrapped JSON document surface:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RESULT_PP.JSON_RESULT_PREPROCESSOR;

SELECT
  CAST("doc_id" AS VARCHAR(10)),
  COALESCE("items[FIRST].label", 'NULL'),
  COALESCE("items[LAST].value", 'NULL')
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "doc_id";
```

And when you want the final document payload:

```sql
SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "_id";
```

## Structured Results From Regular Tables

This is a common pattern: start from ordinary relational tables, then materialize a nested result family that is easier to query, emit with `TO_JSON(...)`, or hand off as document-shaped output.

For example, suppose you have upstream tables like:

- `ORDERS`
- `ORDER_ITEMS`
- `CUSTOMERS`
- `PRODUCTS`
- `ORDER_TAGS`

You can materialize them into one nested result family with either:

- `structured_shape`, which is easier to author
- `synthesized_family`, if you want explicit control over every generated table

Example root-level `structured_shape` fragment:

```json
{
  "kind": "structured_shape",
  "rootTable": "ORDER_REPORT",
  "root": {
    "fromSql": "FROM JVS_RELATIONAL_UPSTREAM.ORDERS o LEFT JOIN (SELECT ORDER_ID, COUNT(*) AS ITEM_COUNT FROM JVS_RELATIONAL_UPSTREAM.ORDER_ITEMS GROUP BY ORDER_ID) item_counts ON item_counts.ORDER_ID = o.ORDER_ID LEFT JOIN (SELECT ORDER_ID, COUNT(*) AS TAG_COUNT FROM JVS_RELATIONAL_UPSTREAM.ORDER_TAGS GROUP BY ORDER_ID) tag_counts ON tag_counts.ORDER_ID = o.ORDER_ID",
    "idSql": "o.ORDER_ID",
    "fields": [
      {"name": "order_id", "sql": "o.ORDER_ID"},
      {"name": "status", "sql": "o.STATUS"},
      {"name": "customer", "kind": "object_ref", "sql": "CAST(100000 + o.CUSTOMER_ID AS DECIMAL(18,0))"},
      {"name": "items", "kind": "array_ref", "sql": "COALESCE(item_counts.ITEM_COUNT, 0)"},
      {"name": "tags", "kind": "array_ref", "sql": "COALESCE(tag_counts.TAG_COUNT, 0)"}
    ]
  }
}
```

For a complete working example, see [tests/test_structured_result_ergonomics.py](../tests/test_structured_result_ergonomics.py).

Once packaged and installed, that relationally sourced result behaves just like any other structured result:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RELATIONAL_RESULT_PP.JSON_RELATIONAL_RESULT_PREPROCESSOR;

SELECT
  CAST("order_id" AS VARCHAR(10)),
  "customer.name",
  "items[FIRST].product.title",
  "items[LAST].sku",
  "tags[LAST]"
FROM JSON_VIEW_RELATIONAL_RESULT.ORDER_REPORT
ORDER BY "order_id";
```

For the final payload, use `TO_JSON(...)` on the wrapped result:

```sql
SELECT
  TO_JSON(*) AS full_json,
  TO_JSON("customer", "items") AS nested_subset_json
FROM JSON_VIEW_RELATIONAL_RESULT.ORDER_REPORT
ORDER BY "_id";
```

## Preview And Programmatic Export

Two secondary paths still matter:

- `structured-results preview-json` for one-shot shape validation without a durable install
- the Python exporter for programmatic export and oracle-style regression checks

If you need that programmatic path, use the export helper directly:

```python
from nano_support import connect
from result_family_json_export import export_root_family_to_json

con = connect()
rows = export_root_family_to_json(con, source_schema="JVS_RESULT_SRC", root_table="DOC_REPORT")
print(rows)
con.close()
```

The same export path works for:

- family-preserving subsets
- durable synthesized result families
- in-session wrapped local-temporary result families

## Choosing Between Durable And Session-Local

Use durable structured results when you want:

- reproducibility
- operator handoff
- further SQL modeling
- reusable nested results

Use the lower-level in-session path when the result family only needs to live inside the current session, for example on top of `LOCAL TEMPORARY` tables.

## See Also

- [tests/test_wrapper_to_json.py](../tests/test_wrapper_to_json.py)
- [tests/test_result_family_materializer.py](../tests/test_result_family_materializer.py)
- [tests/test_result_family_json_export.py](../tests/test_result_family_json_export.py)
- [tests/test_structured_results_from_relational.py](../tests/test_structured_results_from_relational.py)
- [tests/test_structured_result_ergonomics.py](../tests/test_structured_result_ergonomics.py)
- [tests/test_result_family_package_tool.py](../tests/test_result_family_package_tool.py)
