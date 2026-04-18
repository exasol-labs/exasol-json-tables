# Structured Results

The same mapping is also useful in the other direction.

Instead of only querying existing JSON documents, you can materialize a query result back into a
source-like table family and then use it as:

- a nested result that can be queried again through the wrapper surface
- a durable scratch-layer intermediate for downstream SQL
- a shape that can be exported back to JSON-like rows later

The easiest mental model is:

- ordinary wrapper views make JSON-shaped source data easy to query
- structured results let you take SQL output and put it back into that same JSON-shaped contract

You do not need JSON-shaped input for this.

Structured results also work well when your source is plain relational tables such as:

- `orders`
- `order_items`
- `customers`
- `products`

In that case, the structured-result family becomes the layer that turns relational joins back into
a nested document-shaped output.

If the result is nested, it will usually become:

- one root table
- plus child tables for nested objects and arrays

That makes this useful for two different workflows:

- reshape JSON-derived tables into a new nested result
- produce nested document-style output from ordinary relational analytics models

There are now two authoring levels for structured results:

- `structured_shape`: a higher-level nested shape config for common cases
- `synthesized_family`: the lower-level table-family config when you need exact control

## Structured Results Quickstart

For a durable structured result, the easiest starting point is now a `structured_shape` config.
This describes the nested output shape directly and compiles down to the same source-like family contract.

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

If you want the nested result immediately, without first creating a durable package, use the one-shot
preview command:

```bash
python3 tools/structured_result_tool.py preview-json \
  --result-family-config ./dist/result_family_input.json \
  --target-schema JVS_RESULT_PREVIEW \
  --table-kind local_temporary
```

That materializes the family in the current command session and prints the nested JSON-like rows directly.

Then package that durable result family:

```bash
python3 tools/wrapper_package_tool.py generate-result-family-package \
  --source-schema JVS_RESULT_SRC \
  --wrapper-schema JSON_VIEW_RESULT \
  --helper-schema JSON_VIEW_RESULT_INTERNAL \
  --preprocessor-schema JVS_RESULT_PP \
  --preprocessor-script JSON_RESULT_PREPROCESSOR \
  --output-dir ./dist \
  --package-name json_result \
  --result-family-config ./dist/result_family_input.json
```

This writes both the normal wrapper package files and two result-family-specific artifacts:

- the persisted materialization config
- the materialized family manifest

Install it the same way as a normal wrapper package:

```bash
python3 tools/wrapper_package_tool.py install \
  --package-config ./dist/json_result_package.json
```

For result-family packages, `install` also recreates the durable source-like family before it
installs the wrapper views and preprocessor.

If you need exact control over the physical table family, you can still use the lower-level
`synthesized_family` format with explicit `tableSpecs`.

After activation, the structured result behaves like any other wrapped JSON document surface:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RESULT_PP.JSON_RESULT_PREPROCESSOR;

SELECT
  CAST("doc_id" AS VARCHAR(10)),
  COALESCE("items[FIRST].label", 'NULL'),
  COALESCE("items[LAST].value", 'NULL')
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "doc_id";
```

And if you want to turn that source-like family back into nested JSON-like rows, use the export helper
programmatically:

```python
from nano_support import connect
from result_family_json_export import export_root_family_to_json

con = connect()
rows = export_root_family_to_json(con, source_schema="JVS_RESULT_SRC", root_table="DOC_REPORT")
print(rows)
con.close()
```

Use durable structured results when you want reproducibility, operator handoff, or further SQL modeling.
Use the lower-level in-session installer when the result family only needs to live inside the current session,
for example on top of `LOCAL TEMPORARY` tables.

## Structured Results From Regular Tables

This is a very common pattern: start from ordinary relational tables, then materialize a nested result family
that is easier to query, export, or hand off as document-shaped output.

For example, suppose you have upstream tables like:

- `JVS_RELATIONAL_UPSTREAM.ORDERS`
- `JVS_RELATIONAL_UPSTREAM.ORDER_ITEMS`
- `JVS_RELATIONAL_UPSTREAM.CUSTOMERS`
- `JVS_RELATIONAL_UPSTREAM.PRODUCTS`
- `JVS_RELATIONAL_UPSTREAM.ORDER_TAGS`

You can materialize them into one nested result family with either:

- a `structured_shape` config, which is easier to author
- or a lower-level `synthesized_family` config, if you want to control every generated table

For example, here is a higher-level `structured_shape` version:

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

The full shape config also defines the nested `customer`, `items`, and `tags` child nodes. The compact example
above is only showing the root-level part of the shape. For a complete working example, see
[tests/test_structured_result_ergonomics.py](../tests/test_structured_result_ergonomics.py).

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

And it exports back to nested JSON-like rows through the same export helper:

```python
from nano_support import connect
from result_family_json_export import export_root_family_to_json

con = connect()
rows = export_root_family_to_json(con, source_schema="JVS_RELATIONAL_RESULT_SRC", root_table="ORDER_REPORT")
print(rows)
con.close()
```

Durable structured-result families can use the same package tool. Provide a materialization config and the tool will:

- materialize the durable source-like result family into the target source schema
- write the materialization recipe and materialized family manifest into the package
- generate the wrapper/helper/preprocessor artifacts on top of that durable result family

Example:

```bash
python3 tools/wrapper_package_tool.py generate-result-family-package \
  --source-schema JVS_RESULT_SRC \
  --wrapper-schema JSON_VIEW_RESULT \
  --helper-schema JSON_VIEW_RESULT_INTERNAL \
  --preprocessor-schema JVS_RESULT_PP \
  --preprocessor-script JSON_RESULT_PREPROCESSOR \
  --output-dir ./dist \
  --package-name json_result \
  --result-family-config ./dist/result_family_input.json
```

The generated package can then be installed and validated with the same `install` and `validate` commands. For durable result-family packages, `install` recreates the packaged source family before installing the wrapper artifacts.

See also:

- [structured-result-materialization-study.md](../structured-result-materialization-study.md)
- [tests/test_result_family_materializer.py](../tests/test_result_family_materializer.py)
- [tests/test_result_family_json_export.py](../tests/test_result_family_json_export.py)
- [tests/test_structured_results_from_relational.py](../tests/test_structured_results_from_relational.py)
- [tests/test_structured_result_ergonomics.py](../tests/test_structured_result_ergonomics.py)
- [tests/test_result_family_package_tool.py](../tests/test_result_family_package_tool.py)
