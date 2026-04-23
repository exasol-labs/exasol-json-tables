# Python DataFrames

Use this page when you want wrapper data in pandas, polars, or a notebook-style Python workflow.

## Two Safe Patterns

### 1. Interactive Wrapper Queries

For ad hoc exploration, activate the wrapper preprocessor and run the wrapper query directly:

```python
import pandas as pd

con.execute('ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR')

stmt = con.execute("""
    SELECT
      CAST("id" AS VARCHAR(20)) AS DOC_ID,
      CAST("meta.info.note" AS VARCHAR(100)) AS DEEP_NOTE,
      CAST("items[LAST].label" AS VARCHAR(100)) AS LAST_ITEM_LABEL
    FROM "JSON_VIEW"."SAMPLE"
    ORDER BY "_id"
""")

columns = list(stmt.columns())
df = pd.DataFrame(stmt.fetchall(), columns=columns)
```

Why this is the default:

- wrapper syntax such as dotted paths, bracket access, and iterators depends on the session preprocessor
- `execute()` exposes the real query error directly if the SQL is invalid
- explicit `CAST(...) AS UPPERCASE_ALIAS` keeps dataframe columns stable and easy to reference later

### 2. Durable Export Surfaces

If the query should be reused from notebooks, BI tools, or later sessions, publish an ordinary view or table first. Create it in a session where the wrapper preprocessor is already active:

```sql
CREATE VIEW ANALYTICS.SAMPLE_EXPORT AS
SELECT
  CAST("id" AS VARCHAR(20)) AS DOC_ID,
  CAST("meta.info.note" AS VARCHAR(100)) AS DEEP_NOTE,
  CAST("items[LAST].label" AS VARCHAR(100)) AS LAST_ITEM_LABEL
FROM "JSON_VIEW"."SAMPLE";
```

Then downstream Python can use ordinary SQL without any wrapper activation:

```python
df = con.export_to_pandas("""
    SELECT DOC_ID, DEEP_NOTE, LAST_ITEM_LABEL
    FROM ANALYTICS.SAMPLE_EXPORT
    ORDER BY DOC_ID
""")
```

This is the best path when you want `export_to_pandas()` specifically.

## `export_to_pandas()` On Raw Wrapper Queries

PyExasol implements `export_to_pandas()` by wrapping your SQL in `EXPORT (...) INTO CSV`.

On the current PyExasol `2.2.0` stack:

- simple root-wrapper queries can work if the wrapper preprocessor is already active
- iterator-heavy wrapper syntax such as `JOIN item IN s."items"` is still unreliable there
- when it fails, the visible Python error is often an `EmptyDataError` wrapped inside `ExaExportError`, which hides the real SQL problem

So the practical rule is:

- use `execute()` for wrapper queries
- use `export_to_pandas()` for ordinary published views/tables

## Column Naming

For dataframe-oriented outputs:

- cast wrapper expressions to explicit scalar SQL types
- assign uppercase SQL-safe aliases
- avoid reserved words as durable column names

Example:

```sql
SELECT
  CAST("sample_id" AS VARCHAR(40)) AS SAMPLE_ID,
  CAST("measurements[LAST].value" AS DECIMAL(18,4)) AS LAST_MEASUREMENT_VALUE
FROM "JSON_VIEW"."EXPERIMENTS";
```

That keeps later Python, SQL, and BI usage predictable.

## `value` Fields

If the source JSON contains an object-array field named `value`, query it on the wrapper surface as `"value"`:

```sql
SELECT
  m."timepoint",
  m."value",
  m."unit"
FROM "JSON_VIEW"."EXPERIMENTS" e
JOIN m IN e."measurements";
```

The helper schema may still store that physical column as `_value`, but the maintained wrapper surface translates it back to logical `value` for user queries.

For durable exported objects, still prefer an explicit alias such as `MEASUREMENT_VALUE` or `VALUE_NUM`.

## Polars Note

The same SQL guidance applies to polars:

- execute wrapper queries directly when you need wrapper syntax
- publish ordinary views/tables when you want a durable dataframe surface
- use explicit casts and stable aliases either way
