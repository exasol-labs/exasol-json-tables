# Flattened Views

The wrapper surface is JSON-shaped: it keeps the original property spelling and
needs an active `SQL_PREPROCESSOR_SCRIPT` before path, bracket, iterator, or
helper syntax resolves.

Some consumers cannot set session state at all. BI tools, dashboard servers,
pooled connections and generated SQL frequently open a connection they do not
control, so `ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = ...` is not available
to them. Without another surface, those consumers fall back to querying the raw
source tables and hand-quoting structural identifiers such as
`"dimensions|object"`.

The flattened views exist for exactly that case:

- ordinary Exasol views, no preprocessor, no session state
- UPPERCASE column names that are safe to type unquoted
- nested objects folded into the owning entity
- arrays kept as their own views with plain join key columns

They are **additive**. The wrapper views, the helper schema and the preprocessor
are unchanged, and remain the surface for JSON semantics such as
`JSON_TYPEOF(...)`, `JSON_IS_EXPLICIT_NULL(...)`, array iterators and
`TO_JSON(...)`.

## Where They Live

`ingest-and-wrap` derives a fourth schema next to the existing three:

| Schema | Contents |
|---|---|
| `EJT_<NAME>_SRC` | ingested source table family |
| `EJT_<NAME>_VIEW` | wrapper root views (preprocessor required) |
| `EJT_<NAME>_VIEW_INTERNAL` | helper schema |
| `EJT_<NAME>_FLAT` | **flattened views (no preprocessor)** |

For `wrap generate`, the default is the wrapper schema with a trailing `_VIEW`
removed and `_FLAT` appended. Override it with `--flat-schema`, or turn the
whole surface off with `--no-flat-views`.

## What Gets A View

One view per **entity**:

- the root document table
- every array child table

Nested **objects** do not get their own view. They are folded into the entity
that owns them through `LEFT JOIN`, so `customer.address.city` is a column on
the root view rather than a second table to join.

Arrays cannot be folded in without multiplying rows, so each array child table
becomes its own view carrying:

- `PARENT_ID` — the owning row
- `ARRAY_INDEX` — the 0-based position in the source array
- `ROW_ID` — present when the array elements themselves own nested children

## Example

For `orders.json`:

```sql
SELECT ORDER_ID, CUSTOMER_TIER, CUSTOMER_ADDRESS_CITY, PAYMENT_METHOD, TOTAL
FROM EJT_ORDERS_FLAT.ORDERS
WHERE CUSTOMER_ADDRESS_COUNTRY = 'DE';

SELECT o.ORDER_ID, i.SKU, i.QTY, i.UNIT_PRICE
FROM EJT_ORDERS_FLAT.ORDERS o
JOIN EJT_ORDERS_FLAT.ORDERS_ITEMS i ON i.PARENT_ID = o.ROW_ID
ORDER BY o.ORDER_ID, i.ARRAY_INDEX;
```

No activation SQL, no quoted identifiers.

## Column Naming Rules

A nested JSON path becomes one identifier by applying these rules in order:

1. join the path segments with `_`
2. uppercase, and replace every character that is not `A-Z`, `0-9` or `_` with `_`
3. collapse runs of `_`, then trim leading and trailing `_`
   (so a MongoDB-style `_id` property becomes `ID`)
4. fall back to `FIELD` when nothing legal is left
5. prefix `C_` when the result starts with a digit
6. append `_COL` when the result is an Exasol reserved word
7. truncate to the 128 character identifier limit

Examples:

| JSON path | Column |
|---|---|
| `order_id` | `ORDER_ID` |
| `customer.address.city` | `CUSTOMER_ADDRESS_CITY` |
| `sub-category` | `SUB_CATEGORY` |
| `payment.method` | `PAYMENT_METHOD` |
| `order` | `ORDER_COL` |
| `123abc` | `C_123ABC` |

Reserved-word avoidance only applies to the whole identifier. `method` is
reserved, `payment.method` is not, so it stays `PAYMENT_METHOD`.

### Collisions

Flattening is lossy: `sub-category`, `sub category` and `sub_category` all
normalize to `SUB_CATEGORY`, and truncation at 128 characters can collide too.
Names are allocated in a stable order — structural columns first, then the
entity's own properties in source column order, with each folded object expanded
where its property appears. The first claimant keeps the plain name; later ones
get `_2`, `_3`, ... appended, truncated so the result still fits in 128
characters.

That keeps the mapping deterministic for a given source family, but it does mean
a column name can shift if the source shape changes. Use the generated manifest
(below) rather than guessing when the mapping matters.

### Structural Columns

Source structural columns start with `_`, which Exasol does not accept unquoted,
so they are renamed:

| Source column | Flat column | Meaning |
|---|---|---|
| `_id` | `ROW_ID` | row identity, join target for array children |
| `_parent` | `PARENT_ID` | owning row of an array element |
| `_pos` | `ARRAY_INDEX` | 0-based array position |
| `_value` | `ELEMENT_VALUE` | the element of a scalar array |
| `<name>\|array` | `<NAME>_LENGTH` | number of elements in the array |
| `<name>\|object` | `<NAME>_ID` | object row identity |

`<NAME>_ID` is only emitted where a nested array actually needs it as a join
key, so folded objects do not add noise columns.

Because these names are allocated first, a JSON property that flattens onto one
of them is the side that gets the `_2` suffix.

## Variants, Nulls And Other Losses

The flat surface deliberately trades JSON fidelity for SQL ergonomics:

- **Variants** — a property stored across sibling type columns is coalesced back
  into one column, casting to `VARCHAR` when the types differ. This is the same
  projection the wrapper root view uses.
- **Missing vs explicit `null`** — not represented. Both arrive as SQL `NULL`.
  Use the wrapper surface and `JSON_IS_EXPLICIT_NULL(...)` when that distinction
  matters.
- **Document reconstruction** — not available here. Use `TO_JSON(...)` on the
  wrapper surface.

## Discovering The Mapping

The CLI prints the views and their join keys after an install:

```
Flattened views (plain SQL, no ALTER SESSION, UPPERCASE columns) in EJT_ORDERS_FLAT:
  EJT_ORDERS_FLAT.ORDERS   (root documents, 16 columns)
  EJT_ORDERS_FLAT.ORDERS_ITEMS   (array items[], 7 columns)
  EJT_ORDERS_FLAT.ORDERS_TAGS   (array tags[], 3 columns)
SELECT ROW_ID, ORDER_ID, CUSTOMER_ID, CUSTOMER_EMAIL, CUSTOMER_TIER FROM EJT_ORDERS_FLAT.ORDERS LIMIT 5;
Join keys (also in .../ORDERS.source_manifest.json):
  EJT_ORDERS_FLAT.ORDERS.ROW_ID = EJT_ORDERS_FLAT.ORDERS_ITEMS.PARENT_ID   (items[], ordered by ARRAY_INDEX)
  EJT_ORDERS_FLAT.ORDERS.ROW_ID = EJT_ORDERS_FLAT.ORDERS_TAGS.PARENT_ID   (tags[], ordered by ARRAY_INDEX)
```

Three machine-readable sources describe the same thing:

- the wrapper manifest gains a `flatSurface` block listing every entity, its
  parent link, and every column with its originating JSON path and source column
- `--json` runs of `ingest-and-wrap`, `wrap generate`, `wrap deploy` and
  `describe package` carry `objects.flatSchema`, `objects.flatViews` and
  `nextActions.joinKeys`
- the database catalog itself: each view carries a `COMMENT`, and each column's
  `COMMENT` is its original JSON path, so
  `SELECT COLUMN_NAME, COLUMN_COMMENT FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'EJT_ORDERS_FLAT'`
  is a complete mapping

## Refreshing

The flat views are generated as part of the wrapper package, so they are
refreshed the same way: regenerate and reinstall the package. Installing drops
and recreates the flat schema, so it always matches the manifest it was
generated from.
