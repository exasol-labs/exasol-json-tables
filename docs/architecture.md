# Architecture

Exasol JSON Tables is built around one stable idea:

- represent nested JSON as a family of ordinary relational tables

Everything else in the project either:

- produces that table family
- makes it easier to query
- or turns query results back into it

## End-To-End Model

The product has three stages:

1. `Ingest`
   Raw JSON or NDJSON is scanned and converted into a source-like table family.

2. `Query`
   Wrapper views, helper schema objects, and a scoped SQL preprocessor make those tables feel like JSON again in SQL.

3. `Reshape`
   SQL results can be materialized back into the same source-like contract, queried again, and emitted as final JSON through `TO_JSON(...)`.

## The Shared Table Contract

The project does not store JSON as one opaque text column, and it does not require native nested
database types either.

Instead, it emulates JSON types through a small relational contract that every stage understands.
That contract is the seam between ingest, query, and reshape.

### Why This Contract Exists

Exasol tables are relational, but JSON documents contain:

- nested objects
- ordered arrays
- mixed-type values
- the distinction between missing and explicit `null`

The shared contract preserves those semantics by splitting one logical document family across
ordinary tables and ordinary columns.

### Core Structural Columns

The contract uses a small set of recurring markers:

- `_id`
  Stable row identifier for root tables, object tables, and some nested rows.

- `_parent`
  Reference from an array child row back to the owning parent row.

- `_pos`
  Stable zero-based position of an element inside its parent array.

- `<name>|n`
  Explicit-null mask column. This is what preserves the difference between a property that was
  present as `null` and one that was missing entirely.

- `<name>|object`
  Object link column in the parent row. It points at the `_id` of a child object table row.

- `<name>|array`
  Array marker column in the parent row. It stores the array length, while the actual elements
  live in a child array table.

### How Objects Are Emulated

A JSON object property becomes a child table plus a link in the parent row.

Example input:

```json
{"id": 1, "hours": {"mon": "9-5"}}
```

Relational shape:

- parent table columns:
  - `id`
  - `hours|object`
- child object table:
  - `_id`
  - `mon`

So the object is not flattened into one giant row and it is not serialized as text. The parent row
stores the existence and identity of the nested object, and the child table stores its fields.

That is why deep object access like `"hours.mon"` can later be rewritten as ordinary joins over a
stable relational shape.

### How Arrays Are Emulated

A JSON array property becomes:

- an array-size marker in the parent row: `<name>|array`
- a child array table containing one row per element

Example input:

```json
{"id": 1, "tags": ["coffee", "wifi"]}
```

Relational shape:

- parent table columns:
  - `id`
  - `tags|array`
- child array table columns:
  - `_parent`
  - `_pos`
  - `_value`

`_parent` preserves which document the element belongs to. `_pos` preserves array order. That is
what makes these JSON behaviors recoverable later:

- `"tags[0]"`
- `"tags[LAST]"`
- `"tags[SIZE]"`
- `JOIN VALUE tag IN row."tags"`
- final `TO_JSON(*)` reconstruction in the original order

Arrays of objects use the same idea, but the array child rows can themselves contain object links
or nested fields. Arrays of arrays work the same way again: one level becomes a child array table,
and nested array elements can point onward into deeper array tables.

### How Variants Are Emulated

JSON properties are often not type-stable across rows. A field may be a number in one document, a
string in another, and `null` in a third.

Instead of collapsing those values to text, the contract preserves them across sibling columns.

Example input:

```json
[
  {"value": 42},
  {"value": "forty-two"},
  {"value": true},
  {"value": null}
]
```

Possible relational shape:

- `value`
- `value|string`
- `value|boolean`
- `value|n`

The most common scalar type stays on the base column, and alternate scalar types get
`<name>|<type>` sibling columns.

The same grouping idea also extends to non-scalar branches. If a property is an object in some
rows or an array in others, those branches are represented through the same logical family with
markers such as `<name>|object` and `<name>|array`.

This is how the project emulates a JSON-style variant value while still keeping real SQL types:

- numbers stay numeric
- booleans stay boolean
- strings stay string
- explicit `null` stays distinguishable through `<name>|n`

The wrapper layer then turns those sibling columns back into one logical property for functions
such as `JSON_TYPEOF(...)`, `JSON_AS_VARCHAR(...)`, `JSON_AS_DECIMAL(...)`, and `TO_JSON(...)`.

### How Missing vs Explicit `null` Is Emulated

Plain SQL `NULL` is not enough to represent the JSON distinction between:

- property missing
- property present with value `null`

The contract solves that by pairing the logical column with a mask column.

Example:

- `note IS NULL` and `note|n = FALSE` means the property was missing
- `note IS NULL` and `note|n = TRUE` means the property was explicitly `null`

That distinction survives all the way through:

- ingest
- wrapper helper semantics
- structured-result materialization
- final JSON output through `TO_JSON(...)`

### Why This Matters Across The Whole Product

The same relational contract is reused in all three stages:

- ingest writes it
- query interprets it as JSON-like SQL semantics
- reshape writes it again for structured results

Because arrays, objects, variants, and explicit nulls all have stable relational encodings, the
project can round-trip between JSON-shaped data and Exasol SQL without falling back to opaque JSON
text as the primary storage model.

## Ingest Layer

The ingest layer lives in the Rust crate:

- [crates/json_tables_ingest](../crates/json_tables_ingest)

Responsibilities:

- scan JSON / NDJSON
- infer the table-family layout
- emit Parquet staging files
- optionally emit SQL DDL
- optionally upload the result directly into Exasol
- optionally emit a source-manifest JSON artifact

## Query Layer

The query layer lives in the Python package:

- [python/exasol_json_tables](../python/exasol_json_tables)

Key pieces:

- public wrapper/document views
- helper schema objects
- generated manifest JSON
- scoped SQL preprocessor

The query layer hides most raw structural columns from normal user queries and replaces them with a JSON-oriented SQL surface:

- `JSON_IS_EXPLICIT_NULL(...)`
- `JSON_TYPEOF(...)`
- `TO_JSON(*)` and `TO_JSON("field1", "field2")`
- dotted paths such as `"meta.info.note"`
- bracket access such as `"tags[LAST]"`
- rowset expansion such as `JOIN item IN s."items"`

## Reshape Layer

The reshape layer also lives in the Python package.

It can:

- materialize query output back into the source-like contract
- install wrapper surfaces over those result families
- use `TO_JSON(...)` as the primary final outlet on wrapped families
- still support secondary programmatic export back into nested JSON-like rows

That is what makes the same contract useful both as input storage and as a structured-output interchange format.

## Manifest-Aware Seam

Today the seam between ingest and query is both schema-contract based and manifest-aware:

- the query layer can introspect a live source schema and reconstruct table-family metadata
- the ingest layer can emit a source-manifest JSON artifact describing the same layout

That supports two wrapper-generation modes:

- live source-schema introspection
- source-manifest driven generation

Both remain supported. The manifest path is additive, not a replacement for introspection.

## Repository Shape

The project is implemented in two main code areas:

- Rust ingest engine: [crates/json_tables_ingest](../crates/json_tables_ingest)
- Python query/reshape package: [python/exasol_json_tables](../python/exasol_json_tables)

The installed user-facing command is:

- `exasol-json-tables`

Repo-local compatibility wrappers still exist under [tools](../tools), but they are secondary to the installed CLI and the package modules.

## Practical Consequence

If you remember only one thing, it should be this:

The project is not “an importer” plus “a wrapper.”

It is one system where:

- ingest writes the nested table contract
- query makes that contract pleasant to use
- reshape reuses the same contract for nested output and `TO_JSON(...)` turns it back into final JSON
