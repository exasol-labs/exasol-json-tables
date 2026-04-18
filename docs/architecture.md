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
   SQL results can be materialized back into the same source-like contract, queried again, or exported back to nested JSON-like rows.

## The Shared Table Contract

The source-like family uses a small set of structural conventions:

- `<name>|n`
  Explicit-null mask column. Distinguishes `{"field": null}` from a missing property.

- `<name>|object`
  Reference to a child object row.

- `<name>|array`
  Array length in the parent row. The array elements themselves live in a child array table.

- `_parent`
  Parent-row reference used by child array tables.

- `_pos`
  Stable array position inside the parent array.

- `_id`
  Identifier used by root/object tables and some nested rows.

Mixed scalar variants are preserved by folding one logical property across sibling columns such as:

- `value`
- `value|string`
- `value|boolean`

That contract is the seam between ingest, query, and reshape.

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
- dotted paths such as `"meta.info.note"`
- bracket access such as `"tags[LAST]"`
- rowset expansion such as `JOIN item IN s."items"`

## Reshape Layer

The reshape layer also lives in the Python package.

It can:

- materialize query output back into the source-like contract
- install wrapper surfaces over those result families
- export result families back into nested JSON-like rows

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
- reshape reuses the same contract for nested output
