---
name: exasol-json-tables-query
description: Use when working on the query surface of Exasol JSON Tables in this repository. Covers wrapper views, helper schema generation, SQL preprocessor behavior, `TO_JSON(...)`, JSON helper semantics, path and array syntax, and validation of user-facing JSON-friendly SQL on the wrapper surface.
---

# Exasol JSON Tables Query

## When To Use This Skill

Use this skill when the task is about:

- the wrapper query surface
- SQL preprocessor behavior
- `TO_JSON(...)` on wrapper roots or ordinary tables/views
- dotted paths, bracket access, and array rowset syntax
- explicit-null and variant helper semantics
- wrapper package generation, install, deploy, or validate
- debugging why a query works or fails on the wrapper surface

Do not use this skill for ingest-contract changes or structured-result authoring. Use the ingest or rewrite skills for those.

## First Moves

1. Identify whether the issue is:
   - wrapper generation
   - preprocessor rewrite behavior
   - helper semantics
   - session activation / install flow
   - user-facing diagnostics
2. Check whether the failing query is running:
   - directly against the wrapper view
   - through a CTE / derived table / modeling object
   - through an iterator alias
3. Confirm whether the session has the preprocessor activated.

Start with:

- `README.md`
- `docs/query-surface.md`
- `docs/installation.md`
- `docs/identifier-conventions.md`
- `python/exasol_json_tables/generate_preprocessor_sql.py`
- `python/exasol_json_tables/generate_wrapper_preprocessor_sql.py`
- `python/exasol_json_tables/generate_wrapper_views_sql.py`
- `python/exasol_json_tables/wrapper_schema_support.py`
- `python/exasol_json_tables/wrapper_package_tool.py`

Most important tests:

- `tests/test_preprocessor_library_builder.py`
- `tools/test_nano_preprocessor_parser_lane.py`
- `tests/test_preprocessor_refactor_phase0.py`
- `tests/test_preprocessor_early_out.py`
- `tests/test_wrapper_surface.py`
- `tests/test_wrapper_to_json.py`
- `tests/test_regular_table_to_json.py`
- `tests/test_wrapper_errors.py`
- `tests/test_wrapper_modeling.py`
- `tests/test_wrapper_package_tool.py`
- `tests/test_unified_cli.py`
- `tests/test_wrapper_variant_semantics.py`

For agented workflow commands such as `ingest-and-wrap`, `wrap generate`, `wrap install`, `wrap deploy`, and `validate`, prefer `--json`. The JSON summary includes package paths, activation SQL, smoke-test SQL, schema names, wrapper-scope warnings, and installed validation probes. Use `describe package --json` or `describe wrapper --json` when you need to inspect the available roots and example queries. `describe wrapper --json` now also includes recursive `fieldTree` data and per-root `familyTables`, so agents should use that instead of guessing nested field names.

## Mental Model

The maintained query surface is:

- public root/document views in a wrapper schema
- helper objects in a helper schema
- a scoped SQL preprocessor that rewrites JSON-friendly syntax only for allowed wrapper schemas

Users query wrapper views, not raw source/helper tables.

The preprocessor is not optional sugar from a product perspective. It is part of the supported query surface.

`TO_JSON(...)` is part of that supported surface. It is now the primary way users get final JSON back out of wrapper-root queries.

## Core Surface

### Helper functions

- `JSON_IS_EXPLICIT_NULL(expr)`
- `JSON_TYPEOF(expr)`
- `JSON_AS_VARCHAR(expr)`
- `JSON_AS_DECIMAL(expr)`
- `JSON_AS_BOOLEAN(expr)`

### Final-output function

- `TO_JSON(*)`
- `TO_JSON(col1, col2, ...)`

### Syntax

- dotted paths: `"meta.info.note"`
- bracket access: `"items[0]"`, `"items[FIRST]"`, `"items[LAST]"`, `"items[SIZE]"`, `"items[id]"`, `"items[?]"`, `"items[PARAM]"`
- mixed deep access: `"meta.items[LAST].value"`
- rowset expansion: `JOIN item IN s."items"`
- scalar-array expansion: `JOIN VALUE tag IN s."tags"`
- iterator-rooted paths/brackets on object-array iterators

## Important Semantics

### Missing vs explicit null

- `JSON_IS_EXPLICIT_NULL(...)` distinguishes explicit JSON `null` from missing values
- plain `IS NULL` alone does not

### Variant semantics

- use `JSON_TYPEOF(...)` and `JSON_AS_*`
- built-in `TYPEOF(...)` and plain `CAST(...)` reflect wrapper SQL types, not per-row JSON type contract
- use `JSON_TYPEOF(...)` mainly on variant-style fields where the per-row JSON type can change
- for structural wrapper branches backed by fixed object/array markers, traverse them, expand them, or serialize them with `TO_JSON(...)` instead of treating them like late-bound runtime `VARIANT`

### Array semantics

- use `[index]` for positional access
- use `[FIRST]`, `[LAST]`for common array-relative access
- use `[SIZE]` for array length without unnesting
- use `[field_name]` for current-row dynamic selectors such as `"items[id]"`
- use `[?]` for placeholder-based dynamic selectors
- treat `[PARAM]` as a prepared-statement-only spelling; plain Python string execution does not make it work
- use `JOIN ... IN ...` for rowset semantics
- arbitrary SQL expressions inside brackets are intentionally rejected
- do not allow Mongo-style `"items.value"` style traversal through arrays; it should fail with guidance

### `TO_JSON(...)` semantics

- on wrapper roots, `TO_JSON(*)` recursively serializes the whole row
- on wrapper roots, `TO_JSON("field1", "field2")` recursively serializes only the selected top-level branches
- in joined wrapper queries, require qualified top-level subset arguments such as `TO_JSON(s."id", s."meta")`
- on ordinary tables or ordinary views, `TO_JSON` is a flat row serializer
- on contract-encoded source-family tables, `TO_JSON(*)` is intentionally rejected; use the wrapper root instead
- in joined ordinary-table queries, prefer `TO_JSON(alias.*)` or qualified columns such as `TO_JSON(s."id", s."name")`
- derived-table sources are still unsupported
- nested paths or bracket expressions inside `TO_JSON(...)` subset arguments are unsupported

## First Things To Verify In A Bug

1. Is the query rooted in an allowed wrapper schema?
2. Is the preprocessor active in the session?
3. Is the user applying helper/path syntax to:
   - a wrapper root
   - an object-array iterator
   - a scalar VALUE iterator
   - a derived-table root
4. Is this a user error that should produce a `JVS-*` message rather than a raw Exasol error?

## Validation

Use these as the main regression surface:

- dedicated parser-heavy lane:
  - `python3 tools/test_nano_preprocessor_parser_lane.py`
- preprocessor parser/stability baseline:
  - `python3 tests/test_preprocessor_refactor_phase0.py`
- wrapper semantics:
  - `python3 tests/test_wrapper_surface.py`
- wrapper `TO_JSON(...)` semantics:
  - `python3 tests/test_wrapper_to_json.py`
- regular-table `TO_JSON(...)` semantics:
  - `python3 tests/test_regular_table_to_json.py`
- error quality:
  - `python3 tests/test_wrapper_errors.py`
- modeling shapes:
  - `python3 tests/test_wrapper_modeling.py`
- install/deploy lifecycle:
  - `python3 tests/test_wrapper_package_tool.py`
- unified CLI flow:
  - `python3 tests/test_unified_cli.py`
- variant edge cases:
  - `python3 tests/test_wrapper_variant_semantics.py`

Run Nano-backed tests sequentially when they rebuild the same schemas.

## Guidance For Agents

- Treat the wrapper surface as a product contract, not an internal convenience layer.
- Treat `TO_JSON(...)` as part of that product contract, not as a secondary helper.
- Keep wrapper property references quoted exactly as required by the surface, for example `"meta.info.note"` or `item."nested.value"`.
- When generating a durable published view or export table for downstream SQL, prefer uppercase SQL-safe aliases by default.
- Avoid reserved-word aliases such as `source`, `schema`, `value`, `type`, `table`, or `timestamp` unless the user explicitly wants quoted identifiers.
- If an alias would naturally collide with a reserved word, pick a descriptive replacement such as `SOURCE_SITE`, `VALUE_TEXT`, `EVENT_TYPE`, or `ORDER_TS`.
- Separate SQL-facing alias ergonomics from JSON payload ergonomics: use uppercase aliases for durable SQL objects, but keep natural property names inside `TO_JSON(...)` output.
- Prefer explicit `JVS-*` errors over leaking raw SQL resolution failures when misuse is predictable.
- Be careful about scope:
  - helper/path syntax should only activate on allowed wrapper schemas
  - iterator semantics differ between object-array iterators and `VALUE` iterators
- In joined queries, qualify root-document helper arguments with the root alias, for example `JSON_IS_EXPLICIT_NULL(s."note")`.
- For joined wrapper-root `TO_JSON(...)`, qualify top-level arguments with the root alias.
- When changing preprocessor behavior, inspect both happy-path and wrong-first-attempt ergonomics.
- Keep install/activation guidance aligned with package-tool behavior.
- Prefer `--json` for wrapper lifecycle commands instead of scraping printed next-step text.
- Treat `validate --json` as the authoritative capability signal for automation, not a generic green/no-green string.
- When a user complains about quoting friction, first decide whether they need:
  - a wrapper query in the current session
  - or a durable published SQL object
  The fix is often alias strategy, not query-surface behavior.

## Current Boundaries

- Preprocessor activation is session-local.
- Derived-table roots are still a narrower surface than direct wrapper roots.
- Joined wrapper-root `TO_JSON(*)` is not supported; use qualified top-level subsets instead.
- `TO_JSON(alias.*)` is for ordinary tables/views, not the recursive wrapper-root path.
- `VALUE` iterators intentionally do not support full JSON helper/path semantics.
- Built-in `TYPEOF(...)` and plain `CAST(...)` reflect wrapper SQL types, not the original per-row JSON type contract.
- Query-surface work should not silently change the ingest contract unless the manifest/source-family seam is explicitly part of the task.
