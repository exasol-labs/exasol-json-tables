---
name: exasol-json-tables-query
description: Use when working on the query surface of Exasol JSON Tables in this repository. Covers wrapper views, helper schema generation, SQL preprocessor behavior, JSON helper semantics, path and array syntax, and validation of user-facing JSON-friendly SQL on the wrapper surface.
---

# Exasol JSON Tables Query

## When To Use This Skill

Use this skill when the task is about:

- the wrapper query surface
- SQL preprocessor behavior
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
- `python/exasol_json_tables/generate_preprocessor_sql.py`
- `python/exasol_json_tables/generate_wrapper_preprocessor_sql.py`
- `python/exasol_json_tables/generate_wrapper_views_sql.py`
- `python/exasol_json_tables/wrapper_schema_support.py`
- `python/exasol_json_tables/wrapper_package_tool.py`

Most important tests:

- `tests/test_wrapper_surface.py`
- `tests/test_wrapper_errors.py`
- `tests/test_wrapper_modeling.py`
- `tests/test_wrapper_package_tool.py`
- `tests/test_unified_cli.py`
- `tests/test_wrapper_variant_semantics.py`

## Mental Model

The maintained query surface is:

- public root/document views in a wrapper schema
- helper objects in a helper schema
- a scoped SQL preprocessor that rewrites JSON-friendly syntax only for allowed wrapper schemas

Users query wrapper views, not raw source/helper tables.

The preprocessor is not optional sugar from a product perspective. It is part of the supported query surface.

## Core Surface

### Helper functions

- `JSON_IS_EXPLICIT_NULL(expr)`
- `JSON_TYPEOF(expr)`
- `JSON_AS_VARCHAR(expr)`
- `JSON_AS_DECIMAL(expr)`
- `JSON_AS_BOOLEAN(expr)`

### Syntax

- dotted paths: `"meta.info.note"`
- bracket access: `"items[0]"`, `"items[FIRST]"`, `"items[LAST]"`, `"items[SIZE]"`, `"items[id]"`, `"items[?]"`
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

### Array semantics

- use `[index]` for positional access
- use `[FIRST]`, `[LAST]`for common array-relative access
- use `[SIZE]` for array length without unnesting
- use `[field_name]` for current-row dynamic selectors such as `"items[id]"`
- use `[?]` for placeholder-based dynamic selectors
- use `JOIN ... IN ...` for rowset semantics
- arbitrary SQL expressions inside brackets are intentionally rejected
- do not allow Mongo-style `"items.value"` style traversal through arrays; it should fail with guidance

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

- wrapper semantics:
  - `python3 tests/test_wrapper_surface.py`
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
- Prefer explicit `JVS-*` errors over leaking raw SQL resolution failures when misuse is predictable.
- Be careful about scope:
  - helper/path syntax should only activate on allowed wrapper schemas
  - iterator semantics differ between object-array iterators and `VALUE` iterators
- In joined queries, qualify root-document helper arguments with the root alias, for example `JSON_IS_EXPLICIT_NULL(s."note")`.
- When changing preprocessor behavior, inspect both happy-path and wrong-first-attempt ergonomics.
- Keep install/activation guidance aligned with package-tool behavior.

## Current Boundaries

- Preprocessor activation is session-local.
- Derived-table roots are still a narrower surface than direct wrapper roots.
- `VALUE` iterators intentionally do not support full JSON helper/path semantics.
- Built-in `TYPEOF(...)` and plain `CAST(...)` reflect wrapper SQL types, not the original per-row JSON type contract.
- Query-surface work should not silently change the ingest contract unless the manifest/source-family seam is explicitly part of the task.
