---
name: exasol-json-tables-reshape
description: Use when working on the reshape stage of Exasol JSON Tables in this repository. Covers structured results, nested output materialization, structured_shape and synthesized_family authoring, durable or in-session result families, `TO_JSON(...)` as the primary final outlet, and turning relational query output back into the Exasol JSON Tables nested contract.
---

# Exasol JSON Tables Reshape

## When To Use This Skill

Use this skill when the task is about:

- structured results
- nested output materialization
- reshaping SQL output back into the Exasol JSON Tables family contract
- `structured_shape` or `synthesized_family`
- durable or in-session result families
- `TO_JSON(*)` or `TO_JSON("field1", "field2")` as the final output surface
- shaping relational output into document-like results

Do not use this skill for raw ingest or wrapper query-surface work unless the structured-result flow depends on them directly.

## First Moves

1. Identify whether the user wants:
   - one-shot preview of nested output
   - a durable structured result package with `TO_JSON(...)` as the final outlet
2. Decide whether the authoring level should be:
   - `structured_shape` first
   - `synthesized_family` only if exact table-family control is required
3. Check whether the input is:
   - already JSON-shaped
   - or ordinary relational tables being reshaped into nested output

Start with:

- `README.md`
- `docs/structured-results.md`
- `docs/query-surface.md`
- `python/exasol_json_tables/result_family_materializer.py`
- `python/exasol_json_tables/generate_preprocessor_sql.py`
- `python/exasol_json_tables/structured_result_tool.py`
- `python/exasol_json_tables/in_session_wrapper_installer.py`
- `python/exasol_json_tables/wrapper_package_tool.py`

Most relevant tests:

- `tests/test_wrapper_to_json.py`
- `tests/test_regular_table_to_json.py`
- `tests/test_result_family_materializer.py`
- `tests/test_to_json_roundtrip_e2e.py`
- `tests/test_result_family_package_tool.py`
- `tests/test_structured_result_ergonomics.py`
- `tests/test_structured_results_from_relational.py`
- `tests/test_in_session_wrapper_installer.py`

## Mental Model

Structured results reuse the same nested contract as ingest.

That means SQL output becomes:

- one root table
- plus child tables for nested objects and arrays

Once materialized, the result can:

- be wrapped and queried again through the normal query surface
- be kept as a durable intermediate dataset
- be emitted through `TO_JSON(...)` as the primary final outlet

This is the right tool when plain SQL rows are not the final desired shape.

## Preferred Authoring Order

### 1. `structured_shape`

Use this first for common nested outputs. It is the recommended ergonomic layer.

### 2. `synthesized_family`

Use this only when the shape needs exact low-level control over the generated family tables.

## Main Workflows

### One-shot preview

Use when the user wants to see nested output immediately:

```bash
exasol-json-tables structured-results preview-json \
  --result-family-config ./dist/result_family_input.json \
  --target-schema JVS_RESULT_PREVIEW \
  --table-kind local_temporary
```

This command already returns JSON rows directly. Treat it as the quickest shape-validation path, not the main durable outlet.

### Durable package

Use when the result should become a reusable installed surface:

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

Then install it with:

```bash
exasol-json-tables wrap install \
  --package-config ./dist/json_result_package.json
```

For agented packaging flows, prefer `--json` on `structured-results package` and the follow-up wrapper lifecycle commands. Use `describe package --json` after packaging when you need a machine-readable description of the installed result surface.

After install, the default final-output path is SQL:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RESULT_PP.JSON_RESULT_PREPROCESSOR;

SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "_id";
```

## Important Guidance

### Use structured results for nested output from regular tables

Do not frame this as JSON-only functionality. It is also the main answer when users want:

- document-like output from relational tables
- nested reporting payloads
- Mongo-style shaped results
- SQL-owned shape building instead of application-side reconstruction

### Keep shape-building inside the contract

Prefer materializing to the Exasol JSON Tables family contract instead of inventing ad hoc nested-output conventions.

That keeps:

- wrapper querying
- durable packaging
- `TO_JSON(...)` as the final outlet
- preview-json and durable output on one consistent substrate

on one consistent substrate.

### Choose durable vs session-local intentionally

- durable result families are for reuse, handoff, and further SQL modeling
- session-local flows are for immediate preview or ephemeral work

## Validation

Use the smallest relevant validation:

- primary `TO_JSON(...)` wrapper outlet:
  - `python3 tests/test_wrapper_to_json.py`
- flat `TO_JSON(...)` on ordinary tables/views:
  - `python3 tests/test_regular_table_to_json.py`
- low-level family materialization:
  - `python3 tests/test_result_family_materializer.py`
- end-to-end recursive JSON correctness:
  - `python3 tests/test_to_json_roundtrip_e2e.py`
- durable package lifecycle:
  - `python3 tests/test_result_family_package_tool.py`
- ergonomic high-level authoring:
  - `python3 tests/test_structured_result_ergonomics.py`
- relational-to-nested workflow:
  - `python3 tests/test_structured_results_from_relational.py`
- session-local install flow:
  - `python3 tests/test_in_session_wrapper_installer.py`

If the task touches the unified CLI path too, also run:

- `python3 tests/test_unified_cli.py`

## Guidance For Agents

- Start with `structured_shape` unless the user clearly needs low-level control.
- Distinguish three concerns:
  - materializing the family
  - wrapping it for SQL
  - emitting final JSON with `TO_JSON(...)`
- If a user wants Mongo-like nested outputs, structured results are usually the correct answer.
- `structured-results preview-json` already emits JSON rows; use it for preview, then steer durable workflows toward `TO_JSON(...)`.
- Be explicit about whether the result family is:
  - durable
  - session-local
  - for further querying
  - for final `TO_JSON(...)`

## Current Boundaries

- Flat analytical results do not need structured results; plain SQL is still simpler there.
- Structured results are the preferred nested-output path, and the final user-facing outlet should usually be `TO_JSON(...)`.
- `TO_JSON(...)` on ordinary tables is flat; recursive nested output still depends on the wrapped family contract.
- Reshape-stage work should reuse the existing family contract instead of inventing incompatible nested-output representations.
