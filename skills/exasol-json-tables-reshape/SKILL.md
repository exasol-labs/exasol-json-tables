---
name: exasol-json-tables-reshape
description: Use when working on the reshape stage of Exasol JSON Tables in this repository. Covers structured results, nested output materialization, structured_shape and synthesized_family authoring, durable or in-session result families, JSON-like export, and turning relational query output back into the Exasol JSON Tables nested contract.
---

# Exasol JSON Tables Reshape

## When To Use This Skill

Use this skill when the task is about:

- structured results
- nested output materialization
- reshaping SQL output back into the Exasol JSON Tables family contract
- `structured_shape` or `synthesized_family`
- durable or in-session result families
- exporting result families back to nested JSON-like rows
- shaping relational output into document-like results

Do not use this skill for raw ingest or wrapper query-surface work unless the structured-result flow depends on them directly.

## First Moves

1. Identify whether the user wants:
   - one-shot preview of nested output
   - a durable structured result package
   - programmatic export back to JSON-like rows
2. Decide whether the authoring level should be:
   - `structured_shape` first
   - `synthesized_family` only if exact table-family control is required
3. Check whether the input is:
   - already JSON-shaped
   - or ordinary relational tables being reshaped into nested output

Start with:

- `README.md`
- `docs/structured-results.md`
- `python/exasol_json_tables/result_family_materializer.py`
- `python/exasol_json_tables/result_family_json_export.py`
- `python/exasol_json_tables/structured_result_tool.py`
- `python/exasol_json_tables/in_session_wrapper_installer.py`
- `python/exasol_json_tables/wrapper_package_tool.py`

Most relevant tests:

- `tests/test_result_family_materializer.py`
- `tests/test_result_family_json_export.py`
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
- be exported back to nested JSON-like rows

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

This command already returns JSON rows directly. Do not expect a separate summary envelope unless a higher-level wrapper adds one.

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

For agented packaging flows, prefer `--json` on `structured-results package` and the follow-up wrapper lifecycle commands.

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
- nested export

on one consistent substrate.

### Choose durable vs session-local intentionally

- durable result families are for reuse, handoff, and further SQL modeling
- session-local flows are for immediate preview or ephemeral work

## Validation

Use the smallest relevant validation:

- low-level family materialization:
  - `python3 tests/test_result_family_materializer.py`
- nested export:
  - `python3 tests/test_result_family_json_export.py`
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
  - optionally wrapping it for SQL
  - exporting it back to nested JSON-like rows
- If a user wants Mongo-like nested outputs, structured results are usually the correct answer.
- `structured-results preview-json` already emits JSON rows; use `--json` on the packaging / wrapper lifecycle commands around it.
- Be explicit about whether the result family is:
  - durable
  - session-local
  - for further querying
  - for final export

## Current Boundaries

- Flat analytical results do not need structured results; plain SQL is still simpler there.
- Structured results are the preferred nested-output path, but they are a materialization workflow, not a single-statement magic output mode.
- Reshape-stage work should reuse the existing family contract instead of inventing incompatible nested-output representations.
