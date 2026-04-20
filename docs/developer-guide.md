# Developer Guide

This page is the map of the codebase for contributors and advanced users.

## Repository Shape

The project is implemented in two main areas:

- Rust ingest engine: [crates/json_tables_ingest](../crates/json_tables_ingest)
- Python query/reshape package: [python/exasol_json_tables](../python/exasol_json_tables)

The supported user-facing entrypoint is:

- `exasol-json-tables`

That command is provided by the package metadata in [pyproject.toml](../pyproject.toml).

Repo-local wrappers still exist under [tools](../tools), but they are compatibility and developer entrypoints rather than the primary product surface.
They are intentionally thin wrappers over the package modules, not a second implementation surface.

## Main Implementation Files

- ingest crate root: [crates/json_tables_ingest](../crates/json_tables_ingest)
- ingest library: [crates/json_tables_ingest/src/lib.rs](../crates/json_tables_ingest/src/lib.rs)
- ingest CLI entrypoint: [crates/json_tables_ingest/src/main.rs](../crates/json_tables_ingest/src/main.rs)
- Python package root: [python/exasol_json_tables](../python/exasol_json_tables)
- unified CLI orchestration layer: [python/exasol_json_tables/cli.py](../python/exasol_json_tables/cli.py)
- package metadata / console-script entrypoint: [pyproject.toml](../pyproject.toml)
- wrapper package tool: [python/exasol_json_tables/wrapper_package_tool.py](../python/exasol_json_tables/wrapper_package_tool.py)
- wrapper SQL generator: [python/exasol_json_tables/generate_wrapper_views_sql.py](../python/exasol_json_tables/generate_wrapper_views_sql.py)
- wrapper preprocessor generator: [python/exasol_json_tables/generate_wrapper_preprocessor_sql.py](../python/exasol_json_tables/generate_wrapper_preprocessor_sql.py)
- shared wrapper manifest and generation logic: [python/exasol_json_tables/wrapper_schema_support.py](../python/exasol_json_tables/wrapper_schema_support.py)
- shared preprocessor engine: [python/exasol_json_tables/generate_preprocessor_sql.py](../python/exasol_json_tables/generate_preprocessor_sql.py)
- JSON export helper generator for `TO_JSON(...)`: [python/exasol_json_tables/generate_json_export_helper_sql.py](../python/exasol_json_tables/generate_json_export_helper_sql.py)
- hidden export-view generator for recursive `TO_JSON(...)`: [python/exasol_json_tables/generate_json_export_views_sql.py](../python/exasol_json_tables/generate_json_export_views_sql.py)
- structured result-family materializer: [python/exasol_json_tables/result_family_materializer.py](../python/exasol_json_tables/result_family_materializer.py)
- structured result preview/export CLI logic: [python/exasol_json_tables/structured_result_tool.py](../python/exasol_json_tables/structured_result_tool.py)
- in-session wrapper installer: [python/exasol_json_tables/in_session_wrapper_installer.py](../python/exasol_json_tables/in_session_wrapper_installer.py)
- Nano fixture helpers: [python/exasol_json_tables/nano_support.py](../python/exasol_json_tables/nano_support.py)
- compatibility CLI wrappers and developer glue: [tools](../tools)
- executable regressions and studies: [tests](../tests)

## Generated Artifacts

The wrapper package generator produces four main artifacts:

- wrapper SQL
  Public root views plus helper schema objects.

- manifest JSON
  Machine-readable description of roots, tables, relationships, and folded column families.

- preprocessor SQL
  The scoped wrapper preprocessor.

- package config JSON
  The reproducible control-plane artifact for generation, install, validate, and regenerate flows.

The ingest layer can also emit a separate source-manifest JSON artifact that wrapper generation can consume directly.

## Default Generated Wrapper Artifacts

These files are generated on demand and are not checked into git:

- `dist/exasol-json-tables/json_wrapper_views.sql`
- `dist/exasol-json-tables/json_wrapper_manifest.json`
- `dist/exasol-json-tables/json_wrapper_preprocessor.sql`
- `dist/exasol-json-tables/json_wrapper_package.json`

## Supported Product Surface Vs Internal Surface

### Supported Product Surface

- installed CLI: `exasol-json-tables`
- wrapper package outputs
- documented SQL query surface
- documented `TO_JSON(...)` final-output surface
- documented structured-results workflow

### Internal Or Compatibility Surface

- repo-local `tools/` wrappers
- lower-level helper modules used by tests and package generation
- historical or study-oriented documentation under `plans/` and `user-studies/`

## Practical Navigation

If you are trying to understand:

- how JSON gets ingested into the table contract
  Start with [crates/json_tables_ingest/src/lib.rs](../crates/json_tables_ingest/src/lib.rs)

- how the wrapper package is generated
  Start with [python/exasol_json_tables/wrapper_package_tool.py](../python/exasol_json_tables/wrapper_package_tool.py)

- how the SQL surface is rewritten
  Start with [python/exasol_json_tables/generate_preprocessor_sql.py](../python/exasol_json_tables/generate_preprocessor_sql.py), [tests/test_preprocessor_refactor_phase0.py](../tests/test_preprocessor_refactor_phase0.py), and the focused parser lane [tools/test_nano_preprocessor_parser_lane.py](../tools/test_nano_preprocessor_parser_lane.py)

- how final JSON output is generated
  Start with [python/exasol_json_tables/generate_preprocessor_sql.py](../python/exasol_json_tables/generate_preprocessor_sql.py), [python/exasol_json_tables/generate_json_export_helper_sql.py](../python/exasol_json_tables/generate_json_export_helper_sql.py), and [python/exasol_json_tables/generate_json_export_views_sql.py](../python/exasol_json_tables/generate_json_export_views_sql.py)

- how structured results are materialized and then surfaced through `TO_JSON(...)`
  Start with [python/exasol_json_tables/result_family_materializer.py](../python/exasol_json_tables/result_family_materializer.py) and [python/exasol_json_tables/in_session_wrapper_installer.py](../python/exasol_json_tables/in_session_wrapper_installer.py)

- how JSON correctness is verified end to end
  Start with [tests/test_to_json_roundtrip_e2e.py](../tests/test_to_json_roundtrip_e2e.py), [tests/test_wrapper_to_json.py](../tests/test_wrapper_to_json.py), and [tests/test_json_export_views_sql.py](../tests/test_json_export_views_sql.py)

- how the end-to-end user workflow is orchestrated
  Start with [python/exasol_json_tables/cli.py](../python/exasol_json_tables/cli.py)
