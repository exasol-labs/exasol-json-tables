# Testing

This page covers the executable validation surface for Exasol JSON Tables.

## Prerequisites

Install the Python test dependencies first:

```bash
python3 -m pip install -r requirements-dev.txt
```

The Nano-backed tests expect a local Exasol Nano instance on `127.0.0.1:8563` with:

- user: `sys`
- password: `exasol`

Some tests do not need Nano and only validate local packaging or module behavior.

Important: many Nano-backed tests reuse shared schemas and fixtures, so they should be run sequentially rather than in parallel.

## Packaging Surface

```bash
python3 tests/test_packaging_surface.py
```

Verifies:

- the repo defines an installable `exasol-json-tables` console script in `pyproject.toml`
- `python -m exasol_json_tables` works when the package is on `PYTHONPATH`
- the repo-local compatibility wrapper still works as a secondary entrypoint

## Rust Ingest Crate

```bash
cargo test --manifest-path crates/json_tables_ingest/Cargo.toml
```

Verifies the ingest crate unit and integration tests.

The first Cargo run may need to download dependencies from `crates.io`.

Optional Exasol-backed ingest end-to-end tests remain ignored by default:

```bash
cargo test --manifest-path crates/json_tables_ingest/Cargo.toml exasol_e2e -- --ignored
```

The ingest tests use fixtures under [crates/json_tables_ingest/tests/fixtures](../crates/json_tables_ingest/tests/fixtures).

## Unified CLI Workflow

```bash
python3 tests/test_unified_cli.py
```

Verifies:

- unified `ingest` -> `wrap generate` manifest handoff
- unified `wrap install`, `wrap deploy`, and top-level `validate`
- one-shot `ingest-and-wrap` with derived default names and per-run artifact layout
- unified `structured-results preview-json`

## Ingest Manifest Integration

```bash
python3 tests/test_ingest_manifest_integration.py
```

Verifies:

- Rust ingest emits a source-manifest JSON artifact
- wrapper package generation consumes that manifest instead of live source-schema introspection
- the installed wrapper surface still supports deep path queries on Nano

## Wrapper Surface

```bash
python3 tests/test_wrapper_surface.py
```

Verifies:

- public wrapper metadata and helper-schema shape
- dotted path and bracket access
- rowset expansion
- explicit-null helpers
- helper-based variant semantics
- deep recursive traversal

## Wrapper Package Lifecycle

```bash
python3 tests/test_wrapper_package_tool.py
```

Verifies:

- package generation
- targeted preprocessor regeneration
- installation
- installed-package validation
- end-to-end wrapper queries through the installed preprocessor

## Wrapper Error Handling

```bash
python3 tests/test_wrapper_errors.py
```

Verifies:

- malformed path and bracket syntax errors
- iterator misuse errors
- helper arity and scope errors
- generator validation errors

## Modeling And BI Behavior

```bash
python3 tests/test_wrapper_modeling.py
```

Verifies:

- nested CTE stacks with mixed helper, rowset, and deep-path logic
- stacked derived tables over projected wrapper expressions
- `UNION ALL` across multiple wrapper roots with branch-local helper semantics
- `GROUP BY` and `ORDER BY` over projected wrapper expressions
- persisted `CREATE VIEW ... AS SELECT` and `CREATE TABLE ... AS SELECT` flows
- UDF usage on iterator-local helper expressions

## Wrapper Evaluation

```bash
python3 tests/test_wrapper_evaluation.py
```

Verifies:

- wrapper helper semantics on the installed package
- built-in SQL typing behavior on wrapper views
- UDF interoperability on the wrapper surface
- additive source-DDL refresh through package regeneration, install, and validation

## Performance Study

```bash
python3 tests/study_wrapper_performance.py
```

Benchmarks:

- path traversal
- rowset iteration
- explicit-null helper queries
- helper-based variant type and extraction queries
- warm steady-state and isolated cold-start behavior

## Structured Results

### Materialization Study

```bash
python3 tests/study_structured_result_materialization.py
```

Investigates whether the existing JSON table mapping can also serve as a structured-result interchange format for:

- family-preserving filtered copies of JSON documents
- synthesized nested analytical result families
- generic JSON reconstruction from source-like result tables
- local temporary result families versus durable scratch schemas

See also [structured-result-materialization-study.md](../structured-result-materialization-study.md).

### Materializer Regression

```bash
python3 tests/test_result_family_materializer.py
```

Verifies:

- family-preserving subset materialization from helper metadata
- synthesized nested result-family materialization from declarative table specs
- re-wrapping both materialized families through the normal wrapper interface

### In-Session Installer Regression

```bash
python3 tests/test_in_session_wrapper_installer.py
```

Verifies:

- generating wrapper/helper objects from the current database session
- installing the companion preprocessor in the same session
- wrapping and querying a `LOCAL TEMPORARY` result family
- the observed cross-session query behavior while the creating session remains alive

### JSON Export Regression

```bash
python3 tests/test_result_family_json_export.py
```

Verifies:

- exporting family-preserving subsets back to nested JSON-like rows
- exporting durable synthesized result families back to nested JSON-like rows
- exporting in-session wrapped local-temporary result families back to nested JSON-like rows
- preserving scalar-array versus object-array reconstruction
- preserving numeric JSON typing for aggregated export values

### Structured Results From Relational Tables

```bash
python3 tests/test_structured_results_from_relational.py
```

Verifies:

- materializing a synthesized source-like family from plain relational SQL
- packaging and installing that family through the durable package workflow
- querying it through the wrapper surface with path, bracket, and rowset syntax
- exporting it back to nested JSON-like rows

### Structured Result Ergonomics

```bash
python3 tests/test_structured_result_ergonomics.py
```

Verifies:

- authoring structured results with the higher-level `structured_shape` config
- one-shot `preview-json` materialize-and-export workflows
- durable packaging from the same higher-level shape config

### Durable Result-Family Package

```bash
python3 tests/test_result_family_package_tool.py
```

Verifies:

- generating a durable result-family package from a materialization config
- persisting both the materialization recipe and the materialized family manifest
- recreating the durable source family during `install`
- validating the installed source family plus wrapper/preprocessor package together
