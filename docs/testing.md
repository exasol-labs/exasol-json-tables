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
- machine-readable `--json` summaries, validation reports, and failure envelopes
- package and installed-wrapper discovery through `describe ... --json`
- helper-schema autodiscovery for installed wrappers
- installed wrapper inventory through `describe wrappers --json`
- structured ingest error-code classification
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

## Preprocessor Refactor Baseline

```bash
python3 tests/test_preprocessor_refactor_phase0.py
```

Verifies:

- parser-heavy baseline behavior before preprocessor refactors
- comments and string literals that contain JSON-surface syntax
- CTE query-block rewriting
- top-level `UNION ALL` rewriting
- nested-subquery `TO_JSON(*)` rewriting
- generated generic, shared-library, and wrapper preprocessor artifact sizes against recorded guard bands

## Preprocessor Parser Lane

```bash
python3 tools/test_nano_preprocessor_parser_lane.py
```

Use this as the dedicated parser-heavy regression lane before and during preprocessor/parser refactors.

It runs the parser-sensitive Nano-backed tests sequentially:

- `tests/test_preprocessor_refactor_phase0.py`
- `tests/test_wrapper_errors.py`
- `tests/test_wrapper_to_json.py`
- `tests/test_wrapper_surface.py`

This lane exists so parser-oriented coverage stays explicit instead of being scattered across unrelated wrapper validations.

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
- installed-package `TO_JSON(...)` behavior

## Wrapper `TO_JSON` Surface

```bash
python3 tests/test_wrapper_to_json.py
```

Verifies:

- recursive `TO_JSON(*)` on wrapper roots
- selected top-level subset export such as `TO_JSON("meta", "items")`
- joined wrapper-root subset export such as `TO_JSON(s."id", s."meta")`
- repeated `TO_JSON(...)` calls in one query block
- wrong-shape errors such as joined `TO_JSON(*)` or nested-path arguments

## Regular-Table `TO_JSON` Surface

```bash
python3 tests/test_regular_table_to_json.py
```

Verifies:

- flat `TO_JSON(*)` on ordinary tables and ordinary views
- joined `TO_JSON(alias.*)` on ordinary tables
- selected-column export such as `TO_JSON("id", "name")`
- derived-table and unsupported-scope errors

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

## Access Modes

```bash
python3 tests/test_access_modes.py
```

Verifies:

- connection-bootstrap activation on a fresh session
- authoring a published view from a wrapper query
- querying that published view later without any preprocessor activation

## Local Benchmark Harnesses

Some larger exploratory studies are intentionally kept out of the tracked `tests/` suite.
If your local workspace includes the ignored `benchmarks/` folder, the main study entrypoints are:

```bash
python3 benchmarks/study_wrapper_performance.py
python3 benchmarks/study_structured_result_materialization.py
```

These local-only studies benchmark or investigate:

- path traversal
- rowset iteration
- explicit-null helper queries
- helper-based variant type and extraction queries
- warm steady-state and isolated cold-start behavior
- structured-result materialization and JSON reconstruction flows

## Structured Results

### Materializer Regression

```bash
python3 tests/test_result_family_materializer.py
```

For deeper local experimentation beyond the tracked regression, see the optional `benchmarks/study_structured_result_materialization.py` harness when present.

Verifies:

- family-preserving subset materialization from helper metadata
- synthesized nested result-family materialization from declarative table specs
- supported config/materialization serialization helpers
- re-wrapping both materialized families through the normal wrapper interface

### In-Session Installer Regression

```bash
python3 tests/test_in_session_wrapper_installer.py
```

Verifies:

- generating wrapper/helper objects from the current database session
- installing the companion preprocessor in the same session
- wrapping and querying a `LOCAL TEMPORARY` result family
- using `TO_JSON(*)` on the in-session wrapped result family
- the observed cross-session query behavior while the creating session remains alive

### TO_JSON Roundtrip E2E

```bash
python3 tests/test_to_json_roundtrip_e2e.py
```

Verifies:

- ingest-and-wrap on real complex fixture files
- wrapped `TO_JSON(*)` roundtripping back to the original JSON documents
- null, array, and object preservation across the full table-family contract
- end-to-end JSON correctness without relying on a separate Python serializer

### Structured Results From Relational Tables

```bash
python3 tests/test_structured_results_from_relational.py
```

Verifies:

- materializing a synthesized source-like family from plain relational SQL
- packaging and installing that family through the durable package workflow
- querying it through the wrapper surface with path, bracket, and rowset syntax
- emitting nested JSON through `TO_JSON(*)` and subset forms on the wrapped relational result

### Structured Result Ergonomics

```bash
python3 tests/test_structured_result_ergonomics.py
```

Verifies:

- authoring structured results with the higher-level `structured_shape` config
- validating config-first structured-result specs before materialization
- one-shot `preview-json` materialize-and-preview workflows
- parity between `preview-json` and direct wrapped `TO_JSON(*)` output
- durable packaging from the same higher-level shape config

### Quickstart To Structured Result

```bash
python3 tests/test_quickstart_structured_result_flow.py
```

Verifies:

- the one-shot `ingest-and-wrap` quickstart on a real JSON fixture
- helper and rowset queries on the generated wrapper package path
- modeling a nested `structured_shape` result over that generated wrapper
- packaging and deploying the nested result through the normal generated-package lifecycle
- final recursive JSON emission through `TO_JSON(*)` on the wrapped result

### Durable Result-Family Package

```bash
python3 tests/test_result_family_package_tool.py
```

Verifies:

- generating a durable result-family package from a materialization config
- persisting both the materialization recipe and the materialized family manifest
- recreating the durable source family during `install`
- validating the installed source family plus wrapper/preprocessor package together
