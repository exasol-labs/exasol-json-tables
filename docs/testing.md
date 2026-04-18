# Testing

Install Python test dependencies first:

```bash
python3 -m pip install -r requirements-dev.txt
```

The Nano tests expect a local Exasol Nano instance on `127.0.0.1:8563` with the default `sys` / `exasol` credentials used by the helpers.

## Primary Wrapper-Surface Regression

```bash
python3 tests/test_wrapper_surface.py
```

This verifies:

- public wrapper metadata and helper-schema shape
- dotted path and bracket access
- rowset expansion
- explicit-null helpers
- helper-based variant semantics
- deep recursive traversal

## Wrapper Package Lifecycle Regression

```bash
python3 tests/test_wrapper_package_tool.py
```

This verifies:

- package generation
- targeted preprocessor regeneration
- installation
- installed-package validation
- end-to-end wrapper queries through the installed preprocessor

## Wrapper Preprocessor Error Regression

```bash
python3 tests/test_wrapper_errors.py
```

This verifies:

- malformed path and bracket syntax errors
- iterator misuse errors
- helper arity and scope errors
- generator validation errors

## Modeling and BI Regression

```bash
python3 tests/test_wrapper_modeling.py
```

This verifies:

- nested CTE stacks with mixed helper, rowset, and deep-path logic
- stacked derived tables over projected wrapper expressions
- `UNION ALL` across multiple wrapper roots with branch-local helper semantics
- `GROUP BY` / `ORDER BY` over projected wrapper expressions
- persisted `CREATE VIEW ... AS SELECT` and `CREATE TABLE ... AS SELECT` flows
- UDF usage on iterator-local helper expressions

## Final Wrapper-Package Evaluation

```bash
python3 tests/test_wrapper_evaluation.py
```

This verifies:

- wrapper helper semantics on the installed package
- built-in SQL typing behavior on wrapper views
- UDF interoperability on the wrapper surface
- additive source-DDL refresh through package regeneration, install, and validation

## Performance Study

```bash
python3 tests/study_wrapper_performance.py
```

This benchmarks the final wrapper package on Nano for:

- path traversal
- rowset iteration
- explicit-null helper queries
- helper-based variant type and extraction queries
- warm steady-state and isolated cold-start behavior

## Structured Result Materialization Study

```bash
python3 tests/study_structured_result_materialization.py
```

This investigates whether the existing JSON table mapping can also serve as a structured-result interchange format for:

- family-preserving filtered copies of JSON documents
- synthesized nested analytical result families
- generic JSON reconstruction from source-like result tables
- local temporary result families versus durable scratch schemas

See also [structured-result-materialization-study.md](../structured-result-materialization-study.md).

## Result-Family Materializer Regression

```bash
python3 tests/test_result_family_materializer.py
```

This verifies the extracted Phase 1 materialization library for:

- family-preserving subset materialization from helper metadata
- synthesized nested result-family materialization from declarative table specs
- re-wrapping both materialized families through the normal wrapper interface

## In-Session Wrapper Installer Regression

```bash
python3 tests/test_in_session_wrapper_installer.py
```

This verifies the Phase 2 runtime install flow for:

- generating wrapper/helper objects from the current database session
- installing the companion preprocessor in the same session
- wrapping and querying a `LOCAL TEMPORARY` result family
- confirming the observed cross-session query behavior while the creating session remains alive

## Result-Family JSON Export Regression

```bash
python3 tests/test_result_family_json_export.py
```

This verifies the Phase 3 export helper for:

- exporting a family-preserving subset back to nested JSON-like rows
- exporting durable synthesized result families back to nested JSON-like rows
- exporting in-session wrapped local-temporary result families back to nested JSON-like rows
- preserving scalar-array versus object-array reconstruction
- preserving numeric JSON typing for aggregated export values

## Structured Results From Relational Tables Regression

```bash
python3 tests/test_structured_results_from_relational.py
```

This verifies that structured results can also be built from ordinary relational upstream tables by:

- materializing a synthesized source-like family from plain relational SQL
- packaging and installing that family through the durable package workflow
- querying it through the wrapper surface with path, bracket, and rowset syntax
- exporting it back to nested JSON-like rows

## Structured Result Ergonomics Regression

```bash
python3 tests/test_structured_result_ergonomics.py
```

This verifies the ergonomic layer for:

- authoring structured results with the higher-level `structured_shape` config
- one-shot `preview-json` materialize-and-export workflows
- durable packaging from the same higher-level shape config

## Durable Result-Family Package Regression

```bash
python3 tests/test_result_family_package_tool.py
```

This verifies the Phase 4 package flow for:

- generating a durable result-family package from a materialization config
- persisting both the materialization recipe and the materialized family manifest
- recreating the durable source family during `install`
- validating the installed source family plus wrapper/preprocessor package together
