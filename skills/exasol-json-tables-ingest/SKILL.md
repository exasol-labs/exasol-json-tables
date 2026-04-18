---
name: exasol-json-tables-ingest
description: Use when working on the ingest stage of Exasol JSON Tables in this repository. Covers JSON or NDJSON ingestion into the source table-family contract, source-manifest generation, Exasol import flow, Rust ingest crate usage, and validation of ingest artifacts before wrapper generation.
---

# Exasol JSON Tables Ingest

## When To Use This Skill

Use this skill when the task is about:

- ingesting JSON or NDJSON into the Exasol JSON Tables source contract
- changing or debugging the Rust ingest engine
- reasoning about source manifests or source table-family shape
- validating that ingestion produced the right source schema for downstream wrapping
- documenting or testing the ingest workflow

Do not use this skill for wrapper SQL syntax or structured result authoring. Use the query or rewrite skills for those.

## First Moves

1. Identify whether the task is about:
   - raw file ingest
   - source-manifest emission
   - direct Exasol upload
   - source-contract correctness
2. Check whether the caller wants:
   - a one-shot `ingest-and-wrap` workflow
   - or direct ingest behavior only
3. Inspect the current contract before changing behavior.

Start with:

- `README.md`
- `docs/ingest.md`
- `docs/architecture.md`
- `crates/json_tables_ingest/src/lib.rs`
- `crates/json_tables_ingest/src/main.rs`
- `tests/test_ingest_manifest_integration.py`
- `tests/test_unified_cli.py`

## Mental Model

The ingest stage creates the stable relational contract that everything else builds on.

Key contract ideas:

- explicit JSON `null` becomes `<name>|n`
- nested object links become `<name>|object`
- nested array sizes become `<name>|array`
- array child tables use `_parent` and `_pos`
- root/object tables use `_id` where needed
- mixed scalar types become sibling variant columns

Do not treat this as a generic flattening step. It is a shape-preserving contract designed so the wrapper surface can reconstruct JSON-friendly behavior later.

## Main Entry Points

### Product CLI

Use the installed CLI for workflow-level tasks:

- `exasol-json-tables ingest`
- `exasol-json-tables ingest-and-wrap`

For agented or automated workflows, prefer `--json` on these commands so stdout stays machine-readable. The summary includes artifact paths, schema names, package config paths, activation SQL, and wrapper-scope warnings.

### Rust crate

Use Cargo when the task is about ingest implementation itself:

```bash
cargo run --manifest-path crates/json_tables_ingest/Cargo.toml -- --input ./data.json
```

## What To Check During Ingest Work

### Input assumptions

Supported formats:

- JSON array of objects
- NDJSON

The ingest engine auto-detects between them.

### Output expectations

Expect:

- one root table family per top-level document shape
- object child tables
- array child tables
- source manifest when requested

### Manifest seam

The source manifest is additive, not mandatory-first.

Preferred rule:

- if ingest produced a manifest, downstream wrap generation should use it
- if not, wrapper generation can still fall back to live source-schema introspection

## Common Workflows

### Emit a source manifest

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --artifact-dir ./dist/exasol-json-tables
```

Or directly via Cargo:

```bash
cargo run --manifest-path crates/json_tables_ingest/Cargo.toml -- \
  --input ./data.json \
  --manifest-output ./dist/data.source_manifest.json
```

### Upload directly into Exasol

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --exasol exasol://sys:exasol@127.0.0.1:8563/JVS_SRC
```

### One-shot happy path

```bash
exasol-json-tables ingest-and-wrap \
  --input ./data.json \
  --name customer_events
```

## Validation

Use the smallest relevant validation:

- Rust-only contract or manifest changes:
  - `cargo test --manifest-path crates/json_tables_ingest/Cargo.toml`
- Ingest-to-wrap seam changes:
  - `python3 tests/test_ingest_manifest_integration.py`
- Unified workflow changes:
  - `python3 tests/test_unified_cli.py`

If you changed naming, manifests, or artifact-dir behavior, run both integration tests.

## Guidance For Agents

- Prefer manifest-driven downstream flows when a manifest exists.
- Prefer `--json` on workflow commands instead of scraping human log text.
- Preserve compatibility of the source table-family contract unless the task explicitly allows a breaking change.
- Be explicit about whether a change affects:
  - raw source schema
  - manifest schema
  - wrapper generation assumptions
  - unified CLI workflow
- If changing the contract, inspect downstream Python consumers before editing Rust.

## Current Boundaries

- Ingest owns the source contract, not the query syntax.
- Wrapper behavior should not be hardcoded into ingest unless it belongs in the manifest seam.
- A Rust ingest improvement is not automatically a wrapper/query improvement; keep boundaries clear.
