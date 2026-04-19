# Installation

This page covers the practical setup for Exasol JSON Tables.

The supported product entrypoint is:

- `exasol-json-tables`

That command is provided by the Python package defined in [pyproject.toml](../pyproject.toml).

## What You Need

For the full workflow, you need:

- Python 3.9 or newer
- Rust and Cargo, if you want to run the ingest stage from this repo
- access to an Exasol database

If you only want to install wrapper views on top of an existing source schema, the Python package is enough. The Rust ingest engine is only needed for JSON/NDJSON ingestion.

## Standard Install

Install the Python package:

```bash
python3 -m pip install .
```

Build the Rust ingest engine:

```bash
cargo build --manifest-path crates/json_tables_ingest/Cargo.toml
```

Verify the installed CLI:

```bash
exasol-json-tables --help
```

If the console script is not on your shell path yet, `python3 -m exasol_json_tables --help` should behave the same way.

## Development Install

For repo-local development, the simplest setup is:

```bash
python3 -m pip install -r requirements-dev.txt
cargo build --manifest-path crates/json_tables_ingest/Cargo.toml
```

`requirements-dev.txt` installs the local package in editable mode, so the same `exasol-json-tables` command works while you are editing the code.

## Quickstart

The common happy path is:

```bash
exasol-json-tables ingest-and-wrap \
  --input ./data.json \
  --name customer_events \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol-temp-dir /tmp/exasol-json-tables
```

That command:

1. ingests the JSON into Exasol
2. writes a source manifest into a per-run artifact directory
3. generates the wrapper package
4. installs it
5. validates it

If you do not pass explicit connection arguments, this path assumes the local Nano-style defaults described below. For other environments, provide `--dsn`, `--user`, `--password`, or an explicit ingest `--exasol` URL.

After installation, activate the wrapper syntax in the SQL session where you want to query the data:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;
```

That same activation also enables the primary final-output surface:

```sql
SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW.CUSTOMER_EVENTS;
```

If you want to continue from that generated wrapper into a nested modeled result and finish with `TO_JSON(*)` again on the wrapped result family, see the end-to-end example in [structured-results.md](structured-results.md#quickstart-example).

If your environment already uses another SQL preprocessor, remember that Exasol only allows one active `SQL_PREPROCESSOR_SCRIPT` per session. Activating the JSON Tables preprocessor will replace the currently active one for that session.

In that case, use a small master preprocessor script as the single active entrypoint. Keep the real rewrite logic in helper functions or helper scripts, have the master script call the existing preprocessor logic and the JSON Tables preprocessor logic in the required order, and activate the master script instead of trying to enable multiple preprocessors separately.

## Connection Models

There are two common ways to run the workflow.

### Local Nano Defaults

Many tests and examples assume a local Exasol Nano instance at:

- host: `127.0.0.1`
- port: `8563`
- user: `sys`
- password: `exasol`

The wrapper-side helpers default to that environment.

### Explicit Connection Arguments

For other environments, use the connection options on the CLI. Common options include:

- `--dsn`
- `--user`
- `--password`
- `--exasol` for the ingest URL

For example:

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol exasol://sys:exasol@db.example.com:8563/JVS_SRC
```

For direct ingest, the CLI creates the target source schema if it does not already exist. If schema creation is not allowed for your user, the command will now fail early at that step instead of after scan and staging work.

## Compatibility Entrypoints

The `tools/` scripts still exist, but they are now compatibility and developer entrypoints, not the main product surface.

Examples:

- `python3 tools/exasol_json_tables.py`
- `python3 tools/wrapper_package_tool.py`
- `python3 tools/structured_result_tool.py`

Use them when:

- you are working directly in the repo
- you are debugging lower-level modules
- you are following an older internal script or test harness

For normal user-facing workflows, prefer the installed `exasol-json-tables` command.

## Machine-Readable CLI Mode

For automation, CI, and autonomous agents, the main workflow commands support `--json`.

That mode keeps stdout machine-readable and moves human-oriented progress logs to stderr. The JSON summary uses a stable success/failure envelope and includes the key values an agent typically needs next, such as:

- source, wrapper, helper, and preprocessor names
- package config and generated artifact paths
- activation SQL
- smoke-test SQL
- installed validation capability probes
- warnings about session activation and wrapper-only syntax

Example:

```bash
exasol-json-tables ingest-and-wrap \
  --input ./data.json \
  --name customer_events \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol-temp-dir /tmp/exasol-json-tables \
  --json
```

`structured-results preview-json` already returns JSON rows, so it does not need a separate summary envelope unless you add one in a higher-level wrapper. Treat that command as preview/validation. The primary durable final-output path is `TO_JSON(...)` on the installed wrapper or result wrapper.

For package and installed-wrapper discovery, use `exasol-json-tables describe package --json` or `exasol-json-tables describe wrapper --json`. For a full automation-oriented walkthrough, see [automation.md](automation.md).

## Next Reading

- Workflow overview: [README.md](../README.md)
- Ingest details: [ingest.md](ingest.md)
- Query surface: [query-surface.md](query-surface.md)
- Structured results: [structured-results.md](structured-results.md)
