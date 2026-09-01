# Automation

Use the installed `exasol-json-tables` command with `--json` when you want a stable machine-readable control plane for scripts, CI, and autonomous agents.

## What `--json` Gives You

The major workflow commands emit a common JSON envelope:

```json
{
  "schemaVersion": 1,
  "status": "ok",
  "command": "ingest-and-wrap",
  "warnings": [],
  "errors": []
}
```

On success, command-specific payload fields are added on top of that envelope.
On failure, `status` becomes `"error"` and `errors` contains a structured machine-readable record.

Human-oriented progress logs still go to stderr.

If your automation generates durable views or tables for downstream SQL consumers, also follow the aliasing rules in [identifier-conventions.md](identifier-conventions.md). The short version is: keep wrapper property references quoted, but prefer uppercase SQL-safe aliases on exported objects.

## Main Commands

### `ingest-and-wrap --json`

Use this for the one-shot automation path:

```bash
exasol-json-tables ingest-and-wrap \
  --input ./data.json \
  --name customer_events \
  --if-exists replace \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol-temp-dir /tmp/exasol-json-tables \
  --json
```

`--if-exists` controls retries at the whole-workflow level. A workflow owns its source,
wrapper, helper, and preprocessor schemas:

- `fail` is the default and stops before ingest if any workflow schema already exists.
- `replace` drops all four workflow schemas and rebuilds them. Use this with a stable
  `--name` for a logical batch when an unattended retry should restart from scratch.
- `skip` leaves the database unchanged and exits successfully if any workflow schema
  exists. This is useful for create-once jobs; it does not repair a partial prior run.

`replace` is destructive for the four derived or explicitly overridden schema names.
Do not point those schema options at schemas shared with another workflow.

An alternative append-only automation pattern is to use a unique `--name` for every
attempt, for example `customer_events_20260807_attempt_2`. This preserves earlier
attempts for inspection but creates a separate set of schemas and artifacts each time,
so the caller must eventually retire them.

The JSON summary includes:

- `artifacts`
  Package config and generated file paths
- `objects`
  Source, wrapper, helper, flat, and preprocessor names
- `nextActions`
  `activationSql` and `smokeTestSql` for the wrapper surface, plus `flatSchema`,
  `flatViews`, `flatSmokeTestSql`, and `joinKeys` for the preprocessor-free
  flattened surface
- `wrapper`
  The detailed wrapper package summary
- `validation`
  The installed-package validation report when validation ran

It also includes `outcome` (`completed` or `skipped`) and `ifExists`. A skipped run
reports `existingSchemas`; a replacement reports the schemas found in
`replacedSchemas`.

For wrapper-installing workflows, `objects.publicViews`, `nextActions.publicViews`, and `wrapper.publicViews` expose the actual public view names created inside the wrapper schema. `--name` controls the derived schema/package names, not the public view names themselves.

`nextActions.joinKeys` reports the join columns between the generated flattened
views, derived from the same relationships the ingest layer records in
`<name>.source_manifest.json`. Automation should read those instead of inferring
join keys from column names.

### `validate --json`

Use this when an automation step needs a trustworthy capability signal:

```bash
exasol-json-tables validate \
  --package-config ./dist/exasol-json-tables/customer_events_wrapper_package.json \
  --check-installed \
  --json
```

The validation payload includes:

- `validation.checkedInstalled`
- `validation.installed.capabilities`
- `validation.installed.probes`

The capability matrix is the important part for automation:

```json
{
  "rowset": {"supported": true, "ok": true},
  "qualifiedHelper": {"supported": true, "ok": true},
  "toJson": {"supported": true, "ok": true}
}
```

Each executed probe also includes:

- the probe name
- the exact SQL used
- the row count
- a small row preview

### `describe package --json`

Use this when you have a package config and want to discover the wrapped surface without scraping docs:

```bash
exasol-json-tables describe package \
  --package-config ./dist/exasol-json-tables/customer_events_wrapper_package.json \
  --json
```

The description includes:

- root views
- top-level fields
- recursive `fieldTree` discovery per root for nested object and object-array branches
- `familyTables` entries per root so agents can map helper-table names back to paths such as `meta.info` or `items[]`
- object and array fields
- example `TO_JSON(*)`, helper, and rowset queries
- activation SQL when the package config is available

### `describe wrapper --json`

Use this when the wrapper is already installed and you want to inspect it through the helper metadata tables.

If the wrapper schema is known and the installed metadata is unambiguous, the CLI can now autodiscover the helper schema:

```bash
exasol-json-tables describe wrapper \
  --wrapper-schema JSON_VIEW_CUSTOMER_EVENTS \
  --preprocessor-schema JVS_CUSTOMER_EVENTS_PP \
  --preprocessor-script JSON_CUSTOMER_EVENTS_PREPROCESSOR \
  --json
```

The response includes:

- `discovery` metadata showing whether the helper schema was autodiscovered
- `installedState` from live catalog metadata
- the wrapped roots, fields, recursive `fieldTree` data, per-root `familyTables`, and example queries
- a flat `description.querySurface` with each root, canonical JSON path, JSON type, copyable example expression, and any required array iterator expression

The same query surface and the required `ALTER SESSION` statement are printed by the
plain-text command. This is the quickest discovery route when catalog columns expose
storage markers such as `owner|object` rather than query paths such as `owner.team`.

If you do not provide the preprocessor schema and script, the describe output still works, but it cannot emit `activationSql`.

### `describe wrappers --json`

Use this when you want an inventory of installed wrapper packages without any local package-config files:

```bash
exasol-json-tables describe wrappers --json
```

This inventory is intentionally limited to true wrapper packages discovered through `__JVS_*` helper metadata. Ordinary published consumer views are not included.

## Session Activation

Wrapper syntax is session-scoped.

Automation should treat `nextActions.activationSql` as required setup before using:

- dotted paths
- bracket syntax
- rowset iterators
- JSON helper functions
- recursive `TO_JSON(*)` on wrapped roots

If the consumer cannot set session state, use the flattened views in
`nextActions.flatSchema` instead. They are plain SQL with UPPERCASE
unquoted-safe columns and need no activation. See
[flat-views.md](flat-views.md).

## Failure Envelopes

When a `--json` command fails, stdout still stays machine-readable:

```json
{
  "schemaVersion": 1,
  "status": "error",
  "command": "validate",
  "warnings": [],
  "errors": [
    {
      "code": "FILE-NOT-FOUND",
      "message": "...",
      "hint": "...",
      "repro": {"argv": ["validate", "...", "--json"]}
    }
  ]
}
```

That makes it practical to branch on:

- `status`
- the first error `code`
- the provided `repro.argv`

For ingest workflows, the main machine-readable error classes are now:

- `INGEST-WORKFLOW-ALREADY-EXISTS`
- `INGEST-JSON-PARSE-ERROR`
- `INGEST-UNSUPPORTED-INPUT-FORMAT`
- `INGEST-LOCAL-FILESYSTEM-ERROR`
- `INGEST-DATABASE-IMPORT-ERROR`

## Special Case: `structured-results preview-json`

`structured-results preview-json` already returns JSON rows directly.

Treat it as a preview/export command rather than a summary-style workflow command. For durable final output, prefer the wrapped-family path plus `TO_JSON(...)`.
