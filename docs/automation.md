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

## Main Commands

### `ingest-and-wrap --json`

Use this for the one-shot automation path:

```bash
exasol-json-tables ingest-and-wrap \
  --input ./data.json \
  --name customer_events \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol-temp-dir /tmp/exasol-json-tables \
  --json
```

The JSON summary includes:

- `artifacts`
  Package config and generated file paths
- `objects`
  Source, wrapper, helper, and preprocessor names
- `nextActions`
  `activationSql` and `smokeTestSql`
- `wrapper`
  The detailed wrapper package summary
- `validation`
  The installed-package validation report when validation ran

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
- object and array fields
- example `TO_JSON(*)`, helper, and rowset queries
- activation SQL when the package config is available

### `describe wrapper --json`

Use this when the wrapper is already installed and you want to inspect it through the helper metadata tables:

```bash
exasol-json-tables describe wrapper \
  --wrapper-schema JSON_VIEW_CUSTOMER_EVENTS \
  --helper-schema JSON_VIEW_CUSTOMER_EVENTS_INTERNAL \
  --preprocessor-schema JVS_CUSTOMER_EVENTS_PP \
  --preprocessor-script JSON_CUSTOMER_EVENTS_PREPROCESSOR \
  --json
```

If you do not provide the preprocessor schema and script, the describe output still works, but it cannot emit `activationSql`.

## Session Activation

Wrapper syntax is session-scoped.

Automation should treat `nextActions.activationSql` as required setup before using:

- dotted paths
- bracket syntax
- rowset iterators
- JSON helper functions
- recursive `TO_JSON(*)` on wrapped roots

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

## Special Case: `structured-results preview-json`

`structured-results preview-json` already returns JSON rows directly.

Treat it as a preview/export command rather than a summary-style workflow command. For durable final output, prefer the wrapped-family path plus `TO_JSON(...)`.
