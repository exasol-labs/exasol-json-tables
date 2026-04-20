# Changelog

This project does not publish tagged release notes yet, so changes accumulate here under `Unreleased` until the next formal version cut.

The format is loosely based on Keep a Changelog and focuses on user-visible behavior, migration-relevant changes, and operational fixes.

## [Unreleased]

### Added

- Added a user-facing changelog so downstream users can track notable changes between releases.
- Added `publicViews` to the machine-readable wrapper workflow surface:
  - `ingest-and-wrap --json`
  - `wrap generate --json`
  - `wrap install --json`
  - `wrap deploy --json`
  - `validate --json`
  - `describe wrapper --json`
  - `describe wrappers --json`
- Added support for `TO_JSON(item.*)` on object-array iterator rows, so joined array items can now be serialized directly from the wrapper surface.
- Added [docs/identifier-conventions.md](docs/identifier-conventions.md) and aligned the agent skills with explicit guidance for quoted wrapper references, uppercase durable aliases, and reserved-word avoidance.

### Changed

- Hidden JSON export views are now generated for the full table family, not just the public roots. This expands the internal export surface used by `TO_JSON(...)` while keeping the user-facing contract unchanged.
- `structured-results preview-json` now uses the same temporary wrapper plus `TO_JSON(*)` outlet as the installed SQL surface.
- The old product-side Python JSON exporter surface was retired in favor of the SQL-native `TO_JSON(...)` path.

### Fixed

- Fixed wrapper/export generation for documents where array items contain nested object fields. `ingest-and-wrap` now succeeds for shapes such as `reviews[].date`.
- Fixed Python-side CLI flag consistency so commands like `validate`, `wrap install`, `wrap deploy`, and `describe ...` accept `--no-tls` alongside the ingest workflows.
- Fixed `describe wrapper --json` to expose `nextActions.activationSql` like `describe package --json`.
- Fixed `describe wrappers --json` so each wrapper entry includes top-level `wrapperSchema`, `helperSchema`, `sourceSchema`, and `publicViews`.
- Fixed wrapper workflow visibility around actual public view names. `--name` still controls derived schema/package names, and the actual public views are now surfaced explicitly in JSON responses and documented in the user docs.
- Fixed the installed-package hidden export surface so validation and helper-object expectations include all required export views.

### Migration Notes

- If you previously depended on the removed Python exporter helpers such as `export_root_family_to_json`, move to one of these supported paths:
  - final output from installed wrappers: query `TO_JSON(*)` or `TO_JSON(col1, col2, ...)`
  - one-shot preview from structured-results configs: `exasol-json-tables structured-results preview-json`
- If you automate wrapper discovery, prefer the stable top-level JSON fields:
  - `objects.publicViews`
  - `nextActions.activationSql`
  - `wrappers[].wrapperSchema`
  - `wrappers[].publicViews`
