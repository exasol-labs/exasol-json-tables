# Developer Guide

## Generated Artifacts

The package generator produces four artifacts:

- wrapper SQL: public root views plus helper schema objects
- manifest JSON: machine-readable description of roots, tables, relationships, and folded column families
- preprocessor SQL: the scoped wrapper preprocessor
- package config JSON: the reproducible control-plane artifact for generation/install/validate/regenerate

Checked-in examples:

- [examples/json_wrapper_views.sql](../examples/json_wrapper_views.sql)
- [examples/json_wrapper_manifest.json](../examples/json_wrapper_manifest.json)
- [examples/json_wrapper_preprocessor.sql](../examples/json_wrapper_preprocessor.sql)
- [examples/json_wrapper_package.json](../examples/json_wrapper_package.json)

## Repo Guide

Main implementation files:

- wrapper package tool: [tools/wrapper_package_tool.py](../tools/wrapper_package_tool.py)
- wrapper SQL generator: [tools/generate_wrapper_views_sql.py](../tools/generate_wrapper_views_sql.py)
- wrapper preprocessor generator: [tools/generate_wrapper_preprocessor_sql.py](../tools/generate_wrapper_preprocessor_sql.py)
- shared wrapper manifest/generation logic: [tools/wrapper_schema_support.py](../tools/wrapper_schema_support.py)
- structured result-family materializer: [tools/result_family_materializer.py](../tools/result_family_materializer.py)
- structured result preview/export CLI: [tools/structured_result_tool.py](../tools/structured_result_tool.py)
- in-session wrapper installer: [tools/in_session_wrapper_installer.py](../tools/in_session_wrapper_installer.py)
- result-family JSON exporter: [tools/result_family_json_export.py](../tools/result_family_json_export.py)
- shared preprocessor engine: [tools/generate_preprocessor_sql.py](../tools/generate_preprocessor_sql.py)
- Nano fixture helpers: [tools/nano_support.py](../tools/nano_support.py)
- executable regression and benchmark entrypoints: [tests](../tests)
