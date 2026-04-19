#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, quote as url_quote, unquote, urlencode, urlparse, urlunparse

from . import structured_result_tool
from . import wrapper_package_tool
from .generate_preprocessor_sql import validate_identifier
from .wrapper_schema_support import (
    ROOT,
    connect_for_generation,
    load_installed_wrapper_manifest,
    load_installed_wrapper_manifests,
    quote_identifier,
)


DEFAULT_ARTIFACT_DIR = ROOT / "dist" / "exasol-json-tables"
DEFAULT_SCHEMA_PREFIX = "EJT"
IDENTIFIER_TOKEN_RE = re.compile(r"[^A-Za-z0-9]+")
JSON_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class CliCommandError(Exception):
    code: str
    message: str
    hint: str | None = None
    likely_fix: str | None = None

    def __str__(self) -> str:
        return self.message


def _resolved(path: Path | None) -> Path | None:
    if path is None:
        return None
    return path.resolve()


def _default_source_manifest_path(input_path: Path, artifact_dir: Path) -> Path:
    return artifact_dir / f"{input_path.stem}.source_manifest.json"


def _find_single_source_manifest(search_dir: Path) -> Path | None:
    if not search_dir.exists():
        return None
    candidates = sorted(search_dir.glob("*.source_manifest.json"))
    if len(candidates) == 1:
        return candidates[0].resolve()
    return None


def _warn_multiple_source_manifests(search_dir: Path) -> None:
    if not search_dir.exists():
        return
    candidates = sorted(search_dir.glob("*.source_manifest.json"))
    if len(candidates) > 1:
        print(
            "Multiple source manifests found in "
            f"{search_dir}; falling back to live source-schema introspection. "
            "Use --source-manifest to choose one explicitly.",
            file=sys.stderr,
        )


def _artifact_dir_override(current_output_dir: Path, artifact_dir: Path) -> Path:
    if current_output_dir.resolve() == wrapper_package_tool.DEFAULT_PACKAGE_DIR.resolve():
        return artifact_dir.resolve()
    return current_output_dir.resolve()


def _copy_namespace(args: argparse.Namespace, **updates) -> argparse.Namespace:
    payload = vars(args).copy()
    payload.update(updates)
    return argparse.Namespace(**payload)


def _stdout_to_stderr(enabled: bool):
    return contextlib.redirect_stdout(sys.stderr) if enabled else contextlib.nullcontext()


def _emit_json_summary(payload: dict[str, object]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _redacted_argv(argv: list[str]) -> list[str]:
    redacted: list[str] = []
    hide_next = False
    for token in argv:
        if hide_next:
            redacted.append("***")
            hide_next = False
            continue
        if token == "--password":
            redacted.append(token)
            hide_next = True
            continue
        if token.startswith("--password="):
            redacted.append("--password=***")
            continue
        redacted.append(token)
    return redacted


def _wrapper_agent_warnings() -> list[str]:
    return [
        "Wrapper JSON syntax is session-scoped: activate the preprocessor in each SQL session before using path, bracket, iterator, or helper syntax.",
        "Wrapper JSON syntax applies on wrapper schemas only, not on the raw source schema or helper schema.",
    ]


def _build_wrapper_summary_from_config_path(config_path: Path) -> dict[str, object]:
    config_path = config_path.resolve()
    config = wrapper_package_tool.load_package_config(config_path)
    manifest_path = wrapper_package_tool.resolve_configured_path(
        config_path, config["generatedFiles"]["manifest"]
    ).resolve()
    summary: dict[str, object] = {
        "packageConfig": str(config_path),
        "sourceSchema": config["sourceSchema"],
        "wrapperSchema": config["wrapperSchema"],
        "helperSchema": config["helperSchema"],
        "preprocessor": {
            "schema": config["preprocessor"]["schema"],
            "script": config["preprocessor"]["script"],
            "activationSql": wrapper_package_tool.build_activation_sql(config, include_semicolon=True),
            "activationRequired": True,
        },
        "generatedFiles": {
            "manifest": str(manifest_path),
            "viewsSql": str(
                wrapper_package_tool.resolve_configured_path(config_path, config["generatedFiles"]["viewsSql"]).resolve()
            ),
            "preprocessorSql": str(
                wrapper_package_tool.resolve_configured_path(
                    config_path, config["generatedFiles"]["preprocessorSql"]
                ).resolve()
            ),
        },
        "warnings": _wrapper_agent_warnings(),
    }
    if config.get("sourceManifest") is not None:
        summary["sourceManifest"] = str(Path(config["sourceManifest"]).resolve())
    if "resultFamily" in config:
        summary["resultFamily"] = {
            "kind": config["resultFamily"]["kind"],
            "materializationConfig": str(
                wrapper_package_tool.resolve_configured_path(
                    config_path, config["resultFamily"]["materializationConfig"]
                ).resolve()
            ),
            "materializedFamilyManifest": str(
                wrapper_package_tool.resolve_configured_path(
                    config_path, config["resultFamily"]["materializedFamilyManifest"]
                ).resolve()
            ),
        }
    if manifest_path.exists():
        manifest = wrapper_package_tool.load_manifest_and_validate(config, manifest_path)
        summary["smokeTestSql"] = wrapper_package_tool.build_smoke_test_query(config, manifest)
    return summary


def _build_wrapper_artifacts(summary: dict[str, object]) -> dict[str, object]:
    artifacts: dict[str, object] = {
        "packageConfig": summary["packageConfig"],
        "manifest": summary["generatedFiles"]["manifest"],
        "viewsSql": summary["generatedFiles"]["viewsSql"],
        "preprocessorSql": summary["generatedFiles"]["preprocessorSql"],
    }
    if "sourceManifest" in summary:
        artifacts["sourceManifest"] = summary["sourceManifest"]
    if "resultFamily" in summary:
        artifacts["resultFamilyConfig"] = summary["resultFamily"]["materializationConfig"]
        artifacts["resultFamilyManifest"] = summary["resultFamily"]["materializedFamilyManifest"]
    return artifacts


def _build_wrapper_objects(summary: dict[str, object]) -> dict[str, object]:
    return {
        "sourceSchema": summary["sourceSchema"],
        "wrapperSchema": summary["wrapperSchema"],
        "helperSchema": summary["helperSchema"],
        "preprocessorSchema": summary["preprocessor"]["schema"],
        "preprocessorScript": summary["preprocessor"]["script"],
    }


def _build_wrapper_next_actions(summary: dict[str, object]) -> dict[str, object]:
    next_actions: dict[str, object] = {
        "activationSql": summary["preprocessor"]["activationSql"],
        "activationRequired": summary["preprocessor"]["activationRequired"],
    }
    if "smokeTestSql" in summary:
        next_actions["smokeTestSql"] = summary["smokeTestSql"]
    return next_actions


def _json_success_payload(
    command: str,
    *,
    warnings: list[str] | None = None,
    errors: list[dict[str, object]] | None = None,
    **payload: object,
) -> dict[str, object]:
    envelope: dict[str, object] = {
        "schemaVersion": JSON_SCHEMA_VERSION,
        "status": "ok",
        "command": command,
        "warnings": warnings or [],
        "errors": errors or [],
    }
    envelope.update(payload)
    return envelope


def _json_error_payload(
    command: str,
    *,
    code: str,
    message: str,
    hint: str | None = None,
    repro: dict[str, object] | None = None,
    likely_fix: str | None = None,
) -> dict[str, object]:
    error: dict[str, object] = {
        "code": code,
        "message": message,
    }
    if hint is not None:
        error["hint"] = hint
    if repro is not None:
        error["repro"] = repro
    if likely_fix is not None:
        error["likelyFix"] = likely_fix
    return {
        "schemaVersion": JSON_SCHEMA_VERSION,
        "status": "error",
        "command": command,
        "warnings": [],
        "errors": [error],
    }


def _command_label(args: argparse.Namespace) -> str:
    if args.command == "wrap":
        return f"wrap {args.wrap_command}"
    if args.command == "structured-results":
        return f"structured-results {args.structured_command}"
    if args.command == "describe":
        return f"describe {args.describe_command}"
    return str(args.command)


def _error_code_for_message(message: str, exc: BaseException | None = None) -> str:
    if message.startswith("JVS-"):
        return message.split(":", 1)[0]
    if message.startswith("INGEST-"):
        return message.split(":", 1)[0]
    if isinstance(exc, FileNotFoundError):
        return "FILE-NOT-FOUND"
    if isinstance(exc, subprocess.CalledProcessError):
        return "SUBPROCESS-FAILED"
    if isinstance(exc, SystemExit):
        return "COMMAND-FAILED"
    return "UNEXPECTED-ERROR"


def _describe_root_fields(root_table: dict[str, object], root: dict[str, object]) -> dict[str, object]:
    top_level_fields: list[dict[str, object]] = []
    scalar_fields: list[str] = []
    object_fields: list[dict[str, object]] = []
    array_fields: list[dict[str, object]] = []
    relationships_by_segment = {
        relation["segmentName"]: relation
        for relation in root.get("relationships", [])
        if relation["parentTable"] == root["tableName"]
    }
    for group in root_table.get("groups", []):
        visible_name = group.get("visibleName")
        if visible_name in {None, "_id"}:
            continue
        field_kind = "scalar"
        logical_name = str(visible_name)
        if logical_name.endswith("|object"):
            field_kind = "object"
            logical_name = logical_name[: -len("|object")]
        elif logical_name.endswith("|array"):
            field_kind = "array"
            logical_name = logical_name[: -len("|array")]
        top_level_fields.append({"name": logical_name, "kind": field_kind})
        if field_kind == "scalar":
            scalar_fields.append(logical_name)
    for segment_name, relationship in sorted(relationships_by_segment.items()):
        entry = {
            "name": segment_name,
            "childTable": relationship["childTable"],
        }
        if relationship["relationKind"] == "array":
            array_fields.append(entry)
        elif relationship["relationKind"] == "object":
            object_fields.append(entry)
    return {
        "topLevelFields": top_level_fields,
        "scalarFields": scalar_fields,
        "objectFields": object_fields,
        "arrayFields": array_fields,
    }


def _describe_wrapper_manifest(
    manifest: dict[str, object],
    *,
    preprocessor: dict[str, object] | None = None,
) -> dict[str, object]:
    table_lookup = {table["tableName"]: table for table in manifest["tables"]}
    roots: list[dict[str, object]] = []
    for root in sorted(manifest["roots"], key=lambda item: item["publicView"]):
        root_table = table_lookup[root["tableName"]]
        root_fields = _describe_root_fields(root_table, root)
        example_queries: dict[str, object] = {
            "toJsonAll": (
                f'SELECT TO_JSON(*) AS doc_json FROM "{manifest["publicSchema"]}"."{root["publicView"]}" '
                'ORDER BY "_id";'
            ),
        }
        if root_fields["scalarFields"]:
            scalar_name = root_fields["scalarFields"][0]
            example_queries["qualifiedHelper"] = (
                f'SELECT JSON_AS_VARCHAR(s.{quote_identifier(scalar_name)}) AS sample_value '
                f'FROM "{manifest["publicSchema"]}"."{root["publicView"]}" s ORDER BY "_id" LIMIT 5;'
            )
        if root_fields["arrayFields"]:
            array_name = root_fields["arrayFields"][0]["name"]
            example_queries["rowset"] = (
                f'SELECT s."_id", item._index FROM "{manifest["publicSchema"]}"."{root["publicView"]}" s '
                f'JOIN item IN s.{quote_identifier(array_name)} ORDER BY 1, 2;'
            )
        roots.append(
            {
                "publicView": root["publicView"],
                "tableName": root["tableName"],
                **root_fields,
                "exampleQueries": example_queries,
            }
        )
    description: dict[str, object] = {
        "sourceSchema": manifest["sourceSchema"],
        "wrapperSchema": manifest["publicSchema"],
        "helperSchema": manifest["helperSchema"],
        "rootCount": len(roots),
        "roots": roots,
    }
    if preprocessor is not None:
        description["preprocessor"] = preprocessor
    return description


def _wrapper_discovery_metadata(
    *,
    autodiscovered_helper_schema: bool,
    autodiscovered_wrapper_schema: bool,
    manifest: dict[str, object],
) -> dict[str, object]:
    return {
        "surfaceKind": "wrapperPackage",
        "discoveryMethod": "helperSchemaMetadata",
        "autodiscoveredHelperSchema": autodiscovered_helper_schema,
        "autodiscoveredWrapperSchema": autodiscovered_wrapper_schema,
        "discoveryScope": "wrapperPackagesOnly",
        "publishedConsumerSurfacesIncluded": False,
        "wrapperSchema": manifest["publicSchema"],
        "helperSchema": manifest["helperSchema"],
    }


def _resolve_installed_wrapper_manifest(
    con,
    *,
    wrapper_schema: str | None,
    helper_schema: str | None,
) -> tuple[dict[str, object], dict[str, object]]:
    normalized_wrapper = (
        None if wrapper_schema is None else validate_identifier("Wrapper schema", wrapper_schema)
    )
    normalized_helper = (
        None if helper_schema is None else validate_identifier("Helper schema", helper_schema)
    )
    if normalized_helper is not None:
        try:
            manifest = load_installed_wrapper_manifest(con, normalized_helper)
        except ValueError as exc:
            raise CliCommandError(
                code="WRAPPER-NOT-FOUND",
                message=str(exc),
                hint="Check that the helper schema exists and that it contains the installed wrapper metadata tables.",
            ) from exc
        if normalized_wrapper is not None and str(manifest["publicSchema"]).upper() != normalized_wrapper:
            raise CliCommandError(
                code="WRAPPER-DISCOVERY-MISMATCH",
                message=(
                    f'Helper schema {normalized_helper} describes wrapper schema {manifest["publicSchema"]}, '
                    f"not {normalized_wrapper}."
                ),
                hint="Use the matching wrapper schema or omit --helper-schema and let the CLI discover it from metadata.",
            )
        return manifest, _wrapper_discovery_metadata(
            autodiscovered_helper_schema=False,
            autodiscovered_wrapper_schema=normalized_wrapper is None,
            manifest=manifest,
        )

    manifests = load_installed_wrapper_manifests(
        con,
        wrapper_schemas=None if normalized_wrapper is None else [normalized_wrapper],
    )
    if not manifests:
        if normalized_wrapper is not None:
            raise CliCommandError(
                code="WRAPPER-NOT-FOUND",
                message=f"No installed wrapper metadata was found for wrapper schema {normalized_wrapper}.",
                hint="Check that the wrapper package is installed and that the wrapper schema name is correct.",
            )
        raise CliCommandError(
            code="WRAPPER-NOT-FOUND",
            message="No installed wrapper metadata was found in the database.",
            hint="Install a wrapper package first, or use `wrap deploy` / `wrap install` before running discovery.",
        )
    if len(manifests) > 1:
        candidates = [
            {
                "wrapperSchema": manifest["publicSchema"],
                "helperSchema": manifest["helperSchema"],
            }
            for manifest in manifests
        ]
        if normalized_wrapper is not None:
            raise CliCommandError(
                code="WRAPPER-DISCOVERY-AMBIGUOUS",
                message=(
                    f"Multiple helper schemas describe wrapper schema {normalized_wrapper}. "
                    f"Candidates: {candidates}"
                ),
                hint="Supply --helper-schema to choose one installed wrapper surface explicitly.",
            )
        raise CliCommandError(
            code="WRAPPER-DISCOVERY-AMBIGUOUS",
            message=f"Multiple installed wrappers were found. Candidates: {candidates}",
            hint="Supply --wrapper-schema to filter discovery to one installed wrapper surface.",
        )
    manifest = manifests[0]
    return manifest, _wrapper_discovery_metadata(
        autodiscovered_helper_schema=True,
        autodiscovered_wrapper_schema=normalized_wrapper is None,
        manifest=manifest,
    )


def _installed_wrapper_entry(
    con,
    manifest: dict[str, object],
    *,
    preprocessor: dict[str, object] | None = None,
    discovery: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "surfaceKind": "wrapperPackage",
        "discovery": discovery or _wrapper_discovery_metadata(
            autodiscovered_helper_schema=False,
            autodiscovered_wrapper_schema=False,
            manifest=manifest,
        ),
        "description": _describe_wrapper_manifest(manifest, preprocessor=preprocessor),
        "installedState": wrapper_package_tool.build_installed_metadata_summary(con, manifest),
    }


def _classify_ingest_failure(message: str, *, database_context: bool) -> tuple[str, str, str | None]:
    normalized = message.lower()
    if (
        "input file is empty" in normalized
        or "expected top-level json array" in normalized
        or "is not an object" in normalized
    ):
        return (
            "INGEST-UNSUPPORTED-INPUT-FORMAT",
            "The ingest input must be a JSON array of objects or NDJSON with one object per line.",
            "Convert the input to a top-level JSON array or NDJSON object stream and rerun ingest.",
        )
    if (
        " at line " in normalized
        or ("line " in normalized and "column" in normalized)
        or "expected value" in normalized
        or "trailing characters" in normalized
        or "eof while parsing" in normalized
        or "key must be a string" in normalized
    ):
        return (
            "INGEST-JSON-PARSE-ERROR",
            "The input file could not be parsed as valid JSON/NDJSON.",
            "Validate the JSON syntax locally and rerun ingest.",
        )
    if (
        "no such file or directory" in normalized
        or "permission denied" in normalized
        or "file exists" in normalized
        or "not a directory" in normalized
        or "is a directory" in normalized
    ):
        return (
            "INGEST-LOCAL-FILESYSTEM-ERROR",
            "The ingest workflow could not read or write a local file or staging path.",
            "Check the input path, artifact directory, and staging directory permissions.",
        )
    if database_context or any(
        token in normalized
        for token in [
            "connection refused",
            "login failed",
            "authentication",
            "websocket",
            "pyexasol",
            "exasol",
            "could not connect",
            "timed out",
        ]
    ):
        return (
            "INGEST-DATABASE-IMPORT-ERROR",
            "The ingest workflow could not create schemas or import data into Exasol.",
            "Check the Exasol connection URL, credentials, TLS settings, and destination schema.",
        )
    return (
        "INGEST-FAILED",
        "The ingest workflow failed.",
        "Run the same command without --json to inspect the full ingest logs on stderr.",
    )


def _raise_ingest_error(message: str, *, database_context: bool) -> None:
    code, hint, likely_fix = _classify_ingest_failure(message, database_context=database_context)
    raise CliCommandError(
        code=code,
        message=message,
        hint=hint,
        likely_fix=likely_fix,
    )


def add_json_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a machine-readable JSON summary on stdout. Human-oriented progress logs are sent to stderr.",
    )


def add_describe_package_arguments(parser: argparse.ArgumentParser) -> None:
    wrapper_package_tool.add_package_config_argument(parser)
    add_json_argument(parser)


def add_describe_wrapper_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--wrapper-schema", default=None, help="Installed public wrapper schema. Optional when discovery is unambiguous.")
    parser.add_argument(
        "--helper-schema",
        default=None,
        help="Installed helper schema that owns __JVS_* metadata tables. Optional when it can be discovered from metadata.",
    )
    parser.add_argument("--preprocessor-schema", default=None, help="Optional preprocessor schema for activation examples.")
    parser.add_argument("--preprocessor-script", default=None, help="Optional preprocessor script for activation examples.")
    wrapper_package_tool.add_connection_arguments(parser)
    add_json_argument(parser)


def add_describe_wrappers_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--wrapper-schema",
        action="append",
        default=None,
        help="Optional wrapper schema filter. Repeat to limit discovery to specific installed wrappers.",
    )
    wrapper_package_tool.add_connection_arguments(parser)
    add_json_argument(parser)


def _normalize_identifier_token(raw: str, *, fallback: str = "DATA", limit: int = 40) -> str:
    token = IDENTIFIER_TOKEN_RE.sub("_", raw.upper()).strip("_")
    if not token:
        token = fallback
    if not token[0].isalpha():
        token = f"N_{token}"
    token = re.sub(r"_+", "_", token)
    return token[:limit]


def _normalize_slug(raw: str, *, fallback: str = "data", limit: int = 60) -> str:
    slug = IDENTIFIER_TOKEN_RE.sub("_", raw.lower()).strip("_")
    if not slug:
        slug = fallback
    slug = re.sub(r"_+", "_", slug)
    return slug[:limit]


def _derived_workflow_names(raw_name: str, schema_prefix: str) -> dict[str, str]:
    prefix = validate_identifier("Schema prefix", schema_prefix)
    token = _normalize_identifier_token(raw_name)
    slug = _normalize_slug(raw_name)
    stem = f"{prefix}_{token}"
    return {
        "sourceSchema": f"{stem}_SRC",
        "wrapperSchema": f"{stem}_VIEW",
        "helperSchema": f"{stem}_VIEW_INTERNAL",
        "preprocessorSchema": f"{stem}_PP",
        "preprocessorScript": f"{stem}_PREPROCESSOR",
        "packageName": f"{slug}_wrapper",
        "artifactSubdir": slug,
    }


def _parse_exasol_url(url: str) -> dict[str, object]:
    parsed = urlparse(url)
    if parsed.scheme.lower() != "exasol":
        raise SystemExit(f"--exasol must use the exasol:// scheme, got: {url}")
    hostname = parsed.hostname or ""
    if hostname == "":
        raise SystemExit(f"--exasol must include a hostname, got: {url}")
    dsn = hostname if parsed.port is None else f"{hostname}:{parsed.port}"
    return {
        "dsn": dsn,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "schema": parsed.path.lstrip("/") or None,
        "query": dict(parse_qsl(parsed.query, keep_blank_values=True)),
    }


def _build_exasol_url(
    *,
    dsn: str,
    user: str,
    password: str,
    schema: str,
    tls: bool,
    validate_server_certificate: bool,
    extra_query: dict[str, str] | None = None,
) -> str:
    query: dict[str, str] = {}
    if extra_query is not None:
        query.update(extra_query)
    if tls:
        query["tls"] = "1"
    else:
        query.pop("tls", None)
    if validate_server_certificate:
        query["validateservercertificate"] = "1"
    else:
        query["validateservercertificate"] = "0"
    netloc = f"{url_quote(user, safe='')}:{url_quote(password, safe='')}@{dsn}"
    return urlunparse(("exasol", netloc, f"/{schema}", "", urlencode(query), ""))


def _resolve_ingest_connection(args: argparse.Namespace, source_schema: str) -> dict[str, str]:
    if args.exasol:
        parsed = _parse_exasol_url(args.exasol)
        resolved_schema = validate_identifier(
            "Source schema",
            args.source_schema or parsed["schema"] or source_schema,
        )
        return {
            "dsn": str(parsed["dsn"]),
            "user": str(parsed["user"]),
            "password": str(parsed["password"]),
            "sourceSchema": resolved_schema,
            "exasolUrl": _build_exasol_url(
                dsn=str(parsed["dsn"]),
                user=str(parsed["user"]),
                password=str(parsed["password"]),
                schema=resolved_schema,
                tls=args.tls,
                validate_server_certificate=bool(getattr(args, "validate_server_certificate", False)),
                extra_query=dict(parsed["query"]),
            ),
        }
    resolved_schema = validate_identifier("Source schema", args.source_schema or source_schema)
    return {
        "dsn": args.dsn,
        "user": args.user,
        "password": args.password,
        "sourceSchema": resolved_schema,
        "exasolUrl": _build_exasol_url(
            dsn=args.dsn,
            user=args.user,
            password=args.password,
            schema=resolved_schema,
            tls=args.tls,
            validate_server_certificate=bool(getattr(args, "validate_server_certificate", False)),
        ),
    }


def _ensure_schema_exists(
    dsn: str,
    user: str,
    password: str,
    schema: str,
    *,
    validate_server_certificate: bool = False,
) -> None:
    con = connect_for_generation(
        dsn,
        user,
        password,
        validate_certificate=validate_server_certificate,
    )
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(schema)}")
    finally:
        con.close()


def add_ingest_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input", type=Path, required=True, help="Input JSON or NDJSON file.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for generated Parquet / SQL output. Default: artifact dir for local output, otherwise Rust default behavior.",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=DEFAULT_ARTIFACT_DIR,
        help="Directory used by the unified CLI for source-manifest artifacts.",
    )
    parser.add_argument(
        "--manifest-output",
        type=Path,
        default=None,
        help="Explicit source-manifest output path. Default: <artifact-dir>/<input-stem>.source_manifest.json.",
    )
    parser.add_argument(
        "--no-source-manifest",
        action="store_true",
        help="Do not emit a source-manifest artifact from the ingest step.",
    )
    parser.add_argument("--schema-sql", action="store_true", help="Emit Exasol SQL DDL.")
    parser.add_argument("--exasol", default=None, help="Exasol connection URL.")
    parser.add_argument("--exasol-temp-dir", type=Path, default=None, help="Exasol staging directory.")
    parser.add_argument("--exasol-cleanup", action="store_true", help="Clean up staged Parquet files after upload.")
    parser.add_argument(
        "--tls",
        action="store_true",
        default=True,
        help="Use TLS when constructing an Exasol ingest URL. Enabled by default.",
    )
    parser.add_argument(
        "--no-tls",
        dest="tls",
        action="store_false",
        help="Disable TLS when constructing an Exasol ingest URL.",
    )
    parser.add_argument(
        "--validate-server-certificate",
        action="store_true",
        default=False,
        help="Validate the server certificate when constructing an Exasol ingest URL. Default: disabled.",
    )
    parser.add_argument(
        "--cargo-manifest-path",
        type=Path,
        default=ROOT / "crates" / "json_tables_ingest" / "Cargo.toml",
        help="Rust ingest crate manifest path.",
    )
    add_json_argument(parser)


def add_wrap_generate_arguments(parser: argparse.ArgumentParser) -> None:
    wrapper_package_tool.add_connection_arguments(parser)
    wrapper_package_tool.add_generation_arguments(parser)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=DEFAULT_ARTIFACT_DIR,
        help="Unified CLI artifact directory. Used as the default package output dir and for source-manifest autodetection.",
    )
    parser.add_argument(
        "--no-auto-source-manifest",
        action="store_true",
        help="Disable automatic source-manifest discovery from the artifact directory.",
    )
    add_json_argument(parser)


def add_wrap_install_arguments(parser: argparse.ArgumentParser) -> None:
    wrapper_package_tool.add_connection_arguments(parser)
    wrapper_package_tool.add_package_config_argument(parser)
    parser.add_argument("--views-sql", type=Path, default=None, help="Override views SQL path.")
    parser.add_argument("--preprocessor-sql", type=Path, default=None, help="Override preprocessor SQL path.")
    parser.add_argument("--skip-views", action="store_true", help="Skip wrapper/helper SQL installation.")
    parser.add_argument("--skip-source-family", action="store_true", help="Skip durable result-family installation.")
    parser.add_argument("--skip-preprocessor", action="store_true", help="Skip preprocessor SQL installation.")
    parser.add_argument(
        "--activate-session",
        action="store_true",
        help="Activate the preprocessor in the installer session and run the smoke test.",
    )
    add_json_argument(parser)


def add_wrap_deploy_arguments(parser: argparse.ArgumentParser) -> None:
    add_wrap_install_arguments(parser)
    parser.add_argument("--manifest", type=Path, default=None, help="Override manifest path for validation.")
    parser.add_argument(
        "--skip-validate-installed",
        action="store_true",
        help="Install the package but skip the follow-up installed-package validation step.",
    )


def add_wrap_validate_arguments(parser: argparse.ArgumentParser) -> None:
    wrapper_package_tool.add_package_config_argument(parser)
    parser.add_argument("--manifest", type=Path, default=None, help="Override manifest path.")
    parser.add_argument("--views-sql", type=Path, default=None, help="Override views SQL path.")
    parser.add_argument("--preprocessor-sql", type=Path, default=None, help="Override preprocessor SQL path.")
    parser.add_argument("--check-installed", action="store_true", help="Validate installed database objects too.")
    wrapper_package_tool.add_connection_arguments(parser, required=False)
    add_json_argument(parser)


def add_wrap_regenerate_arguments(parser: argparse.ArgumentParser) -> None:
    wrapper_package_tool.add_package_config_argument(parser)
    parser.add_argument("--manifest", type=Path, default=None, help="Override manifest path.")
    parser.add_argument("--output", type=Path, default=None, help="Override preprocessor SQL output.")
    parser.add_argument("--activate-session", action="store_true", help="Append ALTER SESSION to regenerated SQL.")
    add_json_argument(parser)


def add_structured_results_preview_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--result-family-config",
        type=Path,
        required=True,
        help="Structured result config JSON. Supports synthesized_family and structured_shape.",
    )
    parser.add_argument("--target-schema", default="JVS_STRUCTURED_RESULT_PREVIEW", help="Materialization target schema.")
    parser.add_argument(
        "--table-kind",
        choices=["table", "local_temporary"],
        default="local_temporary",
        help="Materialization mode.",
    )
    parser.add_argument("--root-table", default=None, help="Optional root table when the family has multiple roots.")
    parser.add_argument("--dsn", default="127.0.0.1:8563", help="Exasol DSN.")
    parser.add_argument("--user", default="sys", help="Exasol user.")
    parser.add_argument("--password", default="exasol", help="Exasol password.")


def add_structured_results_package_arguments(parser: argparse.ArgumentParser) -> None:
    wrapper_package_tool.add_connection_arguments(parser)
    wrapper_package_tool.add_generation_arguments(parser)
    wrapper_package_tool.add_result_family_arguments(parser)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=DEFAULT_ARTIFACT_DIR,
        help="Unified CLI artifact directory. Used as the default package output dir.",
    )
    add_json_argument(parser)


def add_ingest_and_wrap_arguments(parser: argparse.ArgumentParser) -> None:
    add_ingest_arguments(parser)
    wrapper_package_tool.add_connection_arguments(parser)
    parser.add_argument(
        "--name",
        default=None,
        help="Workflow name used to derive default schemas, package name, and artifact subdirectory. Default: input filename stem.",
    )
    parser.add_argument(
        "--schema-prefix",
        default=DEFAULT_SCHEMA_PREFIX,
        help="Prefix used when deriving default schema and script names.",
    )
    parser.add_argument("--source-schema", default=None, help="Override the derived source schema name.")
    parser.add_argument("--wrapper-schema", default=None, help="Override the derived public wrapper schema name.")
    parser.add_argument("--helper-schema", default=None, help="Override the derived helper schema name.")
    parser.add_argument("--preprocessor-schema", default=None, help="Override the derived preprocessor schema name.")
    parser.add_argument("--preprocessor-script", default=None, help="Override the derived preprocessor script name.")
    parser.add_argument("--package-name", default=None, help="Override the derived package file prefix.")
    parser.add_argument(
        "--run-artifact-dir",
        type=Path,
        default=None,
        help="Directory for this workflow run's generated artifacts. Default: <artifact-dir>/<derived-name>.",
    )
    parser.add_argument(
        "--activate-session",
        action="store_true",
        help="Activate the installed preprocessor in the deploy session and run the smoke test.",
    )
    parser.add_argument(
        "--skip-validate-installed",
        action="store_true",
        help="Skip the installed-package validation step after deployment.",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Unified Exasol JSON Tables workflow CLI. This orchestration layer sits on top of the Rust ingest crate "
            "and the Python wrapper / structured-results package."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest JSON / NDJSON into the source-table contract.")
    add_ingest_arguments(ingest_parser)

    ingest_wrap_parser = subparsers.add_parser(
        "ingest-and-wrap",
        help="Run the common workflow end to end: ingest into Exasol, generate the wrapper package, install it, and validate it.",
    )
    add_ingest_and_wrap_arguments(ingest_wrap_parser)

    wrap_parser = subparsers.add_parser("wrap", help="Generate, install, or validate the wrapper package.")
    wrap_subparsers = wrap_parser.add_subparsers(dest="wrap_command", required=True)
    wrap_generate = wrap_subparsers.add_parser("generate", help="Generate a wrapper package.")
    add_wrap_generate_arguments(wrap_generate)
    wrap_install = wrap_subparsers.add_parser("install", help="Install a wrapper package.")
    add_wrap_install_arguments(wrap_install)
    wrap_deploy = wrap_subparsers.add_parser(
        "deploy",
        help="Install a wrapper package and then validate the installed database objects.",
    )
    add_wrap_deploy_arguments(wrap_deploy)
    wrap_validate = wrap_subparsers.add_parser("validate", help="Validate a wrapper package.")
    add_wrap_validate_arguments(wrap_validate)
    wrap_regenerate = wrap_subparsers.add_parser(
        "regenerate-preprocessor",
        help="Regenerate only the wrapper preprocessor SQL from an existing package config.",
    )
    add_wrap_regenerate_arguments(wrap_regenerate)

    structured_parser = subparsers.add_parser(
        "structured-results",
        help="Preview or package structured results through the unified CLI.",
    )
    structured_subparsers = structured_parser.add_subparsers(dest="structured_command", required=True)
    structured_preview = structured_subparsers.add_parser(
        "preview-json",
        help="Materialize a structured result family and export it back to nested JSON-like rows.",
    )
    add_structured_results_preview_arguments(structured_preview)
    structured_package = structured_subparsers.add_parser(
        "package",
        help="Generate a durable wrapper package from a structured result-family config.",
    )
    add_structured_results_package_arguments(structured_package)

    describe_parser = subparsers.add_parser(
        "describe",
        help="Describe a generated package or an installed wrapper surface for automation and discovery.",
    )
    describe_subparsers = describe_parser.add_subparsers(dest="describe_command", required=True)
    describe_package = describe_subparsers.add_parser(
        "package",
        help="Describe a generated wrapper package from its package config and manifest.",
    )
    add_describe_package_arguments(describe_package)
    describe_wrapper = describe_subparsers.add_parser(
        "wrapper",
        help="Describe an installed wrapper surface from its helper-schema metadata tables.",
    )
    add_describe_wrapper_arguments(describe_wrapper)
    describe_wrappers = describe_subparsers.add_parser(
        "wrappers",
        help="List installed wrapper surfaces discovered from helper-schema metadata tables.",
    )
    add_describe_wrappers_arguments(describe_wrappers)

    validate_parser = subparsers.add_parser(
        "validate",
        help="Convenience alias for wrapper package validation.",
    )
    add_wrap_validate_arguments(validate_parser)
    return parser.parse_args()


def command_ingest(args: argparse.Namespace) -> None:
    input_path = args.input.resolve()
    artifact_dir = args.artifact_dir.resolve()
    try:
        artifact_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _raise_ingest_error(str(exc), database_context=False)
    manifest_output = None
    if not args.no_source_manifest:
        manifest_output = _resolved(args.manifest_output) or _default_source_manifest_path(input_path, artifact_dir)

    output_dir = _resolved(args.output_dir)
    if output_dir is None and args.exasol is None:
        output_dir = artifact_dir

    if args.exasol is not None:
        try:
            parsed_exasol = _parse_exasol_url(args.exasol)
            ingest_schema = parsed_exasol.get("schema")
            ingest_user = str(parsed_exasol.get("user") or "")
            ingest_password = str(parsed_exasol.get("password") or "")
            if ingest_schema and ingest_user and ingest_password:
                _ensure_schema_exists(
                    dsn=str(parsed_exasol["dsn"]),
                    user=ingest_user,
                    password=ingest_password,
                    schema=str(ingest_schema),
                    validate_server_certificate=bool(getattr(args, "validate_server_certificate", False)),
                )
        except CliCommandError:
            raise
        except Exception as exc:
            _raise_ingest_error(str(exc), database_context=True)

    command = [
        "cargo",
        "run",
        "--manifest-path",
        str(args.cargo_manifest_path.resolve()),
        "--",
        "--input",
        str(input_path),
    ]
    if output_dir is not None:
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _raise_ingest_error(str(exc), database_context=False)
        command.extend(["--output-dir", str(output_dir)])
    if args.schema_sql:
        command.append("--schema-sql")
    if manifest_output is not None:
        try:
            manifest_output.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _raise_ingest_error(str(exc), database_context=False)
        command.extend(["--manifest-output", str(manifest_output)])
    if args.exasol is not None:
        command.extend(["--exasol", args.exasol])
    if args.exasol_temp_dir is not None:
        command.extend(["--exasol-temp-dir", str(args.exasol_temp_dir.resolve())])
    if args.exasol_cleanup:
        command.append("--exasol-cleanup")
    capture_logs = bool(args.json) or bool(getattr(args, "_force_stderr_logs", False))
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            check=True,
            capture_output=capture_logs,
            text=capture_logs,
        )
    except subprocess.CalledProcessError as exc:
        details = []
        if exc.stderr:
            details.append(exc.stderr.strip())
        if exc.stdout:
            details.append(exc.stdout.strip())
        message = "\n".join(part for part in details if part).strip() or str(exc)
        _raise_ingest_error(message, database_context=args.exasol is not None)
    if capture_logs:
        if completed.stdout:
            print(completed.stdout, end="", file=sys.stderr)
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
    if manifest_output is not None and not args.json and not getattr(args, "_suppress_human_output", False):
        print(f"Unified CLI wrote source manifest: {manifest_output}")
    if args.json:
        parquet_files: list[str] = []
        if output_dir is not None and output_dir.exists():
            parquet_files = [str(path.resolve()) for path in sorted(output_dir.glob("*.parquet"))]
        summary: dict[str, object] = _json_success_payload(
            "ingest",
            input=str(input_path),
            artifactDir=str(artifact_dir),
            artifacts={
                "sourceManifest": str(manifest_output) if manifest_output is not None else None,
                "outputDir": str(output_dir) if output_dir is not None else None,
                "parquetFiles": parquet_files,
            },
            artifactFiles={
                "sourceManifest": str(manifest_output) if manifest_output is not None else None,
                "outputDir": str(output_dir) if output_dir is not None else None,
            },
        )
        if args.exasol is not None:
            parsed_exasol = _parse_exasol_url(args.exasol)
            summary["exasol"] = {
                "dsn": str(parsed_exasol["dsn"]),
                "sourceSchema": str(parsed_exasol.get("schema") or ""),
                "sourceSchemaEnsured": bool(parsed_exasol.get("schema")),
            }
        _emit_json_summary(summary)


def _derive_phase5_workflow_config(args: argparse.Namespace) -> dict[str, object]:
    workflow_name = args.name or args.input.stem
    defaults = _derived_workflow_names(workflow_name, args.schema_prefix)
    run_artifact_dir = (_resolved(args.run_artifact_dir) or (args.artifact_dir.resolve() / defaults["artifactSubdir"])).resolve()
    connection = _resolve_ingest_connection(args, defaults["sourceSchema"])
    return {
        "workflowName": workflow_name,
        "runArtifactDir": run_artifact_dir,
        "sourceSchema": connection["sourceSchema"],
        "wrapperSchema": validate_identifier("Wrapper schema", args.wrapper_schema or defaults["wrapperSchema"]),
        "helperSchema": validate_identifier("Helper schema", args.helper_schema or defaults["helperSchema"]),
        "preprocessorSchema": validate_identifier(
            "Preprocessor schema",
            args.preprocessor_schema or defaults["preprocessorSchema"],
        ),
        "preprocessorScript": validate_identifier(
            "Preprocessor script",
            args.preprocessor_script or defaults["preprocessorScript"],
        ),
        "packageName": args.package_name or defaults["packageName"],
        "dsn": connection["dsn"],
        "user": connection["user"],
        "password": connection["password"],
        "exasolUrl": connection["exasolUrl"],
    }


def command_wrap_deploy(args: argparse.Namespace) -> dict[str, object]:
    validation_report: dict[str, object] | None = None
    with _stdout_to_stderr(bool(getattr(args, "json", False))):
        wrapper_package_tool.command_install(args)
        if not args.skip_validate_installed:
            validate_args = _copy_namespace(
                args,
                check_installed=True,
                manifest=args.manifest,
                json=False,
            )
            config, manifest, validation_report = wrapper_package_tool.build_validation_report(validate_args)
            wrapper_package_tool.print_validation_report(config, manifest, validation_report)
    return {
        "validatedInstalled": not args.skip_validate_installed,
        "validation": validation_report,
    }


def command_ingest_and_wrap(args: argparse.Namespace) -> None:
    workflow = _derive_phase5_workflow_config(args)
    run_artifact_dir = workflow["runArtifactDir"]
    run_artifact_dir.mkdir(parents=True, exist_ok=True)
    _ensure_schema_exists(
        dsn=str(workflow["dsn"]),
        user=str(workflow["user"]),
        password=str(workflow["password"]),
        schema=str(workflow["sourceSchema"]),
    )

    ingest_args = _copy_namespace(
        args,
        artifact_dir=run_artifact_dir,
        output_dir=_resolved(args.output_dir),
        exasol=str(workflow["exasolUrl"]),
        source_schema=str(workflow["sourceSchema"]),
        json=False,
        _suppress_human_output=True,
        _force_stderr_logs=bool(args.json),
    )
    with _stdout_to_stderr(bool(args.json)):
        command_ingest(ingest_args)

        generate_args = argparse.Namespace(
            dsn=str(workflow["dsn"]),
            user=str(workflow["user"]),
            password=str(workflow["password"]),
            source_schema=str(workflow["sourceSchema"]),
            source_manifest=None,
            wrapper_schema=str(workflow["wrapperSchema"]),
            helper_schema=str(workflow["helperSchema"]),
            preprocessor_schema=str(workflow["preprocessorSchema"]),
            preprocessor_script=str(workflow["preprocessorScript"]),
            output_dir=run_artifact_dir,
            package_name=str(workflow["packageName"]),
            function_names=None,
            variant_typeof_function_names=None,
            variant_varchar_function_names=None,
            variant_decimal_function_names=None,
            variant_boolean_function_names=None,
            to_json_function_names=None,
            blocked_helper_names=None,
            blocked_helper_message="This helper is not available on the wrapper surface yet.",
            activate_session=False,
            artifact_dir=run_artifact_dir,
            no_auto_source_manifest=False,
        )
        resolved_generate_args = _resolve_wrap_generation_args(generate_args)
        wrapper_package_tool.command_generate(resolved_generate_args)

        package_config_path = run_artifact_dir / f"{workflow['packageName']}_package.json"
        deploy_args = argparse.Namespace(
            dsn=str(workflow["dsn"]),
            user=str(workflow["user"]),
            password=str(workflow["password"]),
            package_config=package_config_path,
            views_sql=None,
            preprocessor_sql=None,
            skip_views=False,
            skip_source_family=False,
            skip_preprocessor=False,
            activate_session=args.activate_session,
            manifest=None,
            skip_validate_installed=args.skip_validate_installed,
            json=False,
        )
        deploy_report = command_wrap_deploy(deploy_args)

    package_config_path = run_artifact_dir / f"{workflow['packageName']}_package.json"

    if args.json:
        wrapper_summary = _build_wrapper_summary_from_config_path(package_config_path)
        summary = _json_success_payload(
            "ingest-and-wrap",
            warnings=list(wrapper_summary["warnings"]),
            workflowName=workflow["workflowName"],
            runArtifactDir=str(run_artifact_dir),
            input=str(args.input.resolve()),
            exasol={
                "dsn": str(workflow["dsn"]),
                "sourceSchema": str(workflow["sourceSchema"]),
            },
            artifacts=_build_wrapper_artifacts(wrapper_summary),
            objects=_build_wrapper_objects(wrapper_summary),
            nextActions=_build_wrapper_next_actions(wrapper_summary),
            wrapper=wrapper_summary,
            validatedInstalled=deploy_report["validatedInstalled"],
            validation=deploy_report["validation"],
        )
        _emit_json_summary(summary)
    else:
        print("Unified CLI completed ingest-and-wrap workflow.")
        print(f"Workflow name: {workflow['workflowName']}")
        print(f"Run artifact directory: {run_artifact_dir}")
        print(f"Package config: {package_config_path}")
        print(
            "Schemas: "
            f"{workflow['sourceSchema']} -> {workflow['wrapperSchema']} / {workflow['helperSchema']} "
            f"with {workflow['preprocessorSchema']}.{workflow['preprocessorScript']}"
        )


def _resolve_wrap_generation_args(args: argparse.Namespace) -> argparse.Namespace:
    args.output_dir = _artifact_dir_override(Path(args.output_dir), args.artifact_dir)
    if args.source_manifest is not None:
        args.source_manifest = args.source_manifest.resolve()
        return args
    if args.no_auto_source_manifest:
        return args
    artifact_dir = args.artifact_dir.resolve()
    detected = _find_single_source_manifest(artifact_dir)
    if detected is not None:
        args.source_manifest = detected
        print(
            f"Unified CLI using source manifest: {detected}",
            file=sys.stderr if getattr(args, "json", False) else sys.stdout,
        )
    else:
        _warn_multiple_source_manifests(artifact_dir)
    return args


def command_wrap(args: argparse.Namespace) -> None:
    if args.wrap_command == "generate":
        resolved_args = _resolve_wrap_generation_args(args)
        with _stdout_to_stderr(bool(args.json)):
            wrapper_package_tool.command_generate(resolved_args)
        if args.json:
            package_paths = wrapper_package_tool.build_package_paths(Path(resolved_args.output_dir), resolved_args.package_name)
            wrapper_summary = _build_wrapper_summary_from_config_path(package_paths["packageConfig"])
            _emit_json_summary(
                _json_success_payload(
                    "wrap generate",
                    warnings=list(wrapper_summary["warnings"]),
                    artifacts=_build_wrapper_artifacts(wrapper_summary),
                    objects=_build_wrapper_objects(wrapper_summary),
                    nextActions=_build_wrapper_next_actions(wrapper_summary),
                    wrapper=wrapper_summary,
                )
            )
    elif args.wrap_command == "install":
        with _stdout_to_stderr(bool(args.json)):
            wrapper_package_tool.command_install(args)
        if args.json:
            wrapper_summary = _build_wrapper_summary_from_config_path(args.package_config.resolve())
            summary = _json_success_payload(
                "wrap install",
                warnings=list(wrapper_summary["warnings"]),
                artifacts=_build_wrapper_artifacts(wrapper_summary),
                objects=_build_wrapper_objects(wrapper_summary),
                nextActions=_build_wrapper_next_actions(wrapper_summary),
                wrapper=wrapper_summary,
                activateSession=bool(args.activate_session),
                activationPersistsAfterCommand=False,
                installedSourceFamily=not args.skip_source_family,
                installedViews=not args.skip_views,
                installedPreprocessor=not args.skip_preprocessor,
            )
            _emit_json_summary(summary)
    elif args.wrap_command == "deploy":
        deploy_report = command_wrap_deploy(args)
        if args.json:
            wrapper_summary = _build_wrapper_summary_from_config_path(args.package_config.resolve())
            summary = _json_success_payload(
                "wrap deploy",
                warnings=list(wrapper_summary["warnings"]),
                artifacts=_build_wrapper_artifacts(wrapper_summary),
                objects=_build_wrapper_objects(wrapper_summary),
                nextActions=_build_wrapper_next_actions(wrapper_summary),
                wrapper=wrapper_summary,
                activateSession=bool(args.activate_session),
                activationPersistsAfterCommand=False,
                validatedInstalled=deploy_report["validatedInstalled"],
                validation=deploy_report["validation"],
            )
            _emit_json_summary(summary)
    elif args.wrap_command == "validate":
        with _stdout_to_stderr(bool(args.json)):
            config, manifest, validation_report = wrapper_package_tool.build_validation_report(args)
            wrapper_package_tool.print_validation_report(config, manifest, validation_report)
        if args.json:
            wrapper_summary = _build_wrapper_summary_from_config_path(args.package_config.resolve())
            _emit_json_summary(
                _json_success_payload(
                    "wrap validate",
                    warnings=list(wrapper_summary["warnings"]),
                    artifacts=_build_wrapper_artifacts(wrapper_summary),
                    objects=_build_wrapper_objects(wrapper_summary),
                    nextActions=_build_wrapper_next_actions(wrapper_summary),
                    wrapper=wrapper_summary,
                    checkedInstalled=bool(args.check_installed),
                    validation=validation_report,
                )
            )
    elif args.wrap_command == "regenerate-preprocessor":
        with _stdout_to_stderr(bool(args.json)):
            wrapper_package_tool.command_regenerate_preprocessor(args)
        if args.json:
            config_path = args.package_config.resolve()
            config = wrapper_package_tool.load_package_config(config_path)
            output_path = (
                args.output
                or wrapper_package_tool.resolve_configured_path(config_path, config["generatedFiles"]["preprocessorSql"])
            ).resolve()
            wrapper_summary = _build_wrapper_summary_from_config_path(config_path)
            _emit_json_summary(
                _json_success_payload(
                    "wrap regenerate-preprocessor",
                    warnings=list(wrapper_summary["warnings"]),
                    artifacts={
                        **_build_wrapper_artifacts(wrapper_summary),
                        "output": str(output_path),
                    },
                    objects=_build_wrapper_objects(wrapper_summary),
                    nextActions=_build_wrapper_next_actions(wrapper_summary),
                    output=str(output_path),
                    wrapper=wrapper_summary,
                )
            )
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unknown wrap command: {args.wrap_command}")


def command_structured_results(args: argparse.Namespace) -> None:
    if args.structured_command == "preview-json":
        structured_result_tool.command_preview_json(args)
    elif args.structured_command == "package":
        args.output_dir = _artifact_dir_override(Path(args.output_dir), args.artifact_dir)
        args.source_manifest = None
        with _stdout_to_stderr(bool(args.json)):
            wrapper_package_tool.command_generate_result_family_package(args)
        if args.json:
            package_paths = wrapper_package_tool.build_package_paths(Path(args.output_dir), args.package_name)
            wrapper_summary = _build_wrapper_summary_from_config_path(package_paths["packageConfig"])
            _emit_json_summary(
                _json_success_payload(
                    "structured-results package",
                    warnings=list(wrapper_summary["warnings"]),
                    artifacts=_build_wrapper_artifacts(wrapper_summary),
                    objects=_build_wrapper_objects(wrapper_summary),
                    nextActions=_build_wrapper_next_actions(wrapper_summary),
                    wrapper=wrapper_summary,
                )
            )
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unknown structured-results command: {args.structured_command}")


def command_validate(args: argparse.Namespace) -> None:
    with _stdout_to_stderr(bool(args.json)):
        config, manifest, validation_report = wrapper_package_tool.build_validation_report(args)
        wrapper_package_tool.print_validation_report(config, manifest, validation_report)
    if args.json:
        wrapper_summary = _build_wrapper_summary_from_config_path(args.package_config.resolve())
        _emit_json_summary(
            _json_success_payload(
                "validate",
                warnings=list(wrapper_summary["warnings"]),
                artifacts=_build_wrapper_artifacts(wrapper_summary),
                objects=_build_wrapper_objects(wrapper_summary),
                nextActions=_build_wrapper_next_actions(wrapper_summary),
                wrapper=wrapper_summary,
                checkedInstalled=bool(args.check_installed),
                validation=validation_report,
            )
        )


def command_describe(args: argparse.Namespace) -> None:
    if args.describe_command == "package":
        config_path = args.package_config.resolve()
        config = wrapper_package_tool.load_package_config(config_path)
        manifest_path = wrapper_package_tool.resolve_configured_path(config_path, config["generatedFiles"]["manifest"]).resolve()
        manifest = wrapper_package_tool.load_manifest_and_validate(config, manifest_path)
        wrapper_summary = _build_wrapper_summary_from_config_path(config_path)
        description = _describe_wrapper_manifest(
            manifest,
            preprocessor={
                "schema": config["preprocessor"]["schema"],
                "script": config["preprocessor"]["script"],
                "activationSql": wrapper_package_tool.build_activation_sql(config, include_semicolon=True),
            },
        )
        if args.json:
            _emit_json_summary(
                _json_success_payload(
                    "describe package",
                    warnings=list(wrapper_summary["warnings"]),
                    artifacts=_build_wrapper_artifacts(wrapper_summary),
                    objects=_build_wrapper_objects(wrapper_summary),
                    nextActions=_build_wrapper_next_actions(wrapper_summary),
                    wrapper=wrapper_summary,
                    description=description,
                )
            )
        else:
            print(f"Package config: {config_path}")
            print(f'Wrapper schema: {description["wrapperSchema"]}')
            print(f'Helper schema: {description["helperSchema"]}')
            print(f'Roots: {", ".join(root["publicView"] for root in description["roots"])}')
    elif args.describe_command == "wrapper":
        con = connect_for_generation(
            args.dsn,
            args.user,
            args.password,
            validate_certificate=bool(getattr(args, "validate_server_certificate", False)),
        )
        try:
            manifest, discovery = _resolve_installed_wrapper_manifest(
                con,
                wrapper_schema=args.wrapper_schema,
                helper_schema=args.helper_schema,
            )
            wrapper_entry = _installed_wrapper_entry(con, manifest, preprocessor=None, discovery=discovery)
        finally:
            con.close()
        preprocessor = None
        warnings: list[str] = []
        if args.preprocessor_schema and args.preprocessor_script:
            preprocessor = {
                "schema": validate_identifier("Preprocessor schema", args.preprocessor_schema),
                "script": validate_identifier("Preprocessor script", args.preprocessor_script),
                "activationSql": (
                    "ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "
                    f'{quote_identifier(validate_identifier("Preprocessor schema", args.preprocessor_schema))}.'
                    f'{quote_identifier(validate_identifier("Preprocessor script", args.preprocessor_script))};'
                ),
            }
        else:
            warnings.append(
                "Preprocessor schema/script not supplied, so the describe output cannot include activation SQL."
            )
        wrapper_entry["description"] = _describe_wrapper_manifest(manifest, preprocessor=preprocessor)
        if args.json:
            _emit_json_summary(
                _json_success_payload(
                    "describe wrapper",
                    warnings=warnings,
                    objects={
                        "wrapperSchema": manifest["publicSchema"],
                        "helperSchema": manifest["helperSchema"],
                        "sourceSchema": manifest["sourceSchema"],
                    },
                    discovery=wrapper_entry["discovery"],
                    description=wrapper_entry["description"],
                    installedState=wrapper_entry["installedState"],
                )
            )
        else:
            description = wrapper_entry["description"]
            print(f'Wrapper schema: {description["wrapperSchema"]}')
            print(f'Helper schema: {description["helperSchema"]}')
            print(f'Roots: {", ".join(root["publicView"] for root in description["roots"])}')
    elif args.describe_command == "wrappers":
        con = connect_for_generation(
            args.dsn,
            args.user,
            args.password,
            validate_certificate=bool(getattr(args, "validate_server_certificate", False)),
        )
        try:
            manifests = load_installed_wrapper_manifests(con, wrapper_schemas=args.wrapper_schema)
            wrappers = [
                _installed_wrapper_entry(
                    con,
                    manifest,
                    discovery=_wrapper_discovery_metadata(
                        autodiscovered_helper_schema=False,
                        autodiscovered_wrapper_schema=False,
                        manifest=manifest,
                    ),
                )
                for manifest in manifests
            ]
        finally:
            con.close()
        if args.json:
            _emit_json_summary(
                _json_success_payload(
                    "describe wrappers",
                    objects={
                        "wrapperCount": len(wrappers),
                        "wrapperSchemas": [wrapper["description"]["wrapperSchema"] for wrapper in wrappers],
                    },
                    discovery={
                        "surfaceKind": "wrapperPackage",
                        "discoveryMethod": "helperSchemaMetadata",
                        "discoveryScope": "wrapperPackagesOnly",
                        "publishedConsumerSurfacesIncluded": False,
                    },
                    wrappers=wrappers,
                )
            )
        else:
            if not wrappers:
                print("No installed wrappers found.")
            for wrapper in wrappers:
                description = wrapper["description"]
                print(
                    f'{description["wrapperSchema"]} -> {description["helperSchema"]} '
                    f'({description["rootCount"]} roots)'
                )
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unknown describe command: {args.describe_command}")


def main() -> None:
    wants_json = "--json" in sys.argv[1:]
    try:
        args = parse_args()
    except SystemExit as exc:
        if wants_json and exc.code not in (0, None):
            _emit_json_summary(
                _json_error_payload(
                    "parse",
                    code="ARGPARSE-ERROR",
                    message="Invalid CLI arguments.",
                    hint="Run the command with --help to inspect the expected arguments.",
                    repro={"argv": _redacted_argv(sys.argv[1:])},
                )
            )
        raise

    try:
        if args.command == "ingest":
            command_ingest(args)
        elif args.command == "ingest-and-wrap":
            command_ingest_and_wrap(args)
        elif args.command == "wrap":
            command_wrap(args)
        elif args.command == "structured-results":
            command_structured_results(args)
        elif args.command == "describe":
            command_describe(args)
        elif args.command == "validate":
            command_validate(args)
        else:  # pragma: no cover - defensive
            raise SystemExit(f"Unknown command: {args.command}")
    except CliCommandError as exc:
        if wants_json:
            _emit_json_summary(
                _json_error_payload(
                    _command_label(args),
                    code=exc.code,
                    message=exc.message,
                    hint=exc.hint,
                    repro={"argv": _redacted_argv(sys.argv[1:])},
                    likely_fix=exc.likely_fix,
                )
            )
            raise SystemExit(1) from None
        raise SystemExit(exc.message) from None
    except SystemExit as exc:
        if wants_json and exc.code not in (0, None):
            message = str(exc.code) if isinstance(exc.code, str) else "Command failed."
            _emit_json_summary(
                _json_error_payload(
                    _command_label(args),
                    code=_error_code_for_message(message, exc),
                    message=message,
                    hint="Run the same command without --json to inspect the human-oriented logs on stderr.",
                    repro={"argv": _redacted_argv(sys.argv[1:])},
                )
            )
            raise SystemExit(1) from None
        raise
    except Exception as exc:
        if wants_json:
            _emit_json_summary(
                _json_error_payload(
                    _command_label(args),
                    code=_error_code_for_message(str(exc), exc),
                    message=str(exc),
                    hint="Run the same command without --json to inspect the human-oriented logs on stderr.",
                    repro={"argv": _redacted_argv(sys.argv[1:])},
                    likely_fix="Inspect the package paths, connection arguments, or validation inputs referenced by the error.",
                )
            )
            raise SystemExit(1) from None
        raise


if __name__ == "__main__":
    main()
