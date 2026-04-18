#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qsl, quote as url_quote, unquote, urlencode, urlparse, urlunparse

from . import structured_result_tool
from . import wrapper_package_tool
from .generate_preprocessor_sql import validate_identifier
from .wrapper_schema_support import ROOT, connect_for_generation, quote_identifier


DEFAULT_ARTIFACT_DIR = ROOT / "dist" / "exasol_json_tables"
DEFAULT_SCHEMA_PREFIX = "EJT"
IDENTIFIER_TOKEN_RE = re.compile(r"[^A-Za-z0-9]+")


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


def add_json_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a machine-readable JSON summary on stdout. Human-oriented progress logs are sent to stderr.",
    )


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
                validate_server_certificate=args.validate_server_certificate,
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
            validate_server_certificate=args.validate_server_certificate,
        ),
    }


def _ensure_schema_exists(dsn: str, user: str, password: str, schema: str) -> None:
    con = connect_for_generation(dsn, user, password)
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

    validate_parser = subparsers.add_parser(
        "validate",
        help="Convenience alias for wrapper package validation.",
    )
    add_wrap_validate_arguments(validate_parser)
    return parser.parse_args()


def command_ingest(args: argparse.Namespace) -> None:
    input_path = args.input.resolve()
    artifact_dir = args.artifact_dir.resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest_output = None
    if not args.no_source_manifest:
        manifest_output = _resolved(args.manifest_output) or _default_source_manifest_path(input_path, artifact_dir)

    output_dir = _resolved(args.output_dir)
    if output_dir is None and args.exasol is None:
        output_dir = artifact_dir

    if args.exasol is not None:
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
            )

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
        output_dir.mkdir(parents=True, exist_ok=True)
        command.extend(["--output-dir", str(output_dir)])
    if args.schema_sql:
        command.append("--schema-sql")
    if manifest_output is not None:
        manifest_output.parent.mkdir(parents=True, exist_ok=True)
        command.extend(["--manifest-output", str(manifest_output)])
    if args.exasol is not None:
        command.extend(["--exasol", args.exasol])
    if args.exasol_temp_dir is not None:
        command.extend(["--exasol-temp-dir", str(args.exasol_temp_dir.resolve())])
    if args.exasol_cleanup:
        command.append("--exasol-cleanup")
    capture_logs = bool(args.json) or bool(getattr(args, "_force_stderr_logs", False))
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=capture_logs,
        text=capture_logs,
    )
    if capture_logs:
        if completed.stdout:
            print(completed.stdout, end="", file=sys.stderr)
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
    if manifest_output is not None and not args.json and not getattr(args, "_suppress_human_output", False):
        print(f"Unified CLI wrote source manifest: {manifest_output}")
    if args.json:
        summary: dict[str, object] = {
            "command": "ingest",
            "input": str(input_path),
            "artifactDir": str(artifact_dir),
            "artifactFiles": {
                "sourceManifest": str(manifest_output) if manifest_output is not None else None,
                "outputDir": str(output_dir) if output_dir is not None else None,
            },
        }
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


def command_wrap_deploy(args: argparse.Namespace) -> None:
    with _stdout_to_stderr(bool(getattr(args, "json", False))):
        wrapper_package_tool.command_install(args)
        if not args.skip_validate_installed:
            validate_args = _copy_namespace(
                args,
                check_installed=True,
                manifest=args.manifest,
                json=False,
            )
            wrapper_package_tool.command_validate(validate_args)


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
        command_wrap_deploy(deploy_args)

    package_config_path = run_artifact_dir / f"{workflow['packageName']}_package.json"

    if args.json:
        summary = {
            "command": "ingest-and-wrap",
            "workflowName": workflow["workflowName"],
            "runArtifactDir": str(run_artifact_dir),
            "input": str(args.input.resolve()),
            "exasol": {
                "dsn": str(workflow["dsn"]),
                "sourceSchema": str(workflow["sourceSchema"]),
            },
            "wrapper": _build_wrapper_summary_from_config_path(package_config_path),
            "validatedInstalled": not args.skip_validate_installed,
        }
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
            _emit_json_summary(
                {
                    "command": "wrap generate",
                    "wrapper": _build_wrapper_summary_from_config_path(package_paths["packageConfig"]),
                }
            )
    elif args.wrap_command == "install":
        with _stdout_to_stderr(bool(args.json)):
            wrapper_package_tool.command_install(args)
        if args.json:
            summary = {
                "command": "wrap install",
                "wrapper": _build_wrapper_summary_from_config_path(args.package_config.resolve()),
                "activateSession": bool(args.activate_session),
                "activationPersistsAfterCommand": False,
                "installedSourceFamily": not args.skip_source_family,
                "installedViews": not args.skip_views,
                "installedPreprocessor": not args.skip_preprocessor,
            }
            _emit_json_summary(summary)
    elif args.wrap_command == "deploy":
        command_wrap_deploy(args)
        if args.json:
            summary = {
                "command": "wrap deploy",
                "wrapper": _build_wrapper_summary_from_config_path(args.package_config.resolve()),
                "activateSession": bool(args.activate_session),
                "activationPersistsAfterCommand": False,
                "validatedInstalled": not args.skip_validate_installed,
            }
            _emit_json_summary(summary)
    elif args.wrap_command == "validate":
        with _stdout_to_stderr(bool(args.json)):
            wrapper_package_tool.command_validate(args)
        if args.json:
            _emit_json_summary(
                {
                    "command": "wrap validate",
                    "wrapper": _build_wrapper_summary_from_config_path(args.package_config.resolve()),
                    "checkedInstalled": bool(args.check_installed),
                }
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
            _emit_json_summary(
                {
                    "command": "wrap regenerate-preprocessor",
                    "output": str(output_path),
                    "wrapper": _build_wrapper_summary_from_config_path(config_path),
                }
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
            _emit_json_summary(
                {
                    "command": "structured-results package",
                    "wrapper": _build_wrapper_summary_from_config_path(package_paths["packageConfig"]),
                }
            )
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unknown structured-results command: {args.structured_command}")


def command_validate(args: argparse.Namespace) -> None:
    with _stdout_to_stderr(bool(args.json)):
        wrapper_package_tool.command_validate(args)
    if args.json:
        _emit_json_summary(
            {
                "command": "validate",
                "wrapper": _build_wrapper_summary_from_config_path(args.package_config.resolve()),
                "checkedInstalled": bool(args.check_installed),
            }
        )


def main() -> None:
    args = parse_args()
    if args.command == "ingest":
        command_ingest(args)
    elif args.command == "ingest-and-wrap":
        command_ingest_and_wrap(args)
    elif args.command == "wrap":
        command_wrap(args)
    elif args.command == "structured-results":
        command_structured_results(args)
    elif args.command == "validate":
        command_validate(args)
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
