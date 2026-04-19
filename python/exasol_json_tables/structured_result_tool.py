#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from .in_session_wrapper_installer import install_wrapper_surface_in_session
from .result_family_materializer import (
    materialize_result_family,
    result_family_spec_from_dict,
    validate_result_family_spec,
)
from .wrapper_schema_support import connect_for_generation, quote_identifier


IDENTIFIER_TOKEN_RE = re.compile(r"[^A-Za-z0-9]+")


def _normalize_identifier_token(raw: str, *, fallback: str, limit: int) -> str:
    token = IDENTIFIER_TOKEN_RE.sub("_", raw.upper()).strip("_")
    if not token:
        token = fallback
    token = re.sub(r"_+", "_", token)
    if not token[0].isalpha():
        token = f"N_{token}"
    return token[:limit]


def _preview_surface_names(target_schema: str) -> dict[str, str]:
    target_token = _normalize_identifier_token(target_schema, fallback="PREVIEW", limit=20)
    pid_token = _normalize_identifier_token(str(os.getpid()), fallback="PID", limit=10)
    base = f"JVS_SR_{target_token}_{pid_token}"
    return {
        "wrapper_schema": f"{base}_VIEW",
        "helper_schema": f"{base}_HELPER",
        "preprocessor_schema": f"{base}_PP",
        "preprocessor_script": "JSON_STRUCTURED_PREVIEW",
    }


def _drop_schema_if_exists(con, schema_name: str) -> None:
    con.execute(f"DROP SCHEMA IF EXISTS {quote_identifier(schema_name)} CASCADE")


def _resolve_preview_public_view(manifest: dict[str, object], root_table: str | None) -> str:
    roots = list(manifest["roots"])
    if root_table is None:
        if len(roots) != 1:
            root_names = [str(root["tableName"]) for root in roots]
            raise ValueError(
                "root_table is required when the previewed family has multiple roots: "
                + ", ".join(root_names)
            )
        return str(roots[0]["publicView"])

    normalized_root = root_table.upper()
    for root in roots:
        public_view = str(root["publicView"])
        table_name = str(root["tableName"])
        if public_view.upper() == normalized_root or table_name.upper() == normalized_root:
            return public_view
    root_names = [str(root["tableName"]) for root in roots]
    raise ValueError(f"Unknown root table {root_table!r}. Expected one of {root_names!r}.")


def _query_preview_rows_via_wrapper(con, *, wrapper_schema: str, public_view: str) -> list[object]:
    rows = con.execute(
        f"""
        SELECT TO_JSON(*) AS doc_json
        FROM {quote_identifier(wrapper_schema)}.{quote_identifier(public_view)}
        ORDER BY "_id"
        """
    ).fetchall()
    preview_rows: list[object] = []
    for row in rows:
        if row[0] is None:
            raise ValueError("TO_JSON(*) preview query returned NULL.")
        preview_rows.append(json.loads(row[0]))
    return preview_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ergonomic workflows for structured results, including one-shot preview/export "
            "from either low-level family specs or higher-level structured-shape configs."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    preview_parser = subparsers.add_parser(
        "preview-json",
        help="Materialize a structured result family and immediately export it back to nested JSON-like rows.",
    )
    preview_parser.add_argument(
        "--result-family-config",
        type=Path,
        required=True,
        help="Structured result config JSON. Supports synthesized_family and structured_shape.",
    )
    preview_parser.add_argument(
        "--target-schema",
        default="JVS_STRUCTURED_RESULT_PREVIEW",
        help="Target schema used for the materialized family.",
    )
    preview_parser.add_argument(
        "--table-kind",
        choices=["table", "local_temporary"],
        default="local_temporary",
        help="Materialization mode. Default: local_temporary.",
    )
    preview_parser.add_argument(
        "--root-table",
        default=None,
        help="Optional root table name when the materialized family has multiple roots.",
    )
    preview_parser.add_argument("--dsn", default="127.0.0.1:8563", help="Exasol DSN.")
    preview_parser.add_argument("--user", default="sys", help="Exasol user.")
    preview_parser.add_argument("--password", default="exasol", help="Exasol password.")
    return parser.parse_args()


def command_preview_json(args: argparse.Namespace) -> None:
    config = json.loads(args.result_family_config.read_text())
    spec = result_family_spec_from_dict(config)
    validate_result_family_spec(spec)
    preview_surface = _preview_surface_names(args.target_schema)
    preview_rows: list[object]
    materialized = None
    con = connect_for_generation(
        args.dsn,
        args.user,
        args.password,
        validate_certificate=bool(getattr(args, "validate_server_certificate", False)),
    )
    try:
        materialized = materialize_result_family(
            con,
            target_schema=args.target_schema,
            spec=spec,
            table_kind=args.table_kind,
            reset_schema=True,
        )
        installed_wrapper = install_wrapper_surface_in_session(
            con,
            materialized_family=materialized,
            wrapper_schema=preview_surface["wrapper_schema"],
            helper_schema=preview_surface["helper_schema"],
            preprocessor_schema=preview_surface["preprocessor_schema"],
            preprocessor_script=preview_surface["preprocessor_script"],
            activate_preprocessor_session=True,
        )
        preview_rows = _query_preview_rows_via_wrapper(
            con,
            wrapper_schema=installed_wrapper.wrapper_schema,
            public_view=_resolve_preview_public_view(installed_wrapper.manifest, args.root_table),
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        for schema_name in [
            preview_surface["preprocessor_schema"],
            preview_surface["helper_schema"],
            preview_surface["wrapper_schema"],
        ]:
            try:
                _drop_schema_if_exists(con, schema_name)
            except Exception:
                pass
        if args.table_kind == "local_temporary":
            try:
                _drop_schema_if_exists(con, args.target_schema)
            except Exception:
                pass
        con.close()
    print(json.dumps(preview_rows, indent=2, sort_keys=True))


def main() -> None:
    args = parse_args()
    if args.command == "preview-json":
        command_preview_json(args)
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
