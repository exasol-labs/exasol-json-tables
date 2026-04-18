#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from result_family_json_export import export_root_family_to_json
from result_family_materializer import materialize_result_family, result_family_spec_from_dict
from wrapper_schema_support import connect_for_generation


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
    con = connect_for_generation(args.dsn, args.user, args.password)
    try:
        materialized = materialize_result_family(
            con,
            target_schema=args.target_schema,
            spec=spec,
            table_kind=args.table_kind,
            reset_schema=True,
        )
        rows = export_root_family_to_json(
            con,
            materialized_family=materialized,
            root_table=args.root_table,
        )
    finally:
        con.close()
    print(json.dumps(rows, indent=2, sort_keys=True))


def main() -> None:
    args = parse_args()
    if args.command == "preview-json":
        command_preview_json(args)
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
