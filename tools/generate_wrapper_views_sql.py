#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from generate_preprocessor_sql import validate_identifier
from generate_wrapper_preprocessor_sql import generate_wrapper_preprocessor_sql_text
from wrapper_schema_support import ROOT, connect_for_generation, generate_wrapper_artifacts


DEFAULT_OUTPUT = ROOT / "examples" / "json_wrapper_views.sql"
DEFAULT_MANIFEST_OUTPUT = ROOT / "examples" / "json_wrapper_manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate installable Exasol SQL for the wrapper-view architecture: public root views, "
            "an internal helper schema, a machine-readable manifest, and optionally the companion "
            "wrapper preprocessor package."
        )
    )
    parser.add_argument("--dsn", default="127.0.0.1:8563", help="Exasol DSN.")
    parser.add_argument("--user", default="sys", help="Exasol user.")
    parser.add_argument("--password", default="exasol", help="Exasol password.")
    parser.add_argument("--source-schema", default="JVS_SRC", help="Physical source schema.")
    parser.add_argument("--wrapper-schema", default="JSON_VIEW", help="Generated public wrapper schema.")
    parser.add_argument(
        "--helper-schema",
        default=None,
        help="Generated internal helper schema. Default: <wrapper-schema>_INTERNAL.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output SQL file.")
    parser.add_argument(
        "--manifest-output",
        type=Path,
        default=DEFAULT_MANIFEST_OUTPUT,
        help="Output JSON manifest file.",
    )
    parser.add_argument(
        "--preprocessor-output",
        type=Path,
        default=None,
        help="Optional output SQL file for the companion wrapper preprocessor.",
    )
    parser.add_argument(
        "--preprocessor-schema",
        default="JVS_WRAP_PP",
        help="Schema that will own the generated wrapper preprocessor script.",
    )
    parser.add_argument(
        "--preprocessor-script",
        default="JSON_WRAPPER_PREPROCESSOR",
        help="Generated wrapper preprocessor script name.",
    )
    parser.add_argument(
        "--activate-preprocessor-session",
        action="store_true",
        help="Append an ALTER SESSION statement to the generated wrapper preprocessor SQL.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_schema = validate_identifier("Source schema", args.source_schema)
    wrapper_schema = validate_identifier("Wrapper schema", args.wrapper_schema)
    helper_schema = validate_identifier("Helper schema", args.helper_schema or f"{args.wrapper_schema}_INTERNAL")
    if len({source_schema, wrapper_schema, helper_schema}) != 3:
        raise SystemExit(
            "Source schema, wrapper schema, and helper schema must all be distinct "
            f"(got source={source_schema}, wrapper={wrapper_schema}, helper={helper_schema})."
        )
    con = connect_for_generation(args.dsn, args.user, args.password)
    try:
        artifacts = generate_wrapper_artifacts(con, source_schema, wrapper_schema, helper_schema)
    finally:
        con.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(artifacts.sql)
    args.manifest_output.parent.mkdir(parents=True, exist_ok=True)
    args.manifest_output.write_text(json.dumps(artifacts.manifest, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {args.output}")
    print(f"Wrote {args.manifest_output}")
    if args.preprocessor_output is not None:
        preprocessor_sql = generate_wrapper_preprocessor_sql_text(
            schema=args.preprocessor_schema,
            script=args.preprocessor_script,
            wrapper_schemas=[wrapper_schema],
            helper_schemas=[helper_schema],
            manifests=[artifacts.manifest],
            activate_session=args.activate_preprocessor_session,
        )
        args.preprocessor_output.parent.mkdir(parents=True, exist_ok=True)
        args.preprocessor_output.write_text(preprocessor_sql)
        print(f"Wrote {args.preprocessor_output}")


if __name__ == "__main__":
    main()
