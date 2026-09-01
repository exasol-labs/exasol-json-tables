#!/usr/bin/env python3

"""`COMPILE_SQL`: the JSON Tables syntax reached without a session preprocessor.

Exasol allows one SQL preprocessor per session. That single slot is the largest source
of complexity for a consumer: it has to be set per statement, it cannot be shared with
another project that also wants it, it is unavailable to tools that cannot run
`ALTER SESSION` at all, and it makes a statement spanning two wrapper packages
impossible to express.

The rewrite itself never needed the session. `JVS_PREPROCESSOR_LIB.rewrite` is a pure
function of (text, config), so the same rewrite is reachable as an ordinary script:
text in, physical SQL out. A caller compiles once when a statement is edited and then
runs the returned SQL as many times as it likes.

One compile entry point serves every installed package, because its config is the union
of their manifests -- which is what makes the cross-package statement work.
"""

from __future__ import annotations

from typing import Any, Iterable

from .generate_preprocessor_library_sql import install_preprocessor_library
from .generate_preprocessor_sql import (
    DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT,
    validate_identifier,
)
from .generate_wrapper_preprocessor_sql import generate_wrapper_compile_sql_text
from .wrapper_schema_support import load_installed_wrapper_manifests, quote_identifier, sql_literal


DEFAULT_COMPILE_SCHEMA = "JVS_COMPILE"
DEFAULT_COMPILE_SCRIPT = "COMPILE_SQL"

#: The columns `COMPILE_SQL` returns, in order.
COMPILE_RESULT_COLUMNS = (
    "STATUS",
    "ERROR_CODE",
    "ERROR_MESSAGE",
    "ORIGINAL_SQL",
    "GENERATED_SQL",
    "PLAN_JSON",
    "CLARIFICATION_JSON",
)


def generate_compile_sql_for_packages(
    con,
    *,
    schema: str = DEFAULT_COMPILE_SCHEMA,
    script: str = DEFAULT_COMPILE_SCRIPT,
    wrapper_schemas: Iterable[str] | None = None,
    library_script: str = DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT,
) -> tuple[str, list[dict[str, Any]]]:
    """Render `COMPILE_SQL` for the installed packages, newest catalog state.

    With `wrapper_schemas` unset this covers every package on the database, which is the
    point: a consumer then has one entry point regardless of which packages a statement
    touches.
    """
    requested = None if wrapper_schemas is None else [str(value) for value in wrapper_schemas]
    manifests = load_installed_wrapper_manifests(con, requested)
    if not manifests:
        scope = "this database" if requested is None else ", ".join(requested)
        raise ValueError(
            f"No installed wrapper packages found for {scope}. "
            "Install a wrapper package first; COMPILE_SQL is generated from their manifests."
        )
    sql_text = generate_wrapper_compile_sql_text(
        schema=schema,
        script=script,
        wrapper_schemas=[str(manifest["publicSchema"]) for manifest in manifests],
        helper_schemas=[str(manifest["helperSchema"]) for manifest in manifests],
        manifests=manifests,
        library_script=library_script,
    )
    return sql_text, manifests


def apply_compile_sql(
    con,
    sql_text: str,
    *,
    schema: str = DEFAULT_COMPILE_SCHEMA,
    library_script: str = DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT,
) -> None:
    """Install a rendered compile script, together with the library it imports."""
    validated_schema = validate_identifier("Compile schema", schema)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(validated_schema)}")
    install_preprocessor_library(con, validated_schema, library_script)
    con.execute(_script_statement(sql_text))


def install_compile_sql(
    con,
    *,
    schema: str = DEFAULT_COMPILE_SCHEMA,
    script: str = DEFAULT_COMPILE_SCRIPT,
    wrapper_schemas: Iterable[str] | None = None,
    library_script: str = DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT,
) -> list[dict[str, Any]]:
    """Install or refresh `COMPILE_SQL`, returning the manifests it was built from.

    Re-run this after installing or regenerating any package: the config is baked into
    the script, so a package added later is invisible until the script is refreshed.
    """
    sql_text, manifests = generate_compile_sql_for_packages(
        con,
        schema=schema,
        script=script,
        wrapper_schemas=wrapper_schemas,
        library_script=library_script,
    )
    apply_compile_sql(con, sql_text, schema=schema, library_script=library_script)
    return manifests


def _script_statement(sql_text: str) -> str:
    """The `CREATE ... LUA SCRIPT` statement on its own, without the surrounding file."""
    marker = "CREATE OR REPLACE LUA SCRIPT"
    start = sql_text.index(marker)
    body = sql_text[start:]
    terminator = body.rindex("\n/")
    return body[:terminator]


def compile_sql(
    con,
    sql: str,
    *,
    schema: str = DEFAULT_COMPILE_SCHEMA,
    script: str = DEFAULT_COMPILE_SCRIPT,
) -> dict[str, Any]:
    """Compile one statement, returning the result contract as a dict.

    A convenience for Python callers and tests; the contract itself is the script's
    result table, which any client can fetch.
    """
    rows = con.execute(
        f"EXECUTE SCRIPT {quote_identifier(schema)}.{quote_identifier(script)}({sql_literal(sql)})"
    ).fetchall()
    if not rows:
        raise AssertionError("COMPILE_SQL returned no rows.")
    return dict(zip(COMPILE_RESULT_COLUMNS, rows[0]))


def compile_or_raise(
    con,
    sql: str,
    *,
    schema: str = DEFAULT_COMPILE_SCHEMA,
    script: str = DEFAULT_COMPILE_SCRIPT,
) -> str:
    """The compiled SQL, or the compiler's own error raised as one."""
    result = compile_sql(con, sql, schema=schema, script=script)
    if result["STATUS"] != "OK":
        raise ValueError(f"{result['ERROR_CODE']}: {result['ERROR_MESSAGE']}")
    return str(result["GENERATED_SQL"])
