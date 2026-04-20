from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .generate_json_export_helper_sql import install_json_export_helpers
from .generate_json_export_views_sql import install_json_export_views
from .generate_preprocessor_library_sql import (
    install_preprocessor_library,
)
from .generate_preprocessor_sql import DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT
from .generate_preprocessor_sql import validate_identifier
from .generate_wrapper_preprocessor_sql import (
    DEFAULT_EXPLICIT_NULL_FUNCTION_NAMES,
    DEFAULT_TO_JSON_FUNCTION_NAMES,
    DEFAULT_VARIANT_BOOLEAN_FUNCTION_NAMES,
    DEFAULT_VARIANT_DECIMAL_FUNCTION_NAMES,
    DEFAULT_VARIANT_TYPEOF_FUNCTION_NAMES,
    DEFAULT_VARIANT_VARCHAR_FUNCTION_NAMES,
    generate_wrapper_preprocessor_sql_text,
)
from .result_family_materializer import MaterializedFamilyResult
from .wrapper_schema_support import WrapperArtifacts, generate_wrapper_artifacts


def _split_sql_statements(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";\n") if statement.strip()]


def _resolve_source_schema(
    source_schema: str | None,
    materialized_family: MaterializedFamilyResult | None,
) -> str:
    if source_schema is None and materialized_family is None:
        raise ValueError("Either source_schema or materialized_family must be provided.")
    if source_schema is not None and materialized_family is not None:
        if validate_identifier("Source schema", source_schema) != materialized_family.source_schema:
            raise ValueError(
                f"source_schema={source_schema!r} does not match materialized_family.source_schema="
                f"{materialized_family.source_schema!r}"
            )
        return materialized_family.source_schema
    if materialized_family is not None:
        return materialized_family.source_schema
    return validate_identifier("Source schema", source_schema)


@dataclass(frozen=True)
class InSessionWrapperInstallResult:
    source_schema: str
    wrapper_schema: str
    helper_schema: str
    manifest: dict[str, Any]
    views_sql: str
    preprocessor_schema: str | None = None
    preprocessor_script: str | None = None
    preprocessor_library_script: str | None = None
    preprocessor_sql: str | None = None


def install_wrapper_views_in_session(
    con,
    *,
    source_schema: str | None = None,
    materialized_family: MaterializedFamilyResult | None = None,
    wrapper_schema: str = "JSON_VIEW",
    helper_schema: str | None = None,
) -> InSessionWrapperInstallResult:
    resolved_source_schema = _resolve_source_schema(source_schema, materialized_family)
    validated_wrapper_schema = validate_identifier("Wrapper schema", wrapper_schema)
    validated_helper_schema = validate_identifier(
        "Helper schema",
        helper_schema or f"{validated_wrapper_schema}_INTERNAL",
    )
    if len({resolved_source_schema, validated_wrapper_schema, validated_helper_schema}) != 3:
        raise ValueError(
            "Source schema, wrapper schema, and helper schema must all be distinct "
            f"(got source={resolved_source_schema}, wrapper={validated_wrapper_schema}, "
            f"helper={validated_helper_schema})."
        )

    artifacts: WrapperArtifacts = generate_wrapper_artifacts(
        con,
        resolved_source_schema,
        validated_wrapper_schema,
        validated_helper_schema,
    )
    for statement in _split_sql_statements(artifacts.sql):
        con.execute(statement)
    return InSessionWrapperInstallResult(
        source_schema=resolved_source_schema,
        wrapper_schema=validated_wrapper_schema,
        helper_schema=validated_helper_schema,
        manifest=artifacts.manifest,
        views_sql=artifacts.sql,
    )


def install_wrapper_preprocessor_in_session(
    con,
    *,
    wrapper_schema: str,
    helper_schema: str,
    manifest: dict[str, Any],
    schema: str = "JVS_WRAP_PP",
    script: str = "JSON_WRAPPER_PREPROCESSOR",
    function_names: list[str] | None = None,
    variant_typeof_function_names: list[str] | None = None,
    variant_varchar_function_names: list[str] | None = None,
    variant_decimal_function_names: list[str] | None = None,
    variant_boolean_function_names: list[str] | None = None,
    to_json_function_names: list[str] | None = None,
    blocked_helper_names: list[str] | None = None,
    blocked_helper_message: str = "This helper is not available on the wrapper surface yet.",
    activate_session: bool = False,
    reset_schema: bool = True,
) -> InSessionWrapperInstallResult:
    validated_schema = validate_identifier("Preprocessor schema", schema)
    validated_script = validate_identifier("Preprocessor script", script)
    validated_wrapper_schema = validate_identifier("Wrapper schema", wrapper_schema)
    validated_helper_schema = validate_identifier("Helper schema", helper_schema)
    validated_source_schema = validate_identifier("Source schema", manifest["sourceSchema"])

    install_json_export_helpers(con, validated_helper_schema)
    install_json_export_views(
        con,
        source_schema=validated_source_schema,
        schema=validated_helper_schema,
        udf_schema=validated_helper_schema,
    )

    sql_text = generate_wrapper_preprocessor_sql_text(
        schema=validated_schema,
        script=validated_script,
        wrapper_schemas=[validated_wrapper_schema],
        helper_schemas=[validated_helper_schema],
        manifests=[manifest],
        function_names=function_names or list(DEFAULT_EXPLICIT_NULL_FUNCTION_NAMES),
        variant_typeof_function_names=variant_typeof_function_names or list(DEFAULT_VARIANT_TYPEOF_FUNCTION_NAMES),
        variant_varchar_function_names=variant_varchar_function_names or list(DEFAULT_VARIANT_VARCHAR_FUNCTION_NAMES),
        variant_decimal_function_names=variant_decimal_function_names or list(DEFAULT_VARIANT_DECIMAL_FUNCTION_NAMES),
        variant_boolean_function_names=variant_boolean_function_names or list(DEFAULT_VARIANT_BOOLEAN_FUNCTION_NAMES),
        to_json_function_names=to_json_function_names or list(DEFAULT_TO_JSON_FUNCTION_NAMES),
        blocked_helper_names=blocked_helper_names or [],
        blocked_helper_message=blocked_helper_message,
        activate_session=activate_session,
    )
    from .wrapper_package_tool import execute_generated_preprocessor_sql

    if reset_schema:
        con.execute(f"DROP SCHEMA IF EXISTS {validated_schema} CASCADE")
    install_preprocessor_library(
        con,
        validated_schema,
        DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT,
    )
    execute_generated_preprocessor_sql(con, sql_text)
    if activate_session:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {validated_schema}.{validated_script}")

    return InSessionWrapperInstallResult(
        source_schema=manifest["sourceSchema"],
        wrapper_schema=validated_wrapper_schema,
        helper_schema=validated_helper_schema,
        manifest=manifest,
        views_sql="",
        preprocessor_schema=validated_schema,
        preprocessor_script=validated_script,
        preprocessor_library_script=DEFAULT_PREPROCESSOR_LIBRARY_SCRIPT,
        preprocessor_sql=sql_text,
    )


def install_wrapper_surface_in_session(
    con,
    *,
    source_schema: str | None = None,
    materialized_family: MaterializedFamilyResult | None = None,
    wrapper_schema: str = "JSON_VIEW",
    helper_schema: str | None = None,
    preprocessor_schema: str = "JVS_WRAP_PP",
    preprocessor_script: str = "JSON_WRAPPER_PREPROCESSOR",
    function_names: list[str] | None = None,
    variant_typeof_function_names: list[str] | None = None,
    variant_varchar_function_names: list[str] | None = None,
    variant_decimal_function_names: list[str] | None = None,
    variant_boolean_function_names: list[str] | None = None,
    to_json_function_names: list[str] | None = None,
    blocked_helper_names: list[str] | None = None,
    blocked_helper_message: str = "This helper is not available on the wrapper surface yet.",
    activate_preprocessor_session: bool = False,
    reset_preprocessor_schema: bool = True,
) -> InSessionWrapperInstallResult:
    views_result = install_wrapper_views_in_session(
        con,
        source_schema=source_schema,
        materialized_family=materialized_family,
        wrapper_schema=wrapper_schema,
        helper_schema=helper_schema,
    )
    preprocessor_result = install_wrapper_preprocessor_in_session(
        con,
        wrapper_schema=views_result.wrapper_schema,
        helper_schema=views_result.helper_schema,
        manifest=views_result.manifest,
        schema=preprocessor_schema,
        script=preprocessor_script,
        function_names=function_names,
        variant_typeof_function_names=variant_typeof_function_names,
        variant_varchar_function_names=variant_varchar_function_names,
        variant_decimal_function_names=variant_decimal_function_names,
        variant_boolean_function_names=variant_boolean_function_names,
        to_json_function_names=to_json_function_names,
        blocked_helper_names=blocked_helper_names,
        blocked_helper_message=blocked_helper_message,
        activate_session=activate_preprocessor_session,
        reset_schema=reset_preprocessor_schema,
    )
    return InSessionWrapperInstallResult(
        source_schema=views_result.source_schema,
        wrapper_schema=views_result.wrapper_schema,
        helper_schema=views_result.helper_schema,
        manifest=views_result.manifest,
        views_sql=views_result.views_sql,
        preprocessor_schema=preprocessor_result.preprocessor_schema,
        preprocessor_script=preprocessor_result.preprocessor_script,
        preprocessor_library_script=preprocessor_result.preprocessor_library_script,
        preprocessor_sql=preprocessor_result.preprocessor_sql,
    )
