-- Shared JSON Tables preprocessor runtime library.
function rewrite(sqltext, config)
    local HELPER_KIND_BY_NAME = config.helper_kind_by_name or {}
    local BLOCKED_FUNCTIONS = config.blocked_functions or {}
    local BLOCKED_FUNCTION_MESSAGE = config.blocked_function_message or "This helper is not available in this build."
    local ALLOWED_JSON_SCHEMAS = config.allowed_json_schemas or {}
    local ALLOWED_JSON_SCHEMA_LIST = config.allowed_json_schema_list or ""
    local EXAMPLE_ALLOWED_SCHEMA = config.example_allowed_schema or "JSON_VIEW"
    local HELPER_SCHEMA_BY_ALLOWED_SCHEMA = config.helper_schema_by_allowed_schema or {}
    local GROUP_CONFIG_BY_SCHEMA_AND_TABLE = config.group_config_by_schema_and_table or {}
    local VISIBLE_COLUMNS_BY_SCHEMA_AND_TABLE = config.visible_columns_by_schema_and_table or {}
    local TO_JSON_CONFIG_BY_SCHEMA_AND_TABLE = config.to_json_config_by_schema_and_table or {}
    local REGULAR_TO_JSON_ROW_OBJECT_FUNCTION = config.regular_to_json_row_object_function or ""
    local HELPER_REWRITE_MODE = config.helper_rewrite_mode or "marker"
    local REWRITE_PATH_IDENTIFIERS = config.rewrite_path_identifiers == true

    local function raise_function_error(function_name, message)
        error("JVS-FUNCTION-ERROR: " .. function_name .. ": " .. message, 0)
    end

    local function raise_scope_error(feature_name, message)
        error(
            "JVS-SCOPE-ERROR: " .. feature_name
                .. " is only available for configured JSON schemas ("
                .. ALLOWED_JSON_SCHEMA_LIST .. "). "
                .. message,
            0
        )
    end

    local function json_schema_scope_example()
        return 'Qualify the JSON table in FROM/JOIN using one of the configured JSON schemas, '
                .. 'for example FROM "' .. EXAMPLE_ALLOWED_SCHEMA .. '"."<ROOT_TABLE>".'
    end

    local function normalize(token)
        return sqlparsing.normalize(token)
    end

    local function is_ignored(token)
        return sqlparsing.iswhitespaceorcomment(token)
    end
__PARSER_CORE_LUA__

__ARRAY_ITERATION_LUA__

__PATH_REWRITE_LUA__

__PATH_REWRITE_DISABLED_LUA__

__HELPER_CORE_LUA__

__HELPER_MARKER_LUA__

    local function rewrite_helper_query_block_sql_marker_mode(sqltext)
        local tokens = sqlparsing.tokenize(sqltext)
        local helper_call_replacements = collect_helper_call_replacements(sqltext, tokens)
        local out = {}
        local index = 1
        while index <= #tokens do
            local call = read_call(tokens, index)
            if call and BLOCKED_FUNCTIONS[call.last_identifier] then
                raise_function_error(call.last_identifier, BLOCKED_FUNCTION_MESSAGE)
            end

            local replacement = helper_call_replacements[index]
            if replacement ~= nil then
                out[#out + 1] = replacement.replacement_sql
                index = replacement.closing_paren + 1
            else
                out[#out + 1] = tokens[index]
                index = index + 1
            end
        end
        return table.concat(out)
    end

    local function rewrite_helper_calls_in_sql_marker_mode(raw_sqltext)
        return rewrite_sql_with_query_blocks(raw_sqltext, rewrite_helper_query_block_sql_marker_mode)
    end

__HELPER_WRAPPER_LUA__

__RUNTIME_PIPELINE_LUA__
end
