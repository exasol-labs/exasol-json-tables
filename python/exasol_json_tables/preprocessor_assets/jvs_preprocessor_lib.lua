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
__COMMON_LUA__

__ARRAY_ITERATION_LUA__

__JOIN_MODE_LUA__

__DISABLED_MODE_LUA__

    local function next_significant(tokens, index)
        local current = index
        while current <= #tokens and is_ignored(tokens[current]) do
            current = current + 1
        end
        return current
    end

    local function read_call(tokens, start_index)
        local identifier_index = next_significant(tokens, start_index)
        if identifier_index > #tokens then
            return nil
        end

        local current = identifier_index
        local identifier_parts = parse_identifier_token(tokens[current])
        if identifier_parts == nil then
            return nil
        end

        local last_identifier = normalize(identifier_parts[#identifier_parts])
        if last_identifier == nil then
            return nil
        end

        while true do
            local dot_index = next_significant(tokens, current + 1)
            if dot_index > #tokens or tokens[dot_index] ~= "." then
                break
            end
            local next_identifier = next_significant(tokens, dot_index + 1)
            if next_identifier > #tokens then
                return nil
            end
            current = next_identifier
            local next_identifier_parts = parse_identifier_token(tokens[current])
            if next_identifier_parts == nil then
                return nil
            end
            last_identifier = normalize(next_identifier_parts[#next_identifier_parts])
            if last_identifier == nil then
                return nil
            end
        end

        local opening_paren = next_significant(tokens, current + 1)
        if opening_paren > #tokens or tokens[opening_paren] ~= "(" then
            return nil
        end

        return {
            last_identifier = last_identifier,
            opening_paren = opening_paren
        }
    end

    local function find_matching_paren(tokens, opening_paren)
        local depth = 1
        local top_level_commas = 0
        local index = opening_paren + 1
        while index <= #tokens do
            if not is_ignored(tokens[index]) then
                if tokens[index] == "(" then
                    depth = depth + 1
                elseif tokens[index] == ")" then
                    depth = depth - 1
                    if depth == 0 then
                        return index, top_level_commas
                    end
                elseif tokens[index] == "," and depth == 1 then
                    top_level_commas = top_level_commas + 1
                end
            end
            index = index + 1
        end
        return nil, nil
    end

    local function has_expression_argument(tokens, opening_paren, closing_paren)
        for index = opening_paren + 1, closing_paren - 1 do
            if not is_ignored(tokens[index]) then
                return true
            end
        end
        return false
    end

    local function helper_query_targets_allowed_schema(sqltext)
        local path_tokens = tokenize_path_sql(sqltext)
        local base_binding = read_base_source_binding_from_path_tokens(path_tokens)
        if base_binding == nil then
            return false, json_schema_scope_example()
        end
        if base_binding.kind == "derived_source" then
            return false,
                    'JSON helper functions do not resolve through derived tables yet. '
                    .. 'Move the helper call into the inner SELECT or query the wrapper view directly.'
        end
        if base_binding.kind ~= "json_source" or not table_reference_is_in_allowed_schema(base_binding) then
            return false, json_schema_scope_example()
        end
        return true, nil
    end

__MARKER_HELPER_REWRITE_LUA__

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

__WRAPPER_HELPER_REWRITE_LUA__

    local function rewrite_path_identifiers_in_sql_dispatch(raw_sqltext)
        if REWRITE_PATH_IDENTIFIERS then
            return rewrite_path_identifiers_in_sql_join_mode(raw_sqltext)
        end
        return rewrite_path_identifiers_in_sql_disabled(raw_sqltext)
    end

    local function rewrite_helper_calls_in_sql_dispatch(raw_sqltext)
        if HELPER_REWRITE_MODE == "wrapper" then
            return rewrite_helper_calls_in_sql_wrapper_mode(raw_sqltext)
        end
        return rewrite_helper_calls_in_sql_marker_mode(raw_sqltext)
    end

    local rewritten_sql = rewrite_array_iteration_in_sql(sqltext)
    rewritten_sql = rewrite_path_identifiers_in_sql_dispatch(rewritten_sql)
    rewritten_sql = rewrite_helper_calls_in_sql_dispatch(rewritten_sql)
    return rewritten_sql
end
