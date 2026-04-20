from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .generate_preprocessor_sql import (
    ARRAY_ITERATION_LUA,
    COMMON_LUA,
    DISABLED_MODE_LUA,
    JOIN_MODE_LUA,
    MARKER_HELPER_REWRITE_LUA,
    WRAPPER_EXPLICIT_NULL_HELPER_LUA,
)


LIBRARY_TEMPLATE_PATH = Path(__file__).resolve().parent / "preprocessor_assets" / "jvs_preprocessor_lib.lua"


@dataclass(frozen=True)
class LibraryModule:
    name: str
    placeholder: str
    body: str


HELPER_CORE_LUA = """
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
"""


RUNTIME_PIPELINE_LUA = """
    local function raw_text_reference_known_helper(raw_sqltext_upper)
        if string.find(raw_sqltext_upper, "TO_JSON", 1, true) ~= nil then
            return true
        end
        for function_name in pairs(HELPER_KIND_BY_NAME) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        for function_name in pairs(BLOCKED_FUNCTIONS) do
            if string.find(raw_sqltext_upper, function_name, 1, true) ~= nil then
                return true
            end
        end
        return false
    end

    local function quoted_identifier_contains_path_syntax(raw_sqltext)
        local index = 1
        while true do
            local start_index = string.find(raw_sqltext, '"', index, true)
            if start_index == nil then
                return false
            end
            local current = start_index + 1
            while current <= #raw_sqltext do
                local ch = string.sub(raw_sqltext, current, current)
                if ch == '"' then
                    if string.sub(raw_sqltext, current + 1, current + 1) == '"' then
                        current = current + 2
                    else
                        local identifier_text = string.sub(raw_sqltext, start_index + 1, current - 1)
                        if string.find(identifier_text, ".", 1, true) ~= nil
                                or string.find(identifier_text, "[", 1, true) ~= nil then
                            return true
                        end
                        index = current + 1
                        break
                    end
                else
                    current = current + 1
                end
            end
            if current > #raw_sqltext then
                return false
            end
        end
    end

    local function raw_text_might_need_iterator_rewrite(raw_sqltext)
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, ' IN "', 1, true) ~= nil then
            return true
        end
        return string.find(raw_sqltext, '%f[%w]IN%f[%W]%s*[A-Za-z_][A-Za-z0-9_]*%s*%.%s*"', 1) ~= nil
    end

    local function query_might_need_helper_rewrite(raw_sqltext)
        return raw_text_reference_known_helper(string.upper(raw_sqltext))
    end

    local function query_might_need_path_rewrite(raw_sqltext)
        if not REWRITE_PATH_IDENTIFIERS then
            return false
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        return false
    end

    local function query_might_need_shared_walker_rewrite(raw_sqltext)
        return query_might_need_path_rewrite(raw_sqltext) or query_might_need_helper_rewrite(raw_sqltext)
    end

    local function query_might_need_runtime_rewrite(raw_sqltext)
        if query_might_need_helper_rewrite(raw_sqltext) then
            return true
        end
        if string.find(raw_sqltext, '"', 1, true) == nil then
            return false
        end
        if string.find(raw_sqltext, "[", 1, true) ~= nil then
            return true
        end
        if quoted_identifier_contains_path_syntax(raw_sqltext) then
            return true
        end
        if raw_text_might_need_iterator_rewrite(raw_sqltext) then
            return true
        end
        return false
    end

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

    local function rewrite_query_block_pipeline_sql(query_sql)
        local rewritten_sql = query_sql
        if query_might_need_path_rewrite(query_sql) then
            rewritten_sql = rewrite_path_query_block_sql(rewritten_sql)
        end
        if HELPER_REWRITE_MODE == "wrapper" then
            if query_might_need_helper_rewrite(rewritten_sql) then
                return rewrite_helper_query_block_sql(rewritten_sql)
            end
            return rewritten_sql
        end
        if query_might_need_helper_rewrite(rewritten_sql) then
            return rewrite_helper_query_block_sql_marker_mode(rewritten_sql)
        end
        return rewritten_sql
    end

    local function rewrite_with_shared_query_block_walker(raw_sqltext)
        return rewrite_sql_with_query_blocks(raw_sqltext, rewrite_query_block_pipeline_sql)
    end

    if not query_might_need_runtime_rewrite(sqltext) then
        return sqltext
    end

    local rewritten_sql = rewrite_array_iteration_in_sql(sqltext)
    if not query_might_need_shared_walker_rewrite(rewritten_sql) then
        return rewritten_sql
    end
    return rewrite_with_shared_query_block_walker(rewritten_sql)
"""


def _rename_local_function(block: str, original_name: str, renamed_name: str) -> str:
    original = f"local function {original_name}("
    replacement = f"local function {renamed_name}("
    if original not in block:
        raise ValueError(f"Could not find Lua function {original_name!r} in the shared preprocessor block.")
    return block.replace(original, replacement, 1)


def _format_module_block(module: LibraryModule) -> str:
    return f"-- [module: {module.name}]\n{module.body.strip(chr(10))}"


def compact_lua_body(body: str) -> str:
    compact_lines: list[str] = []
    last_blank = False
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if stripped == "":
            if last_blank:
                continue
            last_blank = True
            compact_lines.append("")
            continue
        last_blank = False
        compact_lines.append(stripped)
    return "\n".join(compact_lines).strip() + "\n"


def iter_preprocessor_library_modules() -> tuple[LibraryModule, ...]:
    return (
        LibraryModule("parser_core", "__PARSER_CORE_LUA__", COMMON_LUA),
        LibraryModule("array_iteration", "__ARRAY_ITERATION_LUA__", ARRAY_ITERATION_LUA),
        LibraryModule(
            "path_rewrite",
            "__PATH_REWRITE_LUA__",
            _rename_local_function(
                JOIN_MODE_LUA,
                "rewrite_path_identifiers_in_sql",
                "rewrite_path_identifiers_in_sql_join_mode",
            ),
        ),
        LibraryModule(
            "path_rewrite_disabled",
            "__PATH_REWRITE_DISABLED_LUA__",
            _rename_local_function(
                DISABLED_MODE_LUA,
                "rewrite_path_identifiers_in_sql",
                "rewrite_path_identifiers_in_sql_disabled",
            ),
        ),
        LibraryModule("helper_core", "__HELPER_CORE_LUA__", HELPER_CORE_LUA),
        LibraryModule("helper_rewrite_marker", "__HELPER_MARKER_LUA__", MARKER_HELPER_REWRITE_LUA),
        LibraryModule(
            "helper_rewrite_wrapper",
            "__HELPER_WRAPPER_LUA__",
            _rename_local_function(
                WRAPPER_EXPLICIT_NULL_HELPER_LUA,
                "rewrite_helper_calls_in_sql",
                "rewrite_helper_calls_in_sql_wrapper_mode",
            ),
        ),
        LibraryModule("runtime_pipeline", "__RUNTIME_PIPELINE_LUA__", RUNTIME_PIPELINE_LUA),
    )


def generate_preprocessor_library_body(*, compact: bool = False) -> str:
    template = LIBRARY_TEMPLATE_PATH.read_text()
    for module in iter_preprocessor_library_modules():
        template = template.replace(module.placeholder, _format_module_block(module))
    rendered = template.strip() + "\n"
    if compact:
        return compact_lua_body(rendered)
    return rendered
