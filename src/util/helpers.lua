local helpers = {}

function helpers.deep_copy(value)
    if type(value) ~= "table" then
        return value
    end
    local copy = {}
    for key, nested in pairs(value) do
        copy[key] = helpers.deep_copy(nested)
    end
    return copy
end

function helpers.starts_with(value, prefix)
    return string.sub(value, 1, string.len(prefix)) == prefix
end

function helpers.collect_tables(from_clause, out)
    out = out or {}
    if not from_clause then
        return out
    end
    if from_clause.type == "table" then
        out[#out + 1] = from_clause
    elseif from_clause.type == "join" then
        helpers.collect_tables(from_clause.left, out)
        helpers.collect_tables(from_clause.right, out)
    end
    return out
end

return helpers
