--- Appender for scalar functions in an SQL statement.
---@class ScalarFunctionAppender: AbstractQueryAppender
local ScalarFunctionAppender = {}
ScalarFunctionAppender.__index = ScalarFunctionAppender
local AbstractQueryAppender = require("exasol.vscl.queryrenderer.AbstractQueryAppender")
setmetatable(ScalarFunctionAppender, {__index = AbstractQueryAppender})

local ExpressionAppender = require("exasol.vscl.queryrenderer.ExpressionAppender")
local ExaError = require("ExaError")

--- Create a new instance of a `ScalarFunctionAppender`.
---@param out_query Query query to which the function will be appended
---@param appender_config AppenderConfig
---@return ScalarFunctionAppender renderer for scalar functions
function ScalarFunctionAppender:new(out_query, appender_config)
    local instance = setmetatable({}, self)
    instance:_init(out_query, appender_config)
    return instance
end

---@param out_query Query query to which the function will be appended
---@param appender_config AppenderConfig
function ScalarFunctionAppender:_init(out_query, appender_config)
    AbstractQueryAppender._init(self, out_query, appender_config)
end

--- Append a scalar function to an SQL query.
---@param scalar_function ScalarFunctionExpression function to append
function ScalarFunctionAppender:append_scalar_function(scalar_function)
    local function_name = string.lower(scalar_function.name)
    local implementation = ScalarFunctionAppender["_" .. function_name]
    if implementation ~= nil then
        implementation(self, scalar_function)
    else
        ExaError:new("E-VSCL-3", "Unable to render unsupported scalar function type {{function_name}}.", {
            function_name = {value = function_name, description = "name of the SQL function that is not yet supported"}
        }):add_ticket_mitigation():raise()
    end
end

-- Alias for main appender function for uniform appender invocation
ScalarFunctionAppender.append = ScalarFunctionAppender.append_scalar_function

---@param expression Expression
function ScalarFunctionAppender:_append_expression(expression)
    local expression_renderer = ExpressionAppender:new(self._out_query, self._appender_config)
    expression_renderer:append_expression(expression)
end

---@param arguments Expression[]
function ScalarFunctionAppender:_append_function_argument_list(arguments)
    self:_append("(")
    if (arguments) then
        for i = 1, #arguments do
            self:_comma(i)
            self:_append_expression(arguments[i])
        end
    end
    self:_append(")")
end

---@param left Expression
---@param operator string
---@param right Expression
function ScalarFunctionAppender:_append_arithmetic_function(left, operator, right)
    self:_append_expression(left)
    self:_append(" ")
    self:_append(operator)
    self:_append(" ")
    self:_append_expression(right)
end

function ScalarFunctionAppender:_append_parameterless_function(scalar_function)
    self:_append(scalar_function.name)
end

function ScalarFunctionAppender:_append_simple_function(f)
    self:_append(string.upper(f.name))
    self:_append_function_argument_list(f.arguments)
end

-- Numeric functions
ScalarFunctionAppender._abs = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._acos = ScalarFunctionAppender._append_simple_function

function ScalarFunctionAppender:_add(f)
    self:_append_arithmetic_function(f.arguments[1], "+", f.arguments[2])
end

ScalarFunctionAppender._asin = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._atan = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._atan = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._atan2 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._ceil = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._cos = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._cosh = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._cot = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._degrees = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._div = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._exp = ScalarFunctionAppender._append_simple_function

function ScalarFunctionAppender:_float_div(f)
    self:_append_arithmetic_function(f.arguments[1], "/", f.arguments[2])
end

ScalarFunctionAppender._floor = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._ln = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._log = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._min_scale = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._mod = ScalarFunctionAppender._append_simple_function

function ScalarFunctionAppender:_mult(f)
    self:_append_arithmetic_function(f.arguments[1], "*", f.arguments[2])
end

function ScalarFunctionAppender:_neg(f)
    self:_append("-")
    self:_append_expression(f.arguments[1])
end

ScalarFunctionAppender._pi = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._power = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._radians = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._rand = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._round = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._sign = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._sin = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._sinh = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._sqrt = ScalarFunctionAppender._append_simple_function

function ScalarFunctionAppender:_sub(f)
    self:_append_arithmetic_function(f.arguments[1], "-", f.arguments[2])
end

ScalarFunctionAppender._tan = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._tanh = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._to_char = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._to_number = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._trunc = ScalarFunctionAppender._append_simple_function

-- String functions
ScalarFunctionAppender._ascii = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_length = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._chr = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._cologne_phonetic = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._concat = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._dump = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._edit_distance = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._initcap = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._insert = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._instr = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._left = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._length = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._locate = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._lower = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._lpad = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._ltrim = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._octet_length = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._regexp_instr = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._regexp_substr = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._repeat = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._replace = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._reverse = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._right = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._rpad = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._rtrim = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._soundex = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._space = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._substr = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._translate = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._trim = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._unicode = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._unicodechr = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._upper = ScalarFunctionAppender._append_simple_function

-- Date / time functions
ScalarFunctionAppender._add_days = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._add_hours = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._add_minutes = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._add_months = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._add_seconds = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._add_weeks = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._add_years = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._convert_tz = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._current_date = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._current_timestamp = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._date_trunc = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._day = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._days_between = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._dbtimezone = ScalarFunctionAppender._append_parameterless_function

function ScalarFunctionAppender:_extract(f)
    local to_extract = string.upper(f.toExtract)
    self:_append("EXTRACT(")
    self:_append(to_extract)
    self:_append(" FROM ")
    self:_append_expression(f.arguments[1])
    self:_append(")")
end

ScalarFunctionAppender._from_posix_time = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hour = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hours_between = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._localtimestamp = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._minute = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._minutes_between = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._month = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._months_between = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._numtodsinterval = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._numtoyminterval = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._posix_time = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._second = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._seconds_between = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._sessiontimezone = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._sysdate = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._systimestamp = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._to_date = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._to_dsinterval = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._to_timestamp = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._to_yminterval = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._week = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._year = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._years_between = ScalarFunctionAppender._append_simple_function

-- Geospatial functions
-- Point functions
ScalarFunctionAppender._st_x = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_y = ScalarFunctionAppender._append_simple_function

-- Linestring functions
ScalarFunctionAppender._st_endpoint = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_isclosed = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_isring = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_length = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_numpoints = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_pointn = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_startpoint = ScalarFunctionAppender._append_simple_function

-- Polygon functions
ScalarFunctionAppender._st_area = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_exteriorring = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_interiorringn = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_numinteriorrings = ScalarFunctionAppender._append_simple_function

-- Geometry collection functions
ScalarFunctionAppender._st_geometryn = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_numgeometries = ScalarFunctionAppender._append_simple_function

-- General geospatial functions
ScalarFunctionAppender._st_boundary = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_buffer = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_centroid = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_contains = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_convexhull = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_crosses = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_difference = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_dimension = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_disjoint = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_distance = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_envelope = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_equals = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_force2d = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_geometrytype = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_intersection = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_intersects = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_isempty = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_issimple = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_overlaps = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_setsrid = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_symdifference = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_touches = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_transform = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_union = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._st_within = ScalarFunctionAppender._append_simple_function

-- Bitwise functions
ScalarFunctionAppender._bit_and = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_check = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_lrotate = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_lshift = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_not = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_or = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_rrotate = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_rshift = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_set = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_to_num = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._bit_xor = ScalarFunctionAppender._append_simple_function

-- Conversion functions
function ScalarFunctionAppender:_cast(f)
    self:_append("CAST(")
    self:_append_expression(f.arguments[1])
    self:_append(" AS ")
    self:_append_data_type(f.dataType)
    self:_append(")")
end

-- Other functions
function ScalarFunctionAppender:_case(f)
    local arguments = f.arguments
    local results = f.results
    self:_append("CASE ")
    self:_append_expression(f.basis)
    for i = 1, #arguments do
        local argument = arguments[i]
        local result = results[i]
        self:_append(" WHEN ")
        self:_append_expression(argument)
        self:_append(" THEN ")
        self:_append_expression(result)
    end
    if (#results > #arguments) then
        self:_append(" ELSE ")
        self:_append_expression(results[#results])
    end
    self:_append(" END")
end

ScalarFunctionAppender._current_schema = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._current_session = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._current_statement = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._current_user = ScalarFunctionAppender._append_parameterless_function
ScalarFunctionAppender._greatest = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hash_md5 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hashtype_md5 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hash_sha1 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hashtype_sha1 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hash_sha256 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hashtype_sha256 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hash_sha512 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hashtype_sha512 = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hash_tiger = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._hashtype_tiger = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._is_boolean = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._is_date = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._is_dsinterval = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._is_number = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._is_timestamp = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._is_yminterval = ScalarFunctionAppender._append_simple_function

function ScalarFunctionAppender:_json_value(f)
    local arguments = f.arguments
    local empty_behavior = f.emptyBehavior
    local error_behavior = f.errorBehavior
    self:_append("JSON_VALUE(")
    self:_append_expression(arguments[1])
    self:_append(", ")
    self:_append_expression(arguments[2])
    self:_append(" RETURNING ")
    self:_append_data_type(f.dataType)
    self:_append(" ")
    self:_append(empty_behavior.type)
    if empty_behavior.type == "DEFAULT" then
        self:_append(" ")
        self:_append_expression(empty_behavior.expression)
    end
    self:_append(" ON EMPTY ")
    self:_append(error_behavior.type)
    if error_behavior.type == "DEFAULT" then
        self:_append(" ")
        self:_append_expression(error_behavior.expression)
    end
    self:_append(" ON ERROR)")
end

ScalarFunctionAppender._least = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._nullifzero = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._session_parameter = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._sys_guid = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._typeof = ScalarFunctionAppender._append_simple_function
ScalarFunctionAppender._zeroifnull = ScalarFunctionAppender._append_simple_function

return ScalarFunctionAppender
