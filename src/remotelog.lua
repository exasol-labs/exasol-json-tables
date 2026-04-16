local levels = {NONE = 1, FATAL = 2, ERROR = 3, WARN = 4, INFO = 5, CONFIG = 6, DEBUG = 7, TRACE = 8}
local fallback_strategies = {CONSOLE = 1, DISCARD = 2, ERROR = 3}

---
-- This module implements a remote log client with the ability to fall back to console logging in case no connection
-- to a remote log receiver is established.
-- <p>
-- You can optionally use a high resolution timer for performance monitoring. Since Lua's <code>os.date()</code>
-- function only has a resolution of seconds, that timer uses <code>socket.gettime()</code>. Note that the values
-- you are getting are not the milliseconds of a second, but the milliseconds counted from when the module was first
-- loaded &mdash; which is typically at the very beginning of the software using this module.
-- </p><p>
-- Use the <code>init()</code> method to set some global parameters for this module.
-- </p>
--
local M = {
    VERSION = "1.1.1",
    level = levels.INFO,
    socket_client = nil,
    connection_timeout = 0.1, -- seconds
    log_client_name = nil,
    timestamp_pattern = "%Y-%m-%d %H:%M:%S", -- example: 2020-09-02 13:56:01
    start_nanos = 0,
    use_high_resolution_time = true,
    fallback_strategies = fallback_strategies,
    fallback_strategy = fallback_strategies.CONSOLE
}

local socket = require("socket")

---
-- Initialize the log module
-- <p>
-- This method allows you to set parameters that apply to all subsequent calls to logging methods. While it is possible
-- to change these settings at runtime, the recommended way is to do this once only, before you use the log for the
-- first time.
-- </p>
-- <p>
-- You can use a high resolution timer. Note that this are <b>not</b> the sub-second units of the timestamp! Lua
-- timestamps only offer second resolution. Rather you get a time difference in milliseconds counted from the first time
-- the log is opened.
--
-- @param timestamp_pattern layout of timestamps displayed in the logs
--
-- @param use_high_resolution_time switch high resolution time display on or off (default)
--
-- @param fallback_strategy what to do if the remote listener connection cannot be established (default: console log)
--
-- @return module loader
--
function M.init(timestamp_pattern, use_high_resolution_time, fallback_strategy)
    if timestamp_pattern then
        M.timestamp_pattern = timestamp_pattern
    end
    M.use_high_resolution_time = use_high_resolution_time or false
    M.fallback_strategy = fallback_strategy or fallback_strategies.CONSOLE
    return M
end

---
-- Set the log client name.
-- <p>
-- This is the name presented when the log is first opened. We recommend using the name of the application or script
-- that uses the log and a version number.
-- </p>
--
function M.set_client_name(log_client_name)
    M.log_client_name = log_client_name
end

local function start_high_resolution_timer()
    if M.use_high_resolution_time then
        M.start_nanos = socket.gettime()
    end
end

local function get_level_name(level)
    for key, value in pairs(levels) do
        if value == level then
            return key
        end
    end
    error("E-LOG-1: Unable to determine log level name for level number " .. level .. ".")
end

local function fallback_print(...)
    if M.fallback_strategy == fallback_strategies.DISCARD then
        return
    elseif M.fallback_strategy == fallback_strategies.CONSOLE then
        if print then print(...) end
    else
        error(string.format(...))
    end
end

---
-- Open a connection to a remote log receiver.
-- <p>
-- This method allows connecting the log to an external process listening on a TCP port. The process can be on a remote
-- host. If the connection cannot be established, the logger falls back to console logging.
-- </p>
-- <p>
-- If you don't use the <code>connect()</code> function, you get regular console logging.
-- </p>
--
-- @param host remote host on which the logging process runs
--
-- @param port TCP port on which the logging process listens
--
function M.connect(host, port)
    local tcp_socket = socket.tcp()
    tcp_socket:settimeout(M.connection_timeout)
    local ok, err = tcp_socket:connect(host, port)
    local log_client_prefix = M.log_client_name and (M.log_client_name .. ": ") or ""
    if ok then
        M.socket_client = tcp_socket
        M.info("%sConnected to log receiver listening on %s:%d with log level %s. Time zone is UTC%s.",
            log_client_prefix, host, port, get_level_name(M.level), os.date("%z"))
    else
        fallback_print(log_client_prefix .. "W-LOG-2: Unable to open socket connection to " .. host .. ":" .. port
            .. " for sending log messages. Falling back to console logging with log level "
            .. get_level_name(M.level) .. ". Timezone is UTC" .. os.date("%z") .. ". Caused by: " .. err)
    end
end

---
-- Close the connection to the remote log receiver.
--
function M.disconnect()
    if(M.socket_client) then
        M.socket_client:close()
    end
end

---
-- Set the log level.
--
-- @param level_name name of the log level, one of: FATAL, ERROR, WARN, INFO, CONFIG, DEBUG, TRACE
--
function M.set_level(level_name)
    local level = levels[level_name]
    if not level then
        M.warn('W-LOG-1: Attempt to set illegal log level "%s". Pick one of: NONE, FATAL, ERROR, WARN, INFO, CONFIG,'
            .. ' DEBUG, TRACE. Falling back to level INFO.', level_name)
        M.level = levels.INFO
    else
        M.level = level
    end
end

---
-- Write to a socket, print or discard message.
-- <p>
-- If a socket connection is established, this method writes to that socket. Otherwise if the global print function
-- exists (e.g. in a unit test) falls back to logging via <code>print()</code>.
-- </p><p>
-- Exasol removed print() in it's Lua implementation, so there is no fallback on a real Exasol instance. You either use
-- remote logging or messages are discarded immediately.
-- </p>
--
-- @param level log level
--
-- @param message log message; otherwise used as format string if any variadic parameters follow
--
-- @param ... parameters to be inserted into formatted message (optional)
--
local function write(level, message, ...)
    if not (M.socket_client or print) then
        return
    else
        local entry
        local formatted_message = (select('#', ...) > 0) and string.format(message, ...) or message
        if M.use_high_resolution_time then
            local current_millis = string.format("%07.3f", (socket.gettime() - M.start_nanos) * 1000)
            entry = {
                os.date(M.timestamp_pattern),
                " (", current_millis, "ms) [", level , "]",
                string.rep(" ", 7 - string.len(level)), formatted_message
            }
        else
            entry = {
                os.date(M.timestamp_pattern),
                " [", level , "]",
                string.rep(" ", 7 - string.len(level)), formatted_message
            }
        end
        if M.socket_client then
            entry[#entry + 1] = "\n"
            M.socket_client:send(table.concat(entry))
        else
            fallback_print(table.concat(entry))
        end
    end
end

---
-- Write a log message on level <code>FATAL</code>.
-- <p>
-- You should use this in cases where you directly need to terminate the running program afterwards. I.e. in case of
-- non-recoverable errors (e.g. data corruption).
-- </p>
--
-- @see info for details about the function paramters
--
-- @param ... log message or message pattern with placeholders and values
--
function M.fatal(...)
    if M.level >= levels.FATAL then
        write("FATAL", ...)
    end
end

---
-- Write a log message on level <code>ERROR<code>.
-- <p>
-- Log potentially recoverable errors on this level.
-- </p>
--
-- @see info for details about the function paramters
--
-- @param ... log message or message pattern with placeholders and values
--
function M.error(...)
    if M.level >= levels.ERROR then
        write("ERROR", ...)
    end
end

---
-- Write a log message on level <code>WARN<code>.
-- <p>
-- Log problems that either are recovered from automatically or do not have immediate adverse effects on this level.
-- </p>
--
-- @see info for details about the function paramters
--
-- @param ... log message or message pattern with placeholders and values
--
function M.warn(...)
    if M.level >= levels.WARN then
        write("WARN", ...)
    end
end

---
-- Write a log message on level <code>info</code>.
-- <p>
-- We recommend using this scarcely and for non-repeating messages only, since this is the default log level. Otherwise
-- a regular log will be cluttered.
-- </p>
-- <p> The parameters can either be a single parameter which will be written to the log as-is. In case multiple
-- parameters are used, the first is treated as message pattern with placeholders as used in the standard library's
-- <code>string.format(...)</code> function.
-- <p>
--
-- @param ... log message or message pattern with placeholders and values
--
function M.info(...)
    if M.level >= levels.INFO then
        write("INFO", ...)
    end
end

---
-- Write a log message on level <code>CONFIG</code>.
-- <p>
-- Messages on this level should be used to log program configuration or environment information.
-- </p>
--
-- @see info for details about the function paramters
--
-- @param ... log message or message pattern with placeholders and values
--
function M.config(...)
    if M.level >= levels.CONFIG then
        write("CONFIG", ...)
    end
end

---
-- Write a log message on level <code>DEBUG</code>.
-- <p>
-- Log information that helps analyzing program flow and error causes on this level.
-- </p>
--
-- @see info for details about the function paramters
--
-- @param ... log message or message pattern with placeholders and values
--
function M.debug(...)
    if M.level >= levels.DEBUG then
        write("DEBUG", ...)
    end
end

---
-- Write a log message on level <code>TRACE</code>.
-- <p>
-- Use this log level for the most details logging information, like internal program state, method entry and exit.
-- parameter values and all other details that are only of interest for someone with intimate knowledge of the internal
-- workings of the program.
-- </p>
--
-- @see info for details about the function paramters
--
-- @param ... log message or message pattern with placeholders and values
--
function M.trace(...)
    if M.level >= levels.TRACE then
        write("TRACE", ...)
    end
end

start_high_resolution_timer()
return M
