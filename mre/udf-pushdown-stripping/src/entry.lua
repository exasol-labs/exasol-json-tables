local Adapter = require("mre_adapter.PassthroughAdapter")
local Properties = require("mre_adapter.AdapterProperties")
local Dispatcher = require("exasol.vscl.RequestDispatcher")

function adapter_call(request_json)
    local adapter = Adapter:new(_G.exa)
    local dispatcher = Dispatcher:new(adapter, Properties)
    return dispatcher:adapter_call(request_json)
end
