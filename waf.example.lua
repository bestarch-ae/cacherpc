-- WAF rules:
-- Request data that is passed to waf.request function
-- has access to 3 fields:
-- jsonrpc - version of jsonrpc protocol used for the request
-- method - remote method name, that should be invoked
-- params - parameter list (json encoded) for the requested method
--
-- All the rules should be defined inside of waf.request function as 
-- a chain of if else statements.
-- The function is required to return a tuple of bool and string values
-- bool should indicated whether to accept or reject the request, 
-- true corresponding to acceptance of request. In case of rejection the string should
-- contain an explanation of what rule rejected the request and why, in case of
-- acceptance, string can be empty but not nil.

local waf = { }

local json = require 'json'

function waf.request(data)
	if data.method == 'getAccountInfo' then
		local params = json.decode(data.params)
		if params[1] == '58KcficuUqPDcMittSddhT8LzsPJoH46YP4uURoMo5EB' then
			return true, ''
        else
            return false, 'Account not in a WAF white list'
		end
	end
    return true, ''
end

return waf
