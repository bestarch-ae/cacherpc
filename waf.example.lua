local waf = { }

local json = require 'json'

function waf.request(data)
	if request.method == 'getAccountInfo' then
		local params = json.decode(request.params)
		if params[1] == '58KcficuUqPDcMittSddhT8LzsPJoH46YP4uURoMo5EB' then
			return true
		end
	end
end

return waf
