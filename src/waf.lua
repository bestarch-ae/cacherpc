local waf = { }

local json = require 'json'

function waf.request(data)
        ------------------------------------------
        --- getProgramAccounts
        ------------------------------------------
        if data.method == 'getProgramAccounts' then
                local params = json.decode(data.params)
                --- TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA logic: required dataSize = 165 and memcmp filters
                if (params[1] == 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') then
                        if params[2] == nil then
                                return false, 'More parameters required for the TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA key'
                        end
                        local filters = params[2].filters
                    	if filters == nil then
                                return false, 'There are no filters in request, required for this TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA key'
                        else
                                local memcmp_cnt = false
                                local datasize_cnt = false
                                for i, filter in ipairs(filters) do
                                        for name, result in pairs(filter) do
                                                if name == 'memcmp' then
                                                        memcmp_cnt = true
						                        end
                                                if (name == 'dataSize') and (result == 165) then
                                                        datasize_cnt = true
                                                end
                                        end
                                end
				                if (memcmp_cnt ~= true) or (datasize_cnt ~= true) then
                                    return false, 'There are no filters with memcmp and datasize in request, required for this TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA key'
                                end
                        end
                --- 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin logic: required any filter
		        elseif (params[1] == '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin') then
                        if params[2] == nil then
                                return false, 'More parameters required for the the 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin key'
                        end
                        local filters = params[2].filters
                        if filters == nil then
                                return false, 'There are no filters in request, required for the 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin key'
                        end
                end
	    ------------------------------------------
        --- getTokenAccountsByDelegate
        ------------------------------------------
	    elseif data.method == 'getTokenAccountsByDelegate' then
                local params = json.decode(data.params)
                --- Do not allow programId filter = TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
                if params[1] then
                        if params[2] == nil then
                                return false, 'There are no parameters required for the pubkey'
                        else
                                if params[2].programId == 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' then
                                        return false, 'Invalid programId, getTokenAccountsByDelegate is prohibited for TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                                end
                        end

		        end
	    ------------------------------------------
        --- getSignatureStatuses
        ------------------------------------------
	    elseif data.method == 'getSignaturesForAddress' then
                local params = json.decode(data.params)
                --- Do not allow request for Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR, 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin
                if (params[1] == 'Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR') or (params[1] == '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin') then
                	return false, 'Invalid key, getSignatureForAddress is not allowed for these keys'
                end
        end
        return true, ''
end

return waf
