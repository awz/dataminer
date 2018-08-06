--------------------------------------------------------
-- Minetest :: DataMiner Mod v2.10 (dataminer)
--
-- See README.txt for licensing and release notes.
-- Copyright (c) 2017-2018, Leslie E. Krause
--------------------------------------------------------

local TX_SESSION_OPENED = 50
local TX_SESSION_CLOSED = 51
local LOG_STARTED = 10
local LOG_CHECKED = 11
local LOG_STOPPED = 12
local LOG_INDEXED = 13

JournalIndex = function ( path, name )
	local journal, err = io.open( path .. "/" .. name, "r" )
	if not journal then
		error( "Cannot open journal file for reading." )
	end

	local self = { }
	local catalog = { }

	self.get_players = function ( period )
		if catalog[ period ] then return catalog[ period ].players end
	end

	self.get_is_online = function ( period )
		if catalog[ period ] then return catalog[ period ].is_online end
	end

	self.records = function ( period )
		if not catalog[ period ] then return function ( ) end end

		-- iterate over only transactions in specified period
		local count = catalog[ period ].length
		journal:seek( "set", catalog[ period ].cursor )

		return function ( )
			if count == 0 then
				count = count - 1
				return os.time( ), LOG_INDEXED, { }
			elseif count > 0 then
				-- sanity check, altho read should never return nil
				local record = assert( journal:read( "*line" ) )
                	        local fields = string.split( record, " ", true )
				count = count - 1
				return tonumber( fields[ 1 ] ), tonumber( fields[ 2 ] ), { select( 3, unpack( fields ) ) }
			end
                end
        end

	self.prepare = function ( on_repeat )
		local period
		local length = 0
		local cursor = 0
		local players = { }
		local is_online = false

		for record in journal:lines( ) do
			local fields = string.split( record, " ", true )
			local optime = tonumber( fields[ 1 ] )
			local opcode = tonumber( fields[ 2 ] )

			if not period or optime >= period * 86400 + 86400 then
				if period then catalog[ period ].length = length end
				local names = { }
				for k, v in pairs( players ) do
					table.insert( names, k )
				end
				period = math.floor( optime / 86400 )
				catalog[ period ] = { cursor = cursor, players = names, is_online = is_online }
				length = 0
			end

			if opcode == LOG_STARTED then
				players = { }
				is_online = true
			elseif opcode == LOG_STOPPED or opcode == LOG_CHECKED then
				players = { }
				is_online = false
			elseif opcode == TX_SESSION_OPENED then
				players[ fields[ 3 ] ] = optime
			elseif opcode == TX_SESSION_CLOSED then
				players[ fields[ 3 ] ] = nil
			end

			cursor = journal:seek( )
			length = length + 1
			on_repeat( )
		end

		if period then catalog[ period ].length = length end
	end

	self.close = function ( )
		journal:close( )
	end

	return self
end
