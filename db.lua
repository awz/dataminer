--------------------------------------------------------
-- Minetest :: DataMiner v2.2 (auth_rx)
--
-- See README.txt for licensing and release notes.
-- Copyright (c) 2017-2018, Leslie E. Krause
--------------------------------------------------------

----------------------------
-- AuthDatabaseReader Class
----------------------------

function AuthDatabaseReader( path, name )
	local data, index
	local self = { }

	-- Private methods

	local db_reload = function ( )
		print( "Reading authentication data from disk..." )

		local file, errmsg = io.open( path .. "/" .. name, "r" )
		if not file then
			error( "Fatal exception in AuthDatabaseReader:db_reload( ), aborting." )
		end

		local head = assert( file:read( "*line" ) )

		index = tonumber( string.match( head, "^auth_rx/2.1 @(%d+)$" ) )
		if not index or index < 0 then
			error( "Fatal exception in AuthDatabaseReader:reload( ), aborting." )
		end

		for line in file:lines( ) do
			if line ~= "" then
				local fields = string.split( line, ":", true )
				if #fields ~= 10 then
					error( "Fatal exception in AuthDatabaseReader:reload( ), aborting." )
				end
				data[ fields[ 1 ] ] = {
					password = fields[ 2 ],
					oldlogin = tonumber( fields[ 3 ] ),
					newlogin = tonumber( fields[ 4 ] ),
					lifetime = tonumber( fields[ 5 ] ),
					total_sessions = tonumber( fields[ 6 ] ),
					total_attempts = tonumber( fields[ 7 ] ),
					total_failures = tonumber( fields[ 8 ] ),
					approved_addrs = string.split( fields[ 9 ], "," ),
					assigned_privs = string.split( fields[ 10 ], "," ),
				}
			end
		end
		file:close( )
	end

	-- Public methods

	self.connect = function ( )
		data = { }
		db_reload( )
	end

	self.disconnect = function ( )
		data = nil
	end

	self.records = function ( )
		return pairs( data )
	end

	self.select_record = function ( username )
		return data[ username ]
	end

	return self
end
