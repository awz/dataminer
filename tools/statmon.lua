--------------------------------------------------------
-- Minetest :: DataMiner Mod v2.2 (dataminer)
--
-- See README.txt for licensing and release notes.
-- Copyright (c) 2017-2018, Leslie E. Krause
--------------------------------------------------------

local TX_CREATE = 20
local TX_SESSION_OPENED = 50
local TX_SESSION_CLOSED = 51
local TX_LOGIN_ATTEMPT = 30
local TX_LOGIN_FAILURE = 31
local LOG_STARTED = 10
local LOG_CHECKED = 11
local LOG_STOPPED = 12
local LOG_INDEXED = 13

local catalog
local rel_date
local rel_time

local cur_clients
local cur_period
local player_login
local player_added
local player_check
local server_start

local server_uptime
local total_players
local total_players_new
local total_sessions
local total_failures
local total_attempts
local max_clients
local min_clients
local max_lifetime
local total_lifetime 
local player_stats
local hourly_stats

------------------------------------------------------------

-- parse the required command-line arguments

local name = "auth.dbx"
local path = "."
if arg[ 2 ] and arg[ 2 ] ~= "auth.db" then
	path, name = string.match( arg[ 2 ], "^(.*)/(.+%.dbx)$" )
	if not path then
		error( "The specified journal file is not recognized." )
	end
end

if not arg[ 1 ] or not string.find( arg[ 1 ], "^days=[0-9]+$" ) then
	error( "The 'days' parameter is invalid or missing." )
end
local days = tonumber( string.sub( arg[ 1 ], 6 ) )

------------------------------------------------------------

function string.split( str, sep, has_nil )
	res = { }
	for val in string.gmatch( str .. sep, "(.-)" .. sep ) do
		if val ~= "" or has_nil then
			table.insert( res, val )
		end
	end
	return res
end

local function get_period( t )
	return math.floor( ( t - rel_time ) / 3600 ) + 1
end

local function on_login_failure( cur_time )
	local p = get_period( cur_time )
	hourly_stats[ p ].failures = hourly_stats[ p ].failures + 1
	total_failures = total_failures + 1
end

local function on_login_attempt( cur_time )
	local p = get_period( cur_time )
	hourly_stats[ p ].attempts = hourly_stats[ p ].attempts + 1
	total_attempts = total_attempts + 1
end

local function on_server_startup( cur_time )
--print( "startup", cur_time )
	server_start = cur_time
end

local function on_server_shutdown( cur_time )
--print( "shutdown", cur_time )
	server_uptime = server_uptime + ( cur_time - server_start )
	server_start = nil
end

local function on_session_opened( cur_time, cur_user )
	while cur_period < get_period( cur_time ) do
		-- initialize client and player stats in prior periods
		local hourly = hourly_stats[ cur_period ]
		if not hourly.players then
			hourly.clients_max = cur_clients
			hourly.clients_min = cur_clients
			hourly.players = cur_clients
		end
		cur_period = cur_period + 1
	end

	local hourly = hourly_stats[ cur_period ]

	if not hourly.players then
		-- initialize client and player stats for this period
		hourly.clients_max = cur_clients + 1
		hourly.clients_min = cur_clients
		hourly.players = cur_clients
		player_check = { }
	elseif cur_clients + 1 > hourly.clients_max then
		-- update client stats for this period, if needed
		hourly.clients_max = cur_clients + 1
	end
	if not player_check[ cur_user ] then
		-- track another unique player
		player_check[ cur_user ] = 1
		hourly.players = hourly.players + 1
	end

	-- update some general stats
	if player_added[ cur_user ] then
		-- only count new players after joining game (sanity check)
		total_players_new = total_players_new + 1
	end
	if not max_clients or cur_clients + 1 > max_clients then
		max_clients = cur_clients + 1
	end
	if not min_clients or cur_clients < min_clients then
		min_clients = cur_clients
	end
end

local function on_session_closed( cur_time, cur_user )
	local old_time = player_login[ cur_user ]
	local lifetime = cur_time - old_time

	while cur_period < get_period( cur_time ) do
		-- initialize client and player stats in prior periods
		local hourly = hourly_stats[ cur_period ]
		if not hourly.players then
			hourly.clients_max = cur_clients
			hourly.clients_min = cur_clients
			hourly.players = cur_clients
		end
		cur_period = cur_period + 1
	end

	local hourly = hourly_stats[ cur_period ]
	local player = player_stats[ cur_user ]

	if not hourly.players then
		-- initialize client and player stats for this period
		hourly.clients_max = cur_clients
		hourly.clients_min = cur_clients - 1
		hourly.players = cur_clients
		player_check = { }
	elseif cur_clients - 1 < hourly.clients_min then
		-- update client stats for this period, if needed
		hourly.clients_min = cur_clients - 1
	end
	if not player_check[ cur_user ] then
		-- track another unique player
		player_check[ cur_user ] = 1
	end

	for p = get_period( old_time ), cur_period do
		-- update session stats in all prior periods
		hourly_stats[ p ].sessions = hourly_stats[ p ].sessions + 1
	end

	-- update some general stats
	if lifetime > max_lifetime then
		max_lifetime = lifetime
	end
	if max_clients == nill or cur_clients > max_clients then
		max_clients = cur_clients
	end
	if min_clients == nil or cur_clients - 1 < min_clients then
		min_clients = cur_clients - 1
	end
	total_sessions = total_sessions + 1
	total_lifetime = total_lifetime + lifetime
	if player then
		player.lifetime = player.lifetime + lifetime
		player.sessions = player.sessions + 1
	else
		player_stats[ cur_user ] = { is_new = player_added[ cur_user ], lifetime = lifetime, sessions = 1 }
		-- if no previous sessions, it's a unique player
		total_players = total_players + 1
	end
end

local function prepare_log( )
	io.write( "Working on it..." )

	local stat_bar = { "-", "\\", "|", "/" }
	local stat_idx = 0

	-- prepare a lookup table of the transaction log

	catalog.prepare( function ( ) 
		-- show an animated progress indicator
		if stat_idx % 50001 == 0 then
			io.write( stat_bar[ stat_idx % 4 + 1 ] .. "\b" )
			io.flush( )
		end
		stat_idx = stat_idx + 1
	end )

	io.write( "Done!\n" )
end

local function analyze_log( )
	local player_list = catalog.get_players( rel_date ) or { }

	cur_clients = #player_list
	player_login = { }
	player_added = { }
	cur_period = 1

	server_uptime = 0
	total_players = 0
	total_players_new = 0
	total_sessions = 0
	total_failures = 0
	total_attempts = 0
	max_clients = 0
	min_clients = 0
	max_lifetime = 0
	total_lifetime = 0
	player_stats = { }
	hourly_stats = { }

	for i = 1, 24 do
		hourly_stats[ i ] = { attempts = 0, failures = 0, sessions = 0 }
	end

	if catalog.get_is_online( rel_date ) then
		server_start = rel_time
	end

	for i, v in ipairs( player_list ) do
		-- initalize pre-existing players
		player_login[ v ] = rel_time	
	end

	for optime, opcode, fields in catalog.records( rel_date ) do
		if opcode == TX_LOGIN_ATTEMPT then
			on_login_attempt( optime )

        	elseif opcode == TX_LOGIN_FAILURE then
			on_login_failure( optime )
        
		elseif opcode == TX_CREATE then
			local cur_user = fields[ 1 ]

			player_added[ cur_user ] = optime
			on_login_attempt( optime )
        
		elseif opcode == TX_SESSION_OPENED then
			-- player joined game
			local cur_user = fields[ 1 ]

			player_login[ cur_user ] = optime
			on_session_opened( optime, cur_user )
			cur_clients = cur_clients + 1

		elseif opcode == TX_SESSION_CLOSED then
			-- player left game
			local cur_user = fields[ 1 ]

			on_session_closed( optime, cur_user )
			cur_clients = cur_clients - 1
			player_login[ cur_user ] = nil

		elseif opcode == LOG_STARTED then
			on_server_startup( optime )

			-- sanity check (these should already not exist!)
			player_login = { }
			player_added = { }
			cur_clients = 0

		elseif opcode == LOG_STOPPED or opcode == LOG_CHECKED then
			on_server_shutdown( optime )

			-- on server shutdown, all players logged off
			for cur_user in pairs( player_login ) do
				on_session_closed( optime, cur_user )
			end
			-- purge stale data for next server startup
			player_login = { }
			player_added = { }
			cur_clients = 0

		elseif opcode == LOG_INDEXED then
			if server_start then
				on_server_shutdown( rel_time + 86399 )
			end
			for cur_user in pairs( player_login ) do
				on_session_closed( rel_time + 86399, cur_user )
			end
			player_added = nil
			player_login = nil
		end
	end
end

local function print_layout( )
	print( string.format( "\27[0J\27[7m %s\27[0m", "DataMiner Mod v2.2" .. string.rep( " ", 118 ) ) )
	print( string.rep( "\n", 41 ) )

	io.write( "\27[39F" )
	print( "\27[1G Player Activity: Hourly Totals" )
	print( "\27[1G======================================================" )
	print( string.format( "\27[1G %-8s %10s %10s %10s %10s", "Period", "Sessions", "Failures", "Attempts", "Players" ) )
	print( "\27[1G------------------------------------------------------" )
	io.write( "\27[24B" )
	print( "\27[1G------------------------------------------------------" )

	io.write( "\27[29A" )
	print( "\27[57G Player Activity: Hourly Trends" )
	print( "\27[57G====================================" )
	print( string.format( "\27[57G %-8s %12s %12s", "Period", "Min Clients", "Max Clients" ) )
	print( "\27[57G------------------------------------" )
	io.write( "\27[24B" )
	print( "\27[57G------------------------------------" )

	io.write( "\27[29A" )
	print( "\27[95G Player Activity: 24-Hour Totals" )
	print( "\27[95G===========================================" )
	print( string.format( "\27[95G %-19s %10s %10s", "Player", "Sessions", "Lifetime" ) )
	print( "\27[95G-------------------------------------------" )
	io.write( "\27[24B" )
	print( "\27[95G-------------------------------------------" )

	io.write( "\27[1B" )
	print( "\27[1G Player Activity: 24-Hour Summary" )
	print( "\27[1G============================================================================================" )
	io.write( "\27[5B" )
	print( "\27[1G--------------------------------------------------------------------------------------------" )

	io.write( "\27[8A" )
	print( "\27[95G Player Details: " )
	print( "\27[95G===========================================" )
	io.write( "\27[5B" )
	print( "\27[95G-------------------------------------------" )

	io.write( "\27[1B\27[1G Press <J> or <L> to navigate days, <I> or <K> scroll the list of players, and <Q> to quit." )
	io.write( "\27[95G Tip: Hold <SHIFT> to accelerate movement." )
end

local function print_report( off )
	local player_list = { }

	io.write( "\27[41F\27" )
	print( "\27[1G Daily Player Analytics Report (" .. os.date( "!%d-%b-%Y UTC", rel_time ) .. ")" )

	io.write( "\27[5B" )
	for i = 1, 24 do
		print( string.format( "\27[1G [%02d:00] %10s %10s %10s %10s", i - 1,
			hourly_stats[ i ].sessions,
			hourly_stats[ i ].failures,
			hourly_stats[ i ].attempts,
			hourly_stats[ i ].players or 0 ) )
	end

	io.write( "\27[24A" )
	for i = 1, 24 do
		print( string.format( "\27[57G [%02d:00] %12s %12s", i - 1,
			hourly_stats[ i ].clients_min or 0,
			hourly_stats[ i ].clients_max or 0 ) )
	end

	for k, v in pairs( player_stats ) do
		-- copy entire table into array for sorting
		table.insert( player_list, { username = k, is_new = v.is_new, sessions = v.sessions, lifetime = v.lifetime } )
	end
	table.sort( player_list, function( a, b ) return a.lifetime > b.lifetime end )

	io.write( "\27[24A" )
	for i = 1, 24 do
		local player = player_list[ i + off ]
		if player then
			print( string.format( ( i == 1 and "\27[95G \27[7m" or "\27[95G " ) .. "%-19s %10d %5dm %02ds\27[0m",
				player.is_new and "* " .. player.username or player.username,
				player.sessions,
				player.lifetime / 60,
				player.lifetime % 60 ) )
			if i == 1 then
				username = player.username
			end
		else print( "\27[95G" .. string.rep( " ", 42 ) ) end
	end

	if username then
		local rec = db.select_record( username )
		io.write( "\27[4B" )
		print( string.format( "\27[95G %-20s %20s", "Username:", username ) )
		print( string.format( "\27[95G %-20s %20s", "Initial Login:", os.date( "!%d-%b-%Y", rec.oldlogin ) ) )
		print( string.format( "\27[95G %-20s %20s", "Latest Login:", os.date( "!%d-%b-%Y", rec.newlogin ) ) )
		print( string.format( "\27[95G %-20s %20d", "Total Sessions:", rec.total_sessions ) )
		print( string.format( "\27[95G %-20s %19dm", "Total Lifetime:", rec.lifetime / 60 ) )
	end

	io.write( "\27[5A" )
	print( string.format( "\27[1G %-30s %10d", "Total Players:", total_players ) )
	print( string.format( "\27[1G %-30s %10d", "Total New Players:", total_players_new ) )
	print( string.format( "\27[1G %-30s %10d", "Total Player Sessions:", total_sessions ) )
	print( string.format( "\27[1G %-30s %10d", "Total Login Failures:", total_failures ) )
	print( string.format( "\27[1G %-30s %10d", "Total Login Attempts:", total_attempts ) )
	io.write( "\27[5A" )
	print( string.format( "\27[49G %-30s %9d%%", "Overall Server Uptime:", ( server_uptime / 86399 ) * 100 ) )
	print( string.format( "\27[49G %-30s %10d", "Maximum Connected Clients:", max_clients ) )
	print( string.format( "\27[49G %-30s %10d", "Minimum Connected Clients:", min_clients ) )
	print( string.format( "\27[49G %-30s %9dm", "Maximum Player Lifetime:", max_lifetime / 60 ) )
	print( string.format( "\27[49G %-30s %9dm", "Average Player Lifetime:", total_players > 0 and ( total_lifetime / total_sessions ) / 60 or 0 ) )

	io.write( "\27[2B" )
end

------------------------------------------------------------

dofile( "../catalog.lua" )
dofile( "../db.lua" )

catalog = JournalIndex( path, name ) 
prepare_log( )

db = AuthDatabaseReader( path, "auth.db" )
db.connect( )

print_layout( )

os.execute( "stty raw -echo" )

local off
local opt

while true do
	-- calculate the relative date given an offset
	rel_date = math.floor( os.time( ) / 86400 ) - days
	rel_time = rel_date * 86400

	if not off then
		analyze_log( )
		print_report( 0 )
		off = 0

	elseif opt then
		print_report( off )
	end

	opt = io.read( 1 )

	if opt == "q" or opt == "Q" then
		break
	elseif opt == "l" and days > 0 then
		days = days - 1
		off = nil
	elseif opt == "j" then
		days = days + 1
		off = nil
	elseif opt == "L" and days > 0 then
		days = math.max( 0, days - 7 )
		off = nil
	elseif opt == "J" then
		days = days + 7
		off = nil
	elseif opt == "i" and off then
		off = math.max( 0, off - 1 )
	elseif opt == "k" and off then
		off = math.min( total_players - 1, off + 1 )
	elseif opt == "I" and off then
		off = math.max( 0, off - 12 )
	elseif opt == "K" and off then
		off = math.min( total_players - 1, off + 12 )
	else
		opt = nil
	end
end

catalog.close( )
db.disconnect( )

os.execute( "stty sane" )
print( )
