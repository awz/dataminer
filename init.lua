--------------------------------------------------------
-- Minetest :: DataMiner Mod v2.2 (dataminer)
--
-- See README.txt for licensing and release notes.
-- Copyright (c) 2017-2018, Leslie E. Krause
--------------------------------------------------------

dofile( minetest.get_modpath( "dataminer" ) .. "/catalog.lua" )

local TX_CREATE = 20
local TX_SESSION_OPENED = 50
local TX_SESSION_CLOSED = 51
local TX_LOGIN_ATTEMPT = 30
local TX_LOGIN_FAILURE = 31
local LOG_STARTED = 10
local LOG_CHECKED = 11
local LOG_STOPPED = 12
local LOG_INDEXED = 13

------------------------------------------------------------

local function analyze_log( days )
	local rel_date = math.floor( os.time( ) / 86400 ) - days
	local rel_time = rel_date * 86400

	local cur_clients
	local player_login = { }
	local player_added = { }
	local cur_period = 1
	local player_check
	local server_start

	local server_uptime = 0
	local total_players = 0
	local total_players_new = 0
	local total_sessions = 0
	local total_failures = 0
	local total_attempts = 0
	local max_clients = 0
	local min_clients = 0
	local max_lifetime = 0
	local total_lifetime = 0
	local player_stats = { }
	local hourly_stats = { }

	-------------------------

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
		server_start = cur_time
	end

	local function on_server_shutdown( cur_time )
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
		if max_clients == nil or cur_clients > max_clients then
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

	-------------------------

	local player_list = catalog.get_players( rel_date ) or { }

	for i = 1, 24 do
		hourly_stats[ i ] = { attempts = 0, failures = 0, sessions = 0 }
	end

	if catalog.get_is_online( rel_date ) then
		server_start = rel_time
	end

	cur_clients = #player_list

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

	return {
		server_uptime = server_uptime,
		total_players = total_players,
		total_players_new = total_players_new,
		total_sessions = total_sessions,
		total_failures = total_failures,
		total_attempts = total_attempts,
		max_clients = max_clients,
		min_clients = min_clients,
		max_lifetime = max_lifetime,
		total_lifetime = total_lifetime,
		player_stats = player_stats,
		hourly_stats = hourly_stats
	}
end

minetest.register_chatcommand( "statmon", {
	description = "View graphical reports of player activity",
	privs = { server = true },
	func = function( name, param )
		local res
		local days = string.match( param, "^%d+$" ) or 1
		local log_index = 1

		local log_names = { "Total Players", "Total Sessions", "Total Attempts", "Total Failures", "Maximum Clients", "Minimum Clients" } 
		local graph_colors = { "#FFFF00", "#00FFFF", "#00FF00", "#FF0000", "#DDDDDD", "#BBBBBB" }
		local graph_types = { GRAPH_TYPEBAR, GRAPH_TYPEBAR, GRAPH_TYPEBAR, GRAPH_TYPEBAR, GRAPH_TYPEBAR, GRAPH_TYPEBAR }

		local get_formspec = function( )
			local dataset = { }
			local max_value = 5
			local max_matrix = { 2, 1, 4, 3, 6, 5 }

			for i, v in ipairs( res.hourly_stats ) do
				-- convert the results into a linear dataset
				local log_ids = { v.players, v.sessions, v.attempts, v.failures, v.clients_max, v.clients_min }
				local value = log_ids[ log_index ] or 0
				local value2 = log_ids[ max_matrix[ log_index ] ] or 0

				table.insert( dataset, value )

				-- find the maximum value to scale the y-axis
				max_value = math.max( max_value, value, value2 )
			end

			-- calculate intervals with some headroom
			max_value = max_value * 1.2
			max_scale = math.ceil( max_value / 40 ) * 5

			local graph = SimpleChart( dataset, { 
				vert_int = 4.5 / math.ceil( max_value / max_scale ),
				vert_off = 5.6,
				horz_int = 0.5,
				horz_off = 0.8,
				horz_pad = 0.5,

				y_range = math.ceil( max_value / max_scale ),
				y_start = 0, 
				y_scale = max_scale,
				x_range = 24,

				bar_color = graph_colors[ log_index ],
				tag_color = "#AAAAAA",
				idx_color = "#DDDDDD",

				on_plot_y = function( y, y_index, v_min, v_max, prop, meta )
					prop.idx_label = tostring( y_index )
				end,
				on_plot_x = function( x, x_index, v_min, v_max, v, prop, meta )
					prop.idx_label = x_index % 2 == 0 and string.format( "%02d:00", x_index ) or ""
					prop.tag_label = string.format( "%3s", v ) -- hack for centering
					return v
				end,
			} )

			local avg_lifetime = res.total_players > 0 and res.total_lifetime / res.total_sessions or 0
			local rel_date = math.floor( os.time( ) / 86400 ) - days

			local formspec = "size[13.5,9]"
				.. default.gui_bg
				.. default.gui_bg_img
				.. string.format( "label[0.3,0.4;%s:]", "Dataset" )
				.. string.format( "dropdown[1.5,0.3;3.3,1;log_name;%s;%d]", table.concat( log_names, "," ), log_index )

				.. string.format( "label[5.1,0.4;Player Analytics Report - %s]", os.date( "!%d-%b-%Y", rel_date * 86400 ) )
				.. "button[10.2,0.2;0.8,1;prev_week;<<]"
				.. "button[10.9,0.2;0.8,1;prev;<]"
				.. "button[11.6,0.2;0.8,1;next;>]"
				.. "button[12.3,0.2;0.8,1;next_week;>>]"

				.. "label[0.8,6.8;" .. minetest.colorize( "#BBBBBB", table.concat( {
					"Total Players:",
					"Total New Players:",
					"Total Player Sessions:",
					"Total Login Failures:",
					"Total Login Attempts:"
				}, "\n" ) ) .. "]"

				.. "label[5.0,6.8;" .. table.concat( {
					res.total_players,
					res.total_players_new,
					res.total_sessions,
					res.total_failures,
					res.total_attempts }, "\n" ) .. "]"

				.. "label[7.0,6.8;" .. minetest.colorize( "#BBBBBB", table.concat( {
					"Overall Server Uptime:",
					"Maximum Connected Clients:",
					"Minimum Connected Clients:",
					"Maximum Player Lifetime:",
					"Average Player Lifetime:"
				}, "\n" ) ) .. "]"

				.. "label[11.8,6.8;" .. table.concat( {
					math.floor( res.server_uptime / 86399 * 100 ) .. "%",
					res.max_clients,
					res.min_clients,
					math.floor( res.max_lifetime / 60 ) .. "m",
					math.floor( avg_lifetime / 60 ) .. "m"
				}, "\n" ) .. "]"

				.. graph.draw( graph_types[ log_index ], 0, 0 )

			return formspec
		end
		local on_close = function( meta, player, fields )
			log_index = ( {
				["Total Players"] = 1,
				["Total Sessions"] = 2,
				["Total Attempts"] = 3,
				["Total Failures"] = 4,
				["Maximum Clients"] = 5,
				["Minimum Clients"] = 6 
			} )[ fields.log_name ] or 1

			if fields.quit then return end

			if fields.prev then
				days = days + 1
				res = analyze_log( days )
			elseif fields.next and days > 0 then
				days = days - 1
				res = analyze_log( days )
			elseif fields.prev_week then
				days = days + 7
				res = analyze_log( days )
			elseif fields.next_week and days > 0 then
				days = math.max( 0, days - 7 )
				res = analyze_log( days )
			end
			minetest.update_form( player, get_formspec( ) )
		end

		res = analyze_log( days )

		minetest.create_form( nil, name, get_formspec( ), on_close )
	end
} )

------------------------------------------------------------

catalog = JournalIndex( minetest.get_worldpath( ), "auth.dbx" )
catalog.prepare( function ( ) end )
