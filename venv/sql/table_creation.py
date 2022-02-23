import pyodbc

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=landingdb;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
cursor.execute('SELECT name FROM sys.schemas')
output = cursor.fetchall()
schemas = [sc[0] for sc in output]
leagues = ['RFPL', 'EPL', 'Bundesliga', 'LaLiga', 'Ligue1', 'SerieA']
for leag in leagues:
    league = leag.lower()
    if league.lower() in schemas:
        continue

    cursor.execute(f'CREATE SCHEMA {league}')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_league_datesData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[match_id] [int] NOT NULL,
	[game_finished] [bit] NOT NULL,
	[home_team] [smallint] NULL,
	[home_team_name] [varchar](50) NULL,
	[away_team] [smallint] NULL,
	[away_team_name] [varchar](50) NULL,
	[home_team_goals] [tinyint] NULL,
	[away_team_goals] [tinyint] NULL,
	[home_team_xG] [decimal](16, 2) NULL,
	[away_team_xG] [decimal](16, 2) NULL,
	[match_date_time] [datetime] NULL,
	[forecast_win] [decimal](4, 3) NULL,
	[forecast_draw] [decimal](4, 3) NULL,
	[forecast_lose] [decimal](4, 3) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
                   ''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_league_teamsData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [smallint] NULL,
	[team_name] [varchar](50) NULL,
	[season] [smallint] NULL,
	[home_or_away] [varchar](2) NULL,
	[expected_goals] [decimal](5, 2) NULL,
	[expected_goals_assists] [decimal](5, 2) NULL,
	[not_penalty_xg] [decimal](5, 2) NULL,
	[not_penalty_xg_assists] [decimal](5, 2) NULL,
	[passes_defensive_action_attack] [smallint] NULL,
	[passes_defensive_action_defence] [smallint] NULL,
	[deep_passes] [smallint] NULL,
	[deep_passes_allowed] [smallint] NULL,
	[goals_scored] [smallint] NULL,
	[goals_conceded] [smallint] NULL,
	[expected_points] [decimal](5, 2) NULL,
	[match_outcome] [varchar](3) NULL,
	[date] [date] NULL,
	[wins] [smallint] NULL,
	[draws] [smallint] NULL,
	[loses] [smallint] NULL,
	[points] [smallint] NULL,
	[non_penalty_xg_diff] [decimal](5, 2) NULL
) ON [PRIMARY]
    ''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_match_rostersData](
	[match_id] [int] NOT NULL,
	[team_id] [int] NOT NULL,
	[roster_id] [int] NOT NULL,
	[player_id] [int] NOT NULL,
	[goals] [tinyint] NULL,
	[own_goals] [tinyint] NULL,
	[shots] [tinyint] NULL,
	[xG] [decimal](16, 3) NULL,
	[minutes_played] [tinyint] NULL,
	[position] [varchar](10) NULL,
	[yellow_card] [tinyint] NULL,
	[red_card] [tinyint] NULL,
	[sub_in] [int] NULL,
	[sub_out] [int] NULL,
	[key_passes] [tinyint] NULL,
	[assists] [tinyint] NULL,
	[xA] [decimal](16, 3) NULL,
	[xGChain] [decimal](16, 3) NULL,
	[xG_buildup] [decimal](16, 3) NULL,
	[h_a] [varchar](1) NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_player_groupsShotZoneData](
	[player_id] [int] NOT NULL,
	[shot_zone] [varchar](30) NOT NULL,
	[goals_scored] [smallint] NULL,
	[shots] [smallint] NULL,
	[xG] [decimal](16, 2) NULL,
	[assists] [smallint] NULL,
	[key_passes] [smallint] NULL,
	[xA] [decimal](16, 2) NULL,
	[non_penalty_goals] [tinyint] NULL,
	[npxG] [decimal](16, 2) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_player_groupsPositionData](
	[player_id] [int] NOT NULL,
	[position] [varchar](10) NOT NULL,
	[games_played] [tinyint] NULL,
	[goals_scored] [smallint] NULL,
	[shots] [smallint] NULL,
	[time] [int] NULL,
	[xG] [decimal](16, 2) NULL,
	[assists] [smallint] NULL,
	[xA] [decimal](16, 2) NULL,
	[key_passes] [smallint] NULL,
	[yellow_cards] [tinyint] NULL,
	[red_cards] [tinyint] NULL,
	[non_penalty_goals] [tinyint] NULL,
	[npxG] [decimal](16, 2) NULL,
	[xG_chain] [decimal](16, 2) NULL,
	[xG_buildup] [decimal](16, 2) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_player_groupsGamePlayData](
	[player_id] [int] NOT NULL,
	[game_play] [varchar](30) NOT NULL,
	[goals_scored] [smallint] NULL,
	[shots] [smallint] NULL,
	[xG] [decimal](16, 2) NULL,
	[assists] [smallint] NULL,
	[key_passes] [smallint] NULL,
	[xA] [decimal](16, 2) NULL,
	[non_penalty_goals] [tinyint] NULL,
	[npxG] [decimal](16, 2) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_playersData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[player_id] [int] NOT NULL,
	[player_name] [nvarchar](255) NOT NULL,
	[games_played] [tinyint] NULL,
	[minutes_played] [int] NULL,
	[goals_scored] [smallint] NULL,
	[xG] [decimal](16, 2) NULL,
	[assists] [smallint] NULL,
	[xA] [decimal](16, 2) NULL,
	[shots] [smallint] NULL,
	[key_passes] [smallint] NULL,
	[yellow_cards] [tinyint] NULL,
	[red_cards] [tinyint] NULL,
	[position] [varchar](25) NULL,
	[team_name] [varchar](255) NULL,
	[non_penalty_goals] [tinyint] NULL,
	[npxG] [decimal](16, 2) NULL,
	[xG_chain] [decimal](16, 2) NULL,
	[xG_buildup] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_player_shotsData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[player_id] [int] NOT NULL,
	[event_id] [int] NOT NULL,
	[minute] [tinyint] NULL,
	[situation] [varchar](30) NULL,
	[shot_type] [varchar](30) NULL,
	[outcome] [varchar](30) NULL,
	[x_cord] [decimal](16, 3) NULL,
	[y_cord] [decimal](16, 3) NULL,
	[xG] [decimal](16, 2) NULL,
	[assisted_by] [nvarchar](50) NULL,
	[last_action] [varchar](30) NULL,
	[match_id] [int] NULL,
	[home_away] [varchar](3) NULL,
	[year] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_player_minMaxPlayerStats](
	[player_id] [int] NOT NULL,
	[position] [varchar](10) NULL,
	[goals_min] [decimal](16, 10) NULL,
	[goals_max] [decimal](16, 10) NULL,
	[goals_avg] [decimal](16, 10) NULL,
	[xG_min] [decimal](16, 10) NULL,
	[xG_max] [decimal](16, 10) NULL,
	[xG_avg] [decimal](16, 10) NULL,
	[shots_min] [decimal](16, 10) NULL,
	[shots_max] [decimal](16, 10) NULL,
	[shots_avg] [decimal](16, 10) NULL,
	[assists_min] [decimal](16, 10) NULL,
	[assists_max] [decimal](16, 10) NULL,
	[assists_avg] [decimal](16, 10) NULL,
	[xA_min] [decimal](16, 10) NULL,
	[xA_max] [decimal](16, 10) NULL,
	[xA_avg] [decimal](16, 10) NULL,
	[key_passes_min] [decimal](16, 10) NULL,
	[key_passes_max] [decimal](16, 10) NULL,
	[key_passes_avg] [decimal](16, 10) NULL,
	[xGChain_min] [decimal](16, 10) NULL,
	[xGChain_max] [decimal](16, 10) NULL,
	[xGChain_avg] [decimal](16, 10) NULL,
	[xGBuildup_min] [decimal](16, 10) NULL,
	[xGBuildup_max] [decimal](16, 10) NULL,
	[xGBuildup_avg] [decimal](16, 10) NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_statTimingData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [int] NOT NULL,
	[timing] [varchar](50) NULL,
	[shots_for] [int] NULL,
	[goals_for] [int] NULL,
	[xG_for] [decimal](16, 2) NULL,
	[shots_against] [int] NULL,
	[goals_against] [int] NULL,
	[xG_against] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_statShotZoneData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [int] NOT NULL,
	[shot_zone] [varchar](50) NULL,
	[shots_for] [int] NULL,
	[goals_for] [int] NULL,
	[xG_for] [decimal](16, 2) NULL,
	[shots_against] [int] NULL,
	[goals_against] [int] NULL,
	[xG_against] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
    ''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_statShotTypeData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [int] NOT NULL,
	[shot_type] [varchar](50) NULL,
	[shots_for] [int] NULL,
	[xG_for] [decimal](16, 2) NULL,
	[shots_against] [int] NULL,
	[xG_against] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
    ''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_statGameStateData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [int] NOT NULL,
	[game_state] [varchar](50) NULL,
	[minutes_played] [int] NULL,
	[shots_for] [int] NULL,
	[goals_for] [int] NULL,
	[xG_for] [decimal](16, 2) NULL,
	[shots_against] [int] NULL,
	[goals_against] [int] NULL,
	[xG_against] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
    ''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_statGamePlayData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [int] NOT NULL,
	[game_play] [varchar](25) NULL,
	[shots_for] [int] NULL,
	[goals_for] [int] NULL,
	[xG_for] [decimal](16, 2) NULL,
	[shots_against] [int] NULL,
	[goals_against] [int] NULL,
	[xG_against] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
    ''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_statFormationData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [int] NOT NULL,
	[formation] [int] NULL,
	[minutes_played] [int] NULL,
	[shots_for] [int] NULL,
	[goals_for] [int] NULL,
	[xG_for] [decimal](16, 2) NULL,
	[shots_against] [int] NULL,
	[goals_against] [int] NULL,
	[xG_against] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_teams_statAttackSpeedData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[team_id] [int] NOT NULL,
	[attack_speed] [varchar](50) NULL,
	[shots_for] [int] NULL,
	[goals_for] [int] NULL,
	[xG_for] [decimal](16, 2) NULL,
	[shots_against] [int] NULL,
	[goals_against] [int] NULL,
	[xG_against] [decimal](16, 2) NULL,
	[LEAGUE] [varchar](30) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
    ''')
    cursor.commit()

    cursor.execute(f'''
    CREATE TABLE [{league}].[landing_player_groupsShotTypeData](
	[player_id] [int] NOT NULL,
	[shot_type] [varchar](30) NOT NULL,
	[goals_scored] [smallint] NULL,
	[shots] [smallint] NULL,
	[xG] [decimal](16, 2) NULL,
	[assists] [smallint] NULL,
	[key_passes] [smallint] NULL,
	[xA] [decimal](16, 2) NULL,
	[non_penalty_goals] [tinyint] NULL,
	[npxG] [decimal](16, 2) NULL,
	[SEASON] [smallint] NULL
) ON [PRIMARY]
''')
    cursor.commit()

    print(league)
