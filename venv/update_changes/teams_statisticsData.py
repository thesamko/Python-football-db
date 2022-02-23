import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

leagues = my_constatns.LEAGUES
base_url = my_constatns.BASE_URL_TEAM

conn = connector.Connection('landingdb')
cursor = conn.cursor

for leag in leagues:
    schema_name = leag.replace('_', '').lower()
    friendly_league_name = leag.replace('_', ' ')
    cursor.execute(
        f'SELECT DISTINCT team_name, season, team_id FROM [landingdb].[{schema_name}].[landing_league_teamsData] WHERE SEASON = 2021 order by 1, 2')
    all_teams_seasons = cursor.fetchall()



    for team_season in all_teams_seasons:
        team_name = team_season[0].replace(" ", "_")
        year = str(team_season[1]).strip()
        team_id = team_season[2]

        identifier = team_name + '/' + year

        url = base_url + '/' + identifier
        url_request = requests.get(url)

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'statisticsData', identifier)
        data = json.loads(json_data)

        #SituationData Start
        for situatuion in data['situation']:
            shots_for = data['situation'][situatuion]['shots']
            goals_for = data['situation'][situatuion]['goals']
            xG_for = data['situation'][situatuion]['xG']
            shots_against = data['situation'][situatuion]['against']['shots']
            goals_against = data['situation'][situatuion]['against']['goals']
            xG_against = data['situation'][situatuion]['against']['xG']
            query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statGamePlayData] WHERE [game_play] = '{situatuion}' AND [team_id] = {team_id} AND SEASON = {year} "
            cursor.execute(query)

            if(cursor.fetchall()):
                query = f'''UPDATE [{schema_name}].[landing_teams_statGamePlayData]
   SET 
      [shots_for] = {shots_for}
      ,[goals_for] = {goals_for}
      ,[xG_for] = {xG_for}
      ,[shots_against] = {shots_against}
      ,[goals_against] = {goals_against}
      ,[xG_against] = {xG_against}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [game_play] = '{situatuion}' AND [team_id] = {team_id} AND SEASON = {year}'''
                cursor.execute(query)
                cursor.commit()
            else:
                query = f'''INSERT INTO [{schema_name}].[landing_teams_statGamePlayData]([team_id],[game_play],[shots_for],[goals_for]
                                    ,[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (team_id, situatuion, shots_for, goals_for, xG_for, shots_against, goals_against, xG_against,friendly_league_name, year))
                cursor.commit()
        # SituationData End


        # FormationData Start
        for formation_data in data['formation']:
            formation = data['formation'][formation_data]['stat'].replace('-','')
            time = data['formation'][formation_data]['time']
            shots = data['formation'][formation_data]['shots']
            goals = data['formation'][formation_data]['goals']
            xG = data['formation'][formation_data]['xG']
            shots_against = data['formation'][formation_data]['against']['shots']
            goals_against = data['formation'][formation_data]['against']['goals']
            xG_against = data['formation'][formation_data]['against']['xG']

            query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statFormationData] WHERE team_id = {team_id} AND SEASON = {year} and formation = {formation}"
            cursor.execute(query)
            if(cursor.fetchall()):
                query = f'''UPDATE [{schema_name}].[landing_teams_statFormationData]
   SET 
      [minutes_played] = {time}
      ,[shots_for] = {shots}
      ,[goals_for] = {goals}
      ,[xG_for] = {xG}
      ,[shots_against] = {shots_against}
      ,[goals_against] = {goals_against}
      ,[xG_against] = {xG_against}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [formation] = {formation} AND [team_id] = {team_id} AND SEASON = {year}'''
                cursor.execute(query)
                cursor.commit()
            else:
                query = f'''INSERT INTO [{schema_name}].[landing_teams_statFormationData]([team_id],[formation],[minutes_played]
                                        ,[shots_for],[goals_for],[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (team_id, formation, time, shots, goals, xG, shots_against, goals_against, xG_against, friendly_league_name,
                year))
                cursor.commit()
        #Formation End


        #GameState Start
        for gamestate in data['gameState']:
            game_state = gamestate
            miuntes_played = data['gameState'][gamestate]['time']
            shots_for = data['gameState'][gamestate]['shots']
            goals_for = data['gameState'][gamestate]['goals']
            xG_for = data['gameState'][gamestate]['xG']
            shots_against = data['gameState'][gamestate]['against']['shots']
            goals_against = data['gameState'][gamestate]['against']['goals']
            xG_against = data['gameState'][gamestate]['against']['xG']
            query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statGameStateData] WHERE [game_state] = '{game_state}' AND [team_id] = {team_id} AND SEASON = {year}"
            cursor.execute(query)

            if(cursor.fetchall()):
                query = f'''UPDATE [{schema_name}].[landing_teams_statGameStateData]
   SET 
      [minutes_played] = {miuntes_played}
      ,[shots_for] = {shots_for}
      ,[goals_for] = {goals_for}
      ,[xG_for] = {xG_for}
      ,[shots_against] = {shots_against}
      ,[goals_against] = {goals_against}
      ,[xG_against] = {xG_against}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [game_state] = '{game_state}' AND [team_id] = {team_id} AND SEASON = {year}'''
                cursor.execute(query)
                cursor.commit()
            else:
                query = f'''INSERT INTO [{schema_name}].[landing_teams_statGameStateData]([team_id],[game_state],[minutes_played]
                                        ,[shots_for],[goals_for],[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, team_id, game_state, miuntes_played, shots_for, goals_for, xG_for, shots_against,
                               goals_against, xG_against, friendly_league_name, year)
                cursor.commit()
        #GameState End

        #Timing Start
        for game_times in data['timing']:
            shots_for = data['timing'][game_times]['shots']
            goals_for = data['timing'][game_times]['goals']
            xG_for = data['timing'][game_times]['xG']
            shots_against = data['timing'][game_times]['against']['shots']
            goals_against = data['timing'][game_times]['against']['goals']
            xG_against = data['timing'][game_times]['against']['xG']

            query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statTimingData] WHERE [timing] = '{game_times}' AND [team_id] = {team_id} AND [SEASON] = {year}"
            cursor.execute(query)
            if(cursor.fetchall()):

                query = f'''UPDATE [{schema_name}].[landing_teams_statTimingData]
   SET [shots_for] = {shots_for}
      ,[goals_for] = {goals_for}
      ,[xG_for] = {xG_for}
      ,[shots_against] = {shots_against}
      ,[goals_against] = {goals_against}
      ,[xG_against] = {xG_against}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [timing] = '{game_times}' AND [team_id] = {team_id} AND [SEASON] = {year}'''
                cursor.execute(query)
                cursor.commit()
            else:
                query = f'''INSERT INTO [{schema_name}].[landing_teams_statTimingData] ([team_id],[timing],[shots_for],[goals_for]
                    ,[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, team_id, game_times, shots_for, goals_for, xG_for, shots_against, goals_against, xG_against, friendly_league_name, year)
                cursor.commit()
        #Timing End

        #ShotZone Start
        for shot_zone in data['shotZone']:
            shots_for = data['shotZone'][shot_zone]['shots']
            goals_for = data['shotZone'][shot_zone]['goals']
            xG_for = data['shotZone'][shot_zone]['xG']
            shots_against = data['shotZone'][shot_zone]['against']['shots']
            goals_against = data['shotZone'][shot_zone]['against']['goals']
            xG_against = data['shotZone'][shot_zone]['against']['xG']

            query = f"SELECT TOP 1 1 FROM [landingdb].[{schema_name}].[landing_teams_statShotZoneData] WHERE team_id = {team_id} AND shot_zone = '{shot_zone}' AND SEASON = {year}"
            cursor.execute(query)

            if(cursor.fetchall()):
                query = f'''UPDATE [{schema_name}].[landing_teams_statShotZoneData]
   SET 
      [shots_for] = {shots_for}
      ,[goals_for] = {goals_for}
      ,[xG_for] = {xG_for}
      ,[shots_against] = {shots_against}
      ,[goals_against] = {goals_against}
      ,[xG_against] = {xG_against}  
      ,[LastUpdated] = GETUTCDATE()
 WHERE team_id = {team_id} AND shot_zone = '{shot_zone}' AND SEASON = {year}'''
                cursor.execute(query)
                cursor.commit()
            else:
                query = f'''INSERT INTO [{schema_name}].[landing_teams_statShotZoneData]([team_id],[shot_zone],[shots_for],[goals_for]
                                        ,[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, team_id, shot_zone, shots_for, goals_for, xG_for, shots_against,
                           goals_against, xG_against, friendly_league_name, year)
                cursor.commit()
        #ShotZone End

        #AttackSpeed Start
        for attack_speed in data['attackSpeed']:
            shots_for = data['attackSpeed'][attack_speed]['shots']
            goals_for = data['attackSpeed'][attack_speed]['goals']
            xG_for = data['attackSpeed'][attack_speed]['xG']
            shots_against = data['attackSpeed'][attack_speed]['against']['shots']
            goals_against = data['attackSpeed'][attack_speed]['against']['goals']
            xG_against = data['attackSpeed'][attack_speed]['against']['xG']

            query = f"SELECT TOP 1 1 FROM [landingdb].[{schema_name}].[landing_teams_statAttackSpeedData] WHERE team_id = {team_id} AND attack_speed = '{attack_speed}' AND SEASON = {year}"
            cursor.execute(query)

            if(cursor.fetchall()):
                query = f'''UPDATE [{schema_name}].[landing_teams_statAttackSpeedData]
   SET [shots_for] = {shots_for}
      ,[goals_for] = {goals_for}
      ,[xG_for] = {xG_for}
      ,[shots_against] = {shots_against}
      ,[goals_against] = {goals_against}
      ,[xG_against] = {xG_against}
      ,[LastUpdated] = GETUTCDATE()
 WHERE team_id = {team_id} AND attack_speed = '{attack_speed}' AND SEASON = {year}'''
                cursor.execute(query)
                cursor.commit()
            else:
                query = f'''INSERT INTO [{schema_name}].[landing_teams_statAttackSpeedData] ([team_id],[attack_speed],[shots_for]
                                        ,[goals_for],[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, team_id, attack_speed, shots_for, goals_for, xG_for, shots_against,
                           goals_against, xG_against, friendly_league_name, year)
                cursor.commit()
        #AttackSpeed End

        #Result Start
        for shot_type in data['result']:
            shots_for = data['result'][shot_type]['shots']
            xG_for = data['result'][shot_type]['xG']
            shots_against = data['result'][shot_type]['against']['shots']
            xG_against = data['result'][shot_type]['against']['xG']

            query = f"SELECT TOP 1 1 FROM [landingdb].[{schema_name}].[landing_teams_statShotTypeData] WHERE team_id = {team_id} AND shot_type = '{shot_type}' AND SEASON = {year}"
            cursor.execute(query)

            if(cursor.fetchall()):
                query = f'''UPDATE [{schema_name}].[landing_teams_statShotTypeData]
   SET [shots_for] = {shots_for}
      ,[xG_for] = {xG_for}
      ,[shots_against] = {shots_against}
      ,[xG_against] = {xG_against}
      ,[LastUpdated] = GETUTCDATE()
 WHERE team_id = {team_id} AND shot_type = '{shot_type}' AND SEASON = {year}
'''
                cursor.execute(query)
                cursor.commit()
            else:
                query = f'''INSERT INTO [{schema_name}].[landing_teams_statShotTypeData]([team_id],[shot_type],[shots_for]
                                    ,[xG_for],[shots_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, team_id, shot_type, shots_for, xG_for, shots_against,
                            xG_against, friendly_league_name, year)
                cursor.commit()
        #Result End




