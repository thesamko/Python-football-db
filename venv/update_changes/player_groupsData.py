import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

leagues = my_constatns.LEAGUES
base_url = my_constatns.BASE_URL_PLAYER

conn = connector.Connection('landingdb')
cursor = conn.cursor

for leag in leagues:
    schema_name = leag.replace('_', '').lower()
    friendly_league_name = leag.replace('_', ' ')
    cursor.execute(f'SELECT DISTINCT player_id FROM [landingdb].[{schema_name}].[landing_teams_playersData]')
    all_players = [id[0] for id in cursor.fetchall()]

    for player_id in all_players:
        url = base_url + str(player_id)
        url_request = requests.get(url)

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'groupsData', player_id)
        data = json.loads(json_data)


        #Situation Start
        cursor.execute(f"SELECT DISTINCT position FROM [landingdb].[{schema_name}].[landing_player_groupsPositionData] WHERE player_id = {player_id} and season = 2021")
        all_positions = [pos[0] for pos in cursor.fetchall()]
        for year in data['position']:
            if int(year) != 2021:
                continue
            for position in data['position'][year]:
                games = data['position'][year][position]['games']
                goals = data['position'][year][position]['goals']
                shots = data['position'][year][position]['shots']
                time = data['position'][year][position]['time']
                xG = data['position'][year][position]['xG']
                assists = data['position'][year][position]['assists']
                xA = data['position'][year][position]['xA']
                key_passes = data['position'][year][position]['key_passes']
                yellow = data['position'][year][position]['yellow']
                red = data['position'][year][position]['red']
                npg = data['position'][year][position]['npg']
                npxG = data['position'][year][position]['npxG']
                xGChain = data['position'][year][position]['xGChain']
                xGBuildup = data['position'][year][position]['xGBuildup']

                if position in all_positions:
                    query = f'''UPDATE [{schema_name}].[landing_player_groupsPositionData]
   SET [games_played] = {games}
      ,[goals_scored] = {goals}
      ,[shots] = {shots}
      ,[time] = {time}
      ,[xG] = {xG}
      ,[assists] = {assists}
      ,[xA] = {xA}
      ,[key_passes] = {key_passes}
      ,[yellow_cards] = {yellow}
      ,[red_cards] = {red}
      ,[non_penalty_goals] = {npg}
      ,[npxG] = {npxG}
      ,[xG_chain] = {xGChain}
      ,[xG_buildup] = {xGChain}
      ,[LastUpdated] = GETUTCDATE()
 WHERE player_id = {player_id} AND position = '{position}' AND season = 2021'''
                    cursor.execute(query)
                    cursor.commit()
                else:
                    query = f'''INSERT INTO [{schema_name}].[landing_player_groupsPositionData]([player_id],[position],[games_played]
                                            ,[goals_scored],[shots],[time],[xG],[assists],[xA],[key_passes],[yellow_cards],[red_cards]
                                            ,[non_penalty_goals],[npxG],[xG_chain],[xG_buildup],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                    cursor.execute(query, (player_id, position, games, goals, shots, time, xG, assists, xA, key_passes, yellow,
                                    red, npg, npxG, xGChain, xGBuildup, year))
                    cursor.commit()
        #Situation End

        #GamePlay Start
        cursor.execute(f" SELECT DISTINCT game_play FROM [landingdb].[{schema_name}].[landing_player_groupsGamePlayData] WHERE player_id = {player_id} and season = 2021")
        all_situations = [gp[0] for gp in cursor.fetchall()]
        for year in data['situation']:
            if int(year) != 2021:
                continue
            for situation in data['situation'][year]:
                goals = data['situation'][year][situation]['goals']
                shots = data['situation'][year][situation]['shots']
                xG = data['situation'][year][situation]['xG']
                assists = data['situation'][year][situation]['assists']
                key_passes = data['situation'][year][situation]['key_passes']
                xA = data['situation'][year][situation]['xA']
                npg = data['situation'][year][situation]['npg']
                npxG = data['situation'][year][situation]['npxG']

                if situation in all_situations:
                    query = f'''UPDATE [{schema_name}].[landing_player_groupsGamePlayData]
   SET [goals_scored] = {goals}
      ,[shots] = {shots}
      ,[xG] = {xG}
      ,[assists] = {assists}
      ,[key_passes] = {key_passes}
      ,[xA] = {xA}
      ,[non_penalty_goals] = {npg}
      ,[npxG] = {npxG}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [player_id] = {player_id} AND game_play = '{situation}' AND SEASON = 2021'''
                    cursor.execute(query)
                    cursor.commit()
                else:
                    query = f'''INSERT INTO [{schema_name}].[landing_player_groupsGamePlayData] ([player_id],[game_play],[goals_scored]
                                           ,[shots],[xG],[assists],[key_passes],[xA],[non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                    cursor.execute(query,(player_id, situation, goals, shots, xG, assists, key_passes, xA, npg, npxG, year))
                    cursor.commit()

        #GamePlay End

        #ShotZone Start
        cursor.execute(f"SELECT DISTINCT [shot_zone] FROM [landingdb].[{schema_name}].[landing_player_groupsShotZoneData] WHERE player_id = {player_id} and season = 2021")
        all_zones = [zone[0] for zone in cursor.fetchall()]

        for year in data['shotZones']:
            if int(year) != 2021:
                continue
            for shot_zones in data['shotZones'][year]:
                goals = data['shotZones'][year][shot_zones]['goals']
                shots = data['shotZones'][year][shot_zones]['shots']
                xG = data['shotZones'][year][shot_zones]['xG']
                assists = data['shotZones'][year][shot_zones]['assists']
                key_passes = data['shotZones'][year][shot_zones]['key_passes']
                xA = data['shotZones'][year][shot_zones]['xA']
                npg = data['shotZones'][year][shot_zones]['npg']
                npxG = data['shotZones'][year][shot_zones]['npxG']

                if shot_zones in all_zones:
                    query = f'''UPDATE [{schema_name}].[landing_player_groupsShotZoneData]
   SET [goals_scored] = {goals}
      ,[shots] = {shots}
      ,[xG] = {xG}
      ,[assists] = {assists}
      ,[key_passes] = {key_passes}
      ,[xA] = {xA}
      ,[non_penalty_goals] = {npg}
      ,[npxG] = {npxG}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [player_id] = {player_id} AND [shot_zone] = '{shot_zones}' AND SEASON = 2021'''
                    cursor.execute(query)
                    cursor.commit()
                else:
                    query = f'''INSERT INTO [{schema_name}].[landing_player_groupsShotZoneData]([player_id],[shot_zone]           ,[goals_scored]
                                            ,[shots],[xG],[assists],[key_passes],[xA],[non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                    cursor.execute(query,(player_id, shot_zones, goals, shots, xG, assists, key_passes, xA, npg, npxG, year))
                    cursor.commit()
        #ShotZone End

        #ShotType Start
        cursor.execute(f"SELECT DISTINCT shot_type FROM [landingdb].[{schema_name}].[landing_player_groupsShotTypeData] WHERE player_id = {player_id} and season = 2021")
        all_shots = [shot[0] for shot in cursor.fetchall()]
        for year in data['shotTypes']:
            if int(year) != 2021:
                continue
            for shot_type in data['shotTypes'][year]:
                goals = data['shotTypes'][year][shot_type]['goals']
                shots = data['shotTypes'][year][shot_type]['shots']
                xG = data['shotTypes'][year][shot_type]['xG']
                assists = data['shotTypes'][year][shot_type]['assists']
                key_passes = data['shotTypes'][year][shot_type]['key_passes']
                xA = data['shotTypes'][year][shot_type]['xA']
                npg = data['shotTypes'][year][shot_type]['npg']
                npxG = data['shotTypes'][year][shot_type]['npxG']

                if shot_type in all_shots:
                    query = f'''UPDATE [{schema_name}].[landing_player_groupsShotTypeData]
   SET [goals_scored] = {goals}
      ,[shots] = {shots}
      ,[xG] = {xG}
      ,[assists] = {assists}
      ,[key_passes] = {key_passes}
      ,[xA] = {xA}
      ,[non_penalty_goals] = {npg}
      ,[npxG] = {npxG}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [player_id] = {player_id} AND [shot_type] = '{shot_type}'
  AND SEASON = 2021'''
                    cursor.execute(query)
                    cursor.commit()
                else:
                    query = f'''INSERT INTO [{schema_name}].[landing_player_groupsShotTypeData] ([player_id],[shot_type],[goals_scored]
                                            ,[shots],[xG],[assists],[key_passes],[xA],[non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                    cursor.execute(query,(player_id, shot_type, goals, shots, xG, assists, key_passes, xA, npg, npxG, year))
                    cursor.commit()
        #ShotType End
