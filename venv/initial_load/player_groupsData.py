import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import *
import my_constatns

leagues = my_constatns.LEAGUES
base_url = my_constatns.BASE_URL_PLAYER
tables_to_truncate = ['[landing_player_groupsPositionData]', '[landing_player_groupsGamePlayData]', '[landing_player_groupsShotZoneData]',
                      '[landing_player_groupsShotTypeData]']

conn = connector.Connection('landingdb')
cursor = conn.cursor


for leag in leagues:
    schema_name = leag.replace('_', '').lower()
    truncate_table(schema_name, tables_to_truncate)
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
        for year in data['position']:
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

                query = f'''INSERT INTO [{schema_name}].[landing_player_groupsPositionData]([player_id],[position],[games_played],[goals_scored],[shots],[time],[xG],[assists],[xA],[key_passes],[yellow_cards],[red_cards]
                        ,[non_penalty_goals],[npxG],[xG_chain],[xG_buildup],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (player_id, position, games, goals, shots, time, xG, assists, xA, key_passes, yellow,
                                       red, npg, npxG, xGChain, xGBuildup, year))
                cursor.commit()
        #Situation End

        #GamePlay Start
        for year in data['situation']:
            for situation in data['situation'][year]:
                goals = data['situation'][year][situation]['goals']
                shots = data['situation'][year][situation]['shots']
                xG = data['situation'][year][situation]['xG']
                assists = data['situation'][year][situation]['assists']
                key_passes = data['situation'][year][situation]['key_passes']
                xA = data['situation'][year][situation]['xA']
                npg = data['situation'][year][situation]['npg']
                npxG = data['situation'][year][situation]['npxG']

                query = f'''INSERT INTO [{schema_name}].[landing_player_groupsGamePlayData] ([player_id],[game_play],[goals_scored]
                        ,[shots],[xG],[assists],[key_passes],[xA],[non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (player_id, situation, goals, shots, xG, assists, key_passes, xA, npg, npxG, year))
                cursor.commit()
        #GamePlay End

        #ShotZone Start
        for year in data['shotZones']:
            for shot_zones in data['shotZones'][year]:
                goals = data['shotZones'][year][shot_zones]['goals']
                shots = data['shotZones'][year][shot_zones]['shots']
                xG = data['shotZones'][year][shot_zones]['xG']
                assists = data['shotZones'][year][shot_zones]['assists']
                key_passes = data['shotZones'][year][shot_zones]['key_passes']
                xA = data['shotZones'][year][shot_zones]['xA']
                npg = data['shotZones'][year][shot_zones]['npg']
                npxG = data['shotZones'][year][shot_zones]['npxG']

                query = f'''INSERT INTO [{schema_name}].[landing_player_groupsShotZoneData]([player_id],[shot_zone],[goals_scored],[shots],[xG],[assists],[key_passes],[xA],[non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (player_id, shot_zones, goals, shots, xG, assists, key_passes, xA, npg, npxG, year))
                cursor.commit()
        #ShotZone End

        #ShotType Start
        for year in data['shotTypes']:
            for shot_type in data['shotTypes'][year]:
                goals = data['shotTypes'][year][shot_type]['goals']
                shots = data['shotTypes'][year][shot_type]['shots']
                xG = data['shotTypes'][year][shot_type]['xG']
                assists = data['shotTypes'][year][shot_type]['assists']
                key_passes = data['shotTypes'][year][shot_type]['key_passes']
                xA = data['shotTypes'][year][shot_type]['xA']
                npg = data['shotTypes'][year][shot_type]['npg']
                npxG = data['shotTypes'][year][shot_type]['npxG']

                query = f'''INSERT INTO [{schema_name}].[landing_player_groupsShotTypeData] ([player_id],[shot_type],[goals_scored]
                        ,[shots],[xG],[assists],[key_passes],[xA],[non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (player_id, shot_type, goals, shots, xG, assists, key_passes, xA, npg, npxG, year))
                cursor.commit()
        #ShotType End
