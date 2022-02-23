import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

leagues = my_constatns.LEAGUES
base_url = my_constatns.BASE_URL_MATCH

conn = connector.Connection('landingdb')
cursor = conn.cursor

for leag in leagues:
    schema_name = leag.replace('_', '').lower()
    friendly_league_name = leag.replace('_', ' ')
    cursor.execute(f'SELECT ld.[match_id] FROM [landingdb].{schema_name}.[landing_league_datesData] ld LEFT JOIN [landingdb].[{schema_name}].[landing_match_rostersData] lm ON ld.match_id = lm.match_id WHERE ld.game_finished = 1 AND lm.match_id IS NULL')
    all_matches = [id[0] for id in cursor.fetchall()]

    for match_id in all_matches:

        url = base_url + str(match_id)
        url_request = requests.get(url)

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'rostersData', match_id)

        data = json.loads(json_data)

        #HomeTeam
        for roster_id in data['h']:
            team_id = data['h'][roster_id]['team_id']
            player_id = data['h'][roster_id]['player_id']
            goals = data['h'][roster_id]['goals']
            own_goals = data['h'][roster_id]['own_goals']
            shots = data['h'][roster_id]['shots']
            xG = data['h'][roster_id]['xG']
            minutes = data['h'][roster_id]['time']
            position = data['h'][roster_id]['position']
            yellow_card = data['h'][roster_id]['yellow_card']
            red_card = data['h'][roster_id]['red_card']
            sub_in = data['h'][roster_id]['roster_in']
            sub_out = data['h'][roster_id]['roster_out']
            key_passes = data['h'][roster_id]['key_passes']
            assists = data['h'][roster_id]['assists']
            xA = data['h'][roster_id]['xA']
            xGChain = data['h'][roster_id]['xGChain']
            xG_buildup = data['h'][roster_id]['xGBuildup']

            query = f'''INSERT INTO [{schema_name}].[landing_match_rostersData] ([match_id],[team_id],[roster_id],[player_id]
                       ,[goals],[own_goals],[shots],[xG],[minutes_played],[position],[yellow_card],[red_card],[sub_in],[sub_out],[key_passes]
                       ,[assists] ,[xA] ,[xGChain] ,[xG_buildup] ,[h_a]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            cursor.execute(query, (match_id, team_id, roster_id, player_id, goals, own_goals, shots, xG, minutes, position,
                                   yellow_card, red_card, sub_in, sub_out, key_passes, assists, xA, xGChain, xG_buildup, 'h'))
            cursor.commit()

        #AwayTeam
        for roster_id in data['a']:
            team_id = data['a'][roster_id]['team_id']
            player_id = data['a'][roster_id]['player_id']
            goals = data['a'][roster_id]['goals']
            own_goals = data['a'][roster_id]['own_goals']
            shots = data['a'][roster_id]['shots']
            xG = data['a'][roster_id]['xG']
            minutes = data['a'][roster_id]['time']
            position = data['a'][roster_id]['position']
            yellow_card = data['a'][roster_id]['yellow_card']
            red_card = data['a'][roster_id]['red_card']
            sub_in = data['a'][roster_id]['roster_in']
            sub_out = data['a'][roster_id]['roster_out']
            key_passes = data['a'][roster_id]['key_passes']
            assists = data['a'][roster_id]['assists']
            xA = data['a'][roster_id]['xA']
            xGChain = data['a'][roster_id]['xGChain']
            xG_buildup = data['a'][roster_id]['xGBuildup']

            query = f'''INSERT INTO [{schema_name}].[landing_match_rostersData] ([match_id],[team_id],[roster_id],[player_id]
                       ,[goals],[own_goals],[shots],[xG],[minutes_played],[position],[yellow_card],[red_card],[sub_in],[sub_out],[key_passes]
                       ,[assists] ,[xA] ,[xGChain] ,[xG_buildup] ,[h_a]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            cursor.execute(query, (match_id, team_id, roster_id, player_id, goals, own_goals, shots, xG, minutes, position,
                                   yellow_card, red_card, sub_in, sub_out, key_passes, assists, xA, xGChain, xG_buildup, 'a'))
            cursor.commit()