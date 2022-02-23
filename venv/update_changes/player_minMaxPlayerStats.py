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



    for id in all_players:
        url = base_url + str(id)
        url_request = requests.get(url)

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'minMaxPlayerStats', id)
        data = json.loads(json_data)

        for position in data:
            player_id = id
            position_played = position
            goals_min = data[position]['goals']['min']
            goals_max = data[position]['goals']['max']
            goals_avg = data[position]['goals']['avg']
            xG_min = data[position]['xG']['min']
            xG_max = data[position]['xG']['max']
            xG_avg = data[position]['xG']['avg']
            shots_min = data[position]['shots']['min']
            shots_max = data[position]['shots']['max']
            shots_avg = data[position]['shots']['avg']
            assists_min = data[position]['assists']['min']
            assists_max = data[position]['assists']['max']
            assists_avg = data[position]['assists']['avg']
            xA_min = data[position]['xA']['min']
            xA_max = data[position]['xA']['max']
            xA_avg = data[position]['xA']['avg']
            key_passes_min = data[position]['key_passes']['min']
            key_passes_max = data[position]['key_passes']['max']
            key_passes_avg = data[position]['key_passes']['avg']
            xGChain_min = data[position]['xGChain']['min']
            xGChain_max = data[position]['xGChain']['max']
            xGChain_avg = data[position]['xGChain']['avg']
            xGBuildup_min = data[position]['xGBuildup']['min']
            xGBuildup_max = data[position]['xGBuildup']['max']
            xGBuildup_avg = data[position]['xGBuildup']['avg']

            query = f"SELECT TOP 1 1 FROM {schema_name}.landing_player_minMaxPlayerStats WHERE position = '{position_played}' and player_id = {player_id}"
            cursor.execute(query)

            if(cursor.fetchall()):
                query = f'''UPDATE [{schema_name}].[landing_player_minMaxPlayerStats]
   SET [goals_min] = {goals_min}
      ,[goals_max] = {goals_max}
      ,[goals_avg] = {goals_avg}
      ,[xG_min] = {xG_min}
      ,[xG_max] = {xG_max}
      ,[xG_avg] = {xG_avg}
      ,[shots_min] = {shots_min}
      ,[shots_max] = {shots_max}
      ,[shots_avg] = {shots_avg}
      ,[assists_min] = {assists_min}
      ,[assists_max] = {assists_max}
      ,[assists_avg] = {assists_avg}
      ,[xA_min] = {xA_min}
      ,[xA_max] = {xA_max}
      ,[xA_avg] = {xA_avg}
      ,[key_passes_min] = {key_passes_min}
      ,[key_passes_max] = {key_passes_max}
      ,[key_passes_avg] = {key_passes_avg}
      ,[xGChain_min] = {xGChain_min}
      ,[xGChain_max] = {xGChain_max}
      ,[xGChain_avg] = {xGChain_avg}
      ,[xGBuildup_min] = {xGBuildup_min}
      ,[xGBuildup_max] = {xGBuildup_max}
      ,[xGBuildup_avg] = {xGBuildup_avg}
      ,[LastUpdated] = GETUTCDATE()
 WHERE [position] = '{position_played}' AND [player_id] = {player_id}'''
                cursor.execute(query)
                cursor.commit()

            else:
                query = f'''INSERT INTO [{schema_name}].[landing_player_minMaxPlayerStats]([player_id],[position],[goals_min]
                                        ,[goals_max],[goals_avg],[xG_min],[xG_max],[xG_avg],[shots_min],[shots_max],[shots_avg],[assists_min]
                                        ,[assists_max],[assists_avg],[xA_min],[xA_max],[xA_avg],[key_passes_min],[key_passes_max],[key_passes_avg]
                                        ,[xGChain_min],[xGChain_max],[xGChain_avg],[xGBuildup_min],[xGBuildup_max],[xGBuildup_avg]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (player_id, position_played, goals_min, goals_max, goals_avg, xG_min, xG_max, xG_avg, shots_min,
                shots_max, shots_avg, assists_min, assists_max, assists_avg, xA_min, xA_max, xA_avg, key_passes_min, key_passes_max,
                key_passes_avg, xGChain_min,xGChain_max, xGChain_avg, xGBuildup_min, xGBuildup_max, xGBuildup_avg))
                cursor.commit()
