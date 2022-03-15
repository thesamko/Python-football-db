import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import *
import my_constatns

class MinMaxPlayerStats:
    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.base_url = my_constatns.BASE_URL_PLAYER
        self.conn = connector.Connection('landingdb')
        self.cursor = self.conn.cursor

    def get_clean_data(self, player_id):
        url = self.base_url + str(player_id)
        url_request = requests.get(url)
        soup = BeautifulSoup(url_request.content, "lxml")
        json_data = parse_into_json(soup, 'minMaxPlayerStats', player_id)
        data = json.loads(json_data)
        return data

    def load_data_to_server(self, data_record, query):
        self.cursor.execute(query, data_record)
        self.cursor.commit()

    def update_server_data(self, query):
        self.cursor.execute(query)
        self.cursor.commit()

    def min_max_data_to_tuple(self, data, player_id, position):
        goals_min = data['goals']['min']
        goals_max = data['goals']['max']
        goals_avg = data['goals']['avg']
        xG_min = data['xG']['min']
        xG_max = data['xG']['max']
        xG_avg = data['xG']['avg']
        shots_min = data['shots']['min']
        shots_max = data['shots']['max']
        shots_avg = data['shots']['avg']
        assists_min = data['assists']['min']
        assists_max = data['assists']['max']
        assists_avg = data['assists']['avg']
        xA_min = data['xA']['min']
        xA_max = data['xA']['max']
        xA_avg = data['xA']['avg']
        key_passes_min = data['key_passes']['min']
        key_passes_max = data['key_passes']['max']
        key_passes_avg = data['key_passes']['avg']
        xGChain_min = data['xGChain']['min']
        xGChain_max = data['xGChain']['max']
        xGChain_avg = data['xGChain']['avg']
        xGBuildup_min = data['xGBuildup']['min']
        xGBuildup_max = data['xGBuildup']['max']
        xGBuildup_avg = data['xGBuildup']['avg']
        return player_id, position, goals_min, goals_max, goals_avg, xG_min, xG_max, xG_avg, shots_min, shots_max, shots_avg,\
               assists_min, assists_max, assists_avg, xA_min, xA_max, xA_avg, key_passes_min, key_passes_max, key_passes_avg, \
               xGChain_min, xGChain_max, xGChain_avg, xGBuildup_min, xGBuildup_max, xGBuildup_avg

    def incremental_load(self):
        for leag in self.leagues:
            schema_name = leag.replace('_', '').lower()
            self.cursor.execute(f'SELECT DISTINCT player_id FROM [landingdb].[{schema_name}].[landing_teams_playersData]')
            all_players = [id[0] for id in self.cursor.fetchall()]

            for player_id in all_players:
                data = self.get_clean_data(player_id)
                if is_failed(data):
                    print("Missing data at source or error in parsing for player " + str(data['identifier']))
                    continue
                for position in data:
                    min_max_row_record = self.min_max_data_to_tuple(data[position], player_id, position)
                    query = f"SELECT TOP 1 1 FROM {schema_name}.landing_player_minMaxPlayerStats WHERE position = '{position}' and player_id = {player_id}"
                    self.cursor.execute(query)
                    if self.cursor.fetchall():
                        query = f'''UPDATE [{schema_name}].[landing_player_minMaxPlayerStats]
                           SET [goals_min] = {min_max_row_record[2]}
                              ,[goals_max] = {min_max_row_record[3]}
                              ,[goals_avg] = {min_max_row_record[4]}
                              ,[xG_min] = {min_max_row_record[5]}
                              ,[xG_max] = {min_max_row_record[6]}
                              ,[xG_avg] = {min_max_row_record[7]}
                              ,[shots_min] = {min_max_row_record[8]}
                              ,[shots_max] = {min_max_row_record[9]}
                              ,[shots_avg] = {min_max_row_record[10]}
                              ,[assists_min] = {min_max_row_record[11]}
                              ,[assists_max] = {min_max_row_record[12]}
                              ,[assists_avg] = {min_max_row_record[13]}
                              ,[xA_min] = {min_max_row_record[14]}
                              ,[xA_max] = {min_max_row_record[15]}
                              ,[xA_avg] = {min_max_row_record[16]}
                              ,[key_passes_min] = {min_max_row_record[17]}
                              ,[key_passes_max] = {min_max_row_record[18]}
                              ,[key_passes_avg] = {min_max_row_record[19]}
                              ,[xGChain_min] = {min_max_row_record[20]}
                              ,[xGChain_max] = {min_max_row_record[21]}
                              ,[xGChain_avg] = {min_max_row_record[22]}
                              ,[xGBuildup_min] = {min_max_row_record[23]}
                              ,[xGBuildup_max] = {min_max_row_record[24]}
                              ,[xGBuildup_avg] = {min_max_row_record[25]}
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE [position] = '{min_max_row_record[1]}' AND [player_id] = {min_max_row_record[0]}'''
                        self.update_server_data(query)
                    else:
                        min_max_query = f'''INSERT INTO [{schema_name}].[landing_player_minMaxPlayerStats]([player_id],[position],[goals_min]
                                                                ,[goals_max],[goals_avg],[xG_min],[xG_max],[xG_avg],[shots_min],[shots_max],[shots_avg],[assists_min]
                                                                ,[assists_max],[assists_avg],[xA_min],[xA_max],[xA_avg],[key_passes_min],[key_passes_max],[key_passes_avg]
                                                                ,[xGChain_min],[xGChain_max],[xGChain_avg],[xGBuildup_min],[xGBuildup_max],[xGBuildup_avg]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(min_max_row_record, min_max_query)

    def full_load(self):
        for leag in self.leagues:
            schema_name = leag.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_player_minMaxPlayerStats')
            self.cursor.commit()
            self.cursor.execute(f'SELECT DISTINCT player_id FROM [landingdb].[{schema_name}].[landing_teams_playersData]')
            all_players = [id[0] for id in self.cursor.fetchall()]

            for player_id in all_players:
                data = self.get_clean_data(player_id)
                if is_failed(data):
                    print("Missing data at source or error in parsing for player " + str(data['identifier']))
                    continue
                min_max_query = f'''INSERT INTO [{schema_name}].[landing_player_minMaxPlayerStats]([player_id],[position],[goals_min]
                                        ,[goals_max],[goals_avg],[xG_min],[xG_max],[xG_avg],[shots_min],[shots_max],[shots_avg],[assists_min]
                                        ,[assists_max],[assists_avg],[xA_min],[xA_max],[xA_avg],[key_passes_min],[key_passes_max],[key_passes_avg]
                                        ,[xGChain_min],[xGChain_max],[xGChain_avg],[xGBuildup_min],[xGBuildup_max],[xGBuildup_avg]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)'''
                for position in data:
                    min_max_row_record = self.min_max_data_to_tuple(data[position], player_id, position)
                    self.load_data_to_server(min_max_row_record, min_max_query)



