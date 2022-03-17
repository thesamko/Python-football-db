import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

class RostersData:
    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.base_url = my_constatns.BASE_URL_MATCH
        self.conn = connector.Connection('landingdb')
        self.cursor = self.conn.cursor

    def get_clean_data(self, match_id):
        url = self.base_url + str(match_id)
        url_request = requests.get(url)
        soup = BeautifulSoup(url_request.content, "lxml")
        json_data = parse_into_json(soup, 'rostersData', match_id)
        data = json.loads(json_data)
        return data

    def cleaned_data_to_tuple(self, data, match_id, roster_id, side):
        team_id = data['team_id']
        player_id = data['player_id']
        goals = data['goals']
        own_goals = data['own_goals']
        shots = data['shots']
        xG = data['xG']
        minutes = data['time']
        position = data['position']
        yellow_card = data['yellow_card']
        red_card = data['red_card']
        sub_in = data['roster_in']
        sub_out = data['roster_out']
        key_passes = data['key_passes']
        assists = data['assists']
        xA = data['xA']
        xGChain = data['xGChain']
        xG_buildup = data['xGBuildup']
        return (match_id, roster_id, team_id, player_id, goals, own_goals, shots, xG, minutes, position, yellow_card, red_card, sub_in, sub_out,
                key_passes, assists, xA, xGChain, xG_buildup, side)

    def load_data_to_server(self, data, schema_name, match_id):
        for roster_id in data['h']:
            query = f'''INSERT INTO [{schema_name}].[landing_match_rostersData] ([match_id],[team_id],[roster_id],[player_id]
                      ,[goals],[own_goals],[shots],[xG],[minutes_played],[position],[yellow_card],[red_card],[sub_in],[sub_out],[key_passes]
                      ,[assists] ,[xA] ,[xGChain] ,[xG_buildup] ,[h_a]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            result = self.cleaned_data_to_tuple(data['h'][roster_id], match_id, roster_id, 'h')
            self.cursor.execute(query, result)
            self.cursor.commit()

        for roster_id in data['a']:
            query = f'''INSERT INTO [{schema_name}].[landing_match_rostersData] ([match_id],[team_id],[roster_id],[player_id]
                                          ,[goals],[own_goals],[shots],[xG],[minutes_played],[position],[yellow_card],[red_card],[sub_in],[sub_out],[key_passes]
                                          ,[assists] ,[xA] ,[xGChain] ,[xG_buildup] ,[h_a]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            result = self.cleaned_data_to_tuple(data['a'][roster_id], match_id, roster_id, 'a')
            self.cursor.execute(query, result)
            self.cursor.commit()


    def full_load(self):
        for leag in self.leagues:
            schema_name = leag.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_match_rostersData')
            self.cursor.commit()
            friendly_league_name = leag.replace('_', ' ')
            self.cursor.execute(f'SELECT ld.[match_id] FROM [landingdb].{schema_name}.[landing_league_datesData] ld')
            all_matches = [id[0] for id in self.cursor.fetchall()]

            for match_id in all_matches:
                data = self.get_clean_data(match_id)
                self.load_data_to_server(data, schema_name, match_id)

    def incremental_load(self):
        for leag in self.leagues:
            schema_name = leag.replace('_', '').lower()
            friendly_league_name = leag.replace('_', ' ')
            self.cursor.execute(f'SELECT ld.[match_id] FROM [landingdb].{schema_name}.[landing_league_datesData] ld LEFT JOIN [landingdb].[{schema_name}].[landing_match_rostersData] lm ON ld.match_id = lm.match_id WHERE ld.game_finished = 1 AND lm.match_id IS NULL')
            all_matches = [id[0] for id in self.cursor.fetchall()]

            for match_id in all_matches:
                data = self.get_clean_data(match_id)
                self.load_data_to_server(data, schema_name, match_id)
