import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import *
import my_constatns
import pandas as pd

class RostersData:
    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.base_url = my_constatns.BASE_URL_MATCH
        self.conn = connector.Connection('landingdb')
        self.alchemy_connection = self.conn.create_alchemy_engine()
        self.cursor = self.conn.cursor
        self.all_data = []

    def get_clean_data(self, match_id):
        url = self.base_url + str(match_id)
        url_request = requests.get(url)
        soup = BeautifulSoup(url_request.content, "lxml")
        json_data = parse_into_json(soup, 'rostersData', match_id)
        data = json.loads(json_data)
        return data

    def load_to_list(self, data, match_id, roster_id, side):
        self.all_data.append({
            'match_id': match_id,
            'team_id': data['team_id'],
            'roster_id': roster_id,
            'player_id': data['player_id'],
            'goals': data['goals'],
            'own_goals': data['own_goals'],
            'shots': data['shots'],
            'xG': data['xG'],
            'minutes_played': data['time'],
            'position': data['position'],
            'yellow_card': data['yellow_card'],
            'red_card': data['red_card'],
            'sub_in': data['roster_in'],
            'sub_out': data['roster_out'],
            'key_passes': data['key_passes'],
            'assists': data['assists'],
            'xA': data['xA'],
            'xGChain': data['xGChain'],
            'xG_buildup': data['xGBuildup'],
            'h_a': side
        })

    def parse_data(self, data, match_id):
        if is_failed(data):
            print("Missing data at source or error in parsing for match " + str(data['identifier']))
        else:
            for roster_id in data['h']:
                self.load_to_list(data['h'][roster_id], match_id, roster_id, 'h')

            for roster_id in data['a']:
                self.load_to_list(data['a'][roster_id], match_id, roster_id, 'a')

    def full_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_match_rostersData')
            self.cursor.commit()
            self.cursor.execute(f'SELECT [match_id] FROM [landingdb].{schema_name}.[landing_league_datesData] WHERE SEASON = 2020')
            all_matches = [id[0] for id in self.cursor.fetchall()]

            for match_id in all_matches:
                data = self.get_clean_data(match_id)
                self.parse_data(data, match_id)

            league_data_df = pd.DataFrame(self.all_data)
            league_data_df.to_sql('landing_match_rostersData', self.alchemy_connection, schema=schema_name,
                                  if_exists='append', index=False)
            self.all_data = []

    def incremental_load(self):
        schema_name = ''
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'SELECT ld.[match_id] FROM [landingdb].{schema_name}.[landing_league_datesData] ld '
                                f'LEFT JOIN [landingdb].[{schema_name}].[landing_match_rostersData] lm '
                                f'ON ld.match_id = lm.match_id WHERE ld.game_finished = 1 AND lm.match_id IS NULL')
            all_matches = [id[0] for id in self.cursor.fetchall()]

            for match_id in all_matches:
                data = self.get_clean_data(match_id)
                self.parse_data(data, match_id)

        league_data_df = pd.DataFrame(self.all_data)
        league_data_df.to_sql('landing_match_rostersData', self.alchemy_connection, schema=schema_name,
                              if_exists='append', index=False)
        self.all_data = []
