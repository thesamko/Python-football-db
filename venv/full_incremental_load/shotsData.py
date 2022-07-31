import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import *
import my_constatns
import pandas as pd


class ShotsData:
    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.base_url = my_constatns.BASE_URL_PLAYER
        self.conn = connector.Connection('landingdb')
        self.alchemy_connection = self.conn.create_alchemy_engine()
        self.cursor = self.conn.cursor

    def get_clean_data(self, player_id):
        url = self.base_url + str(player_id)
        url_request = requests.get(url)
        soup = BeautifulSoup(url_request.content, "lxml")
        json_data = parse_into_json(soup, 'shotsData', player_id)
        data = json.loads(json_data)
        return data

    def shots_data_to_tuple(self, shots_data, player_id):
        event_id = shots_data['id']
        minute = shots_data['minute']
        situation = shots_data['situation']
        shot_type = shots_data['shotType']
        outcome = shots_data['result']
        x_cord = shots_data['X']
        y_cord = shots_data['Y']
        xG = shots_data['xG']
        assisted_by = shots_data['player_assisted']
        last_action = shots_data['lastAction']
        match_id = shots_data['match_id']
        home_away = shots_data['h_a']
        year = shots_data['season']
        return player_id, event_id, minute, situation, shot_type, outcome, x_cord, y_cord, xG, assisted_by, last_action,\
               match_id, home_away, year

    def load_data_to_server(self, data_record, query):
        self.cursor.execute(query, data_record)
        self.cursor.commit()

    def incremental_load(self):
        for leag in self.leagues:
            schema_name = leag.replace('_', '').lower()

            self.cursor.execute(f'SELECT DISTINCT player_id FROM [landingdb].[{schema_name}].[landing_teams_playersData] WHERE season = {my_constatns.CURRENT_SEASON}')
            all_players = [id[0] for id in self.cursor.fetchall()]

            for player_id in all_players:
                data = self.get_clean_data(player_id)
                end_range = len(data)
                query = f"SELECT DISTINCT match_id FROM [landingdb].[{schema_name}].[landing_player_shotsData] WHERE player_id = {player_id}"
                self.cursor.execute(query)
                games_played = [id[0] for id in self.cursor.fetchall()]

                for i in range(0, end_range):
                    shots_data = data.pop()
                    try:
                        match_id = shots_data['match_id']
                    except:
                        print("error at " + str(player_id))
                    if int(match_id) in games_played:
                        break
                    #self.all_records.append(shots_data)
            #df = pd.DataFrame(self.all_records)
            #for index, r in df.iterrows():
             #   self.cursor.execute(f"INSERT INTO [{schema_name}].[landing_player_shotsData_test]([player_id],[event_id],"
              #                      "[minute],[situation],[shot_type],[outcome],[x_cord],[y_cord],[xG],[assisted_by],"
               #                     "[last_action],[match_id],[home_away],[year]) "
                #                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", r.player_id, r.id, r.minute, r.situation,
                 #                   r.shotType, r.result, r.X, r.Y, r.xG, r.player_assisted, r.lastAction, r.match_id, r.h_a, my_constatns.CURRENT_SEASON)
            #self.conn.commit()
            #self.cursor.close()
                    shot_query = f'''INSERT INTO [{schema_name}].[landing_player_shotsData]([player_id],[event_id],[minute],[situation],[shot_type]
                                                                ,[outcome],[x_cord],[y_cord],[xG],[assisted_by],[last_action],[match_id],[home_away],[year]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                    shots_row_record = self.shots_data_to_tuple(shots_data, player_id)
                    self.load_data_to_server(shots_row_record, shot_query)

    def full_load(self):
        league_data = []
        schema_name = ''
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_player_shotsData')
            self.cursor.commit()
            self.cursor.execute(f'SELECT DISTINCT player_id FROM [landingdb].[{schema_name}].[landing_teams_playersData]')
            all_players = [id[0] for id in self.cursor.fetchall()]

            for player_id in all_players:
                data = self.get_clean_data(player_id)

                for shots_data in data:
                    league_data.append({
                        'player_id': shots_data['player_id'],
                        'event_id': shots_data['id'],
                        'minute': shots_data['minute'],
                        'situation': shots_data['situation'],
                        'shot_type': shots_data['shotType'],
                        'outcome': shots_data['result'],
                        'x_cord': shots_data['X'],
                        'y_cord': shots_data['Y'],
                        'xG': shots_data['xG'],
                        'assisted_by': shots_data['player_assisted'],
                        'last_action': shots_data['lastAction'],
                        'match_id': shots_data['match_id'],
                        'home_away': shots_data['h_a'],
                        'year': shots_data['season']
                    })

            with self.alchemy_connection.begin() as conn:
                league_data_df = pd.DataFrame(league_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_player_shotsData OFF")
                league_data_df.to_sql('landing_player_shotsData', self.alchemy_connection, schema=schema_name,
                                      if_exists='append', index=False)
            league_data = []
