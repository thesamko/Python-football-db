import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns
import pandas as pd


class DatesData:

    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.seasons = my_constatns.SEASONS
        self.base_url = my_constatns.BASE_URL_LEAGUE
        self.conn = connector.Connection('landingdb')
        self.alchemy_connection = self.conn.create_alchemy_engine()
        self.cursor = self.conn.cursor

    def get_clean_data(self, league, year):
        url = self.base_url + '/' + league + '/' + year
        url_request = requests.get(url)
        identifier = league + '/' + year

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'datesData', identifier)
        data = json.loads(json_data)
        return data

    def incremental_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()

            data = self.get_clean_data(league, '2021')
            self.cursor.execute(f'SELECT match_id FROM [landingdb].[{schema_name}].[landing_league_datesData] WHERE game_finished = 0')
            unplayed_games = [id[0] for id in self.cursor.fetchall()]
            for team_data in data:
                match_id = team_data['id']
                game_finished = team_data['isResult']
                if game_finished and int(match_id) in unplayed_games:
                    home_team_goals = team_data['goals']['h']
                    away_team_goals = team_data['goals']['a']
                    home_team_xG = team_data['xG']['h']
                    away_team_xG = team_data['xG']['a']
                    match_date_time = team_data['datetime']
                    forecast_win = team_data['forecast']['w']
                    forecast_draw = team_data['forecast']['d']
                    forecast_lose = team_data['forecast']['l']
                    query = f'''
                       UPDATE [{schema_name}].[landing_league_datesData]
              SET 
                  [game_finished] = 1
                 ,[home_team_goals] = {home_team_goals}
                 ,[away_team_goals] = {away_team_goals}
                 ,[home_team_xG] = {home_team_xG}
                 ,[away_team_xG] = {away_team_xG}
                 ,[match_date_time] = '{match_date_time}'
                 ,[forecast_win] = {forecast_win}
                 ,[forecast_draw] = {forecast_draw}
                 ,[forecast_lose] = {forecast_lose}
                 ,[LastUpdated] = GETUTCDATE()
            WHERE [match_id] = {match_id}
                       '''
                    self.cursor.execute(query)
                    self.cursor.commit()

    def full_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_league_datesData')
            self.cursor.commit()
            for year in self.seasons:
                data = self.get_clean_data(league, year)
                season_data = []
                for team_data in data:
                    season_data.append({
                        'match_id': team_data['id'],
                        'game_finished': team_data['isResult'],
                        'home_team': team_data['h']['id'],
                        'home_team_name': team_data['h']['title'],
                        'away_team': team_data['a']['id'],
                        'away_team_name': team_data['a']['title'],
                        'home_team_goals': team_data['goals']['h'],
                        'away_team_goals': team_data['goals']['a'],
                        'home_team_xG': team_data['xG']['h'],
                        'away_team_xG': team_data['xG']['a'],
                        'match_date_time': team_data['datetime'],
                        'forecast_win': team_data['forecast']['w'] if team_data['isResult'] else 0,
                        'forecast_draw': team_data['forecast']['d'] if team_data['isResult'] else 0,
                        'forecast_lose': team_data['forecast']['l'] if team_data['isResult'] else 0,
                        'LEAGUE': league,
                        'SEASON': year
                    })

                with self.alchemy_connection.begin() as conn:
                    season_data_df = pd.DataFrame(season_data)
                    conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_league_datesData OFF")
                    season_data_df.to_sql('landing_league_datesData', self.alchemy_connection, schema=schema_name,
                                          if_exists='append', index=False)
