import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import *
import my_constatns
import pandas as pd

class PlayerData:
    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.base_url = my_constatns.BASE_URL_TEAM
        self.conn = connector.Connection('landingdb')
        self.alchemy_connection = self.conn.create_alchemy_engine()
        self.cursor = self.conn.cursor
        self.league_data = []

    def get_clean_data(self, team, year):
        identifier = team + '/' + year
        url = self.base_url + '/' + identifier
        url_request = requests.get(url)
        soup = BeautifulSoup(url_request.content, "lxml")
        json_data = parse_into_json(soup, 'playersData', identifier)
        data = json.loads(json_data)
        return data

    def player_data_to_list(self, player_data, team_id, friendly_league_name, year):
        self.league_data.append({
            'player_id': player_data['id'],
            'player_name': player_data['player_name'],
            'games_played': player_data['games'],
            'minutes_played': player_data['time'],
            'goals_scored': player_data['goals'],
            'xG': player_data['xG'],
            'assists': player_data['assists'],
            'xA': player_data['xA'],
            'shots': player_data['shots'],
            'key_passes': player_data['key_passes'],
            'yellow_cards': player_data['yellow_cards'],
            'red_cards': player_data['red_cards'],
            'position': player_data['position'],
            'team_id': team_id,
            'non_penalty_goals': player_data['npg'],
            'npxG': player_data['npxG'],
            'xG_chain': player_data['xGChain'],
            'xG_buildup': player_data['xGBuildup'],
            'LEAGUE': friendly_league_name,
            'SEASON': year
        })

    def player_data_to_tuple(self, player_data, team_id, friendly_league_name, year):
        player_id = player_data['id']
        player_name = player_data['player_name']
        games_played = player_data['games']
        minutes_played = player_data['time']
        goals_scored = player_data['goals']
        xG = player_data['xG']
        assists = player_data['assists']
        xA = player_data['xA']
        shots = player_data['shots']
        key_passes = player_data['key_passes']
        yellow_cards = player_data['yellow_cards']
        red_cards = player_data['red_cards']
        position = player_data['position']
        team_name = player_data['team_title']
        non_penalty_goals = player_data['npg']
        npxG = player_data['npxG']
        xG_chain = player_data['xGChain']
        xG_buildup = player_data['xGBuildup']
        return player_id, player_name, games_played, minutes_played, goals_scored, xG, assists, \
               xA, shots, key_passes, yellow_cards, red_cards, position, team_id, non_penalty_goals, \
               npxG, xG_chain, xG_buildup, friendly_league_name, year

    def load_data_to_server(self, data_record, query):
        self.cursor.execute(query, data_record)
        self.cursor.commit()

    def update_server_data(self, query):
        self.cursor.execute(query)
        self.cursor.commit()

    def incremental_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            friendly_league_name = league.replace('_', ' ')
            self.cursor.execute(f'SELECT DISTINCT player_id FROM {schema_name}.landing_teams_playersData '
                                f'WHERE season = {my_constatns.CURRENT_SEASON}')
            all_players = [id[0] for id in self.cursor.fetchall()]
            self.cursor.execute(f'SELECT DISTINCT team_name, season, team_id '
                                f'FROM [landingdb].[{schema_name}].[landing_league_teamsData] '
                                f'WHERE season = {my_constatns.CURRENT_SEASON} order by 1, 2')
            all_teams_seasons = self.cursor.fetchall()

            for team_season in all_teams_seasons:
                team = team_season[0].replace(" ", "_")
                year = str(team_season[1]).strip()
                team_id = team_season[2]
                data = self.get_clean_data(team, year)

                for player_data in data:
                    player_data_row_record = self.player_data_to_tuple(player_data, team_id, friendly_league_name, year)
                    player_id = player_data_row_record[0]
                    if int(player_id) in all_players:
                        query = f"SELECT TOP 1 1 FROM {schema_name}.landing_teams_playersData " \
                                f"WHERE season = {my_constatns.CURRENT_SEASON} and player_id = {player_id} " \
                                f"and team_id = {team_id}"
                        self.cursor.execute(query)
                        if self.cursor.fetchall():
                            query = f'''UPDATE [{schema_name}].[landing_teams_playersData]
                               SET 
                                  [games_played] = {player_data_row_record[2]}
                                  ,[minutes_played] = {player_data_row_record[3]}
                                  ,[goals_scored] = {player_data_row_record[4]}
                                  ,[xG] = {player_data_row_record[5]}
                                  ,[assists] = {player_data_row_record[6]}
                                  ,[xA] = {player_data_row_record[7]}
                                  ,[shots] = {player_data_row_record[8]}
                                  ,[key_passes] = {player_data_row_record[9]}
                                  ,[yellow_cards] = {player_data_row_record[10]}
                                  ,[red_cards] = {player_data_row_record[11]}
                                  ,[position] = '{player_data_row_record[12]}'
                                  ,[non_penalty_goals] = {player_data_row_record[14]}
                                  ,[npxG] = {player_data_row_record[15]}
                                  ,[xG_chain] = {player_data_row_record[16]}
                                  ,[xG_buildup] = {player_data_row_record[17]}
                                  ,[LastUpdated] = GETUTCDATE()
                             WHERE player_id = {player_id} and team_id = {team_id}'''
                            self.update_server_data(query)
                        else:
                            player_data_query = f'''INSERT INTO {schema_name}.landing_teams_playersData([player_id],
                            [player_name],[games_played],[minutes_played],[goals_scored],[xG],[assists],[xA],[shots],
                            [key_passes],[yellow_cards],[red_cards],[position],[team_id],[non_penalty_goals],[npxG],
                            [xG_chain],[xG_buildup],[LEAGUE],[SEASON]) 
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

                            player_data_row_record = self.player_data_to_tuple(player_data, team_id,
                                                                               friendly_league_name, year)
                            self.load_data_to_server(player_data_row_record, player_data_query)
                    else:
                        player_data_query = f'''INSERT INTO {schema_name}.landing_teams_playersData([player_id],
                        [player_name],[games_played],[minutes_played],[goals_scored],[xG],[assists],[xA],[shots],
                        [key_passes],[yellow_cards],[red_cards],[position],[team_id],[non_penalty_goals],[npxG],
                        [xG_chain],[xG_buildup],[LEAGUE],[SEASON]) 
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''


                        player_data_row_record = self.player_data_to_tuple(player_data, team_id,
                                                                           friendly_league_name, year)
                        self.load_data_to_server(player_data_row_record, player_data_query)

    def full_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_teams_playersData')
            self.cursor.commit()
            friendly_league_name = league.replace('_', ' ')
            self.cursor.execute(f'SELECT DISTINCT team_name, season, team_id '
                                f'FROM [landingdb].[{schema_name}].[landing_league_teamsData] order by 1, 2')
            all_teams_seasons = self.cursor.fetchall()

            for team_season in all_teams_seasons:
                team = team_season[0].replace(" ", "_")
                year = str(team_season[1]).strip()
                team_id = team_season[2]
                data = self.get_clean_data(team, year)

                for player_data in data:
                    self.player_data_to_list(player_data, team_id, friendly_league_name, year)

            with self.alchemy_connection.begin() as conn:
                league_data_df = pd.DataFrame(self.league_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_playersData OFF")
                league_data_df.to_sql('landing_teams_playersData', self.alchemy_connection, schema=schema_name,
                                      if_exists='append', index=False)
            self.league_data = []

