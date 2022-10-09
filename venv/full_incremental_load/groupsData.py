import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import *
import my_constatns
import pandas as pd


class GroupsData:

    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.base_url = my_constatns.BASE_URL_PLAYER
        self.tables_to_truncate = ['[landing_player_groupsPositionData]', '[landing_player_groupsGamePlayData]',
                                   '[landing_player_groupsShotZoneData]', '[landing_player_groupsShotTypeData]']
        self.conn = connector.Connection('landingdb')
        self.alchemy_connection = self.conn.create_alchemy_engine()
        self.cursor = self.conn.cursor
        self.position_data_list = []
        self.situation_data_list = []
        self.shot_zone_data_list = []
        self.shot_type_data_list = []

    def get_clean_data(self, player_id):
        url = self.base_url + str(player_id)
        try:
            url_request = requests.get(url)
        except:
            print(f"Fail at {player_id}")
            return {'identifier': player_id}

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'groupsData', player_id)
        data = json.loads(json_data)
        return data

    def not_current_season(self, year):
        return int(year) != my_constatns.CURRENT_SEASON

    def position_data_to_list(self, data, player_id, position, year):
        self.position_data_list.append({
            'player_id': player_id,
            'position': position,
            'games_played': data['games'],
            'goals_scored': data['goals'],
            'shots': data['shots'],
            'time': data['time'],
            'xG': data['xG'],
            'assists': data['assists'],
            'xA': data['xA'],
            'key_passes': data['key_passes'],
            'yellow_cards': data['yellow'],
            'red_cards': data['red'],
            'non_penalty_goals': data['npg'],
            'npxG': data['npxG'],
            'xG_chain': data['xGChain'],
            'xG_buildup': data['xGBuildup'],
            'SEASON': year
        })

    def situation_data_to_list(self, data, player_id, situation, year):
        self.situation_data_list.append({
            'player_id': player_id,
            'game_play': situation,
            'goals_scored': data['goals'],
            'shots': data['shots'],
            'xG': data['xG'],
            'assists': data['assists'],
            'key_passes': data['key_passes'],
            'xA': data['xA'],
            'non_penalty_goals': data['npg'],
            'npxG': data['npxG'],
            'SEASON': year
        })

    def shot_zone_data_to_list(self, data, player_id, shot_zone, year):
        self.shot_zone_data_list.append({
            'player_id': player_id,
            'shot_zone': shot_zone,
            'goals_scored': data['goals'],
            'shots': data['shots'],
            'xG': data['xG'],
            'assists': data['assists'],
            'key_passes': data['key_passes'],
            'xA': data['xA'],
            'non_penalty_goals': data['npg'],
            'npxG': data['npxG'],
            'SEASON': year
        })

    def shot_type_data_to_list(self, data, player_id, shot_type, year):
        self.shot_type_data_list.append({
            'player_id': player_id,
            'shot_type': shot_type,
            'goals_scored': data['goals'],
            'shots': data['shots'],
            'xG': data['xG'],
            'assists': data['assists'],
            'key_passes': data['key_passes'],
            'xA': data['xA'],
            'non_penalty_goals': data['npg'],
            'npxG': data['npxG'],
            'SEASON': year
        })

    def load_data_to_server(self, data_record, query):
        self.cursor.execute(query, data_record)
        self.cursor.commit()

    def update_server_data(self, query):
        self.cursor.execute(query)
        self.cursor.commit()

    def position_data_to_tuple(self, data, player_id, position, year):
        games = data['games']
        goals = data['goals']
        shots = data['shots']
        time = data['time']
        xG = data['xG']
        assists = data['assists']
        xA = data['xA']
        key_passes = data['key_passes']
        yellow = data['yellow']
        red = data['red']
        npg = data['npg']
        npxG = data['npxG']
        xGChain = data['xGChain']
        xGBuildup = data['xGBuildup']
        return (player_id, position, games, goals, shots, time, xG, assists, xA, key_passes, yellow, red, npg, npxG,
                xGChain, xGBuildup, year)

    def situation_data_to_tuple(self, data, player_id, situation, year):
        goals = data['goals']
        shots = data['shots']
        xG = data['xG']
        assists = data['assists']
        key_passes = data['key_passes']
        xA = data['xA']
        npg = data['npg']
        npxG = data['npxG']
        return player_id, situation, goals, shots, xG, assists, key_passes, xA, npg, npxG, year

    def shotzone_data_to_tuple(self, data, player_id, shot_zone, year):
        goals = data['goals']
        shots = data['shots']
        xG = data['xG']
        assists = data['assists']
        key_passes = data['key_passes']
        xA = data['xA']
        npg = data['npg']
        npxG = data['npxG']
        return player_id, shot_zone, goals, shots, xG, assists, key_passes, xA, npg, npxG, year

    def shottype_data_to_tuple(self, data, player_id, shot_type, year):
        goals = data['goals']
        shots = data['shots']
        xG = data['xG']
        assists = data['assists']
        key_passes = data['key_passes']
        xA = data['xA']
        npg = data['npg']
        npxG = data['npxG']
        return player_id, shot_type, goals, shots, xG, assists, key_passes, xA, npg, npxG, year

    def incremental_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'SELECT DISTINCT player_id '
                                f'FROM [landingdb].[{schema_name}].[landing_teams_playersData] '
                                f'WHERE season = {my_constatns.CURRENT_SEASON}')
            all_players = [player_id[0] for player_id in self.cursor.fetchall()]

            for player_id in all_players:
                data = self.get_clean_data(player_id)
                if is_failed(data):
                    print("Missing data at source or error in parsing for player " + str(data['identifier']))
                    continue
                self.cursor.execute(f"SELECT DISTINCT position "
                                    f"FROM [landingdb].[{schema_name}].[landing_player_groupsPositionData] "
                                    f"WHERE player_id = {player_id} and season = {my_constatns.CURRENT_SEASON}")
                all_positions = [pos[0] for pos in self.cursor.fetchall()]
                #PositionData
                position_query = f'''INSERT INTO [{schema_name}].[landing_player_groupsPositionData]([player_id],
                [position],[games_played],[goals_scored],[shots],[time],[xG],[assists],[xA],[key_passes],[yellow_cards],
                [red_cards],[non_penalty_goals],[npxG],[xG_chain],[xG_buildup],[SEASON]) 
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                for year in data['position']:
                    if self.not_current_season(year):
                        continue
                    for position in data['position'][year]:
                        position_row_record = self.position_data_to_tuple(data['position'][year][position], player_id,
                                                                          position, year)
                        if position in all_positions:
                            query = f'''UPDATE [{schema_name}].[landing_player_groupsPositionData]
                               SET [games_played] = {position_row_record[2]}
                                  ,[goals_scored] = {position_row_record[3]}
                                  ,[shots] = {position_row_record[4]}
                                  ,[time] = {position_row_record[5]}
                                  ,[xG] = {position_row_record[6]}
                                  ,[assists] = {position_row_record[7]}
                                  ,[xA] = {position_row_record[8]}
                                  ,[key_passes] = {position_row_record[9]}
                                  ,[yellow_cards] = {position_row_record[10]}
                                  ,[red_cards] = {position_row_record[11]}
                                  ,[non_penalty_goals] = {position_row_record[12]}
                                  ,[npxG] = {position_row_record[13]}
                                  ,[xG_chain] = {position_row_record[14]}
                                  ,[xG_buildup] = {position_row_record[15]}
                                  ,[LastUpdated] = GETUTCDATE()
                             WHERE player_id = {position_row_record[0]} AND position = '{position_row_record[1]}' 
                             AND season = {my_constatns.CURRENT_SEASON}'''
                            self.update_server_data(query)
                        else:
                            self.load_data_to_server(position_row_record, position_query)

                #SituationData
                self.cursor.execute(f" SELECT DISTINCT game_play "
                                    f"FROM [landingdb].[{schema_name}].[landing_player_groupsGamePlayData] "
                                    f"WHERE player_id = {player_id} and season = {my_constatns.CURRENT_SEASON}")
                all_situations = [sit[0] for sit in self.cursor.fetchall()]
                for year in data['situation']:
                    if self.not_current_season(year):
                        continue
                    for situation in data['situation'][year]:
                        situation_row_record = self.situation_data_to_tuple(data['situation'][year][situation],
                                                                            player_id, situation, year)
                        if situation in all_situations:
                            query = f'''UPDATE [{schema_name}].[landing_player_groupsGamePlayData]
                               SET [goals_scored] = {situation_row_record[2]}
                                  ,[shots] = {situation_row_record[3]}
                                  ,[xG] = {situation_row_record[4]}
                                  ,[assists] = {situation_row_record[5]}
                                  ,[key_passes] = {situation_row_record[6]}
                                  ,[xA] = {situation_row_record[7]}
                                  ,[non_penalty_goals] = {situation_row_record[8]}
                                  ,[npxG] = {situation_row_record[9]}
                                  ,[LastUpdated] = GETUTCDATE()
                             WHERE [player_id] = {situation_row_record[0]} AND game_play = '{situation_row_record[1]}' 
                             AND SEASON = {my_constatns.CURRENT_SEASON}'''
                            self.update_server_data(query)
                        else:
                            situation_query = f'''INSERT INTO [{schema_name}].[landing_player_groupsGamePlayData] 
                            ([player_id],[game_play],[goals_scored],[shots],[xG],[assists],[key_passes],[xA],
                            [non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                            self.load_data_to_server(situation_row_record, situation_query)

                #ShotZoneData
                self.cursor.execute(f"SELECT DISTINCT [shot_zone] "
                                    f"FROM [landingdb].[{schema_name}].[landing_player_groupsShotZoneData] "
                                    f"WHERE player_id = {player_id} and season = {my_constatns.CURRENT_SEASON}")
                all_zones = [zone[0] for zone in self.cursor.fetchall()]
                for year in data['shotZones']:
                    if self.not_current_season(year):
                        continue
                    for shot_zones in data['shotZones'][year]:
                        shotzone_row_record = self.shotzone_data_to_tuple(data['shotZones'][year][shot_zones],
                                                                          player_id, shot_zones, year)
                        if shot_zones in all_zones:
                            query = f'''UPDATE [{schema_name}].[landing_player_groupsShotZoneData]
                               SET [goals_scored] = {shotzone_row_record[2]}
                                  ,[shots] = {shotzone_row_record[3]}
                                  ,[xG] = {shotzone_row_record[4]}
                                  ,[assists] = {shotzone_row_record[5]}
                                  ,[key_passes] = {shotzone_row_record[6]}
                                  ,[xA] = {shotzone_row_record[7]}
                                  ,[non_penalty_goals] = {shotzone_row_record[8]}
                                  ,[npxG] = {shotzone_row_record[9]}
                                  ,[LastUpdated] = GETUTCDATE()
                             WHERE [player_id] = {shotzone_row_record[0]} AND [shot_zone] = '{shotzone_row_record[1]}' 
                             AND SEASON = {my_constatns.CURRENT_SEASON}'''
                            self.update_server_data(query)
                        else:
                            shotzone_query = f'''INSERT INTO [{schema_name}].[landing_player_groupsShotZoneData]
                            ([player_id],[shot_zone],[goals_scored],[shots],[xG],[assists],[key_passes],[xA],
                            [non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                            self.load_data_to_server(shotzone_row_record, shotzone_query)

                #ShotTypeData
                self.cursor.execute(f"SELECT DISTINCT shot_type "
                                    f"FROM [landingdb].[{schema_name}].[landing_player_groupsShotTypeData] "
                                    f"WHERE player_id = {player_id} and season = {my_constatns.CURRENT_SEASON}")
                all_shots = [shot[0] for shot in self.cursor.fetchall()]
                for year in data['shotTypes']:
                    if self.not_current_season(year):
                        continue
                    for shot_type in data['shotTypes'][year]:
                        shottype_row_record = self.shottype_data_to_tuple(data['shotTypes'][year][shot_type],
                                                                          player_id, shot_type, year)
                        if shot_type in all_shots:
                            query = f'''UPDATE [{schema_name}].[landing_player_groupsShotTypeData]
                               SET [goals_scored] = {shottype_row_record[2]}
                                  ,[shots] = {shottype_row_record[3]}
                                  ,[xG] = {shottype_row_record[4]}
                                  ,[assists] = {shottype_row_record[5]}
                                  ,[key_passes] = {shottype_row_record[6]}
                                  ,[xA] = {shottype_row_record[7]}
                                  ,[non_penalty_goals] = {shottype_row_record[8]}
                                  ,[npxG] = {shottype_row_record[9]}
                                  ,[LastUpdated] = GETUTCDATE()
                             WHERE [player_id] = {shottype_row_record[0]} AND [shot_type] = '{shottype_row_record[1]}'
                              AND SEASON = {my_constatns.CURRENT_SEASON}'''
                            self.update_server_data(query)
                        else:
                            shottype_query = f'''INSERT INTO [{schema_name}].[landing_player_groupsShotTypeData] 
                            ([player_id],[shot_type],[goals_scored],[shots],[xG],[assists],[key_passes],[xA],
                            [non_penalty_goals],[npxG],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                            self.load_data_to_server(shottype_row_record, shottype_query)

    def full_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            truncate_table(schema_name, self.tables_to_truncate)
            self.cursor.execute(
                    f'SELECT DISTINCT player_id FROM [landingdb].[{schema_name}].[landing_teams_playersData]')
            all_players = [player_id[0] for player_id in self.cursor.fetchall()]

            for player_id in all_players:
                data = self.get_clean_data(player_id)
                if is_failed(data):
                    print("Missing data at source or error in parsing for player " + str(data['identifier']))
                    continue
                #PositionData
                for year in data['position']:
                    for position in data['position'][year]:
                        self.position_data_to_list(data['position'][year][position], player_id, position, year)

                #SituationData
                for year in data['situation']:
                    for situation in data['situation'][year]:
                        self.situation_data_to_list(data['situation'][year][situation], player_id, situation, year)

                #ShotZoneData
                for year in data['shotZones']:
                    for shot_zones in data['shotZones'][year]:
                        self.shot_zone_data_to_list(data['shotZones'][year][shot_zones], player_id, shot_zones, year)

                #ShotTypeData
                for year in data['shotTypes']:
                    for shot_type in data['shotTypes'][year]:
                        self.shot_type_data_to_list(data['shotTypes'][year][shot_type], player_id, shot_type, year)

            league_position_data_df = pd.DataFrame(self.position_data_list)
            league_situation_data_df = pd.DataFrame(self.situation_data_list)
            league_shot_zone_data_df = pd.DataFrame(self.shot_zone_data_list)
            league_shot_type_data_df = pd.DataFrame(self.shot_type_data_list)
            league_position_data_df.to_sql('landing_player_groupsPositionData', self.alchemy_connection,
                                           schema=schema_name, if_exists='append', index=False)
            league_situation_data_df.to_sql('landing_player_groupsGamePlayData', self.alchemy_connection,
                                            schema=schema_name, if_exists='append', index=False)
            league_shot_zone_data_df.to_sql('landing_player_groupsShotZoneData', self.alchemy_connection,
                                            schema=schema_name, if_exists='append', index=False)
            league_shot_type_data_df.to_sql('landing_player_groupsShotTypeData', self.alchemy_connection,
                                            schema=schema_name, if_exists='append', index=False)
            self.position_data_list = []
            self.situation_data_list = []
            self.shot_zone_data_list = []
            self.shot_type_data_list = []
