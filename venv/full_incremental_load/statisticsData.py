import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import *
import my_constatns
import pandas as pd


class StatisticsData:
    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.base_url = my_constatns.BASE_URL_TEAM
        self.conn = connector.Connection('landingdb')
        self.cursor = self.conn.cursor
        self.alchemy_connection = self.conn.create_alchemy_engine()
        self.tables_to_truncate = ['[landing_teams_statAttackSpeedData]', '[landing_teams_statFormationData]',
                                   '[landing_teams_statGamePlayData]', '[landing_teams_statGameStateData]',
                                   '[landing_teams_statShotTypeData]', '[landing_teams_statShotZoneData]',
                                   '[landing_teams_statTimingData]']
        self.game_play_data = []
        self.formation_data = []
        self.game_state_data = []
        self.game_time_data = []
        self.shot_zone_data = []
        self.attack_speed_data = []
        self.shot_type_data = []

    def get_clean_data(self, team_name, year):
        url = self.base_url + '/' + team_name + '/' + year
        url_request = requests.get(url)
        identifier = team_name + '/' + year

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'statisticsData', identifier)
        data = json.loads(json_data)
        return data

    def game_play_data_to_list(self, data, team_id, situation, league_name, year):
        self.game_play_data.append({
            'team_id': team_id,
            'game_play': situation,
            'shots_for': data['shots'],
            'goals_for': data['goals'],
            'xG_for': data['xG'],
            'shots_against': data['against']['shots'],
            'goals_against': data['against']['goals'],
            'xG_against': data['against']['xG'],
            'LEAGUE': league_name,
            'SEASON': year
        })

    def formation_data_to_list(self, data, team_id, league_name, year):
        self.formation_data.append({
            'team_id': team_id,
            'formation': data['stat'].replace('-', ''),
            'minutes_played': data['time'],
            'shots_for': data['shots'],
            'goals_for': data['goals'],
            'xG_for': data['xG'],
            'shots_against': data['against']['shots'],
            'goals_against': data['against']['goals'],
            'xG_against': data['against']['xG'],
            'LEAGUE': league_name,
            'SEASON': year
        })

    def game_state_data_to_list(self, data, team_id, game_state, league_name, year):
        self.game_state_data.append({
            'team_id': team_id,
            'game_state': game_state,
            'minutes_played': data['time'],
            'shots_for': data['shots'],
            'goals_for': data['goals'],
            'xG_for': data['xG'],
            'shots_against': data['against']['shots'],
            'goals_against': data['against']['goals'],
            'xG_against': data['against']['xG'],
            'LEAGUE': league_name,
            'SEASON': year
        })

    def game_time_data_to_list(self, data, team_id, game_time, league_name, year):
        self.game_time_data.append({
            'team_id': team_id,
            'timing': game_time,
            'shots_for': data['shots'],
            'goals_for': data['goals'],
            'xG_for': data['xG'],
            'shots_against': data['against']['shots'],
            'goals_against': data['against']['goals'],
            'xG_against': data['against']['xG'],
            'LEAGUE': league_name,
            'SEASON': year
        })

    def shot_zone_data_to_list(self, data, team_id, shot_zone, league_name, year):
        self.shot_zone_data.append({
            'team_id': team_id,
            'shot_zone': shot_zone,
            'shots_for': data['shots'],
            'goals_for': data['goals'],
            'xG_for': data['xG'],
            'shots_against': data['against']['shots'],
            'goals_against': data['against']['goals'],
            'xG_against': data['against']['xG'],
            'LEAGUE': league_name,
            'SEASON': year
        })

    def attack_speed_data_to_list(self, data, team_id, attack_speed, league_name, year):
        self.attack_speed_data.append({
            'team_id': team_id,
            'attack_speed': attack_speed,
            'shots_for': data['shots'],
            'goals_for': data['goals'],
            'xG_for': data['xG'],
            'shots_against': data['against']['shots'],
            'goals_against': data['against']['goals'],
            'xG_against': data['against']['xG'],
            'LEAGUE': league_name,
            'SEASON': year
        })

    def shot_type_data_to_list(self, data, team_id, shot_type, league_name, year):
        self.shot_type_data.append({
            'team_id': team_id,
            'shot_type': shot_type,
            'shots_for': data['shots'],
            'xG_for': data['xG'],
            'shots_against': data['against']['shots'],
            'xG_against': data['against']['xG'],
            'LEAGUE': league_name,
            'SEASON': year
        })


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
            self.cursor.execute(f'SELECT DISTINCT team_name, season, team_id FROM [landingdb].[{schema_name}].[landing_league_teamsData] WHERE SEASON = {my_constatns.CURRENT_SEASON} order by 1, 2')
            all_teams_seasons = self.cursor.fetchall()

            for team_season in all_teams_seasons:
                team_name = team_season[0].replace(" ", "_")
                year = str(team_season[1]).strip()
                team_id = team_season[2]
                data = self.get_clean_data(team_name, year)

                for situation in data['situation']:
                    sit_condition_query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statGamePlayData] WHERE [game_play] = '{situation}' AND [team_id] = {team_id} AND SEASON = {year} "
                    situation_row_record = self.situation_row_record(data['situation'][situation],team_id, situation, friendly_league_name, year)
                    self.cursor.execute(sit_condition_query)
                    if self.cursor.fetchall():
                        situation_query = f'''UPDATE [{schema_name}].[landing_teams_statGamePlayData]
                           SET 
                              [shots_for] = {situation_row_record[2]}
                              ,[goals_for] = {situation_row_record[3]}
                              ,[xG_for] = {situation_row_record[4]}
                              ,[shots_against] = {situation_row_record[5]}
                              ,[goals_against] = {situation_row_record[6]}
                              ,[xG_against] = {situation_row_record[7]}
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE [game_play] = '{situation}' AND [team_id] = {team_id} AND SEASON = {year}'''
                        self.update_server_data(situation_query)
                    else:
                        situation_query = f'''INSERT INTO [{schema_name}].[landing_teams_statGamePlayData]([team_id],[game_play],[shots_for],[goals_for]
                                                                                ,[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(situation_row_record, situation_query)

                for formation_data in data['formation']:
                    formation = data['formation'][formation_data]['stat'].replace('-', '')
                    formation_row_record = self.formation_row_record(data['formation'][formation_data], team_id,friendly_league_name, year)
                    for_condition_query = query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statFormationData] WHERE team_id = {team_id} AND SEASON = {year} and formation = {formation}"
                    self.cursor.execute(for_condition_query)
                    if self.cursor.fetchall():
                        formation_query = f'''UPDATE [{schema_name}].[landing_teams_statFormationData]
                           SET 
                              [minutes_played] = {formation_row_record[2]}
                              ,[shots_for] = {formation_row_record[3]}
                              ,[goals_for] = {formation_row_record[4]}
                              ,[xG_for] = {formation_row_record[5]}
                              ,[shots_against] = {formation_row_record[6]}
                              ,[goals_against] = {formation_row_record[7]}
                              ,[xG_against] = {formation_row_record[8]}
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE [formation] = {formation} AND [team_id] = {team_id} AND SEASON = {year}'''
                        self.update_server_data(formation_query)
                    else:
                        formation_query = f'''INSERT INTO [{schema_name}].[landing_teams_statFormationData]([team_id],[formation],[minutes_played]
                                                                ,[shots_for],[goals_for],[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(formation_row_record, formation_query)

                for gamestate in data['gameState']:
                    gamestate_row_record = self.gamestate_row_record(data['gameState'][gamestate], team_id, gamestate, friendly_league_name, year)
                    gam_condition_query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statGameStateData] WHERE [game_state] = '{gamestate}' AND [team_id] = {team_id} AND SEASON = {year}"
                    self.cursor.execute(gam_condition_query)
                    if self.cursor.fetchall():
                        gamestate_query = f'''UPDATE [{schema_name}].[landing_teams_statGameStateData]
                           SET 
                              [minutes_played] = {gamestate_row_record[2]}
                              ,[shots_for] = {gamestate_row_record[3]}
                              ,[goals_for] = {gamestate_row_record[4]}
                              ,[xG_for] = {gamestate_row_record[5]}
                              ,[shots_against] = {gamestate_row_record[6]}
                              ,[goals_against] = {gamestate_row_record[7]}
                              ,[xG_against] = {gamestate_row_record[8]}
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE [game_state] = '{gamestate}' AND [team_id] = {team_id} AND SEASON = {year}'''
                        self.update_server_data(gamestate_query)
                    else:
                        gamestate_query = f'''INSERT INTO [{schema_name}].[landing_teams_statGameStateData]([team_id],[game_state],[minutes_played]
                                                                ,[shots_for],[goals_for],[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(gamestate_row_record, gamestate_query)

                for game_times in data['timing']:
                    gametime_row_record = self.gametime_row_record(data['timing'][game_times], team_id, game_times, friendly_league_name, year)
                    gamt_condition_query = f"SELECT TOP 1 1 FROM [{schema_name}].[landing_teams_statTimingData] WHERE [timing] = '{game_times}' AND [team_id] = {team_id} AND [SEASON] = {year}"
                    self.cursor.execute(gamt_condition_query)
                    if self.cursor.fetchall():
                        gametime_query = f'''UPDATE [{schema_name}].[landing_teams_statTimingData]
                           SET [shots_for] = {gametime_row_record[2]}
                              ,[goals_for] = {gametime_row_record[3]}
                              ,[xG_for] = {gametime_row_record[4]}
                              ,[shots_against] = {gametime_row_record[5]}
                              ,[goals_against] = {gametime_row_record[6]}
                              ,[xG_against] = {gametime_row_record[7]}
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE [timing] = '{game_times}' AND [team_id] = {team_id} AND [SEASON] = {year}'''
                        self.update_server_data(gametime_query)
                    else:
                        gametime_query = f'''INSERT INTO [{schema_name}].[landing_teams_statTimingData] ([team_id],[timing],[shots_for],[goals_for]
                                                                ,[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(gametime_row_record,gametime_query)

                for shot_zone in data['shotZone']:
                    shotzone_row_record = self.shotzone_row_record(data['shotZone'][shot_zone], team_id, shot_zone, friendly_league_name, year)
                    sz_condition_query = f"SELECT TOP 1 1 FROM [landingdb].[{schema_name}].[landing_teams_statShotZoneData] WHERE team_id = {team_id} AND shot_zone = '{shot_zone}' AND SEASON = {year}"
                    self.cursor.execute(sz_condition_query)
                    if self.cursor.fetchall():
                        shotzone_query = f'''UPDATE [{schema_name}].[landing_teams_statShotZoneData]
                           SET 
                              [shots_for] = {shotzone_row_record[2]}
                              ,[goals_for] = {shotzone_row_record[3]}
                              ,[xG_for] = {shotzone_row_record[4]}
                              ,[shots_against] = {shotzone_row_record[5]}
                              ,[goals_against] = {shotzone_row_record[6]}
                              ,[xG_against] = {shotzone_row_record[7]}  
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE team_id = {team_id} AND shot_zone = '{shot_zone}' AND SEASON = {year}'''
                        self.update_server_data(shotzone_query)
                    else:
                        shotzone_query = f'''INSERT INTO [{schema_name}].[landing_teams_statShotZoneData]([team_id],[shot_zone],[shots_for],[goals_for]
                                                                ,[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(shotzone_row_record, shotzone_query)

                for attack_speed in data['attackSpeed']:
                    attackspeed_row_record = self.attackspeed_row_record(data['attackSpeed'][attack_speed],team_id,attack_speed,friendly_league_name,year)
                    ap_condition_query = f"SELECT TOP 1 1 FROM [landingdb].[{schema_name}].[landing_teams_statAttackSpeedData] WHERE team_id = {team_id} AND attack_speed = '{attack_speed}' AND SEASON = {year}"
                    self.cursor.execute(ap_condition_query)
                    if self.cursor.fetchall():
                        attackspeed_query = f'''UPDATE [{schema_name}].[landing_teams_statAttackSpeedData]
                           SET [shots_for] = {attackspeed_row_record[2]}
                              ,[goals_for] = {attackspeed_row_record[3]}
                              ,[xG_for] = {attackspeed_row_record[4]}
                              ,[shots_against] = {attackspeed_row_record[5]}
                              ,[goals_against] = {attackspeed_row_record[6]}
                              ,[xG_against] = {attackspeed_row_record[7]}
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE team_id = {team_id} AND attack_speed = '{attack_speed}' AND SEASON = {year}'''
                        self.update_server_data(attackspeed_query)
                    else:
                        attackspeed_query = f'''INSERT INTO [{schema_name}].[landing_teams_statAttackSpeedData] ([team_id],[attack_speed],[shots_for]
                                                                ,[goals_for],[xG_for],[shots_against],[goals_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(attackspeed_row_record,attackspeed_query)

                for shot_type in data['result']:
                    shottype_row_record = self.shottype_row_record(data['result'][shot_type], team_id, shot_type, friendly_league_name, year)
                    st_condition_query = f"SELECT TOP 1 1 FROM [landingdb].[{schema_name}].[landing_teams_statShotTypeData] WHERE team_id = {team_id} AND shot_type = '{shot_type}' AND SEASON = {year}"
                    self.cursor.execute(st_condition_query)
                    if self.cursor.fetchall():
                        shottype_query = f'''UPDATE [{schema_name}].[landing_teams_statShotTypeData]
                           SET [shots_for] = {shottype_row_record[2]}
                              ,[xG_for] = {shottype_row_record[3]}
                              ,[shots_against] = {shottype_row_record[4]}
                              ,[xG_against] = {shottype_row_record[5]}
                              ,[LastUpdated] = GETUTCDATE()
                         WHERE team_id = {team_id} AND shot_type = '{shot_type}' AND SEASON = {year}'''
                        self.update_server_data(shottype_query)
                    else:
                        shottype_query = f'''INSERT INTO [{schema_name}].[landing_teams_statShotTypeData]([team_id],[shot_type],[shots_for]
                                                            ,[xG_for],[shots_against],[xG_against],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.load_data_to_server(shottype_row_record, shottype_query)

    def full_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            truncate_table(schema_name, self.tables_to_truncate)
            friendly_league_name = league.replace('_', ' ')
            self.cursor.execute(
                f'SELECT DISTINCT team_name, season, team_id '
                f'FROM [landingdb].[{schema_name}].[landing_league_teamsData] order by 1, 2')
            all_teams_seasons = self.cursor.fetchall()

            for team_season in all_teams_seasons:
                team_name = team_season[0].replace(" ", "_")
                year = str(team_season[1]).strip()
                team_id = team_season[2]
                data = self.get_clean_data(team_name, year)

                for situation in data['situation']:
                    self.game_play_data_to_list(data['situation'][situation], team_id, situation, friendly_league_name,
                                                year)

                for formation_data in data['formation']:
                    self.formation_data_to_list(data['formation'][formation_data], team_id, friendly_league_name, year)

                for game_state in data['gameState']:
                    self.game_state_data_to_list(data['gameState'][game_state], team_id, game_state, friendly_league_name, year)

                for game_times in data['timing']:
                    self.game_time_data_to_list(data['timing'][game_times], team_id, game_times, friendly_league_name,
                                                year)

                for shot_zone in data['shotZone']:
                    self.shot_zone_data_to_list(data['shotZone'][shot_zone], team_id, shot_zone, friendly_league_name,
                                                year)

                for attack_speed in data['attackSpeed']:
                    self.attack_speed_data_to_list(data['attackSpeed'][attack_speed], team_id, attack_speed,
                                                   friendly_league_name, year)

                for shot_type in data['result']:
                    self.shot_type_data_to_list(data['result'][shot_type], team_id, shot_type, friendly_league_name, year)

            with self.alchemy_connection.begin() as conn:
                game_play_data_df = pd.DataFrame(self.game_play_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_statGamePlayData OFF")
                game_play_data_df.to_sql('landing_teams_statGamePlayData', self.alchemy_connection,
                                         schema=schema_name, if_exists='append', index=False)

                formation_data_df = pd.DataFrame(self.formation_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_statFormationData OFF")
                formation_data_df.to_sql('landing_teams_statFormationData', self.alchemy_connection,
                                         schema=schema_name, if_exists='append', index=False)

                game_state_data_df = pd.DataFrame(self.game_state_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_statGameStateData OFF")
                game_state_data_df.to_sql('landing_teams_statGameStateData', self.alchemy_connection,
                                          schema=schema_name, if_exists='append', index=False)

                game_time_data_df = pd.DataFrame(self.game_time_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_statTimingData OFF")
                game_time_data_df.to_sql('landing_teams_statTimingData', self.alchemy_connection,
                                         schema=schema_name, if_exists='append', index=False)

                shot_zone_data_df = pd.DataFrame(self.shot_zone_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_statShotZoneData OFF")
                shot_zone_data_df.to_sql('landing_teams_statShotZoneData', self.alchemy_connection,
                                         schema=schema_name, if_exists='append', index=False)

                attack_speed_data_df = pd.DataFrame(self.attack_speed_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_statAttackSpeedData OFF")
                attack_speed_data_df.to_sql('landing_teams_statAttackSpeedData', self.alchemy_connection,
                                            schema=schema_name, if_exists='append', index=False)

                shot_type_data_df = pd.DataFrame(self.shot_type_data)
                conn.exec_driver_sql(f"SET IDENTITY_INSERT {schema_name}.landing_teams_statShotTypeData OFF")
                shot_type_data_df.to_sql('landing_teams_statShotTypeData', self.alchemy_connection,
                                         schema=schema_name, if_exists='append', index=False)

            self.game_play_data = []
            self.formation_data = []
            self.game_state_data = []
            self.game_time_data = []
            self.shot_zone_data = []
            self.attack_speed_data = []
            self.shot_type_data = []





