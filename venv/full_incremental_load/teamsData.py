import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

class TeamsData:

    def __init__(self):
        self.leagues = my_constatns.LEAGUES
        self.seasons = my_constatns.SEASONS
        self.base_url = my_constatns.BASE_URL_LEAGUE
        self.conn = connector.Connection('landingdb')
        self.cursor = self.conn.cursor

    def get_clean_data(self, league, year):
        url = self.base_url + '/' + league + '/' + year
        url_request = requests.get(url)
        identifier = league + '/' + year

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'teamsData', identifier)
        data = json.loads(json_data)
        return data

    def incremental_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            year = my_constatns.CURRENT_SEASON
            data = self.get_clean_data(league, str(year))

            for team_id in data:
                self.cursor.execute(
                    f'SELECT MAX(date) FROM [landingdb].[{schema_name}].[landing_league_teamsData] WHERE team_id = {team_id}')
                output = self.cursor.fetchone()
                team_name = data[team_id]['title']

                for event in data[team_id]['history']:
                    if event['date'][:9] > output[0]:
                        h_a = event['h_a']
                        xG = event['xG']
                        xGA = event['xGA']
                        npxG = event['npxG']
                        npxGA = event['npxGA']
                        ppda_att = event['ppda']['att']
                        ppda_def = event['ppda']['def']
                        deep = event['deep']
                        deep_allowed = event['deep_allowed']
                        scored = event['scored']
                        missed = event['missed']
                        xpts = event['xpts']
                        result = event['result']
                        date = event['date']
                        wins = event['wins']
                        draws = event['draws']
                        loses = event['loses']
                        pts = event['pts']
                        npxGD = event['npxGD']

                        query = f'''INSERT INTO {schema_name}.landing_league_teamsData ([team_id],[team_name],[season],[home_or_away]
                                   ,[expected_goals],[expected_goals_assists],[not_penalty_xg],[not_penalty_xg_assists],[passes_defensive_action_attack]
                                   ,[passes_defensive_action_defence],[deep_passes],[deep_passes_allowed],[goals_scored],[goals_conceded],[expected_points]
                                   ,[match_outcome],[date],[wins] ,[draws],[loses],[points],[non_penalty_xg_diff]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.cursor.execute(query, (team_id, team_name, year, h_a, xG, xGA, npxG, npxGA, ppda_att, ppda_def, deep, deep_allowed,
                        scored, missed, xpts, result, date, wins, draws, loses, pts, npxGD))
                        self.cursor.commit()

    def full_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_league_teamsData')
            self.cursor.commit()
            for year in self.seasons:
                data = self.get_clean_data(league, year)
                for team_id in data:
                    team_name = data[team_id]['title']
                    for event in data[team_id]['history']:
                        h_a = event['h_a']
                        xG = event['xG']
                        xGA = event['xGA']
                        npxG = event['npxG']
                        npxGA = event['npxGA']
                        ppda_att = event['ppda']['att']
                        ppda_def = event['ppda']['def']
                        deep = event['deep']
                        deep_allowed = event['deep_allowed']
                        scored = event['scored']
                        missed = event['missed']
                        xpts = event['xpts']
                        result = event['result']
                        date = event['date']
                        wins = event['wins']
                        draws = event['draws']
                        loses = event['loses']
                        pts = event['pts']
                        npxGD = event['npxGD']

                        query = f'''INSERT INTO {schema_name}.landing_league_teamsData ([team_id],[team_name],[season],[home_or_away]
                   ,[expected_goals],[expected_goals_assists],[not_penalty_xg],[not_penalty_xg_assists],[passes_defensive_action_attack]
                   ,[passes_defensive_action_defence],[deep_passes],[deep_passes_allowed],[goals_scored],[goals_conceded],[expected_points]
                   ,[match_outcome],[date],[wins] ,[draws],[loses],[points],[non_penalty_xg_diff]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                        self.cursor.execute(query, (
                        team_id, team_name, year, h_a, xG, xGA, npxG, npxGA, ppda_att, ppda_def, deep, deep_allowed,
                        scored,
                        missed, xpts, result, date, wins, draws, loses, pts, npxGD))
                        self.cursor.commit()