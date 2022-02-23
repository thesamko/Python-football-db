import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

leagues = my_constatns.LEAGUES
seasons = my_constatns.SEASONS
base_url = my_constatns.BASE_URL_LEAGUE

conn = connector.Connection('landingdb')
cursor = conn.cursor

for league in leagues:
    schema_name = league.replace('_', '').lower()
    cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_league_datesData')
    cursor.commit()
    for year in seasons:
        url = base_url + '/' + league + '/' + year
        url_request = requests.get(url)
        identifier = league + '/' + year

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'datesData', identifier)

        data = json.loads(json_data)

        for team_data in data:
            match_id = team_data['id']
            game_finished = team_data['isResult']
            home_teamID = team_data['h']['id']
            home_team_name = team_data['h']['title']
            away_teamID = team_data['a']['id']
            away_team_name = team_data['a']['title']
            home_team_goals = team_data['goals']['h']
            away_team_goals = team_data['goals']['a']
            home_team_xG = team_data['xG']['h']
            away_team_xG = team_data['xG']['a']
            match_date_time = team_data['datetime']
            forecast_win = team_data['forecast']['w'] if team_data['isResult'] else 0
            forecast_draw = team_data['forecast']['d'] if team_data['isResult'] else 0
            forecast_lose = team_data['forecast']['l'] if team_data['isResult'] else 0

            query = f'''INSERT INTO {schema_name}.landing_league_datesData ([match_id],[game_finished],[home_team],[home_team_name]
            ,[away_team],[away_team_name],[home_team_goals],[away_team_goals],[home_team_xG],[away_team_xG],[match_date_time]
            ,[forecast_win],[forecast_draw],[forecast_lose],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            cursor.execute(query, (match_id, game_finished, home_teamID, home_team_name, away_teamID, away_team_name,
                                       home_team_goals, away_team_goals, home_team_xG, away_team_xG, match_date_time,
                                       forecast_win, forecast_lose, forecast_draw, league, year))
            cursor.commit()
