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
    for year in seasons:
        identifier = league + '/' + year
        url = base_url + '/' + identifier
        url_request = requests.get(url)


        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'datesData', identifier)

        data = json.loads(json_data)

        cursor.execute(f'SELECT match_id FROM [landingdb].[{schema_name}].[landing_league_datesData] WHERE game_finished = 0')
        unplayed_games = [id[0] for id in cursor.fetchall()]

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
                cursor.execute(query)
                cursor.commit()
