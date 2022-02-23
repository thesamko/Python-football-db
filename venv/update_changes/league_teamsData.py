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

        json_data = parse_into_json(soup, 'teamsData', identifier)
        data = json.loads(json_data)

        for team_id in data:
            cursor.execute(f'SELECT MAX(date) FROM [landingdb].[{schema_name}].[landing_league_teamsData] WHERE team_id = {team_id}')
            output = cursor.fetchone()
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
                    cursor.execute(query, (team_id, team_name, year, h_a, xG, xGA, npxG, npxGA, ppda_att, ppda_def, deep, deep_allowed, scored,
                    missed, xpts, result, date, wins, draws, loses, pts, npxGD))
                    cursor.commit()
