import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

leagues = my_constatns.LEAGUES
base_url = my_constatns.BASE_URL_PLAYER

conn = connector.Connection('landingdb')
cursor = conn.cursor

for leag in leagues:
    schema_name = leag.replace('_', '').lower()
    friendly_league_name = leag.replace('_', ' ')
    cursor.execute(f'SELECT DISTINCT player_id FROM [landingdb].[{schema_name}].[landing_teams_playersData]')
    all_players = [id[0] for id in cursor.fetchall()]

    for player_id in all_players:
        url = base_url + str(player_id)
        url_request = requests.get(url)

        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'shotsData', player_id)
        data = json.loads(json_data)

        query = f"SELECT DISTINCT match_id FROM [landingdb].[{schema_name}].[landing_player_shotsData] WHERE player_id = {player_id}"
        cursor.execute(query)
        games_played = [id[0] for id in cursor.fetchall()]


        for shots_data in data:
            match_id = shots_data['match_id']
            if int(match_id) in games_played:
                continue

            player_id = shots_data['player_id']
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
            home_away = shots_data['h_a']
            year = shots_data['season']

            query = f'''INSERT INTO [{schema_name}].[landing_player_shotsData]([player_id],[event_id],[minute],[situation],[shot_type]
                                    ,[outcome],[x_cord],[y_cord],[xG],[assisted_by],[last_action],[match_id],[home_away],[year]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            cursor.execute(query, (player_id, event_id, minute, situation, shot_type, outcome, x_cord, y_cord, xG, assisted_by, last_action,
            match_id, home_away, year))
            cursor.commit()



