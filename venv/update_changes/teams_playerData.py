import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns

leagues = my_constatns.LEAGUES
base_url = my_constatns.BASE_URL_TEAM

conn = connector.Connection('landingdb')
cursor = conn.cursor

for leag in leagues:
    schema_name = leag.replace('_', '').lower()
    friendly_league_name = leag.replace('_', ' ')
    cursor.execute(f'SELECT DISTINCT player_id FROM {schema_name}.landing_teams_playersData WHERE season = 2021')
    all_players = [id[0] for id in cursor.fetchall()]
    cursor.execute(f'SELECT DISTINCT team_name, season, team_id FROM [landingdb].[{schema_name}].[landing_league_teamsData] WHERE season = 2021 order by 1, 2')
    all_teams_seasons = cursor.fetchall()

    for team_season in all_teams_seasons:
        team = team_season[0].replace(" ", "_")
        year = str(team_season[1]).strip()
        team_id = team_season[2]
        identifier = team + '/' + year
        url = base_url + '/' + identifier
        url_request = requests.get(url)



        soup = BeautifulSoup(url_request.content, "lxml")

        json_data = parse_into_json(soup, 'playersData', identifier)
        data = json.loads(json_data)


        for player_data in data:
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

            if int(player_id) in all_players:
                query = f"SELECT TOP 1 1 FROM {schema_name}.landing_teams_playersData WHERE season = 2021 and player_id = {player_id} and team_id = {team_id}"
                cursor.execute(query)
                if(cursor.fetchall()):

                    query = f'''UPDATE [{schema_name}].[landing_teams_playersData]
   SET 
      [games_played] = {games_played}
      ,[minutes_played] = {minutes_played}
      ,[goals_scored] = {goals_scored}
      ,[xG] = {xG}
      ,[assists] = {assists}
      ,[xA] = {xA}
      ,[shots] = {shots}
      ,[key_passes] = {key_passes}
      ,[yellow_cards] = {yellow_cards}
      ,[red_cards] = {red_cards}
      ,[position] = '{position}'
      ,[non_penalty_goals] = {non_penalty_goals}
      ,[npxG] = {npxG}
      ,[xG_chain] = {xG_chain}
      ,[xG_buildup] = {xG_buildup}
      ,[LastUpdated] = GETUTCDATE()
 WHERE player_id = {player_id} and team_id = {team_id}'''
                    cursor.execute(query)
                    cursor.commit()
                else:
                    query = f"INSERT INTO {schema_name}.landing_teams_playersData VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    cursor.execute(query, (player_id, player_name, games_played, minutes_played, goals_scored, xG,
                                           assists, xA, shots, key_passes, yellow_cards, red_cards, position, team_id,
                                           non_penalty_goals,
                                           npxG, xG_chain, xG_buildup, friendly_league_name, year))
                    cursor.commit()
            else:
                query = f'''INSERT INTO {schema_name}.landing_teams_playersData([player_id],[player_name],[games_played],[minutes_played]
                                    ,[goals_scored],[xG],[assists],[xA],[shots],[key_passes],[yellow_cards],[red_cards],[position],[team_id]
                                    ,[non_penalty_goals],[npxG],[xG_chain],[xG_buildup],[LEAGUE],[SEASON]) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(query, (player_id, player_name, games_played, minutes_played, goals_scored, xG,
                                       assists, xA, shots, key_passes, yellow_cards, red_cards, position, team_id,
                                       non_penalty_goals,
                                       npxG, xG_chain, xG_buildup, friendly_league_name, year))
                cursor.commit()
