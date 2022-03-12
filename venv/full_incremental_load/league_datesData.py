import requests
from bs4 import BeautifulSoup
import json
from sql import connector
from utils import parse_into_json
import my_constatns


class DatesData:

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

        json_data = parse_into_json(soup, 'datesData', identifier)

        return json.loads(json_data)

    def incremental_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            for year in self.seasons:
                data = self.get_clean_data(league, year)

                self.cursor.execute(f'SELECT match_id FROM [landingdb].[{schema_name}].[landing_league_datesData] WHERE game_finished = 0')
                unplayed_games = [id[0] for id in self.cursor.fetchall()]


    def full_load(self):
        for league in self.leagues:
            schema_name = league.replace('_', '').lower()
            self.cursor.execute(f'TRUNCATE TABLE {schema_name}.landing_league_datesData')
            self.cursor.commit()
            for year in self.seasons:
                data = self.get_clean_data(league, year)