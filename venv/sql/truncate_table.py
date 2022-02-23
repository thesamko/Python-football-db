from sql import connector

connection = connector.Connection('landingdb')
cursor = connection.cursor


leagues = ['RFPL', 'EPL', 'Bundesliga', 'LaLiga', 'Ligue1', 'SerieA']
table_name = 'factMinMaxPlayerStats'

for leag in leagues:
    league = leag.lower()

    cursor.execute(f'TRUNCATE TABLE {league}.{table_name}')
    cursor.commit()