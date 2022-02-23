from sql import connector

connection = connector.Cursor('dev_football_db')
cursor = connection.cursor



leagues = ['RFPL',  'LaLiga', 'Ligue1', 'SerieA'] #EPL', 'Bundesliga'


sp = 'factTeamShotZone_INSERT'


for leag in leagues:
    league = leag.lower()


    sproc_query = f"exec {league}.{sp}"
    cursor.execute(sproc_query)
    cursor.commit()

