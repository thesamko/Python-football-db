from sql import connector

landing_connection = connector.Cursor('landingdb')
landing_cursor = landing_connection.cursor

dev_connection = connector.Cursor('dev_football_db')
dev_cursor = dev_connection.cursor



leagues = ['RFPL', 'EPL', 'Bundesliga', 'LaLiga', 'Ligue1', 'SerieA']
for leag in leagues:
    league = leag.lower()

    query = f'''SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{league}' AND TABLE_TYPE = 'VIEW'
    AND TABLE_NAME not in ('dimMatches', 'dimTeams', 'factPlayerShotZone', 'dimPlayers')'''
    landing_cursor.execute(query)
    all_tables = [tb[0] for tb in landing_cursor.fetchall()]

    for table in all_tables:
        table_query = f"exec GenerateTableScript '{league}', {table}"
        landing_cursor.execute(table_query)
        table_statement = landing_cursor.fetchone()[0]
        dev_cursor.execute(table_statement)
        dev_cursor.commit()
